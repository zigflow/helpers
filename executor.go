/*
 * Copyright 2026 Zigflow authors <https://github.com/zigflow/helpers/graphs/contributors>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package temporal

import (
	"errors"
	"fmt"

	"go.temporal.io/sdk/workflow"
)

const (
	HookNameContinueAsNew = "ContinueAsNew"
)

var ErrSkipTask = errors.New("skip task")

type ExecutorOption func(e *Executor) error

type BeforeHookFn func(workflow.Context, *Executor) error

type AfterHookFn func(workflow.Context, *Executor, error) error

type Hook[T any] struct {
	Name string
	Fn   T
}

type Executor struct {
	before []Hook[BeforeHookFn]

	StartTaskID int
	Input       ExecutorInput
	TaskID      int
}

func (e *Executor) Execute(ctx workflow.Context, fn func(workflow.Context) error) error {
	logger := workflow.GetLogger(ctx)

	defer func() {
		e.TaskID += 1
		logger.Debug("Incrementing the executionCount", "taskId", e.TaskID)
	}()

	logger.Debug("Iterating over before hooks")
	for _, hook := range e.before {
		logger.Info("Executing before hook", "name", hook.Name, "taskId", e.TaskID)
		if err := hook.Fn(ctx, e); err != nil {
			if errors.Is(err, ErrSkipTask) {
				logger.Info("Skipping task", "taskId", e.TaskID)
				return nil
			}
			if workflow.IsContinueAsNewError(err) {
				logger.Debug("Continue as new", hook.Name, "error", err)
			} else {
				logger.Error("Hook errored", "name", hook.Name, "error", err)
			}
			return err
		}
	}

	logger.Debug("Executing function")
	if err := fn(ctx); err != nil {
		// Error running the function
		logger.Error("Error executing function", "error", err)
		return err
	}

	return nil
}

func NewExecutor(input ExecutorInput, opts ...ExecutorOption) (*Executor, error) {
	e := &Executor{
		before: []Hook[BeforeHookFn]{},
		Input:  input,
		TaskID: 0,
	}

	if input != nil {
		fmt.Println("---")
		fmt.Printf("%+v\n", input.GetTaskID())
		// fmt.Println(*input.GetTaskID())
		fmt.Println("---")
	}

	taskID := input.GetTaskID()
	if taskID != nil && *taskID > 0 {
		e.StartTaskID = *taskID
		// os.Exit(1)
	}

	for _, o := range opts {
		if err := o(e); err != nil {
			return nil, err
		}
	}

	return e, nil
}

type ExecutorInput interface {
	GetTaskID() *int
	SetTaskID(taskID int)
}

func NewExecutorWithDefaults(input ExecutorInput) (*Executor, error) {
	return NewExecutor(input, DefaultHooks...)
}

var DefaultHooks = []ExecutorOption{
	WithBeforeHook(
		Hook[BeforeHookFn]{
			Name: HookNameContinueAsNew,
			Fn:   HookContinueAsNew,
		},
	),
}

func WithBeforeHook(hook Hook[BeforeHookFn]) ExecutorOption {
	return func(e *Executor) error {
		e.before = append(e.before, hook)

		return nil
	}
}

// @todo(sje): do basically what Zigflow does:
// - check if continue as new suggested
//   - if it is, return a continue as new error
//   - if not, check if we've come from a CAN and should skip this task
func HookContinueAsNew(ctx workflow.Context, e *Executor) error {
	logger := workflow.GetLogger(ctx)
	info := workflow.GetInfo(ctx)

	isSuggested := info.GetContinueAsNewSuggested()
	if !isSuggested && e.TaskID > 5 {
		isSuggested = true
	}
	if isSuggested {
		logger.Info(
			"Continue-as-new suggested",
			"reason",
			info.GetContinueAsNewSuggestedReasons(), // Returns an array of ints
			"taskId",
			e.TaskID,
		)

		// Wait for handlers to finish
		if err := workflow.Await(ctx, func() bool {
			return workflow.AllHandlersFinished(ctx)
		}); err != nil {
			return fmt.Errorf("error waiting for handlers to finish: %w", err)
		}

		e.Input.SetTaskID(e.TaskID)

		return workflow.NewContinueAsNewError(ctx, info.WorkflowType.Name, e.Input)
	}
	fmt.Println("yes")
	fmt.Println(e.StartTaskID)
	fmt.Println(e.TaskID)
	// Continue-as-new not suggested - check if we're resuming and should skip
	logger.Debug("Continue-as-new not suggested")
	if e.StartTaskID > 0 {
		logger.Debug("Continuing workflow as new, checking if we should skip task", "taskId", e.TaskID, "lastTaskId", e.StartTaskID)
		if e.StartTaskID == e.TaskID {
			logger.Debug("Starting from this task", "taskId", e.TaskID, "lastTaskId", e.StartTaskID)
			e.StartTaskID = 0
		} else {
			logger.Debug("Skipping task", "taskId", e.TaskID, "lastTaskId", e.StartTaskID)
			return ErrSkipTask
		}
	}

	return nil
}
