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
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/activity"
	sdktemporal "go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/workflow"
)

// Compensation step names used by the tests.
const (
	stepFirst  = "first"
	stepSecond = "second"
	stepThird  = "third"
)

// undoActivityName is the registered name of the activity the compensations
// run, so that a compensation is proved to have done real workflow work rather
// than merely to have been called.
const undoActivityName = "undo"

// compensationTaskQueue is a non-default task queue put on the parent context's
// activity options. An activity that reports running on it proves the context
// Compensate builds inherited the parent's configuration.
const compensationTaskQueue = "compensation-queue"

// undoActivity reports the step it undid alongside the task queue it ran on, so
// the workflow can assert both that the compensation completed and which
// activity options it ran with.
func undoActivity(ctx context.Context, step string) (string, error) {
	return step + "@" + activity.GetInfo(ctx).TaskQueue, nil
}

// recorder collects the order in which compensations ran.
type recorder struct {
	mu    sync.Mutex
	calls []string
}

func (r *recorder) record(name string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.calls = append(r.calls, name)
}

func (r *recorder) recorded() []string {
	r.mu.Lock()
	defer r.mu.Unlock()

	return append([]string(nil), r.calls...)
}

// runInWorkflow executes fn inside the Temporal test environment so that it gets
// a real workflow.Context, and fails the test if the workflow does not complete.
func runInWorkflow(t *testing.T, fn func(ctx workflow.Context)) {
	t.Helper()

	var suite testsuite.WorkflowTestSuite

	env := suite.NewTestWorkflowEnvironment()
	env.RegisterWorkflowWithOptions(func(ctx workflow.Context) error {
		fn(ctx)

		return nil
	}, workflow.RegisterOptions{Name: "compensator-test"})

	env.ExecuteWorkflow("compensator-test")

	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())
}

func TestCompensator(t *testing.T) {
	tests := []struct {
		name string
		// steps are the compensation names to register, in registration order.
		steps []string
		// failing are the steps that return an error.
		failing map[string]bool
		// expected is the order the compensations are expected to run in.
		expected []string
	}{
		{
			name:     "no compensations",
			steps:    nil,
			expected: nil,
		},
		{
			name:     "one compensation",
			steps:    []string{stepFirst},
			expected: []string{stepFirst},
		},
		{
			name:     "multiple compensations run in LIFO order",
			steps:    []string{stepFirst, stepSecond, stepThird},
			expected: []string{stepThird, stepSecond, stepFirst},
		},
		{
			name:     "a failing compensation does not stop the others",
			steps:    []string{stepFirst, stepSecond, stepThird},
			failing:  map[string]bool{stepThird: true},
			expected: []string{stepThird, stepSecond, stepFirst},
		},
		{
			name:     "every compensation is attempted when they all fail",
			steps:    []string{stepFirst, stepSecond, stepThird},
			failing:  map[string]bool{stepFirst: true, stepSecond: true, stepThird: true},
			expected: []string{stepThird, stepSecond, stepFirst},
		},
		{
			name:     "a failure in the middle does not stop the rest",
			steps:    []string{stepFirst, stepSecond, stepThird},
			failing:  map[string]bool{stepSecond: true},
			expected: []string{stepThird, stepSecond, stepFirst},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			rec := &recorder{}

			runInWorkflow(t, func(ctx workflow.Context) {
				var c Compensator

				for _, step := range test.steps {
					c.Add(func(workflow.Context) error {
						rec.record(step)

						if test.failing[step] {
							return fmt.Errorf("%s failed: %w", step, errBoom)
						}

						return nil
					})
				}

				assert.NotPanics(t, func() { c.Compensate(ctx) })
			})

			assert.Equal(t, test.expected, rec.recorded())
		})
	}
}

// TestCompensatorCompensatesAfterTheParentContextIsCancelled covers the
// disconnected context Compensate builds, by observable behaviour rather than by
// comparing contexts.
//
// The context handed to Compensate is cancelled first, and the same activity
// call is made twice: once on that cancelled context, where it must fail, and
// once from inside each compensation, where it must complete. Activity options
// are only ever set on the parent, before cancellation, and carry a non-default
// task queue, so an activity that completes at all also proves the compensation
// context inherited the parent's configuration.
func TestCompensatorCompensatesAfterTheParentContextIsCancelled(t *testing.T) {
	var suite testsuite.WorkflowTestSuite

	env := suite.NewTestWorkflowEnvironment()
	env.RegisterActivityWithOptions(undoActivity, activity.RegisterOptions{Name: undoActivityName})

	rec := &recorder{}

	// parentErr is the result of using the cancelled context directly. It is
	// what makes the compensations' success meaningful.
	var parentErr error

	env.RegisterWorkflowWithOptions(func(ctx workflow.Context) error {
		ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
			StartToCloseTimeout: time.Minute,
			TaskQueue:           compensationTaskQueue,
		})

		cancellableCtx, cancel := workflow.WithCancel(ctx)

		var c Compensator

		for _, step := range []string{stepFirst, stepSecond} {
			c.Add(func(compensationCtx workflow.Context) error {
				var undone string

				// No activity options are set here: whatever the compensation
				// runs with has to have come from the parent context.
				if err := workflow.ExecuteActivity(compensationCtx, undoActivityName, step).
					Get(compensationCtx, &undone); err != nil {
					return err
				}

				rec.record(undone)

				return nil
			})
		}

		// Cancel the context Compensate is given, standing in for a workflow
		// that is being cancelled.
		cancel()

		parentErr = workflow.ExecuteActivity(cancellableCtx, undoActivityName, "parent").
			Get(cancellableCtx, nil)

		c.Compensate(cancellableCtx)

		return nil
	}, workflow.RegisterOptions{Name: "compensator-cancelled-parent"})

	env.ExecuteWorkflow("compensator-cancelled-parent")

	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())

	require.Error(t, parentErr, "the context given to Compensate must really be cancelled")
	assert.True(
		t,
		sdktemporal.IsCanceledError(parentErr),
		"the parent context should fail activities with a cancellation error, got %v", parentErr,
	)

	assert.Equal(
		t,
		[]string{
			stepSecond + "@" + compensationTaskQueue,
			stepFirst + "@" + compensationTaskQueue,
		},
		rec.recorded(),
		"every compensation should run its activity to completion, in LIFO order, "+
			"on the parent's activity options",
	)
}

// TestCompensatorZeroValue proves a Compensator needs no construction.
func TestCompensatorZeroValue(t *testing.T) {
	runInWorkflow(t, func(ctx workflow.Context) {
		var c Compensator

		assert.NotPanics(t, func() { c.Compensate(ctx) })
	})
}
