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

package autocontinueasnew

import (
	"context"
	"fmt"
	"time"

	temporal "github.com/zigflow/helpers"
	"go.temporal.io/sdk/workflow"
)

type WorkflowInput struct {
	Name   string `json:"name"`
	TaskID *int   `json:"taskId"`
}

func (i *WorkflowInput) GetTaskID() *int {
	return i.TaskID
}

func (i *WorkflowInput) SetTaskID(taskID int) {
	i.TaskID = &taskID
}

var _ temporal.ExecutorInput = &WorkflowInput{}

func Workflow(ctx workflow.Context, input *WorkflowInput) (string, error) {
	ao := workflow.ActivityOptions{
		StartToCloseTimeout: 10 * time.Second,
	}
	ctx = workflow.WithActivityOptions(ctx, ao)

	executor, err := temporal.NewExecutorWithDefaults(input)
	if err != nil {
		return "", err
	}

	var result string
	for i := range 7 {
		if err := executor.Execute(ctx, func(ctx workflow.Context) error {
			opts := workflow.GetActivityOptions(ctx)
			opts.Summary = fmt.Sprintf("Activity #%d", i)

			return workflow.ExecuteActivity(workflow.WithActivityOptions(ctx, opts), Activity, input.Name).Get(ctx, &result)
		}); err != nil {
			return "", err
		}
	}

	return result, nil
}

func Activity(ctx context.Context, name string) (string, error) {
	return "Hello " + name + "!", nil
}
