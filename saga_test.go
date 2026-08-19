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
	"fmt"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/workflow"
)

// Compensation step names used by the tests.
const (
	stepFirst  = "first"
	stepSecond = "second"
	stepThird  = "third"
)

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

func TestCompensatorPassesTheWorkflowContext(t *testing.T) {
	var (
		received []workflow.Context
		given    workflow.Context
	)

	runInWorkflow(t, func(ctx workflow.Context) {
		var c Compensator

		for range 2 {
			c.Add(func(compensationCtx workflow.Context) error {
				received = append(received, compensationCtx)

				return nil
			})
		}

		given = ctx
		c.Compensate(ctx)
	})

	require.Len(t, received, 2)

	for i, ctx := range received {
		assert.Equal(t, given, ctx, "compensation %d should be given the context passed to Compensate", i)
		assert.NotNil(t, workflow.GetInfo(ctx), "the context should be a usable workflow context")
	}
}

// TestCompensatorCompensateIsNotIdempotent documents that Compensate does not
// clear the registered functions, so calling it twice runs them all twice.
func TestCompensatorCompensateIsNotIdempotent(t *testing.T) {
	rec := &recorder{}

	runInWorkflow(t, func(ctx workflow.Context) {
		var c Compensator

		c.Add(func(workflow.Context) error {
			rec.record(stepFirst)

			return nil
		})
		c.Add(func(workflow.Context) error {
			rec.record(stepSecond)

			return nil
		})

		c.Compensate(ctx)
		c.Compensate(ctx)
	})

	assert.Equal(t, []string{stepSecond, stepFirst, stepSecond, stepFirst}, rec.recorded())
}

// TestCompensatorAddAfterCompensate covers registering another compensation
// after a first round has already run.
func TestCompensatorAddAfterCompensate(t *testing.T) {
	rec := &recorder{}

	runInWorkflow(t, func(ctx workflow.Context) {
		var c Compensator

		c.Add(func(workflow.Context) error {
			rec.record(stepFirst)

			return nil
		})

		c.Compensate(ctx)

		c.Add(func(workflow.Context) error {
			rec.record(stepSecond)

			return nil
		})

		c.Compensate(ctx)
	})

	assert.Equal(t, []string{stepFirst, stepSecond, stepFirst}, rec.recorded())
}

// TestCompensatorZeroValue proves a Compensator needs no construction.
func TestCompensatorZeroValue(t *testing.T) {
	runInWorkflow(t, func(ctx workflow.Context) {
		var c Compensator

		assert.NotPanics(t, func() { c.Compensate(ctx) })
	})
}
