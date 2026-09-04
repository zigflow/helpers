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
	"slices"

	"go.temporal.io/sdk/workflow"
)

// Compensator is a LIFO stack of compensation functions for the saga pattern.
//
// Usage pattern:
//  1. Declare a Compensator at the top of the workflow function. The zero
//     value is ready to use.
//  2. Defer a block that calls [Compensator.Compensate] when the workflow is
//     failing.
//  3. After each forward step succeeds, call [Compensator.Add] to register its
//     undo.
//  4. If the workflow fails or is cancelled, the deferred block calls
//     Compensate with the original workflow context, and the registered
//     functions run in reverse order.
//
// Compensate derives its own disconnected context, so callers never need
// [workflow.NewDisconnectedContext] themselves.
type Compensator struct {
	fns []func(workflow.Context) error
}

// Add registers a compensation function, undoing the forward step that has
// just succeeded. Registration order is the order steps succeed in, and
// [Compensator.Compensate] calls the functions in reverse.
func (c *Compensator) Add(fn func(workflow.Context) error) {
	c.fns = append(c.fns, fn)
}

// Compensate runs every registered compensation in reverse order.
//
// All of them are attempted even when one fails: a failure is logged through
// the workflow logger and the next compensation still runs. Nothing is
// returned, so the original workflow error is not replaced.
//
// Pass the original workflow context. Compensate derives a disconnected
// context from it, which keeps the parent's configuration but not its
// cancellation, so compensations still run for a workflow that is being
// cancelled. That context is cancelled once Compensate returns, so a
// compensation must complete its work before returning.
func (c *Compensator) Compensate(ctx workflow.Context) {
	disconnectedCtx, cancel := workflow.NewDisconnectedContext(ctx)
	defer cancel()

	for _, v := range slices.Backward(c.fns) {
		if err := v(disconnectedCtx); err != nil {
			workflow.GetLogger(disconnectedCtx).Error("compensation step failed", "error", err)
		}
	}
}
