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
//  4. If the workflow returns an error, the deferred block compensates, running
//     the registered functions in reverse order.
//
// Compensate uses whichever [workflow.Context] the caller passes it. It does
// not create a disconnected context, so a workflow that has to compensate
// after cancellation must create one itself with
// [workflow.NewDisconnectedContext].
type Compensator struct {
	fns []func(workflow.Context) error
}

// Add registers a compensation function, undoing the forward step that has
// just succeeded. Registration order is the order steps succeed in, and
// [Compensator.Compensate] calls the functions in reverse.
func (c *Compensator) Add(fn func(workflow.Context) error) {
	c.fns = append(c.fns, fn)
}

// Compensate runs every registered compensation in reverse order, using ctx.
//
// All of them are attempted even when one fails: a failure is logged through
// the workflow logger and the next compensation still runs. Nothing is
// returned, so the original workflow error is not masked.
func (c *Compensator) Compensate(ctx workflow.Context) {
	for _, v := range slices.Backward(c.fns) {
		if err := v(ctx); err != nil {
			workflow.GetLogger(ctx).Error("compensation step failed", "error", err)
		}
	}
}
