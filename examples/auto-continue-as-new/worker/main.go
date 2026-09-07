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

package main

import (
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	temporal "github.com/zigflow/helpers"
	autocontinueasnew "github.com/zigflow/helpers/examples/auto-continue-as-new"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
)

func main() {
	zerolog.SetGlobalLevel(zerolog.InfoLevel)
	c, err := temporal.NewConnectionWithEnvvars(
		temporal.WithZerolog(&log.Logger),
	)
	if err != nil {
		log.Fatal().Err(err).Msg("Unable to create client")
	}
	defer c.Close()

	w := worker.New(c, "app", worker.Options{})

	w.RegisterWorkflowWithOptions(autocontinueasnew.Workflow, workflow.RegisterOptions{
		Name: "helloworld",
	})
	w.RegisterActivity(autocontinueasnew.Activity)

	if err := w.Run(worker.InterruptCh()); err != nil {
		log.Fatal().Err(err).Msg("Error starting workflow")
	}
}
