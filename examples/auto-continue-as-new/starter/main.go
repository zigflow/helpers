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
	"context"

	"github.com/google/uuid"
	"github.com/rs/zerolog/log"
	temporal "github.com/zigflow/helpers"
	autocontinueasnew "github.com/zigflow/helpers/examples/auto-continue-as-new"
	"go.temporal.io/sdk/client"
)

func main() {
	c, err := temporal.NewConnectionWithEnvvars(
		temporal.WithZerolog(&log.Logger),
	)
	if err != nil {
		log.Fatal().Err(err).Msg("Unable to create client")
	}
	defer c.Close()

	workflowOptions := client.StartWorkflowOptions{
		ID:        "AutoContinueAsNew-" + uuid.NewString(),
		TaskQueue: "app",
	}

	ctx := context.Background()
	we, err := c.ExecuteWorkflow(ctx, workflowOptions, autocontinueasnew.Workflow, &autocontinueasnew.WorkflowInput{
		Name: "Dave",
	})
	if err != nil {
		log.Fatal().Err(err).Msg("Error executing workflow")
	}

	var result string
	if err := we.Get(ctx, &result); err != nil {
		log.Fatal().Err(err).Msg("Error getting result")
	}

	log.Info().Str("result", result).Msg("Workflow successfully executed")
}
