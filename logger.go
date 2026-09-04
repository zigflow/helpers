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
	"log/slog"

	"github.com/rs/zerolog"
	slogzerolog "github.com/samber/slog-zerolog/v2"
	"go.temporal.io/sdk/log"
)

// NewZerologHandler adapts a Zerolog logger to the Temporal SDK's logger
// interface, so client and worker logs join the application's own output.
// Levels and structured key/value pairs are passed through, and the level
// configured on zlog still applies.
//
// See [WithZerolog] to use the result as a client logger.
func NewZerologHandler(zlog *zerolog.Logger) log.Logger {
	return log.NewStructuredLogger(slog.New(slogzerolog.Option{
		Logger: zlog,
	}.NewZerologHandler()))
}
