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
	"io"
	"log/slog"

	"go.temporal.io/sdk/log"
)

// slogBackend describes log/slog behind the SDK's own adapter. Unlike Zerolog,
// log/slog needs no adapter from this package: log.NewStructuredLogger already
// turns an *slog.Logger into an SDK logger, so it is passed straight to
// [WithLogger]. The shared backend tests keep that documented usage honest.
func slogBackend() loggerBackend {
	newHandler := func(w io.Writer, minLevel slog.Level) slog.Handler {
		return slog.NewJSONHandler(w, &slog.HandlerOptions{Level: minLevel})
	}

	return loggerBackend{
		name: "slog",
		newLogger: func(w io.Writer, minLevel slog.Level) log.Logger {
			return log.NewStructuredLogger(slog.New(newHandler(w, minLevel)))
		},
		newOption: func(w io.Writer) Option {
			return WithLogger(log.NewStructuredLogger(slog.New(newHandler(w, slog.LevelDebug))))
		},
		levelKey:   slog.LevelKey,
		messageKey: slog.MessageKey,
		timeKey:    slog.TimeKey,
		levelName:  slog.Level.String,
	}
}
