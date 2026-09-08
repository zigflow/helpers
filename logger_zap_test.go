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
	"go.uber.org/zap"
	"go.uber.org/zap/exp/zapslog"
	"go.uber.org/zap/zapcore"
)

// zapEncoderConfig is the encoder configuration the Zap backend is tested with.
// It is the one zap.NewProduction uses, so the field names asserted against are
// the ones the documented example produces.
var zapEncoderConfig = zap.NewProductionEncoderConfig()

// zapLevels maps the levels the shared backend tests are driven by onto zap's
// own.
var zapLevels = map[slog.Level]zapcore.Level{
	slog.LevelDebug: zapcore.DebugLevel,
	slog.LevelInfo:  zapcore.InfoLevel,
	slog.LevelWarn:  zapcore.WarnLevel,
	slog.LevelError: zapcore.ErrorLevel,
}

// zapBackend describes Zap behind its slog bridge. Like log/slog, Zap needs no
// adapter from this package: zapslog bridges a zapcore.Core into a slog.Handler,
// which the SDK's log.NewStructuredLogger then accepts. The shared backend
// tests keep that documented chain honest.
func zapBackend() loggerBackend {
	newHandler := func(w io.Writer, minLevel slog.Level) slog.Handler {
		core := zapcore.NewCore(
			zapcore.NewJSONEncoder(zapEncoderConfig),
			zapcore.AddSync(w),
			zapLevels[minLevel],
		)

		return zapslog.NewHandler(zap.New(core).Core())
	}

	return loggerBackend{
		name: "zap",
		newLogger: func(w io.Writer, minLevel slog.Level) log.Logger {
			return log.NewStructuredLogger(slog.New(newHandler(w, minLevel)))
		},
		newOption: func(w io.Writer) Option {
			return WithLogger(log.NewStructuredLogger(slog.New(newHandler(w, slog.LevelDebug))))
		},
		levelKey:   zapEncoderConfig.LevelKey,
		messageKey: zapEncoderConfig.MessageKey,
		timeKey:    zapEncoderConfig.TimeKey,
		levelName: func(level slog.Level) string {
			return zapLevels[level].String()
		},
	}
}
