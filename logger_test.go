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
	"bytes"
	"io"
	"log/slog"
	"testing"

	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"go.temporal.io/sdk/log"
)

// zerologLevels maps the levels the shared backend tests are driven by onto
// zerolog's own.
var zerologLevels = map[slog.Level]zerolog.Level{
	slog.LevelDebug: zerolog.DebugLevel,
	slog.LevelInfo:  zerolog.InfoLevel,
	slog.LevelWarn:  zerolog.WarnLevel,
	slog.LevelError: zerolog.ErrorLevel,
}

// zerologBackend describes [NewZerologHandler], the adapter this package
// provides because Zerolog does not satisfy the SDK's logger interface itself.
func zerologBackend() loggerBackend {
	newZerolog := func(w io.Writer, minLevel slog.Level) *zerolog.Logger {
		zlog := zerolog.New(w).Level(zerologLevels[minLevel])

		return &zlog
	}

	return loggerBackend{
		name: "zerolog",
		newLogger: func(w io.Writer, minLevel slog.Level) log.Logger {
			return NewZerologHandler(newZerolog(w, minLevel))
		},
		newOption: func(w io.Writer) Option {
			return WithZerolog(newZerolog(w, slog.LevelDebug))
		},
		levelKey:   zerolog.LevelFieldName,
		messageKey: zerolog.MessageFieldName,
		timeKey:    zerolog.TimestampFieldName,
		levelName: func(level slog.Level) string {
			return zerologLevels[level].String()
		},
	}
}

// TestNewZerologHandlerDisabled covers a completely silenced logger. It is not
// part of the shared backend tests because [zerolog.Disabled] has no equivalent
// among the slog levels those are driven by.
func TestNewZerologHandlerDisabled(t *testing.T) {
	t.Parallel()

	var buf bytes.Buffer

	zlog := zerolog.New(&buf).Level(zerolog.Disabled)

	logger := NewZerologHandler(&zlog)
	logger.Debug("debug message")
	logger.Error("error message")

	assert.Empty(t, buf.String())
}
