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
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/log"
)

// newBufferedLogger returns a Temporal logger that writes JSON to the buffer.
func newBufferedLogger(t *testing.T, level zerolog.Level) (log.Logger, *bytes.Buffer) {
	t.Helper()

	var buf bytes.Buffer

	zlog := zerolog.New(&buf).Level(level)

	return NewZerologHandler(&zlog), &buf
}

// logLines decodes each JSON line written to the buffer.
func logLines(t *testing.T, buf *bytes.Buffer) []map[string]any {
	t.Helper()

	var lines []map[string]any

	for raw := range strings.SplitSeq(strings.TrimSpace(buf.String()), "\n") {
		if raw == "" {
			continue
		}

		entry := map[string]any{}
		require.NoError(t, json.Unmarshal([]byte(raw), &entry), "log line should be valid JSON: %s", raw)

		lines = append(lines, entry)
	}

	return lines
}

// onlyLine decodes the buffer and asserts exactly one entry was written.
func onlyLine(t *testing.T, buf *bytes.Buffer) map[string]any {
	t.Helper()

	lines := logLines(t, buf)
	require.Len(t, lines, 1)

	return lines[0]
}

func TestNewZerologHandlerLevels(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		logFn         func(l log.Logger, msg string, keyvals ...any)
		expectedLevel string
	}{
		{
			name:          "debug",
			logFn:         func(l log.Logger, msg string, kv ...any) { l.Debug(msg, kv...) },
			expectedLevel: "debug",
		},
		{
			name:          "info",
			logFn:         func(l log.Logger, msg string, kv ...any) { l.Info(msg, kv...) },
			expectedLevel: "info",
		},
		{
			name:          "warn",
			logFn:         func(l log.Logger, msg string, kv ...any) { l.Warn(msg, kv...) },
			expectedLevel: "warn",
		},
		{
			name:          "error",
			logFn:         func(l log.Logger, msg string, kv ...any) { l.Error(msg, kv...) },
			expectedLevel: "error",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			logger, buf := newBufferedLogger(t, zerolog.TraceLevel)

			test.logFn(logger, "hello world")

			entry := onlyLine(t, buf)

			assert.Equal(t, test.expectedLevel, entry[zerolog.LevelFieldName])
			assert.Equal(t, "hello world", entry[zerolog.MessageFieldName])
		})
	}
}

func TestNewZerologHandlerKeyValues(t *testing.T) {
	t.Parallel()

	t.Run("string key values become fields", func(t *testing.T) {
		t.Parallel()

		logger, buf := newBufferedLogger(t, zerolog.TraceLevel)

		logger.Info("hello", "workflowID", "abc-123", "attempt", 2, "retry", true)

		entry := onlyLine(t, buf)

		assert.Equal(t, "abc-123", entry["workflowID"])
		assert.Equal(t, float64(2), entry["attempt"])
		assert.Equal(t, true, entry["retry"])
	})

	t.Run("no key values", func(t *testing.T) {
		t.Parallel()

		logger, buf := newBufferedLogger(t, zerolog.TraceLevel)

		logger.Info("hello")

		entry := onlyLine(t, buf)

		assert.Equal(t, "hello", entry[zerolog.MessageFieldName])
	})

	t.Run("errors are rendered", func(t *testing.T) {
		t.Parallel()

		logger, buf := newBufferedLogger(t, zerolog.TraceLevel)

		logger.Error("it broke", "error", errors.New("something went wrong"))

		// The exact shape of an encoded error is the logging library's business;
		// what matters is that the message survives.
		assert.Contains(t, buf.String(), "something went wrong")
	})

	t.Run("every entry carries a timestamp", func(t *testing.T) {
		t.Parallel()

		logger, buf := newBufferedLogger(t, zerolog.TraceLevel)

		logger.Info("hello")

		entry := onlyLine(t, buf)

		assert.NotEmpty(t, entry[zerolog.TimestampFieldName])
	})
}

// TestNewZerologHandlerRespectsTheZerologLevel proves level filtering is
// delegated to the supplied zerolog logger.
func TestNewZerologHandlerRespectsTheZerologLevel(t *testing.T) {
	t.Parallel()

	logger, buf := newBufferedLogger(t, zerolog.WarnLevel)

	logger.Debug("debug message")
	logger.Info("info message")
	logger.Warn("warn message")
	logger.Error("error message")

	lines := logLines(t, buf)

	require.Len(t, lines, 2)
	assert.Equal(t, "warn message", lines[0][zerolog.MessageFieldName])
	assert.Equal(t, "error message", lines[1][zerolog.MessageFieldName])
}

// TestNewZerologHandlerDisabled covers a completely silenced logger.
func TestNewZerologHandlerDisabled(t *testing.T) {
	t.Parallel()

	logger, buf := newBufferedLogger(t, zerolog.Disabled)

	logger.Debug("debug message")
	logger.Error("error message")

	assert.Empty(t, buf.String())
}

// TestNewZerologHandlerSupportsOptionalInterfaces covers the optional Temporal
// logger interfaces, which the SDK uses to add context to log lines.
func TestNewZerologHandlerSupportsOptionalInterfaces(t *testing.T) {
	t.Parallel()

	t.Run("with adds fields to every subsequent entry", func(t *testing.T) {
		t.Parallel()

		logger, buf := newBufferedLogger(t, zerolog.TraceLevel)

		withLogger, ok := logger.(log.WithLogger)
		require.True(t, ok, "the logger should support log.WithLogger")

		child := withLogger.With("component", "worker")
		child.Info("first")
		child.Warn("second")

		lines := logLines(t, buf)

		require.Len(t, lines, 2)

		for _, line := range lines {
			assert.Equal(t, "worker", line["component"])
		}

		// The parent logger is unchanged.
		buf.Reset()
		logger.Info("third")
		assert.NotContains(t, onlyLine(t, buf), "component")
	})

	t.Run("caller skip returns a working logger", func(t *testing.T) {
		t.Parallel()

		logger, buf := newBufferedLogger(t, zerolog.TraceLevel)

		skipLogger, ok := logger.(log.WithSkipCallers)
		require.True(t, ok, "the logger should support log.WithSkipCallers")

		skipLogger.WithCallerSkip(1).Info("hello")

		assert.Equal(t, "hello", onlyLine(t, buf)[zerolog.MessageFieldName])
	})
}

// TestNewZerologHandlerUsesTheSuppliedLogger proves the handler writes to the
// logger it is given rather than zerolog's global logger.
func TestNewZerologHandlerUsesTheSuppliedLogger(t *testing.T) {
	t.Parallel()

	var first, second bytes.Buffer

	firstLog := zerolog.New(&first)
	secondLog := zerolog.New(&second)

	NewZerologHandler(&firstLog).Info("to the first logger")
	NewZerologHandler(&secondLog).Info("to the second logger")

	assert.Contains(t, first.String(), "to the first logger")
	assert.NotContains(t, first.String(), "to the second logger")
	assert.Contains(t, second.String(), "to the second logger")
}

// TestWithZerolog covers the connection option that installs the adapter.
func TestWithZerolog(t *testing.T) {
	t.Parallel()

	var buf bytes.Buffer

	zlog := zerolog.New(&buf)

	o := mustApplyOptions(t, WithZerolog(&zlog))

	require.NotNil(t, o.Logger)

	o.Logger.Info("through the client options")

	assert.Contains(t, buf.String(), "through the client options")
}
