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
	"errors"
	"log/slog"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/log"
)

// Unlike Zerolog, log/slog needs no adapter from this package: the SDK ships
// log.NewStructuredLogger. These tests cover that combination behind
// [WithLogger] so the documented usage stays honest.

// newBufferedSlogLogger returns a Temporal logger, backed by log/slog, that
// writes JSON to the buffer.
func newBufferedSlogLogger(t *testing.T, level slog.Level) (log.Logger, *bytes.Buffer) {
	t.Helper()

	var buf bytes.Buffer

	logger := slog.New(slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: level}))

	return log.NewStructuredLogger(logger), &buf
}

func TestStructuredLoggerLevels(t *testing.T) {
	t.Parallel()

	// The level written to the log line is slog's own name for it, so the
	// expectation is taken from the level rather than spelled out again.
	tests := []struct {
		level slog.Level
		logFn func(l log.Logger, msg string, keyvals ...any)
	}{
		{
			level: slog.LevelDebug,
			logFn: func(l log.Logger, msg string, kv ...any) { l.Debug(msg, kv...) },
		},
		{
			level: slog.LevelInfo,
			logFn: func(l log.Logger, msg string, kv ...any) { l.Info(msg, kv...) },
		},
		{
			level: slog.LevelWarn,
			logFn: func(l log.Logger, msg string, kv ...any) { l.Warn(msg, kv...) },
		},
		{
			level: slog.LevelError,
			logFn: func(l log.Logger, msg string, kv ...any) { l.Error(msg, kv...) },
		},
	}

	for _, test := range tests {
		t.Run(strings.ToLower(test.level.String()), func(t *testing.T) {
			t.Parallel()

			logger, buf := newBufferedSlogLogger(t, slog.LevelDebug)

			test.logFn(logger, "hello world")

			entry := onlyLine(t, buf)

			assert.Equal(t, test.level.String(), entry[slog.LevelKey])
			assert.Equal(t, "hello world", entry[slog.MessageKey])
		})
	}
}

// TestStructuredLoggerKeyValues covers how key/value pairs reach the log line.
//
//nolint:dupl // deliberately mirrors the equivalent Zerolog test
func TestStructuredLoggerKeyValues(t *testing.T) {
	t.Parallel()

	t.Run("string key values become fields", func(t *testing.T) {
		t.Parallel()

		logger, buf := newBufferedSlogLogger(t, slog.LevelDebug)

		logger.Info("hello", "workflowID", "abc-123", "attempt", 2, "retry", true)

		entry := onlyLine(t, buf)

		assert.Equal(t, "abc-123", entry["workflowID"])
		assert.Equal(t, float64(2), entry["attempt"])
		assert.Equal(t, true, entry["retry"])
	})

	t.Run("no key values", func(t *testing.T) {
		t.Parallel()

		logger, buf := newBufferedSlogLogger(t, slog.LevelDebug)

		logger.Info("hello")

		entry := onlyLine(t, buf)

		assert.Equal(t, "hello", entry[slog.MessageKey])
	})

	t.Run("errors are rendered", func(t *testing.T) {
		t.Parallel()

		logger, buf := newBufferedSlogLogger(t, slog.LevelDebug)

		logger.Error("it broke", "error", errors.New("something went wrong"))

		// The exact shape of an encoded error is the logging library's business;
		// what matters is that the message survives.
		assert.Contains(t, buf.String(), "something went wrong")
	})

	t.Run("every entry carries a timestamp", func(t *testing.T) {
		t.Parallel()

		logger, buf := newBufferedSlogLogger(t, slog.LevelDebug)

		logger.Info("hello")

		entry := onlyLine(t, buf)

		assert.NotEmpty(t, entry[slog.TimeKey])
	})
}

// TestStructuredLoggerRespectsTheSlogLevel proves level filtering is delegated
// to the supplied slog logger.
func TestStructuredLoggerRespectsTheSlogLevel(t *testing.T) {
	t.Parallel()

	logger, buf := newBufferedSlogLogger(t, slog.LevelWarn)

	logger.Debug("debug message")
	logger.Info("info message")
	logger.Warn("warn message")
	logger.Error("error message")

	lines := logLines(t, buf)

	require.Len(t, lines, 2)
	assert.Equal(t, "warn message", lines[0][slog.MessageKey])
	assert.Equal(t, "error message", lines[1][slog.MessageKey])
}

// TestStructuredLoggerSupportsOptionalInterfaces covers the optional Temporal
// logger interfaces, which the SDK uses to add context to log lines.
//
//nolint:dupl // deliberately mirrors the equivalent Zerolog test
func TestStructuredLoggerSupportsOptionalInterfaces(t *testing.T) {
	t.Parallel()

	t.Run("with adds fields to every subsequent entry", func(t *testing.T) {
		t.Parallel()

		logger, buf := newBufferedSlogLogger(t, slog.LevelDebug)

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

		logger, buf := newBufferedSlogLogger(t, slog.LevelDebug)

		skipLogger, ok := logger.(log.WithSkipCallers)
		require.True(t, ok, "the logger should support log.WithSkipCallers")

		skipLogger.WithCallerSkip(1).Info("hello")

		assert.Equal(t, "hello", onlyLine(t, buf)[slog.MessageKey])
	})
}

// TestWithLoggerSlogUsesTheSuppliedLogger proves the SDK adapter writes to the
// slog logger it is given rather than slog's default logger.
func TestWithLoggerSlogUsesTheSuppliedLogger(t *testing.T) {
	t.Parallel()

	var first, second bytes.Buffer

	firstLog := slog.New(slog.NewJSONHandler(&first, nil))
	secondLog := slog.New(slog.NewJSONHandler(&second, nil))

	log.NewStructuredLogger(firstLog).Info("to the first logger")
	log.NewStructuredLogger(secondLog).Info("to the second logger")

	assert.Contains(t, first.String(), "to the first logger")
	assert.NotContains(t, first.String(), "to the second logger")
	assert.Contains(t, second.String(), "to the second logger")
}

// TestWithLoggerSlog covers the connection option that installs an slog-backed
// logger, which needs no adapter from this package.
func TestWithLoggerSlog(t *testing.T) {
	t.Parallel()

	var buf bytes.Buffer

	logger := slog.New(slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug}))

	o := mustApplyOptions(t, WithLogger(log.NewStructuredLogger(logger)))

	require.NotNil(t, o.Logger)

	o.Logger.Info("through the client options", "workflowID", "abc-123")

	entry := onlyLine(t, &buf)

	assert.Equal(t, "INFO", entry[slog.LevelKey])
	assert.Equal(t, "through the client options", entry[slog.MessageKey])
	assert.Equal(t, "abc-123", entry["workflowID"])
}
