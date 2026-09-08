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
	"io"
	"log/slog"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/log"
)

// Every logging backend the package documents is expected to behave the same
// way behind the SDK's logger interface: the level a method writes, how key and
// value pairs are rendered, that filtering is left to the backend, and that the
// optional Temporal interfaces work. Those expectations live here once and are
// driven against each backend by [TestLoggerBackends]. Anything genuinely
// specific to one library stays in that library's own test file.

// loggerBackend describes one logging library behind a Temporal logger. Levels
// are expressed as [slog.Level] because it is the only level type all of the
// backends have in common; each backend maps them onto its own.
type loggerBackend struct {
	// name identifies the backend in subtest names.
	name string

	// newLogger builds a Temporal logger that writes JSON to w, filtered at
	// minLevel and above.
	newLogger func(w io.Writer, minLevel slog.Level) log.Logger

	// newOption installs an equivalent logger on a connection, covering the
	// option callers are told to reach for. It logs everything, so that level
	// filtering stays the concern of newLogger.
	newOption func(w io.Writer) Option

	// levelKey, messageKey and timeKey are the JSON field names the backend
	// writes those parts of an entry to.
	levelKey, messageKey, timeKey string

	// levelName is the backend's own spelling of a level.
	levelName func(level slog.Level) string
}

// loggerBackends lists every logging backend the package supports or documents.
func loggerBackends() []loggerBackend {
	return []loggerBackend{
		zerologBackend(),
		slogBackend(),
		zapBackend(),
	}
}

// logMethods pairs each method of the SDK's logger interface with the level it
// is expected to write at.
var logMethods = []struct {
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

// TestLoggerBackends runs the shared expectations against every backend.
func TestLoggerBackends(t *testing.T) {
	t.Parallel()

	for _, backend := range loggerBackends() {
		t.Run(backend.name, func(t *testing.T) {
			t.Parallel()

			t.Run("levels", backend.assertLevels)
			t.Run("key values", backend.assertKeyValues)
			t.Run("level filtering", backend.assertLevelFiltering)
			t.Run("optional interfaces", backend.assertOptionalInterfaces)
			t.Run("uses the supplied logger", backend.assertUsesTheSuppliedLogger)
			t.Run("client option", backend.assertClientOption)
		})
	}
}

// buffered returns a logger writing JSON to a fresh buffer.
func (b *loggerBackend) buffered(minLevel slog.Level) (log.Logger, *bytes.Buffer) {
	var buf bytes.Buffer

	return b.newLogger(&buf, minLevel), &buf
}

// assertLevels covers the level each of the logger methods writes.
func (b *loggerBackend) assertLevels(t *testing.T) {
	t.Parallel()

	for _, method := range logMethods {
		t.Run(strings.ToLower(method.level.String()), func(t *testing.T) {
			t.Parallel()

			logger, buf := b.buffered(slog.LevelDebug)

			method.logFn(logger, "hello world")

			entry := onlyLine(t, buf)

			assert.Equal(t, b.levelName(method.level), entry[b.levelKey])
			assert.Equal(t, "hello world", entry[b.messageKey])
		})
	}
}

// assertKeyValues covers how key and value pairs reach the log line.
func (b *loggerBackend) assertKeyValues(t *testing.T) {
	t.Parallel()

	t.Run("string key values become fields", func(t *testing.T) {
		t.Parallel()

		logger, buf := b.buffered(slog.LevelDebug)

		logger.Info("hello", "workflowID", "abc-123", "attempt", 2, "retry", true)

		entry := onlyLine(t, buf)

		assert.Equal(t, "abc-123", entry["workflowID"])
		assert.Equal(t, float64(2), entry["attempt"])
		assert.Equal(t, true, entry["retry"])
	})

	t.Run("no key values", func(t *testing.T) {
		t.Parallel()

		logger, buf := b.buffered(slog.LevelDebug)

		logger.Info("hello")

		entry := onlyLine(t, buf)

		assert.Equal(t, "hello", entry[b.messageKey])
	})

	t.Run("errors are rendered", func(t *testing.T) {
		t.Parallel()

		logger, buf := b.buffered(slog.LevelDebug)

		logger.Error("it broke", "error", errors.New("something went wrong"))

		// The exact shape of an encoded error is the logging library's business;
		// what matters is that the message survives.
		assert.Contains(t, buf.String(), "something went wrong")
	})

	t.Run("every entry carries a timestamp", func(t *testing.T) {
		t.Parallel()

		logger, buf := b.buffered(slog.LevelDebug)

		logger.Info("hello")

		entry := onlyLine(t, buf)

		assert.NotEmpty(t, entry[b.timeKey])
	})
}

// assertLevelFiltering proves filtering is delegated to the supplied logger
// rather than done by the adapter.
func (b *loggerBackend) assertLevelFiltering(t *testing.T) {
	t.Parallel()

	logger, buf := b.buffered(slog.LevelWarn)

	logger.Debug("debug message")
	logger.Info("info message")
	logger.Warn("warn message")
	logger.Error("error message")

	lines := logLines(t, buf)

	require.Len(t, lines, 2)
	assert.Equal(t, "warn message", lines[0][b.messageKey])
	assert.Equal(t, "error message", lines[1][b.messageKey])
}

// assertOptionalInterfaces covers the optional Temporal logger interfaces,
// which the SDK uses to add context to log lines.
func (b *loggerBackend) assertOptionalInterfaces(t *testing.T) {
	t.Parallel()

	t.Run("with adds fields to every subsequent entry", func(t *testing.T) {
		t.Parallel()

		logger, buf := b.buffered(slog.LevelDebug)

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

		logger, buf := b.buffered(slog.LevelDebug)

		skipLogger, ok := logger.(log.WithSkipCallers)
		require.True(t, ok, "the logger should support log.WithSkipCallers")

		skipLogger.WithCallerSkip(1).Info("hello")

		assert.Equal(t, "hello", onlyLine(t, buf)[b.messageKey])
	})
}

// assertUsesTheSuppliedLogger proves the adapter writes to the logger it is
// given rather than to the library's global one.
func (b *loggerBackend) assertUsesTheSuppliedLogger(t *testing.T) {
	t.Parallel()

	firstLogger, first := b.buffered(slog.LevelDebug)
	secondLogger, second := b.buffered(slog.LevelDebug)

	firstLogger.Info("to the first logger")
	secondLogger.Info("to the second logger")

	assert.Contains(t, first.String(), "to the first logger")
	assert.NotContains(t, first.String(), "to the second logger")
	assert.Contains(t, second.String(), "to the second logger")
}

// assertClientOption covers the connection option that installs the logger.
func (b *loggerBackend) assertClientOption(t *testing.T) {
	t.Parallel()

	var buf bytes.Buffer

	o := mustApplyOptions(t, b.newOption(&buf))

	require.NotNil(t, o.Logger)

	o.Logger.Info("through the client options", "workflowID", "abc-123")

	entry := onlyLine(t, &buf)

	assert.Equal(t, b.levelName(slog.LevelInfo), entry[b.levelKey])
	assert.Equal(t, "through the client options", entry[b.messageKey])
	assert.Equal(t, "abc-123", entry["workflowID"])
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
