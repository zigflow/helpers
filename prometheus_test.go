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
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"testing"
	"time"

	prom "github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// freeListenAddress is an address the OS will turn into an unused port, so tests
// never depend on a specific port being available.
const freeListenAddress = "127.0.0.1:0"

// promFatalEnv marks the child process used to observe the fatal error path.
const promFatalEnv = "TEMPORAL_HELPERS_TEST_PROMETHEUS_FATAL"

// registeredMetricNames returns the names of every metric family in a registry.
func registeredMetricNames(t *testing.T, registry *prom.Registry) []string {
	t.Helper()

	families, err := registry.Gather()
	require.NoError(t, err)

	names := make([]string, 0, len(families))
	for _, family := range families {
		names = append(names, family.GetName())
	}

	return names
}

func TestNewPrometheusHandler(t *testing.T) {
	t.Parallel()

	t.Run("registers metrics against the supplied registry", func(t *testing.T) {
		t.Parallel()

		registry := prom.NewRegistry()

		handler, err := NewPrometheusHandler(freeListenAddress, "myprefix", registry)

		require.NoError(t, err)
		require.NotNil(t, handler)

		// Creating a metric registers it with the registry immediately.
		handler.Counter("things_done").Inc(1)

		assert.Contains(t, registeredMetricNames(t, registry), "myprefix_things_done_total")
	})

	t.Run("no prefix leaves metric names unprefixed", func(t *testing.T) {
		t.Parallel()

		registry := prom.NewRegistry()

		handler, err := NewPrometheusHandler(freeListenAddress, "", registry)

		require.NoError(t, err)

		handler.Counter("unprefixed_things").Inc(1)

		assert.Contains(t, registeredMetricNames(t, registry), "unprefixed_things_total")
	})

	t.Run("metric names are sanitised for prometheus", func(t *testing.T) {
		t.Parallel()

		registry := prom.NewRegistry()

		handler, err := NewPrometheusHandler(freeListenAddress, "my.prefix", registry)

		require.NoError(t, err)

		handler.Counter("things.done").Inc(1)
		handler.Gauge("things.pending").Update(1)

		names := registeredMetricNames(t, registry)

		assert.Contains(t, names, "my_prefix_things_done_total")
		assert.Contains(t, names, "my_prefix_things_pending")
	})

	t.Run("tags become sanitised prometheus labels", func(t *testing.T) {
		t.Parallel()

		registry := prom.NewRegistry()

		handler, err := NewPrometheusHandler(freeListenAddress, "tagged", registry)

		require.NoError(t, err)

		handler.WithTags(map[string]string{"task.queue": "my-queue"}).Counter("tagged_things").Inc(1)

		families, err := registry.Gather()
		require.NoError(t, err)

		var labels []string

		for _, family := range families {
			if family.GetName() != "tagged_tagged_things_total" {
				continue
			}

			for _, metric := range family.GetMetric() {
				for _, label := range metric.GetLabel() {
					labels = append(labels, label.GetName()+"="+label.GetValue())
				}
			}
		}

		// Label names and values are both sanitised by the configured options.
		assert.Equal(t, []string{"task_queue=my_queue"}, labels)
	})

	t.Run("a nil registry uses the prometheus default registry", func(t *testing.T) {
		t.Parallel()

		handler, err := NewPrometheusHandler(freeListenAddress, "defaultregistry", nil)

		require.NoError(t, err)

		handler.Counter("things_done").Inc(1)

		families, err := prom.DefaultGatherer.Gather()
		require.NoError(t, err)

		names := make([]string, 0, len(families))
		for _, family := range families {
			names = append(names, family.GetName())
		}

		assert.Contains(t, names, "defaultregistry_things_done_total")
	})
}

// TestNewPrometheusHandlerWithoutListenAddress covers the branch where no listen
// address is given and the metrics handler is instead attached to Go's default
// HTTP mux. There can only ever be one such test in the package because the
// underlying library panics if "/metrics" is registered on that mux twice.
func TestNewPrometheusHandlerWithoutListenAddress(t *testing.T) {
	registry := prom.NewRegistry()

	handler, err := NewPrometheusHandler("", "defaultmux", registry)

	require.NoError(t, err)

	handler.Counter("things_done").Inc(1)

	rec := httptest.NewRecorder()
	http.DefaultServeMux.ServeHTTP(rec, httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/metrics", http.NoBody))

	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "defaultmux_things_done_total")
}

// TestWithPrometheusMetrics covers the connection option wrapper.
func TestWithPrometheusMetrics(t *testing.T) {
	t.Parallel()

	registry := prom.NewRegistry()

	o := mustApplyOptions(t, WithPrometheusMetrics(freeListenAddress, "clientoption", registry))

	require.NotNil(t, o.MetricsHandler)

	o.MetricsHandler.Counter("things_done").Inc(1)

	assert.Contains(t, registeredMetricNames(t, registry), "clientoption_things_done_total")
}

// TestNewPrometheusHandlerInvalidListenAddress documents that an unusable listen
// address does not produce an error from NewPrometheusHandler. The failure
// happens asynchronously on the reporter's goroutine, which calls the OnError
// handler, which calls log.Fatal and terminates the process. That has to be
// observed from a child process.
func TestNewPrometheusHandlerInvalidListenAddress(t *testing.T) {
	if os.Getenv(promFatalEnv) == "1" {
		runInvalidListenAddressChild()

		return
	}

	// The child is killed by log.Fatal; the timeout only stops the test hanging
	// forever if that ever stops being true.
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	//nolint:gosec // deliberately re-executes this test binary
	cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestNewPrometheusHandlerInvalidListenAddress$")
	cmd.Env = append(os.Environ(), promFatalEnv+"=1")

	// The child blocks on stdin until it is killed, so it never trips the Go
	// deadlock detector and the test needs no sleeps.
	stdin, err := cmd.StdinPipe()
	require.NoError(t, err)

	defer func() {
		_ = stdin.Close()
	}()

	var stderr bytes.Buffer
	cmd.Stderr = &stderr

	err = cmd.Run()

	var exitErr *exec.ExitError
	require.ErrorAs(t, err, &exitErr, "stderr: %s", stderr.String())
	assert.Equal(t, 1, exitErr.ExitCode(), "stderr: %s", stderr.String())
	assert.Contains(t, stderr.String(), "Error in Prometheus reporter")
}

// runInvalidListenAddressChild is the child half of
// TestNewPrometheusHandlerInvalidListenAddress.
func runInvalidListenAddressChild() {
	handler, err := NewPrometheusHandler("not-a-valid-address", "child", prom.NewRegistry())
	if err != nil {
		fmt.Fprintln(os.Stderr, "unexpected error from NewPrometheusHandler:", err)
		os.Exit(2)
	}

	if handler == nil {
		fmt.Fprintln(os.Stderr, "unexpected nil handler")
		os.Exit(2)
	}

	// Block until the reporter's goroutine terminates the process.
	_, _ = io.Copy(io.Discard, os.Stdin)

	fmt.Fprintln(os.Stderr, "the process was not terminated")
	os.Exit(3)
}
