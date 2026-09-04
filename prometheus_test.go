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
	"sync"
	"testing"
	"time"

	prom "github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/client"
)

// freeListenAddress is an address the OS will turn into an unused port, so tests
// never depend on a specific port being available.
const freeListenAddress = "127.0.0.1:0"

// promFatalEnv names the child process used to observe the fatal error path and
// selects which failure that child triggers.
const promFatalEnv = "TEMPORAL_HELPERS_TEST_PROMETHEUS_FATAL"

// The failures the fatal child process can trigger.
const (
	fatalModeListen   = "listen"
	fatalModeRegister = "register"
)

// Names used by the error handler tests. The metric is registered up front by
// the test so that tally's own registration of the same name fails.
const (
	collidingMetric = "colliding_counter"
	collidingHelp   = "registered by the test so that tally's registration collides"
)

// errorHandlerMessage is the error NewPrometheusHandler returns when it is given
// more than one error handler.
const errorHandlerMessage = "only a single error handler may be supplied"

// metricsPath is the path tally registers its handler on.
const metricsPath = "/metrics"

// collidingCounter builds a counter registered under the name tally derives for
// prefix and name - "<prefix>_<name>_total" - but with a different help string,
// so registering both in one registry is a guaranteed error.
func collidingCounter(prefix, name string) (collector prom.Collector, fqName string) {
	fqName = prefix + "_" + name + "_total"

	return prom.NewCounter(prom.CounterOpts{Name: fqName, Help: collidingHelp}), fqName
}

// collidingRegistry returns a dedicated registry that already holds the metric
// tally will try to register, along with that metric's name.
func collidingRegistry(t *testing.T, prefix string) (registry *prom.Registry, fqName string) {
	t.Helper()

	registry = prom.NewRegistry()

	collector, fqName := collidingCounter(prefix, collidingMetric)
	require.NoError(t, registry.Register(collector))

	return registry, fqName
}

// errorRecorder records the errors an onError handler is called with. It is
// mutex guarded because tally may report from its own goroutine.
type errorRecorder struct {
	mu   sync.Mutex
	errs []error
}

func (r *errorRecorder) onError(err error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.errs = append(r.errs, err)
}

func (r *errorRecorder) errors() []error {
	r.mu.Lock()
	defer r.mu.Unlock()

	return append([]error(nil), r.errs...)
}

// isolateDefaultServeMux swaps http.DefaultServeMux for an empty mux and
// restores the original when the test finishes. Tests that give
// NewPrometheusHandler no listen address need this: tally registers its handler
// on whatever mux is installed, and that mux panics on a second registration of
// the same path. Callers must not be parallel.
func isolateDefaultServeMux(t *testing.T) *http.ServeMux {
	t.Helper()

	original := http.DefaultServeMux
	t.Cleanup(func() { http.DefaultServeMux = original })

	mux := http.NewServeMux()
	http.DefaultServeMux = mux

	return mux
}

// isolateDefaultRegistry swaps the Prometheus default registerer and gatherer
// for a dedicated registry and restores the originals when the test finishes, so
// a test exercising the nil registry path leaves no metrics behind in the real
// default registry. Callers must not be parallel.
func isolateDefaultRegistry(t *testing.T) *prom.Registry {
	t.Helper()

	originalRegisterer, originalGatherer := prom.DefaultRegisterer, prom.DefaultGatherer
	t.Cleanup(func() {
		prom.DefaultRegisterer, prom.DefaultGatherer = originalRegisterer, originalGatherer
	})

	registry := prom.NewRegistry()
	prom.DefaultRegisterer, prom.DefaultGatherer = registry, registry

	return registry
}

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
}

// TestNewPrometheusHandlerNilRegistry covers the branch where no registry is
// given and metrics go to the Prometheus default registerer instead. It is not
// parallel and swaps the default registerer and gatherer for a dedicated
// registry, so the metric it registers cannot collide with a repeat run of the
// test or leak into anything else that uses the real default registry.
func TestNewPrometheusHandlerNilRegistry(t *testing.T) {
	registry := isolateDefaultRegistry(t)

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
	assert.Equal(t, names, registeredMetricNames(t, registry), "the default gatherer is the registry the metric was registered with")
}

// TestNewPrometheusHandlerWithoutListenAddress covers the branch where no listen
// address is given and the metrics handler is instead attached to Go's default
// HTTP mux. It is not parallel and installs its own mux for the duration of the
// test, because the underlying library registers "/metrics" on whatever mux is
// installed and a mux panics if the same path is registered twice.
func TestNewPrometheusHandlerWithoutListenAddress(t *testing.T) {
	mux := isolateDefaultServeMux(t)

	registry := prom.NewRegistry()

	handler, err := NewPrometheusHandler("", "defaultmux", registry)

	require.NoError(t, err)

	handler.Counter("things_done").Inc(1)

	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, httptest.NewRequestWithContext(context.Background(), http.MethodGet, metricsPath, http.NoBody))

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

// TestNewPrometheusHandlerErrorHandler covers the optional onError argument.
//
// The trigger for every case is the same deterministic failure: a dedicated
// registry already holds the metric tally derives from the prefix and metric
// name, so tally's own registration of it fails and the reporter reports that
// error through ConfigurationOptions.OnError. tally reports registration errors
// synchronously, while the metric is being allocated, so every assertion here is
// synchronous and nothing waits or sleeps.
//
// This test is not parallel: one subtest installs its own default serve mux.
func TestNewPrometheusHandlerErrorHandler(t *testing.T) {
	t.Run("rejects more than one error handler", func(t *testing.T) {
		// The reporter is never created, so tally never registers its handler
		// on the default mux. An isolated mux lets that be asserted.
		mux := isolateDefaultServeMux(t)

		tests := []struct {
			name     string
			handlers []func(error)
		}{
			{
				name:     "two handlers",
				handlers: []func(error){func(error) {}, func(error) {}},
			},
			{
				name:     "three handlers",
				handlers: []func(error){func(error) {}, func(error) {}, func(error) {}},
			},
			{
				name:     "a handler and an explicit nil",
				handlers: []func(error){func(error) {}, nil},
			},
		}

		for _, test := range tests {
			t.Run(test.name, func(t *testing.T) {
				// No listen address, so a reporter would register on the mux.
				handler, err := NewPrometheusHandler("", "multiplehandlers", prom.NewRegistry(), test.handlers...)

				require.Error(t, err)
				assert.ErrorContains(t, err, errorHandlerMessage)
				assert.Nil(t, handler, "no handler is returned alongside the error")

				rec := httptest.NewRecorder()
				req := httptest.NewRequestWithContext(context.Background(), http.MethodGet, metricsPath, http.NoBody)
				mux.ServeHTTP(rec, req)

				assert.Equal(t, http.StatusNotFound, rec.Code, "the reporter must not have been created or started")
			})
		}
	})

	t.Run("uses the supplied error handler", func(t *testing.T) {
		const prefix = "customhandler"

		registry, fqName := collidingRegistry(t, prefix)
		recorder := &errorRecorder{}

		handler, err := NewPrometheusHandler(freeListenAddress, prefix, registry, recorder.onError)

		require.NoError(t, err)
		require.NotNil(t, handler)

		// The supplied handler swallows the error, so allocating the metric
		// neither panics (tally's default) nor exits (our default).
		require.NotPanics(t, func() {
			handler.Counter(collidingMetric).Inc(1)
		})

		errs := recorder.errors()

		require.Len(t, errs, 1, "the supplied handler is called once, for the failed registration")
		assert.ErrorContains(t, errs[0], fqName)
		assert.ErrorContains(t, errs[0], "different label names or a different help string")
	})

	t.Run("passes an explicit nil handler through to tally", func(t *testing.T) {
		const prefix = "nilhandler"

		registry, fqName := collidingRegistry(t, prefix)

		handler, err := NewPrometheusHandler(freeListenAddress, prefix, registry, nil)

		require.NoError(t, err)
		require.NotNil(t, handler)

		// tally treats a nil ConfigurationOptions.OnError as "use my default",
		// and its default panics with the registration error. Recovering here
		// keeps that contained: reaching this assertion at all proves the nil
		// was passed through rather than replaced by our default handler, which
		// would have called log.Fatal and taken the test binary with it.
		recovered := func() (recovered any) {
			defer func() { recovered = recover() }()

			handler.Counter(collidingMetric).Inc(1)

			return nil
		}()

		require.NotNil(t, recovered, "tally panics when OnError is nil")

		panicErr, ok := recovered.(error)
		require.True(t, ok, "tally panics with the registration error, got %T", recovered)
		assert.ErrorContains(t, panicErr, fqName)
	})
}

// TestNewPrometheusHandlerDefaultErrorHandler documents that with no onError
// argument the default handler is still log.Fatal, for both of the failures
// tally reports: a listen address it cannot bind, which it reports
// asynchronously from the reporter's own goroutine, and a metric registration it
// cannot make, which it reports synchronously. Neither produces an error from
// NewPrometheusHandler, and log.Fatal terminates the process, so both have to be
// observed from a child process.
func TestNewPrometheusHandlerDefaultErrorHandler(t *testing.T) {
	if mode := os.Getenv(promFatalEnv); mode != "" {
		runDefaultErrorHandlerChild(mode)

		return
	}

	tests := []struct {
		name string
		mode string
		// expected fragments of the child's stderr
		expected []string
	}{
		{
			name:     "listen failure",
			mode:     fatalModeListen,
			expected: []string{"Error in Prometheus reporter", "not-a-valid-address"},
		},
		{
			name:     "registration failure",
			mode:     fatalModeRegister,
			expected: []string{"Error in Prometheus reporter", "different label names or a different help string"},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			// The child is killed by log.Fatal; the timeout only stops the test
			// hanging forever if that ever stops being true.
			ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
			defer cancel()

			//nolint:gosec // deliberately re-executes this test binary
			cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestNewPrometheusHandlerDefaultErrorHandler$")
			cmd.Env = append(os.Environ(), promFatalEnv+"="+test.mode)

			// The child may block on stdin until it is killed, so it never
			// trips the Go deadlock detector and the test needs no sleeps.
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

			for _, expected := range test.expected {
				assert.Contains(t, stderr.String(), expected)
			}
		})
	}
}

// runDefaultErrorHandlerChild is the child half of
// TestNewPrometheusHandlerDefaultErrorHandler. It always calls
// NewPrometheusHandler without an onError argument, so the default handler is
// the one under test, and it expects never to return.
func runDefaultErrorHandlerChild(mode string) {
	switch mode {
	case fatalModeListen:
		mustChildHandler("not-a-valid-address", "child", prom.NewRegistry())

		// Block until the reporter's goroutine terminates the process.
		_, _ = io.Copy(io.Discard, os.Stdin)
	case fatalModeRegister:
		const prefix = "child"

		registry := prom.NewRegistry()

		collector, _ := collidingCounter(prefix, collidingMetric)
		if err := registry.Register(collector); err != nil {
			fmt.Fprintln(os.Stderr, "unexpected error registering the colliding metric:", err)
			os.Exit(2)
		}

		handler := mustChildHandler(freeListenAddress, prefix, registry)

		// Allocating the metric fails to register it, which the default handler
		// turns into a fatal error.
		handler.Counter(collidingMetric).Inc(1)
	default:
		fmt.Fprintln(os.Stderr, "unknown child mode:", mode)
		os.Exit(2)
	}

	fmt.Fprintln(os.Stderr, "the process was not terminated")
	os.Exit(3)
}

// mustChildHandler builds a handler with the default error handler, exiting the
// child process if that does not work.
func mustChildHandler(listenAddress, prefix string, registry *prom.Registry) client.MetricsHandler {
	handler, err := NewPrometheusHandler(listenAddress, prefix, registry)
	if err != nil {
		fmt.Fprintln(os.Stderr, "unexpected error from NewPrometheusHandler:", err)
		os.Exit(2)
	}

	if handler == nil {
		fmt.Fprintln(os.Stderr, "unexpected nil handler")
		os.Exit(2)
	}

	return handler
}
