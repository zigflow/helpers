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
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"time"

	prom "github.com/prometheus/client_golang/prometheus"
	"github.com/rs/zerolog/log"
	"github.com/uber-go/tally/v4"
	"github.com/uber-go/tally/v4/prometheus"
	"go.temporal.io/sdk/client"
	sdktally "go.temporal.io/sdk/contrib/tally"
)

// PrometheusHandler is a Temporal metrics handler backed by a Prometheus
// reporter.
//
// It embeds the SDK's metrics handler interface, so it can be passed straight
// to [WithMetrics], and it owns the underlying Tally scope. The caller owns the
// handler, and is responsible for calling [PrometheusHandler.Close].
type PrometheusHandler struct {
	client.MetricsHandler
	closer   io.Closer
	server   *http.Server
	listener net.Listener
}

// Close stops the owned HTTP server and listener, then flushes and releases the
// underlying Tally scope. It returns any errors encountered during cleanup.
// The zero value closes without error. An endpoint registered on Go's default
// HTTP mux remains registered.
func (h *PrometheusHandler) Close() error {
	var errs []error

	if h.server != nil {
		if err := h.server.Close(); err != nil {
			errs = append(errs, err)
		}
	}

	// Also close the listener directly in case Close was called before
	// the Serve goroutine registered it with the HTTP server.
	if h.listener != nil {
		if err := h.listener.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
			errs = append(errs, err)
		}
	}

	if h.closer != nil {
		if err := h.closer.Close(); err != nil {
			errs = append(errs, err)
		}
	}

	return errors.Join(errs...)
}

// NewPrometheusHandler creates a Prometheus-backed metrics handler for a
// Temporal client. The caller owns it and should call
// [PrometheusHandler.Close] when it is no longer needed, usually with defer.
//
// Metrics are served on listenAddress under /metrics, with prefix prepended to
// every metric name. A port of 0 (for example "0.0.0.0:0") lets the OS pick a
// free port, which is logged. An empty listenAddress attaches the metrics
// endpoint to Go's default HTTP mux instead of starting a server of its own. A
// nil registry uses the Prometheus default registry. Timers are reported as
// histograms.
//
// The optional onError argument decides how the reporter reports its own
// failures:
//
//   - not supplied: failures are logged at fatal level, terminating the process
//   - one function: that function is called with the error
//   - explicitly nil: nil is passed through to Tally, so Tally's own default
//     behaviour applies
//   - more than one: an error is returned and no handler is created
//
// Listen failures are returned synchronously. Metric registration and unexpected
// HTTP serving failures are reported through onError.
func NewPrometheusHandler(
	listenAddress,
	prefix string,
	registry *prom.Registry,
	onError ...func(error),
) (*PrometheusHandler, error) {
	errorHandler := func(err error) {
		log.Fatal().Err(err).Msg("Error in Prometheus reporter")
	}
	if len(onError) > 1 {
		return nil, fmt.Errorf("only a single error handler may be supplied")
	}
	if len(onError) > 0 {
		errorHandler = onError[0]
	}

	reporterOpts := prometheus.Options{
		DefaultTimerType: prometheus.HistogramTimerType,
		OnRegisterError:  errorHandler,
	}
	if registry != nil {
		reporterOpts.Registerer = registry
		reporterOpts.Gatherer = registry
	}

	reporter := prometheus.NewReporter(reporterOpts)

	var listener net.Listener
	var server *http.Server

	if listenAddress == "" {
		http.Handle("/metrics", reporter.HTTPHandler())
	} else {
		var err error
		listener, err = (&net.ListenConfig{}).Listen(context.Background(), "tcp", listenAddress)
		if err != nil {
			return nil, fmt.Errorf("error listening for Prometheus: %w", err)
		}

		mux := http.NewServeMux()
		mux.Handle("/metrics", reporter.HTTPHandler())
		server = &http.Server{
			Handler:           mux,
			ReadHeaderTimeout: 5 * time.Second,
		}
	}

	scopeOpts := tally.ScopeOptions{
		CachedReporter:  reporter,
		Separator:       prometheus.DefaultSeparator,
		SanitizeOptions: &sdktally.PrometheusSanitizeOptions,
		Prefix:          prefix,
	}
	scope, closer := tally.NewRootScope(scopeOpts, time.Second)
	scope = sdktally.NewPrometheusNamingScope(scope)

	handler := &PrometheusHandler{
		MetricsHandler: sdktally.NewMetricsHandler(scope),
		closer:         closer,
		server:         server,
		listener:       listener,
	}

	if server != nil {
		log.Info().Str("address", listener.Addr().String()).Msg("Starting Prometheus service")

		go func() {
			if err := server.Serve(listener); err != nil && !errors.Is(err, http.ErrServerClosed) {
				if errorHandler == nil {
					panic(err)
				}
				errorHandler(err)
			}
		}()
	}

	return handler, nil
}
