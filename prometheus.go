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
	"fmt"
	"io"
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
	closer io.Closer
}

// Close flushes and releases the underlying Tally scope, and reports whatever
// the scope reports. A handler with no scope to release, such as the zero
// value, closes without error.
func (h *PrometheusHandler) Close() error {
	if h.closer == nil {
		return nil
	}

	return h.closer.Close()
}

// NewPrometheusHandler creates a Prometheus-backed metrics handler for a
// Temporal client. The caller owns it and should call
// [PrometheusHandler.Close] when it is no longer needed, usually with defer.
//
// Metrics are served on listenAddress under /metrics, with prefix prepended to
// every metric name. An empty listenAddress attaches the metrics endpoint to
// Go's default HTTP mux instead of starting a server of its own. A nil registry
// uses the Prometheus default registry. Timers are reported as histograms.
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
// Reporter failures are not returned from here. A listen address that cannot be
// bound, for example, is reported asynchronously through onError once the
// reporter is running.
func NewPrometheusHandler(
	listenAddress,
	prefix string,
	registry *prom.Registry,
	onError ...func(error),
) (*PrometheusHandler, error) {
	c := prometheus.Configuration{
		ListenAddress: listenAddress,
		TimerType:     "histogram",
	}

	errorHandler := func(err error) {
		log.Fatal().Err(err).Msg("Error in Prometheus reporter")
	}
	if len(onError) > 1 {
		return nil, fmt.Errorf("only a single error handler may be supplied")
	}
	if len(onError) > 0 {
		errorHandler = onError[0]
	}

	reporter, err := c.NewReporter(
		prometheus.ConfigurationOptions{
			Registry: registry,
			OnError:  errorHandler,
		},
	)
	if err != nil {
		return nil, fmt.Errorf("error creating reporter: %w", err)
	}

	scopeOpts := tally.ScopeOptions{
		CachedReporter:  reporter,
		Separator:       prometheus.DefaultSeparator,
		SanitizeOptions: &sdktally.PrometheusSanitizeOptions,
		Prefix:          prefix,
	}
	scope, closer := tally.NewRootScope(scopeOpts, time.Second)
	scope = sdktally.NewPrometheusNamingScope(scope)

	log.Info().Str("address", listenAddress).Msg("Starting Prometheus service")
	return &PrometheusHandler{
		MetricsHandler: sdktally.NewMetricsHandler(scope),
		closer:         closer,
	}, nil
}
