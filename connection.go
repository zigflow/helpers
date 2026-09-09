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
	"crypto/tls"
	"fmt"

	prom "github.com/prometheus/client_golang/prometheus"
	"github.com/rs/zerolog"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/contrib/envconfig"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/interceptor"
	"go.temporal.io/sdk/log"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
)

// Option configures a [client.Options] before the connection is dialled.
// Options are applied in the order they are given, so a later option
// overwrites an earlier one that sets the same field.
type Option func(*client.Options) error

// TLSOption configures the [tls.Config] built by [WithTLS]. TLS options are
// applied in order, and only when TLS is enabled.
type TLSOption func(*tls.Config) error

// Create a connection to Temporal
func newConnection(clientOptions *client.Options, options ...Option) (client.Client, error) {
	for _, o := range options {
		if err := o(clientOptions); err != nil {
			return nil, err
		}
	}
	return client.Dial(*clientOptions)
}

// NewConnectionWithEnvvars creates a Temporal connection using the Temporal
// SDK environment configuration as its starting point, then applies options on
// top, so an option always wins over the environment.
//
// This is experimental.
//
// @link https://docs.temporal.io/develop/environment-configuration#sdk-usage-example-go
func NewConnectionWithEnvvars(options ...Option) (client.Client, error) {
	clientOptions, err := envconfig.LoadDefaultClientOptions()
	if err != nil {
		return nil, fmt.Errorf("error loading environment config: %w", err)
	}

	return newConnection(&clientOptions, options...)
}

// NewConnection creates a Temporal connection from the supplied options only,
// starting from a zero-valued [client.Options]. Anything an option does not set
// keeps the SDK's own default.
func NewConnection(options ...Option) (client.Client, error) {
	clientOptions := &client.Options{}
	return newConnection(clientOptions, options...)
}

// WithAPICredentials authenticates with a Temporal API key. An empty apiKey is
// a no-op, so it leaves any credentials already configured in place.
func WithAPICredentials(apiKey string) Option {
	return func(o *client.Options) error {
		if apiKey != "" {
			return WithCredentials(client.NewAPIKeyStaticCredentials(apiKey))(o)
		}
		return nil
	}
}

// WithAuthDetection chooses an authentication method from whichever values are
// supplied, in precedence order:
//
//  1. an API key, when apiKey is not empty
//  2. mTLS, when both certPath and certKey are not empty
//  3. otherwise no authentication option is applied, via [WithNoOp]
//
// Only one method is ever used, and the choice is made when the option is
// built rather than when it is applied.
func WithAuthDetection(apiKey, certPath, certKey string) Option {
	if apiKey != "" {
		return WithAPICredentials(apiKey)
	}

	if certKey != "" && certPath != "" {
		return WithMTLS(certPath, certKey)
	}

	return WithNoOp()
}

// WithConnectionOptions replaces the whole of the client's connection options.
//
// TLS is the exception: when connection.TLS is nil, TLS configured by [WithTLS]
// is preserved, so the two options compose in either order without silently
// discarding each other's settings. Set connection.TLS to override that
// deliberately.
func WithConnectionOptions(connection *client.ConnectionOptions) Option {
	return func(o *client.Options) error {
		tlsConfig := o.ConnectionOptions.TLS

		o.ConnectionOptions = *connection

		// Avoid blatting away anything set by WithTLS
		if connection.TLS == nil {
			o.ConnectionOptions.TLS = tlsConfig
		}
		return nil
	}
}

// WithContextPropagators sets the context propagators used to carry values
// between the client, workflows and activities, for example a trace or tenant
// identifier held in the caller's [context.Context].
//
// The whole set is replaced rather than added to, so a later call overwrites an
// earlier one; pass every propagator in a single call. A nil or empty slice
// leaves no propagators configured.
//
// Propagators run in the order they are given, and the same set must be given
// to every client and worker that takes part, otherwise a value injected at one
// end is not extracted at the other.
func WithContextPropagators(propagators []workflow.ContextPropagator) Option {
	return func(o *client.Options) error {
		o.ContextPropagators = propagators
		return nil
	}
}

// WithCredentials sets the credentials used to authenticate with Temporal.
// [WithAPICredentials], [WithMTLS] and [WithAuthDetection] are usually more
// convenient.
func WithCredentials(credential client.Credentials) Option {
	return func(o *client.Options) error {
		o.Credentials = credential
		return nil
	}
}

// WithDataConverter sets the converter used for workflow and activity
// payloads. It does not affect failures; see [WithDataAndFailureConverter].
func WithDataConverter(cvt converter.DataConverter) Option {
	return func(o *client.Options) error {
		o.DataConverter = cvt
		return nil
	}
}

// WithDataAndFailureConverter applies cvt as both the data converter and, via
// [WithFailureConverter], the failure converter. This is normally what an
// encrypting or compressing converter wants, so that failure detail is covered
// as well as payloads.
func WithDataAndFailureConverter(cvt converter.DataConverter) Option {
	return func(o *client.Options) error {
		if err := WithDataConverter(cvt)(o); err != nil {
			return err
		}

		return WithFailureConverter(cvt)(o)
	}
}

// WithExternalStorage sets the external storage used for payloads too large to
// send to the Temporal server inline.
func WithExternalStorage(st converter.ExternalStorage) Option {
	return func(o *client.Options) error {
		o.ExternalStorage = st
		return nil
	}
}

// WithExternalStorageFactory sets the external storage from an [ExternalConfig]
// by invoking e.Factory and handing the drivers it returns to
// [WithExternalStorage], together with e.StorageDriverSelector and
// e.PayloadSizeThreshold.
//
// The factory runs when the option is applied rather than when it is built, so
// building an option never talks to the storage backend. An [ExternalConfig]
// without a Factory is an error, and so is a factory that fails; a failing
// factory's error is wrapped rather than returned as it is, so the cause is
// still reachable with [errors.Is] and [errors.As].
func WithExternalStorageFactory(e ExternalConfig) Option {
	return func(o *client.Options) error {
		if e.Factory == nil {
			return fmt.Errorf("external storage factory must have a factory defined")
		}

		drivers, err := e.Factory()
		if err != nil {
			return fmt.Errorf("error invoking external storage factory: %w", err)
		}

		return WithExternalStorage(converter.ExternalStorage{
			Drivers:              drivers,
			DriverSelector:       e.StorageDriverSelector,
			PayloadSizeThreshold: e.PayloadSizeThreshold,
		})(o)
	}
}

// WithFailureConverter sets a failure converter that encodes failures with cvt.
// Common failure attributes, such as the message and stack trace, are encoded
// too, so a converter that encrypts payloads also covers failure detail.
func WithFailureConverter(cvt converter.DataConverter) Option {
	return func(o *client.Options) error {
		o.FailureConverter = temporal.NewDefaultFailureConverter(
			temporal.DefaultFailureConverterOptions{
				DataConverter:          cvt,
				EncodeCommonAttributes: true,
			},
		)
		return nil
	}
}

// WithHostPort sets the address of the Temporal frontend. An empty hostPort
// falls back to the SDK default of [client.DefaultHostPort].
func WithHostPort(hostPort string) Option {
	return func(o *client.Options) error {
		if hostPort == "" {
			hostPort = client.DefaultHostPort
		}
		o.HostPort = hostPort
		return nil
	}
}

// WithInterceptors sets the interceptors applied to client calls, such as
// starting a workflow or sending a signal.
//
// Earlier interceptors wrap later ones, so the first one given is the
// outermost. The whole set is replaced rather than added to, so a later call
// overwrites an earlier one; pass every interceptor in a single call. A nil or
// empty slice leaves no interceptors configured.
//
// An interceptor that also implements [interceptor.WorkerInterceptor] is used
// for worker interception as well, wrapping any interceptor set in the worker's
// own options. The same interceptor should not be given in both places.
func WithInterceptors(interceptors []interceptor.ClientInterceptor) Option {
	return func(o *client.Options) error {
		o.Interceptors = interceptors
		return nil
	}
}

// WithLogger sets the logger used by the client, and by any worker created from
// it. Use [WithZerolog] to pass an existing Zerolog logger.
func WithLogger(logger log.Logger) Option {
	return func(o *client.Options) error {
		o.Logger = logger
		return nil
	}
}

// WithMetrics sets the client's metrics handler. Pass the handler returned by
// [NewPrometheusHandler] when the caller needs to close it; [WithPrometheusMetrics]
// is the shorter option when it does not.
func WithMetrics(metrics client.MetricsHandler) Option {
	return func(o *client.Options) error {
		o.MetricsHandler = metrics
		return nil
	}
}

// WithMTLS authenticates with an mTLS client certificate, loading the key pair
// from disk when the option is applied. A pair that cannot be loaded, or whose
// key does not match its certificate, is reported as an error from the
// connection constructor.
//
// The SDK applies the certificate to the connection's TLS configuration when
// the client is dialled, creating one if none is set, so TLS is in use without
// also calling [WithTLS]. Use [WithTLS] when the TLS configuration itself needs
// customising, for example with [WithTLSServerName].
func WithMTLS(certPath, certKey string) Option {
	return func(o *client.Options) error {
		// Use the crypto/tls package to create a cert object
		cert, err := tls.LoadX509KeyPair(certPath, certKey)
		if err != nil {
			return fmt.Errorf("error loading tls key pair: %w", err)
		}

		return WithCredentials(client.NewMTLSCredentials(cert))(o)
	}
}

// WithNamespace sets the Temporal namespace. An empty namespace falls back to
// the SDK default of [client.DefaultNamespace].
func WithNamespace(namespace string) Option {
	return func(o *client.Options) error {
		if namespace == "" {
			namespace = client.DefaultNamespace
		}
		o.Namespace = namespace
		return nil
	}
}

// WithNoOp does nothing. It is useful where an [Option] has to be returned but
// there is nothing to configure, as [WithAuthDetection] does when no
// credentials are supplied.
func WithNoOp() Option {
	return func(o *client.Options) error {
		return nil
	}
}

// WithPrometheusMetrics
//
// Convenience helper that creates a Prometheus metrics handler and attaches it
// to the client options in a single call.
//
// By design, this does not expose the closer, so the metrics handler cannot be
// shut down by the caller - that is the trade-off for the shorter call site. If
// you need lifecycle control, create the handler yourself with
// NewPrometheusHandler, defer its Close method, and pass it to WithMetrics:
//
//	metrics, err := temporal.NewPrometheusHandler(
//		opts.temporal.MetricsListenAddress,
//		opts.temporal.MetricsPrefix,
//		nil,
//	)
//	if err != nil {
//		return err
//	}
//	defer metrics.Close()
//
//	c, err := temporal.NewConnection(
//		temporal.WithMetrics(metrics),
//	)
func WithPrometheusMetrics(listenAddress, prefix string, registry *prom.Registry, onError ...func(error)) Option {
	return func(o *client.Options) error {
		h, err := NewPrometheusHandler(listenAddress, prefix, registry, onError...)
		if err != nil {
			return err
		}
		return WithMetrics(h)(o)
	}
}

// WithTLS enables TLS and builds the connection's [tls.Config] from tlsOpts.
//
// When enabled is false the option is a no-op: it does not clear TLS
// configuration set elsewhere, so a disabled flag cannot accidentally undo
// mTLS credentials or [WithConnectionOptions].
func WithTLS(enabled bool, tlsOpts ...TLSOption) Option {
	return func(o *client.Options) error {
		if !enabled {
			return nil
		}

		tlsConfig := new(tls.Config)

		for _, opt := range tlsOpts {
			if err := opt(tlsConfig); err != nil {
				return fmt.Errorf("error configuring tls options: %w", err)
			}
		}

		o.ConnectionOptions.TLS = tlsConfig
		return nil
	}
}

// WithTLSServerName overrides the TLS server name (SNI) used to validate the
// server certificate. It is needed when the endpoint address does not match the
// certificate hostname, for example behind AWS PrivateLink. An empty serverName
// is a no-op.
func WithTLSServerName(serverName string) TLSOption {
	return func(c *tls.Config) error {
		if serverName == "" {
			return nil
		}
		c.ServerName = serverName
		return nil
	}
}

// WithZerolog uses an existing Zerolog logger as the client logger. It is
// shorthand for [WithLogger] with [NewZerologHandler].
func WithZerolog(logger *zerolog.Logger) Option {
	return WithLogger(NewZerologHandler(logger))
}
