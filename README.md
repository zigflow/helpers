# Helpers

A collection of Temporal helpers

A thin convenience layer over the [Temporal Go SDK](https://github.com/temporalio/sdk-go),
holding the pieces that tend to be rewritten in every service.

<!-- toc -->

* [Overview](#overview)
* [Installation](#installation)
* [Connection](#connection)
  * [TLS](#tls)
* [Authentication](#authentication)
* [External storage](#external-storage)
* [Environment configuration](#environment-configuration)
* [Cobra/Viper integration](#cobraviper-integration)
* [Health checks](#health-checks)
* [Prometheus](#prometheus)
  * [Lifecycle-aware usage](#lifecycle-aware-usage)
  * [Convenience usage](#convenience-usage)
  * [Error handling](#error-handling)
* [Logging](#logging)
* [Saga compensation](#saga-compensation)
* [Go compatibility](#go-compatibility)
* [Contributing](#contributing)
  * [Open in a container](#open-in-a-container)
  * [Commit style](#commit-style)

<!-- Regenerate with "pre-commit run -a markdown-toc" -->

<!-- tocstop -->

## Overview

The package provides helpers that are commonly reused across Temporal Go
applications:

* client connection configuration
* authentication and TLS
* external storage for large payloads
* Cobra/Viper CLI integration
* health and readiness endpoints
* Prometheus metrics
* Zerolog integration
* saga compensation

These helpers build on the Temporal Go SDK rather than replacing it. A
connection is described by a list of `Option` values and produces an ordinary
`client.Client`, and the logging and metrics helpers remain directly compatible
with the SDK's logger and metrics handler interfaces. The SDK stays directly usable
alongside anything here, and nothing is hidden behind a new abstraction.

Full API documentation is on
[pkg.go.dev](https://pkg.go.dev/github.com/zigflow/helpers).

## Installation

```bash
go get github.com/zigflow/helpers
```

The package is named `temporal`, so it is usually imported as:

```go
import temporal "github.com/zigflow/helpers"
```

## Connection

`NewConnection` dials Temporal using only the options supplied. Anything an
option does not set keeps the SDK default.

```go
c, err := temporal.NewConnection(
    temporal.WithHostPort("temporal.example.com:7233"),
    temporal.WithNamespace("my-namespace"),
    temporal.WithTLS(true,
        temporal.WithTLSServerName("temporal.example.com"),
    ),
)
if err != nil {
    return fmt.Errorf("error connecting to temporal: %w", err)
}
defer c.Close()
```

Options are applied in the order they are given, so a later option overwrites
an earlier one that sets the same field. An empty host and port falls back to
`client.DefaultHostPort`, and an empty namespace to `client.DefaultNamespace`.

### TLS

* `WithTLS(true, ...)` enables TLS and builds the connection's `tls.Config`
  from the `TLSOption` values given to it.
* `WithTLS(false)` is a no-op. It does not clear an existing TLS
  configuration, so a disabled flag cannot accidentally undo TLS configured
  elsewhere.
* `WithTLS` and `WithConnectionOptions` compose in either order without
  removing each other's settings. `WithConnectionOptions` replaces the whole
  of the connection options, but preserves TLS configured by `WithTLS` when
  its own `TLS` field is nil.

`WithTLSServerName` overrides the server name (SNI) used to validate the
server certificate. It is needed when the endpoint address does not match the
certificate hostname, for example behind AWS PrivateLink.

## Authentication

* `WithAPICredentials(apiKey)` authenticates with a Temporal API key. An empty
  key is a no-op.
* `WithMTLS(certPath, certKey)` authenticates with an mTLS client certificate,
  loading the key pair from disk when the option is applied. A pair that
  cannot be loaded is reported as an error from `NewConnection`.
* `WithAuthDetection(apiKey, certPath, certKey)` chooses between them.

`WithAuthDetection` uses the following precedence, and only ever applies one
method:

1. an API key, when `apiKey` is supplied
2. mTLS, when both the certificate and the key are supplied
3. otherwise no authentication option is applied

## External storage

Payloads too large to send to the Temporal server inline can be offloaded to
external storage, leaving only a reference in the workflow history.
`ExternalConfigS3Factory` builds an S3-backed storage driver, and
`WithExternalStorageFactory` attaches it to a connection:

```go
c, err := temporal.NewConnection(
    temporal.WithExternalStorageFactory(temporal.ExternalConfig{
        PayloadSizeThreshold: 1024 * 1024,
        Factory: temporal.ExternalConfigS3Factory(ctx, &temporal.S3Config{
            Bucket: "my-payload-bucket",
            Region: "eu-west-2",
        }),
    }),
)
```

The factory runs when the option is applied rather than when it is built, so
an AWS configuration that cannot be loaded is reported as an error from
`NewConnection`. An `ExternalConfig` without a `Factory` is an error too.

`Bucket` and `Region` are the only `S3Config` fields most deployments need.
The rest are optional, and only matter for credentials the AWS SDK cannot
resolve on its own, for S3-compatible storage, or to override a driver
default:

* `AccessKeyID` and `SecretAccessKey` set static credentials. Leave both empty
  to use the AWS default credential chain, which covers the environment,
  shared configuration files, and instance or workload identity.
* `SessionToken` accompanies temporary credentials. It may only be set
  alongside both `AccessKeyID` and `SecretAccessKey`; on its own, or with only
  one of them, it is reported as an error.
* `Endpoint` points at an S3-compatible service such as MinIO or LocalStack.
  An empty endpoint uses whichever AWS endpoint the SDK resolves for the
  region.
* `UsePathStyle` addresses the bucket in the request path rather than in the
  hostname, which S3-compatible services usually require.
* `DriverName` names the driver, defaulting to `aws.s3driver`. Every driver
  attached to a client needs a unique name, so this is only needed when more
  than one is registered.
* `MaxPayloadSize` is the largest payload the driver accepts, defaulting to
  50 MiB. Anything above it is reported as an error rather than stored.

`ExternalConfig` carries the settings that apply whatever the backend:

* `PayloadSizeThreshold` is the serialized payload size, in bytes, at which a
  payload is offloaded instead of being sent inline. Zero uses the SDK default
  of 256 KiB.
* `StorageDriverSelector` routes each payload to a particular driver, or
  leaves it inline. When it is nil, the first driver stores every payload over
  the threshold.

This is experimental, following the status of the underlying SDK support.

## Environment configuration

`NewConnectionWithEnvvars` uses the
[Temporal SDK environment configuration](https://docs.temporal.io/develop/environment-configuration#sdk-usage-example-go)
as its starting point, then applies the supplied options on top, so an option
always wins over the environment.

```go
c, err := temporal.NewConnectionWithEnvvars(
    temporal.WithZerolog(&log.Logger),
)
```

This is experimental, following the status of the underlying SDK support.

## Cobra/Viper integration

`NewCobraOpts` registers the Temporal flags on a command and binds them to a
`TemporalOpts`, which `ParseCobraOpts` then converts into connection options.

Each flag takes its default from Viper, so values can equally come from a
config file or the environment. The API key's default is hidden from the help
output so it is never printed to the terminal.

```go
var opts struct {
    temporal *temporal.TemporalOpts
}

cmd := &cobra.Command{
    Use:   "run",
    Short: "Run a Temporal worker",
    RunE: func(cmd *cobra.Command, args []string) error {
        metrics, err := temporal.NewPrometheusHandler(
            opts.temporal.MetricsListenAddress,
            opts.temporal.MetricsPrefix,
            nil,
        )
        if err != nil {
            return fmt.Errorf("error creating prometheus handler: %w", err)
        }
        defer metrics.Close()

        c, err := temporal.NewConnection(
            append(
                temporal.ParseCobraOpts(opts.temporal),
                temporal.WithZerolog(&log.Logger),
                temporal.WithMetrics(metrics),
            )...,
        )
        if err != nil {
            return fmt.Errorf("error connecting to temporal: %w", err)
        }
        defer c.Close()

        w := worker.New(c, TaskQueue, worker.Options{})

        if err := temporal.NewHealthCheck(
            cmd.Context(),
            []string{TaskQueue},
            opts.temporal.HealthListenAddress,
            c,
        ); err != nil {
            return fmt.Errorf("error creating health check: %w", err)
        }

        if err := w.Run(worker.InterruptCh()); err != nil {
            return fmt.Errorf("worker stopped: %w", err)
        }

        return nil
    },
}

opts.temporal = temporal.NewCobraOpts(cmd, &temporal.TemporalOpts{})
```

`ParseCobraOpts` covers the client: host and port, namespace, TLS with its
server name, and whichever authentication `WithAuthDetection` selects. Any
extra options passed to it are appended, so they are applied last and win over
the derived ones. The health and metrics listen addresses are not included,
because they configure servers rather than the client, and are wired up
separately as above.

## Health checks

`NewHealthCheck` starts an HTTP health server for a worker and serves three
endpoints:

| Endpoint  | Checks                                                     |
| --------- | ---------------------------------------------------------- |
| `/livez`  | Temporal connectivity                                      |
| `/readyz` | Temporal connectivity and the configured task queues       |
| `/health` | alias of `/readyz`                                         |

Readiness describes both the workflow and the activity task queue for every
task queue it was given. A healthy check responds `200` and an unhealthy one
`503`, both with a JSON body describing what was checked. Each request gets
its own two second timeout.

The listener is created synchronously, so an address that cannot be bound is
returned as an error and nothing is started:

```go
if err := temporal.NewHealthCheck(ctx, []string{TaskQueue}, "0.0.0.0:3000", c); err != nil {
    return fmt.Errorf("error creating health check: %w", err)
}
```

The server itself then runs in the background and shuts down when its context
is cancelled. Errors raised by the running server after that point are logged
rather than returned, so they are not observable through the returned error.

## Prometheus

### Lifecycle-aware usage

This is the recommended approach. `NewPrometheusHandler` returns a
`PrometheusHandler` that owns the underlying Tally scope, which the caller
closes when it is done with it.

```go
metrics, err := temporal.NewPrometheusHandler(
    "0.0.0.0:9090",
    "my_app",
    nil,
)
if err != nil {
    return err
}
defer metrics.Close()

c, err := temporal.NewConnection(
    temporal.WithMetrics(metrics),
)
```

Metrics are served on the given address under `/metrics`, with the prefix
prepended to every metric name. An empty address attaches the metrics endpoint
to Go's default HTTP mux instead of starting a server of its own, and a nil
registry uses the Prometheus default registry.

### Convenience usage

`WithPrometheusMetrics` creates the handler and attaches it to the client in
one call:

```go
c, err := temporal.NewConnection(
    temporal.WithPrometheusMetrics("0.0.0.0:9090", "my_app", nil),
)
```

By design it does not expose the closer, so the metrics reporter cannot be
shut down by the caller. That makes it most appropriate when the reporter is
expected to live for the lifetime of the process. Use
`NewPrometheusHandler` with `WithMetrics` whenever lifecycle ownership
matters.

### Error handling

Both take the same optional `onError` argument, which decides how the reporter
reports its own failures:

* no handler supplied: failures are logged at fatal level, terminating the
  process
* one handler supplied: that handler is called with the error
* an explicit `nil`: `nil` is passed through to Tally, so Tally applies its own
  default behaviour
* more than one handler: an error is returned and no handler is created

Reporter failures are not returned by the constructor. A listen address that
cannot be bound, for example, is reported asynchronously through `onError`
once the reporter is running.

## Logging

`NewZerologHandler` adapts a Zerolog logger to the SDK's logger interface, and
`WithZerolog` applies one directly to a connection:

```go
c, err := temporal.NewConnection(
    temporal.WithZerolog(&log.Logger),
)
```

Levels and structured key/value pairs are passed through, and the level
configured on the Zerolog logger still applies.

## Saga compensation

`Compensator` is a LIFO stack of compensation functions. `Add` registers a
compensation after its forward step has succeeded, and `Compensate` runs the
registered functions in reverse order.

Pass `Compensate` the original workflow context. It derives a disconnected
context internally, so there is no need to call
`workflow.NewDisconnectedContext` first.

```go
func MyWorkflow(ctx workflow.Context) (err error) {
    var saga temporal.Compensator

    defer func() {
        if err == nil {
            return
        }

        saga.Compensate(ctx)
    }()

    if err = workflow.ExecuteActivity(ctx, CreateOrder).Get(ctx, nil); err != nil {
        return err
    }
    saga.Add(func(ctx workflow.Context) error {
        return workflow.ExecuteActivity(ctx, CancelOrder).Get(ctx, nil)
    })

    if err = workflow.ExecuteActivity(ctx, TakePayment).Get(ctx, nil); err != nil {
        return err
    }
    saga.Add(func(ctx workflow.Context) error {
        return workflow.ExecuteActivity(ctx, RefundPayment).Get(ctx, nil)
    })

    return nil
}
```

Every registered compensation is attempted even when one fails: the failure is
logged through the workflow logger and the next compensation still runs.
`Compensate` returns nothing, so the original workflow error is not replaced.

The disconnected context keeps the parent's configuration, such as activity
options, but not its cancellation, so compensations still run for a workflow
that is being cancelled. It is cancelled once `Compensate` returns, so a
compensation must complete its work before returning.

## Go compatibility

This module requires Go 1.26.0, as declared in [go.mod](./go.mod).

> The minimum supported Go version follows the minimum supported version of the
> Temporal Go SDK.

## Contributing

### Open in a container

* [Open in a container](https://code.visualstudio.com/docs/devcontainers/containers)

### Commit style

All commits must be done in the [Conventional Commit](https://www.conventionalcommits.org)
format.

```git
<type>[optional scope]: <description>

[optional body]

[optional footer(s)]
```
