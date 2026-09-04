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

// Package temporal provides reusable helpers for building applications with the
// Temporal Go SDK.
//
// The helpers cover the parts of a Temporal service that tend to be rewritten
// every time: client connection and TLS configuration, authentication, Cobra
// and Viper flag wiring, health and readiness endpoints, Prometheus metrics,
// Zerolog integration and saga compensation.
//
// It is a thin convenience layer, not a framework or an abstraction over
// Temporal. A connection is described by a list of [Option] values and produces
// an ordinary Temporal client, and the logging and metrics helpers remain directly
// compatible with the SDK's logger and metrics handler interfaces, so the SDK
// stays directly usable alongside anything here.
//
// Start at [NewConnection] for the connection options, [NewCobraOpts] for CLI
// integration, [NewHealthCheck] for health endpoints, [NewPrometheusHandler]
// for metrics and [Compensator] for saga compensation.
package temporal
