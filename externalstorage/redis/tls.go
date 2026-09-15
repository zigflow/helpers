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

package redis

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
)

func BuildTLSConfig(
	enabled bool,
	caFile string,
	certFile string,
	keyFile string,
	serverName string,
	insecureSkipVerify bool,
) (*tls.Config, error) {
	if !enabled {
		return nil, nil
	}

	cfg := &tls.Config{
		MinVersion:         tls.VersionTLS12,
		ServerName:         serverName,
		InsecureSkipVerify: insecureSkipVerify, //nolint:gosec
	}

	if caFile != "" {
		caPEM, err := os.ReadFile(caFile)
		if err != nil {
			return nil, fmt.Errorf("read Redis TLS CA file: %w", err)
		}

		caPool, err := x509.SystemCertPool()
		if err != nil {
			return nil, fmt.Errorf("load system certificate pool: %w", err)
		}
		if caPool == nil {
			caPool = x509.NewCertPool()
		}

		if ok := caPool.AppendCertsFromPEM(caPEM); !ok {
			return nil, fmt.Errorf("parse Redis TLS CA file %q", caFile)
		}

		cfg.RootCAs = caPool
	}

	if certFile != "" || keyFile != "" {
		if certFile == "" || keyFile == "" {
			return nil, fmt.Errorf(
				"redis TLS certificate and key must both be configured",
			)
		}

		cert, err := tls.LoadX509KeyPair(certFile, keyFile)
		if err != nil {
			return nil, fmt.Errorf("load Redis TLS client certificate: %w", err)
		}

		cfg.Certificates = []tls.Certificate{cert}
	}

	return cfg, nil
}
