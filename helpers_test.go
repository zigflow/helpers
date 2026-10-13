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
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"encoding/pem"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/client"
)

// Values shared by tests across the package.
const (
	testHostPort   = "temporal.example.com:7233"
	testNamespace  = "my-namespace"
	testServerName = "sni.example.com"
	// testKey is a fake Temporal API key.
	testKey = "my-api-key"
)

// applyOptions applies the options, in order, to a fresh client.Options in the
// same way newConnection does, stopping at the first error.
func applyOptions(t *testing.T, options ...Option) (*client.Options, error) {
	t.Helper()

	o := &client.Options{}
	for _, opt := range options {
		if err := opt(o); err != nil {
			return o, err
		}
	}

	return o, nil
}

// mustApplyOptions applies the options and fails the test if any of them error.
func mustApplyOptions(t *testing.T, options ...Option) *client.Options {
	t.Helper()

	o, err := applyOptions(t, options...)
	require.NoError(t, err)

	return o
}

// assertAPIKeyCredentials asserts that the given credentials were built by
// client.NewAPIKeyStaticCredentials. The concrete type is unexported by the SDK,
// so the assertion is made against a reference value built by the same
// constructor rather than against the type's name or internals.
func assertAPIKeyCredentials(t *testing.T, got client.Credentials) {
	t.Helper()

	if !assert.NotNil(t, got, "expected credentials to be set") {
		return
	}

	assert.Equal(
		t,
		reflect.TypeOf(client.NewAPIKeyStaticCredentials("reference")),
		reflect.TypeOf(got),
		"expected API key credentials",
	)
}

// certKeyPair is a PEM encoded certificate/key pair written to disk.
type certKeyPair struct {
	certPath string
	keyPath  string
	cert     tls.Certificate
}

// newCertKeyPair generates a self-signed certificate and writes the certificate
// and key to a temporary directory as PEM files.
func newCertKeyPair(t *testing.T, name string) certKeyPair {
	t.Helper()

	certPEM, keyPEM := generateCertKeyPEM(t, name)

	dir := t.TempDir()
	certPath := filepath.Join(dir, name+".pem")
	keyPath := filepath.Join(dir, name+".key")

	require.NoError(t, os.WriteFile(certPath, certPEM, 0o600))
	require.NoError(t, os.WriteFile(keyPath, keyPEM, 0o600))

	cert, err := tls.X509KeyPair(certPEM, keyPEM)
	require.NoError(t, err)

	return certKeyPair{certPath: certPath, keyPath: keyPath, cert: cert}
}

// generateCertKeyPEM builds a self-signed P-256 certificate and its private key
// in PEM form. Validity dates are fixed so no test depends on the clock.
func generateCertKeyPEM(t *testing.T, commonName string) (certPEM, keyPEM []byte) {
	t.Helper()

	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)

	template := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: commonName},
		NotBefore:    time.Date(2020, time.January, 1, 0, 0, 0, 0, time.UTC),
		NotAfter:     time.Date(2100, time.January, 1, 0, 0, 0, 0, time.UTC),
		KeyUsage:     x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		IsCA:         true,
	}

	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	require.NoError(t, err)

	keyDER, err := x509.MarshalECPrivateKey(key)
	require.NoError(t, err)

	certPEM = pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	keyPEM = pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER})

	return certPEM, keyPEM
}

// writeFile writes content to a file in a new temporary directory.
func writeFile(t *testing.T, name, content string) string {
	t.Helper()

	path := filepath.Join(t.TempDir(), name)
	require.NoError(t, os.WriteFile(path, []byte(content), 0o600))

	return path
}

// logWaitTimeout bounds how long a test waits for a log entry written from
// another goroutine. It only exists so a broken implementation fails rather than
// hangs.
const logWaitTimeout = 5 * time.Second

// logCapture records the JSON entries written by a zerolog logger. It is mutex
// guarded because servers log from their own goroutines.
type logCapture struct {
	mu      sync.Mutex
	entries []map[string]any
	// written is signalled after every entry, so waiters never poll.
	written chan struct{}
}

func (c *logCapture) Write(p []byte) (int, error) {
	entry := map[string]any{}
	if err := json.Unmarshal(p, &entry); err != nil {
		return 0, err
	}

	c.mu.Lock()
	c.entries = append(c.entries, entry)
	c.mu.Unlock()

	select {
	case c.written <- struct{}{}:
	default:
	}

	return len(p), nil
}

func (c *logCapture) find(message string) (map[string]any, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	for _, entry := range c.entries {
		if entry[zerolog.MessageFieldName] == message {
			return entry, true
		}
	}

	return nil, false
}

// waitForMessage returns the first entry with the given message, waiting a
// bounded time for it to be written.
func (c *logCapture) waitForMessage(t *testing.T, message string) map[string]any {
	t.Helper()

	timeout := time.NewTimer(logWaitTimeout)
	defer timeout.Stop()

	for {
		if entry, ok := c.find(message); ok {
			return entry
		}

		select {
		case <-c.written:
		case <-timeout.C:
			t.Fatalf("no %q log entry after %s", message, logWaitTimeout)
		}
	}
}

// captureLogs swaps the global zerolog logger for one that records its entries
// and restores the original when the test finishes. Loggers derived from the
// global one before the swap is undone keep writing to the capture, so entries
// logged from a goroutine are still recorded. Callers must not be parallel.
func captureLogs(t *testing.T) *logCapture {
	t.Helper()

	original := log.Logger
	t.Cleanup(func() { log.Logger = original })

	c := &logCapture{written: make(chan struct{}, 1)}
	log.Logger = zerolog.New(c)

	return c
}

// requireAssignedAddress asserts that a logged address is on the expected host
// with a real, OS-assigned port rather than 0, and returns it.
func requireAssignedAddress(t *testing.T, entry map[string]any, expectedHost string) string {
	t.Helper()

	address, ok := entry["address"].(string)
	require.True(t, ok, "the log entry has an address: %v", entry)

	host, port, err := net.SplitHostPort(address)
	require.NoError(t, err)
	assert.Equal(t, expectedHost, host)

	p, err := strconv.Atoi(port)
	require.NoError(t, err)
	require.NotZero(t, p, "the logged port is the assigned one, not 0")

	return address
}
