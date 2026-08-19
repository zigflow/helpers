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
	"encoding/pem"
	"math/big"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

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
func applyOptions(t *testing.T, options ...Options) (*client.Options, error) {
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
func mustApplyOptions(t *testing.T, options ...Options) *client.Options {
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
