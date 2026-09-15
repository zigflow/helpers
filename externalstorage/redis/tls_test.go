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
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"io/fs"
	"math/big"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// testServerName is a non-empty server name, used to prove the configured value
// reaches the TLS config rather than being derived from the address.
const testServerName = "redis.zigflow.test"

// errLoadClientCert is the message BuildTLSConfig uses when go-redis cannot be
// given a usable client certificate.
const errLoadClientCert = "load Redis TLS client certificate"

// certAuthority is a throwaway CA generated per test. Its PEM is what a caller
// would put in a CA file, and its key signs the client certificates.
type certAuthority struct {
	cert *x509.Certificate
	key  *ecdsa.PrivateKey
	pem  []byte
}

// keyPair is an issued certificate and its private key, already written to
// disk, because BuildTLSConfig only accepts file paths.
type keyPair struct {
	certFile string
	keyFile  string
	leaf     *x509.Certificate
}

// newCA issues a self-signed CA. Certificates are generated rather than
// committed as fixtures so the tests carry no expiry date of their own.
func newCA(t *testing.T) *certAuthority {
	t.Helper()

	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)

	template := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "zigflow-test-ca"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
		BasicConstraintsValid: true,
		IsCA:                  true,
	}

	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	require.NoError(t, err)

	cert, err := x509.ParseCertificate(der)
	require.NoError(t, err)

	return &certAuthority{
		cert: cert,
		key:  key,
		pem:  encodePEM(t, "CERTIFICATE", der),
	}
}

// issue signs a client certificate with ca and writes the certificate and key
// to their own files.
func (c *certAuthority) issue(t *testing.T, commonName string) keyPair {
	t.Helper()

	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)

	template := &x509.Certificate{
		SerialNumber: big.NewInt(2),
		Subject:      pkix.Name{CommonName: commonName},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
	}

	der, err := x509.CreateCertificate(rand.Reader, template, c.cert, &key.PublicKey, c.key)
	require.NoError(t, err)

	leaf, err := x509.ParseCertificate(der)
	require.NoError(t, err)

	keyDER, err := x509.MarshalECPrivateKey(key)
	require.NoError(t, err)

	return keyPair{
		certFile: writeFile(t, commonName+".crt", encodePEM(t, "CERTIFICATE", der)),
		keyFile:  writeFile(t, commonName+".key", encodePEM(t, "EC PRIVATE KEY", keyDER)),
		leaf:     leaf,
	}
}

func encodePEM(t *testing.T, blockType string, der []byte) []byte {
	t.Helper()

	return pem.EncodeToMemory(&pem.Block{Type: blockType, Bytes: der})
}

// writeFile writes contents to a uniquely named file in the test's temporary
// directory and returns its path.
func writeFile(t *testing.T, name string, contents []byte) string {
	t.Helper()

	path := filepath.Join(t.TempDir(), name)
	require.NoError(t, os.WriteFile(path, contents, 0o600))

	return path
}

// missingPath is a path inside the test's temporary directory that is
// deliberately never created.
func missingPath(t *testing.T, name string) string {
	t.Helper()

	return filepath.Join(t.TempDir(), name)
}

// ---- Disabled ----

func TestBuildTLSConfigDisabled(t *testing.T) {
	// A disabled config must return a nil *tls.Config, because go-redis treats
	// any non-nil config as a request to use TLS.
	cfg, err := BuildTLSConfig(false, "", "", "", "", false)
	require.NoError(t, err)
	assert.Nil(t, cfg)
}

func TestBuildTLSConfigDisabledIgnoresOtherArguments(t *testing.T) {
	// The enabled flag gates everything else, so unusable paths must not be
	// read, let alone reported, while TLS is switched off.
	cfg, err := BuildTLSConfig(
		false,
		missingPath(t, "ca.pem"),
		missingPath(t, "client.crt"),
		missingPath(t, "client.key"),
		testServerName,
		true,
	)
	require.NoError(t, err)
	assert.Nil(t, cfg)
}

// ---- Enabled without files ----

func TestBuildTLSConfigEnabledWithoutFiles(t *testing.T) {
	cfg, err := BuildTLSConfig(true, "", "", "", "", false)
	require.NoError(t, err)
	require.NotNil(t, cfg)

	// TLS 1.2 is the floor regardless of the caller's input, so an otherwise
	// empty configuration cannot negotiate a deprecated protocol version.
	assert.Equal(t, uint16(tls.VersionTLS12), cfg.MinVersion)
	// No CA file means the platform trust store is used, which is expressed as
	// a nil RootCAs rather than an empty pool that would trust nothing.
	assert.Nil(t, cfg.RootCAs)
	assert.Empty(t, cfg.Certificates)
	assert.Empty(t, cfg.ServerName)
	assert.False(t, cfg.InsecureSkipVerify)
}

func TestBuildTLSConfigServerName(t *testing.T) {
	cfg, err := BuildTLSConfig(true, "", "", "", testServerName, false)
	require.NoError(t, err)
	require.NotNil(t, cfg)

	assert.Equal(t, testServerName, cfg.ServerName)
}

func TestBuildTLSConfigInsecureSkipVerify(t *testing.T) {
	tests := []struct {
		name  string
		given bool
	}{
		{name: "verification stays enabled by default"},
		{name: "verification can be disabled deliberately", given: true},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			cfg, err := BuildTLSConfig(true, "", "", "", testServerName, test.given)
			require.NoError(t, err)
			require.NotNil(t, cfg)

			// Skipping verification is a security decision the caller owns, so
			// the flag must be carried through verbatim in both directions.
			assert.Equal(t, test.given, cfg.InsecureSkipVerify)
			// Disabling verification must not weaken anything else.
			assert.Equal(t, uint16(tls.VersionTLS12), cfg.MinVersion)
		})
	}
}

// ---- CA file ----

func TestBuildTLSConfigCAFileIsTrusted(t *testing.T) {
	ca := newCA(t)
	pair := ca.issue(t, "client")

	cfg, err := BuildTLSConfig(true, writeFile(t, "ca.pem", ca.pem), "", "", testServerName, false)
	require.NoError(t, err)
	require.NotNil(t, cfg)
	require.NotNil(t, cfg.RootCAs)

	// Prove the pool actually trusts the CA by verifying a certificate it
	// signed, rather than inspecting the pool's contents.
	chains, err := pair.leaf.Verify(x509.VerifyOptions{
		Roots:     cfg.RootCAs,
		KeyUsages: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
	})
	require.NoError(t, err)
	assert.NotEmpty(t, chains)
}

func TestBuildTLSConfigCAFileKeepsSystemRoots(t *testing.T) {
	systemPool, err := x509.SystemCertPool()
	if err != nil {
		t.Skipf("system certificate pool unavailable: %v", err)
	}

	ca := newCA(t)

	cfg, err := BuildTLSConfig(true, writeFile(t, "ca.pem", ca.pem), "", "", testServerName, false)
	require.NoError(t, err)
	require.NotNil(t, cfg)

	// A configured CA is additive. Replacing the system roots would break TLS
	// against a managed Redis service using a publicly trusted certificate.
	require.NotNil(t, systemPool)
	require.True(t, systemPool.AppendCertsFromPEM(ca.pem))
	assert.True(
		t,
		cfg.RootCAs.Equal(systemPool),
		"RootCAs must be the system pool plus the configured CA",
	)
}

func TestBuildTLSConfigCAFileErrors(t *testing.T) {
	tests := []struct {
		name     string
		caFile   func(t *testing.T) string
		wantErr  string
		wantPath bool
	}{
		{
			name:    "unreadable CA file",
			caFile:  func(t *testing.T) string { t.Helper(); return missingPath(t, "ca.pem") },
			wantErr: "read Redis TLS CA file",
		},
		{
			name: "CA file containing no PEM",
			caFile: func(t *testing.T) string {
				t.Helper()

				return writeFile(t, "ca.pem", []byte("not a certificate"))
			},
			wantErr:  "parse Redis TLS CA file",
			wantPath: true,
		},
		{
			name: "empty CA file",
			caFile: func(t *testing.T) string {
				t.Helper()

				return writeFile(t, "ca.pem", nil)
			},
			wantErr:  "parse Redis TLS CA file",
			wantPath: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			caFile := test.caFile(t)

			cfg, err := BuildTLSConfig(true, caFile, "", "", testServerName, false)
			require.Error(t, err)
			// A partially built config would silently fall back to the system
			// trust store, so nothing may be returned alongside the error.
			assert.Nil(t, cfg)
			assert.ErrorContains(t, err, test.wantErr)

			if test.wantPath {
				// The message names the offending file, so the cause is
				// actionable without reproducing the failure.
				assert.ErrorContains(t, err, caFile)
			}
		})
	}
}

func TestBuildTLSConfigCAFilePreservesReadError(t *testing.T) {
	cfg, err := BuildTLSConfig(true, missingPath(t, "ca.pem"), "", "", testServerName, false)
	require.Error(t, err)
	assert.Nil(t, cfg)
	// The underlying filesystem error is wrapped, so callers can tell a missing
	// file from a permission problem.
	assert.ErrorIs(t, err, fs.ErrNotExist)
}

// ---- Client certificate ----

func TestBuildTLSConfigClientCertificate(t *testing.T) {
	ca := newCA(t)
	pair := ca.issue(t, "client")

	cfg, err := BuildTLSConfig(true, "", pair.certFile, pair.keyFile, testServerName, false)
	require.NoError(t, err)
	require.NotNil(t, cfg)

	require.Len(t, cfg.Certificates, 1)
	assert.Equal(t, pair.leaf.Raw, cfg.Certificates[0].Certificate[0])
	assert.NotNil(t, cfg.Certificates[0].PrivateKey)
	// A client certificate says nothing about which roots to trust.
	assert.Nil(t, cfg.RootCAs)
}

func TestBuildTLSConfigCAAndClientCertificate(t *testing.T) {
	ca := newCA(t)
	pair := ca.issue(t, "client")

	cfg, err := BuildTLSConfig(
		true,
		writeFile(t, "ca.pem", ca.pem),
		pair.certFile,
		pair.keyFile,
		testServerName,
		false,
	)
	require.NoError(t, err)
	require.NotNil(t, cfg)

	// Mutual TLS needs both halves, so configuring one must not discard the
	// other.
	assert.NotNil(t, cfg.RootCAs)
	assert.Len(t, cfg.Certificates, 1)
	assert.Equal(t, testServerName, cfg.ServerName)
	assert.Equal(t, uint16(tls.VersionTLS12), cfg.MinVersion)
}

func TestBuildTLSConfigClientCertificateErrors(t *testing.T) {
	tests := []struct {
		name    string
		files   func(t *testing.T) (certFile, keyFile string)
		wantErr string
	}{
		{
			// Half a client certificate cannot authenticate anything. Ignoring
			// it would connect without the credential the caller asked for.
			name: "certificate without key",
			files: func(t *testing.T) (string, string) {
				t.Helper()

				return newCA(t).issue(t, "client").certFile, ""
			},
			wantErr: "redis TLS certificate and key must both be configured",
		},
		{
			name: "key without certificate",
			files: func(t *testing.T) (string, string) {
				t.Helper()

				return "", newCA(t).issue(t, "client").keyFile
			},
			wantErr: "redis TLS certificate and key must both be configured",
		},
		{
			name: "mismatched certificate and key",
			files: func(t *testing.T) (string, string) {
				t.Helper()

				ca := newCA(t)

				return ca.issue(t, "client").certFile, ca.issue(t, "other").keyFile
			},
			wantErr: errLoadClientCert,
		},
		{
			name: "unreadable key file",
			files: func(t *testing.T) (string, string) {
				t.Helper()

				return newCA(t).issue(t, "client").certFile, missingPath(t, "client.key")
			},
			wantErr: errLoadClientCert,
		},
		{
			name: "certificate file containing no PEM",
			files: func(t *testing.T) (string, string) {
				t.Helper()

				pair := newCA(t).issue(t, "client")

				return writeFile(t, "client.crt", []byte("not a certificate")), pair.keyFile
			},
			wantErr: errLoadClientCert,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			certFile, keyFile := test.files(t)

			cfg, err := BuildTLSConfig(true, "", certFile, keyFile, testServerName, false)
			require.Error(t, err)
			// Returning a config without the certificate would connect
			// anonymously against a server expecting mutual TLS.
			assert.Nil(t, cfg)
			assert.ErrorContains(t, err, test.wantErr)
		})
	}
}
