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
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/log"
)

// errBoom is a sentinel used to prove errors are propagated unchanged.
var errBoom = errors.New("boom")

// testAuthority is a gRPC authority used to prove connection options propagate.
const testAuthority = "my-authority"

// recordingDataConverter wraps a data converter and counts ToPayload calls so
// tests can prove the supplied converter is the one actually used.
type recordingDataConverter struct {
	converter.DataConverter

	name           string
	toPayloadCalls *int
}

func newRecordingDataConverter(name string) recordingDataConverter {
	calls := 0

	return recordingDataConverter{
		DataConverter:  converter.GetDefaultDataConverter(),
		name:           name,
		toPayloadCalls: &calls,
	}
}

func (r recordingDataConverter) ToPayload(value any) (*commonpb.Payload, error) {
	*r.toPayloadCalls++

	return r.DataConverter.ToPayload(value)
}

// stubMetricsHandler is a do-nothing metrics handler that is identifiable by name.
type stubMetricsHandler struct {
	client.MetricsHandler

	name string
}

// stubLogger is a do-nothing Temporal logger that is identifiable by name.
type stubLogger struct {
	log.Logger

	name string
}

// stubStorageDriver is a do-nothing storage driver that is identifiable by name,
// so a test can prove which drivers reached the client options.
type stubStorageDriver struct {
	converter.StorageDriver

	name string
}

func (s stubStorageDriver) Name() string { return s.name }

// stubDriverSelector is a do-nothing storage driver selector that is
// identifiable by name.
type stubDriverSelector struct {
	converter.StorageDriverSelector

	name string
}

// stubFactory returns an ExternalConfigFactory that reports the given drivers
// and error, alongside a count of how many times it has been called.
func stubFactory(drivers []converter.StorageDriver, err error) (factory ExternalConfigFactory, calls *int) {
	count := 0

	return func() ([]converter.StorageDriver, error) {
		count++

		return drivers, err
	}, &count
}

func TestWithHostPort(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		hostPort string
		expected string
	}{
		{
			name:     "supplied host port",
			hostPort: testHostPort,
			expected: testHostPort,
		},
		{
			name:     "empty host port falls back to the Temporal default",
			hostPort: "",
			expected: client.DefaultHostPort,
		},
		{
			name:     "the host port is not validated",
			hostPort: "no-port-here",
			expected: "no-port-here",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			o := mustApplyOptions(t, WithHostPort(test.hostPort))

			assert.Equal(t, test.expected, o.HostPort)
		})
	}
}

func TestWithNamespace(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		namespace string
		expected  string
	}{
		{
			name:      "supplied namespace",
			namespace: testNamespace,
			expected:  testNamespace,
		},
		{
			name:      "empty namespace falls back to the Temporal default",
			namespace: "",
			expected:  client.DefaultNamespace,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			o := mustApplyOptions(t, WithNamespace(test.namespace))

			assert.Equal(t, test.expected, o.Namespace)
		})
	}
}

func TestWithAPICredentials(t *testing.T) {
	t.Parallel()

	t.Run("api key supplied", func(t *testing.T) {
		t.Parallel()

		o := mustApplyOptions(t, WithAPICredentials(testKey))

		assertAPIKeyCredentials(t, o.Credentials)
	})

	t.Run("empty api key leaves credentials unset", func(t *testing.T) {
		t.Parallel()

		o := mustApplyOptions(t, WithAPICredentials(""))

		assert.Nil(t, o.Credentials)
	})

	t.Run("empty api key does not clear existing credentials", func(t *testing.T) {
		t.Parallel()

		o := mustApplyOptions(t, WithAPICredentials(testKey), WithAPICredentials(""))

		assertAPIKeyCredentials(t, o.Credentials)
	})
}

func TestWithAuthDetection(t *testing.T) {
	t.Parallel()

	kp := newCertKeyPair(t, "auth-detection")

	t.Run("api key takes precedence over mtls", func(t *testing.T) {
		t.Parallel()

		o := mustApplyOptions(t, WithAuthDetection(testKey, kp.certPath, kp.keyPath))

		assertAPIKeyCredentials(t, o.Credentials)
	})

	t.Run("mtls when certificate and key are supplied", func(t *testing.T) {
		t.Parallel()

		o := mustApplyOptions(t, WithAuthDetection("", kp.certPath, kp.keyPath))

		assert.Equal(t, client.NewMTLSCredentials(kp.cert), o.Credentials)
	})

	t.Run("mtls errors are propagated", func(t *testing.T) {
		t.Parallel()

		_, err := applyOptions(t, WithAuthDetection("", "/does/not/exist.pem", "/does/not/exist.key"))

		assert.ErrorContains(t, err, "error loading tls key pair")
	})

	incomplete := []struct {
		name     string
		apiKey   string
		certPath string
		certKey  string
	}{
		{name: "no credentials at all"},
		{name: "certificate without key", certPath: kp.certPath},
		{name: "key without certificate", certKey: kp.keyPath},
	}

	for _, test := range incomplete {
		t.Run("no-op when "+test.name, func(t *testing.T) {
			t.Parallel()

			o, err := applyOptions(t, WithAuthDetection(test.apiKey, test.certPath, test.certKey))

			assert.NoError(t, err)
			assert.Nil(t, o.Credentials)
		})
	}
}

func TestWithConnectionOptions(t *testing.T) {
	t.Parallel()

	connectionOpts := &client.ConnectionOptions{
		Authority:      testAuthority,
		MaxPayloadSize: 1234,
	}

	o := mustApplyOptions(t, WithConnectionOptions(connectionOpts))

	assert.Equal(t, *connectionOpts, o.ConnectionOptions)
}

func TestWithCredentials(t *testing.T) {
	t.Parallel()

	// mTLS credentials are used here because API key credentials are backed by a
	// func type, which cannot be compared for equality.
	kp := newCertKeyPair(t, "credentials")
	credentials := client.NewMTLSCredentials(kp.cert)

	o := mustApplyOptions(t, WithCredentials(credentials))

	assert.Equal(t, credentials, o.Credentials)
}

func TestWithDataConverter(t *testing.T) {
	t.Parallel()

	cvt := newRecordingDataConverter("data")

	o := mustApplyOptions(t, WithDataConverter(cvt))

	assert.Equal(t, cvt, o.DataConverter)
	assert.Nil(t, o.FailureConverter, "the failure converter should be left alone")
}

func TestWithFailureConverter(t *testing.T) {
	t.Parallel()

	t.Run("uses the supplied data converter and encodes common attributes", func(t *testing.T) {
		t.Parallel()

		cvt := newRecordingDataConverter("failure")

		o := mustApplyOptions(t, WithFailureConverter(cvt))

		require.NotNil(t, o.FailureConverter)
		assert.Nil(t, o.DataConverter, "the data converter should be left alone")

		failure := o.FailureConverter.ErrorToFailure(errors.New("something went wrong"))

		require.NotNil(t, failure)
		assert.NotNil(t, failure.GetEncodedAttributes(), "common attributes should be encoded")
		assert.NotEqual(t, "something went wrong", failure.GetMessage(), "the message should be encoded away")
		assert.Positive(t, *cvt.toPayloadCalls, "the supplied data converter should encode the attributes")
	})

	t.Run("round trips an error through the encoded attributes", func(t *testing.T) {
		t.Parallel()

		cvt := newRecordingDataConverter("round-trip")

		o := mustApplyOptions(t, WithFailureConverter(cvt))

		failure := o.FailureConverter.ErrorToFailure(errors.New("something went wrong"))
		err := o.FailureConverter.FailureToError(failure)

		require.Error(t, err)
		assert.ErrorContains(t, err, "something went wrong")
	})

	t.Run("nil errors convert to nil failures", func(t *testing.T) {
		t.Parallel()

		o := mustApplyOptions(t, WithFailureConverter(newRecordingDataConverter("nil")))

		assert.Nil(t, o.FailureConverter.ErrorToFailure(nil))
	})
}

func TestWithDataAndFailureConverter(t *testing.T) {
	t.Parallel()

	cvt := newRecordingDataConverter("both")

	o := mustApplyOptions(t, WithDataAndFailureConverter(cvt))

	assert.Equal(t, cvt, o.DataConverter)
	require.NotNil(t, o.FailureConverter)

	failure := o.FailureConverter.ErrorToFailure(errors.New("something went wrong"))

	require.NotNil(t, failure)
	assert.NotNil(t, failure.GetEncodedAttributes())
}

func TestWithExternalStorage(t *testing.T) {
	t.Parallel()

	storage := converter.ExternalStorage{
		PayloadSizeThreshold: 4096,
	}

	o := mustApplyOptions(t, WithExternalStorage(storage))

	assert.Equal(t, storage, o.ExternalStorage)
}

func TestWithExternalStorageFactory(t *testing.T) {
	t.Parallel()

	// A slice the tests can recognise again, so that a driver list built from
	// anything other than the factory's return value would be spotted.
	drivers := []converter.StorageDriver{
		stubStorageDriver{name: "first"},
		stubStorageDriver{name: "second"},
	}

	t.Run("the factory result and the config are passed to the client options", func(t *testing.T) {
		t.Parallel()

		factory, calls := stubFactory(drivers, nil)
		selector := stubDriverSelector{name: "selector"}

		o := mustApplyOptions(t, WithExternalStorageFactory(ExternalConfig{
			Factory:               factory,
			PayloadSizeThreshold:  4096,
			StorageDriverSelector: selector,
		}))

		assert.Equal(t, drivers, o.ExternalStorage.Drivers)
		assert.Equal(t, selector, o.ExternalStorage.DriverSelector)
		assert.Equal(t, 4096, o.ExternalStorage.PayloadSizeThreshold)
		assert.Equal(t, 1, *calls, "the factory should be invoked exactly once")
	})

	t.Run("the factory is only invoked once the option is applied", func(t *testing.T) {
		t.Parallel()

		factory, calls := stubFactory(drivers, nil)

		option := WithExternalStorageFactory(ExternalConfig{Factory: factory})

		require.Zero(t, *calls, "building the option should not invoke the factory")

		_, err := applyOptions(t, option)

		require.NoError(t, err)
		assert.Equal(t, 1, *calls)
	})

	t.Run("a config without a factory is an error", func(t *testing.T) {
		t.Parallel()

		o, err := applyOptions(t, WithExternalStorageFactory(ExternalConfig{
			PayloadSizeThreshold: 4096,
		}))

		assert.EqualError(t, err, "external storage factory must have a factory defined")
		assert.Equal(t, client.Options{}, *o, "a rejected config should leave the client options alone")
	})

	t.Run("factory errors are wrapped", func(t *testing.T) {
		t.Parallel()

		factory, _ := stubFactory(drivers, errBoom)

		o, err := applyOptions(t, WithExternalStorageFactory(ExternalConfig{Factory: factory}))

		assert.ErrorIs(t, err, errBoom, "the cause should survive the wrapping")
		assert.ErrorContains(t, err, "error invoking external storage factory")
		assert.Equal(t, client.Options{}, *o, "a failed factory should leave the client options alone")
	})
}

func TestWithLogger(t *testing.T) {
	t.Parallel()

	logger := stubLogger{name: "stub"}

	o := mustApplyOptions(t, WithLogger(logger))

	assert.Equal(t, logger, o.Logger)
}

func TestWithMetrics(t *testing.T) {
	t.Parallel()

	metrics := stubMetricsHandler{name: "stub"}

	o := mustApplyOptions(t, WithMetrics(metrics))

	assert.Equal(t, metrics, o.MetricsHandler)
}

func TestWithNoOp(t *testing.T) {
	t.Parallel()

	o, err := applyOptions(t, WithNoOp())

	assert.NoError(t, err)
	assert.Equal(t, &client.Options{}, o, "WithNoOp should not change anything")
}

func TestWithMTLS(t *testing.T) {
	t.Parallel()

	kp := newCertKeyPair(t, "mtls")
	other := newCertKeyPair(t, "other")

	t.Run("valid certificate and key pair", func(t *testing.T) {
		t.Parallel()

		o := mustApplyOptions(t, WithMTLS(kp.certPath, kp.keyPath))

		assert.Equal(t, client.NewMTLSCredentials(kp.cert), o.Credentials)
	})

	tests := []struct {
		name     string
		certPath string
		keyPath  string
	}{
		{
			name:     "missing certificate",
			certPath: "/does/not/exist.pem",
			keyPath:  kp.keyPath,
		},
		{
			name:     "missing key",
			certPath: kp.certPath,
			keyPath:  "/does/not/exist.key",
		},
		{
			name:     "certificate is not pem encoded",
			certPath: writeFile(t, "garbage.pem", "not a certificate"),
			keyPath:  kp.keyPath,
		},
		{
			name:     "key does not match the certificate",
			certPath: kp.certPath,
			keyPath:  other.keyPath,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			o, err := applyOptions(t, WithMTLS(test.certPath, test.keyPath))

			assert.ErrorContains(t, err, "error loading tls key pair")
			assert.Nil(t, o.Credentials)
		})
	}
}

func TestWithTLSServerName(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		serverName string
		expected   string
	}{
		{
			name:       "supplied server name",
			serverName: "temporal.example.com",
			expected:   "temporal.example.com",
		},
		{
			name:       "empty server name is left unset",
			serverName: "",
			expected:   "",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			cfg := new(tls.Config)

			assert.NoError(t, WithTLSServerName(test.serverName)(cfg))
			assert.Equal(t, test.expected, cfg.ServerName)
		})
	}
}

func TestWithTLS(t *testing.T) {
	t.Parallel()

	t.Run("disabled does not configure tls", func(t *testing.T) {
		t.Parallel()

		o := mustApplyOptions(t, WithTLS(false))

		assert.Nil(t, o.ConnectionOptions.TLS)
	})

	t.Run("disabled does not run the nested tls options", func(t *testing.T) {
		t.Parallel()

		called := false
		o, err := applyOptions(t, WithTLS(false, func(*tls.Config) error {
			called = true

			return errBoom
		}))

		assert.NoError(t, err)
		assert.False(t, called, "nested options should not run when tls is disabled")
		assert.Nil(t, o.ConnectionOptions.TLS)
	})

	t.Run("enabled with no options creates an empty tls config", func(t *testing.T) {
		t.Parallel()

		o := mustApplyOptions(t, WithTLS(true))

		require.NotNil(t, o.ConnectionOptions.TLS)
		assert.Equal(t, new(tls.Config), o.ConnectionOptions.TLS)
	})

	t.Run("enabled applies the nested tls options", func(t *testing.T) {
		t.Parallel()

		o := mustApplyOptions(t, WithTLS(
			true,
			WithTLSServerName("temporal.example.com"),
			func(c *tls.Config) error {
				c.InsecureSkipVerify = true

				return nil
			},
		))

		require.NotNil(t, o.ConnectionOptions.TLS)
		assert.Equal(t, "temporal.example.com", o.ConnectionOptions.TLS.ServerName)
		assert.True(t, o.ConnectionOptions.TLS.InsecureSkipVerify)
	})

	t.Run("wraps errors from the nested tls options", func(t *testing.T) {
		t.Parallel()

		o, err := applyOptions(t, WithTLS(true, func(*tls.Config) error {
			return errBoom
		}))

		assert.ErrorIs(t, err, errBoom)
		assert.ErrorContains(t, err, "error configuring tls options")
		assert.Nil(t, o.ConnectionOptions.TLS, "tls should not be applied when an option fails")
	})

	t.Run("stops at the first failing tls option", func(t *testing.T) {
		t.Parallel()

		called := false
		_, err := applyOptions(t, WithTLS(
			true,
			func(*tls.Config) error { return errBoom },
			func(*tls.Config) error {
				called = true

				return nil
			},
		))

		assert.ErrorIs(t, err, errBoom)
		assert.False(t, called, "options after a failure should not run")
	})
}

// TestOptionOrdering defines the desired option-composition behaviour:
//
//   - later options win when two options deliberately target the same setting
//   - WithTLS only modifies ConnectionOptions.TLS, so unrelated connection
//     options such as Authority survive it
//   - WithConnectionOptions does not remove an existing TLS configuration when
//     the supplied ConnectionOptions has none
//
// WithTLS and WithConnectionOptions compose; neither overwrites the other.
func TestOptionOrdering(t *testing.T) {
	t.Parallel()

	t.Run("later host port wins", func(t *testing.T) {
		t.Parallel()

		o := mustApplyOptions(t, WithHostPort("first:7233"), WithHostPort("second:7233"))

		assert.Equal(t, "second:7233", o.HostPort)
	})

	t.Run("later namespace wins", func(t *testing.T) {
		t.Parallel()

		o := mustApplyOptions(t, WithNamespace("first"), WithNamespace(""))

		assert.Equal(t, client.DefaultNamespace, o.Namespace, "an empty namespace still overwrites")
	})

	t.Run("later credentials win", func(t *testing.T) {
		t.Parallel()

		kp := newCertKeyPair(t, "ordering")

		o := mustApplyOptions(t, WithAPICredentials(testKey), WithMTLS(kp.certPath, kp.keyPath))

		assert.Equal(t, client.NewMTLSCredentials(kp.cert), o.Credentials)
	})

	t.Run("with tls preserves existing connection options", func(t *testing.T) {
		t.Parallel()

		o := mustApplyOptions(
			t,
			WithConnectionOptions(&client.ConnectionOptions{
				Authority: testAuthority,
			}),
			WithTLS(true, WithTLSServerName("temporal.example.com")),
		)

		assert.Equal(t, testAuthority, o.ConnectionOptions.Authority)
		require.NotNil(t, o.ConnectionOptions.TLS)
		assert.Equal(t, "temporal.example.com", o.ConnectionOptions.TLS.ServerName)
	})

	t.Run("with connection options preserves existing tls", func(t *testing.T) {
		t.Parallel()

		o := mustApplyOptions(
			t,
			WithTLS(true, WithTLSServerName("temporal.example.com")),
			WithConnectionOptions(&client.ConnectionOptions{
				Authority: testAuthority,
			}),
		)

		assert.Equal(t, testAuthority, o.ConnectionOptions.Authority)
		require.NotNil(t, o.ConnectionOptions.TLS)
		assert.Equal(t, "temporal.example.com", o.ConnectionOptions.TLS.ServerName)
	})

	t.Run("later tls wins without clobbering other connection options", func(t *testing.T) {
		t.Parallel()

		o := mustApplyOptions(
			t,
			WithConnectionOptions(&client.ConnectionOptions{
				Authority: testAuthority,
				TLS: &tls.Config{
					ServerName: "first.example.com",
				},
			}),
			WithTLS(true, WithTLSServerName("second.example.com")),
		)

		assert.Equal(t, testAuthority, o.ConnectionOptions.Authority)
		require.NotNil(t, o.ConnectionOptions.TLS)
		assert.Equal(t, "second.example.com", o.ConnectionOptions.TLS.ServerName)
	})
}

func TestNewConnection(t *testing.T) {
	t.Parallel()

	t.Run("starts from empty client options", func(t *testing.T) {
		t.Parallel()

		var captured client.Options

		// The failing option prevents any attempt to dial a real server.
		c, err := NewConnection(func(o *client.Options) error {
			captured = *o

			return errBoom
		})

		assert.ErrorIs(t, err, errBoom)
		assert.Nil(t, c)
		assert.Equal(t, client.Options{}, captured)
	})

	t.Run("applies options in order and stops at the first error", func(t *testing.T) {
		t.Parallel()

		var captured client.Options

		called := false

		c, err := NewConnection(
			WithHostPort(testHostPort),
			WithNamespace(testNamespace),
			func(o *client.Options) error {
				captured = *o

				return errBoom
			},
			func(*client.Options) error {
				called = true

				return nil
			},
		)

		assert.ErrorIs(t, err, errBoom)
		assert.Nil(t, c)
		assert.False(t, called, "options after a failure should not run")
		assert.Equal(t, testHostPort, captured.HostPort)
		assert.Equal(t, testNamespace, captured.Namespace)
	})
}

func TestNewConnectionWithEnvvars(t *testing.T) {
	// Not parallel: these subtests set environment variables.
	t.Run("uses the environment config as the starting point", func(t *testing.T) {
		t.Setenv("TEMPORAL_CONFIG_FILE", "/does/not/exist.toml")
		t.Setenv("TEMPORAL_ADDRESS", "env.example.com:7233")
		t.Setenv("TEMPORAL_NAMESPACE", "env-namespace")

		var captured client.Options

		// The failing option prevents any attempt to dial a real server.
		c, err := NewConnectionWithEnvvars(func(o *client.Options) error {
			captured = *o

			return errBoom
		})

		assert.ErrorIs(t, err, errBoom)
		assert.Nil(t, c)
		assert.Equal(t, "env.example.com:7233", captured.HostPort)
		assert.Equal(t, "env-namespace", captured.Namespace)
	})

	t.Run("supplied options override the environment config", func(t *testing.T) {
		t.Setenv("TEMPORAL_CONFIG_FILE", "/does/not/exist.toml")
		t.Setenv("TEMPORAL_ADDRESS", "env.example.com:7233")

		var captured client.Options

		_, err := NewConnectionWithEnvvars(
			WithHostPort("override.example.com:7233"),
			func(o *client.Options) error {
				captured = *o

				return errBoom
			},
		)

		assert.ErrorIs(t, err, errBoom)
		assert.Equal(t, "override.example.com:7233", captured.HostPort)
	})

	t.Run("wraps environment config errors", func(t *testing.T) {
		t.Setenv("TEMPORAL_CONFIG_FILE", writeFile(t, "invalid.toml", "this is not = valid = toml"))

		called := false

		c, err := NewConnectionWithEnvvars(func(*client.Options) error {
			called = true

			return nil
		})

		assert.ErrorContains(t, err, "error loading environment config")
		assert.Nil(t, c)
		assert.False(t, called, "options should not be applied when the environment config fails to load")
	})
}
