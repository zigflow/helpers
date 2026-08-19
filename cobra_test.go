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
	"testing"

	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/client"
)

// Values supplied on the command line by the flag parsing tests.
const (
	flagHostPort   = "flag.example.com:7233"
	flagNamespace  = "flag-namespace"
	flagServerName = "flag.sni.example.com"
	// flagKey is a fake Temporal API key.
	flagKey = "flag-api-key"
)

// resetViper isolates a test from Viper's global state, both before and after it
// runs. Tests using this must not call t.Parallel.
func resetViper(t *testing.T) {
	t.Helper()

	viper.Reset()
	t.Cleanup(viper.Reset)
}

// newTestCommand returns a command with the Temporal flags registered against a
// zero-valued TemporalOpts.
func newTestCommand(t *testing.T) (*cobra.Command, *TemporalOpts) {
	t.Helper()

	cmd := &cobra.Command{Use: "test"}
	opts := NewCobraOpts(cmd, &TemporalOpts{})

	return cmd, opts
}

// lookupFlag returns a registered flag, failing the test if it is missing.
func lookupFlag(t *testing.T, cmd *cobra.Command, name string) *pflag.Flag {
	t.Helper()

	flag := cmd.Flags().Lookup(name)
	require.NotNil(t, flag, "flag %q should be registered", name)

	return flag
}

func TestNewCobraOptsRegistersFlags(t *testing.T) {
	resetViper(t)

	cmd, _ := newTestCommand(t)

	tests := []struct {
		flag      string
		shorthand string
		defValue  string
	}{
		{flag: "health-listen-address", defValue: "0.0.0.0:3000"},
		{flag: "metrics-listen-address", defValue: "0.0.0.0:9090"},
		{flag: "metrics-prefix", defValue: ""},
		{flag: "temporal-address", shorthand: "H", defValue: client.DefaultHostPort},
		{flag: "temporal-api-key", defValue: ""},
		{flag: "tls-client-cert-path", defValue: ""},
		{flag: "tls-client-key-path", defValue: ""},
		{flag: "temporal-namespace", shorthand: "n", defValue: client.DefaultNamespace},
		{flag: "temporal-server-name", defValue: ""},
		{flag: "temporal-tls", defValue: "false"},
	}

	for _, test := range tests {
		t.Run(test.flag, func(t *testing.T) {
			flag := lookupFlag(t, cmd, test.flag)

			assert.Equal(t, test.shorthand, flag.Shorthand)
			assert.Equal(t, test.defValue, flag.DefValue)
			assert.NotEmpty(t, flag.Usage, "flag should be documented")
		})
	}

	assert.Equal(t, len(tests), countFlags(cmd), "no unexpected flags should be registered")
}

// countFlags returns the number of flags registered on the command.
func countFlags(cmd *cobra.Command) int {
	count := 0
	cmd.Flags().VisitAll(func(*pflag.Flag) { count++ })

	return count
}

func TestNewCobraOptsDefaults(t *testing.T) {
	resetViper(t)

	_, opts := newTestCommand(t)

	assert.Equal(t, &TemporalOpts{
		Address:              client.DefaultHostPort,
		HealthListenAddress:  "0.0.0.0:3000",
		MetricsListenAddress: "0.0.0.0:9090",
		Namespace:            client.DefaultNamespace,
	}, opts)
}

func TestNewCobraOptsReturnsTheSuppliedStruct(t *testing.T) {
	resetViper(t)

	cmd := &cobra.Command{Use: "test"}
	in := &TemporalOpts{}

	out := NewCobraOpts(cmd, in)

	assert.Same(t, in, out)
}

func TestNewCobraOptsUsesViperValues(t *testing.T) {
	resetViper(t)

	viper.Set("health_listen_address", "127.0.0.1:1234")
	viper.Set("metrics_listen_address", "127.0.0.1:5678")
	viper.Set("metrics_prefix", "my_prefix")
	viper.Set("temporal_address", testHostPort)
	viper.Set("temporal_api_key", testKey)
	viper.Set("temporal_tls_client_cert_path", "/certs/client.pem")
	viper.Set("temporal_tls_client_key_path", "/certs/client.key")
	viper.Set("temporal_namespace", testNamespace)
	viper.Set("temporal_server_name", testServerName)
	viper.Set("temporal_tls", true)

	cmd, opts := newTestCommand(t)

	assert.Equal(t, &TemporalOpts{
		Address:              testHostPort,
		APIKey:               testKey,
		HealthListenAddress:  "127.0.0.1:1234",
		MetricsListenAddress: "127.0.0.1:5678",
		MetricsPrefix:        "my_prefix",
		MTLSCertPath:         "/certs/client.pem",
		MTLSKeyPath:          "/certs/client.key",
		Namespace:            testNamespace,
		ServerName:           testServerName,
		TLSEnabled:           true,
	}, opts)

	// Viper values become the flag defaults too, so `--help` reflects them.
	assert.Equal(t, testHostPort, lookupFlag(t, cmd, "temporal-address").DefValue)
	assert.Equal(t, "true", lookupFlag(t, cmd, "temporal-tls").DefValue)
}

func TestNewCobraOptsHidesTheAPIKeyDefault(t *testing.T) {
	tests := []struct {
		name             string
		apiKey           string
		expectedDefValue string
	}{
		{
			name:             "no api key configured",
			apiKey:           "",
			expectedDefValue: "",
		},
		{
			name:             "api key configured",
			apiKey:           "super-secret",
			expectedDefValue: "***",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			resetViper(t)

			if test.apiKey != "" {
				viper.Set("temporal_api_key", test.apiKey)
			}

			cmd, opts := newTestCommand(t)

			flag := lookupFlag(t, cmd, "temporal-api-key")

			assert.Equal(t, test.expectedDefValue, flag.DefValue)
			assert.Equal(t, test.apiKey, opts.APIKey, "the value itself is still available to the program")

			if test.apiKey != "" {
				assert.NotContains(t, cmd.UsageString(), test.apiKey, "the api key should never be printed")
			}
		})
	}
}

func TestNewCobraOptsFlagParsing(t *testing.T) {
	resetViper(t)

	cmd, opts := newTestCommand(t)

	require.NoError(t, cmd.ParseFlags([]string{
		"--health-listen-address", "127.0.0.1:1",
		"--metrics-listen-address", "127.0.0.1:2",
		"--metrics-prefix", "prefix",
		"-H", flagHostPort,
		"--temporal-api-key", flagKey,
		"--tls-client-cert-path", "/flag/client.pem",
		"--tls-client-key-path", "/flag/client.key",
		"-n", flagNamespace,
		"--temporal-server-name", flagServerName,
		"--temporal-tls",
	}))

	assert.Equal(t, &TemporalOpts{
		Address:              flagHostPort,
		APIKey:               flagKey,
		HealthListenAddress:  "127.0.0.1:1",
		MetricsListenAddress: "127.0.0.1:2",
		MetricsPrefix:        "prefix",
		MTLSCertPath:         "/flag/client.pem",
		MTLSKeyPath:          "/flag/client.key",
		Namespace:            flagNamespace,
		ServerName:           flagServerName,
		TLSEnabled:           true,
	}, opts)
}

func TestParseCobraOpts(t *testing.T) {
	t.Parallel()

	kp := newCertKeyPair(t, "cobra")

	t.Run("returns the base options plus any overrides", func(t *testing.T) {
		t.Parallel()

		assert.Len(t, ParseCobraOpts(&TemporalOpts{}), 4)
		assert.Len(t, ParseCobraOpts(&TemporalOpts{}, WithNoOp(), WithHostPort(testHostPort)), 6)
	})

	t.Run("empty options fall back to the Temporal defaults", func(t *testing.T) {
		t.Parallel()

		o := mustApplyOptions(t, ParseCobraOpts(&TemporalOpts{})...)

		assert.Equal(t, client.DefaultHostPort, o.HostPort)
		assert.Equal(t, client.DefaultNamespace, o.Namespace)
		assert.Nil(t, o.ConnectionOptions.TLS)
		assert.Nil(t, o.Credentials)
	})

	t.Run("propagates the address, namespace, tls and server name", func(t *testing.T) {
		t.Parallel()

		o := mustApplyOptions(t, ParseCobraOpts(&TemporalOpts{
			Address:    testHostPort,
			Namespace:  testNamespace,
			ServerName: testServerName,
			TLSEnabled: true,
		})...)

		assert.Equal(t, testHostPort, o.HostPort)
		assert.Equal(t, testNamespace, o.Namespace)
		require.NotNil(t, o.ConnectionOptions.TLS)
		assert.Equal(t, testServerName, o.ConnectionOptions.TLS.ServerName)
	})

	t.Run("the server name is ignored when tls is disabled", func(t *testing.T) {
		t.Parallel()

		o := mustApplyOptions(t, ParseCobraOpts(&TemporalOpts{
			ServerName: testServerName,
			TLSEnabled: false,
		})...)

		assert.Nil(t, o.ConnectionOptions.TLS)
	})

	t.Run("propagates an api key", func(t *testing.T) {
		t.Parallel()

		o := mustApplyOptions(t, ParseCobraOpts(&TemporalOpts{APIKey: testKey})...)

		assertAPIKeyCredentials(t, o.Credentials)
	})

	t.Run("propagates an mtls certificate and key", func(t *testing.T) {
		t.Parallel()

		o := mustApplyOptions(t, ParseCobraOpts(&TemporalOpts{
			MTLSCertPath: kp.certPath,
			MTLSKeyPath:  kp.keyPath,
		})...)

		assert.Equal(t, client.NewMTLSCredentials(kp.cert), o.Credentials)
	})

	t.Run("an api key takes precedence over mtls", func(t *testing.T) {
		t.Parallel()

		o := mustApplyOptions(t, ParseCobraOpts(&TemporalOpts{
			APIKey:       testKey,
			MTLSCertPath: kp.certPath,
			MTLSKeyPath:  kp.keyPath,
		})...)

		assertAPIKeyCredentials(t, o.Credentials)
	})

	t.Run("propagates mtls errors", func(t *testing.T) {
		t.Parallel()

		_, err := applyOptions(t, ParseCobraOpts(&TemporalOpts{
			MTLSCertPath: "/does/not/exist.pem",
			MTLSKeyPath:  "/does/not/exist.key",
		})...)

		assert.ErrorContains(t, err, "error loading tls key pair")
	})

	t.Run("overrides are applied last so they take precedence", func(t *testing.T) {
		t.Parallel()

		o := mustApplyOptions(t, ParseCobraOpts(
			&TemporalOpts{
				Address:   testHostPort,
				Namespace: testNamespace,
			},
			WithHostPort("override.example.com:7233"),
			WithNamespace("override-namespace"),
		)...)

		assert.Equal(t, "override.example.com:7233", o.HostPort)
		assert.Equal(t, "override-namespace", o.Namespace)
	})

	t.Run("overrides are applied in the order they are given", func(t *testing.T) {
		t.Parallel()

		o := mustApplyOptions(t, ParseCobraOpts(
			&TemporalOpts{},
			WithHostPort("first.example.com:7233"),
			WithHostPort("second.example.com:7233"),
		)...)

		assert.Equal(t, "second.example.com:7233", o.HostPort)
	})

	t.Run("an override can add tls when it is not enabled by the opts", func(t *testing.T) {
		t.Parallel()

		o := mustApplyOptions(t, ParseCobraOpts(
			&TemporalOpts{},
			WithTLS(true, WithTLSServerName("override.example.com")),
		)...)

		require.NotNil(t, o.ConnectionOptions.TLS)
		assert.Equal(t, "override.example.com", o.ConnectionOptions.TLS.ServerName)
	})

	t.Run("propagates errors from overrides", func(t *testing.T) {
		t.Parallel()

		_, err := applyOptions(t, ParseCobraOpts(&TemporalOpts{}, func(*client.Options) error {
			return errBoom
		})...)

		assert.ErrorIs(t, err, errBoom)
	})
}

// TestParseCobraOptsFromCommand ties the two halves together: flags parsed by
// Cobra should end up in the resulting Temporal client options.
func TestParseCobraOptsFromCommand(t *testing.T) {
	resetViper(t)

	cmd, opts := newTestCommand(t)

	require.NoError(t, cmd.ParseFlags([]string{
		"-H", flagHostPort,
		"-n", flagNamespace,
		"--temporal-tls",
		"--temporal-server-name", flagServerName,
		"--temporal-api-key", flagKey,
	}))

	o := mustApplyOptions(t, ParseCobraOpts(opts)...)

	assert.Equal(t, flagHostPort, o.HostPort)
	assert.Equal(t, flagNamespace, o.Namespace)
	require.NotNil(t, o.ConnectionOptions.TLS)
	assert.Equal(t, flagServerName, o.ConnectionOptions.TLS.ServerName)
	assertAPIKeyCredentials(t, o.Credentials)
}
