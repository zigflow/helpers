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
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/sdk/converter"
)

// Values the S3 factory is configured with.
const (
	testBucket = "my-bucket"
	testRegion = "eu-west-2"

	// Fake AWS credentials. Nothing signed with them ever reaches AWS: every
	// request a test makes is either answered by a local server or is sent to a
	// host that cannot be resolved.
	testAccessKeyID     = "test-access-key-id"
	testSecretAccessKey = "test-secret-access-key"
	testSessionToken    = "test-session-token"
)

// defaultS3DriverName is the name s3driver falls back to when Options.DriverName
// is empty, and the type it reports regardless of how it is named.
const defaultS3DriverName = "aws.s3driver"

// The .invalid TLD is reserved by RFC 6761, so a request addressed to this
// endpoint cannot leave the machine. The addressing tests read the URL the SDK
// built out of the resulting failure, which is the only way to observe how the
// bucket was addressed without a request succeeding.
const (
	unresolvableHost     = "s3.example.invalid:4566"
	unresolvableEndpoint = "http://" + unresolvableHost
)

// storeTimeout bounds a Store call so that a test asserting on a failed request
// cannot hang if the machine's DNS resolver is a black hole. The URL is still
// named in the error when the deadline is what stops the request.
const storeTimeout = 15 * time.Second

// isolateAWSEnv stops the AWS SDK picking up any real configuration belonging to
// the machine running the tests: no shared config or credentials files, no
// ambient credentials, region or endpoint, and no calls out to the EC2 instance
// metadata service. Tests using this must not call t.Parallel.
func isolateAWSEnv(t *testing.T) {
	t.Helper()

	for _, key := range []string{
		"AWS_PROFILE",
		"AWS_ACCESS_KEY_ID",
		"AWS_SECRET_ACCESS_KEY",
		"AWS_SESSION_TOKEN",
		"AWS_REGION",
		"AWS_DEFAULT_REGION",
		"AWS_ENDPOINT_URL",
		"AWS_ENDPOINT_URL_S3",
	} {
		t.Setenv(key, "")
	}

	// Paths that deliberately do not exist, so the shared config lookup finds
	// nothing rather than reading the developer's ~/.aws.
	dir := t.TempDir()
	t.Setenv("AWS_CONFIG_FILE", filepath.Join(dir, "config"))
	t.Setenv("AWS_SHARED_CREDENTIALS_FILE", filepath.Join(dir, "credentials"))
	t.Setenv("AWS_EC2_METADATA_DISABLED", "true")
}

// fakeS3 stands in for S3 on the loopback interface. It records every request it
// receives and answers with the least the driver needs to get through a store: a
// HEAD reports the object as absent so the driver follows it with a PUT, which is
// accepted.
type fakeS3 struct {
	*httptest.Server

	mu       sync.Mutex
	requests []*http.Request
}

func newFakeS3(t *testing.T) *fakeS3 {
	t.Helper()

	f := &fakeS3{}
	f.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		f.mu.Lock()
		f.requests = append(f.requests, r.Clone(context.Background()))
		f.mu.Unlock()

		if r.Method == http.MethodHead {
			w.WriteHeader(http.StatusNotFound)

			return
		}

		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(f.Close)

	return f
}

// received returns the requests handled so far, in the order they arrived.
func (f *fakeS3) received() []*http.Request {
	f.mu.Lock()
	defer f.mu.Unlock()

	return append([]*http.Request(nil), f.requests...)
}

// firstRequest returns the first request the server handled, failing the test if
// the driver never called it.
func (f *fakeS3) firstRequest(t *testing.T) *http.Request {
	t.Helper()

	requests := f.received()
	require.NotEmpty(t, requests, "expected the driver to send a request to the fake S3 server")

	return requests[0]
}

// callFactory invokes the ExternalConfigFactory returned by
// ExternalConfigS3Factory. The factory is lazy, so this call is what exercises
// every piece of validation and wiring under test.
func callFactory(t *testing.T, cfg *S3Config) ([]converter.StorageDriver, error) {
	t.Helper()

	return ExternalConfigS3Factory(t.Context(), cfg)()
}

// mustCallFactory invokes the factory and returns the single driver it builds.
func mustCallFactory(t *testing.T, cfg *S3Config) converter.StorageDriver {
	t.Helper()

	drivers, err := callFactory(t, cfg)
	require.NoError(t, err)
	require.Len(t, drivers, 1)
	require.NotNil(t, drivers[0])

	return drivers[0]
}

// storePayload has the driver store a payload of the given size. Storing is the
// only thing that makes the driver use the S3 client the factory built, so every
// assertion about that client is made through this call.
func storePayload(t *testing.T, driver converter.StorageDriver, size int) error {
	t.Helper()

	ctx, cancel := context.WithTimeout(t.Context(), storeTimeout)
	defer cancel()

	_, err := driver.Store(
		converter.StorageDriverStoreContext{Context: ctx},
		[]*commonpb.Payload{{Data: make([]byte, size)}},
	)

	return err
}

// credentialsConfig returns a config pointed at the given endpoint with static
// credentials, which is what the tests that need a request to be signed use.
func credentialsConfig(endpoint string) *S3Config {
	return &S3Config{
		Bucket:          testBucket,
		Region:          testRegion,
		Endpoint:        endpoint,
		AccessKeyID:     testAccessKeyID,
		SecretAccessKey: testSecretAccessKey,
		UsePathStyle:    true,
	}
}

func TestExternalConfigS3FactoryNilConfig(t *testing.T) {
	isolateAWSEnv(t)

	factory := ExternalConfigS3Factory(t.Context(), nil)
	require.NotNil(t, factory, "a nil config must not stop the factory being returned, it is only read when called")

	drivers, err := factory()

	assert.Nil(t, drivers)
	assert.EqualError(t, err, "s3factory: config missing as second argument")
}

func TestExternalConfigS3FactoryCredentialValidation(t *testing.T) {
	const wantErr = "s3factory: SessionToken requires AccessKeyID and SecretAccessKey to also be set"

	tests := []struct {
		name            string
		accessKeyID     string
		secretAccessKey string
		sessionToken    string
		expected        string
	}{
		{
			name:         "a session token on its own",
			sessionToken: testSessionToken,
			expected:     wantErr,
		},
		{
			name:            "a session token without an access key id",
			secretAccessKey: testSecretAccessKey,
			sessionToken:    testSessionToken,
			expected:        wantErr,
		},
		{
			name:         "a session token without a secret access key",
			accessKeyID:  testAccessKeyID,
			sessionToken: testSessionToken,
			expected:     wantErr,
		},
		{
			name:            "complete static credentials",
			accessKeyID:     testAccessKeyID,
			secretAccessKey: testSecretAccessKey,
		},
		{
			name:            "complete static credentials with a session token",
			accessKeyID:     testAccessKeyID,
			secretAccessKey: testSecretAccessKey,
			sessionToken:    testSessionToken,
		},
		{
			name: "no credentials at all, so the sdk resolves them itself",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			isolateAWSEnv(t)

			drivers, err := callFactory(t, &S3Config{
				Bucket:          testBucket,
				Region:          testRegion,
				AccessKeyID:     test.accessKeyID,
				SecretAccessKey: test.secretAccessKey,
				SessionToken:    test.sessionToken,
			})

			if test.expected != "" {
				assert.Nil(t, drivers)
				assert.EqualError(t, err, test.expected)

				return
			}

			require.NoError(t, err)
			assert.Len(t, drivers, 1)
		})
	}
}

// TestExternalConfigS3FactoryStaticCredentials proves the static credentials are
// not merely accepted but are the ones the request is signed with, which an
// error-free construction on its own would not show.
func TestExternalConfigS3FactoryStaticCredentials(t *testing.T) {
	tests := []struct {
		name         string
		sessionToken string
	}{
		{
			name: "without a session token",
		},
		{
			name:         "with a session token",
			sessionToken: testSessionToken,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			isolateAWSEnv(t)

			server := newFakeS3(t)

			cfg := credentialsConfig(server.URL)
			cfg.SessionToken = test.sessionToken

			require.NoError(t, storePayload(t, mustCallFactory(t, cfg), 128))

			req := server.firstRequest(t)

			assert.Contains(t, req.Header.Get("Authorization"), "Credential="+testAccessKeyID+"/")
			assert.Equal(t, test.sessionToken, req.Header.Get("X-Amz-Security-Token"))
		})
	}
}

func TestExternalConfigS3FactoryDriverName(t *testing.T) {
	tests := []struct {
		name       string
		driverName string
		expected   string
	}{
		{
			name:       "an empty driver name falls back to the s3driver default",
			driverName: "",
			expected:   defaultS3DriverName,
		},
		{
			name:       "a supplied driver name is registered as given",
			driverName: "my-custom-driver",
			expected:   "my-custom-driver",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			isolateAWSEnv(t)

			driver := mustCallFactory(t, &S3Config{
				Bucket:     testBucket,
				Region:     testRegion,
				DriverName: test.driverName,
			})

			assert.Equal(t, test.expected, driver.Name())
			assert.Equal(t, defaultS3DriverName, driver.Type(), "naming a driver must not change the type it reports")
		})
	}
}

func TestExternalConfigS3FactoryMaxPayloadSize(t *testing.T) {
	// A limit small enough that the payloads below sit either side of it. The
	// driver checks the size before it touches S3, so the tests that expect a
	// rejection need no server.
	const maxPayloadSize = 1024

	t.Run("the configured maximum is passed through to the driver", func(t *testing.T) {
		isolateAWSEnv(t)

		driver := mustCallFactory(t, &S3Config{
			Bucket:         testBucket,
			Region:         testRegion,
			Endpoint:       unresolvableEndpoint,
			MaxPayloadSize: maxPayloadSize,
		})

		err := storePayload(t, driver, maxPayloadSize*2)

		assert.ErrorContains(t, err, fmt.Sprintf("exceeds maximum %d", maxPayloadSize))
	})

	t.Run("a zero maximum falls back to the s3driver default", func(t *testing.T) {
		isolateAWSEnv(t)

		server := newFakeS3(t)

		cfg := credentialsConfig(server.URL)
		cfg.MaxPayloadSize = 0

		// A payload well over the limit used above is stored without complaint,
		// so zero cannot have been forwarded as the limit. The exact 50 MiB
		// default is not probed because doing so would need a payload that size.
		assert.NoError(t, storePayload(t, mustCallFactory(t, cfg), maxPayloadSize*64))
		assert.NotEmpty(t, server.received())
	})

	t.Run("an invalid maximum is reported", func(t *testing.T) {
		isolateAWSEnv(t)

		drivers, err := callFactory(t, &S3Config{
			Bucket:         testBucket,
			Region:         testRegion,
			MaxPayloadSize: -1,
		})

		assert.Nil(t, drivers)
		assert.ErrorContains(t, err, "MaxPayloadSize must be positive")
	})
}

func TestExternalConfigS3FactoryEndpoint(t *testing.T) {
	t.Run("a supplied endpoint is where the requests go", func(t *testing.T) {
		isolateAWSEnv(t)

		server := newFakeS3(t)

		require.NoError(t, storePayload(t, mustCallFactory(t, credentialsConfig(server.URL)), 128))

		req := server.firstRequest(t)

		assert.Equal(t, http.MethodHead, req.Method)
		assert.Contains(t, req.URL.Path, testBucket)
	})

	// An empty endpoint has to leave whatever the SDK resolved in place. That
	// cannot be shown by letting the request run to real S3, so the endpoint is
	// supplied through the environment instead: it still arrives at the fake
	// server, which it could not do if the factory had overwritten it.
	t.Run("an empty endpoint leaves the resolved endpoint alone", func(t *testing.T) {
		isolateAWSEnv(t)

		server := newFakeS3(t)
		t.Setenv("AWS_ENDPOINT_URL_S3", server.URL)

		cfg := credentialsConfig("")

		require.NoError(t, storePayload(t, mustCallFactory(t, cfg), 128))
		assert.NotEmpty(t, server.received())
	})
}

// TestExternalConfigS3FactoryUsePathStyle asserts on how the bucket was
// addressed, which is the only observable effect of the flag. The endpoint is
// unresolvable, so the URL is read back out of the failure rather than from a
// request that arrived.
func TestExternalConfigS3FactoryUsePathStyle(t *testing.T) {
	tests := []struct {
		name         string
		usePathStyle bool
		expected     string
		unexpected   string
	}{
		{
			name:         "path style puts the bucket in the path",
			usePathStyle: true,
			expected:     unresolvableEndpoint + "/" + testBucket + "/",
			unexpected:   testBucket + "." + unresolvableHost,
		},
		{
			name:         "virtual host style puts the bucket in the host",
			usePathStyle: false,
			expected:     "http://" + testBucket + "." + unresolvableHost + "/",
			unexpected:   unresolvableEndpoint + "/" + testBucket + "/",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			isolateAWSEnv(t)

			cfg := credentialsConfig(unresolvableEndpoint)
			cfg.UsePathStyle = test.usePathStyle

			err := storePayload(t, mustCallFactory(t, cfg), 128)

			require.Error(t, err, "a request to an unresolvable endpoint must fail")
			assert.ErrorContains(t, err, test.expected)
			assert.NotContains(t, err.Error(), test.unexpected)
		})
	}
}

// TestExternalConfigS3FactoryRegion reads the region back out of the credential
// scope the request was signed with, so a region that was accepted but never
// applied would not pass.
func TestExternalConfigS3FactoryRegion(t *testing.T) {
	tests := []struct {
		name   string
		region string
	}{
		{
			name:   "the configured region",
			region: testRegion,
		},
		{
			name:   "a different region, to show it is not fixed",
			region: "us-east-1",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			isolateAWSEnv(t)

			server := newFakeS3(t)

			cfg := credentialsConfig(server.URL)
			cfg.Region = test.region

			require.NoError(t, storePayload(t, mustCallFactory(t, cfg), 128))

			auth := server.firstRequest(t).Header.Get("Authorization")

			assert.Contains(t, auth, "/"+test.region+"/s3/aws4_request")
		})
	}
}
