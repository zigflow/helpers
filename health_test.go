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
	"encoding/json"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/client"
)

// Task queue names and the type names the healthcheck reports.
const (
	testTaskQueue    = "my-task-queue"
	nameWorkflowType = "workflow"
	nameActivityType = "activity"
	nameUnknownType  = "unknown"
)

// describeCall records a single DescribeTaskQueue invocation.
type describeCall struct {
	taskQueue string
	queueType enumspb.TaskQueueType
}

// fakeTemporalClient is a client.Client that only implements the two methods the
// healthcheck uses. Every other method is inherited from the embedded nil
// interface and will panic if the healthcheck ever starts calling it.
type fakeTemporalClient struct {
	client.Client

	mu sync.Mutex

	// checkHealthErr is returned by CheckHealth.
	checkHealthErr error
	// describeErrs maps a task queue and type to the error to return.
	describeErrs map[describeCall]error

	checkHealthCalls int
	describeCalls    []describeCall
}

func newFakeTemporalClient() *fakeTemporalClient {
	return &fakeTemporalClient{describeErrs: map[describeCall]error{}}
}

func (f *fakeTemporalClient) failTaskQueue(taskQueue string, queueType enumspb.TaskQueueType, err error) {
	f.describeErrs[describeCall{taskQueue: taskQueue, queueType: queueType}] = err
}

func (f *fakeTemporalClient) CheckHealth(
	context.Context,
	*client.CheckHealthRequest,
) (*client.CheckHealthResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.checkHealthCalls++

	if f.checkHealthErr != nil {
		return nil, f.checkHealthErr
	}

	return &client.CheckHealthResponse{}, nil
}

func (f *fakeTemporalClient) DescribeTaskQueue(
	_ context.Context,
	taskQueue string,
	queueType enumspb.TaskQueueType,
) (*workflowservice.DescribeTaskQueueResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	call := describeCall{taskQueue: taskQueue, queueType: queueType}
	f.describeCalls = append(f.describeCalls, call)

	if err, ok := f.describeErrs[call]; ok {
		return nil, err
	}

	return &workflowservice.DescribeTaskQueueResponse{}, nil
}

func (f *fakeTemporalClient) calls() []describeCall {
	f.mu.Lock()
	defer f.mu.Unlock()

	return append([]describeCall(nil), f.describeCalls...)
}

// serve runs a GET request through the healthcheck handler.
func serve(h *healthcheck, path string) *httptest.ResponseRecorder {
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequestWithContext(context.Background(), http.MethodGet, path, http.NoBody))

	return rec
}

// decodeBody decodes a JSON response body into the given target.
func decodeBody(t *testing.T, rec *httptest.ResponseRecorder, target any) {
	t.Helper()

	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), target))
}

func newTestHealthcheck(c client.Client, taskQueues ...string) *healthcheck {
	return &healthcheck{
		client:     c,
		taskQueues: taskQueues,
		timeout:    time.Second,
	}
}

func TestTaskQueueTypeName(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		input    enumspb.TaskQueueType
		expected string
	}{
		{
			name:     "workflow",
			input:    enumspb.TASK_QUEUE_TYPE_WORKFLOW,
			expected: nameWorkflowType,
		},
		{
			name:     "activity",
			input:    enumspb.TASK_QUEUE_TYPE_ACTIVITY,
			expected: nameActivityType,
		},
		{
			name:     "unspecified",
			input:    enumspb.TASK_QUEUE_TYPE_UNSPECIFIED,
			expected: nameUnknownType,
		},
		{
			name:     "nexus",
			input:    enumspb.TASK_QUEUE_TYPE_NEXUS,
			expected: nameUnknownType,
		},
		{
			name:     "out of range",
			input:    enumspb.TaskQueueType(9999),
			expected: nameUnknownType,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, test.expected, taskQueueTypeName(test.input))
		})
	}
}

func TestWriteJSON(t *testing.T) {
	t.Parallel()

	t.Run("writes the status, content type and encoded body", func(t *testing.T) {
		t.Parallel()

		rec := httptest.NewRecorder()

		writeJSON(rec, http.StatusTeapot, liveResponse{Healthy: true})

		assert.Equal(t, http.StatusTeapot, rec.Code)
		assert.Equal(t, "application/json", rec.Header().Get("Content-Type"))
		assert.JSONEq(t, `{"healthy":true}`, rec.Body.String())
	})

	t.Run("omits empty optional fields", func(t *testing.T) {
		t.Parallel()

		rec := httptest.NewRecorder()

		writeJSON(rec, http.StatusServiceUnavailable, readyResponse{Healthy: false})

		assert.JSONEq(t, `{"healthy":false,"temporalOk":false}`, rec.Body.String())
	})

	t.Run("includes the error when set", func(t *testing.T) {
		t.Parallel()

		rec := httptest.NewRecorder()

		writeJSON(rec, http.StatusServiceUnavailable, liveResponse{Error: "nope"})

		assert.JSONEq(t, `{"healthy":false,"error":"nope"}`, rec.Body.String())
	})
}

func TestHealthcheckRouting(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		path         string
		expectedCode int
		// readiness responses carry a taskQueues key, liveness ones do not
		readiness bool
	}{
		{name: "livez", path: "/livez", expectedCode: http.StatusOK},
		{name: "readyz", path: "/readyz", expectedCode: http.StatusOK, readiness: true},
		{name: "health", path: "/health", expectedCode: http.StatusOK, readiness: true},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHealthcheck(newFakeTemporalClient(), testTaskQueue)

			rec := serve(h, test.path)

			assert.Equal(t, test.expectedCode, rec.Code)

			body := map[string]any{}
			decodeBody(t, rec, &body)

			assert.Equal(t, true, body["healthy"])
			assert.Equal(t, test.readiness, body["temporalOk"] != nil, "readiness responses report temporalOk")
			assert.Equal(t, test.readiness, body["taskQueues"] != nil, "readiness responses report task queues")
		})
	}

	t.Run("unknown paths are not found", func(t *testing.T) {
		t.Parallel()

		h := newTestHealthcheck(newFakeTemporalClient(), testTaskQueue)

		for _, path := range []string{"/", "/nope", "/livez/", "/healthz"} {
			rec := serve(h, path)

			assert.Equal(t, http.StatusNotFound, rec.Code, "path %q", path)
		}
	})
}

func TestHealthcheckLiveness(t *testing.T) {
	t.Parallel()

	t.Run("temporal healthy", func(t *testing.T) {
		t.Parallel()

		fake := newFakeTemporalClient()
		h := newTestHealthcheck(fake, testTaskQueue)

		rec := serve(h, "/livez")

		assert.Equal(t, http.StatusOK, rec.Code)
		assert.JSONEq(t, `{"healthy":true}`, rec.Body.String())
		assert.Equal(t, 1, fake.checkHealthCalls)
		assert.Empty(t, fake.calls(), "liveness should not describe task queues")
	})

	t.Run("temporal unhealthy", func(t *testing.T) {
		t.Parallel()

		fake := newFakeTemporalClient()
		fake.checkHealthErr = errBoom
		h := newTestHealthcheck(fake, testTaskQueue)

		rec := serve(h, "/livez")

		assert.Equal(t, http.StatusServiceUnavailable, rec.Code)

		var body liveResponse
		decodeBody(t, rec, &body)

		assert.False(t, body.Healthy)
		assert.Equal(t, errBoom.Error(), body.Error)
	})
}

func TestHealthcheckReadiness(t *testing.T) {
	t.Parallel()

	t.Run("temporal unavailable", func(t *testing.T) {
		t.Parallel()

		fake := newFakeTemporalClient()
		fake.checkHealthErr = errBoom
		h := newTestHealthcheck(fake, testTaskQueue)

		rec := serve(h, "/readyz")

		assert.Equal(t, http.StatusServiceUnavailable, rec.Code)

		var body readyResponse
		decodeBody(t, rec, &body)

		assert.False(t, body.Healthy)
		assert.False(t, body.TemporalOK)
		assert.Equal(t, errBoom.Error(), body.Error)
		assert.Empty(t, body.TaskQueues)
		assert.Empty(t, fake.calls(), "task queues should not be described when Temporal is unavailable")
	})

	t.Run("no task queues", func(t *testing.T) {
		t.Parallel()

		fake := newFakeTemporalClient()
		h := newTestHealthcheck(fake)

		rec := serve(h, "/readyz")

		assert.Equal(t, http.StatusOK, rec.Code)

		var body readyResponse
		decodeBody(t, rec, &body)

		assert.True(t, body.Healthy)
		assert.True(t, body.TemporalOK)
		assert.Empty(t, body.TaskQueues)
		assert.Empty(t, fake.calls())
	})

	t.Run("healthy workflow and activity pollers", func(t *testing.T) {
		t.Parallel()

		fake := newFakeTemporalClient()
		h := newTestHealthcheck(fake, testTaskQueue)

		rec := serve(h, "/readyz")

		assert.Equal(t, http.StatusOK, rec.Code)

		var body readyResponse
		decodeBody(t, rec, &body)

		assert.True(t, body.Healthy)
		assert.True(t, body.TemporalOK)
		assert.Empty(t, body.Error)
		assert.Equal(t, []taskQueueHealth{
			{
				TaskQueue: testTaskQueue,
				Healthy:   true,
				Checks: []taskQueueTypeHealth{
					{Type: nameWorkflowType, Healthy: true},
					{Type: nameActivityType, Healthy: true},
				},
			},
		}, body.TaskQueues)

		assert.Equal(t, []describeCall{
			{taskQueue: testTaskQueue, queueType: enumspb.TASK_QUEUE_TYPE_WORKFLOW},
			{taskQueue: testTaskQueue, queueType: enumspb.TASK_QUEUE_TYPE_ACTIVITY},
		}, fake.calls())
	})

	unhealthyTests := []struct {
		name           string
		failedType     enumspb.TaskQueueType
		expectedChecks []taskQueueTypeHealth
	}{
		{
			name:       "unhealthy workflow queue",
			failedType: enumspb.TASK_QUEUE_TYPE_WORKFLOW,
			expectedChecks: []taskQueueTypeHealth{
				{Type: nameWorkflowType, Healthy: false, Error: errBoom.Error()},
				{Type: nameActivityType, Healthy: true},
			},
		},
		{
			name:       "unhealthy activity queue",
			failedType: enumspb.TASK_QUEUE_TYPE_ACTIVITY,
			expectedChecks: []taskQueueTypeHealth{
				{Type: nameWorkflowType, Healthy: true},
				{Type: nameActivityType, Healthy: false, Error: errBoom.Error()},
			},
		},
	}

	for _, test := range unhealthyTests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			fake := newFakeTemporalClient()
			fake.failTaskQueue(testTaskQueue, test.failedType, errBoom)
			h := newTestHealthcheck(fake, testTaskQueue)

			rec := serve(h, "/readyz")

			assert.Equal(t, http.StatusServiceUnavailable, rec.Code)

			var body readyResponse
			decodeBody(t, rec, &body)

			assert.False(t, body.Healthy)
			assert.True(t, body.TemporalOK, "Temporal itself is still reachable")
			assert.Empty(t, body.Error)
			assert.Equal(t, []taskQueueHealth{
				{TaskQueue: testTaskQueue, Healthy: false, Checks: test.expectedChecks},
			}, body.TaskQueues)

			// Both types are always checked, even when the first one fails.
			assert.Len(t, fake.calls(), 2)
		})
	}

	t.Run("multiple queues with a mixture of results", func(t *testing.T) {
		t.Parallel()

		fake := newFakeTemporalClient()
		fake.failTaskQueue("bad-queue", enumspb.TASK_QUEUE_TYPE_ACTIVITY, errBoom)
		h := newTestHealthcheck(fake, "good-queue", "bad-queue", "another-good-queue")

		rec := serve(h, "/readyz")

		assert.Equal(t, http.StatusServiceUnavailable, rec.Code)

		var body readyResponse
		decodeBody(t, rec, &body)

		assert.False(t, body.Healthy, "one unhealthy queue makes the whole check unhealthy")
		assert.True(t, body.TemporalOK)

		require.Len(t, body.TaskQueues, 3)
		assert.Equal(t, "good-queue", body.TaskQueues[0].TaskQueue)
		assert.True(t, body.TaskQueues[0].Healthy)
		assert.Equal(t, "bad-queue", body.TaskQueues[1].TaskQueue)
		assert.False(t, body.TaskQueues[1].Healthy)
		assert.Equal(t, "another-good-queue", body.TaskQueues[2].TaskQueue)
		assert.True(t, body.TaskQueues[2].Healthy, "a later queue is not tainted by an earlier failure")

		assert.Equal(t, []describeCall{
			{taskQueue: "good-queue", queueType: enumspb.TASK_QUEUE_TYPE_WORKFLOW},
			{taskQueue: "good-queue", queueType: enumspb.TASK_QUEUE_TYPE_ACTIVITY},
			{taskQueue: "bad-queue", queueType: enumspb.TASK_QUEUE_TYPE_WORKFLOW},
			{taskQueue: "bad-queue", queueType: enumspb.TASK_QUEUE_TYPE_ACTIVITY},
			{taskQueue: "another-good-queue", queueType: enumspb.TASK_QUEUE_TYPE_WORKFLOW},
			{taskQueue: "another-good-queue", queueType: enumspb.TASK_QUEUE_TYPE_ACTIVITY},
		}, fake.calls())
	})

	t.Run("every queue type failing", func(t *testing.T) {
		t.Parallel()

		fake := newFakeTemporalClient()
		fake.failTaskQueue(testTaskQueue, enumspb.TASK_QUEUE_TYPE_WORKFLOW, errBoom)
		fake.failTaskQueue(testTaskQueue, enumspb.TASK_QUEUE_TYPE_ACTIVITY, errBoom)
		h := newTestHealthcheck(fake, testTaskQueue)

		rec := serve(h, "/health")

		assert.Equal(t, http.StatusServiceUnavailable, rec.Code)

		var body readyResponse
		decodeBody(t, rec, &body)

		require.Len(t, body.TaskQueues, 1)
		assert.False(t, body.TaskQueues[0].Healthy)

		for _, check := range body.TaskQueues[0].Checks {
			assert.False(t, check.Healthy)
			assert.Equal(t, errBoom.Error(), check.Error)
		}
	})
}

// TestHealthcheckTimeout proves the handler applies its own deadline to the
// Temporal calls rather than passing the request context straight through.
func TestHealthcheckTimeout(t *testing.T) {
	t.Parallel()

	var deadlineSet bool

	h := newTestHealthcheck(&deadlineRecordingClient{onCheck: func(ctx context.Context) {
		_, deadlineSet = ctx.Deadline()
	}})

	rec := serve(h, "/livez")

	assert.Equal(t, http.StatusOK, rec.Code)
	assert.True(t, deadlineSet, "the healthcheck should apply its timeout to the Temporal call")
}

// deadlineRecordingClient reports the context it is called with.
type deadlineRecordingClient struct {
	client.Client

	onCheck func(ctx context.Context)
}

func (d *deadlineRecordingClient) CheckHealth(
	ctx context.Context,
	_ *client.CheckHealthRequest,
) (*client.CheckHealthResponse, error) {
	d.onCheck(ctx)

	return &client.CheckHealthResponse{}, nil
}

// Bounds for the lifecycle tests below. They are deliberately small: every wait
// is for work that has already been started on another goroutine, so the
// deadlines only exist so a broken implementation fails rather than hangs.
const (
	// maxAddressAttempts bounds how many loopback ports a test will try before
	// giving up on finding a free one.
	maxAddressAttempts = 20
	// requestTimeout bounds a single healthcheck HTTP request.
	requestTimeout = 5 * time.Second
	// shutdownTimeout bounds how long a test waits for the server to stop.
	shutdownTimeout = 5 * time.Second
	// pollInterval is how long requireNotServing waits between connection
	// attempts once one has been refused... it retries rather than sleeps.
	pollInterval = 5 * time.Millisecond
)

// freeLoopbackAddress asks the kernel for an unused loopback port and returns
// the address it assigned. The listener is closed before returning because
// NewHealthCheck takes an address and binds the port itself, so a test cannot
// hand it a listener it is already holding. That leaves a window in which
// something else could claim the port, so callers must treat a bind failure as
// "pick another port" rather than a test failure - see startHealthCheck.
func freeLoopbackAddress(t *testing.T) string {
	t.Helper()

	l, err := (&net.ListenConfig{}).Listen(t.Context(), "tcp", "127.0.0.1:0")
	require.NoError(t, err)

	address := l.Addr().String()
	require.NoError(t, l.Close())

	return address
}

// startHealthCheck starts a healthcheck on a free loopback address, moving to a
// different port if the one it picked was claimed in the meantime, and returns
// the address it is listening on. Any error other than "address already in use"
// fails the test immediately.
func startHealthCheck(t *testing.T, ctx context.Context, c client.Client, taskQueues ...string) string {
	t.Helper()

	for range maxAddressAttempts {
		address := freeLoopbackAddress(t)

		err := NewHealthCheck(ctx, taskQueues, address, c)
		if err == nil {
			return address
		}

		require.ErrorIs(t, err, syscall.EADDRINUSE, "unexpected error starting the healthcheck")
	}

	t.Fatalf("no free loopback port found after %d attempts", maxAddressAttempts)

	return ""
}

// newTestHTTPClient builds a client with its own transport so that no test
// touches the shared default transport, and with keep-alives disabled so that a
// pooled connection cannot make a stopped server look like it is still up.
func newTestHTTPClient(t *testing.T) *http.Client {
	t.Helper()

	transport := &http.Transport{DisableKeepAlives: true}
	t.Cleanup(transport.CloseIdleConnections)

	return &http.Client{Transport: transport, Timeout: requestTimeout}
}

// getLiveness performs a GET /livez against the address, returning the status
// code and body, or the transport error if the request never got a response.
func getLiveness(t *testing.T, c *http.Client, address string) (status int, body string, err error) {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), requestTimeout)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://"+address+"/livez", http.NoBody)
	require.NoError(t, err)

	resp, err := c.Do(req)
	if err != nil {
		return 0, "", err
	}
	defer func() { _ = resp.Body.Close() }()

	raw, err := io.ReadAll(resp.Body)
	require.NoError(t, err)

	return resp.StatusCode, string(raw), nil
}

// requireNotServing waits for the address to stop answering requests. Shutdown
// runs on a goroutine, so the settled state can only be observed by retrying;
// each attempt is a real connection attempt that fails as soon as the listener
// has gone, so nothing here sleeps waiting for a fixed period.
func requireNotServing(t *testing.T, c *http.Client, address string) {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()

	ticker := time.NewTicker(pollInterval)
	defer ticker.Stop()

	for {
		if _, _, err := getLiveness(t, c, address); err != nil {
			return
		}

		select {
		case <-ctx.Done():
			t.Fatalf("%s was still serving requests after %s", address, shutdownTimeout)
		case <-ticker.C:
		}
	}
}

// TestNewHealthCheck covers the startup and shutdown behaviour of
// NewHealthCheck. The request handling itself is covered by the handler tests
// above, so these tests only ever ask for /livez as proof of life.
//
// None of these subtests are parallel: they each occupy a loopback port for
// their duration and the retry in startHealthCheck is there to survive
// interference from outside the package, not from the tests next to it.
func TestNewHealthCheck(t *testing.T) {
	t.Run("returns an error when the address cannot be bound", func(t *testing.T) {
		occupied, err := (&net.ListenConfig{}).Listen(t.Context(), "tcp", "127.0.0.1:0")
		require.NoError(t, err)
		t.Cleanup(func() { _ = occupied.Close() })

		address := occupied.Addr().String()

		// The context is never cancelled: if NewHealthCheck reports an error it
		// must not have started any goroutines that need cancelling.
		err = NewHealthCheck(context.Background(), []string{testTaskQueue}, address, newFakeTemporalClient())

		require.Error(t, err)
		assert.ErrorContains(t, err, "error listening on")
		assert.ErrorContains(t, err, address, "the error names the address that could not be bound")
		assert.ErrorIs(t, err, syscall.EADDRINUSE)

		// Releasing the port shows the failed call left nothing serving on it.
		require.NoError(t, occupied.Close())
		requireNotServing(t, newTestHTTPClient(t), address)
	})

	t.Run("returns nil once the listener is serving", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		t.Cleanup(cancel)

		address := startHealthCheck(t, ctx, newFakeTemporalClient(), testTaskQueue)

		// The listener is created before NewHealthCheck returns, so the
		// connection is accepted even if Serve has not been scheduled yet: no
		// polling is needed to see the first request answered.
		status, body, err := getLiveness(t, newTestHTTPClient(t), address)

		require.NoError(t, err)
		assert.Equal(t, http.StatusOK, status)
		assert.JSONEq(t, `{"healthy":true}`, body, "the fake client reports a healthy Temporal")
	})

	t.Run("shuts down when the context is cancelled", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		httpClient := newTestHTTPClient(t)
		address := startHealthCheck(t, ctx, newFakeTemporalClient(), testTaskQueue)

		status, _, err := getLiveness(t, httpClient, address)
		require.NoError(t, err, "the healthcheck should serve before the context is cancelled")
		require.Equal(t, http.StatusOK, status)

		cancel()

		requireNotServing(t, httpClient, address)
	})

	t.Run("an already cancelled context binds but does not stay up", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		// The address is a literal IP, so net.ListenConfig.Listen needs no name
		// resolution and binds without ever consulting the context: the current
		// implementation returns nil here rather than a context error.
		// startHealthCheck asserts that nil for us.
		address := startHealthCheck(t, ctx, newFakeTemporalClient(), testTaskQueue)

		// The shutdown goroutine sees the cancelled context straight away, so
		// the server tears itself down. A request that lands in the gap between
		// Listen and Shutdown may still be answered, so only the settled state
		// is asserted.
		requireNotServing(t, newTestHTTPClient(t), address)
	})
}
