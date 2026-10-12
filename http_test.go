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
	"bytes"
	"context"
	"errors"
	"io"
	"math"
	"net/http"
	"net/http/cookiejar"
	"net/url"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	temporalSDK "go.temporal.io/sdk/temporal"
)

// fakeTransport is an http.RoundTripper backed by a function. It closes the
// request body after the function returns, as the RoundTripper contract
// requires.
type fakeTransport struct {
	fn transportFunc
}

func (f *fakeTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	if req.Body != nil {
		defer func() { _ = req.Body.Close() }()
	}

	return f.fn(req)
}

// fakeBody is a controllable response body. It returns data, then err (or
// io.EOF when err is nil), and records whether it was closed.
type fakeBody struct {
	data   []byte
	err    error
	onRead func()
	closed atomic.Bool
}

func (b *fakeBody) Read(p []byte) (int, error) {
	if b.onRead != nil {
		b.onRead()
	}

	if len(b.data) > 0 {
		n := copy(p, b.data)
		b.data = b.data[n:]
		return n, nil
	}

	if b.err != nil {
		return 0, b.err
	}

	return 0, io.EOF
}

func (b *fakeBody) Close() error {
	b.closed.Store(true)
	return nil
}

const testURL = "https://example.com/"

// transportFunc is the function behind a fakeTransport.
type transportFunc func(*http.Request) (*http.Response, error)

var (
	errTestRead      = errors.New("read failed")
	errTestTransport = errors.New("transport failed")
)

func newResponse(req *http.Request, status int, header http.Header, body io.ReadCloser) *http.Response {
	if header == nil {
		header = http.Header{}
	}

	return &http.Response{
		StatusCode:    status,
		Status:        strconv.Itoa(status) + " " + http.StatusText(status),
		Proto:         "HTTP/1.1",
		ProtoMajor:    1,
		ProtoMinor:    1,
		Header:        header,
		Body:          body,
		ContentLength: -1,
		Request:       req,
	}
}

// respond returns a transport function that replies with a fresh body on
// every call.
func respond(status int, header http.Header, body []byte) transportFunc {
	return func(req *http.Request) (*http.Response, error) {
		return newResponse(req, status, header.Clone(), io.NopCloser(bytes.NewReader(body))), nil
	}
}

func respondWithBody(status int, body io.ReadCloser) transportFunc {
	return func(req *http.Request) (*http.Response, error) {
		return newResponse(req, status, nil, body), nil
	}
}

func unexpectedTransport(t *testing.T) transportFunc {
	return func(req *http.Request) (*http.Response, error) {
		t.Errorf("unexpected request to %s", req.URL)
		return nil, errors.New("unexpected request")
	}
}

// waitForRequestContext blocks until the request context ends, failing the
// test rather than hanging if it never does.
func waitForRequestContext(t *testing.T, req *http.Request) error {
	select {
	case <-req.Context().Done():
		return req.Context().Err()
	case <-time.After(5 * time.Second):
		t.Error("request context was not done")
		return errors.New("request context was not done")
	}
}

func newTestHTTPActivity(
	t *testing.T,
	fn transportFunc,
	opts ...HTTPOption,
) *HTTPActivity {
	t.Helper()

	h, err := NewHTTPActivity(append([]HTTPOption{WithTransport(&fakeTransport{fn: fn})}, opts...)...)
	require.NoError(t, err)

	return h
}

func requireApplicationError(
	t *testing.T,
	err error,
	errType string,
	nonRetryable bool,
) *temporalSDK.ApplicationError {
	t.Helper()

	var appErr *temporalSDK.ApplicationError
	require.ErrorAs(t, err, &appErr)
	assert.Equal(t, errType, appErr.Type())
	assert.Equal(t, nonRetryable, appErr.NonRetryable())

	return appErr
}

func requireStatusErrorDetails(t *testing.T, appErr *temporalSDK.ApplicationError) HTTPStatusErrorDetails {
	t.Helper()

	var details HTTPStatusErrorDetails
	require.NoError(t, appErr.Details(&details))

	return details
}

func TestHTTPActivityCallRequest(t *testing.T) {
	binaryBody := []byte{0x00, 0x01, 0xfe, 0xff, '\r', '\n'}

	tests := []struct {
		name       string
		input      HTTPRequest
		wantMethod string
		wantBody   []byte
	}{
		{
			name:       "empty method defaults to GET",
			input:      HTTPRequest{URL: testURL},
			wantMethod: http.MethodGet,
			wantBody:   []byte{},
		},
		{
			name:       "explicit method and binary body are preserved",
			input:      HTTPRequest{Method: http.MethodPut, URL: testURL, Body: binaryBody},
			wantMethod: http.MethodPut,
			wantBody:   binaryBody,
		},
		{
			name:       "custom method is preserved",
			input:      HTTPRequest{Method: "PROPFIND", URL: "http://example.com/"},
			wantMethod: "PROPFIND",
			wantBody:   []byte{},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			h := newTestHTTPActivity(t, func(req *http.Request) (*http.Response, error) {
				assert.Equal(t, test.wantMethod, req.Method)

				body, err := io.ReadAll(req.Body)
				assert.NoError(t, err)
				assert.Equal(t, test.wantBody, body)

				return respond(http.StatusOK, nil, nil)(req)
			})

			_, err := h.Call(context.Background(), test.input)
			require.NoError(t, err)
		})
	}
}

func TestHTTPActivityCallCopiesHeaders(t *testing.T) {
	input := http.Header{
		"Content-Type": {"application/octet-stream"},
		"X-Multi":      {"a", "b"},
	}
	want := input.Clone()

	h := newTestHTTPActivity(t, func(req *http.Request) (*http.Response, error) {
		assert.Equal(t, "application/octet-stream", req.Header.Get("Content-Type"))
		assert.Equal(t, []string{"a", "b"}, req.Header.Values("X-Multi"))

		req.Header.Set("X-Multi", "mutated")
		req.Header.Add("X-Added", "1")

		return respond(http.StatusOK, nil, nil)(req)
	})

	_, err := h.Call(context.Background(), HTTPRequest{URL: testURL, Headers: input})
	require.NoError(t, err)
	assert.Equal(t, want, input)
}

func TestHTTPActivityCallRejectsInvalidRequests(t *testing.T) {
	tests := []struct {
		name  string
		input HTTPRequest
	}{
		{name: "invalid method", input: HTTPRequest{Method: "BAD METHOD", URL: testURL}},
		{name: "unparseable URL", input: HTTPRequest{URL: "http://[::1"}},
		{name: "empty URL", input: HTTPRequest{URL: ""}},
		{name: "relative URL", input: HTTPRequest{URL: "/relative"}},
		{name: "unsupported scheme", input: HTTPRequest{URL: "ftp://example.com/"}},
		{name: "missing host", input: HTTPRequest{URL: "http:///path"}},
		{name: "opaque URL", input: HTTPRequest{URL: "http:opaque"}},
		{
			name:  "invalid header name",
			input: HTTPRequest{URL: testURL, Headers: http.Header{"Bad Name": {"value"}}},
		},
		{
			name:  "empty header name",
			input: HTTPRequest{URL: testURL, Headers: http.Header{"": {"value"}}},
		},
		{
			name:  "header value with line break",
			input: HTTPRequest{URL: testURL, Headers: http.Header{"X-Test": {"a\r\nInjected: b"}}},
		},
		{
			name:  "header value with control character",
			input: HTTPRequest{URL: testURL, Headers: http.Header{"X-Test": {"a\x00b"}}},
		},
		{
			name: "malformed existing query with additional values",
			input: HTTPRequest{
				URL:   "https://example.com/?a=1;b=2",
				Query: map[string][]string{"c": {"3"}},
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			h := newTestHTTPActivity(t, unexpectedTransport(t))

			res, err := h.Call(context.Background(), test.input)
			assert.Nil(t, res)
			_ = requireApplicationError(t, err, ErrorTypeHTTPRequest, true)
		})
	}
}

func TestHTTPActivityCallQuery(t *testing.T) {
	t.Run("values are appended and escaped", func(t *testing.T) {
		const special = "hello world&=?/"

		query := map[string][]string{
			"a": {"2", "3"},
			"c": {special},
		}
		want := map[string][]string{
			"a": {"2", "3"},
			"c": {special},
		}

		h := newTestHTTPActivity(t, func(req *http.Request) (*http.Response, error) {
			got, err := url.ParseQuery(req.URL.RawQuery)
			assert.NoError(t, err)
			assert.Equal(t, url.Values{
				"a": {"1", "2", "3"},
				"b": {"x/y"},
				"c": {special},
			}, got)

			return respond(http.StatusOK, nil, nil)(req)
		})

		_, err := h.Call(context.Background(), HTTPRequest{
			URL:   "https://example.com/search?a=1&b=x%2Fy",
			Query: query,
		})
		require.NoError(t, err)
		assert.Equal(t, want, query)
	})

	rawQueryTests := []struct {
		name  string
		query map[string][]string
	}{
		{name: "nil query", query: nil},
		{name: "empty query", query: map[string][]string{}},
	}

	for _, test := range rawQueryTests {
		t.Run(test.name+" preserves the raw query", func(t *testing.T) {
			const rawQuery = "z=1&a=%7e&b=a+b&c=1;d=2"

			h := newTestHTTPActivity(t, func(req *http.Request) (*http.Response, error) {
				assert.Equal(t, rawQuery, req.URL.RawQuery)
				return respond(http.StatusOK, nil, nil)(req)
			})

			_, err := h.Call(context.Background(), HTTPRequest{
				URL:   "https://example.com/p?" + rawQuery,
				Query: test.query,
			})
			require.NoError(t, err)
		})
	}
}

func TestHTTPActivityCallSuccess(t *testing.T) {
	body := []byte{0x00, 0xff, 'o', 'k', '\n'}

	for _, status := range []int{http.StatusOK, http.StatusCreated, http.StatusAccepted, http.StatusPartialContent} {
		t.Run(strconv.Itoa(status), func(t *testing.T) {
			header := http.Header{
				"Content-Type": {"application/octet-stream"},
				"X-Multi":      {"a", "b"},
			}

			h := newTestHTTPActivity(t, respond(status, header, body))

			res, err := h.Call(context.Background(), HTTPRequest{URL: testURL})
			require.NoError(t, err)
			assert.Equal(t, status, res.StatusCode)
			assert.Equal(t, header, res.Header)
			assert.Equal(t, body, res.Body)
		})
	}

	t.Run("empty body", func(t *testing.T) {
		h := newTestHTTPActivity(t, respond(http.StatusNoContent, nil, nil))

		res, err := h.Call(context.Background(), HTTPRequest{URL: testURL})
		require.NoError(t, err)
		assert.Equal(t, http.StatusNoContent, res.StatusCode)
		assert.Empty(t, res.Body)
	})
}

func TestHTTPActivityCallBodyLimit(t *testing.T) {
	tests := []struct {
		name    string
		opts    []HTTPOption
		size    int
		wantErr bool
	}{
		{name: "configured limit exactly", opts: []HTTPOption{WithMaxBodySize(16)}, size: 16},
		{name: "configured limit plus one", opts: []HTTPOption{WithMaxBodySize(16)}, size: 17, wantErr: true},
		{name: "default limit exactly", size: 1 << 20},
		{name: "default limit plus one", size: 1<<20 + 1, wantErr: true},
		{name: "limit above 2 MiB", opts: []HTTPOption{WithMaxBodySize(3 << 20)}, size: 5 << 19},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			body := bytes.Repeat([]byte{'x'}, test.size)
			h := newTestHTTPActivity(t, respond(http.StatusOK, nil, body), test.opts...)

			res, err := h.Call(context.Background(), HTTPRequest{URL: testURL})
			if test.wantErr {
				assert.Nil(t, res)
				_ = requireApplicationError(t, err, ErrorTypeHTTPResponseTooLarge, true)
				return
			}

			require.NoError(t, err)
			assert.Equal(t, body, res.Body)
		})
	}
}

func TestHTTPActivityCallStatusPolicy(t *testing.T) {
	tests := []struct {
		status        int
		wantRetryable bool
	}{
		{status: http.StatusRequestTimeout, wantRetryable: true},
		{status: http.StatusTooManyRequests, wantRetryable: true},
		{status: http.StatusNotImplemented},
		{status: http.StatusHTTPVersionNotSupported},
		{status: http.StatusVariantAlsoNegotiates},
		{status: http.StatusLoopDetected},
		{status: http.StatusNetworkAuthenticationRequired},
		{status: http.StatusInternalServerError, wantRetryable: true},
		{status: http.StatusBadGateway, wantRetryable: true},
		{status: http.StatusServiceUnavailable, wantRetryable: true},
		{status: http.StatusGatewayTimeout, wantRetryable: true},
		{status: http.StatusInsufficientStorage, wantRetryable: true},
		{status: 599, wantRetryable: true},
		{status: http.StatusMultipleChoices},
		{status: http.StatusNotModified},
		{status: http.StatusBadRequest},
		{status: http.StatusUnauthorized},
		{status: http.StatusForbidden},
		{status: http.StatusNotFound},
		{status: http.StatusConflict},
		{status: http.StatusMisdirectedRequest},
		{status: http.StatusUnprocessableEntity},
		{status: http.StatusTooEarly},
	}

	for _, test := range tests {
		t.Run(strconv.Itoa(test.status), func(t *testing.T) {
			h := newTestHTTPActivity(t, respond(test.status, nil, nil))

			res, err := h.Call(context.Background(), HTTPRequest{URL: testURL})
			assert.Nil(t, res)

			appErr := requireApplicationError(t, err, ErrorTypeHTTPStatus, !test.wantRetryable)
			assert.Equal(t, test.status, requireStatusErrorDetails(t, appErr).StatusCode)
		})
	}
}

func TestHTTPActivityCallRetryAfter(t *testing.T) {
	future := time.Now().Add(time.Hour).UTC().Format(http.TimeFormat)
	past := time.Now().Add(-time.Hour).UTC().Format(http.TimeFormat)
	maxSeconds := math.MaxInt64 / int64(time.Second)

	tests := []struct {
		name          string
		status        int
		retryAfter    *string
		wantRetryable bool
		wantMin       time.Duration
		wantMax       time.Duration
	}{
		{name: "413 absent", status: http.StatusRequestEntityTooLarge},
		{name: "413 invalid", status: http.StatusRequestEntityTooLarge, retryAfter: new("soon")},
		{name: "413 zero", status: http.StatusRequestEntityTooLarge, retryAfter: new("0")},
		{name: "413 negative", status: http.StatusRequestEntityTooLarge, retryAfter: new("-10")},
		{name: "413 past date", status: http.StatusRequestEntityTooLarge, retryAfter: &past},
		{
			name:          "413 positive seconds",
			status:        http.StatusRequestEntityTooLarge,
			retryAfter:    new("120"),
			wantRetryable: true,
			wantMin:       120 * time.Second,
			wantMax:       120 * time.Second,
		},
		{
			name:          "413 future date",
			status:        http.StatusRequestEntityTooLarge,
			retryAfter:    &future,
			wantRetryable: true,
			wantMin:       58 * time.Minute,
			wantMax:       61 * time.Minute,
		},
		{
			name:          "503 positive seconds",
			status:        http.StatusServiceUnavailable,
			retryAfter:    new("120"),
			wantRetryable: true,
			wantMin:       120 * time.Second,
			wantMax:       120 * time.Second,
		},
		{
			name:          "503 surrounding whitespace",
			status:        http.StatusServiceUnavailable,
			retryAfter:    new("  30\t"),
			wantRetryable: true,
			wantMin:       30 * time.Second,
			wantMax:       30 * time.Second,
		},
		{
			name:          "503 future date",
			status:        http.StatusServiceUnavailable,
			retryAfter:    &future,
			wantRetryable: true,
			wantMin:       58 * time.Minute,
			wantMax:       61 * time.Minute,
		},
		{
			name:          "429 positive seconds",
			status:        http.StatusTooManyRequests,
			retryAfter:    new("5"),
			wantRetryable: true,
			wantMin:       5 * time.Second,
			wantMax:       5 * time.Second,
		},
		{name: "503 absent", status: http.StatusServiceUnavailable, wantRetryable: true},
		{name: "503 invalid", status: http.StatusServiceUnavailable, retryAfter: new("soon"), wantRetryable: true},
		{name: "503 past date", status: http.StatusServiceUnavailable, retryAfter: &past, wantRetryable: true},
		{
			name:          "503 overflows int64",
			status:        http.StatusServiceUnavailable,
			retryAfter:    new("99999999999999999999"),
			wantRetryable: true,
		},
		{
			name:          "503 overflows duration",
			status:        http.StatusServiceUnavailable,
			retryAfter:    new(strconv.FormatInt(maxSeconds+1, 10)),
			wantRetryable: true,
		},
		{name: "404 positive seconds", status: http.StatusNotFound, retryAfter: new("120")},
		{name: "501 positive seconds", status: http.StatusNotImplemented, retryAfter: new("120")},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			header := http.Header{}
			if test.retryAfter != nil {
				header["Retry-After"] = []string{*test.retryAfter}
			}

			h := newTestHTTPActivity(t, respond(test.status, header, nil))

			_, err := h.Call(context.Background(), HTTPRequest{URL: testURL})
			appErr := requireApplicationError(t, err, ErrorTypeHTTPStatus, !test.wantRetryable)

			delay := appErr.NextRetryDelay()
			assert.GreaterOrEqual(t, delay, test.wantMin)
			assert.LessOrEqual(t, delay, test.wantMax)
		})
	}
}

func TestHTTPActivityCallStatusErrorDetails(t *testing.T) {
	tests := []struct {
		name          string
		opts          []HTTPOption
		status        int
		body          []byte
		wantRetryable bool
		wantStatus    string
		wantBody      []byte
		wantTruncated bool
	}{
		{
			name:          "retryable",
			status:        http.StatusServiceUnavailable,
			body:          []byte("unavailable"),
			wantRetryable: true,
			wantStatus:    "503 Service Unavailable",
			wantBody:      []byte("unavailable"),
		},
		{
			name:       "non-retryable",
			status:     http.StatusNotFound,
			body:       []byte{0x00, 0xff},
			wantStatus: "404 Not Found",
			wantBody:   []byte{0x00, 0xff},
		},
		{
			name:     "preview at 4 KiB limit",
			status:   http.StatusNotFound,
			body:     bytes.Repeat([]byte{'a'}, 4<<10),
			wantBody: bytes.Repeat([]byte{'a'}, 4<<10),
		},
		{
			name:          "preview over 4 KiB limit",
			status:        http.StatusNotFound,
			body:          append(bytes.Repeat([]byte{'a'}, 4<<10), 'b'),
			wantBody:      bytes.Repeat([]byte{'a'}, 4<<10),
			wantTruncated: true,
		},
		{
			name:     "preview at smaller body limit",
			opts:     []HTTPOption{WithMaxBodySize(100)},
			status:   http.StatusNotFound,
			body:     bytes.Repeat([]byte{'a'}, 100),
			wantBody: bytes.Repeat([]byte{'a'}, 100),
		},
		{
			name:          "preview over smaller body limit",
			opts:          []HTTPOption{WithMaxBodySize(100)},
			status:        http.StatusNotFound,
			body:          append(bytes.Repeat([]byte{'a'}, 100), 'b'),
			wantBody:      bytes.Repeat([]byte{'a'}, 100),
			wantTruncated: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			h := newTestHTTPActivity(t, respond(test.status, nil, test.body), test.opts...)

			_, err := h.Call(context.Background(), HTTPRequest{URL: testURL})
			appErr := requireApplicationError(t, err, ErrorTypeHTTPStatus, !test.wantRetryable)

			details := requireStatusErrorDetails(t, appErr)
			assert.Equal(t, test.status, details.StatusCode)
			if test.wantStatus != "" {
				assert.Equal(t, test.wantStatus, details.Status)
			}
			assert.Equal(t, test.wantBody, details.Body)
			assert.Equal(t, test.wantTruncated, details.BodyTruncated)
		})
	}
}

func TestHTTPActivityCallReadFailures(t *testing.T) {
	t.Run("2xx body", func(t *testing.T) {
		h := newTestHTTPActivity(t, respondWithBody(http.StatusOK, &fakeBody{data: []byte("partial"), err: errTestRead}))

		res, err := h.Call(context.Background(), HTTPRequest{URL: testURL})
		assert.Nil(t, res)
		require.ErrorIs(t, err, errTestRead)
	})

	previewTests := []struct {
		status        int
		wantRetryable bool
	}{
		{status: http.StatusServiceUnavailable, wantRetryable: true},
		{status: http.StatusNotFound},
	}

	for _, test := range previewTests {
		t.Run("error preview "+strconv.Itoa(test.status), func(t *testing.T) {
			h := newTestHTTPActivity(t, respondWithBody(test.status, &fakeBody{data: []byte("partial"), err: errTestRead}))

			_, err := h.Call(context.Background(), HTTPRequest{URL: testURL})
			appErr := requireApplicationError(t, err, ErrorTypeHTTPStatus, !test.wantRetryable)
			require.ErrorIs(t, err, errTestRead)

			details := requireStatusErrorDetails(t, appErr)
			assert.Equal(t, test.status, details.StatusCode)
			assert.Equal(t, []byte("partial"), details.Body)
			assert.False(t, details.BodyTruncated)
		})
	}
}

func TestHTTPActivityCallTransportFailure(t *testing.T) {
	h := newTestHTTPActivity(t, func(*http.Request) (*http.Response, error) {
		return nil, errTestTransport
	})

	res, err := h.Call(context.Background(), HTTPRequest{URL: testURL})
	assert.Nil(t, res)
	_ = requireApplicationError(t, err, ErrorTypeHTTPRequest, false)
	require.ErrorIs(t, err, errTestTransport)
	assert.False(t, temporalSDK.IsCanceledError(err))
}

func TestHTTPActivityCallCancellation(t *testing.T) {
	t.Run("during request execution", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		t.Cleanup(cancel)

		h := newTestHTTPActivity(t, func(req *http.Request) (*http.Response, error) {
			cancel()
			return nil, waitForRequestContext(t, req)
		})

		res, err := h.Call(ctx, HTTPRequest{URL: testURL})
		assert.Nil(t, res)
		assert.True(t, temporalSDK.IsCanceledError(err), "got %v", err)
	})

	for _, status := range []int{http.StatusOK, http.StatusServiceUnavailable} {
		t.Run("during response reading "+strconv.Itoa(status), func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			t.Cleanup(cancel)

			body := &fakeBody{onRead: cancel, err: context.Canceled}
			h := newTestHTTPActivity(t, respondWithBody(status, body))

			res, err := h.Call(ctx, HTTPRequest{URL: testURL})
			assert.Nil(t, res)
			assert.True(t, temporalSDK.IsCanceledError(err), "got %v", err)
		})
	}
}

func TestHTTPActivityCallDeadlines(t *testing.T) {
	t.Run("client timeout", func(t *testing.T) {
		h := newTestHTTPActivity(t, func(req *http.Request) (*http.Response, error) {
			return nil, waitForRequestContext(t, req)
		}, WithTimeout(20*time.Millisecond))

		res, err := h.Call(context.Background(), HTTPRequest{URL: testURL})
		assert.Nil(t, res)
		_ = requireApplicationError(t, err, ErrorTypeHTTPRequest, false)
		assert.False(t, temporalSDK.IsCanceledError(err))
	})

	t.Run("context deadline", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
		t.Cleanup(cancel)

		h := newTestHTTPActivity(t, func(req *http.Request) (*http.Response, error) {
			return nil, waitForRequestContext(t, req)
		}, WithTimeout(0))

		res, err := h.Call(ctx, HTTPRequest{URL: testURL})
		assert.Nil(t, res)
		_ = requireApplicationError(t, err, ErrorTypeHTTPRequest, false)
		require.ErrorIs(t, err, context.DeadlineExceeded)
		assert.False(t, temporalSDK.IsCanceledError(err))
	})
}

func TestHTTPActivityCallClosesResponseBody(t *testing.T) {
	tests := []struct {
		name   string
		opts   []HTTPOption
		status int
		body   *fakeBody
	}{
		{name: "success", status: http.StatusOK, body: &fakeBody{data: []byte("ok")}},
		{name: "status failure", status: http.StatusNotFound, body: &fakeBody{data: []byte("missing")}},
		{
			name:   "oversized",
			opts:   []HTTPOption{WithMaxBodySize(4)},
			status: http.StatusOK,
			body:   &fakeBody{data: []byte("12345")},
		},
		{name: "read failure", status: http.StatusOK, body: &fakeBody{err: errTestRead}},
		{name: "preview read failure", status: http.StatusServiceUnavailable, body: &fakeBody{err: errTestRead}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			h := newTestHTTPActivity(t, respondWithBody(test.status, test.body), test.opts...)

			_, _ = h.Call(context.Background(), HTTPRequest{URL: testURL})
			assert.True(t, test.body.closed.Load())
		})
	}
}

func TestNewHTTPActivityDefaults(t *testing.T) {
	h, err := NewHTTPActivity()
	require.NoError(t, err)

	assert.Equal(t, 5*time.Second, h.client.Timeout)
	assert.Equal(t, int64(1<<20), h.maxBodyBytes)
	assert.Equal(t, int64(4<<10), h.maxPreviewLimit)
	assert.Equal(t, http.DefaultTransport, h.client.Transport)
	assert.Nil(t, h.client.Jar)
	assert.Nil(t, h.client.CheckRedirect)
}

func TestNewHTTPActivityOptions(t *testing.T) {
	transportA := &fakeTransport{}
	transportB := &fakeTransport{}
	jar, err := cookiejar.New(nil)
	require.NoError(t, err)

	tests := []struct {
		name  string
		opts  []HTTPOption
		check func(t *testing.T, h *HTTPActivity)
	}{
		{
			name:  "timeout override",
			opts:  []HTTPOption{WithTimeout(2 * time.Second)},
			check: func(t *testing.T, h *HTTPActivity) { assert.Equal(t, 2*time.Second, h.client.Timeout) },
		},
		{
			name:  "zero timeout",
			opts:  []HTTPOption{WithTimeout(0)},
			check: func(t *testing.T, h *HTTPActivity) { assert.Zero(t, h.client.Timeout) },
		},
		{
			name:  "body size override",
			opts:  []HTTPOption{WithMaxBodySize(10)},
			check: func(t *testing.T, h *HTTPActivity) { assert.Equal(t, int64(10), h.maxBodyBytes) },
		},
		{
			name:  "transport override",
			opts:  []HTTPOption{WithTransport(transportA)},
			check: func(t *testing.T, h *HTTPActivity) { assert.Same(t, transportA, h.client.Transport) },
		},
		{
			name:  "nil transport",
			opts:  []HTTPOption{WithTransport(nil)},
			check: func(t *testing.T, h *HTTPActivity) { assert.Nil(t, h.client.Transport) },
		},
		{
			name:  "cookie jar",
			opts:  []HTTPOption{WithCookieJar(jar)},
			check: func(t *testing.T, h *HTTPActivity) { assert.Same(t, jar, h.client.Jar) },
		},
		{
			name:  "nil cookie jar",
			opts:  []HTTPOption{WithCookieJar(nil)},
			check: func(t *testing.T, h *HTTPActivity) { assert.Nil(t, h.client.Jar) },
		},
		{
			name: "later options take precedence",
			opts: []HTTPOption{
				WithTimeout(time.Second),
				WithMaxBodySize(1),
				WithTransport(transportA),
				WithCookieJar(jar),
				WithTimeout(2 * time.Second),
				WithMaxBodySize(2),
				WithTransport(transportB),
				WithCookieJar(nil),
			},
			check: func(t *testing.T, h *HTTPActivity) {
				assert.Equal(t, 2*time.Second, h.client.Timeout)
				assert.Equal(t, int64(2), h.maxBodyBytes)
				assert.Same(t, transportB, h.client.Transport)
				assert.Nil(t, h.client.Jar)
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			h, err := NewHTTPActivity(test.opts...)
			require.NoError(t, err)
			test.check(t, h)
		})
	}
}

func TestNewHTTPActivityOptionErrors(t *testing.T) {
	tests := []struct {
		name string
		opts []HTTPOption
	}{
		{name: "nil option", opts: []HTTPOption{WithTimeout(time.Second), nil}},
		{name: "negative timeout", opts: []HTTPOption{WithTimeout(-time.Nanosecond)}},
		{name: "zero body size", opts: []HTTPOption{WithMaxBodySize(0)}},
		{name: "negative body size", opts: []HTTPOption{WithMaxBodySize(-1)}},
		{name: "maximum int64 body size", opts: []HTTPOption{WithMaxBodySize(math.MaxInt64)}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			h, err := NewHTTPActivity(test.opts...)
			require.Error(t, err)
			assert.Nil(t, h)
		})
	}
}

func TestHTTPActivityCallRedirects(t *testing.T) {
	const (
		startPath = "/start"
		finalPath = "/final"
	)

	tests := []struct {
		name      string
		opts      []HTTPOption
		wantPaths []string
	}{
		{name: "followed by default", wantPaths: []string{startPath, finalPath}},
		{name: "disabled", opts: []HTTPOption{WithNoRedirect()}, wantPaths: []string{startPath}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var paths []string

			h := newTestHTTPActivity(t, func(req *http.Request) (*http.Response, error) {
				paths = append(paths, req.URL.Path)

				switch req.URL.Path {
				case startPath:
					return respond(http.StatusFound, http.Header{"Location": {finalPath}}, nil)(req)
				case finalPath:
					return respond(http.StatusOK, nil, []byte("done"))(req)
				default:
					return unexpectedTransport(t)(req)
				}
			}, test.opts...)

			res, err := h.Call(context.Background(), HTTPRequest{URL: "https://example.com" + startPath})
			assert.Equal(t, test.wantPaths, paths)

			if len(test.wantPaths) == 2 {
				require.NoError(t, err)
				assert.Equal(t, []byte("done"), res.Body)
				return
			}

			assert.Nil(t, res)
			appErr := requireApplicationError(t, err, ErrorTypeHTTPStatus, true)
			assert.Equal(t, http.StatusFound, requireStatusErrorDetails(t, appErr).StatusCode)
		})
	}
}

func TestHTTPActivityCallCookies(t *testing.T) {
	tests := []struct {
		name        string
		opts        func(jar http.CookieJar) []HTTPOption
		wantCookies []string
	}{
		{
			name:        "reused within the same activity",
			opts:        func(jar http.CookieJar) []HTTPOption { return []HTTPOption{WithCookieJar(jar)} },
			wantCookies: []string{"", "session=abc"},
		},
		{
			name: "disabled by a later nil jar",
			opts: func(jar http.CookieJar) []HTTPOption {
				return []HTTPOption{WithCookieJar(jar), WithCookieJar(nil)}
			},
			wantCookies: []string{"", ""},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			jar, err := cookiejar.New(nil)
			require.NoError(t, err)

			var cookies []string

			h := newTestHTTPActivity(t, func(req *http.Request) (*http.Response, error) {
				cookies = append(cookies, req.Header.Get("Cookie"))
				return respond(http.StatusOK, http.Header{"Set-Cookie": {"session=abc; Path=/"}}, nil)(req)
			}, test.opts(jar)...)

			for _, path := range []string{"/login", "/data"} {
				_, err := h.Call(context.Background(), HTTPRequest{URL: "https://example.com" + path})
				require.NoError(t, err)
			}

			assert.Equal(t, test.wantCookies, cookies)
		})
	}
}

func TestHTTPActivityCallConcurrent(t *testing.T) {
	const calls = 32

	var count atomic.Int64

	h := newTestHTTPActivity(t, func(req *http.Request) (*http.Response, error) {
		count.Add(1)
		return respond(http.StatusOK, nil, []byte(req.URL.Query().Get("id")))(req)
	})

	var wg sync.WaitGroup
	for i := range calls {
		wg.Go(func() {
			id := strconv.Itoa(i)

			res, err := h.Call(context.Background(), HTTPRequest{
				URL:   testURL,
				Query: map[string][]string{"id": {id}},
			})
			if assert.NoError(t, err) {
				assert.Equal(t, []byte(id), res.Body)
			}
		})
	}
	wg.Wait()

	assert.Equal(t, int64(calls), count.Load())
}
