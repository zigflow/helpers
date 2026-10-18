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
	"fmt"
	"io"
	"math"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	temporalSDK "go.temporal.io/sdk/temporal"
	"golang.org/x/net/http/httpguts"
)

// Application error types returned by HTTPActivity.Call.
const (
	// ErrorTypeHTTPResponseTooLarge is the type of the non-retryable error
	// returned when a 2xx response body exceeds the maximum body size.
	ErrorTypeHTTPResponseTooLarge = "HTTPResponseTooLarge"

	// ErrorTypeHTTPRequest is the type of errors returned when a request is
	// invalid or cannot be executed. Validation errors are non-retryable.
	// Errors executing the request are retryable.
	ErrorTypeHTTPRequest = "HTTPRequestError"

	// ErrorTypeHTTPStatus is the type of errors returned for non-2xx
	// responses. Their details contain an HTTPStatusErrorDetails value.
	ErrorTypeHTTPStatus = "HTTPStatusError"
)

var nonRetryableStatusCodes = map[int]struct{}{
	http.StatusNotImplemented:                {},
	http.StatusHTTPVersionNotSupported:       {},
	http.StatusVariantAlsoNegotiates:         {},
	http.StatusLoopDetected:                  {},
	http.StatusNetworkAuthenticationRequired: {},
}

var retryableStatusCodes = map[int]struct{}{
	http.StatusRequestTimeout:  {},
	http.StatusTooManyRequests: {},
}

// HTTPActivity makes HTTP requests from Temporal activities. Register its Call
// method with a worker as an activity.
//
// HTTPActivity must be constructed with NewHTTPActivity. The zero value is not
// usable. An HTTPActivity may be used by concurrent activity executions,
// provided any transport or cookie jar supplied through options supports
// concurrent use and is not reconfigured while calls are running.
type HTTPActivity struct {
	client          *http.Client
	maxBodyBytes    int64
	maxPreviewLimit int64
}

// HTTPRequest describes the HTTP request made by HTTPActivity.Call. It is
// serialised as activity input, so Body and Headers count towards Temporal
// payload size.
type HTTPRequest struct {
	// Method is the HTTP method. An empty method means GET.
	Method string `json:"method"`

	// URL is the request URL. It must be an absolute HTTP or HTTPS URL with
	// a host.
	URL string `json:"url"`

	// Headers are the request headers. Header names and values are validated
	// and copied before the request is sent.
	Headers http.Header `json:"header,omitempty"`

	// Body is the raw request body. The helper does not encode its content
	// or choose a Content-Type; callers supply the appropriate header.
	Body []byte `json:"body,omitempty"`

	// Query holds values appended to existing URL query parameters.
	// When non-empty, the combined query is re-encoded, which can change
	// parameter order and escaping.
	Query map[string][]string `json:"query,omitempty"`
}

// HTTPResponse is the result of a 2xx response returned by HTTPActivity.Call.
// It is serialised as the activity result.
type HTTPResponse struct {
	// StatusCode is the 2xx HTTP status code.
	StatusCode int `json:"statusCode"`

	// Header is a copy of the response headers.
	Header http.Header `json:"header"`

	// Body is the complete response body, no larger than the configured
	// maximum body size.
	Body []byte `json:"body,omitempty"`
}

// HTTPStatusErrorDetails describes a non-2xx response. It is attached as the
// details of an ErrorTypeHTTPStatus application error.
type HTTPStatusErrorDetails struct {
	// StatusCode is the HTTP status code.
	StatusCode int `json:"statusCode"`

	// Status is the status code followed by the standard text returned by
	// http.StatusText, which is empty for unrecognised codes. For example,
	// "404 Not Found".
	Status string `json:"status"`

	// Body is a bounded preview of the response body. The preview limit is
	// fixed at 4 KiB and can only be reduced indirectly by a smaller maximum
	// body size. A read failure may leave a partial preview, with the read
	// error attached as the application error's cause.
	Body []byte `json:"body,omitempty"`

	// BodyTruncated reports whether Body was truncated because the response
	// body exceeded the preview limit. A false value does not guarantee a
	// complete preview if reading failed.
	BodyTruncated bool `json:"bodyTruncated,omitempty"`
}

// Call makes the HTTP request described by input and returns the response.
//
// Call runs inside a Temporal activity and makes one HTTP client call per
// activity attempt; redirects and transport behaviour may result in additional
// requests. Temporal controls retries according to the activity's retry policy
// and the retryability of the returned error. Temporal may execute an activity
// more than once, so requests with side effects are only safe to retry when
// the server supports idempotency, for example through an idempotency key
// supplied in Headers that remains stable across activity retries.
//
// Invalid URLs, header names or header values return a non-retryable
// ErrorTypeHTTPRequest error, as do malformed existing query strings when
// input.Query is non-empty. Errors executing the request return a retryable
// ErrorTypeHTTPRequest error. Cancellation observed during request execution
// or response reading returns a Temporal cancelled error. Deadline expiry is
// handled as a failure, not cancellation.
//
// A 2xx response is returned as an HTTPResponse. If its body exceeds the
// maximum body size, Call returns a non-retryable
// ErrorTypeHTTPResponseTooLarge error. Failures reading a 2xx response body
// return a wrapped error without a helper-defined application error type.
//
// Any other status returns an ErrorTypeHTTPStatus error whose details contain
// an HTTPStatusErrorDetails value. The helper's current retry policy is:
//
//   - 408 and 429 are retryable
//   - 413 is retryable only when the parsed Retry-After delay is positive
//   - 501, 505, 506, 508 and 511 are non-retryable
//   - other 5xx statuses are retryable
//   - all other statuses, including 3xx, are non-retryable
//
// This policy is not a universal guarantee and may not match the semantics of
// every API. For retryable responses, a valid positive
// Retry-After value, in seconds or as an HTTP date, overrides the next retry
// delay on Temporal servers that support it.
func (h *HTTPActivity) Call(ctx context.Context, input HTTPRequest) (*HTTPResponse, error) {
	method := input.Method
	if method == "" {
		method = http.MethodGet
	}

	req, err := http.NewRequestWithContext(ctx, method, input.URL, bytes.NewReader(input.Body))
	if err != nil {
		return nil, temporalSDK.NewNonRetryableApplicationError(
			"error creating http request",
			ErrorTypeHTTPRequest,
			err,
		)
	}

	if err := h.validateURL(req); err != nil {
		return nil, err
	}

	if input.Headers != nil {
		if err := h.validateHeaders(input.Headers); err != nil {
			return nil, err
		}

		req.Header = input.Headers.Clone()
	}

	if len(input.Query) > 0 {
		q, err := url.ParseQuery(req.URL.RawQuery)
		if err != nil {
			return nil, temporalSDK.NewNonRetryableApplicationError(
				"error parsing HTTP request query",
				ErrorTypeHTTPRequest,
				err,
			)
		}

		for k, vs := range input.Query {
			for _, v := range vs {
				q.Add(k, v)
			}
		}
		req.URL.RawQuery = q.Encode()
	}

	resp, err := h.client.Do(req)
	if err != nil {
		if errors.Is(ctx.Err(), context.Canceled) {
			return nil, temporalSDK.NewCanceledError()
		}

		return nil, temporalSDK.NewApplicationErrorWithCause(
			"error executing HTTP request",
			ErrorTypeHTTPRequest,
			err,
		)
	}
	defer func() {
		_ = resp.Body.Close()
	}()

	result, err := h.parseResponse(resp)

	if errors.Is(ctx.Err(), context.Canceled) {
		return nil, temporalSDK.NewCanceledError()
	}

	return result, err
}

// isRetryable applies the helper's default HTTP status retry policy.
// Most 5xx responses are retryable. Most 4xx responses are non-retryable.
// API-specific semantics can require a different policy.
func (h *HTTPActivity) isRetryable(statusCode int, retryDelay time.Duration) bool {
	// Check it doesn't overlap with the non-retryable status codes
	if _, ok := nonRetryableStatusCodes[statusCode]; ok {
		return false
	}

	// Check for custom status codes
	if _, ok := retryableStatusCodes[statusCode]; ok {
		return true
	}

	if statusCode == http.StatusRequestEntityTooLarge && retryDelay > 0 {
		return true
	}

	if statusCode >= 500 && statusCode < 600 {
		return true
	}

	return false
}

func (h *HTTPActivity) parseResponse(resp *http.Response) (*HTTPResponse, error) {
	statusCode := resp.StatusCode

	if statusCode < 200 || statusCode >= 300 {
		previewLimit := min(h.maxBodyBytes, h.maxPreviewLimit)
		body, readErr := io.ReadAll(
			io.LimitReader(resp.Body, previewLimit+1),
		)

		truncated := int64(len(body)) > previewLimit
		if truncated {
			body = body[:previewLimit]
		}

		retryDelay := h.parseRetryAfter(resp)
		isRetryable := h.isRetryable(resp.StatusCode, retryDelay)

		status := fmt.Sprintf(
			"%d %s",
			statusCode,
			http.StatusText(statusCode),
		)

		options := temporalSDK.ApplicationErrorOptions{
			NonRetryable: !isRetryable,
			Cause:        readErr,
			Details: []any{
				HTTPStatusErrorDetails{
					StatusCode:    statusCode,
					Status:        status,
					Body:          body,
					BodyTruncated: truncated,
				},
			},
		}

		if isRetryable {
			options.NextRetryDelay = retryDelay
		}

		return nil, temporalSDK.NewApplicationErrorWithOptions(
			"HTTP call returned "+status,
			ErrorTypeHTTPStatus,
			options,
		)
	}

	body, err := io.ReadAll(io.LimitReader(resp.Body, h.maxBodyBytes+1))
	if int64(len(body)) > h.maxBodyBytes {
		return nil, temporalSDK.NewNonRetryableApplicationError(
			fmt.Sprintf("response body exceeds %d bytes", h.maxBodyBytes),
			ErrorTypeHTTPResponseTooLarge,
			nil,
		)
	}
	if err != nil {
		return nil, fmt.Errorf("error reading http response: %w", err)
	}

	return &HTTPResponse{
		StatusCode: statusCode,
		Header:     resp.Header.Clone(),
		Body:       body,
	}, nil
}

func (h *HTTPActivity) parseRetryAfter(resp *http.Response) time.Duration {
	retryDelay := resp.Header.Get("Retry-After")

	// Check a value set
	retryDelay = strings.TrimSpace(retryDelay)
	if retryDelay != "" {
		const maxRetryAfterSeconds = math.MaxInt64 / int64(time.Second)

		seconds, err := strconv.ParseInt(retryDelay, 10, 64)
		if err == nil {
			if seconds <= 0 || seconds > maxRetryAfterSeconds {
				return 0
			}

			return time.Duration(seconds) * time.Second
		}

		if retryAt, err := http.ParseTime(retryDelay); err == nil {
			delay := time.Until(retryAt)

			if delay > 0 {
				return delay
			}
		}
	}

	return 0
}

func (h *HTTPActivity) validateHeaders(headers http.Header) error {
	for name, values := range headers {
		if !httpguts.ValidHeaderFieldName(name) {
			return temporalSDK.NewNonRetryableApplicationError(
				"invalid HTTP header name",
				ErrorTypeHTTPRequest,
				nil,
			)
		}

		for _, value := range values {
			if !httpguts.ValidHeaderFieldValue(value) {
				return temporalSDK.NewNonRetryableApplicationError(
					fmt.Sprintf("invalid value for HTTP header %q", name),
					ErrorTypeHTTPRequest,
					nil,
				)
			}
		}
	}

	return nil
}

func (h *HTTPActivity) validateURL(req *http.Request) error {
	if (req.URL.Scheme != "http" && req.URL.Scheme != "https") ||
		req.URL.Hostname() == "" ||
		req.URL.Opaque != "" {
		return temporalSDK.NewNonRetryableApplicationError(
			"URL must be an absolute HTTP or HTTPS URL with a host",
			ErrorTypeHTTPRequest,
			nil,
		)
	}

	return nil
}

// NewHTTPActivity returns an HTTPActivity configured with opts.
//
// The defaults are a five-second client timeout, Go's default transport and
// redirect policy, no cookie jar, a 1 MiB response-body limit and a 4 KiB
// error-body preview.
//
// Options apply in order, so a later option overrides an earlier option that
// sets the same value. NewHTTPActivity returns an error if any option is nil
// or returns an error.
func NewHTTPActivity(opts ...HTTPOption) (*HTTPActivity, error) {
	// Create the HTTPActivity with sensible defaults
	h := &HTTPActivity{
		client: &http.Client{
			Timeout:   5 * time.Second,
			Transport: http.DefaultTransport,
		},
		maxBodyBytes:    1 << 20, // 1 MiB default; configurable for larger payloads.
		maxPreviewLimit: 4 << 10, // 4 KiB
	}

	for i, opt := range opts {
		if opt == nil {
			return nil, fmt.Errorf("http option %d is nil", i)
		}

		if err := opt(h); err != nil {
			return nil, fmt.Errorf("error setting http option: %w", err)
		}
	}

	return h, nil
}

// HTTPOption configures an HTTPActivity during construction.
type HTTPOption func(*HTTPActivity) error

// WithCookieJar sets the cookie jar used by the HTTP client. A nil jar
// disables the cookie jar, which is the default.
//
// The jar is shared by every call made through the HTTPActivity. It is not
// persisted by Temporal, and activity attempts may run on different workers.
// The jar must support concurrent use and must not be reconfigured while calls
// are running.
func WithCookieJar(jar http.CookieJar) HTTPOption {
	return func(h *HTTPActivity) error {
		h.client.Jar = jar
		return nil
	}
}

// WithMaxBodySize sets the maximum size, in bytes, of a 2xx response body. The
// default is 1 MiB. Larger bodies fail with a non-retryable
// ErrorTypeHTTPResponseTooLarge error. The error-body preview is limited to
// 4 KiB, or to this size if it is smaller.
//
// The limit excludes headers and serialisation overhead and does not guarantee
// that the result fits Temporal's payload limits. Larger limits are supported,
// including when external storage is configured separately.
//
// The option returns an error unless maxBodyBytes is greater than zero and
// less than math.MaxInt64.
func WithMaxBodySize(maxBodyBytes int64) HTTPOption {
	return func(h *HTTPActivity) error {
		if maxBodyBytes <= 0 || maxBodyBytes == math.MaxInt64 {
			return fmt.Errorf(
				"maximum body size must be positive and below %d: %d",
				int64(math.MaxInt64),
				maxBodyBytes,
			)
		}

		h.maxBodyBytes = maxBodyBytes
		return nil
	}
}

// WithNoRedirect stops the HTTP client following redirects. 3xx responses are
// then returned to Call, which the current status policy treats as
// non-retryable ErrorTypeHTTPStatus errors.
func WithNoRedirect() HTTPOption {
	return func(h *HTTPActivity) error {
		h.client.CheckRedirect = func(_ *http.Request, _ []*http.Request) error {
			return http.ErrUseLastResponse
		}
		return nil
	}
}

// WithTimeout sets the HTTP client timeout, which covers the whole request
// including reading the response body. The default is five seconds. A zero
// timeout disables the client timeout. The request context, including any
// Temporal activity timeouts, still applies. The option returns an error if
// timeout is negative.
func WithTimeout(timeout time.Duration) HTTPOption {
	return func(h *HTTPActivity) error {
		if timeout < 0 {
			return fmt.Errorf("timeout cannot be below 0: %d", timeout)
		}

		h.client.Timeout = timeout
		return nil
	}
}

// WithTransport sets the transport used to make requests. A nil transport
// selects Go's default transport. The transport must support concurrent use
// and must not be reconfigured while calls are running.
func WithTransport(transport http.RoundTripper) HTTPOption {
	return func(h *HTTPActivity) error {
		h.client.Transport = transport
		return nil
	}
}
