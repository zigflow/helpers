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
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/gogo/protobuf/proto"
	"github.com/google/uuid"
	goredis "github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/sdk/converter"
)

// hookTimeout bounds the waits inside the ordering hook so a driver that never
// issues the expected number of concurrent commands fails the test instead of
// hanging it.
const hookTimeout = 30 * time.Second

// testKeyPrefix is a non-default key prefix, used to prove a configured prefix
// reaches the Redis keys rather than the default being applied regardless.
const testKeyPrefix = "tenant-a:payloads"

// newServer starts an in-process Redis server. It implements the real Redis
// command semantics the driver depends on (GET, SET, key expiry), so the tests
// exercise actual Redis behaviour without requiring a Redis instance or Docker.
func newServer(t *testing.T) *miniredis.Miniredis {
	t.Helper()

	return miniredis.RunT(t)
}

// newClient connects to server. The pool is sized generously because several
// tests deliberately hold multiple commands in flight at once. Each mutator is
// applied to the options before the client is built.
func newClient(
	t *testing.T,
	server *miniredis.Miniredis,
	mutators ...func(*goredis.UniversalOptions),
) goredis.UniversalClient {
	t.Helper()

	opts := &goredis.UniversalOptions{
		Addrs:    []string{server.Addr()},
		PoolSize: 32,
	}
	for _, mutate := range mutators {
		mutate(opts)
	}

	client := goredis.NewUniversalClient(opts)
	t.Cleanup(func() { _ = client.Close() })

	return client
}

// newDriver builds a driver against server. opts is mutated to carry the
// client, so callers only specify the options under test.
func newDriver(t *testing.T, server *miniredis.Miniredis, opts *Options) converter.StorageDriver {
	t.Helper()

	if opts == nil {
		opts = &Options{}
	}
	opts.Client = newClient(t, server)

	driver, err := New(opts)
	require.NoError(t, err)
	require.NotNil(t, driver)

	return driver
}

// noRetries stops go-redis from retrying a command, so the tests that stop the
// server observe the failure immediately instead of waiting out the backoff.
func noRetries(opts *goredis.UniversalOptions) {
	opts.MaxRetries = -1
	opts.DialTimeout = 500 * time.Millisecond
}

// storeCtx and retrieveCtx build the driver call contexts. Only the embedded
// context is used by the driver, so Target is left unset.
func storeCtx(t *testing.T) converter.StorageDriverStoreContext {
	t.Helper()

	return converter.StorageDriverStoreContext{Context: t.Context()}
}

func retrieveCtx(t *testing.T) converter.StorageDriverRetrieveContext {
	t.Helper()

	return converter.StorageDriverRetrieveContext{Context: t.Context()}
}

// newPayload returns a payload whose data identifies it, so round trips can be
// matched back to their input position.
func newPayload(body string) *commonpb.Payload {
	return &commonpb.Payload{
		Metadata: map[string][]byte{"encoding": []byte("json/plain")},
		Data:     []byte(body),
	}
}

// newPayloads returns count payloads bodied "payload-0", "payload-1", and so on.
func newPayloads(count int) []*commonpb.Payload {
	payloads := make([]*commonpb.Payload, count)
	for i := range payloads {
		payloads[i] = newPayload(fmt.Sprintf("payload-%d", i))
	}

	return payloads
}

// claimKeys returns the Redis key carried by each claim, in claim order.
func claimKeys(t *testing.T, claims []converter.StorageDriverClaim) []string {
	t.Helper()

	keys := make([]string, len(claims))
	for i, claim := range claims {
		key, ok := claim.ClaimData[claimDataKey]
		require.Truef(t, ok, "claim %d is missing the %q entry", i, claimDataKey)
		keys[i] = key
	}

	return keys
}

// payloadBodies returns the data of each payload as a string, in payload order.
func payloadBodies(t *testing.T, payloads []*commonpb.Payload) []string {
	t.Helper()

	bodies := make([]string, len(payloads))
	for i, payload := range payloads {
		require.NotNilf(t, payload, "payload %d is nil", i)
		bodies[i] = string(payload.GetData())
	}

	return bodies
}

// ---- New ----

func TestNewRejectsNilClient(t *testing.T) {
	driver, err := New(&Options{})
	require.Error(t, err)
	assert.ErrorContains(t, err, "client is required")
	assert.Nil(t, driver)
}

func TestNewRejectsNilOptions(t *testing.T) {
	// Nil options are normalised to empty options, which still have no client,
	// so construction must fail rather than return an unusable driver.
	driver, err := New(nil)
	require.Error(t, err)
	assert.ErrorContains(t, err, "client is required")
	assert.Nil(t, driver)
}

func TestNewDriverName(t *testing.T) {
	tests := []struct {
		name  string
		given string
		want  string
	}{
		{
			name: "empty driver name falls back to the default",
			want: defaultDriverName,
		},
		{
			name:  "custom driver name is used verbatim",
			given: "payloads.eu-west-1",
			want:  "payloads.eu-west-1",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			server := newServer(t)
			driver := newDriver(t, server, &Options{DriverName: test.given})

			assert.Equal(t, test.want, driver.Name())
			// The type is fixed regardless of the instance name, so several
			// instances of this driver can be registered side by side.
			assert.Equal(t, driverType, driver.Type())
		})
	}
}

func TestNewKeyPrefix(t *testing.T) {
	tests := []struct {
		name  string
		given string
		want  string
	}{
		{
			name: "empty key prefix falls back to the default",
			want: defaultKeyPrefix,
		},
		{
			name:  "custom key prefix is used verbatim",
			given: testKeyPrefix,
			want:  testKeyPrefix,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			server := newServer(t)
			driver := newDriver(t, server, &Options{KeyPrefix: test.given})

			internal, ok := driver.(*redisDriver)
			require.True(t, ok, "New must return the Redis driver implementation")
			assert.Equal(t, test.want, internal.keyPrefix)
		})
	}
}

func TestNewTTL(t *testing.T) {
	tests := []struct {
		name    string
		ttl     time.Duration
		wantErr bool
	}{
		{
			name: "zero TTL is accepted and means no expiry",
			ttl:  0,
		},
		{
			name: "positive TTL is accepted",
			ttl:  time.Hour,
		},
		{
			// A negative TTL cannot express a valid expiry, and go-redis would
			// forward it to Redis as an immediate expiry, silently discarding
			// every payload the moment it is stored. It must be rejected up
			// front rather than producing claims that can never be retrieved.
			name:    "negative TTL is rejected",
			ttl:     -time.Second,
			wantErr: true,
		},
		{
			name:    "smallest negative TTL is rejected",
			ttl:     -time.Nanosecond,
			wantErr: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			server := newServer(t)

			driver, err := New(&Options{Client: newClient(t, server), TTL: test.ttl})
			if test.wantErr {
				require.Error(t, err)
				assert.ErrorContains(t, err, "TTL must not be negative")
				assert.Nil(t, driver)

				return
			}

			require.NoError(t, err)
			require.NotNil(t, driver)

			internal, ok := driver.(*redisDriver)
			require.True(t, ok, "New must return the Redis driver implementation")
			assert.Equal(t, test.ttl, internal.ttl)
		})
	}
}

// ---- Name and Type ----

func TestName(t *testing.T) {
	server := newServer(t)

	assert.Equal(t, defaultDriverName, newDriver(t, server, nil).Name())
	assert.Equal(
		t,
		"custom.redis.driver",
		newDriver(t, server, &Options{DriverName: "custom.redis.driver"}).Name(),
	)
}

func TestType(t *testing.T) {
	server := newServer(t)

	// Type must not vary with configuration, so a custom name and prefix still
	// report the Redis driver type.
	driver := newDriver(t, server, &Options{DriverName: "custom", KeyPrefix: "custom"})
	assert.Equal(t, "redis.driver", driver.Type())
	assert.Equal(t, driverType, driver.Type())
}

// ---- Store and Retrieve round trips ----

func TestStoreRetrieveSinglePayload(t *testing.T) {
	server := newServer(t)
	driver := newDriver(t, server, nil)

	payload := newPayload(`{"hello":"world"}`)

	claims, err := driver.Store(storeCtx(t), []*commonpb.Payload{payload})
	require.NoError(t, err)
	require.Len(t, claims, 1)

	// Exactly one key is written, and the claim points at it.
	keys := claimKeys(t, claims)
	assert.Equal(t, keys, server.Keys())

	retrieved, err := driver.Retrieve(retrieveCtx(t), claims)
	require.NoError(t, err)
	require.Len(t, retrieved, 1)

	// Both the data and the metadata survive the round trip, because the
	// metadata carries the encoding the data converter needs to decode it.
	assert.Equal(t, payload.GetData(), retrieved[0].GetData())
	assert.Equal(t, payload.GetMetadata(), retrieved[0].GetMetadata())
}

func TestStoreRetrieveMultiplePayloads(t *testing.T) {
	server := newServer(t)
	driver := newDriver(t, server, nil)

	payloads := newPayloads(5)

	claims, err := driver.Store(storeCtx(t), payloads)
	require.NoError(t, err)
	require.Len(t, claims, len(payloads))

	// Every payload gets its own key, so no payload can overwrite another.
	keys := claimKeys(t, claims)
	assert.Len(t, server.Keys(), len(payloads))
	assert.ElementsMatch(t, keys, server.Keys())
	assert.Len(t, uniqueStrings(keys), len(keys), "claim keys must be unique")

	retrieved, err := driver.Retrieve(retrieveCtx(t), claims)
	require.NoError(t, err)
	require.Len(t, retrieved, len(payloads))
	assert.Equal(t, payloadBodies(t, payloads), payloadBodies(t, retrieved))
}

func TestStoreIdenticalPayloadsGetDistinctKeys(t *testing.T) {
	server := newServer(t)
	driver := newDriver(t, server, nil)

	// Identical payloads must not be deduplicated onto a single key: the SDK
	// holds one claim per payload and may expire them independently.
	payloads := []*commonpb.Payload{newPayload("same"), newPayload("same")}

	claims, err := driver.Store(storeCtx(t), payloads)
	require.NoError(t, err)
	require.Len(t, claims, 2)

	keys := claimKeys(t, claims)
	assert.NotEqual(t, keys[0], keys[1])
	assert.Len(t, server.Keys(), 2)
}

func TestRetrieveFollowsClaimOrderNotStoreOrder(t *testing.T) {
	server := newServer(t)
	driver := newDriver(t, server, nil)

	payloads := newPayloads(4)

	claims, err := driver.Store(storeCtx(t), payloads)
	require.NoError(t, err)

	// Retrieve must answer in the order it was asked, so reversing the claims
	// reverses the payloads.
	reversed := make([]converter.StorageDriverClaim, len(claims))
	for i, claim := range claims {
		reversed[len(claims)-1-i] = claim
	}

	retrieved, err := driver.Retrieve(retrieveCtx(t), reversed)
	require.NoError(t, err)
	assert.Equal(
		t,
		[]string{"payload-3", "payload-2", "payload-1", "payload-0"},
		payloadBodies(t, retrieved),
	)
}

func TestStoreEmptyPayloads(t *testing.T) {
	server := newServer(t)
	driver := newDriver(t, server, nil)

	claims, err := driver.Store(storeCtx(t), nil)
	require.NoError(t, err)
	assert.Empty(t, claims)
	assert.Empty(t, server.Keys(), "storing nothing must not write to Redis")
}

func TestRetrieveEmptyClaims(t *testing.T) {
	server := newServer(t)
	driver := newDriver(t, server, nil)

	payloads, err := driver.Retrieve(retrieveCtx(t), nil)
	require.NoError(t, err)
	assert.Empty(t, payloads)
}

// ---- Key naming ----

func TestStoreKeyPrefix(t *testing.T) {
	tests := []struct {
		name       string
		keyPrefix  string
		wantPrefix string
	}{
		{
			name:       "default prefix is applied",
			wantPrefix: defaultKeyPrefix + ":",
		},
		{
			name:       "custom prefix is applied",
			keyPrefix:  testKeyPrefix,
			wantPrefix: testKeyPrefix + ":",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			server := newServer(t)
			driver := newDriver(t, server, &Options{KeyPrefix: test.keyPrefix})

			claims, err := driver.Store(storeCtx(t), newPayloads(3))
			require.NoError(t, err)

			for _, key := range claimKeys(t, claims) {
				assert.True(
					t,
					strings.HasPrefix(key, test.wantPrefix),
					"key %q must start with %q",
					key,
					test.wantPrefix,
				)

				// The remainder is a UUID, which is what keeps concurrently
				// stored payloads from colliding.
				_, err := uuid.Parse(strings.TrimPrefix(key, test.wantPrefix))
				assert.NoErrorf(t, err, "key %q must end in a UUID", key)
			}
		})
	}
}

// ---- TTL behaviour ----

func TestStoreAppliesTTL(t *testing.T) {
	server := newServer(t)
	const ttl = 90 * time.Second
	driver := newDriver(t, server, &Options{TTL: ttl})

	claims, err := driver.Store(storeCtx(t), newPayloads(2))
	require.NoError(t, err)

	for _, key := range claimKeys(t, claims) {
		// The in-process server only advances time when told to, so the
		// remaining TTL is compared with a tolerance rather than slept on.
		assert.InDelta(
			t,
			float64(ttl),
			float64(server.TTL(key)),
			float64(5*time.Second),
			"key %q must expire after roughly the configured TTL",
			key,
		)
	}

	// Once the TTL has elapsed the keys are gone and the claims no longer
	// resolve, which is the behaviour a TTL is configured for.
	server.FastForward(ttl + time.Second)
	assert.Empty(t, server.Keys())

	payloads, err := driver.Retrieve(retrieveCtx(t), claims)
	require.Error(t, err)
	assert.Nil(t, payloads)
}

func TestStoreZeroTTLIsPersistent(t *testing.T) {
	server := newServer(t)
	driver := newDriver(t, server, &Options{TTL: 0})

	claims, err := driver.Store(storeCtx(t), newPayloads(2))
	require.NoError(t, err)

	keys := claimKeys(t, claims)
	for _, key := range keys {
		// A zero TTL means no expiry at all, reported as a zero remaining TTL.
		assert.Zero(t, server.TTL(key), "key %q must not have an expiry", key)
	}

	// Fast forwarding well past any plausible expiry leaves the keys intact.
	server.FastForward(24 * time.Hour)
	assert.ElementsMatch(t, keys, server.Keys())

	retrieved, err := driver.Retrieve(retrieveCtx(t), claims)
	require.NoError(t, err)
	assert.Equal(t, []string{"payload-0", "payload-1"}, payloadBodies(t, retrieved))
}

// ---- Claim validation ----

func TestRetrieveInvalidClaims(t *testing.T) {
	tests := []struct {
		name  string
		claim converter.StorageDriverClaim
	}{
		{
			name:  "missing key entry",
			claim: converter.StorageDriverClaim{ClaimData: map[string]string{"bucket": "zigflow"}},
		},
		{
			name:  "empty key entry",
			claim: converter.StorageDriverClaim{ClaimData: map[string]string{claimDataKey: ""}},
		},
		{
			name:  "empty claim data",
			claim: converter.StorageDriverClaim{ClaimData: map[string]string{}},
		},
		{
			name:  "nil claim data",
			claim: converter.StorageDriverClaim{},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			server := newServer(t)
			driver := newDriver(t, server, nil)

			payloads, err := driver.Retrieve(retrieveCtx(t), []converter.StorageDriverClaim{test.claim})
			require.Error(t, err)
			// The message names the entry the claim should have carried, so the
			// cause is actionable.
			assert.ErrorContains(t, err, fmt.Sprintf("redis claim is missing %q", claimDataKey))
			assert.Nil(t, payloads)
		})
	}
}

func TestRetrieveRejectsWholeBatchOnOneBadClaim(t *testing.T) {
	server := newServer(t)
	driver := newDriver(t, server, nil)

	claims, err := driver.Store(storeCtx(t), newPayloads(3))
	require.NoError(t, err)

	// A partially decodable batch is useless to the SDK, so one unusable claim
	// must fail the call rather than yield a slice with a nil hole in it.
	claims[1] = converter.StorageDriverClaim{}

	payloads, err := driver.Retrieve(retrieveCtx(t), claims)
	require.Error(t, err)
	assert.Nil(t, payloads)
}

// ---- Redis-side failures ----

func TestRetrieveKeyNotFound(t *testing.T) {
	server := newServer(t)
	driver := newDriver(t, server, nil)

	const missing = defaultKeyPrefix + ":00000000-0000-0000-0000-000000000000"

	payloads, err := driver.Retrieve(retrieveCtx(t), []converter.StorageDriverClaim{
		{ClaimData: map[string]string{claimDataKey: missing}},
	})
	require.Error(t, err)
	assert.Nil(t, payloads)

	// A missing key is its own condition: the payload has expired or was never
	// stored, which is not the same as Redis being unreachable. The message
	// quotes the key and must not be reported as a read failure.
	assert.ErrorContains(t, err, fmt.Sprintf("redis claim %q", missing))
	assert.NotContains(t, err.Error(), "error reading redis data")
	// The underlying go-redis sentinel is deliberately not wrapped, so callers
	// cannot mistake a missing payload for a transport error.
	assert.NotErrorIs(t, err, goredis.Nil)
}

func TestRetrieveExpiredKeyIsNotFound(t *testing.T) {
	server := newServer(t)
	driver := newDriver(t, server, &Options{TTL: time.Minute})

	claims, err := driver.Store(storeCtx(t), newPayloads(1))
	require.NoError(t, err)

	server.FastForward(2 * time.Minute)

	payloads, err := driver.Retrieve(retrieveCtx(t), claims)
	require.Error(t, err)
	assert.Nil(t, payloads)
	assert.ErrorContains(t, err, fmt.Sprintf("redis claim %q", claimKeys(t, claims)[0]))
}

func TestRetrieveMalformedPayload(t *testing.T) {
	server := newServer(t)
	driver := newDriver(t, server, nil)

	const key = defaultKeyPrefix + ":malformed"
	// Data that is not a protobuf-encoded payload at all, as would happen if
	// the key were written by something other than this driver.
	require.NoError(t, server.Set(key, string([]byte{0xff, 0xff, 0xff, 0xff})))

	payloads, err := driver.Retrieve(retrieveCtx(t), []converter.StorageDriverClaim{
		{ClaimData: map[string]string{claimDataKey: key}},
	})
	require.Error(t, err)
	assert.ErrorContains(t, err, "error unmarshalling payload")
	assert.Nil(t, payloads)
}

func TestRetrievePropagatesGetFailure(t *testing.T) {
	server := newServer(t)
	client := newClient(t, server)

	driver, err := New(&Options{Client: client})
	require.NoError(t, err)

	claims, err := driver.Store(storeCtx(t), newPayloads(2))
	require.NoError(t, err)

	// The hook is added only after the payloads are stored, so the failure is
	// isolated to the read path.
	client.AddHook(failHook{command: "get", err: errBackend})

	payloads, err := driver.Retrieve(retrieveCtx(t), claims)
	require.Error(t, err)
	assert.Nil(t, payloads)
	// A transport failure is reported as a read error and keeps the cause, so
	// it is distinguishable from a payload that is simply not there.
	assert.ErrorContains(t, err, "error reading redis data")
	assert.ErrorIs(t, err, errBackend)
}

func TestStorePropagatesSetFailure(t *testing.T) {
	server := newServer(t)
	client := newClient(t, server)
	client.AddHook(failHook{command: "set", err: errBackend})

	driver, err := New(&Options{Client: client})
	require.NoError(t, err)

	claims, err := driver.Store(storeCtx(t), newPayloads(3))
	require.Error(t, err)
	assert.Nil(t, claims, "a failed store must not return partially filled claims")
	assert.ErrorContains(t, err, "error writing to redis")
	assert.ErrorIs(t, err, errBackend)
	assert.Empty(t, server.Keys())
}

func TestStorePropagatesUnreachableRedis(t *testing.T) {
	server := newServer(t)

	driver, err := New(&Options{Client: newClient(t, server, noRetries)})
	require.NoError(t, err)

	// Stopping the server produces a genuine connection failure rather than an
	// injected one, proving the error is surfaced and not retried forever.
	server.Close()

	claims, err := driver.Store(storeCtx(t), newPayloads(1))
	require.Error(t, err)
	assert.ErrorContains(t, err, "error writing to redis")
	assert.Nil(t, claims)
}

func TestRetrievePropagatesUnreachableRedis(t *testing.T) {
	server := newServer(t)

	driver, err := New(&Options{Client: newClient(t, server, noRetries)})
	require.NoError(t, err)

	claims, err := driver.Store(storeCtx(t), newPayloads(1))
	require.NoError(t, err)

	server.Close()

	payloads, err := driver.Retrieve(retrieveCtx(t), claims)
	require.Error(t, err)
	assert.ErrorContains(t, err, "error reading redis data")
	assert.Nil(t, payloads)
}

// ---- Ordering under concurrency ----

func TestStoreAndRetrievePreserveOrderUnderConcurrency(t *testing.T) {
	const total = 5

	server := newServer(t)
	client := newClient(t, server)

	driver, err := New(&Options{Client: client})
	require.NoError(t, err)

	payloads := newPayloads(total)

	// Force the SET commands to complete in the exact reverse of the input
	// order. If Store built its claims from completion order the assertion
	// below would fail on every run rather than occasionally.
	storeOrder := newOrderHook(t, total, storeCommandIndex(t))
	client.AddHook(storeOrder)

	claims, err := driver.Store(storeCtx(t), payloads)
	require.NoError(t, err)
	require.Len(t, claims, total)

	assert.Equal(
		t,
		[]int{4, 3, 2, 1, 0},
		storeOrder.completions(),
		"the hook must have completed the stores in reverse input order",
	)

	keys := claimKeys(t, claims)
	// Claim i must address the payload that was at position i of the input.
	for i, key := range keys {
		stored, err := server.Get(key)
		require.NoErrorf(t, err, "key %q from claim %d was not stored", key, i)

		decoded := &commonpb.Payload{}
		require.NoError(t, proto.Unmarshal([]byte(stored), decoded))
		assert.Equalf(
			t,
			fmt.Sprintf("payload-%d", i),
			string(decoded.GetData()),
			"claim %d addresses the wrong payload",
			i,
		)
	}

	// Now do the same on the read path: the GETs complete in reverse claim
	// order, and the payloads must still come back in claim order.
	retrieveOrder := newOrderHook(t, total, keyIndex(keys))
	client.AddHook(retrieveOrder)

	retrieved, err := driver.Retrieve(retrieveCtx(t), claims)
	require.NoError(t, err)

	assert.Equal(
		t,
		[]int{4, 3, 2, 1, 0},
		retrieveOrder.completions(),
		"the hook must have completed the retrievals in reverse claim order",
	)
	assert.Equal(t, payloadBodies(t, payloads), payloadBodies(t, retrieved))
}

// ---- Test doubles ----

// errBackend stands in for a Redis-side or transport failure. It is a distinct
// value so tests can assert the driver wraps the cause rather than replacing it.
var errBackend = fmt.Errorf("backend unavailable")

// failHook makes one Redis command fail while leaving the rest of the client
// working, which is how the tests reach the driver's error branches without
// needing a broken Redis instance.
type failHook struct {
	command string
	err     error
}

var _ goredis.Hook = failHook{}

func (failHook) DialHook(next goredis.DialHook) goredis.DialHook { return next }

func (h failHook) ProcessHook(next goredis.ProcessHook) goredis.ProcessHook {
	return func(ctx context.Context, cmd goredis.Cmder) error {
		if cmd.Name() == h.command {
			cmd.SetErr(h.err)

			return h.err
		}

		return next(ctx, cmd)
	}
}

func (h failHook) ProcessPipelineHook(next goredis.ProcessPipelineHook) goredis.ProcessPipelineHook {
	return next
}

// orderHook holds every command it recognises until all of them have arrived,
// then releases them so that they complete in reverse index order. That makes
// the ordering assertions deterministic: the driver's concurrent operations
// always finish in the opposite order to its input.
type orderHook struct {
	t     *testing.T
	total int
	// index maps a Redis command to the input slice position it belongs to.
	// Commands it does not recognise pass straight through.
	index func(goredis.Cmder) (int, bool)

	mu         sync.Mutex
	arrived    int
	allArrived chan struct{}
	// done[i] is closed once the command for index i has completed, which is
	// what index i-1 waits for.
	done []chan struct{}

	completionMu    sync.Mutex
	completionOrder []int
}

var _ goredis.Hook = (*orderHook)(nil)

func newOrderHook(t *testing.T, total int, index func(goredis.Cmder) (int, bool)) *orderHook {
	t.Helper()

	hook := &orderHook{
		t:          t,
		total:      total,
		index:      index,
		allArrived: make(chan struct{}),
		done:       make([]chan struct{}, total),
	}
	for i := range hook.done {
		hook.done[i] = make(chan struct{})
	}

	return hook
}

func (*orderHook) DialHook(next goredis.DialHook) goredis.DialHook { return next }

func (h *orderHook) ProcessHook(next goredis.ProcessHook) goredis.ProcessHook {
	return func(ctx context.Context, cmd goredis.Cmder) error {
		i, ok := h.index(cmd)
		if !ok {
			return next(ctx, cmd)
		}

		// Waiting for every command first proves they really are in flight
		// together, so the reversal below is not just sequential execution.
		if !h.waitForAll() {
			return next(ctx, cmd)
		}

		// Each command waits for its successor, so the last index completes
		// first and the first index completes last.
		if i < h.total-1 && !h.wait(h.done[i+1]) {
			return next(ctx, cmd)
		}

		err := next(ctx, cmd)

		h.completionMu.Lock()
		h.completionOrder = append(h.completionOrder, i)
		h.completionMu.Unlock()

		close(h.done[i])

		return err
	}
}

func (*orderHook) ProcessPipelineHook(next goredis.ProcessPipelineHook) goredis.ProcessPipelineHook {
	return next
}

// waitForAll blocks until total recognised commands are in flight. It reports
// false, and fails the test, if that never happens.
func (h *orderHook) waitForAll() bool {
	h.mu.Lock()
	h.arrived++
	if h.arrived == h.total {
		close(h.allArrived)
	}
	h.mu.Unlock()

	return h.wait(h.allArrived)
}

func (h *orderHook) wait(ch <-chan struct{}) bool {
	select {
	case <-ch:
		return true
	case <-time.After(hookTimeout):
		h.t.Errorf("timed out waiting for %d concurrent redis commands", h.total)

		return false
	}
}

// completions returns the input positions in the order their commands finished.
func (h *orderHook) completions() []int {
	h.completionMu.Lock()
	defer h.completionMu.Unlock()

	return append([]int(nil), h.completionOrder...)
}

// storeCommandIndex recognises the driver's SET commands and reads the input
// position back out of the payload being written.
func storeCommandIndex(t *testing.T) func(goredis.Cmder) (int, bool) {
	t.Helper()

	return func(cmd goredis.Cmder) (int, bool) {
		args := cmd.Args()
		if cmd.Name() != "set" || len(args) < 3 {
			return 0, false
		}

		var data []byte
		switch value := args[2].(type) {
		case []byte:
			data = value
		case string:
			data = []byte(value)
		default:
			return 0, false
		}

		payload := &commonpb.Payload{}
		if err := proto.Unmarshal(data, payload); err != nil {
			return 0, false
		}

		var i int
		if _, err := fmt.Sscanf(string(payload.GetData()), "payload-%d", &i); err != nil {
			return 0, false
		}

		return i, true
	}
}

// keyIndex recognises the driver's GET commands by key and maps each key back
// to its position in keys.
func keyIndex(keys []string) func(goredis.Cmder) (int, bool) {
	positions := make(map[string]int, len(keys))
	for i, key := range keys {
		positions[key] = i
	}

	return func(cmd goredis.Cmder) (int, bool) {
		args := cmd.Args()
		if cmd.Name() != "get" || len(args) < 2 {
			return 0, false
		}

		key, ok := args[1].(string)
		if !ok {
			return 0, false
		}

		i, ok := positions[key]

		return i, ok
	}
}

// uniqueStrings returns the distinct values in values.
func uniqueStrings(values []string) []string {
	seen := make(map[string]struct{}, len(values))
	unique := make([]string, 0, len(values))
	for _, value := range values {
		if _, ok := seen[value]; ok {
			continue
		}
		seen[value] = struct{}{}
		unique = append(unique, value)
	}

	return unique
}
