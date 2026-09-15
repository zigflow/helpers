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
	"errors"
	"fmt"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/google/uuid"
	goredis "github.com/redis/go-redis/v9"
	"go.temporal.io/api/common/v1"
	"go.temporal.io/sdk/converter"
	"golang.org/x/sync/errgroup"
)

const (
	claimDataKey      = "key"
	driverType        = "redis.driver"
	defaultDriverName = "redis.driver"
	defaultKeyPrefix  = "zigflow:claim"
)

type Options struct {
	Client     goredis.UniversalClient
	DriverName string
	KeyPrefix  string
	TTL        time.Duration
}

type redisDriver struct {
	client     goredis.UniversalClient
	driverName string
	keyPrefix  string
	ttl        time.Duration
}

func (r *redisDriver) Name() string {
	return r.driverName
}

func (r *redisDriver) Retrieve(
	ctx converter.StorageDriverRetrieveContext,
	claims []converter.StorageDriverClaim,
) ([]*common.Payload, error) {
	payloads := make([]*common.Payload, len(claims))
	g, gctx := errgroup.WithContext(ctx.Context)
	for i, claim := range claims {
		g.Go(func() error {
			key, ok := claim.ClaimData[claimDataKey]
			if !ok || key == "" {
				return fmt.Errorf("redis claim is missing %q", claimDataKey)
			}

			data, err := r.client.Get(gctx, key).Bytes()
			if err != nil {
				if errors.Is(err, goredis.Nil) {
					return fmt.Errorf("redis claim %q does not exist", key)
				}

				return fmt.Errorf("error reading redis data: %w", err)
			}

			payload := &common.Payload{}
			if err := proto.Unmarshal(data, payload); err != nil {
				return fmt.Errorf("error unmarshalling payload: %w", err)
			}
			payloads[i] = payload

			return nil
		})
	}

	if err := g.Wait(); err != nil {
		return nil, err
	}

	return payloads, nil
}

func (r *redisDriver) Store(ctx converter.StorageDriverStoreContext, payloads []*common.Payload) ([]converter.StorageDriverClaim, error) {
	claims := make([]converter.StorageDriverClaim, len(payloads))
	g, gctx := errgroup.WithContext(ctx.Context)
	for i, payload := range payloads {
		g.Go(func() error {
			key := fmt.Sprintf("%s:%s", r.keyPrefix, uuid.NewString())
			data, err := proto.Marshal(payload)
			if err != nil {
				return fmt.Errorf("failed to marshal payload: %w", err)
			}

			if err := r.client.Set(gctx, key, data, r.ttl).Err(); err != nil {
				return fmt.Errorf("error writing to redis: %w", err)
			}

			claims[i] = converter.StorageDriverClaim{
				ClaimData: map[string]string{claimDataKey: key},
			}

			return nil
		})
	}

	if err := g.Wait(); err != nil {
		return nil, err
	}

	return claims, nil
}

func (r *redisDriver) Type() string {
	return driverType
}

var _ converter.StorageDriver = &redisDriver{}

func New(opts *Options) (converter.StorageDriver, error) {
	if opts == nil {
		opts = &Options{}
	}

	if opts.Client == nil {
		return nil, fmt.Errorf("client is required")
	}

	name := opts.DriverName
	if name == "" {
		name = defaultDriverName
	}

	keyPrefix := opts.KeyPrefix
	if keyPrefix == "" {
		keyPrefix = defaultKeyPrefix
	}

	if opts.TTL < 0 {
		return nil, fmt.Errorf("TTL must not be negative")
	}

	return &redisDriver{
		client:     opts.Client,
		driverName: name,
		keyPrefix:  keyPrefix,
		ttl:        opts.TTL,
	}, nil
}
