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

// This file contains the external storage helpers, which keep payloads too
// large to send to the Temporal server inline out of the workflow history by
// offloading them to Amazon S3.
//
// The three pieces fit together in one direction: S3Config describes the bucket
// and how to reach it, ExternalConfigS3Factory turns that into an
// ExternalConfigFactory which builds the s3driver storage driver when it is
// called, and ExternalConfig pairs that factory with the settings that apply
// whatever the backend. Pass the ExternalConfig to WithExternalStorageFactory to
// attach it to a connection.
//
// Bucket and Region are the only fields most deployments set. The credential
// fields are optional: when AccessKeyID and SecretAccessKey are empty the AWS
// SDK resolves credentials through its default chain, so an application running
// under an instance or workload identity needs none of them, and SessionToken
// may only be set alongside both of the others. Endpoint and UsePathStyle are
// for S3-compatible storage rather than S3 itself, and DriverName and
// MaxPayloadSize override the s3driver defaults.

import (
	"context"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"go.temporal.io/sdk/contrib/aws/s3driver"
	"go.temporal.io/sdk/contrib/aws/s3driver/awssdkv2"
	"go.temporal.io/sdk/converter"
)

type ExternalConfigFactory func() ([]converter.StorageDriver, error)

type ExternalConfig struct {
	Factory               ExternalConfigFactory
	PayloadSizeThreshold  int
	StorageDriverSelector converter.StorageDriverSelector
}

type S3Config struct {
	Bucket string
	Region string

	// These are all optional - ignored if empty
	DriverName      string
	MaxPayloadSize  int
	Endpoint        string
	AccessKeyID     string
	SecretAccessKey string
	SessionToken    string
	UsePathStyle    bool
}

func ExternalConfigS3Factory(ctx context.Context, cfg *S3Config) ExternalConfigFactory {
	return func() ([]converter.StorageDriver, error) {
		if cfg == nil {
			return nil, fmt.Errorf("s3factory: config missing as second argument")
		}
		if cfg.SessionToken != "" && (cfg.AccessKeyID == "" || cfg.SecretAccessKey == "") {
			return nil, fmt.Errorf("s3factory: SessionToken requires AccessKeyID and SecretAccessKey to also be set")
		}

		configOpts := []func(*config.LoadOptions) error{
			config.WithRegion(cfg.Region),
		}

		if cfg.AccessKeyID != "" && cfg.SecretAccessKey != "" {
			configOpts = append(configOpts, config.WithCredentialsProvider(
				credentials.NewStaticCredentialsProvider(
					cfg.AccessKeyID,
					cfg.SecretAccessKey,
					cfg.SessionToken, // fine to be empty
				),
			))
		}

		c, err := config.LoadDefaultConfig(ctx, configOpts...)
		if err != nil {
			return nil, err
		}

		opts := s3driver.Options{
			Client: awssdkv2.NewClient(s3.NewFromConfig(c, func(o *s3.Options) {
				if cfg.Endpoint != "" {
					o.BaseEndpoint = aws.String(cfg.Endpoint)
				}
				o.UsePathStyle = cfg.UsePathStyle
			})),
			Bucket:         s3driver.StaticBucket(cfg.Bucket),
			MaxPayloadSize: cfg.MaxPayloadSize, // Defaults to 50MiB
		}
		if d := cfg.DriverName; d != "" {
			opts.DriverName = d // Defaults to "aws.s3driver"
		}

		driver, err := s3driver.NewDriver(opts)
		if err != nil {
			return nil, err
		}

		return []converter.StorageDriver{driver}, nil
	}
}
