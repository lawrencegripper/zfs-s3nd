package storage

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

type S3Config struct {
	Endpoint        string
	Region          string
	Bucket          string
	AccessKeyID     string
	SecretAccessKey string
	ForcePathStyle  bool
}

type ObjectInfo struct {
	Key    string
	Size   int64
	SHA256 string
	ETag   string
}

type S3Store struct {
	bucket string
	client *s3.Client
}

func NewS3Store(ctx context.Context, cfg S3Config) (*S3Store, error) {
	if cfg.Bucket == "" {
		return nil, fmt.Errorf("s3 bucket is required")
	}
	if cfg.Region == "" {
		cfg.Region = "us-east-1"
	}

	loadOptions := []func(*config.LoadOptions) error{
		config.WithRegion(cfg.Region),
	}
	if cfg.AccessKeyID != "" || cfg.SecretAccessKey != "" {
		loadOptions = append(loadOptions, config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(cfg.AccessKeyID, cfg.SecretAccessKey, "")))
	}

	awsCfg, err := config.LoadDefaultConfig(ctx, loadOptions...)
	if err != nil {
		return nil, fmt.Errorf("load aws config: %w", err)
	}

	client := s3.NewFromConfig(awsCfg, func(o *s3.Options) {
		o.UsePathStyle = cfg.ForcePathStyle
		if cfg.Endpoint != "" {
			o.BaseEndpoint = aws.String(cfg.Endpoint)
		}
	})

	return &S3Store{bucket: cfg.Bucket, client: client}, nil
}

func (s *S3Store) PutBytes(ctx context.Context, key string, data []byte) (ObjectInfo, error) {
	sha := sha256.Sum256(data)
	shaHex := hex.EncodeToString(sha[:])
	_, err := s.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(key),
		Body:   bytes.NewReader(data),
		Metadata: map[string]string{
			"sha256": shaHex,
		},
	})
	if err != nil {
		return ObjectInfo{}, fmt.Errorf("put object %s: %w", key, err)
	}
	info, err := s.Head(ctx, key)
	if err != nil {
		return ObjectInfo{}, err
	}
	if info.Size != int64(len(data)) {
		return ObjectInfo{}, fmt.Errorf("object %s size mismatch: got %d want %d", key, info.Size, len(data))
	}
	if info.SHA256 != "" && info.SHA256 != shaHex {
		return ObjectInfo{}, fmt.Errorf("object %s sha256 metadata mismatch: got %s want %s", key, info.SHA256, shaHex)
	}
	info.SHA256 = shaHex
	return info, nil
}

func (s *S3Store) GetBytes(ctx context.Context, key string) ([]byte, error) {
	body, err := s.GetReader(ctx, key)
	if err != nil {
		return nil, err
	}
	defer body.Close()
	data, err := io.ReadAll(body)
	if err != nil {
		return nil, fmt.Errorf("read object %s: %w", key, err)
	}
	return data, nil
}

func (s *S3Store) GetReader(ctx context.Context, key string) (io.ReadCloser, error) {
	out, err := s.client.GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String(s.bucket), Key: aws.String(key)})
	if err != nil {
		return nil, fmt.Errorf("get object %s: %w", key, err)
	}
	return out.Body, nil
}

func (s *S3Store) Head(ctx context.Context, key string) (ObjectInfo, error) {
	out, err := s.client.HeadObject(ctx, &s3.HeadObjectInput{Bucket: aws.String(s.bucket), Key: aws.String(key)})
	if err != nil {
		return ObjectInfo{}, fmt.Errorf("head object %s: %w", key, err)
	}
	info := ObjectInfo{Key: key}
	if out.ContentLength != nil {
		info.Size = *out.ContentLength
	}
	if out.ETag != nil {
		info.ETag = *out.ETag
	}
	if out.Metadata != nil {
		info.SHA256 = out.Metadata["sha256"]
	}
	return info, nil
}

func (s *S3Store) Delete(ctx context.Context, key string) error {
	_, err := s.client.DeleteObject(ctx, &s3.DeleteObjectInput{Bucket: aws.String(s.bucket), Key: aws.String(key)})
	if err != nil {
		return fmt.Errorf("delete object %s: %w", key, err)
	}
	return nil
}
