package storage

import (
	"context"
	"io"
)

type Store interface {
	PutBytes(ctx context.Context, key string, data []byte) (ObjectInfo, error)
	GetBytes(ctx context.Context, key string) ([]byte, error)
	GetReader(ctx context.Context, key string) (io.ReadCloser, error)
	Head(ctx context.Context, key string) (ObjectInfo, error)
	Delete(ctx context.Context, key string) error
}
