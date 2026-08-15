package storage

import (
	"context"
	"fmt"
	"io"
	"sync"
)

// FaultStore wraps another Store and injects deterministic failures for tests.
// Failure points are 1-based call numbers; a value <= 0 disables that failure.
type FaultStore struct {
	Base Store

	PutError error
	PutAt    int64

	GetError error
	GetAt    int64

	HeadError error
	HeadAt    int64

	DeleteError error
	DeleteAt    int64

	mu          sync.Mutex
	putCalls    int64
	getCalls    int64
	headCalls   int64
	deleteCalls int64
}

func NewFaultStore(base Store) *FaultStore {
	return &FaultStore{Base: base}
}

func (s *FaultStore) PutBytes(ctx context.Context, key string, data []byte) (ObjectInfo, error) {
	if err := s.nextError(&s.putCalls, s.PutAt, s.PutError, "put"); err != nil {
		return ObjectInfo{}, err
	}
	return s.base().PutBytes(ctx, key, data)
}

func (s *FaultStore) GetBytes(ctx context.Context, key string) ([]byte, error) {
	if err := s.nextError(&s.getCalls, s.GetAt, s.GetError, "get"); err != nil {
		return nil, err
	}
	return s.base().GetBytes(ctx, key)
}

func (s *FaultStore) GetReader(ctx context.Context, key string) (io.ReadCloser, error) {
	if err := s.nextError(&s.getCalls, s.GetAt, s.GetError, "get"); err != nil {
		return nil, err
	}
	return s.base().GetReader(ctx, key)
}

func (s *FaultStore) Head(ctx context.Context, key string) (ObjectInfo, error) {
	if err := s.nextError(&s.headCalls, s.HeadAt, s.HeadError, "head"); err != nil {
		return ObjectInfo{}, err
	}
	return s.base().Head(ctx, key)
}

func (s *FaultStore) Delete(ctx context.Context, key string) error {
	if err := s.nextError(&s.deleteCalls, s.DeleteAt, s.DeleteError, "delete"); err != nil {
		return err
	}
	return s.base().Delete(ctx, key)
}

func (s *FaultStore) base() Store {
	if s.Base == nil {
		panic("storage.FaultStore requires Base")
	}
	return s.Base
}

func (s *FaultStore) nextError(counter *int64, failAt int64, configured error, op string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	*counter = *counter + 1
	if failAt <= 0 || *counter != failAt {
		return nil
	}
	if configured != nil {
		return configured
	}
	return fmt.Errorf("injected %s failure", op)
}

var _ Store = (*FaultStore)(nil)
