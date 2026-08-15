package storage

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"sync"
)

type MemoryStore struct {
	mu      sync.Mutex
	objects map[string][]byte
}

func NewMemoryStore() *MemoryStore {
	return &MemoryStore{objects: make(map[string][]byte)}
}

func (s *MemoryStore) PutBytes(_ context.Context, key string, data []byte) (ObjectInfo, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	copyData := append([]byte(nil), data...)
	s.objects[key] = copyData
	sha := sha256.Sum256(copyData)
	return ObjectInfo{Key: key, Size: int64(len(copyData)), SHA256: hex.EncodeToString(sha[:])}, nil
}

func (s *MemoryStore) GetBytes(_ context.Context, key string) ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	data, ok := s.objects[key]
	if !ok {
		return nil, fmt.Errorf("object %s not found", key)
	}
	return append([]byte(nil), data...), nil
}

func (s *MemoryStore) GetReader(ctx context.Context, key string) (io.ReadCloser, error) {
	data, err := s.GetBytes(ctx, key)
	if err != nil {
		return nil, err
	}
	return io.NopCloser(bytes.NewReader(data)), nil
}

func (s *MemoryStore) Head(_ context.Context, key string) (ObjectInfo, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	data, ok := s.objects[key]
	if !ok {
		return ObjectInfo{}, fmt.Errorf("object %s not found", key)
	}
	sha := sha256.Sum256(data)
	return ObjectInfo{Key: key, Size: int64(len(data)), SHA256: hex.EncodeToString(sha[:])}, nil
}

func (s *MemoryStore) Delete(_ context.Context, key string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.objects, key)
	return nil
}
