package e2e

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/lawrencegripper/zfs-s3nd/internal/storage"
)

func TestSSHShutdownWaitsForInflightUpload(t *testing.T) {
	baseStore := storage.NewMemoryStore()
	blockingStore := &blockingPutStore{Store: baseStore, entered: make(chan struct{}), release: make(chan struct{})}
	fixture := startSSHFixture(t, 1024, blockingStore)
	fixture.srv.ShutdownPollInterval = 10 * time.Millisecond

	uploadDone := make(chan error, 1)
	go func() {
		_, err := fixture.runUpload(t, "tank/photos@snap1", 0, 0xaaa, []byte("hello-world"))
		uploadDone <- err
	}()

	select {
	case <-blockingStore.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("upload did not reach object storage put")
	}

	shutdownDone := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		shutdownDone <- fixture.srv.Shutdown(ctx)
	}()

	select {
	case err := <-shutdownDone:
		t.Fatalf("shutdown returned before in-flight upload completed: %v", err)
	case <-time.After(150 * time.Millisecond):
	}

	close(blockingStore.release)

	select {
	case err := <-uploadDone:
		if err != nil {
			t.Fatalf("upload failed during graceful shutdown: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("upload did not finish after releasing object storage put")
	}

	select {
	case err := <-shutdownDone:
		if err != nil {
			t.Fatalf("shutdown failed after upload completed: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("shutdown did not finish after upload completed")
	}
}

type blockingPutStore struct {
	storage.Store
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func (s *blockingPutStore) PutBytes(ctx context.Context, key string, data []byte) (storage.ObjectInfo, error) {
	s.once.Do(func() { close(s.entered) })
	select {
	case <-s.release:
	case <-ctx.Done():
		return storage.ObjectInfo{}, ctx.Err()
	}
	return s.Store.PutBytes(ctx, key, data)
}
