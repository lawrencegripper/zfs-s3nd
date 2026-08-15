package integration

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/lawrencegripper/zfs-s3nd/internal/config"
	"github.com/lawrencegripper/zfs-s3nd/internal/storage"
)

func TestS3StorePutHeadGetDelete(t *testing.T) {
	if os.Getenv("RUN_S3_TESTS") != "1" {
		t.Skip("set RUN_S3_TESTS=1 to run S3/RustFS integration tests")
	}
	cfg, err := config.Load()
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	store, err := storage.NewS3Store(ctx, storage.S3Config{
		Endpoint:        cfg.S3Endpoint,
		Region:          cfg.S3Region,
		Bucket:          cfg.S3Bucket,
		AccessKeyID:     cfg.S3AccessKeyID,
		SecretAccessKey: cfg.S3SecretAccessKey,
		ForcePathStyle:  cfg.S3ForcePathStyle,
	})
	if err != nil {
		t.Fatalf("new store: %v", err)
	}

	key := storage.KeyBuilder{RootPrefix: config.DefaultStorageRootPrefix}.ChunkKey("nas-home", "tank", "photos", "test-snapshot", 0)
	data := []byte("fake-zfs-stream-chunk")
	info, err := store.PutBytes(ctx, key, data)
	if err != nil {
		t.Fatalf("put bytes: %v", err)
	}
	if info.Size != int64(len(data)) {
		t.Fatalf("size got %d want %d", info.Size, len(data))
	}
	if info.SHA256 == "" {
		t.Fatal("expected sha256")
	}

	head, err := store.Head(ctx, key)
	if err != nil {
		t.Fatalf("head: %v", err)
	}
	if head.Size != int64(len(data)) {
		t.Fatalf("head size got %d want %d", head.Size, len(data))
	}

	got, err := store.GetBytes(ctx, key)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if string(got) != string(data) {
		t.Fatalf("data got %q want %q", got, data)
	}
	if err := store.Delete(ctx, key); err != nil {
		t.Fatalf("delete: %v", err)
	}
}
