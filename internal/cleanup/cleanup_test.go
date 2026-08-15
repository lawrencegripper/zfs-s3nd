package cleanup

import (
	"bytes"
	"context"
	"errors"
	"io"
	"path/filepath"
	"testing"
	"time"

	"github.com/lawrencegripper/zfs-s3nd/internal/catalog"
	"github.com/lawrencegripper/zfs-s3nd/internal/ingest"
	"github.com/lawrencegripper/zfs-s3nd/internal/storage"
)

func TestCleanupAbandonedUploadDeletesObjectsAndCatalogRows(t *testing.T) {
	ctx := context.Background()
	cat, err := catalog.Open(filepath.Join(t.TempDir(), "catalog.db"))
	if err != nil {
		t.Fatalf("open catalog: %v", err)
	}
	defer cat.Close()
	if err := cat.Migrate(); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	store := storage.NewMemoryStore()
	repo := catalog.NewRepository(cat.DB())
	svc := ingest.Service{Repo: repo, Store: store, Keys: storage.KeyBuilder{RootPrefix: "root"}, ChunkSize: 5}
	_, err = svc.Receive(ctx, ingest.Request{Source: "nas-home", Pool: "tank", Dataset: "photos", Snapshot: "snap1"}, &failAfterReader{r: bytes.NewReader([]byte("hello-world")), after: len("hello-world"), err: errors.New("client disconnected")})
	if err == nil {
		t.Fatal("expected receive failure")
	}

	var uploadID, snapshotID string
	if err := cat.DB().QueryRowContext(ctx, `SELECT id, snapshot_id FROM upload_sessions WHERE status = 'failed'`).Scan(&uploadID, &snapshotID); err != nil {
		t.Fatalf("query failed upload: %v", err)
	}
	chunks, err := repo.ListUploadSessionChunks(ctx, uploadID)
	if err != nil {
		t.Fatalf("list chunks: %v", err)
	}
	if len(chunks) != 2 {
		t.Fatalf("chunks got %d want 2", len(chunks))
	}
	for _, chunk := range chunks {
		if _, err := store.Head(ctx, chunk.ObjectKey); err != nil {
			t.Fatalf("head chunk before cleanup: %v", err)
		}
	}

	result, err := (AbandonedUploadCleaner{Repo: repo, Store: store}).Cleanup(ctx, time.Now().Add(time.Minute), 100)
	if err != nil {
		t.Fatalf("cleanup: %v", err)
	}
	if result.UploadsDeleted != 1 || result.SnapshotsDeleted != 1 || result.ObjectsDeleted != 2 {
		t.Fatalf("result = %+v, want 1 upload, 1 snapshot, 2 objects", result)
	}
	for _, chunk := range chunks {
		if _, err := store.Head(ctx, chunk.ObjectKey); err == nil {
			t.Fatalf("chunk %s still exists after cleanup", chunk.ObjectKey)
		}
	}
	var count int
	if err := cat.DB().QueryRowContext(ctx, `SELECT count(*) FROM upload_sessions WHERE id = ?`, uploadID).Scan(&count); err != nil {
		t.Fatalf("count upload sessions: %v", err)
	}
	if count != 0 {
		t.Fatalf("upload rows got %d want 0", count)
	}
	if err := cat.DB().QueryRowContext(ctx, `SELECT count(*) FROM snapshots WHERE id = ?`, snapshotID).Scan(&count); err != nil {
		t.Fatalf("count snapshots: %v", err)
	}
	if count != 0 {
		t.Fatalf("snapshot rows got %d want 0", count)
	}
}

func TestCleanupAbandonedUploadDeletesStoredManifestKey(t *testing.T) {
	ctx := context.Background()
	cat, err := catalog.Open(filepath.Join(t.TempDir(), "catalog.db"))
	if err != nil {
		t.Fatalf("open catalog: %v", err)
	}
	defer cat.Close()
	if err := cat.Migrate(); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	store := storage.NewMemoryStore()
	repo := catalog.NewRepository(cat.DB())
	started, err := repo.StartUpload(ctx, catalog.DatasetIdentity{Source: "nas-home", Pool: "tank", Dataset: "photos", Snapshot: "snap1"}, 5)
	if err != nil {
		t.Fatalf("start upload: %v", err)
	}
	manifestKey := "root/sources/nas-home/pools/tank/datasets/photos/@snapshots/snap1/manifest.json"
	if _, err := store.PutBytes(ctx, manifestKey, []byte(`{"manifest":"staged"}`)); err != nil {
		t.Fatalf("put manifest: %v", err)
	}
	if err := repo.SetUploadManifestObjectKey(ctx, started.UploadSessionID, manifestKey); err != nil {
		t.Fatalf("set upload manifest: %v", err)
	}
	if err := repo.FailUpload(ctx, started.SnapshotID, started.UploadSessionID, started.OperationID, errors.New("catalog commit failed")); err != nil {
		t.Fatalf("fail upload: %v", err)
	}

	result, err := (AbandonedUploadCleaner{Repo: repo, Store: store}).Cleanup(ctx, time.Now().Add(time.Minute), 100)
	if err != nil {
		t.Fatalf("cleanup: %v", err)
	}
	if result.UploadsDeleted != 1 || result.SnapshotsDeleted != 1 || result.ObjectsDeleted != 1 {
		t.Fatalf("result = %+v, want 1 upload, 1 snapshot, 1 object", result)
	}
	if _, err := store.Head(ctx, manifestKey); err == nil {
		t.Fatalf("manifest %s still exists after cleanup", manifestKey)
	}
}

type failAfterReader struct {
	r     io.Reader
	after int
	read  int
	err   error
}

func (r *failAfterReader) Read(p []byte) (int, error) {
	if r.read >= r.after {
		return 0, r.err
	}
	if remaining := r.after - r.read; len(p) > remaining {
		p = p[:remaining]
	}
	n, err := r.r.Read(p)
	r.read += n
	if err != nil {
		return n, err
	}
	return n, nil
}
