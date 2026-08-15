package ingest

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/lawrencegripper/zfs-s3nd/internal/catalog"
	"github.com/lawrencegripper/zfs-s3nd/internal/manifest"
	"github.com/lawrencegripper/zfs-s3nd/internal/storage"
)

func TestReceiveChunksManifestAndCatalog(t *testing.T) {
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
	svc := Service{Repo: catalog.NewRepository(cat.DB()), Store: store, Keys: storage.KeyBuilder{RootPrefix: "zfs-s3nd/v1"}, ChunkSize: 5}
	input := []byte("hello-world!")
	result, err := svc.Receive(ctx, Request{Source: "nas-home", Pool: "tank", Dataset: "photos", Snapshot: "snap1", Raw: true}, bytes.NewReader(input))
	if err != nil {
		t.Fatalf("receive: %v", err)
	}
	if result.BytesReceived != int64(len(input)) {
		t.Fatalf("bytes got %d want %d", result.BytesReceived, len(input))
	}
	if len(result.Chunks) != 3 {
		t.Fatalf("chunks got %d want 3", len(result.Chunks))
	}
	sha := sha256.Sum256(input)
	if result.StreamSHA256 != hex.EncodeToString(sha[:]) {
		t.Fatalf("stream sha mismatch")
	}

	manifestBytes, err := store.GetBytes(ctx, result.ManifestKey)
	if err != nil {
		t.Fatalf("get manifest: %v", err)
	}
	mani, err := manifest.Unmarshal(manifestBytes)
	if err != nil {
		t.Fatalf("unmarshal manifest: %v", err)
	}
	if mani.Identity.Source != "nas-home" || mani.Identity.Pool != "tank" || mani.Identity.Dataset != "photos" || mani.Identity.Snapshot != "snap1" {
		t.Fatalf("bad manifest identity: %+v", mani.Identity)
	}
	if mani.Stream.SizeBytes != int64(len(input)) || len(mani.Chunks) != 3 {
		t.Fatalf("bad manifest stream/chunks: %+v", mani.Stream)
	}

	var status string
	var chunkCount int
	if err := cat.DB().QueryRowContext(ctx, `SELECT status, chunk_count FROM snapshots WHERE id = ?`, result.SnapshotID).Scan(&status, &chunkCount); err != nil {
		t.Fatalf("query snapshot: %v", err)
	}
	if status != "committed" || chunkCount != 3 {
		t.Fatalf("snapshot status/chunk count got %s/%d", status, chunkCount)
	}
}

func TestReceiveMarksFailedAndAllowsRetryAfterReadError(t *testing.T) {
	ctx := context.Background()
	cat, err := catalog.Open(filepath.Join(t.TempDir(), "catalog.db"))
	if err != nil {
		t.Fatalf("open catalog: %v", err)
	}
	defer cat.Close()
	if err := cat.Migrate(); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	svc := Service{Repo: catalog.NewRepository(cat.DB()), Store: storage.NewMemoryStore(), Keys: storage.KeyBuilder{RootPrefix: "root"}, ChunkSize: 5}
	req := Request{Source: "nas-home", Pool: "tank", Dataset: "photos", Snapshot: "snap1"}
	boom := errors.New("client disconnected")
	_, err = svc.Receive(ctx, req, &errorAfterChunksReader{chunks: [][]byte{[]byte("hello")}, err: boom})
	if err == nil || !strings.Contains(err.Error(), boom.Error()) {
		t.Fatalf("receive error got %v want %q", err, boom)
	}

	assertUploadFailureRecorded(t, ctx, cat, "client disconnected")

	result, err := svc.Receive(ctx, req, bytes.NewReader([]byte("hello-again")))
	if err != nil {
		t.Fatalf("retry receive: %v", err)
	}
	if result.BytesReceived != int64(len("hello-again")) {
		t.Fatalf("retry bytes got %d", result.BytesReceived)
	}
	var status string
	if err := cat.DB().QueryRowContext(ctx, `SELECT status FROM snapshots WHERE id = ?`, result.SnapshotID).Scan(&status); err != nil {
		t.Fatalf("query retried snapshot: %v", err)
	}
	if status != "committed" {
		t.Fatalf("retry snapshot status got %q want committed", status)
	}
}

func TestReceiveRetriesTransientStoragePutError(t *testing.T) {
	ctx := context.Background()
	cat, err := catalog.Open(filepath.Join(t.TempDir(), "catalog.db"))
	if err != nil {
		t.Fatalf("open catalog: %v", err)
	}
	defer cat.Close()
	if err := cat.Migrate(); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	baseStore := storage.NewMemoryStore()
	faultStore := storage.NewFaultStore(baseStore)
	faultStore.PutAt = 2
	faultStore.PutError = errors.New("temporary bucket write failed")
	svc := Service{Repo: catalog.NewRepository(cat.DB()), Store: faultStore, Keys: storage.KeyBuilder{RootPrefix: "root"}, ChunkSize: 5, PutRetryInitialBackoff: time.Millisecond}
	result, err := svc.Receive(ctx, Request{Source: "nas-home", Pool: "tank", Dataset: "photos", Snapshot: "snap1"}, bytes.NewReader([]byte("hello-world")))
	if err != nil {
		t.Fatalf("receive should retry transient put failure: %v", err)
	}
	if result.BytesReceived != int64(len("hello-world")) {
		t.Fatalf("bytes got %d", result.BytesReceived)
	}
	if _, err := baseStore.Head(ctx, result.Chunks[1].ObjectKey); err != nil {
		t.Fatalf("retried chunk was not stored: %v", err)
	}
}

func TestReceiveMarksFailedAndAllowsRetryAfterStoragePutError(t *testing.T) {
	ctx := context.Background()
	cat, err := catalog.Open(filepath.Join(t.TempDir(), "catalog.db"))
	if err != nil {
		t.Fatalf("open catalog: %v", err)
	}
	defer cat.Close()
	if err := cat.Migrate(); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	baseStore := storage.NewMemoryStore()
	putErr := errors.New("bucket write failed")
	failingStore := alwaysFailPutStore{Store: baseStore, err: putErr}
	svc := Service{Repo: catalog.NewRepository(cat.DB()), Store: failingStore, Keys: storage.KeyBuilder{RootPrefix: "root"}, ChunkSize: 5, PutRetryMaxElapsed: time.Millisecond, PutRetryInitialBackoff: time.Millisecond}
	req := Request{Source: "nas-home", Pool: "tank", Dataset: "photos", Snapshot: "snap1"}
	_, err = svc.Receive(ctx, req, bytes.NewReader([]byte("hello-world")))
	if err == nil || !strings.Contains(err.Error(), "bucket write failed") {
		t.Fatalf("receive error got %v want bucket failure", err)
	}

	assertUploadFailureRecorded(t, ctx, cat, "bucket write failed")

	svc.Store = baseStore
	if _, err := svc.Receive(ctx, req, bytes.NewReader([]byte("hello-world"))); err != nil {
		t.Fatalf("retry receive: %v", err)
	}
}

func TestReceiveMarksFailedAndAllowsRetryAfterManifestPutError(t *testing.T) {
	ctx := context.Background()
	cat, err := catalog.Open(filepath.Join(t.TempDir(), "catalog.db"))
	if err != nil {
		t.Fatalf("open catalog: %v", err)
	}
	defer cat.Close()
	if err := cat.Migrate(); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	baseStore := storage.NewMemoryStore()
	putErr := errors.New("manifest write failed")
	failingStore := manifestFailPutStore{Store: baseStore, err: putErr}
	svc := Service{Repo: catalog.NewRepository(cat.DB()), Store: failingStore, Keys: storage.KeyBuilder{RootPrefix: "root"}, ChunkSize: 5, PutRetryMaxElapsed: time.Millisecond, PutRetryInitialBackoff: time.Millisecond}
	req := Request{Source: "nas-home", Pool: "tank", Dataset: "photos", Snapshot: "snap1"}
	_, err = svc.Receive(ctx, req, bytes.NewReader([]byte("hello-world")))
	if err == nil || !strings.Contains(err.Error(), "manifest write failed") {
		t.Fatalf("receive error got %v want manifest failure", err)
	}

	assertUploadFailureRecorded(t, ctx, cat, "manifest write failed")

	svc.Store = baseStore
	if _, err := svc.Receive(ctx, req, bytes.NewReader([]byte("hello-world"))); err != nil {
		t.Fatalf("retry receive: %v", err)
	}
}

func TestReceiveMarksFailedAndAllowsRetryAfterCatalogCommitError(t *testing.T) {
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
	svc := Service{Repo: catalog.NewRepository(cat.DB()), Store: store, Keys: storage.KeyBuilder{RootPrefix: "root"}, ChunkSize: 5}
	svc.BeforeCatalogCommit = func(ctx context.Context, commit StartedCommit) error {
		_, err := cat.DB().ExecContext(ctx, `
CREATE TRIGGER fail_snapshot_commit
BEFORE UPDATE OF status ON snapshots
WHEN NEW.status = 'committed'
BEGIN
  SELECT RAISE(ABORT, 'forced catalog commit failure');
END;`)
		return err
	}
	req := Request{Source: "nas-home", Pool: "tank", Dataset: "photos", Snapshot: "snap1"}
	_, err = svc.Receive(ctx, req, bytes.NewReader([]byte("hello-world")))
	if err == nil || !strings.Contains(err.Error(), "forced catalog commit failure") {
		t.Fatalf("receive error got %v want catalog commit failure", err)
	}

	assertUploadFailureRecorded(t, ctx, cat, "forced catalog commit failure")

	var manifestKey string
	if err := cat.DB().QueryRowContext(ctx, `SELECT manifest_object_key FROM upload_sessions WHERE status = 'failed' ORDER BY started_at DESC LIMIT 1`).Scan(&manifestKey); err != nil {
		t.Fatalf("query failed upload manifest key: %v", err)
	}
	if _, err := store.Head(ctx, manifestKey); err != nil {
		t.Fatalf("manifest should remain for cleanup/reconciliation: %v", err)
	}

	if _, err := cat.DB().ExecContext(ctx, `DROP TRIGGER fail_snapshot_commit`); err != nil {
		t.Fatalf("drop trigger: %v", err)
	}
	svc.BeforeCatalogCommit = nil
	if _, err := svc.Receive(ctx, req, bytes.NewReader([]byte("hello-world"))); err != nil {
		t.Fatalf("retry receive: %v", err)
	}
}

func TestReceiveRecordsFailureAfterRequestContextIsCanceled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cat, err := catalog.Open(filepath.Join(t.TempDir(), "catalog.db"))
	if err != nil {
		t.Fatalf("open catalog: %v", err)
	}
	defer cat.Close()
	if err := cat.Migrate(); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	svc := Service{Repo: catalog.NewRepository(cat.DB()), Store: storage.NewMemoryStore(), Keys: storage.KeyBuilder{RootPrefix: "root"}, ChunkSize: 5}
	_, err = svc.Receive(ctx, Request{Source: "nas-home", Pool: "tank", Dataset: "photos", Snapshot: "snap1"}, cancelingReader{cancel: cancel})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("receive error got %v want context cancellation", err)
	}
	assertUploadFailureRecorded(t, context.Background(), cat, context.Canceled.Error())
}

func TestReceiveRejectsIncrementalChainBeyondLimit(t *testing.T) {
	ctx := context.Background()
	cat, err := catalog.Open(filepath.Join(t.TempDir(), "catalog.db"))
	if err != nil {
		t.Fatalf("open catalog: %v", err)
	}
	defer cat.Close()
	if err := cat.Migrate(); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	repo := catalog.NewRepository(cat.DB())
	store := storage.NewMemoryStore()
	svc := Service{Repo: repo, Store: store, Keys: storage.KeyBuilder{RootPrefix: "root"}, ChunkSize: 5, MaxIncrementalChainDepth: 2}

	if _, err := svc.Receive(ctx, Request{Source: "nas-home", Pool: "tank", Dataset: "photos", Snapshot: "s1"}, bytes.NewReader([]byte("full"))); err != nil {
		t.Fatalf("receive full: %v", err)
	}
	if _, err := svc.Receive(ctx, Request{Source: "nas-home", Pool: "tank", Dataset: "photos", Snapshot: "s2", BaseSnapshot: "s1"}, bytes.NewReader([]byte("inc"))); err != nil {
		t.Fatalf("receive incremental: %v", err)
	}
	_, err = svc.Receive(ctx, Request{Source: "nas-home", Pool: "tank", Dataset: "photos", Snapshot: "s3", BaseSnapshot: "s2"}, bytes.NewReader([]byte("too-deep")))
	if err == nil || !strings.Contains(err.Error(), "incremental chain depth would be 3") {
		t.Fatalf("expected chain depth rejection, got %v", err)
	}
}

func TestReceiveRejectsDuplicateSnapshot(t *testing.T) {
	ctx := context.Background()
	cat, err := catalog.Open(filepath.Join(t.TempDir(), "catalog.db"))
	if err != nil {
		t.Fatalf("open catalog: %v", err)
	}
	defer cat.Close()
	if err := cat.Migrate(); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	svc := Service{Repo: catalog.NewRepository(cat.DB()), Store: storage.NewMemoryStore(), Keys: storage.KeyBuilder{RootPrefix: "root"}, ChunkSize: 5}
	req := Request{Source: "nas-home", Pool: "tank", Dataset: "photos", Snapshot: "snap1"}
	if _, err := svc.Receive(ctx, req, bytes.NewReader([]byte("hello"))); err != nil {
		t.Fatalf("first receive: %v", err)
	}
	if _, err := svc.Receive(ctx, req, bytes.NewReader([]byte("hello"))); err == nil {
		t.Fatal("expected duplicate snapshot error")
	}
}

type cancelingReader struct {
	cancel context.CancelFunc
}

func (r cancelingReader) Read([]byte) (int, error) {
	r.cancel()
	return 0, context.Canceled
}

type alwaysFailPutStore struct {
	storage.Store
	err error
}

func (s alwaysFailPutStore) PutBytes(context.Context, string, []byte) (storage.ObjectInfo, error) {
	return storage.ObjectInfo{}, s.err
}

type manifestFailPutStore struct {
	storage.Store
	err error
}

func (s manifestFailPutStore) PutBytes(ctx context.Context, key string, data []byte) (storage.ObjectInfo, error) {
	if strings.HasSuffix(key, "/manifest.json") {
		return storage.ObjectInfo{}, s.err
	}
	return s.Store.PutBytes(ctx, key, data)
}

type errorAfterChunksReader struct {
	chunks [][]byte
	err    error
}

func (r *errorAfterChunksReader) Read(p []byte) (int, error) {
	if len(r.chunks) == 0 {
		return 0, r.err
	}
	n := copy(p, r.chunks[0])
	if n == len(r.chunks[0]) {
		r.chunks = r.chunks[1:]
	} else {
		r.chunks[0] = r.chunks[0][n:]
	}
	return n, nil
}

func assertUploadFailureRecorded(t *testing.T, ctx context.Context, cat *catalog.Catalog, wantReason string) {
	t.Helper()

	var snapshotStatus, uploadStatus, operationStatus, snapshotReason, uploadReason, operationReason string
	if err := cat.DB().QueryRowContext(ctx, `SELECT status, coalesce(failure_reason, '') FROM snapshots ORDER BY created_at DESC LIMIT 1`).Scan(&snapshotStatus, &snapshotReason); err != nil {
		t.Fatalf("query snapshot failure: %v", err)
	}
	if err := cat.DB().QueryRowContext(ctx, `SELECT status, coalesce(failure_reason, '') FROM upload_sessions ORDER BY started_at DESC LIMIT 1`).Scan(&uploadStatus, &uploadReason); err != nil {
		t.Fatalf("query upload failure: %v", err)
	}
	if err := cat.DB().QueryRowContext(ctx, `SELECT status, coalesce(failure_reason, '') FROM operations ORDER BY started_at DESC LIMIT 1`).Scan(&operationStatus, &operationReason); err != nil {
		t.Fatalf("query operation failure: %v", err)
	}
	if snapshotStatus != "failed" || uploadStatus != "failed" || operationStatus != "failed" {
		t.Fatalf("statuses got snapshot=%s upload=%s operation=%s, want all failed", snapshotStatus, uploadStatus, operationStatus)
	}
	if !strings.Contains(snapshotReason, wantReason) || !strings.Contains(uploadReason, wantReason) || !strings.Contains(operationReason, wantReason) {
		t.Fatalf("failure reasons got snapshot=%q upload=%q operation=%q, want containing %q", snapshotReason, uploadReason, operationReason, wantReason)
	}
}
