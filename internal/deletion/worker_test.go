package deletion

import (
	"bytes"
	"context"
	"database/sql"
	"path/filepath"
	"testing"

	"github.com/lawrencegripper/zfs-s3nd/internal/catalog"
	"github.com/lawrencegripper/zfs-s3nd/internal/catalog/db"
	"github.com/lawrencegripper/zfs-s3nd/internal/ingest"
	"github.com/lawrencegripper/zfs-s3nd/internal/storage"
)

func TestWorkerRefusesToDeleteCommittedSnapshotWithChildren(t *testing.T) {
	ctx := context.Background()
	cat, err := catalog.Open(filepath.Join(t.TempDir(), "catalog.db"))
	if err != nil {
		t.Fatalf("open catalog: %v", err)
	}
	defer cat.Close()
	if err := cat.Migrate(); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	q := db.New(cat.DB())

	store := storage.NewMemoryStore()
	svc := ingest.Service{Repo: catalog.NewRepository(cat.DB()), Store: store, Keys: storage.KeyBuilder{RootPrefix: "root"}, ChunkSize: 5}
	full, err := svc.Receive(ctx, ingest.Request{Source: "nas", Pool: "tank", Dataset: "photos", Snapshot: "snap1"}, bytes.NewReader([]byte("full-stream")))
	if err != nil {
		t.Fatalf("receive full: %v", err)
	}
	inc, err := svc.Receive(ctx, ingest.Request{Source: "nas", Pool: "tank", Dataset: "photos", Snapshot: "snap2", BaseSnapshot: "snap1"}, bytes.NewReader([]byte("incremental-stream")))
	if err != nil {
		t.Fatalf("receive incremental: %v", err)
	}
	if err := q.CreateOperation(ctx, db.CreateOperationParams{ID: "op_cleanup", Type: "cleanup", Status: "queued", SnapshotID: sql.NullString{String: full.SnapshotID, Valid: true}, Summary: sql.NullString{String: "snapshot deletion queued", Valid: true}}); err != nil {
		t.Fatalf("create operation: %v", err)
	}

	workerResult, err := (Worker{DB: cat.DB(), Store: store}).RunOnce(ctx)
	if err != nil {
		t.Fatalf("run worker: %v", err)
	}
	if workerResult.OperationsProcessed != 0 || workerResult.OperationsFailed != 1 || workerResult.ObjectsDeleted != 0 {
		t.Fatalf("worker result = %+v", workerResult)
	}
	if _, err := store.Head(ctx, full.ManifestKey); err != nil {
		t.Fatalf("parent manifest was deleted: %v", err)
	}
	if _, err := store.Head(ctx, inc.ManifestKey); err != nil {
		t.Fatalf("child manifest was deleted: %v", err)
	}
	for _, snapshotID := range []string{full.SnapshotID, inc.SnapshotID} {
		var status string
		if err := cat.DB().QueryRowContext(ctx, `SELECT status FROM snapshots WHERE id = ?`, snapshotID).Scan(&status); err != nil {
			t.Fatalf("query snapshot %s: %v", snapshotID, err)
		}
		if status != "committed" {
			t.Fatalf("snapshot %s status got %q", snapshotID, status)
		}
	}
	var opStatus string
	if err := cat.DB().QueryRowContext(ctx, `SELECT status FROM operations WHERE id = 'op_cleanup'`).Scan(&opStatus); err != nil {
		t.Fatalf("query operation: %v", err)
	}
	if opStatus != "failed" {
		t.Fatalf("operation status got %q", opStatus)
	}
}

func TestWorkerProcessesQueuedDatasetDeletion(t *testing.T) {
	ctx := context.Background()
	cat, err := catalog.Open(filepath.Join(t.TempDir(), "catalog.db"))
	if err != nil {
		t.Fatalf("open catalog: %v", err)
	}
	defer cat.Close()
	if err := cat.Migrate(); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	q := db.New(cat.DB())

	store := storage.NewMemoryStore()
	svc := ingest.Service{Repo: catalog.NewRepository(cat.DB()), Store: store, Keys: storage.KeyBuilder{RootPrefix: "root"}, ChunkSize: 5}
	result, err := svc.Receive(ctx, ingest.Request{Source: "nas", Pool: "tank", Dataset: "photos", Snapshot: "snap1"}, bytes.NewReader([]byte("hello-world")))
	if err != nil {
		t.Fatalf("receive: %v", err)
	}
	var datasetID string
	if err := cat.DB().QueryRowContext(ctx, `SELECT dataset_id FROM snapshots WHERE id = ?`, result.SnapshotID).Scan(&datasetID); err != nil {
		t.Fatalf("query dataset id: %v", err)
	}
	if err := q.CreateOperation(ctx, db.CreateOperationParams{ID: "op_cleanup", Type: "cleanup", Status: "queued", DatasetID: sql.NullString{String: datasetID, Valid: true}, Summary: sql.NullString{String: "dataset deletion queued", Valid: true}}); err != nil {
		t.Fatalf("create operation: %v", err)
	}

	workerResult, err := (Worker{DB: cat.DB(), Store: store}).RunOnce(ctx)
	if err != nil {
		t.Fatalf("run worker: %v", err)
	}
	if workerResult.OperationsProcessed != 1 || workerResult.ObjectsDeleted == 0 {
		t.Fatalf("worker result = %+v", workerResult)
	}
	if _, err := q.GetDatasetDetail(ctx, datasetID); err != sql.ErrNoRows {
		t.Fatalf("dataset after cleanup err=%v", err)
	}
	if _, err := store.Head(ctx, result.ManifestKey); err == nil {
		t.Fatalf("manifest still exists after cleanup")
	}
	var status string
	if err := cat.DB().QueryRowContext(ctx, `SELECT status FROM operations WHERE id = 'op_cleanup'`).Scan(&status); err != nil {
		t.Fatalf("query operation: %v", err)
	}
	if status != "succeeded" {
		t.Fatalf("operation status got %q", status)
	}
}
