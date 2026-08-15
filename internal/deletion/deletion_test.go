package deletion

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"path/filepath"
	"testing"

	"github.com/lawrencegripper/zfs-s3nd/internal/catalog"
	"github.com/lawrencegripper/zfs-s3nd/internal/ingest"
	"github.com/lawrencegripper/zfs-s3nd/internal/storage"
)

func TestDeleteSnapshotInvalidatesCatalogBeforeDeletingObjects(t *testing.T) {
	ctx := context.Background()
	cat, store, result := createCommittedSnapshot(t, ctx)
	defer cat.Close()

	if _, err := cat.DB().ExecContext(ctx, `UPDATE snapshots SET stream_validation_status = 'succeeded', chain_validation_status = 'succeeded' WHERE id = ?`, result.SnapshotID); err != nil {
		t.Fatalf("mark snapshot valid: %v", err)
	}

	observed := false
	checkingStore := &deleteCheckingStore{Store: store, beforeDelete: func() error {
		observed = true
		return requireSnapshotInvalid(ctx, cat.DB(), result.SnapshotID)
	}}
	if _, err := (Deleter{DB: cat.DB(), Store: checkingStore}).DeleteSnapshot(ctx, result.SnapshotID); err != nil {
		t.Fatalf("delete snapshot: %v", err)
	}
	if !observed {
		t.Fatal("storage deletion was not observed")
	}
}

func TestDeleteDatasetInvalidatesSnapshotsBeforeDeletingObjects(t *testing.T) {
	ctx := context.Background()
	cat, store, result := createCommittedSnapshot(t, ctx)
	defer cat.Close()

	if _, err := cat.DB().ExecContext(ctx, `UPDATE snapshots SET stream_validation_status = 'succeeded', chain_validation_status = 'succeeded' WHERE id = ?`, result.SnapshotID); err != nil {
		t.Fatalf("mark snapshot valid: %v", err)
	}
	var datasetID string
	if err := cat.DB().QueryRowContext(ctx, `SELECT dataset_id FROM snapshots WHERE id = ?`, result.SnapshotID).Scan(&datasetID); err != nil {
		t.Fatalf("query dataset id: %v", err)
	}

	observed := false
	checkingStore := &deleteCheckingStore{Store: store, beforeDelete: func() error {
		observed = true
		return requireSnapshotInvalid(ctx, cat.DB(), result.SnapshotID)
	}}
	if _, err := (Deleter{DB: cat.DB(), Store: checkingStore}).DeleteDataset(ctx, datasetID); err != nil {
		t.Fatalf("delete dataset: %v", err)
	}
	if !observed {
		t.Fatal("storage deletion was not observed")
	}
}

func createCommittedSnapshot(t *testing.T, ctx context.Context) (*catalog.Catalog, *storage.MemoryStore, ingest.Result) {
	t.Helper()
	cat, err := catalog.Open(filepath.Join(t.TempDir(), "catalog.db"))
	if err != nil {
		t.Fatalf("open catalog: %v", err)
	}
	if err := cat.Migrate(); err != nil {
		cat.Close()
		t.Fatalf("migrate: %v", err)
	}
	store := storage.NewMemoryStore()
	result, err := (ingest.Service{Repo: catalog.NewRepository(cat.DB()), Store: store, Keys: storage.KeyBuilder{RootPrefix: "root"}, ChunkSize: 5}).Receive(ctx, ingest.Request{Source: "nas", Pool: "tank", Dataset: "photos", Snapshot: "snap1"}, bytes.NewReader([]byte("hello-world")))
	if err != nil {
		cat.Close()
		t.Fatalf("receive snapshot: %v", err)
	}
	return cat, store, result
}

func requireSnapshotInvalid(ctx context.Context, database *sql.DB, snapshotID string) error {
	var status, streamStatus, chainStatus string
	if err := database.QueryRowContext(ctx, `SELECT status, stream_validation_status, chain_validation_status FROM snapshots WHERE id = ?`, snapshotID).Scan(&status, &streamStatus, &chainStatus); err != nil {
		return fmt.Errorf("query snapshot during object deletion: %w", err)
	}
	if status == "committed" || streamStatus == "succeeded" || chainStatus == "succeeded" {
		return fmt.Errorf("snapshot remained valid during object deletion: status=%s stream=%s chain=%s", status, streamStatus, chainStatus)
	}
	return nil
}

type deleteCheckingStore struct {
	storage.Store
	beforeDelete func() error
}

func (s *deleteCheckingStore) Delete(ctx context.Context, key string) error {
	if err := s.beforeDelete(); err != nil {
		return err
	}
	return s.Store.Delete(ctx, key)
}
