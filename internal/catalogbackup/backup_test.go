package catalogbackup

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/lawrencegripper/zfs-s3nd/internal/catalog"
	"github.com/lawrencegripper/zfs-s3nd/internal/storage"
)

func TestRunnerBacksUpCatalogToStoreAndRecordsRows(t *testing.T) {
	ctx := context.Background()
	cat, err := catalog.Open(filepath.Join(t.TempDir(), "catalog.db"))
	if err != nil {
		t.Fatalf("open catalog: %v", err)
	}
	defer cat.Close()
	if err := cat.Migrate(); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	if _, err := cat.DB().ExecContext(ctx, `INSERT INTO admin_credentials (singleton, password_hash) VALUES (1, 'hash')`); err != nil {
		t.Fatalf("insert admin credentials: %v", err)
	}

	store := storage.NewMemoryStore()
	now := time.Date(2026, 7, 3, 4, 5, 6, 0, time.UTC)
	result, err := (Runner{Catalog: cat, Repo: catalog.NewRepository(cat.DB()), Store: store, Keys: storage.KeyBuilder{RootPrefix: "zfs-s3nd/v1"}, TempDir: t.TempDir(), Now: func() time.Time { return now }}).Run(ctx)
	if err != nil {
		t.Fatalf("run backup: %v", err)
	}
	if result.ObjectKey == "" || result.MetadataObjectKey == "" || result.SizeBytes <= 0 || result.ChecksumSHA256 == "" {
		t.Fatalf("bad result: %+v", result)
	}

	backupBytes, err := store.GetBytes(ctx, result.ObjectKey)
	if err != nil {
		t.Fatalf("get backup object: %v", err)
	}
	sha := sha256.Sum256(backupBytes)
	if got := hex.EncodeToString(sha[:]); got != result.ChecksumSHA256 {
		t.Fatalf("checksum got %s want %s", got, result.ChecksumSHA256)
	}
	if _, err := store.GetBytes(ctx, result.MetadataObjectKey); err != nil {
		t.Fatalf("get metadata object: %v", err)
	}

	restoredPath := filepath.Join(t.TempDir(), "restored.db")
	if err := writeFile(restoredPath, backupBytes); err != nil {
		t.Fatalf("write restored db: %v", err)
	}
	restored, err := sql.Open("sqlite3", restoredPath)
	if err != nil {
		t.Fatalf("open restored db: %v", err)
	}
	defer restored.Close()
	var passwordHash string
	if err := restored.QueryRowContext(ctx, `SELECT password_hash FROM admin_credentials WHERE singleton = 1`).Scan(&passwordHash); err != nil {
		t.Fatalf("query restored db: %v", err)
	}
	if passwordHash != "hash" {
		t.Fatalf("restored password hash got %q", passwordHash)
	}

	var backupStatus, operationStatus, objectKey, metadataKey string
	if err := cat.DB().QueryRowContext(ctx, `SELECT status, object_key, metadata_object_key FROM catalog_backups WHERE id = ?`, result.BackupID).Scan(&backupStatus, &objectKey, &metadataKey); err != nil {
		t.Fatalf("query catalog backup row: %v", err)
	}
	if err := cat.DB().QueryRowContext(ctx, `SELECT status FROM operations WHERE id = ?`, result.OperationID).Scan(&operationStatus); err != nil {
		t.Fatalf("query operation row: %v", err)
	}
	if backupStatus != "succeeded" || operationStatus != "succeeded" || objectKey != result.ObjectKey || metadataKey != result.MetadataObjectKey {
		t.Fatalf("rows backupStatus=%s operationStatus=%s objectKey=%s metadataKey=%s", backupStatus, operationStatus, objectKey, metadataKey)
	}
}

func TestRunnerDeletesCatalogBackupsOlderThanRetention(t *testing.T) {
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
	oldStarted := time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC)
	recentStarted := time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC)
	for _, key := range []string{"old.sqlite", "old.json", "recent.sqlite", "recent.json"} {
		if _, err := store.PutBytes(ctx, key, []byte(key)); err != nil {
			t.Fatalf("put %s: %v", key, err)
		}
	}
	if err := repo.RecordCatalogBackup(ctx, catalog.CatalogBackupRecord{ID: "old", ObjectKey: "old.sqlite", MetadataObjectKey: "old.json", SizeBytes: 1, ChecksumSHA256: "old", StartedAt: oldStarted, CompletedAt: oldStarted, Status: "succeeded"}); err != nil {
		t.Fatalf("record old backup: %v", err)
	}
	if err := repo.RecordCatalogBackup(ctx, catalog.CatalogBackupRecord{ID: "recent", ObjectKey: "recent.sqlite", MetadataObjectKey: "recent.json", SizeBytes: 1, ChecksumSHA256: "recent", StartedAt: recentStarted, CompletedAt: recentStarted, Status: "succeeded"}); err != nil {
		t.Fatalf("record recent backup: %v", err)
	}

	now := time.Date(2026, 7, 3, 4, 5, 6, 0, time.UTC)
	if _, err := (Runner{Catalog: cat, Repo: repo, Store: store, Keys: storage.KeyBuilder{RootPrefix: "root"}, TempDir: t.TempDir(), Now: func() time.Time { return now }}).Run(ctx); err != nil {
		t.Fatalf("run backup: %v", err)
	}

	if _, err := store.GetBytes(ctx, "old.sqlite"); err == nil {
		t.Fatalf("old backup object still exists")
	}
	if _, err := store.GetBytes(ctx, "old.json"); err == nil {
		t.Fatalf("old backup metadata still exists")
	}
	if _, err := store.GetBytes(ctx, "recent.sqlite"); err != nil {
		t.Fatalf("recent backup object missing: %v", err)
	}
	var count int
	if err := cat.DB().QueryRowContext(ctx, `SELECT count(*) FROM catalog_backups WHERE id = 'old'`).Scan(&count); err != nil {
		t.Fatalf("query old backup row: %v", err)
	}
	if count != 0 {
		t.Fatalf("old backup row count = %d", count)
	}
}

func writeFile(path string, data []byte) error {
	return os.WriteFile(path, data, 0o600)
}
