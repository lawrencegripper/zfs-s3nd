package catalog

import (
	"context"
	"crypto/sha256"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestMigrateAndHealth(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "catalog.db")
	cat, err := Open(dbPath)
	if err != nil {
		t.Fatalf("open catalog: %v", err)
	}
	defer cat.Close()

	if err := cat.Migrate(); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	if err := cat.Health(context.Background()); err != nil {
		t.Fatalf("health: %v", err)
	}

	var tableCount int
	if err := cat.DB().QueryRowContext(context.Background(), `SELECT count(*) FROM sqlite_master WHERE type = 'table' AND name IN ('admin_credentials', 'snapshots', 'snapshot_chunks', 'operations', 'catalog_backups')`).Scan(&tableCount); err != nil {
		t.Fatalf("count tables: %v", err)
	}
	if tableCount != 5 {
		t.Fatalf("expected key tables to exist, got %d", tableCount)
	}

	repo := NewRepository(cat.DB())
	if _, err := repo.StartUpload(context.Background(), DatasetIdentity{Source: "nas-home", Pool: "tank", Dataset: "photos", Snapshot: "snap1"}, 1024); err != nil {
		t.Fatalf("start upload using migrated schema: %v", err)
	}
}

func TestMigrationFilesAreImmutable(t *testing.T) {
	expected := map[string]string{
		"000001_initial.sql": "ec19f52e9c366410ebee1f9768879977c82e8b014b3abfd278c0288c8fdd5074",
	}

	dir, err := findMigrationsDir()
	if err != nil {
		t.Fatalf("find migrations: %v", err)
	}
	paths, err := filepath.Glob(filepath.Join(dir, "*.sql"))
	if err != nil {
		t.Fatalf("list migrations: %v", err)
	}
	if len(paths) != len(expected) {
		t.Fatalf("found %d migration files, want %d pinned checksums", len(paths), len(expected))
	}
	for _, path := range paths {
		name := filepath.Base(path)
		want, ok := expected[name]
		if !ok {
			t.Fatalf("migration %s has no pinned checksum", name)
		}
		contents, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("read migration %s: %v", name, err)
		}
		got := fmt.Sprintf("%x", sha256.Sum256(contents))
		if got != want {
			t.Fatalf("migration %s was modified: checksum %s, want %s; add a new migration instead", name, got, want)
		}
	}
}

func TestAdminCredentialsAreSingleton(t *testing.T) {
	cat, err := Open(filepath.Join(t.TempDir(), "catalog.db"))
	if err != nil {
		t.Fatalf("open catalog: %v", err)
	}
	defer cat.Close()
	if err := cat.Migrate(); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	if _, err := cat.DB().ExecContext(context.Background(), `INSERT INTO admin_credentials (singleton) VALUES (2)`); err == nil {
		t.Fatal("expected singleton constraint failure")
	}
}

func TestReconcileStaleUploadsMarksNonTerminalUploadsFailed(t *testing.T) {
	ctx := context.Background()
	cat, err := Open(filepath.Join(t.TempDir(), "catalog.db"))
	if err != nil {
		t.Fatalf("open catalog: %v", err)
	}
	defer cat.Close()
	if err := cat.Migrate(); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	repo := NewRepository(cat.DB())
	started, err := repo.StartUpload(ctx, DatasetIdentity{Source: "nas-home", Pool: "tank", Dataset: "photos", Snapshot: "snap1"}, 1024)
	if err != nil {
		t.Fatalf("start upload: %v", err)
	}
	oldHeartbeat := FormatTime(time.Now().Add(-30 * time.Minute))
	if _, err := cat.DB().ExecContext(ctx, `UPDATE upload_sessions SET last_heartbeat_at = ? WHERE id = ?`, oldHeartbeat, started.UploadSessionID); err != nil {
		t.Fatalf("age upload: %v", err)
	}

	result, err := repo.ReconcileStaleUploads(ctx, time.Now().Add(-10*time.Minute), 100)
	if err != nil {
		t.Fatalf("reconcile stale uploads: %v", err)
	}
	if result.StaleUploadsFailed != 1 || result.OperationID == "" {
		t.Fatalf("result = %+v, want one failed upload and operation id", result)
	}

	var snapshotStatus, uploadStatus, uploadOpStatus, reconciliationStatus string
	var snapshotReason, uploadReason, uploadOpReason string
	if err := cat.DB().QueryRowContext(ctx, `SELECT status, failure_reason FROM snapshots WHERE id = ?`, started.SnapshotID).Scan(&snapshotStatus, &snapshotReason); err != nil {
		t.Fatalf("query snapshot: %v", err)
	}
	if err := cat.DB().QueryRowContext(ctx, `SELECT status, failure_reason FROM upload_sessions WHERE id = ?`, started.UploadSessionID).Scan(&uploadStatus, &uploadReason); err != nil {
		t.Fatalf("query upload: %v", err)
	}
	if err := cat.DB().QueryRowContext(ctx, `SELECT status, failure_reason FROM operations WHERE id = ?`, started.OperationID).Scan(&uploadOpStatus, &uploadOpReason); err != nil {
		t.Fatalf("query upload operation: %v", err)
	}
	if err := cat.DB().QueryRowContext(ctx, `SELECT status FROM operations WHERE id = ?`, result.OperationID).Scan(&reconciliationStatus); err != nil {
		t.Fatalf("query reconciliation operation: %v", err)
	}
	if snapshotStatus != "failed" || uploadStatus != "failed" || uploadOpStatus != "failed" || reconciliationStatus != "succeeded" {
		t.Fatalf("statuses snapshot=%s upload=%s uploadOp=%s reconciliation=%s", snapshotStatus, uploadStatus, uploadOpStatus, reconciliationStatus)
	}
	for name, reason := range map[string]string{"snapshot": snapshotReason, "upload": uploadReason, "upload operation": uploadOpReason} {
		if !strings.Contains(reason, "stale upload session "+started.UploadSessionID) {
			t.Fatalf("%s failure reason %q does not mention stale upload", name, reason)
		}
	}
}

func TestReconcileStaleUploadsLeavesFreshUploadsRunning(t *testing.T) {
	ctx := context.Background()
	cat, err := Open(filepath.Join(t.TempDir(), "catalog.db"))
	if err != nil {
		t.Fatalf("open catalog: %v", err)
	}
	defer cat.Close()
	if err := cat.Migrate(); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	repo := NewRepository(cat.DB())
	started, err := repo.StartUpload(ctx, DatasetIdentity{Source: "nas-home", Pool: "tank", Dataset: "photos", Snapshot: "snap1"}, 1024)
	if err != nil {
		t.Fatalf("start upload: %v", err)
	}

	result, err := repo.ReconcileStaleUploads(ctx, time.Now().Add(-10*time.Minute), 100)
	if err != nil {
		t.Fatalf("reconcile stale uploads: %v", err)
	}
	if result.StaleUploadsFailed != 0 || result.OperationID != "" {
		t.Fatalf("result = %+v, want no-op", result)
	}
	var snapshotStatus, uploadStatus string
	if err := cat.DB().QueryRowContext(ctx, `SELECT status FROM snapshots WHERE id = ?`, started.SnapshotID).Scan(&snapshotStatus); err != nil {
		t.Fatalf("query snapshot: %v", err)
	}
	if err := cat.DB().QueryRowContext(ctx, `SELECT status FROM upload_sessions WHERE id = ?`, started.UploadSessionID).Scan(&uploadStatus); err != nil {
		t.Fatalf("query upload: %v", err)
	}
	if snapshotStatus != "uploading" || uploadStatus != "uploading" {
		t.Fatalf("statuses snapshot=%s upload=%s, want uploading", snapshotStatus, uploadStatus)
	}
}
