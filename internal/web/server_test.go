package web

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"database/sql"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"golang.org/x/crypto/ssh"

	"github.com/lawrencegripper/zfs-s3nd/internal/appsettings"
	"github.com/lawrencegripper/zfs-s3nd/internal/catalog"
	"github.com/lawrencegripper/zfs-s3nd/internal/catalog/db"
	"github.com/lawrencegripper/zfs-s3nd/internal/deletion"
	"github.com/lawrencegripper/zfs-s3nd/internal/ingest"
	"github.com/lawrencegripper/zfs-s3nd/internal/storage"
	"github.com/lawrencegripper/zfs-s3nd/internal/validation"
)

func TestDashboardRendersSummaryOperationsBackupsAndSSHKeys(t *testing.T) {
	ctx := context.Background()
	cat := openTestCatalog(t)
	q := db.New(cat.DB())
	if err := q.CreateOperation(ctx, db.CreateOperationParams{ID: "op_1", Type: "upload", Status: "failed"}); err != nil {
		t.Fatalf("create operation: %v", err)
	}
	if err := q.CreateOperation(ctx, db.CreateOperationParams{ID: "op_catalog", Type: "catalog_backup", Status: "succeeded", Summary: sql.NullString{String: "catalog operation hidden", Valid: true}}); err != nil {
		t.Fatalf("create catalog operation: %v", err)
	}
	if err := q.CreateCatalogBackup(ctx, db.CreateCatalogBackupParams{ID: "catbak_1", ObjectKey: "root/@catalog-backups/backup.sqlite", SizeBytes: 123, ChecksumSha256: "abc", StartedAt: catalog.FormatTime(testTime()), CompletedAt: sql.NullString{String: catalog.FormatTime(testTime()), Valid: true}, Status: "succeeded"}); err != nil {
		t.Fatalf("create backup: %v", err)
	}
	key := testAuthorizedKey(t)
	pub, _, _, _, _ := ssh.ParseAuthorizedKey([]byte(key))
	if err := q.CreateSSHKey(ctx, db.CreateSSHKeyParams{ID: "key_1", Name: "laptop", PublicKey: key, FingerprintSha256: ssh.FingerprintSHA256(pub)}); err != nil {
		t.Fatalf("create key: %v", err)
	}
	svc := ingest.Service{Repo: catalog.NewRepository(cat.DB()), Store: storage.NewMemoryStore(), Keys: storage.KeyBuilder{RootPrefix: "root"}, ChunkSize: 5}
	snapshot, err := svc.Receive(ctx, ingest.Request{Source: "nas-home", Pool: "tank", Dataset: "photos", Snapshot: "snap1"}, bytes.NewReader([]byte("hello-world")))
	if err != nil {
		t.Fatalf("receive snapshot: %v", err)
	}
	if err := q.CreateValidationJob(ctx, db.CreateValidationJobParams{ID: "val_1", SnapshotID: sql.NullString{String: snapshot.SnapshotID, Valid: true}, Type: "stream_check", Executor: "railway_sandbox", ResultSummary: sql.NullString{String: "fromguid mismatch", Valid: true}}); err != nil {
		t.Fatalf("create validation job: %v", err)
	}
	if err := q.FailValidationJob(ctx, db.FailValidationJobParams{ID: "val_1", ResultSummary: sql.NullString{String: "fromguid mismatch", Valid: true}}); err != nil {
		t.Fatalf("fail validation job: %v", err)
	}
	if err := q.UpdateSnapshotChainValidationStatus(ctx, db.UpdateSnapshotChainValidationStatusParams{ID: snapshot.SnapshotID, ChainValidationStatus: "failed"}); err != nil {
		t.Fatalf("update chain validation status: %v", err)
	}
	started, err := catalog.NewRepository(cat.DB()).StartUpload(ctx, catalog.DatasetIdentity{Source: "nas-home", Pool: "tank", Dataset: "photos", Snapshot: "snap-active"}, 5)
	if err != nil {
		t.Fatalf("start upload: %v", err)
	}
	if err := catalog.NewRepository(cat.DB()).UpdateUploadProgress(ctx, started.UploadSessionID, 1, 1, 10_000_000); err != nil {
		t.Fatalf("update upload progress: %v", err)
	}
	startedAt := catalog.FormatTime(time.Now().Add(-10 * time.Second))
	if _, err := cat.DB().ExecContext(ctx, `UPDATE upload_sessions SET started_at = ?, last_heartbeat_at = ? WHERE id = ?`, startedAt, catalog.FormatTime(time.Now()), started.UploadSessionID); err != nil {
		t.Fatalf("set upload timing: %v", err)
	}

	server := newTestServer(cat)
	for _, route := range []struct {
		path  string
		wants []string
	}{
		{"/", []string{"ZFS S3nd", "Problems", "Catalog backup", "Current activity", "snap-active", "Mbps", "Backups", "Failed chain validations"}},
		{"/activity", []string{"Activity", "Operations", "Validation jobs", "fromguid mismatch", "railway_sandbox", "Upload", "Failed", "snap-active"}},
		{"/settings", []string{"Access and integrations", "Backup policy", "Upload throughput limit", "45 Mbps", "Maximum incremental chain depth", "laptop"}},
	} {
		rr := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, route.path, nil)
		server.http.Handler.ServeHTTP(rr, req)
		if rr.Code != http.StatusOK {
			t.Fatalf("GET %s status got %d body=%s", route.path, rr.Code, rr.Body.String())
		}
		body := rr.Body.String()
		for _, want := range route.wants {
			if !strings.Contains(body, want) {
				t.Fatalf("GET %s body missing %q:\n%s", route.path, want, body)
			}
		}
		if strings.Contains(body, "catalog operation hidden") {
			t.Fatalf("GET %s should not show catalog backup operations:\n%s", route.path, body)
		}
	}
}

func TestDatasetAndSnapshotPagesRenderRestoreCommand(t *testing.T) {
	ctx := context.Background()
	cat := openTestCatalog(t)
	svc := ingest.Service{Repo: catalog.NewRepository(cat.DB()), Store: storage.NewMemoryStore(), Keys: storage.KeyBuilder{RootPrefix: "root"}, ChunkSize: 5}
	full, err := svc.Receive(ctx, ingest.Request{Source: "nas-home", Pool: "tank", Dataset: "photos", Snapshot: "snap1"}, bytes.NewReader([]byte("hello-world")))
	if err != nil {
		t.Fatalf("receive full: %v", err)
	}
	inc, err := svc.Receive(ctx, ingest.Request{Source: "nas-home", Pool: "tank", Dataset: "photos", Snapshot: "snap2", BaseSnapshot: "snap1"}, bytes.NewReader([]byte("incremental")))
	if err != nil {
		t.Fatalf("receive incremental: %v", err)
	}
	var datasetID string
	if err := cat.DB().QueryRowContext(ctx, `SELECT dataset_id FROM snapshots WHERE id = ?`, full.SnapshotID).Scan(&datasetID); err != nil {
		t.Fatalf("query dataset id: %v", err)
	}

	server := newTestServer(cat)
	server.SetRestoreSSHCommandPrefix("ssh -p 15227 truenas@example.test")
	for _, route := range []struct{ path, want string }{
		{"/datasets", "photos"},
		{"/datasets/" + datasetID, "snap1"},
		{"/snapshots/" + inc.SnapshotID, "ssh -p 15227 restore@example.test restore-stream"},
		{"/snapshots/" + inc.SnapshotID, inc.SnapshotID},
		{"/snapshots/" + inc.SnapshotID, inc.ManifestKey},
	} {
		rr := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, route.path, nil)
		server.http.Handler.ServeHTTP(rr, req)
		if rr.Code != http.StatusOK {
			t.Fatalf("GET %s status got %d body=%s", route.path, rr.Code, rr.Body.String())
		}
		if !strings.Contains(rr.Body.String(), route.want) {
			t.Fatalf("GET %s body missing %q:\n%s", route.path, route.want, rr.Body.String())
		}
	}
	rr := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/snapshots/"+inc.SnapshotID, nil)
	server.http.Handler.ServeHTTP(rr, req)
	if strings.Contains(rr.Body.String(), "zfs-s3nd restore") {
		t.Fatalf("snapshot page should show SSH restore commands, not local CLI helpers:\n%s", rr.Body.String())
	}
}

func TestManualSnapshotValidation(t *testing.T) {
	ctx := context.Background()
	cat := openTestCatalog(t)
	store := storage.NewMemoryStore()
	svc := ingest.Service{Repo: catalog.NewRepository(cat.DB()), Store: store, Keys: storage.KeyBuilder{RootPrefix: "root"}, ChunkSize: 5}
	result, err := svc.Receive(ctx, ingest.Request{Source: "nas-home", Pool: "tank", Dataset: "photos", Snapshot: "snap1"}, bytes.NewReader([]byte("hello-world")))
	if err != nil {
		t.Fatalf("receive: %v", err)
	}
	server := newTestServer(cat)
	server.SetValidationRunner(validation.Runner{DB: cat.DB(), Store: store, Checker: webFakeChecker{}, Executor: "local"})

	rr := httptest.NewRecorder()
	form := strings.NewReader("mode=stream")
	req := httptest.NewRequest(http.MethodPost, "/snapshots/"+result.SnapshotID+"/validate", form)
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	server.http.Handler.ServeHTTP(rr, req)
	if rr.Code != http.StatusSeeOther {
		t.Fatalf("status got %d body=%s", rr.Code, rr.Body.String())
	}
	waitForCondition(t, func() bool {
		var streamStatus, chainStatus string
		if err := cat.DB().QueryRowContext(ctx, `SELECT stream_validation_status, chain_validation_status FROM snapshots WHERE id = ?`, result.SnapshotID).Scan(&streamStatus, &chainStatus); err != nil {
			return false
		}
		return streamStatus == "succeeded" && chainStatus == "pending"
	})
}

func TestManualDatasetValidation(t *testing.T) {
	ctx := context.Background()
	cat := openTestCatalog(t)
	store := storage.NewMemoryStore()
	svc := ingest.Service{Repo: catalog.NewRepository(cat.DB()), Store: store, Keys: storage.KeyBuilder{RootPrefix: "root"}, ChunkSize: 5}
	result, err := svc.Receive(ctx, ingest.Request{Source: "nas-home", Pool: "tank", Dataset: "photos", Snapshot: "snap1"}, bytes.NewReader([]byte("hello-world")))
	if err != nil {
		t.Fatalf("receive: %v", err)
	}
	var datasetID string
	if err := cat.DB().QueryRowContext(ctx, `SELECT dataset_id FROM snapshots WHERE id = ?`, result.SnapshotID).Scan(&datasetID); err != nil {
		t.Fatalf("query dataset id: %v", err)
	}
	server := newTestServer(cat)
	server.SetValidationRunner(validation.Runner{DB: cat.DB(), Store: store, Checker: webFakeChecker{}, Executor: "local"})

	rr := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/datasets/"+datasetID+"/validate", nil)
	server.http.Handler.ServeHTTP(rr, req)
	if rr.Code != http.StatusSeeOther {
		t.Fatalf("status got %d body=%s", rr.Code, rr.Body.String())
	}
	waitForCondition(t, func() bool {
		var chainStatus string
		if err := cat.DB().QueryRowContext(ctx, `SELECT chain_validation_status FROM snapshots WHERE id = ?`, result.SnapshotID).Scan(&chainStatus); err != nil {
			return false
		}
		return chainStatus == "succeeded"
	})
}

type webFakeChecker struct{}

func (webFakeChecker) Check(_ context.Context, _ string, stream io.Reader) (validation.StreamGUIDs, error) {
	_, err := io.Copy(io.Discard, stream)
	return validation.StreamGUIDs{}, err
}

func TestDeleteSnapshotRemovesCatalogAndObjects(t *testing.T) {
	ctx := context.Background()
	cat := openTestCatalog(t)
	store := storage.NewMemoryStore()
	svc := ingest.Service{Repo: catalog.NewRepository(cat.DB()), Store: store, Keys: storage.KeyBuilder{RootPrefix: "root"}, ChunkSize: 5}
	result, err := svc.Receive(ctx, ingest.Request{Source: "nas-home", Pool: "tank", Dataset: "photos", Snapshot: "snap1"}, bytes.NewReader([]byte("hello-world")))
	if err != nil {
		t.Fatalf("receive: %v", err)
	}
	if _, err := store.Head(ctx, result.ManifestKey); err != nil {
		t.Fatalf("manifest before delete: %v", err)
	}

	server := newTestServer(cat)
	server.SetStore(store)
	rr := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/snapshots/"+result.SnapshotID+"/delete", nil)
	server.http.Handler.ServeHTTP(rr, req)
	if rr.Code != http.StatusSeeOther {
		t.Fatalf("status got %d body=%s", rr.Code, rr.Body.String())
	}
	if _, err := (deletion.Worker{DB: cat.DB(), Store: store}).RunOnce(ctx); err != nil {
		t.Fatalf("run cleanup worker: %v", err)
	}
	waitForCondition(t, func() bool {
		_, err := db.New(cat.DB()).GetSnapshotDetail(ctx, result.SnapshotID)
		return err == sql.ErrNoRows
	})
	waitForCondition(t, func() bool {
		_, err := store.Head(ctx, result.ManifestKey)
		return err != nil
	})
	for _, chunk := range result.Chunks {
		objectKey := chunk.ObjectKey
		waitForCondition(t, func() bool {
			_, err := store.Head(ctx, objectKey)
			return err != nil
		})
	}
}

func TestDeleteDatasetRemovesCatalogAndObjects(t *testing.T) {
	ctx := context.Background()
	cat := openTestCatalog(t)
	store := storage.NewMemoryStore()
	svc := ingest.Service{Repo: catalog.NewRepository(cat.DB()), Store: store, Keys: storage.KeyBuilder{RootPrefix: "root"}, ChunkSize: 5}
	result, err := svc.Receive(ctx, ingest.Request{Source: "nas-home", Pool: "tank", Dataset: "photos", Snapshot: "snap1"}, bytes.NewReader([]byte("hello-world")))
	if err != nil {
		t.Fatalf("receive: %v", err)
	}
	inc, err := svc.Receive(ctx, ingest.Request{Source: "nas-home", Pool: "tank", Dataset: "photos", Snapshot: "snap2", BaseSnapshot: "snap1"}, bytes.NewReader([]byte("incremental")))
	if err != nil {
		t.Fatalf("receive incremental: %v", err)
	}
	var datasetID string
	if err := cat.DB().QueryRowContext(ctx, `SELECT dataset_id FROM snapshots WHERE id = ?`, result.SnapshotID).Scan(&datasetID); err != nil {
		t.Fatalf("query dataset id: %v", err)
	}

	server := newTestServer(cat)
	server.SetStore(store)
	rr := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/datasets/"+datasetID+"/delete", nil)
	server.http.Handler.ServeHTTP(rr, req)
	if rr.Code != http.StatusSeeOther {
		t.Fatalf("status got %d body=%s", rr.Code, rr.Body.String())
	}
	if _, err := (deletion.Worker{DB: cat.DB(), Store: store}).RunOnce(ctx); err != nil {
		t.Fatalf("run cleanup worker: %v", err)
	}
	waitForCondition(t, func() bool {
		_, err := db.New(cat.DB()).GetDatasetDetail(ctx, datasetID)
		return err == sql.ErrNoRows
	})
	for _, deleted := range []ingest.Result{result, inc} {
		manifestKey := deleted.ManifestKey
		waitForCondition(t, func() bool {
			_, err := store.Head(ctx, manifestKey)
			return err != nil
		})
		for _, chunk := range deleted.Chunks {
			objectKey := chunk.ObjectKey
			waitForCondition(t, func() bool {
				_, err := store.Head(ctx, objectKey)
				return err != nil
			})
		}
	}
}

func waitForCondition(t *testing.T, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("condition not met before timeout")
}

func TestFormatBitrate(t *testing.T) {
	for _, tc := range []struct {
		name    string
		bytes   int64
		elapsed time.Duration
		want    string
	}{
		{name: "zero", bytes: 0, elapsed: 10 * time.Second, want: "0 Mbps"},
		{name: "megabits", bytes: 10_000_000, elapsed: 10 * time.Second, want: "8 Mbps"},
		{name: "gigabits", bytes: 1_250_000_000, elapsed: 10 * time.Second, want: "1 Gbps"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := formatBitrate(tc.bytes, tc.elapsed); got != tc.want {
				t.Fatalf("formatBitrate(%d, %s) = %q, want %q", tc.bytes, tc.elapsed, got, tc.want)
			}
		})
	}
}

func TestFormatBytes(t *testing.T) {
	for _, tc := range []struct {
		name  string
		bytes int64
		want  string
	}{
		{name: "bytes", bytes: 999, want: "999 B"},
		{name: "kilobytes", bytes: 1500, want: "1.5 KB"},
		{name: "megabytes", bytes: 10_000_000, want: "10 MB"},
		{name: "gigabytes", bytes: 1_250_000_000, want: "1.2 GB"},
		{name: "terabytes", bytes: 2_000_000_000_000, want: "2 TB"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := formatBytes(tc.bytes); got != tc.want {
				t.Fatalf("formatBytes(%d) = %q, want %q", tc.bytes, got, tc.want)
			}
		})
	}
}

func TestDashboardShowsFirstRunUploadInstructions(t *testing.T) {
	cat := openTestCatalog(t)
	rr := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	newTestServer(cat).http.Handler.ServeHTTP(rr, req)
	body := rr.Body.String()
	for _, want := range []string{"Back up your first dataset", "Import public keys from GitHub", "zfs snapshot tank/photos@zs3-first", "ssh [named_source]@&lt;ssh-host&gt;"} {
		if !strings.Contains(body, want) {
			t.Fatalf("dashboard missing %q:\n%s", want, body)
		}
	}
	for _, unwanted := range []string{"ssh-keygen -t ed25519", "ssh-ed25519 AAAA"} {
		if strings.Contains(body, unwanted) {
			t.Fatalf("dashboard unexpectedly contains %q:\n%s", unwanted, body)
		}
	}
}

func TestDashboardShowsFirstUploadInstructionsAfterSSHKey(t *testing.T) {
	ctx := context.Background()
	cat := openTestCatalog(t)
	q := db.New(cat.DB())
	key := testAuthorizedKey(t)
	pub, _, _, _, _ := ssh.ParseAuthorizedKey([]byte(key))
	if err := q.CreateSSHKey(ctx, db.CreateSSHKeyParams{ID: "key_1", Name: "truenas", PublicKey: key, FingerprintSha256: ssh.FingerprintSHA256(pub)}); err != nil {
		t.Fatalf("create key: %v", err)
	}
	server := newTestServer(cat)
	server.SetRestoreSSHCommandPrefix("ssh -p 15227 [named_source]@example.test")
	rr := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	server.http.Handler.ServeHTTP(rr, req)
	body := rr.Body.String()
	for _, want := range []string{"Back up your first dataset", "zfs snapshot tank/photos@zs3-first", "ssh -p 15227 [named_source]@example.test"} {
		if !strings.Contains(body, want) {
			t.Fatalf("dashboard missing %q:\n%s", want, body)
		}
	}
}

func TestStatusPageRendersChecks(t *testing.T) {
	cat := openTestCatalog(t)
	server := newTestServer(cat)

	rr := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/status", nil)
	server.http.Handler.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("status got %d body=%s", rr.Code, rr.Body.String())
	}
	body := rr.Body.String()
	for _, want := range []string{"Status", "Checks", "Catalog", "Storage", "object store is not configured", "Fix this"} {
		if !strings.Contains(body, want) {
			t.Fatalf("status page missing %q:\n%s", want, body)
		}
	}
}

func TestDashboardRequiresSessionWhenAdminPasswordSet(t *testing.T) {
	cat := openTestCatalog(t)
	server := New(":0", cat, slog.New(slog.NewTextHandler(io.Discard, nil)), "secret")

	rr := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.Header.Set("Accept", "application/json")
	server.http.Handler.ServeHTTP(rr, req)
	if rr.Code != http.StatusUnauthorized {
		t.Fatalf("unauthenticated status got %d", rr.Code)
	}

	rr = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodGet, "/healthz", nil)
	server.http.Handler.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("healthz status got %d", rr.Code)
	}
}

func TestFormLoginWhenAdminPasswordSet(t *testing.T) {
	cat := openTestCatalog(t)
	server := New(":0", cat, slog.New(slog.NewTextHandler(io.Discard, nil)), "secret-password")

	rr := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.Header.Set("Accept", "text/html")
	server.http.Handler.ServeHTTP(rr, req)
	if rr.Code != http.StatusSeeOther || rr.Header().Get("Location") != "/login" {
		t.Fatalf("unauthenticated got status=%d location=%q", rr.Code, rr.Header().Get("Location"))
	}

	rr = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodPost, "/login", strings.NewReader("password=secret-password"))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	server.http.Handler.ServeHTTP(rr, req)
	cookie := rr.Header().Get("Set-Cookie")
	if rr.Code != http.StatusSeeOther || cookie == "" {
		t.Fatalf("login got status=%d cookie=%q", rr.Code, cookie)
	}

	rr = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodGet, "/", nil)
	req.Header.Set("Cookie", cookie)
	server.http.Handler.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("session-authenticated status got %d body=%s", rr.Code, rr.Body.String())
	}
}

func TestFormLoginRateLimit(t *testing.T) {
	cat := openTestCatalog(t)
	server := New(":0", cat, slog.New(slog.NewTextHandler(io.Discard, nil)), "secret-password")

	for i := 0; i < 5; i++ {
		rr := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodPost, "/login", strings.NewReader("password=wrong"))
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		server.http.Handler.ServeHTTP(rr, req)
		if rr.Code != http.StatusSeeOther {
			t.Fatalf("failed login %d status got %d body=%s", i+1, rr.Code, rr.Body.String())
		}
	}

	rr := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/login", strings.NewReader("password=secret-password"))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	server.http.Handler.ServeHTTP(rr, req)
	if rr.Code != http.StatusTooManyRequests {
		t.Fatalf("rate-limited login status got %d body=%s", rr.Code, rr.Body.String())
	}
}

func TestForwardedProtoSetsSecureCookie(t *testing.T) {
	cat := openTestCatalog(t)
	server := New(":0", cat, slog.New(slog.NewTextHandler(io.Discard, nil)), "secret-password")

	rr := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/login", strings.NewReader("password=secret-password"))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set("X-Forwarded-Proto", "https")
	server.http.Handler.ServeHTTP(rr, req)
	if !strings.Contains(strings.ToLower(rr.Header().Get("Set-Cookie")), "secure") {
		t.Fatalf("forwarded https did not set secure cookie: %q", rr.Header().Get("Set-Cookie"))
	}
}

func TestMetricsRequireAPIToken(t *testing.T) {
	ctx := context.Background()
	cat := openTestCatalog(t)
	server := New(":0", cat, slog.New(slog.NewTextHandler(io.Discard, nil)), "secret-password")
	token, _, err := server.createAPIToken(ctx, "prometheus")
	if err != nil {
		t.Fatalf("create api token: %v", err)
	}

	rr := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	server.http.Handler.ServeHTTP(rr, req)
	if rr.Code != http.StatusUnauthorized {
		t.Fatalf("unauthenticated metrics status got %d", rr.Code)
	}

	login := httptest.NewRecorder()
	loginReq := httptest.NewRequest(http.MethodPost, "/login", strings.NewReader("password=secret-password"))
	loginReq.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	server.http.Handler.ServeHTTP(login, loginReq)
	cookie := login.Header().Get("Set-Cookie")
	if cookie == "" {
		t.Fatalf("login did not set a session cookie: status=%d body=%s", login.Code, login.Body.String())
	}

	rr = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodGet, "/metrics", nil)
	req.Header.Set("Cookie", cookie)
	server.http.Handler.ServeHTTP(rr, req)
	if rr.Code != http.StatusUnauthorized {
		t.Fatalf("cookie-authenticated metrics status got %d", rr.Code)
	}

	rr = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodGet, "/metrics", nil)
	req.Header.Set("Authorization", "Bearer "+token)
	server.http.Handler.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("bearer metrics status got %d body=%s", rr.Code, rr.Body.String())
	}
	body := rr.Body.String()
	for _, want := range []string{"# TYPE zfs_s3end_sources gauge", "zfs_s3end_committed_snapshots", "zfs_s3end_active_uploads"} {
		if !strings.Contains(body, want) {
			t.Fatalf("metrics body missing %q:\n%s", want, body)
		}
	}
}

func TestAPITokenCreationAndBearerAPIAuth(t *testing.T) {
	cat := openTestCatalog(t)
	server := New(":0", cat, slog.New(slog.NewTextHandler(io.Discard, nil)), "secret-password")

	rr := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/api/v1/datasets", nil)
	server.http.Handler.ServeHTTP(rr, req)
	if rr.Code != http.StatusUnauthorized {
		t.Fatalf("unauthenticated API status got %d", rr.Code)
	}

	login := httptest.NewRecorder()
	loginReq := httptest.NewRequest(http.MethodPost, "/login", strings.NewReader("password=secret-password"))
	loginReq.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	server.http.Handler.ServeHTTP(login, loginReq)
	cookie := login.Header().Get("Set-Cookie")
	if cookie == "" {
		t.Fatalf("login did not set a session cookie: status=%d body=%s", login.Code, login.Body.String())
	}

	rr = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodPost, "/api/v1/api-tokens", strings.NewReader(`{"name":"automation"}`))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Cookie", cookie)
	server.http.Handler.ServeHTTP(rr, req)
	if rr.Code != http.StatusForbidden {
		t.Fatalf("create token without csrf status got %d body=%s", rr.Code, rr.Body.String())
	}

	rr = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodPost, "/api/v1/api-tokens", strings.NewReader(`{"name":"automation"}`))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Cookie", cookie)
	req.Header.Set("X-CSRF-Token", server.csrfTokenForSession(sessionCookieValue(t, cookie)))
	server.http.Handler.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("create token status got %d body=%s", rr.Code, rr.Body.String())
	}
	var created struct {
		Token string `json:"token"`
	}
	if err := json.Unmarshal(rr.Body.Bytes(), &created); err != nil {
		t.Fatalf("parse token response: %v", err)
	}
	if !strings.HasPrefix(created.Token, "zs3_") {
		t.Fatalf("token got %q", created.Token)
	}

	rr = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodGet, "/api/v1/datasets", nil)
	req.Header.Set("Authorization", "Bearer "+created.Token)
	server.http.Handler.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("bearer API status got %d body=%s", rr.Code, rr.Body.String())
	}
	var lastUsed sql.NullString
	if err := cat.DB().QueryRowContext(context.Background(), `SELECT last_used_at FROM api_tokens WHERE name = 'automation'`).Scan(&lastUsed); err != nil {
		t.Fatalf("query api token: %v", err)
	}
	if !lastUsed.Valid || lastUsed.String == "" {
		t.Fatalf("api token last_used_at was not updated")
	}
}

func TestFirstSetupCreatesAdminPasswordAndSession(t *testing.T) {
	cat := openTestCatalog(t)
	server := New(":0", cat, slog.New(slog.NewTextHandler(io.Discard, nil)))

	rr := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	server.http.Handler.ServeHTTP(rr, req)
	if rr.Code != http.StatusSeeOther || rr.Header().Get("Location") != "/setup" {
		t.Fatalf("unconfigured root got status=%d location=%q", rr.Code, rr.Header().Get("Location"))
	}

	rr = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodPost, "/setup", strings.NewReader("password=correct-horse-battery&confirm_password=correct-horse-battery"))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	server.http.Handler.ServeHTTP(rr, req)
	cookie := rr.Header().Get("Set-Cookie")
	if rr.Code != http.StatusSeeOther || cookie == "" {
		t.Fatalf("setup got status=%d cookie=%q body=%s", rr.Code, cookie, rr.Body.String())
	}

	rr = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodGet, "/", nil)
	req.Header.Set("Cookie", cookie)
	server.http.Handler.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("authenticated root got status=%d body=%s", rr.Code, rr.Body.String())
	}
}

func TestImportSSHKeysFromGitHub(t *testing.T) {
	ctx := context.Background()
	cat := openTestCatalog(t)
	key := testAuthorizedKey(t)
	github := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/octocat.keys" {
			http.NotFound(w, r)
			return
		}
		_, _ = w.Write([]byte(key + "\n"))
	}))
	defer github.Close()
	oldGitHubKeysBaseURL := githubKeysBaseURL
	githubKeysBaseURL = github.URL
	t.Cleanup(func() { githubKeysBaseURL = oldGitHubKeysBaseURL })

	form := url.Values{"github_username": {"octocat"}}
	rr := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/settings/ssh-keys/github", strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	newTestServer(cat).http.Handler.ServeHTTP(rr, req)
	if rr.Code != http.StatusSeeOther {
		t.Fatalf("status got %d body=%s", rr.Code, rr.Body.String())
	}
	keys, err := db.New(cat.DB()).ListSSHKeys(ctx)
	if err != nil {
		t.Fatalf("list keys: %v", err)
	}
	if len(keys) != 1 || keys[0].Name != "github:octocat:1" {
		t.Fatalf("keys = %+v", keys)
	}
}

func TestDeleteSSHKey(t *testing.T) {
	ctx := context.Background()
	cat := openTestCatalog(t)
	q := db.New(cat.DB())
	key := testAuthorizedKey(t)
	pub, _, _, _, _ := ssh.ParseAuthorizedKey([]byte(key))
	if err := q.CreateSSHKey(ctx, db.CreateSSHKeyParams{ID: "key_1", Name: "nas", PublicKey: key, FingerprintSha256: ssh.FingerprintSHA256(pub)}); err != nil {
		t.Fatalf("create key: %v", err)
	}

	rr := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/settings/ssh-keys/key_1/delete", nil)
	newTestServer(cat).http.Handler.ServeHTTP(rr, req)
	if rr.Code != http.StatusSeeOther {
		t.Fatalf("status got %d body=%s", rr.Code, rr.Body.String())
	}
	keys, err := q.ListSSHKeys(ctx)
	if err != nil {
		t.Fatalf("list keys: %v", err)
	}
	if len(keys) != 0 {
		t.Fatalf("keys = %+v", keys)
	}
}

func TestAddSSHKey(t *testing.T) {
	ctx := context.Background()
	cat := openTestCatalog(t)
	key := testAuthorizedKey(t)
	form := url.Values{"name": {"nas"}, "public_key": {key}}
	rr := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/settings/ssh-keys", strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	newTestServer(cat).http.Handler.ServeHTTP(rr, req)
	if rr.Code != http.StatusSeeOther {
		t.Fatalf("status got %d body=%s", rr.Code, rr.Body.String())
	}

	keys, err := db.New(cat.DB()).ListSSHKeys(ctx)
	if err != nil {
		t.Fatalf("list keys: %v", err)
	}
	if len(keys) != 1 || keys[0].Name != "nas" {
		t.Fatalf("keys = %+v", keys)
	}
	pub, _, _, _, _ := ssh.ParseAuthorizedKey([]byte(key))
	if keys[0].FingerprintSha256 != ssh.FingerprintSHA256(pub) {
		t.Fatalf("fingerprint got %q", keys[0].FingerprintSha256)
	}
}

func openTestCatalog(t *testing.T) *catalog.Catalog {
	t.Helper()
	cat, err := catalog.Open(filepath.Join(t.TempDir(), "catalog.db"))
	if err != nil {
		t.Fatalf("open catalog: %v", err)
	}
	t.Cleanup(func() { _ = cat.Close() })
	if err := cat.Migrate(); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	return cat
}

func newTestServer(cat *catalog.Catalog) *Server {
	server := New(":0", cat, slog.New(slog.NewTextHandler(io.Discard, nil)))
	settingsManager, err := appsettings.New(context.Background(), cat.DB(), appsettings.Overrides{})
	if err != nil {
		panic(err)
	}
	server.SetSettingsManager(settingsManager)
	server.disableAuth = true
	return server
}

func sessionCookieValue(t *testing.T, setCookie string) string {
	t.Helper()
	for _, part := range strings.Split(setCookie, ";") {
		part = strings.TrimSpace(part)
		if strings.HasPrefix(part, "zfs_s3end_session=") {
			return strings.TrimPrefix(part, "zfs_s3end_session=")
		}
	}
	t.Fatalf("zfs_s3end_session cookie not found in %q", setCookie)
	return ""
}

func testAuthorizedKey(t *testing.T) string {
	t.Helper()
	_, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}
	signer, err := ssh.NewSignerFromKey(privateKey)
	if err != nil {
		t.Fatalf("signer: %v", err)
	}
	return strings.TrimSpace(string(ssh.MarshalAuthorizedKey(signer.PublicKey())))
}

func testTime() time.Time {
	return time.Date(2026, 7, 3, 12, 0, 0, 0, time.UTC)
}
