package validation

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"io"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/lawrencegripper/zfs-s3nd/internal/catalog"
	"github.com/lawrencegripper/zfs-s3nd/internal/catalog/db"
	"github.com/lawrencegripper/zfs-s3nd/internal/ingest"
	"github.com/lawrencegripper/zfs-s3nd/internal/manifest"
	"github.com/lawrencegripper/zfs-s3nd/internal/storage"
)

func TestValidateChainChecksIncrementalObjectChainAndGUIDs(t *testing.T) {
	ctx := context.Background()
	cat := openTestCatalog(t)
	store := storage.NewMemoryStore()
	full, inc := createCommittedChain(t, ctx, cat, store)

	checker := &fakeChecker{guids: map[string]StreamGUIDs{
		full.ManifestKey: {ToGUID: "0xaaa"},
		inc.ManifestKey:  {FromGUID: "0xaaa", ToGUID: "0xbbb"},
	}}
	if err := (Validator{DB: cat.DB(), Store: store, Checker: checker}).ValidateChain(ctx, inc.SnapshotID); err != nil {
		t.Fatalf("validate chain: %v", err)
	}
	if got, want := checker.checked, []string{full.ManifestKey, inc.ManifestKey}; fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("checked manifests got %v want %v", got, want)
	}
}

func TestValidateSnapshotReadsEachChunkOnce(t *testing.T) {
	ctx := context.Background()
	cat := openTestCatalog(t)
	baseStore := storage.NewMemoryStore()
	full, _ := createCommittedChain(t, ctx, cat, baseStore)
	store := &countingReaderStore{Store: baseStore}

	if err := (Validator{DB: cat.DB(), Store: store, Checker: &fakeChecker{}}).ValidateSnapshot(ctx, full.SnapshotID); err != nil {
		t.Fatalf("validate snapshot: %v", err)
	}
	if store.readerCalls != len(full.Chunks) {
		t.Fatalf("chunk reads got %d want %d", store.readerCalls, len(full.Chunks))
	}
}

func TestValidateSnapshotChecksOnlyCurrentStreamAndParentGUID(t *testing.T) {
	ctx := context.Background()
	cat := openTestCatalog(t)
	store := storage.NewMemoryStore()
	full, inc := createCommittedGUIDChain(t, ctx, cat, store)
	manifestBytes, err := store.GetBytes(ctx, full.ManifestKey)
	if err != nil {
		t.Fatalf("get full manifest: %v", err)
	}
	mani, err := manifest.Unmarshal(manifestBytes)
	if err != nil {
		t.Fatalf("parse full manifest: %v", err)
	}
	if err := store.Delete(ctx, mani.Chunks[0].ObjectKey); err != nil {
		t.Fatalf("delete parent chunk: %v", err)
	}

	checker := &fakeChecker{guids: map[string]StreamGUIDs{
		inc.ManifestKey: {FromGUID: "0xaaa", ToGUID: "0xbbb"},
	}}
	if err := (Validator{DB: cat.DB(), Store: store, Checker: checker}).ValidateSnapshot(ctx, inc.SnapshotID); err != nil {
		t.Fatalf("validate snapshot: %v", err)
	}
	if got, want := checker.checked, []string{inc.ManifestKey}; fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("checked manifests got %v want %v", got, want)
	}
}

func TestValidateSnapshotFailsWhenParentGUIDMismatches(t *testing.T) {
	ctx := context.Background()
	cat := openTestCatalog(t)
	store := storage.NewMemoryStore()
	_, inc := createCommittedGUIDChain(t, ctx, cat, store)
	if _, err := cat.DB().ExecContext(ctx, `UPDATE snapshots SET stream_from_guid = '' WHERE id = ?`, inc.SnapshotID); err != nil {
		t.Fatalf("clear catalog fromguid: %v", err)
	}

	err := (Validator{DB: cat.DB(), Store: store, Checker: &fakeChecker{guids: map[string]StreamGUIDs{
		inc.ManifestKey: {FromGUID: "0xdead", ToGUID: "0xbbb"},
	}}}).ValidateSnapshot(ctx, inc.SnapshotID)
	if err == nil || !strings.Contains(err.Error(), "fromguid 0xdead does not match parent toguid 0xaaa") {
		t.Fatalf("error got %v want parent guid mismatch", err)
	}
}

func TestRunSnapshotRecordsSingleStreamValidation(t *testing.T) {
	ctx := context.Background()
	cat := openTestCatalog(t)
	store := storage.NewMemoryStore()
	_, inc := createCommittedGUIDChain(t, ctx, cat, store)

	if err := (Runner{DB: cat.DB(), Store: store, Checker: &fakeChecker{guids: map[string]StreamGUIDs{
		inc.ManifestKey: {FromGUID: "0xaaa", ToGUID: "0xbbb"},
	}}, Executor: "local"}).RunSnapshot(ctx, inc.SnapshotID); err != nil {
		t.Fatalf("run snapshot: %v", err)
	}
	jobs, err := db.New(cat.DB()).ListLatestValidationJobs(ctx, 1)
	if err != nil {
		t.Fatalf("list validation jobs: %v", err)
	}
	if len(jobs) != 1 || jobs[0].Status != "succeeded" || jobs[0].ResultSummary.String != "snapshot stream validation succeeded" {
		t.Fatalf("unexpected jobs: %+v", jobs)
	}
	var streamStatus, chainStatus string
	if err := cat.DB().QueryRowContext(ctx, `SELECT stream_validation_status, chain_validation_status FROM snapshots WHERE id = ?`, inc.SnapshotID).Scan(&streamStatus, &chainStatus); err != nil {
		t.Fatalf("query validation statuses: %v", err)
	}
	if streamStatus != "succeeded" || chainStatus != "pending" {
		t.Fatalf("statuses stream=%q chain=%q", streamStatus, chainStatus)
	}
}

func TestValidateChainFailureDoesNotMarkStreamValidationFailed(t *testing.T) {
	ctx := context.Background()
	cat := openTestCatalog(t)
	store := storage.NewMemoryStore()
	_, inc := createCommittedGUIDChain(t, ctx, cat, store)

	result, err := (Runner{DB: cat.DB(), Store: store, Checker: &fakeChecker{guids: map[string]StreamGUIDs{
		inc.ManifestKey: {FromGUID: "0xdead", ToGUID: "0xbbb"},
	}}}).RunDue(ctx, 10)
	if err != nil {
		t.Fatalf("run due: %v", err)
	}
	if result.Failed == 0 {
		t.Fatalf("expected chain validation failure, result=%+v", result)
	}
	var streamStatus, chainStatus string
	if err := cat.DB().QueryRowContext(ctx, `SELECT stream_validation_status, chain_validation_status FROM snapshots WHERE id = ?`, inc.SnapshotID).Scan(&streamStatus, &chainStatus); err != nil {
		t.Fatalf("query validation statuses: %v", err)
	}
	if streamStatus != "pending" || chainStatus != "failed" {
		t.Fatalf("statuses stream=%q chain=%q", streamStatus, chainStatus)
	}
}

func TestValidateChainFailsWhenAncestorNotCommitted(t *testing.T) {
	ctx := context.Background()
	cat := openTestCatalog(t)
	store := storage.NewMemoryStore()
	full, inc := createCommittedChain(t, ctx, cat, store)
	if _, err := cat.DB().ExecContext(ctx, `UPDATE snapshots SET status = 'failed' WHERE id = ?`, full.SnapshotID); err != nil {
		t.Fatalf("mark base failed: %v", err)
	}

	err := (Validator{DB: cat.DB(), Store: store, Checker: &fakeChecker{}}).ValidateChain(ctx, inc.SnapshotID)
	if err == nil || !strings.Contains(err.Error(), "snap1 is failed, not committed") {
		t.Fatalf("error got %v want uncommitted ancestor", err)
	}
}

func TestValidateChainFailsWhenChunkMissing(t *testing.T) {
	ctx := context.Background()
	cat := openTestCatalog(t)
	store := storage.NewMemoryStore()
	full, inc := createCommittedChain(t, ctx, cat, store)
	manifestBytes, err := store.GetBytes(ctx, full.ManifestKey)
	if err != nil {
		t.Fatalf("get manifest: %v", err)
	}
	mani, err := manifest.Unmarshal(manifestBytes)
	if err != nil {
		t.Fatalf("parse manifest: %v", err)
	}
	if err := store.Delete(ctx, mani.Chunks[0].ObjectKey); err != nil {
		t.Fatalf("delete chunk: %v", err)
	}

	err = (Validator{DB: cat.DB(), Store: store, Checker: &fakeChecker{}}).ValidateChain(ctx, inc.SnapshotID)
	if err == nil || !strings.Contains(err.Error(), "chunk 0") || !strings.Contains(err.Error(), "not found") {
		t.Fatalf("error got %v want missing chunk", err)
	}
}

func TestValidateSnapshotFailsWhenManifestStreamHashDoesNotMatchChunks(t *testing.T) {
	ctx := context.Background()
	cat := openTestCatalog(t)
	store := storage.NewMemoryStore()
	full, _ := createCommittedChain(t, ctx, cat, store)
	manifestBytes, err := store.GetBytes(ctx, full.ManifestKey)
	if err != nil {
		t.Fatalf("get manifest: %v", err)
	}
	mani, err := manifest.Unmarshal(manifestBytes)
	if err != nil {
		t.Fatalf("parse manifest: %v", err)
	}
	mani.Stream.SHA256 = strings.Repeat("0", 64)
	rewritten, err := mani.MarshalCanonical()
	if err != nil {
		t.Fatalf("marshal manifest: %v", err)
	}
	if _, err := store.PutBytes(ctx, full.ManifestKey, rewritten); err != nil {
		t.Fatalf("put manifest: %v", err)
	}

	err = (Validator{DB: cat.DB(), Store: store, Checker: &fakeChecker{}}).ValidateSnapshot(ctx, full.SnapshotID)
	if err == nil || !strings.Contains(err.Error(), "stream sha256 mismatch") {
		t.Fatalf("error got %v want stream sha mismatch", err)
	}
}

func TestValidateChainFailsWhenManifestLineageDoesNotMatchCatalogChain(t *testing.T) {
	ctx := context.Background()
	cat := openTestCatalog(t)
	store := storage.NewMemoryStore()
	_, inc := createCommittedChain(t, ctx, cat, store)
	manifestBytes, err := store.GetBytes(ctx, inc.ManifestKey)
	if err != nil {
		t.Fatalf("get manifest: %v", err)
	}
	mani, err := manifest.Unmarshal(manifestBytes)
	if err != nil {
		t.Fatalf("parse manifest: %v", err)
	}
	mani.Lineage.BaseSnapshot = "wrong-base"
	rewritten, err := mani.MarshalCanonical()
	if err != nil {
		t.Fatalf("marshal manifest: %v", err)
	}
	if _, err := store.PutBytes(ctx, inc.ManifestKey, rewritten); err != nil {
		t.Fatalf("put manifest: %v", err)
	}

	err = (Validator{DB: cat.DB(), Store: store, Checker: &fakeChecker{}}).ValidateChain(ctx, inc.SnapshotID)
	if err == nil || !strings.Contains(err.Error(), "manifest base wrong-base does not match previous snapshot snap1") {
		t.Fatalf("error got %v want lineage mismatch", err)
	}
}

func TestValidateChainFailsWhenIncrementalGUIDDoesNotMatchPreviousSnapshot(t *testing.T) {
	ctx := context.Background()
	cat := openTestCatalog(t)
	store := storage.NewMemoryStore()
	full, inc := createCommittedChain(t, ctx, cat, store)

	err := (Validator{DB: cat.DB(), Store: store, Checker: &fakeChecker{guids: map[string]StreamGUIDs{
		full.ManifestKey: {ToGUID: "0xaaa"},
		inc.ManifestKey:  {FromGUID: "0xdead", ToGUID: "0xbbb"},
	}}}).ValidateChain(ctx, inc.SnapshotID)
	if err == nil || !strings.Contains(err.Error(), "fromguid 0xdead does not match previous toguid 0xaaa") {
		t.Fatalf("error got %v want guid mismatch", err)
	}
}

func TestRunDueRecordsValidationStatusAndJobs(t *testing.T) {
	ctx := context.Background()
	cat := openTestCatalog(t)
	store := storage.NewMemoryStore()
	full, inc := createCommittedChain(t, ctx, cat, store)

	result, err := (Runner{DB: cat.DB(), Store: store, Checker: &fakeChecker{guids: map[string]StreamGUIDs{
		full.ManifestKey: {ToGUID: "0xaaa"},
		inc.ManifestKey:  {FromGUID: "0xaaa", ToGUID: "0xbbb"},
	}}, Executor: "railway_sandbox"}).RunDue(ctx, 10)
	if err != nil {
		t.Fatalf("run due: %v", err)
	}
	if result.Checked != 2 || result.Succeeded != 2 || result.Failed != 0 {
		t.Fatalf("result = %+v", result)
	}

	jobs, err := db.New(cat.DB()).ListLatestValidationJobs(ctx, 10)
	if err != nil {
		t.Fatalf("list validation jobs: %v", err)
	}
	if len(jobs) != 2 {
		t.Fatalf("jobs got %d want 2", len(jobs))
	}
	for _, job := range jobs {
		if job.Status != "succeeded" || job.ResultSummary.String != "restore chain validation succeeded" || job.Executor != "railway_sandbox" || job.Type != "restore_check" {
			t.Fatalf("unexpected job: %+v", job)
		}
	}
	var streamStatus, chainStatus string
	if err := cat.DB().QueryRowContext(ctx, `SELECT stream_validation_status, chain_validation_status FROM snapshots WHERE id = ?`, inc.SnapshotID).Scan(&streamStatus, &chainStatus); err != nil {
		t.Fatalf("query validation statuses: %v", err)
	}
	if streamStatus != "pending" || chainStatus != "succeeded" {
		t.Fatalf("statuses stream=%q chain=%q", streamStatus, chainStatus)
	}
	failed, err := db.New(cat.DB()).CountFailedValidations(ctx)
	if err != nil {
		t.Fatalf("count failed validations: %v", err)
	}
	if failed != 0 {
		t.Fatalf("failed validations = %d", failed)
	}
}

func TestRunDueRecordsValidationFailure(t *testing.T) {
	ctx := context.Background()
	cat := openTestCatalog(t)
	store := storage.NewMemoryStore()
	full, inc := createCommittedChain(t, ctx, cat, store)

	result, err := (Runner{DB: cat.DB(), Store: store, Checker: &fakeChecker{guids: map[string]StreamGUIDs{
		full.ManifestKey: {ToGUID: "0xaaa"},
		inc.ManifestKey:  {FromGUID: "0xdead", ToGUID: "0xbbb"},
	}}}).RunDue(ctx, 10)
	if err != nil {
		t.Fatalf("run due: %v", err)
	}
	if result.Checked != 2 || result.Succeeded != 1 || result.Failed != 1 {
		t.Fatalf("result = %+v", result)
	}
	jobs, err := db.New(cat.DB()).ListLatestValidationJobs(ctx, 10)
	if err != nil {
		t.Fatalf("list validation jobs: %v", err)
	}
	var failedJob *db.ListLatestValidationJobsRow
	for i := range jobs {
		if jobs[i].Status == "failed" {
			failedJob = &jobs[i]
		}
	}
	if failedJob == nil || !strings.Contains(failedJob.ResultSummary.String, "fromguid 0xdead") {
		t.Fatalf("failed job = %+v jobs=%+v", failedJob, jobs)
	}
	failed, err := db.New(cat.DB()).CountFailedValidations(ctx)
	if err != nil {
		t.Fatalf("count failed validations: %v", err)
	}
	if failed != 1 {
		t.Fatalf("failed validations = %d", failed)
	}
}

func TestReconcileStaleJobsFailsRunningValidationState(t *testing.T) {
	ctx := context.Background()
	cat := openTestCatalog(t)
	store := storage.NewMemoryStore()
	_, inc := createCommittedGUIDChain(t, ctx, cat, store)
	old := catalog.FormatTime(time.Now().Add(-time.Hour))
	q := db.New(cat.DB())
	if err := q.CreateValidationJob(ctx, db.CreateValidationJobParams{ID: "val_stale", SnapshotID: sql.NullString{String: inc.SnapshotID, Valid: true}, Type: "restore_check", Executor: "local", ResultSummary: sql.NullString{String: "running", Valid: true}}); err != nil {
		t.Fatalf("create validation job: %v", err)
	}
	if _, err := cat.DB().ExecContext(ctx, `UPDATE validation_jobs SET started_at = ? WHERE id = 'val_stale'`, old); err != nil {
		t.Fatalf("age validation job: %v", err)
	}
	if err := q.CreateValidationOperation(ctx, db.CreateValidationOperationParams{ID: "op_stale", SnapshotID: sql.NullString{String: inc.SnapshotID, Valid: true}, ValidationJobID: sql.NullString{String: "val_stale", Valid: true}, Summary: sql.NullString{String: "running", Valid: true}}); err != nil {
		t.Fatalf("create validation operation: %v", err)
	}
	if _, err := cat.DB().ExecContext(ctx, `UPDATE operations SET started_at = ? WHERE id = 'op_stale'`, old); err != nil {
		t.Fatalf("age operation: %v", err)
	}
	if err := q.UpdateSnapshotChainValidationStatus(ctx, db.UpdateSnapshotChainValidationStatusParams{ID: inc.SnapshotID, ChainValidationStatus: "running"}); err != nil {
		t.Fatalf("mark chain running: %v", err)
	}

	result, err := ReconcileStaleJobs(ctx, cat.DB(), time.Now().Add(-10*time.Minute))
	if err != nil {
		t.Fatalf("reconcile stale jobs: %v", err)
	}
	if result.JobsFailed != 1 {
		t.Fatalf("result = %+v", result)
	}
	var jobStatus, opStatus, chainStatus string
	if err := cat.DB().QueryRowContext(ctx, `SELECT status FROM validation_jobs WHERE id = 'val_stale'`).Scan(&jobStatus); err != nil {
		t.Fatalf("query job: %v", err)
	}
	if err := cat.DB().QueryRowContext(ctx, `SELECT status FROM operations WHERE id = 'op_stale'`).Scan(&opStatus); err != nil {
		t.Fatalf("query operation: %v", err)
	}
	if err := cat.DB().QueryRowContext(ctx, `SELECT chain_validation_status FROM snapshots WHERE id = ?`, inc.SnapshotID).Scan(&chainStatus); err != nil {
		t.Fatalf("query chain status: %v", err)
	}
	if jobStatus != "failed" || opStatus != "failed" || chainStatus != "failed" {
		t.Fatalf("statuses job=%q op=%q chain=%q", jobStatus, opStatus, chainStatus)
	}
}

func TestZstreamdumpBeginRecords(t *testing.T) {
	if got := zstreamdumpBeginRecords("Total DRR_BEGIN records = 0 (0 bytes)"); got != 0 {
		t.Fatalf("begin records got %d want 0", got)
	}
	if got := zstreamdumpBeginRecords("Total DRR_BEGIN records = 1 (312 bytes)"); got != 1 {
		t.Fatalf("begin records got %d want 1", got)
	}
}

func TestParseZstreamdumpGUIDs(t *testing.T) {
	output := `BEGIN record
	fromguid = 0xaaa
	toguid = 0xbbb
END record`
	got := ParseZstreamdumpGUIDs(output)
	if got.FromGUID != "0xaaa" || got.ToGUID != "0xbbb" {
		t.Fatalf("guids = %+v", got)
	}
}

func TestParseZstreamdumpGUIDsNormalizesEquivalentHexFormats(t *testing.T) {
	output := `BEGIN record
	fromguid = 4A593502E3560C5E
	toguid = 00e17067bc72fe7f93
END record`
	got := ParseZstreamdumpGUIDs(output)
	if got.FromGUID != "0x4a593502e3560c5e" || got.ToGUID != "0xe17067bc72fe7f93" {
		t.Fatalf("guids = %+v", got)
	}
}

type countingReaderStore struct {
	storage.Store
	readerCalls int
}

func (s *countingReaderStore) GetReader(ctx context.Context, key string) (io.ReadCloser, error) {
	s.readerCalls++
	return s.Store.GetReader(ctx, key)
}

type fakeChecker struct {
	guids   map[string]StreamGUIDs
	checked []string
}

func (f *fakeChecker) Check(_ context.Context, manifestKey string, stream io.Reader) (StreamGUIDs, error) {
	data, err := io.ReadAll(stream)
	if err != nil {
		return StreamGUIDs{}, err
	}
	if len(data) == 0 {
		return StreamGUIDs{}, fmt.Errorf("empty stream")
	}
	f.checked = append(f.checked, manifestKey)
	return f.guids[manifestKey], nil
}

func createCommittedGUIDChain(t *testing.T, ctx context.Context, cat *catalog.Catalog, store *storage.MemoryStore) (ingest.Result, ingest.Result) {
	t.Helper()
	svc := ingest.Service{Repo: catalog.NewRepository(cat.DB()), Store: store, Keys: storage.KeyBuilder{RootPrefix: "root"}, ChunkSize: 5}
	full, err := svc.Receive(ctx, ingest.Request{Source: "nas-home", Pool: "tank", Dataset: "photos", Snapshot: "snap1", ToGUID: "0xaaa"}, bytes.NewReader([]byte("full-stream")))
	if err != nil {
		t.Fatalf("receive full: %v", err)
	}
	inc, err := svc.Receive(ctx, ingest.Request{Source: "nas-home", Pool: "tank", Dataset: "photos", Snapshot: "snap2", BaseSnapshot: "snap1", FromGUID: "0xaaa", ToGUID: "0xbbb"}, bytes.NewReader([]byte("incremental-stream")))
	if err != nil {
		t.Fatalf("receive incremental: %v", err)
	}
	return full, inc
}

func createCommittedChain(t *testing.T, ctx context.Context, cat *catalog.Catalog, store *storage.MemoryStore) (ingest.Result, ingest.Result) {
	t.Helper()
	svc := ingest.Service{Repo: catalog.NewRepository(cat.DB()), Store: store, Keys: storage.KeyBuilder{RootPrefix: "root"}, ChunkSize: 5}
	full, err := svc.Receive(ctx, ingest.Request{Source: "nas-home", Pool: "tank", Dataset: "photos", Snapshot: "snap1"}, bytes.NewReader([]byte("full-stream")))
	if err != nil {
		t.Fatalf("receive full: %v", err)
	}
	inc, err := svc.Receive(ctx, ingest.Request{Source: "nas-home", Pool: "tank", Dataset: "photos", Snapshot: "snap2", BaseSnapshot: "snap1"}, bytes.NewReader([]byte("incremental-stream")))
	if err != nil {
		t.Fatalf("receive incremental: %v", err)
	}
	return full, inc
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
