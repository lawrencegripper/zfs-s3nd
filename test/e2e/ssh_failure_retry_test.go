package e2e

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"database/sql"
	"encoding/binary"
	"fmt"
	"io"
	"log/slog"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"golang.org/x/crypto/ssh"

	"github.com/lawrencegripper/zfs-s3nd/internal/catalog"
	"github.com/lawrencegripper/zfs-s3nd/internal/ingest"
	"github.com/lawrencegripper/zfs-s3nd/internal/sshserver"
	"github.com/lawrencegripper/zfs-s3nd/internal/storage"
	"github.com/lawrencegripper/zfs-s3nd/internal/validation"
	"github.com/lawrencegripper/zfs-s3nd/internal/zfsstream"
)

func TestSSHChunkStorageFailureThenRetry(t *testing.T) {
	ctx := context.Background()
	baseStore := storage.NewMemoryStore()
	failingStore := alwaysFailPutStore{Store: baseStore, err: fmt.Errorf("injected e2e bucket failure")}
	fixture := startSSHFixture(t, 5, failingStore)

	output, err := fixture.runUpload(t, "tank/photos@snap1", 0, 0xaaa, []byte("hello-world"))
	if err == nil {
		t.Fatalf("expected storage failure, output=%s", output)
	}
	if !strings.Contains(string(output), "injected e2e bucket failure") {
		t.Fatalf("output %q does not mention storage failure", output)
	}
	waitForUploadStatus(t, ctx, fixture.cat, "snap1", "failed")
	assertNoCommittedSnapshot(t, ctx, fixture.cat, "snap1")

	fixture.srv.Ingest.Store = baseStore
	output, err = fixture.runUpload(t, "tank/photos@snap1", 0, 0xaaa, []byte("hello-world"))
	if err != nil {
		t.Fatalf("retry upload failed: %v output=%s", err, output)
	}
	if !strings.Contains(string(output), "ok snapshot=") {
		t.Fatalf("unexpected retry output: %s", output)
	}
	waitForSnapshotStatus(t, ctx, fixture.cat, "snap1", "committed")
}

func TestSSHManifestStorageFailureThenRetry(t *testing.T) {
	ctx := context.Background()
	baseStore := storage.NewMemoryStore()
	failingStore := manifestFailPutStore{Store: baseStore, err: fmt.Errorf("injected e2e manifest failure")}
	fixture := startSSHFixture(t, 1024, failingStore)

	output, err := fixture.runUpload(t, "tank/photos@snap1", 0, 0xaaa, []byte("hello-world"))
	if err == nil {
		t.Fatalf("expected manifest failure, output=%s", output)
	}
	if !strings.Contains(string(output), "injected e2e manifest failure") {
		t.Fatalf("output %q does not mention manifest failure", output)
	}
	waitForUploadStatus(t, ctx, fixture.cat, "snap1", "failed")
	assertNoCommittedSnapshot(t, ctx, fixture.cat, "snap1")

	fixture.srv.Ingest.Store = baseStore
	output, err = fixture.runUpload(t, "tank/photos@snap1", 0, 0xaaa, []byte("hello-world"))
	if err != nil {
		t.Fatalf("retry upload failed: %v output=%s", err, output)
	}
	if !strings.Contains(string(output), "ok snapshot=") {
		t.Fatalf("unexpected retry output: %s", output)
	}
	waitForSnapshotStatus(t, ctx, fixture.cat, "snap1", "committed")
}

func TestValidationRunDueValidatesUploadedChain(t *testing.T) {
	ctx := context.Background()
	fixture := startSSHFixture(t, 1024, storage.NewMemoryStore())

	output, err := fixture.runUpload(t, "tank/photos@snap1", 0, 0xaaa, []byte("full-stream"))
	if err != nil {
		t.Fatalf("full upload failed: %v output=%s", err, output)
	}
	output, err = fixture.runUpload(t, "tank/photos@snap2", 0xaaa, 0xbbb, []byte("incremental-stream"))
	if err != nil {
		t.Fatalf("incremental upload failed: %v output=%s", err, output)
	}

	runner := validation.Runner{DB: fixture.cat.DB(), Store: fixture.srv.Ingest.Store, Checker: e2eStreamChecker{}, Executor: "local"}
	result, err := runner.RunDue(ctx, 10)
	if err != nil {
		t.Fatalf("run due validation: %v", err)
	}
	if result.Checked != 2 || result.Succeeded != 2 || result.Failed != 0 {
		t.Fatalf("validation result = %+v; jobs=%s", result, validationJobSummaries(t, ctx, fixture.cat))
	}
	waitForChainValidationStatus(t, ctx, fixture.cat, "snap1", "succeeded")
	waitForChainValidationStatus(t, ctx, fixture.cat, "snap2", "succeeded")

	var succeededJobs int
	if err := fixture.cat.DB().QueryRowContext(ctx, `SELECT count(*) FROM validation_jobs WHERE status = 'succeeded' AND type = 'restore_check'`).Scan(&succeededJobs); err != nil {
		t.Fatalf("count validation jobs: %v", err)
	}
	if succeededJobs != 2 {
		t.Fatalf("succeeded validation jobs got %d want 2", succeededJobs)
	}
}

func TestSSHCatalogCommitFailureThenRetry(t *testing.T) {
	ctx := context.Background()
	fixture := startSSHFixture(t, 1024, storage.NewMemoryStore())
	fixture.srv.Ingest.BeforeCatalogCommit = func(ctx context.Context, commit ingest.StartedCommit) error {
		_, err := fixture.cat.DB().ExecContext(ctx, `
CREATE TRIGGER fail_snapshot_commit_e2e
BEFORE UPDATE OF status ON snapshots
WHEN NEW.status = 'committed'
BEGIN
  SELECT RAISE(ABORT, 'forced e2e catalog commit failure');
END;`)
		return err
	}

	output, err := fixture.runUpload(t, "tank/photos@snap1", 0, 0xaaa, []byte("hello-world"))
	if err == nil {
		t.Fatalf("expected catalog commit failure, output=%s", output)
	}
	if !strings.Contains(string(output), "forced e2e catalog commit failure") {
		t.Fatalf("output %q does not mention catalog commit failure", output)
	}
	waitForUploadStatus(t, ctx, fixture.cat, "snap1", "failed")
	assertNoCommittedSnapshot(t, ctx, fixture.cat, "snap1")

	if _, err := fixture.cat.DB().ExecContext(ctx, `DROP TRIGGER fail_snapshot_commit_e2e`); err != nil {
		t.Fatalf("drop trigger: %v", err)
	}
	fixture.srv.Ingest.BeforeCatalogCommit = nil
	output, err = fixture.runUpload(t, "tank/photos@snap1", 0, 0xaaa, []byte("hello-world"))
	if err != nil {
		t.Fatalf("retry upload failed: %v output=%s", err, output)
	}
	if !strings.Contains(string(output), "ok snapshot=") {
		t.Fatalf("unexpected retry output: %s", output)
	}
	waitForSnapshotStatus(t, ctx, fixture.cat, "snap1", "committed")
}

type e2eStreamChecker struct{}

func (e2eStreamChecker) Check(_ context.Context, _ string, stream io.Reader) (validation.StreamGUIDs, error) {
	header := make([]byte, zfsstream.BeginRecordSize)
	if _, err := io.ReadFull(stream, header); err != nil {
		return validation.StreamGUIDs{}, err
	}
	begin, err := zfsstream.ParseBegin(header)
	if err != nil {
		return validation.StreamGUIDs{}, err
	}
	_, _ = io.Copy(io.Discard, stream)
	return validation.StreamGUIDs{FromGUID: begin.FromGUID, ToGUID: begin.ToGUID}, nil
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

type sshFixture struct {
	cat          *catalog.Catalog
	srv          *sshserver.Server
	addr         string
	clientSigner ssh.Signer
}

func startSSHFixture(t *testing.T, chunkSize int64, store storage.Store) *sshFixture {
	t.Helper()
	ctx := context.Background()
	cat, err := catalog.Open(filepath.Join(t.TempDir(), "catalog.db"))
	if err != nil {
		t.Fatalf("open catalog: %v", err)
	}
	t.Cleanup(func() { _ = cat.Close() })
	if err := cat.Migrate(); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	_, clientPrivate, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("client key: %v", err)
	}
	clientSigner, err := ssh.NewSignerFromKey(clientPrivate)
	if err != nil {
		t.Fatalf("client signer: %v", err)
	}
	clientPub := clientSigner.PublicKey()
	fingerprint := ssh.FingerprintSHA256(clientPub)
	if _, err := cat.DB().ExecContext(ctx, `INSERT INTO ssh_keys (id, name, public_key, fingerprint_sha256) VALUES ('key_1', 'test', ?, ?)`, string(ssh.MarshalAuthorizedKey(clientPub)), fingerprint); err != nil {
		t.Fatalf("insert ssh key: %v", err)
	}

	_, hostPrivate, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("host key: %v", err)
	}
	hostSigner, err := ssh.NewSignerFromKey(hostPrivate)
	if err != nil {
		t.Fatalf("host signer: %v", err)
	}
	if store == nil {
		store = storage.NewMemoryStore()
	}
	srv := &sshserver.Server{
		Addr:   "127.0.0.1:0",
		Signer: hostSigner,
		DB:     cat.DB(),
		Ingest: ingest.Service{Repo: catalog.NewRepository(cat.DB()), Store: store, Keys: storage.KeyBuilder{RootPrefix: "root"}, ChunkSize: chunkSize, PutRetryMaxElapsed: time.Millisecond, PutRetryInitialBackoff: time.Millisecond},
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
	errCh := make(chan error, 1)
	go func() { errCh <- srv.ListenAndServe() }()
	t.Cleanup(func() {
		_ = srv.Close()
		select {
		case err := <-errCh:
			if err != nil {
				t.Fatalf("server error: %v", err)
			}
		case <-time.After(2 * time.Second):
			t.Fatalf("server did not stop")
		}
	})

	return &sshFixture{cat: cat, srv: srv, addr: waitForAddr(t, srv), clientSigner: clientSigner}
}

func (f *sshFixture) dial(t *testing.T) *ssh.Client {
	t.Helper()
	client, err := ssh.Dial("tcp", f.addr, &ssh.ClientConfig{
		User:            "backup",
		Auth:            []ssh.AuthMethod{ssh.PublicKeys(f.clientSigner)},
		HostKeyCallback: ssh.InsecureIgnoreHostKey(),
		Timeout:         5 * time.Second,
	})
	if err != nil {
		t.Fatalf("ssh dial: %v", err)
	}
	return client
}

func (f *sshFixture) runUpload(t *testing.T, toName string, fromGUID, toGUID uint64, data []byte) ([]byte, error) {
	t.Helper()
	client := f.dial(t)
	defer client.Close()
	channel, requests, err := client.OpenChannel("session", nil)
	if err != nil {
		t.Fatalf("open session channel: %v", err)
	}
	defer channel.Close()
	ok, err := channel.SendRequest("shell", true, nil)
	if err != nil || !ok {
		t.Fatalf("shell request ok=%v err=%v", ok, err)
	}
	exitStatus := make(chan uint32, 1)
	go func() {
		for req := range requests {
			if req.Type == "exit-status" && len(req.Payload) == 4 {
				exitStatus <- binary.BigEndian.Uint32(req.Payload)
				return
			}
		}
	}()
	stream := append(fakeZFSBegin(toName, fromGUID, toGUID), data...)
	if _, err := channel.Write(stream); err != nil {
		t.Fatalf("write stream: %v", err)
	}
	if err := channel.CloseWrite(); err != nil {
		t.Fatalf("close write: %v", err)
	}
	var stderr bytes.Buffer
	stderrDone := make(chan struct{})
	go func() {
		_, _ = io.Copy(&stderr, channel.Stderr())
		close(stderrDone)
	}()
	stdout, readErr := io.ReadAll(channel)
	if readErr != nil {
		t.Fatalf("read output: %v", readErr)
	}
	<-stderrDone
	output := append(stdout, stderr.Bytes()...)
	select {
	case status := <-exitStatus:
		if status != 0 {
			return output, fmt.Errorf("ssh upload failed with exit status %d", status)
		}
		return output, nil
	case <-time.After(5 * time.Second):
		return output, fmt.Errorf("timed out waiting for ssh upload exit status")
	}
}

func fakeZFSBegin(toName string, fromGUID, toGUID uint64) []byte {
	header := make([]byte, 312)
	binary.LittleEndian.PutUint32(header[0:4], 0)
	binary.LittleEndian.PutUint64(header[8:16], 0x2f5bacbac)
	binary.LittleEndian.PutUint64(header[16:24], 1)
	binary.LittleEndian.PutUint64(header[40:48], toGUID)
	binary.LittleEndian.PutUint64(header[48:56], fromGUID)
	copy(header[56:], []byte(toName))
	return header
}

func waitForAddr(t *testing.T, srv *sshserver.Server) string {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		addr := srv.AddrString()
		if addr != "" {
			return addr
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("server did not start")
	return ""
}

func waitForUploadStatus(t *testing.T, ctx context.Context, cat *catalog.Catalog, snapshotName, want string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	var got string
	var reason sql.NullString
	for time.Now().Before(deadline) {
		err := cat.DB().QueryRowContext(ctx, `
SELECT us.status, us.failure_reason
FROM upload_sessions us
JOIN snapshots s ON s.id = us.snapshot_id
WHERE s.name = ?
ORDER BY us.started_at DESC
LIMIT 1`, snapshotName).Scan(&got, &reason)
		if err == nil && got == want {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("upload status for %s got %q reason=%q want %q", snapshotName, got, reason.String, want)
}

func waitForSnapshotStatus(t *testing.T, ctx context.Context, cat *catalog.Catalog, snapshotName, want string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	var got string
	for time.Now().Before(deadline) {
		err := cat.DB().QueryRowContext(ctx, `SELECT status FROM snapshots WHERE name = ? ORDER BY created_at DESC LIMIT 1`, snapshotName).Scan(&got)
		if err == nil && got == want {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("snapshot status for %s got %q want %q", snapshotName, got, want)
}

func waitForChainValidationStatus(t *testing.T, ctx context.Context, cat *catalog.Catalog, snapshotName, want string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	var got string
	for time.Now().Before(deadline) {
		err := cat.DB().QueryRowContext(ctx, `SELECT chain_validation_status FROM snapshots WHERE name = ? ORDER BY created_at DESC LIMIT 1`, snapshotName).Scan(&got)
		if err == nil && got == want {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("chain validation status for %s got %q want %q; jobs=%s", snapshotName, got, want, validationJobSummaries(t, ctx, cat))
}

func validationJobSummaries(t *testing.T, ctx context.Context, cat *catalog.Catalog) string {
	t.Helper()
	rows, err := cat.DB().QueryContext(ctx, `SELECT status, result_summary FROM validation_jobs ORDER BY started_at ASC`)
	if err != nil {
		t.Fatalf("list validation jobs: %v", err)
	}
	defer rows.Close()
	var parts []string
	for rows.Next() {
		var status string
		var summary sql.NullString
		if err := rows.Scan(&status, &summary); err != nil {
			t.Fatalf("scan validation job: %v", err)
		}
		parts = append(parts, status+":"+summary.String)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate validation jobs: %v", err)
	}
	return strings.Join(parts, "; ")
}

func assertNoCommittedSnapshot(t *testing.T, ctx context.Context, cat *catalog.Catalog, snapshotName string) {
	t.Helper()
	var count int
	if err := cat.DB().QueryRowContext(ctx, `SELECT count(*) FROM snapshots WHERE name = ? AND status = 'committed'`, snapshotName).Scan(&count); err != nil {
		t.Fatalf("count committed snapshots: %v", err)
	}
	if count != 0 {
		t.Fatalf("committed snapshots for %s got %d want 0", snapshotName, count)
	}
}
