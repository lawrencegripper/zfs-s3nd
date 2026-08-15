package sshserver

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/binary"
	"fmt"
	"io"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"golang.org/x/crypto/ssh"

	"github.com/lawrencegripper/zfs-s3nd/internal/catalog"
	"github.com/lawrencegripper/zfs-s3nd/internal/ingest"
	"github.com/lawrencegripper/zfs-s3nd/internal/storage"
	"github.com/lawrencegripper/zfs-s3nd/internal/zfsstream"
)

func TestSSHReceiveEndToEnd(t *testing.T) {
	ctx := context.Background()
	cat, err := catalog.Open(filepath.Join(t.TempDir(), "catalog.db"))
	if err != nil {
		t.Fatalf("open catalog: %v", err)
	}
	defer cat.Close()
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

	store := storage.NewMemoryStore()
	srv := &Server{
		Addr:                     "127.0.0.1:0",
		Signer:                   hostSigner,
		DB:                       cat.DB(),
		Ingest:                   ingest.Service{Repo: catalog.NewRepository(cat.DB()), Store: store, Keys: storage.KeyBuilder{RootPrefix: "root"}, ChunkSize: 5},
		UploadRateBytesPerSecond: 1_250_000,
	}
	errCh := make(chan error, 1)
	go func() { errCh <- srv.ListenAndServe() }()
	t.Cleanup(func() { _ = srv.Close() })
	addr := waitForAddr(t, srv)

	client, err := ssh.Dial("tcp", addr, &ssh.ClientConfig{
		User:            "nas-home",
		Auth:            []ssh.AuthMethod{ssh.PublicKeys(clientSigner)},
		HostKeyCallback: ssh.InsecureIgnoreHostKey(),
		Timeout:         5 * time.Second,
	})
	if err != nil {
		t.Fatalf("ssh dial: %v", err)
	}
	defer client.Close()

	session, err := client.NewSession()
	if err != nil {
		t.Fatalf("new session: %v", err)
	}
	session.Stdin = bytes.NewReader([]byte("should-not-be-read"))
	output, err := session.CombinedOutput("receive nas-home tank photos snap1")
	_ = session.Close()
	if err == nil {
		t.Fatalf("expected unsupported receive command before upload, output=%s", output)
	}

	fullStream := append(fakeZFSBegin("tank/auto@s1", 0, 0xaaa, 0), []byte("full-payload")...)
	fullOutput := runShellUpload(t, client, fullStream, "100")
	fullSnapshotID := parseSnapshotID(t, fullOutput)
	if !strings.Contains(fullOutput, "ok snapshot=") {
		t.Fatalf("auto full upload output: %s", fullOutput)
	}
	if !strings.Contains(fullOutput, "upload limit: 10 Mbps") {
		t.Fatalf("client request should be clamped to server limit, output: %s", fullOutput)
	}
	incStream := append(fakeZFSBegin("tank/auto@s2", 0xaaa, 0xbbb, 0), []byte("incremental-payload")...)
	incOutput := runShellUpload(t, client, incStream)
	incSnapshotID := parseSnapshotID(t, incOutput)
	if !strings.Contains(incOutput, "ok snapshot=") {
		t.Fatalf("auto incremental upload output: %s", incOutput)
	}
	if fullSnapshotID == incSnapshotID {
		t.Fatalf("snapshot ids should differ")
	}
	thirdStream := append(fakeZFSBegin("tank/auto@s3", 0xbbb, 0xccc, 0), []byte("third-payload")...)
	thirdOutput := runShellUpload(t, client, thirdStream)
	thirdSnapshotID := parseSnapshotID(t, thirdOutput)

	var status string
	if err := cat.DB().QueryRowContext(ctx, `SELECT status FROM snapshots WHERE name = 's1'`).Scan(&status); err != nil {
		t.Fatalf("query snapshot: %v", err)
	}
	if status != "committed" {
		t.Fatalf("status got %q", status)
	}

	session, err = client.NewSession()
	if err != nil {
		t.Fatalf("new session: %v", err)
	}
	output, err = session.CombinedOutput("state tank auto")
	_ = session.Close()
	if err != nil {
		t.Fatalf("ssh state failed: %v output=%s", err, output)
	}
	for _, want := range []string{`"source":"nas-home"`, `"pool":"tank"`, `"dataset":"auto"`, `"name":"s1"`, `"name":"s2"`, `"base_snapshot":"s1"`, `"chain_depth":2`} {
		if !strings.Contains(string(output), want) {
			t.Fatalf("state output missing %s: %s", want, output)
		}
	}

	session, err = client.NewSession()
	if err != nil {
		t.Fatalf("new session: %v", err)
	}
	restored, err := session.Output("restore-stream " + incSnapshotID)
	_ = session.Close()
	if err != nil {
		t.Fatalf("restore-stream failed: %v", err)
	}
	if !bytes.Equal(restored, incStream) {
		t.Fatalf("restore-stream output mismatch got %q want %q", restored, incStream)
	}

	session, err = client.NewSession()
	if err != nil {
		t.Fatalf("new session: %v", err)
	}
	restoreWithHint, err := session.CombinedOutput("restore-stream " + incSnapshotID)
	_ = session.Close()
	if err != nil {
		t.Fatalf("restore-stream with hint failed: %v", err)
	}
	if !bytes.Contains(restoreWithHint, []byte("restore requires parent snapshot")) || !bytes.Contains(restoreWithHint, []byte(fullSnapshotID)) {
		t.Fatalf("restore-stream combined output missing parent requirement: %q", restoreWithHint)
	}
	if !bytes.Contains(restoreWithHint, []byte("next restore:")) || !bytes.Contains(restoreWithHint, []byte(thirdSnapshotID)) {
		t.Fatalf("restore-stream combined output missing next restore hint: %q", restoreWithHint)
	}
	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("server error: %v", err)
		}
	default:
	}
}

func TestSSHRejectsUnknownKey(t *testing.T) {
	cat, err := catalog.Open(filepath.Join(t.TempDir(), "catalog.db"))
	if err != nil {
		t.Fatalf("open catalog: %v", err)
	}
	defer cat.Close()
	if err := cat.Migrate(); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	_, clientPrivate, _ := ed25519.GenerateKey(rand.Reader)
	clientSigner, _ := ssh.NewSignerFromKey(clientPrivate)
	_, hostPrivate, _ := ed25519.GenerateKey(rand.Reader)
	hostSigner, _ := ssh.NewSignerFromKey(hostPrivate)
	srv := &Server{Addr: "127.0.0.1:0", Signer: hostSigner, DB: cat.DB(), Ingest: ingest.Service{Repo: catalog.NewRepository(cat.DB()), Store: storage.NewMemoryStore(), Keys: storage.KeyBuilder{RootPrefix: "root"}, ChunkSize: 5}}
	go func() { _ = srv.ListenAndServe() }()
	t.Cleanup(func() { _ = srv.Close() })
	addr := waitForAddr(t, srv)
	_, err = ssh.Dial("tcp", addr, &ssh.ClientConfig{User: "backup", Auth: []ssh.AuthMethod{ssh.PublicKeys(clientSigner)}, HostKeyCallback: ssh.InsecureIgnoreHostKey(), Timeout: 5 * time.Second})
	if err == nil {
		t.Fatal("expected auth failure")
	}
}

func waitForAddr(t *testing.T, srv *Server) string {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		addr := srv.AddrString()
		if addr != "" {
			return addr
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal(fmt.Errorf("server did not start"))
	return ""
}

func runShellUpload(t *testing.T, client *ssh.Client, stream []byte, requestedLimitMbps ...string) string {
	t.Helper()
	channel, requests, err := client.OpenChannel("session", nil)
	if err != nil {
		t.Fatalf("open session channel: %v", err)
	}
	defer channel.Close()
	if len(requestedLimitMbps) > 0 {
		payload := ssh.Marshal(struct {
			Name  string
			Value string
		}{Name: "ZFS_S3END_UPLOAD_THROUGHPUT_LIMIT_MBIT", Value: requestedLimitMbps[0]})
		ok, err := channel.SendRequest("env", true, payload)
		if err != nil || !ok {
			t.Fatalf("upload throughput env request ok=%v err=%v", ok, err)
		}
	}
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
	stderr := make(chan []byte, 1)
	go func() {
		output, _ := io.ReadAll(channel.Stderr())
		stderr <- output
	}()
	if _, err := channel.Write(stream); err != nil {
		t.Fatalf("write stream: %v", err)
	}
	if err := channel.CloseWrite(); err != nil {
		t.Fatalf("close write: %v", err)
	}
	output, err := io.ReadAll(channel)
	if err != nil {
		t.Fatalf("read output: %v", err)
	}
	select {
	case stderrOutput := <-stderr:
		output = append(output, stderrOutput...)
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for shell upload stderr")
	}
	select {
	case status := <-exitStatus:
		if status != 0 {
			t.Fatalf("shell upload exit status %d output=%s", status, output)
		}
	case <-time.After(5 * time.Second):
		t.Fatalf("timed out waiting for shell upload exit status output=%s", output)
	}
	return string(output)
}

func parseSnapshotID(t *testing.T, output string) string {
	t.Helper()
	for _, field := range strings.Fields(output) {
		if strings.HasPrefix(field, "snapshot=") {
			return strings.TrimPrefix(field, "snapshot=")
		}
	}
	t.Fatalf("snapshot id not found in output %q", output)
	return ""
}

func fakeZFSBegin(toName string, fromGUID, toGUID uint64, features uint64) []byte {
	header := make([]byte, zfsstream.BeginRecordSize)
	binary.LittleEndian.PutUint32(header[0:4], 0)
	binary.LittleEndian.PutUint64(header[8:16], 0x2f5bacbac)
	binary.LittleEndian.PutUint64(header[16:24], (features<<2)|1)
	binary.LittleEndian.PutUint64(header[40:48], toGUID)
	binary.LittleEndian.PutUint64(header[48:56], fromGUID)
	copy(header[56:], []byte(toName))
	return header
}
