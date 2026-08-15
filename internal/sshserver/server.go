package sshserver

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/crypto/ssh"

	"github.com/lawrencegripper/zfs-s3nd/internal/appsettings"
	"github.com/lawrencegripper/zfs-s3nd/internal/catalog"
	"github.com/lawrencegripper/zfs-s3nd/internal/catalog/db"
	"github.com/lawrencegripper/zfs-s3nd/internal/ingest"
	"github.com/lawrencegripper/zfs-s3nd/internal/restore"
	"github.com/lawrencegripper/zfs-s3nd/internal/restoreplan"
	"github.com/lawrencegripper/zfs-s3nd/internal/zfsstream"
)

type Server struct {
	Addr                         string
	Signer                       ssh.Signer
	DB                           db.DBTX
	Ingest                       ingest.Service
	PostUploadValidation         func(snapshotID string)
	UploadRateBytesPerSecond     int64
	UploadRateBytesPerSecondFunc func() int64
	Logger                       *slog.Logger

	listener             net.Listener
	mu                   sync.Mutex
	draining             atomic.Bool
	ShutdownPollInterval time.Duration
	authMu               sync.Mutex
	authAttempts         map[string]authAttempt
}

type authAttempt struct {
	Failures     int
	Last         time.Time
	BlockedUntil time.Time
}

const uploadThroughputLimitEnv = "ZFS_S3END_UPLOAD_THROUGHPUT_LIMIT_MBIT"

type sessionOptions struct {
	requestedUploadRateBytesPerSecond int64
	hasRequestedUploadRate            bool
	err                               error
}

func (s *Server) ListenAndServe() error {
	if s.Signer == nil {
		return fmt.Errorf("ssh host signer is required")
	}
	if s.DB == nil {
		return fmt.Errorf("db is required")
	}
	if s.Logger == nil {
		s.Logger = slog.Default()
	}
	config := &ssh.ServerConfig{PublicKeyCallback: s.publicKeyCallback}
	config.AddHostKey(s.Signer)

	ln, err := net.Listen("tcp", s.Addr)
	if err != nil {
		return err
	}
	s.mu.Lock()
	s.listener = ln
	s.mu.Unlock()
	s.Logger.Info("starting ssh server", "addr", ln.Addr().String())
	for {
		conn, err := ln.Accept()
		if err != nil {
			if errors.Is(err, net.ErrClosed) {
				return nil
			}
			return err
		}
		go s.handleConn(conn, config)
	}
}

func (s *Server) AddrString() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.listener == nil {
		return ""
	}
	return s.listener.Addr().String()
}

func (s *Server) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.listener != nil {
		return s.listener.Close()
	}
	return nil
}

func (s *Server) Shutdown(ctx context.Context) error {
	s.draining.Store(true)
	if err := s.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
		return err
	}
	interval := s.ShutdownPollInterval
	if interval <= 0 {
		interval = 5 * time.Second
	}
	for {
		uploads, err := db.New(s.DB).ListActiveUploads(ctx)
		if err != nil {
			return fmt.Errorf("list active uploads during ssh shutdown: %w", err)
		}
		if len(uploads) == 0 {
			s.Logger.Info("ssh shutdown completed; no active uploads")
			return nil
		}
		s.Logger.Info("ssh shutdown waiting for active uploads", "active_uploads", len(uploads))
		for _, upload := range uploads {
			s.Logger.Info("active upload during ssh shutdown", "source", upload.SourceName, "pool", upload.PoolName, "dataset", upload.DatasetPath, "snapshot", upload.TargetSnapshotName, "status", upload.Status, "bytes_received", upload.BytesReceived, "chunks_completed", upload.ChunksCompleted, "started_at", upload.StartedAt, "last_heartbeat_at", upload.LastHeartbeatAt)
		}
		timer := time.NewTimer(interval)
		select {
		case <-ctx.Done():
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			return ctx.Err()
		case <-timer.C:
		}
	}
}

func (s *Server) publicKeyCallback(conn ssh.ConnMetadata, key ssh.PublicKey) (*ssh.Permissions, error) {
	remote := sshAuthKey(conn.RemoteAddr().String())
	now := time.Now()
	if !s.allowAuthAttempt(remote, now) {
		return nil, fmt.Errorf("too many authentication attempts")
	}
	fingerprint := ssh.FingerprintSHA256(key)
	queries := db.New(s.DB)
	row, err := queries.GetSSHKeyByFingerprint(context.Background(), fingerprint)
	if err != nil {
		s.recordAuthFailure(remote, now)
		s.Logger.Warn("ssh public key authentication failed", "remote_addr", conn.RemoteAddr().String(), "fingerprint", fingerprint, "error", err)
		return nil, fmt.Errorf("unauthorized key %s: %w", fingerprint, err)
	}
	loginUser := conn.User()
	if err := validateName("ssh username", loginUser); err != nil {
		s.recordAuthFailure(remote, now)
		return nil, err
	}
	s.recordAuthSuccess(remote)
	_ = queries.TouchSSHKey(context.Background(), row.ID)
	return &ssh.Permissions{Extensions: map[string]string{"key_id": row.ID, "fingerprint": fingerprint, "source_name": loginUser}}, nil
}

func sshAuthKey(remote string) string {
	if host, _, err := net.SplitHostPort(remote); err == nil {
		return host
	}
	return remote
}

func (s *Server) allowAuthAttempt(key string, now time.Time) bool {
	s.authMu.Lock()
	defer s.authMu.Unlock()
	if s.authAttempts == nil {
		s.authAttempts = make(map[string]authAttempt)
	}
	attempt := s.authAttempts[key]
	return attempt.BlockedUntil.IsZero() || now.After(attempt.BlockedUntil)
}

func (s *Server) recordAuthFailure(key string, now time.Time) {
	s.authMu.Lock()
	defer s.authMu.Unlock()
	if s.authAttempts == nil {
		s.authAttempts = make(map[string]authAttempt)
	}
	attempt := s.authAttempts[key]
	if now.Sub(attempt.Last) > 15*time.Minute {
		attempt.Failures = 0
	}
	attempt.Failures++
	attempt.Last = now
	if attempt.Failures >= 5 {
		attempt.BlockedUntil = now.Add(time.Duration(attempt.Failures-4) * time.Minute)
	}
	s.authAttempts[key] = attempt
}

func (s *Server) recordAuthSuccess(key string) {
	s.authMu.Lock()
	defer s.authMu.Unlock()
	delete(s.authAttempts, key)
}

func (s *Server) handleConn(raw net.Conn, config *ssh.ServerConfig) {
	conn, chans, reqs, err := ssh.NewServerConn(raw, config)
	if err != nil {
		s.Logger.Warn("ssh handshake failed", "error", err)
		_ = raw.Close()
		return
	}
	defer conn.Close()
	connCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		_ = conn.Wait()
		cancel()
	}()
	go ssh.DiscardRequests(reqs)
	for newChannel := range chans {
		if newChannel.ChannelType() != "session" {
			_ = newChannel.Reject(ssh.UnknownChannelType, "only session channels are supported")
			continue
		}
		channel, requests, err := newChannel.Accept()
		if err != nil {
			s.Logger.Warn("ssh channel accept failed", "error", err)
			continue
		}
		go s.handleSession(connCtx, conn, channel, requests)
	}
}

func (s *Server) handleSession(ctx context.Context, conn *ssh.ServerConn, channel ssh.Channel, requests <-chan *ssh.Request) {
	defer channel.Close()
	var options sessionOptions
	for req := range requests {
		switch req.Type {
		case "env":
			var payload struct {
				Name  string
				Value string
			}
			if err := ssh.Unmarshal(req.Payload, &payload); err != nil || payload.Name != uploadThroughputLimitEnv {
				_ = req.Reply(false, nil)
				continue
			}
			if options.hasRequestedUploadRate {
				options.err = fmt.Errorf("duplicate %s request", uploadThroughputLimitEnv)
				_ = req.Reply(false, nil)
				continue
			}
			options.hasRequestedUploadRate = true
			rate, err := appsettings.ParseThroughputLimitMbps(payload.Value)
			if err != nil {
				options.err = fmt.Errorf("invalid %s: %w", uploadThroughputLimitEnv, err)
				_ = req.Reply(false, nil)
				continue
			}
			options.requestedUploadRateBytesPerSecond = rate
			_ = req.Reply(true, nil)

		case "exec":
			if options.hasRequestedUploadRate {
				_ = req.Reply(false, nil)
				_, _ = io.WriteString(channel.Stderr(), uploadThroughputLimitEnv+" is only valid for uploads\n")
				s.sendExitStatus(channel, 1)
				return
			}
			if s.draining.Load() {
				_ = req.Reply(false, nil)
				_, _ = io.WriteString(channel.Stderr(), "server shutting down\n")
				s.sendExitStatus(channel, 1)
				return
			}
			command, err := parseExecPayload(req.Payload)
			if err != nil {
				_ = req.Reply(false, nil)
				_, _ = io.WriteString(channel.Stderr(), err.Error()+"\n")
				s.sendExitStatus(channel, 1)
				return
			}
			if !isSupportedExecCommand(command) {
				_ = req.Reply(false, nil)
				_, _ = io.WriteString(channel.Stderr(), "unsupported command\n")
				s.sendExitStatus(channel, 1)
				return
			}
			_ = req.Reply(true, nil)
			s.runCommand(ctx, conn, channel, command)
			return
		case "shell":
			if options.err != nil {
				_ = req.Reply(false, nil)
				_, _ = io.WriteString(channel.Stderr(), options.err.Error()+"\n")
				s.sendExitStatus(channel, 1)
				return
			}
			if s.draining.Load() {
				_ = req.Reply(false, nil)
				_, _ = io.WriteString(channel.Stderr(), "server shutting down\n")
				s.sendExitStatus(channel, 1)
				return
			}
			_ = req.Reply(true, nil)
			s.runAutoReceive(ctx, conn, channel, options)
			return
		case "pty-req":
			_ = req.Reply(false, nil)
		default:
			_ = req.Reply(false, nil)
		}
	}
}

func isSupportedExecCommand(command string) bool {
	fields := strings.Fields(command)
	if len(fields) == 0 {
		return false
	}
	switch fields[0] {
	case "state", "restore-stream":
		return true
	default:
		return false
	}
}

func (s *Server) runCommand(ctx context.Context, conn *ssh.ServerConn, channel ssh.Channel, command string) {
	done := make(chan struct{})
	go func() {
		select {
		case <-ctx.Done():
			_ = channel.Close()
		case <-done:
		}
	}()
	defer close(done)

	fields := strings.Fields(command)
	if len(fields) == 0 {
		_, _ = io.WriteString(channel.Stderr(), "unsupported command\n")
		s.sendExitStatus(channel, 1)
		return
	}
	switch fields[0] {
	case "state":
		s.runState(ctx, channel, command, conn.Permissions.Extensions["source_name"])
	case "restore-stream":
		s.runRestoreStream(ctx, channel, fields)
	default:
		_, _ = io.WriteString(channel.Stderr(), "unsupported command\n")
		s.sendExitStatus(channel, 1)
	}
	_ = channel.CloseWrite()
}

const receiveStallTimeout = 5 * time.Minute

var errReceiveStalled = errors.New("upload stalled: no new bytes received for 5m0s")

func (s *Server) runAutoReceive(ctx context.Context, conn *ssh.ServerConn, channel ssh.Channel, options sessionOptions) {
	defer channel.CloseWrite()
	sourceName := conn.Permissions.Extensions["source_name"]
	stallReader := newStallTimeoutReader(channel, receiveStallTimeout, func() { _ = channel.Close() })
	defer stallReader.Stop()

	header := make([]byte, zfsstream.BeginRecordSize)
	if _, err := io.ReadFull(stallReader, header); err != nil {
		s.Logger.Warn("ssh upload failed before zfs stream header", "source", sourceName, "error", err)
		_, _ = io.WriteString(channel.Stderr(), fmt.Sprintf("read zfs stream header: %v\n", err))
		s.sendExitStatus(channel, 1)
		return
	}
	begin, err := zfsstream.ParseBegin(header)
	if err != nil {
		s.Logger.Warn("ssh upload failed parsing zfs stream header", "source", sourceName, "error", err)
		_, _ = io.WriteString(channel.Stderr(), err.Error()+"\n")
		s.sendExitStatus(channel, 1)
		return
	}
	for _, check := range []struct {
		kind  string
		value string
	}{
		{kind: "source", value: sourceName},
		{kind: "pool", value: begin.Pool},
		{kind: "snapshot", value: begin.Snapshot},
	} {
		if err := validateName(check.kind, check.value); err != nil {
			_, _ = io.WriteString(channel.Stderr(), err.Error()+"\n")
			s.sendExitStatus(channel, 1)
			return
		}
	}
	if err := validateDataset(begin.Dataset); err != nil {
		_, _ = io.WriteString(channel.Stderr(), err.Error()+"\n")
		s.sendExitStatus(channel, 1)
		return
	}

	baseSnapshot := ""
	if begin.FromGUID != "" && begin.FromGUID != "0" {
		base, err := db.New(s.DB).GetCommittedSnapshotByToGUID(ctx, db.GetCommittedSnapshotByToGUIDParams{Name: sourceName, Name_2: begin.Pool, Path: begin.Dataset, StreamToGuid: begin.FromGUID})
		if err != nil {
			s.Logger.Warn("ssh upload incremental base not found", "source", sourceName, "pool", begin.Pool, "dataset", begin.Dataset, "snapshot", begin.Snapshot, "from_guid", begin.FromGUID, "to_guid", begin.ToGUID, "error", err)
			_, _ = io.WriteString(channel.Stderr(), fmt.Sprintf("base snapshot with toguid %s not found for %s/%s/%s: %v\n", begin.FromGUID, sourceName, begin.Pool, begin.Dataset, err))
			s.sendExitStatus(channel, 1)
			return
		}
		baseSnapshot = base.Name
	}

	configuredRate := s.configuredUploadRateBytesPerSecond()
	effectiveRate := effectiveUploadRate(configuredRate, options)
	s.Logger.Info("ssh upload started", "source", sourceName, "pool", begin.Pool, "dataset", begin.Dataset, "snapshot", begin.Snapshot, "configured_rate_bytes_per_second", configuredRate, "requested_rate_bytes_per_second", options.requestedUploadRateBytesPerSecond, "effective_rate_bytes_per_second", effectiveRate)
	reader := io.MultiReader(bytes.NewReader(header), stallReader)
	if effectiveRate > 0 {
		reader = newRateLimitedReader(ctx, reader, effectiveRate)
		_, _ = fmt.Fprintf(channel.Stderr(), "upload limit: %s\n", humanBitrate(effectiveRate, time.Second))
	}
	progress := newProgressReporter(channel.Stderr(), time.Minute)
	defer progress.Stop()
	reader = progress.Wrap(reader)

	result, err := s.Ingest.Receive(ctx, ingest.Request{Source: sourceName, Pool: begin.Pool, Dataset: begin.Dataset, Snapshot: begin.Snapshot, BaseSnapshot: baseSnapshot, Raw: begin.Raw, Compressed: begin.Compressed, FromGUID: begin.FromGUID, ToGUID: begin.ToGUID}, reader)
	if err != nil {
		s.Logger.Warn("ssh upload failed", "source", sourceName, "pool", begin.Pool, "dataset", begin.Dataset, "snapshot", begin.Snapshot, "base_snapshot", baseSnapshot, "from_guid", begin.FromGUID, "to_guid", begin.ToGUID, "bytes_received", progress.Bytes(), "error", err)
		_, _ = io.WriteString(channel.Stderr(), err.Error()+"\n")
		s.sendExitStatus(channel, 1)
		return
	}
	s.Logger.Info("ssh upload succeeded", "source", sourceName, "pool", begin.Pool, "dataset", begin.Dataset, "snapshot", begin.Snapshot, "snapshot_id", result.SnapshotID, "upload_session_id", result.UploadSessionID, "operation_id", result.OperationID, "bytes_received", result.BytesReceived, "chunks", len(result.Chunks))
	if s.PostUploadValidation != nil {
		s.PostUploadValidation(result.SnapshotID)
	}
	_, _ = fmt.Fprintf(channel, "ok snapshot=%s bytes=%d chunks=%d manifest=%s\n", result.SnapshotID, result.BytesReceived, len(result.Chunks), result.ManifestKey)
	s.sendExitStatus(channel, 0)
}

func (s *Server) runRestoreStream(ctx context.Context, channel ssh.Channel, fields []string) {
	defer channel.CloseWrite()
	if len(fields) != 2 {
		_, _ = io.WriteString(channel.Stderr(), "usage: restore-stream <snapshot-id>\n")
		s.sendExitStatus(channel, 1)
		return
	}
	if s.Ingest.Store == nil {
		_, _ = io.WriteString(channel.Stderr(), "storage is not configured\n")
		s.sendExitStatus(channel, 1)
		return
	}
	var snapshotExists int
	if err := s.DB.QueryRowContext(ctx, `SELECT 1 FROM snapshots WHERE id = ?`, fields[1]).Scan(&snapshotExists); err == sql.ErrNoRows {
		_, _ = io.WriteString(channel.Stderr(), "snapshot not found\n")
		s.sendExitStatus(channel, 1)
		return
	} else if err != nil {
		_, _ = io.WriteString(channel.Stderr(), err.Error()+"\n")
		s.sendExitStatus(channel, 1)
		return
	}
	plan, err := restoreplan.Build(ctx, catalog.NewRepository(s.DB), fields[1])
	if err != nil {
		_, _ = io.WriteString(channel.Stderr(), err.Error()+"\n")
		s.sendExitStatus(channel, 1)
		return
	}
	if err := plan.ValidateStreamable(); err != nil {
		_, _ = io.WriteString(channel.Stderr(), err.Error()+"\n")
		s.sendExitStatus(channel, 1)
		return
	}
	_, _ = io.WriteString(channel.Stderr(), plan.ParentRequirement())
	if err := restore.StreamSnapshot(ctx, s.Ingest.Store, plan.ManifestKey(), channel); err != nil {
		_, _ = io.WriteString(channel.Stderr(), err.Error()+"\n")
		s.sendExitStatus(channel, 1)
		return
	}
	_, _ = io.WriteString(channel.Stderr(), plan.SSHNextRestore())
	s.sendExitStatus(channel, 0)
}

type stateResponse struct {
	Source                   string          `json:"source"`
	Pool                     string          `json:"pool"`
	Dataset                  string          `json:"dataset"`
	MaxIncrementalChainDepth int64           `json:"max_incremental_chain_depth"`
	Snapshots                []stateSnapshot `json:"snapshots"`
}

type stateSnapshot struct {
	Name                   string `json:"name"`
	BaseSnapshot           string `json:"base_snapshot,omitempty"`
	ManifestObjectKey      string `json:"manifest_object_key"`
	Status                 string `json:"status"`
	StreamValidationStatus string `json:"stream_validation_status"`
	ChainValidationStatus  string `json:"chain_validation_status"`
	StreamSHA256           string `json:"stream_sha256,omitempty"`
	FromGUID               string `json:"from_guid,omitempty"`
	ToGUID                 string `json:"to_guid,omitempty"`
	ChainDepth             int64  `json:"chain_depth"`
	ChunkCount             int64  `json:"chunk_count"`
	UploadedAt             string `json:"uploaded_at,omitempty"`
}

func (s *Server) runState(ctx context.Context, channel ssh.Channel, command, defaultSource string) {
	cmd, err := ParseStateCommandForSource(command, defaultSource)
	if err != nil {
		_, _ = io.WriteString(channel.Stderr(), err.Error()+"\n")
		s.sendExitStatus(channel, 1)
		return
	}
	rows, err := db.New(s.DB).ListCommittedSnapshotsForIdentity(ctx, db.ListCommittedSnapshotsForIdentityParams{Name: cmd.Source, Name_2: cmd.Pool, Path: cmd.Dataset})
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		_, _ = io.WriteString(channel.Stderr(), err.Error()+"\n")
		s.sendExitStatus(channel, 1)
		return
	}
	repo := catalog.NewRepository(s.DB)
	response := stateResponse{Source: cmd.Source, Pool: cmd.Pool, Dataset: cmd.Dataset, MaxIncrementalChainDepth: s.Ingest.EffectiveMaxIncrementalChainDepth(), Snapshots: make([]stateSnapshot, 0, len(rows))}
	for _, row := range rows {
		chainDepth := int64(1)
		if depth, err := repo.ChainDepth(ctx, row.ID); err == nil && depth > 0 {
			chainDepth = depth
		}
		response.Snapshots = append(response.Snapshots, stateSnapshot{
			Name:                   row.Name,
			BaseSnapshot:           nullString(row.ParentSnapshotName),
			ManifestObjectKey:      nullString(row.ManifestObjectKey),
			Status:                 row.Status,
			StreamValidationStatus: row.StreamValidationStatus,
			ChainValidationStatus:  row.ChainValidationStatus,
			StreamSHA256:           nullString(row.StreamSha256),
			FromGUID:               row.StreamFromGuid,
			ToGUID:                 row.StreamToGuid,
			ChainDepth:             chainDepth,
			ChunkCount:             row.ChunkCount,
			UploadedAt:             nullString(row.UploadCompletedAt),
		})
	}
	if err := json.NewEncoder(channel).Encode(response); err != nil {
		_, _ = io.WriteString(channel.Stderr(), err.Error()+"\n")
		s.sendExitStatus(channel, 1)
		return
	}
	s.sendExitStatus(channel, 0)
}

func (s *Server) configuredUploadRateBytesPerSecond() int64 {
	if s.UploadRateBytesPerSecondFunc != nil {
		return s.UploadRateBytesPerSecondFunc()
	}
	return s.UploadRateBytesPerSecond
}

func nullString(value sql.NullString) string {
	if !value.Valid {
		return ""
	}
	return value.String
}

type stallTimeoutReader struct {
	reader    io.Reader
	timeout   time.Duration
	onTimeout func()
	timer     *time.Timer
	mu        sync.Mutex
	timedOut  atomic.Bool
}

func newStallTimeoutReader(reader io.Reader, timeout time.Duration, onTimeout func()) *stallTimeoutReader {
	r := &stallTimeoutReader{reader: reader, timeout: timeout, onTimeout: onTimeout}
	r.timer = time.AfterFunc(timeout, func() {
		r.timedOut.Store(true)
		if onTimeout != nil {
			onTimeout()
		}
	})
	return r
}

func (r *stallTimeoutReader) Read(p []byte) (int, error) {
	n, err := r.reader.Read(p)
	if n > 0 {
		r.reset()
	}
	if r.timedOut.Load() {
		return n, errReceiveStalled
	}
	return n, err
}

func (r *stallTimeoutReader) Stop() {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.timer != nil {
		r.timer.Stop()
	}
}

func (r *stallTimeoutReader) reset() {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.timer != nil {
		r.timer.Reset(r.timeout)
	}
}

type progressReporter struct {
	writer   io.Writer
	interval time.Duration
	started  time.Time
	bytes    atomic.Int64
	stopCh   chan struct{}
	doneCh   chan struct{}
}

func newProgressReporter(writer io.Writer, interval time.Duration) *progressReporter {
	p := &progressReporter{writer: writer, interval: interval, started: time.Now(), stopCh: make(chan struct{}), doneCh: make(chan struct{})}
	go p.run()
	return p
}

func (p *progressReporter) Wrap(reader io.Reader) io.Reader {
	return &progressReader{reader: reader, reporter: p}
}

func (p *progressReporter) Stop() {
	close(p.stopCh)
	<-p.doneCh
}

func (p *progressReporter) add(n int64) {
	p.bytes.Add(n)
}

func (p *progressReporter) Bytes() int64 {
	return p.bytes.Load()
}

func (p *progressReporter) run() {
	defer close(p.doneCh)
	ticker := time.NewTicker(p.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			p.write()
		case <-p.stopCh:
			return
		}
	}
}

func (p *progressReporter) write() {
	bytes := p.bytes.Load()
	elapsed := time.Since(p.started)
	_, _ = fmt.Fprintf(p.writer, "progress: received=%s rate=%s elapsed=%s\n", humanBytes(bytes), humanBitrate(bytes, elapsed), elapsed.Round(time.Second))
}

type progressReader struct {
	reader   io.Reader
	reporter *progressReporter
}

func (r *progressReader) Read(p []byte) (int, error) {
	n, err := r.reader.Read(p)
	if n > 0 {
		r.reporter.add(int64(n))
	}
	return n, err
}

func humanBytes(bytes int64) string {
	if bytes < 1000 {
		return fmt.Sprintf("%d B", bytes)
	}
	units := []string{"KB", "MB", "GB", "TB"}
	value := float64(bytes)
	unit := -1
	for value >= 1000 && unit < len(units)-1 {
		value /= 1000
		unit++
	}
	formatted := fmt.Sprintf("%.1f", value)
	formatted = strings.TrimSuffix(formatted, ".0")
	return formatted + " " + units[unit]
}

func humanBitrate(bytes int64, elapsed time.Duration) string {
	if bytes <= 0 || elapsed <= 0 {
		return "0 Mbps"
	}
	bps := float64(bytes) * 8 / elapsed.Seconds()
	units := []string{"bps", "Kbps", "Mbps", "Gbps", "Tbps"}
	unit := 0
	for bps >= 1000 && unit < len(units)-1 {
		bps /= 1000
		unit++
	}
	if unit == 0 {
		return fmt.Sprintf("%.0f %s", bps, units[unit])
	}
	formatted := fmt.Sprintf("%.1f", bps)
	formatted = strings.TrimSuffix(formatted, ".0")
	return formatted + " " + units[unit]
}

func (s *Server) sendExitStatus(channel ssh.Channel, status uint32) {
	var payload [4]byte
	binary.BigEndian.PutUint32(payload[:], status)
	_, _ = channel.SendRequest("exit-status", false, payload[:])
}

func parseExecPayload(payload []byte) (string, error) {
	if len(payload) < 4 {
		return "", fmt.Errorf("invalid exec payload")
	}
	length := binary.BigEndian.Uint32(payload[:4])
	if int(length) != len(payload)-4 {
		return "", fmt.Errorf("invalid exec command length")
	}
	return string(payload[4:]), nil
}
