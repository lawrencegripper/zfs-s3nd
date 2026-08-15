package sshserver

import (
	"bytes"
	"context"
	"errors"
	"io"
	"testing"
	"time"
)

func TestEffectiveUploadRateCannotRaiseConfiguredLimit(t *testing.T) {
	configured := int64(5_625_000)
	for _, test := range []struct {
		name      string
		options   sessionOptions
		wantBytes int64
	}{
		{name: "omitted", wantBytes: configured},
		{name: "zero uses default", options: sessionOptions{hasRequestedUploadRate: true}, wantBytes: configured},
		{name: "lower request", options: sessionOptions{hasRequestedUploadRate: true, requestedUploadRateBytesPerSecond: 1_250_000}, wantBytes: 1_250_000},
		{name: "higher request clamped", options: sessionOptions{hasRequestedUploadRate: true, requestedUploadRateBytesPerSecond: 10_000_000}, wantBytes: configured},
	} {
		t.Run(test.name, func(t *testing.T) {
			if got := effectiveUploadRate(configured, test.options); got != test.wantBytes {
				t.Fatalf("effectiveUploadRate got %d want %d", got, test.wantBytes)
			}
		})
	}
}

func TestRateLimitedReaderBoundsReadAheadToBurst(t *testing.T) {
	source := &recordingReader{remaining: 2 * maxUploadBurstBytes}
	reader := newRateLimitedReader(context.Background(), source, 100*1024*1024)
	n, err := reader.Read(make([]byte, 2*maxUploadBurstBytes))
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if n != int(maxUploadBurstBytes) {
		t.Fatalf("first read got %d bytes want %d", n, maxUploadBurstBytes)
	}
	if source.maxRequested != int(maxUploadBurstBytes) {
		t.Fatalf("underlying read requested %d bytes want %d", source.maxRequested, maxUploadBurstBytes)
	}
}

type recordingReader struct {
	remaining    int64
	maxRequested int
}

func (r *recordingReader) Read(p []byte) (int, error) {
	if len(p) > r.maxRequested {
		r.maxRequested = len(p)
	}
	if r.remaining == 0 {
		return 0, io.EOF
	}
	n := min(len(p), int(r.remaining))
	r.remaining -= int64(n)
	return n, nil
}

func TestRateLimitedReaderEnforcesRateAfterBoundedBurst(t *testing.T) {
	const bytesPerSecond = int64(64 * 1024)
	payload := bytes.Repeat([]byte("x"), 64*1024)
	started := time.Now()
	got, err := io.ReadAll(newRateLimitedReader(context.Background(), bytes.NewReader(payload), bytesPerSecond))
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	elapsed := time.Since(started)
	if !bytes.Equal(got, payload) {
		t.Fatal("read payload changed")
	}
	if elapsed < 400*time.Millisecond {
		t.Fatalf("reader completed too quickly: %s", elapsed)
	}
	if elapsed > 2*time.Second {
		t.Fatalf("reader completed too slowly: %s", elapsed)
	}
}

func TestRateLimitedReaderCancellationInterruptsWait(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	reader := newRateLimitedReader(ctx, bytes.NewReader(make([]byte, 64*1024)), 1024).(*rateLimitedReader)
	if _, err := io.ReadFull(reader, make([]byte, minUploadBurstBytes)); err != nil {
		t.Fatalf("consume initial burst: %v", err)
	}
	cancel()
	started := time.Now()
	if _, err := reader.Read(make([]byte, 1)); !errors.Is(err, context.Canceled) {
		t.Fatalf("read error got %v want context canceled", err)
	}
	if elapsed := time.Since(started); elapsed > 100*time.Millisecond {
		t.Fatalf("cancellation took %s", elapsed)
	}
}
