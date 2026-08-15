package sshserver

import (
	"context"
	"io"
	"math"
	"time"
)

const (
	minUploadBurstBytes = int64(32 * 1024)
	maxUploadBurstBytes = int64(1024 * 1024)
)

type rateLimitedReader struct {
	ctx            context.Context
	reader         io.Reader
	bytesPerSecond int64
	burstBytes     int64
	tokens         float64
	lastRefill     time.Time
}

func effectiveUploadRate(configured int64, options sessionOptions) int64 {
	requested := options.requestedUploadRateBytesPerSecond
	if configured <= 0 {
		return requested
	}
	if !options.hasRequestedUploadRate || requested <= 0 || requested >= configured {
		return configured
	}
	return requested
}

func newRateLimitedReader(ctx context.Context, reader io.Reader, bytesPerSecond int64) io.Reader {
	if bytesPerSecond <= 0 {
		return reader
	}
	if ctx == nil {
		ctx = context.Background()
	}
	burst := bytesPerSecond / 4
	if burst < minUploadBurstBytes {
		burst = minUploadBurstBytes
	}
	if burst > maxUploadBurstBytes {
		burst = maxUploadBurstBytes
	}
	now := time.Now()
	return &rateLimitedReader{
		ctx:            ctx,
		reader:         reader,
		bytesPerSecond: bytesPerSecond,
		burstBytes:     burst,
		tokens:         float64(burst),
		lastRefill:     now,
	}
}

func (r *rateLimitedReader) Read(p []byte) (int, error) {
	if len(p) == 0 {
		return r.reader.Read(p)
	}
	for {
		now := time.Now()
		r.refill(now)
		if r.tokens >= 1 {
			allowed := min(len(p), int(r.tokens))
			r.tokens -= float64(allowed)
			n, err := r.reader.Read(p[:allowed])
			if n < allowed {
				r.tokens = math.Min(float64(r.burstBytes), r.tokens+float64(allowed-n))
			}
			return n, err
		}

		seconds := (1 - r.tokens) / float64(r.bytesPerSecond)
		wait := time.Duration(math.Ceil(seconds * float64(time.Second)))
		if wait < time.Millisecond {
			wait = time.Millisecond
		}
		timer := time.NewTimer(wait)
		select {
		case <-r.ctx.Done():
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			return 0, r.ctx.Err()
		case <-timer.C:
		}
	}
}

func (r *rateLimitedReader) refill(now time.Time) {
	if now.Before(r.lastRefill) {
		r.lastRefill = now
		return
	}
	elapsed := now.Sub(r.lastRefill).Seconds()
	r.tokens = math.Min(float64(r.burstBytes), r.tokens+elapsed*float64(r.bytesPerSecond))
	r.lastRefill = now
}
