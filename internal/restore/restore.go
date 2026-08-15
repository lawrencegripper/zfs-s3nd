package restore

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"

	"github.com/lawrencegripper/zfs-s3nd/internal/manifest"
	"github.com/lawrencegripper/zfs-s3nd/internal/storage"
)

func StreamSnapshot(ctx context.Context, store storage.Store, manifestKey string, out io.Writer) error {
	manifestBytes, err := store.GetBytes(ctx, manifestKey)
	if err != nil {
		return fmt.Errorf("read manifest: %w", err)
	}
	mani, err := manifest.Unmarshal(manifestBytes)
	if err != nil {
		return fmt.Errorf("parse manifest: %w", err)
	}
	streamHash := sha256.New()
	var total int64
	for _, chunk := range mani.Chunks {
		reader, err := store.GetReader(ctx, chunk.ObjectKey)
		if err != nil {
			return fmt.Errorf("open chunk %d: %w", chunk.Index, err)
		}
		var buf bytes.Buffer
		hash := sha256.New()
		written, copyErr := io.Copy(io.MultiWriter(&buf, hash), reader)
		closeErr := reader.Close()
		if copyErr != nil {
			return fmt.Errorf("read chunk %d: %w", chunk.Index, copyErr)
		}
		if closeErr != nil {
			return fmt.Errorf("close chunk %d: %w", chunk.Index, closeErr)
		}
		if written != chunk.SizeBytes {
			return fmt.Errorf("chunk %d size mismatch: read %d want %d", chunk.Index, written, chunk.SizeBytes)
		}
		got := hex.EncodeToString(hash.Sum(nil))
		if got != chunk.SHA256 {
			return fmt.Errorf("chunk %d sha256 mismatch: got %s want %s", chunk.Index, got, chunk.SHA256)
		}
		if _, err := streamHash.Write(buf.Bytes()); err != nil {
			return fmt.Errorf("hash chunk %d: %w", chunk.Index, err)
		}
		if outWritten, err := out.Write(buf.Bytes()); err != nil {
			return fmt.Errorf("write chunk %d: %w", chunk.Index, err)
		} else if int64(outWritten) != chunk.SizeBytes {
			return fmt.Errorf("write chunk %d: wrote %d want %d", chunk.Index, outWritten, chunk.SizeBytes)
		}
		total += chunk.SizeBytes
	}
	if total != mani.Stream.SizeBytes {
		return fmt.Errorf("stream size mismatch: read %d want %d", total, mani.Stream.SizeBytes)
	}
	gotStreamSHA := hex.EncodeToString(streamHash.Sum(nil))
	if gotStreamSHA != mani.Stream.SHA256 {
		return fmt.Errorf("stream sha256 mismatch: got %s want %s", gotStreamSHA, mani.Stream.SHA256)
	}
	return nil
}
