package restore

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"testing"

	"github.com/lawrencegripper/zfs-s3nd/internal/manifest"
	"github.com/lawrencegripper/zfs-s3nd/internal/storage"
)

func TestStreamSnapshotConcatenatesManifestChunks(t *testing.T) {
	ctx := context.Background()
	store := storage.NewMemoryStore()
	_, _ = store.PutBytes(ctx, "chunks/0", []byte("hello-"))
	_, _ = store.PutBytes(ctx, "chunks/1", []byte("world"))
	stream := []byte("hello-world")
	sha := sha256.Sum256(stream)
	chunk0SHA := sha256.Sum256([]byte("hello-"))
	chunk1SHA := sha256.Sum256([]byte("world"))
	mani := manifest.New(
		manifest.Identity{Source: "nas", Pool: "tank", Dataset: "photos", Snapshot: "snap1"},
		manifest.Lineage{},
		manifest.Stream{SizeBytes: int64(len(stream)), SHA256: hex.EncodeToString(sha[:]), ChunkSize: 6},
		[]manifest.Chunk{
			{Index: 0, ObjectKey: "chunks/0", SizeBytes: 6, OffsetStart: 0, OffsetEnd: 6, SHA256: hex.EncodeToString(chunk0SHA[:])},
			{Index: 1, ObjectKey: "chunks/1", SizeBytes: 5, OffsetStart: 6, OffsetEnd: 11, SHA256: hex.EncodeToString(chunk1SHA[:])},
		},
	)
	manifestBytes, err := mani.MarshalCanonical()
	if err != nil {
		t.Fatalf("manifest: %v", err)
	}
	_, _ = store.PutBytes(ctx, "manifest.json", manifestBytes)
	var out bytes.Buffer
	if err := StreamSnapshot(ctx, store, "manifest.json", &out); err != nil {
		t.Fatalf("stream: %v", err)
	}
	if out.String() != string(stream) {
		t.Fatalf("got %q want %q", out.String(), stream)
	}
}

func TestStreamSnapshotFailsWhenChunkHashMismatches(t *testing.T) {
	ctx := context.Background()
	store := storage.NewMemoryStore()
	_, _ = store.PutBytes(ctx, "chunks/0", []byte("corrupt!"))
	stream := []byte("expected")
	streamSHA := sha256.Sum256(stream)
	chunkSHA := sha256.Sum256(stream)
	mani := manifest.New(
		manifest.Identity{Source: "nas", Pool: "tank", Dataset: "photos", Snapshot: "snap1"},
		manifest.Lineage{},
		manifest.Stream{SizeBytes: int64(len(stream)), SHA256: hex.EncodeToString(streamSHA[:]), ChunkSize: int64(len(stream))},
		[]manifest.Chunk{{Index: 0, ObjectKey: "chunks/0", SizeBytes: int64(len(stream)), OffsetStart: 0, OffsetEnd: int64(len(stream)), SHA256: hex.EncodeToString(chunkSHA[:])}},
	)
	manifestBytes, err := mani.MarshalCanonical()
	if err != nil {
		t.Fatalf("manifest: %v", err)
	}
	_, _ = store.PutBytes(ctx, "manifest.json", manifestBytes)
	var out bytes.Buffer
	err = StreamSnapshot(ctx, store, "manifest.json", &out)
	if err == nil {
		t.Fatalf("expected hash mismatch error")
	}
}
