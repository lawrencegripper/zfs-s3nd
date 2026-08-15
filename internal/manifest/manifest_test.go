package manifest

import (
	"strings"
	"testing"
)

func TestManifestMarshalValidateRoundTrip(t *testing.T) {
	m := New(
		Identity{Source: "nas-home", Pool: "tank", Dataset: "photos", Snapshot: "snap1"},
		Lineage{},
		Stream{Raw: true, SizeBytes: 5, SHA256: strings.Repeat("a", 64), ChunkSize: 64},
		[]Chunk{{Index: 0, ObjectKey: "chunks/000", SizeBytes: 5, OffsetStart: 0, OffsetEnd: 5, SHA256: strings.Repeat("b", 64)}},
	)
	data, err := m.MarshalCanonical()
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if !strings.Contains(string(data), `"version": 1`) {
		t.Fatalf("manifest missing version: %s", data)
	}
	if !strings.Contains(string(data), `"format": "zfs-s3nd.snapshot.v1"`) {
		t.Fatalf("manifest missing format: %s", data)
	}
	if !strings.Contains(string(data), `"algorithm": "XChaCha20-Poly1305"`) || !strings.Contains(string(data), `"kdf": "argon2id"`) {
		t.Fatalf("manifest missing encryption metadata: %s", data)
	}
	got, err := Unmarshal(data)
	if err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if got.Identity.Dataset != "photos" || got.Chunks[0].SizeBytes != 5 {
		t.Fatalf("unexpected roundtrip manifest: %+v", got)
	}
}

func TestManifestRejectsUnsupportedVersion(t *testing.T) {
	m := New(
		Identity{Source: "nas-home", Pool: "tank", Dataset: "photos", Snapshot: "snap1"},
		Lineage{},
		Stream{SizeBytes: 5, SHA256: strings.Repeat("a", 64), ChunkSize: 64},
		[]Chunk{{Index: 0, ObjectKey: "chunks/000", SizeBytes: 5, OffsetStart: 0, OffsetEnd: 5, SHA256: strings.Repeat("b", 64)}},
	)
	m.Version = 2
	if err := m.Validate(); err == nil || !strings.Contains(err.Error(), "unsupported manifest version 2") {
		t.Fatalf("validation error got %v want unsupported version", err)
	}
}

func TestManifestRejectsBadChunkOrder(t *testing.T) {
	m := New(
		Identity{Source: "nas-home", Pool: "tank", Dataset: "photos", Snapshot: "snap1"},
		Lineage{},
		Stream{SizeBytes: 5, SHA256: strings.Repeat("a", 64), ChunkSize: 64},
		[]Chunk{{Index: 1, ObjectKey: "chunks/001", SizeBytes: 5, OffsetStart: 0, OffsetEnd: 5, SHA256: strings.Repeat("b", 64)}},
	)
	if err := m.Validate(); err == nil {
		t.Fatal("expected validation failure")
	}
}
