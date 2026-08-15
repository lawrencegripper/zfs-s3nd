package storage

import "testing"

func TestKeyBuilderHumanReadableLayout(t *testing.T) {
	builder := KeyBuilder{RootPrefix: "zfs-s3nd/v1"}

	manifest := builder.ManifestKey("nas-home", "tank", "home/lg", "hourly-001")
	want := "zfs-s3nd/v1/sources/nas-home/pools/tank/datasets/home/lg/@snapshots/hourly-001/manifest.json"
	if manifest != want {
		t.Fatalf("manifest key mismatch\n got: %s\nwant: %s", manifest, want)
	}

	chunk := builder.ChunkKey("nas-home", "tank", "photos", "auto-2026-07-01", 42)
	want = "zfs-s3nd/v1/sources/nas-home/pools/tank/datasets/photos/@snapshots/auto-2026-07-01/chunks/000000000042.zfschunk"
	if chunk != want {
		t.Fatalf("chunk key mismatch\n got: %s\nwant: %s", chunk, want)
	}
}

func TestKeyBuilderEscapesUnsafeComponents(t *testing.T) {
	builder := KeyBuilder{RootPrefix: "root"}
	key := builder.ManifestKey("@bad source", "tank", "data set", "snap one")
	want := "root/sources/%40bad%20source/pools/tank/datasets/data%20set/@snapshots/snap%20one/manifest.json"
	if key != want {
		t.Fatalf("escaped key mismatch\n got: %s\nwant: %s", key, want)
	}
}
