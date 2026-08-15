package storage

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"strings"
	"testing"
)

func TestEncryptedStoreEncryptsAtRestAndDecryptsReads(t *testing.T) {
	ctx := context.Background()
	base := NewMemoryStore()
	store, err := NewEncryptedStore(base, "test passphrase")
	if err != nil {
		t.Fatalf("new encrypted store: %v", err)
	}

	plaintext := []byte("zfs send stream bytes")
	info, err := store.PutBytes(ctx, "chunks/000", plaintext)
	if err != nil {
		t.Fatalf("put: %v", err)
	}
	sha := sha256.Sum256(plaintext)
	if info.Size != int64(len(plaintext)) || info.SHA256 != hex.EncodeToString(sha[:]) {
		t.Fatalf("put info = %+v", info)
	}

	raw, err := base.GetBytes(ctx, "chunks/000")
	if err != nil {
		t.Fatalf("get raw: %v", err)
	}
	if bytes.Equal(raw, plaintext) || bytes.Contains(raw, plaintext) {
		t.Fatalf("ciphertext contains plaintext: %q", raw)
	}
	if !bytes.HasPrefix(raw, encryptedObjectMagic) {
		t.Fatalf("ciphertext missing magic prefix: %q", raw[:min(len(raw), len(encryptedObjectMagic))])
	}

	got, err := store.GetBytes(ctx, "chunks/000")
	if err != nil {
		t.Fatalf("get decrypted: %v", err)
	}
	if !bytes.Equal(got, plaintext) {
		t.Fatalf("decrypted got %q want %q", got, plaintext)
	}

	reader, err := store.GetReader(ctx, "chunks/000")
	if err != nil {
		t.Fatalf("reader: %v", err)
	}
	readerBytes, err := io.ReadAll(reader)
	_ = reader.Close()
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if !bytes.Equal(readerBytes, plaintext) {
		t.Fatalf("reader got %q want %q", readerBytes, plaintext)
	}

	head, err := store.Head(ctx, "chunks/000")
	if err != nil {
		t.Fatalf("head: %v", err)
	}
	if head.Size != int64(len(plaintext)) || head.SHA256 != hex.EncodeToString(sha[:]) {
		t.Fatalf("head = %+v", head)
	}
}

func TestEncryptedStoreKeepsManifestPlaintext(t *testing.T) {
	ctx := context.Background()
	base := NewMemoryStore()
	store, err := NewEncryptedStore(base, "test passphrase")
	if err != nil {
		t.Fatalf("new encrypted store: %v", err)
	}
	manifest := []byte(`{"version":1,"format":"zfs-s3nd.snapshot.v1"}`)
	if _, err := store.PutBytes(ctx, "root/sources/nas/pools/tank/datasets/photos/@snapshots/s1/manifest.json", manifest); err != nil {
		t.Fatalf("put manifest: %v", err)
	}
	raw, err := base.GetBytes(ctx, "root/sources/nas/pools/tank/datasets/photos/@snapshots/s1/manifest.json")
	if err != nil {
		t.Fatalf("get raw manifest: %v", err)
	}
	if !bytes.Equal(raw, manifest) {
		t.Fatalf("manifest should be plaintext, got %q", raw)
	}
	got, err := store.GetBytes(ctx, "root/sources/nas/pools/tank/datasets/photos/@snapshots/s1/manifest.json")
	if err != nil {
		t.Fatalf("get manifest: %v", err)
	}
	if !bytes.Equal(got, manifest) {
		t.Fatalf("manifest got %q want %q", got, manifest)
	}
}

func TestEncryptedStoreRejectsWrongKey(t *testing.T) {
	ctx := context.Background()
	base := NewMemoryStore()
	writer, err := NewEncryptedStore(base, "test passphrase")
	if err != nil {
		t.Fatalf("new writer: %v", err)
	}
	if _, err := writer.PutBytes(ctx, "chunks/000", []byte("secret stream")); err != nil {
		t.Fatalf("put: %v", err)
	}

	reader, err := NewEncryptedStore(base, "wrong passphrase")
	if err != nil {
		t.Fatalf("new reader: %v", err)
	}
	_, err = reader.GetBytes(ctx, "chunks/000")
	if err == nil || !strings.Contains(err.Error(), "decrypt object") {
		t.Fatalf("error got %v want decrypt failure", err)
	}
}

func TestEncryptedStoreRejectsPlaintextObjects(t *testing.T) {
	ctx := context.Background()
	base := NewMemoryStore()
	if _, err := base.PutBytes(ctx, "old/chunk", []byte("legacy plaintext")); err != nil {
		t.Fatalf("put legacy: %v", err)
	}
	store, err := NewEncryptedStore(base, "test passphrase")
	if err != nil {
		t.Fatalf("new encrypted store: %v", err)
	}
	_, err = store.GetBytes(ctx, "old/chunk")
	if err == nil || !strings.Contains(err.Error(), "is not encrypted") {
		t.Fatalf("error got %v want plaintext rejection", err)
	}
}
