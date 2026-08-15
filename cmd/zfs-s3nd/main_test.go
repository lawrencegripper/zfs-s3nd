package main

import (
	"context"
	"io"
	"log/slog"
	"strings"
	"testing"

	"github.com/lawrencegripper/zfs-s3nd/internal/config"
)

func TestConfiguredStoreRequiresDurableObjectStorage(t *testing.T) {
	_, err := configuredStore(context.Background(), config.Config{StorageEncryptionPassphrase: "correct horse battery staple"}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err == nil || !strings.Contains(err.Error(), "S3_BUCKET") {
		t.Fatalf("configuredStore error got %v want missing S3_BUCKET", err)
	}
}
