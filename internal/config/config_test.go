package config

import (
	"testing"
	"time"
)

func TestLoadReadsWebAdminPassword(t *testing.T) {
	t.Setenv("WEB_ADMIN_PASSWORD", "secret")
	cfg, err := Load()
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if cfg.WebAdminPassword != "secret" {
		t.Fatalf("WebAdminPassword got %q", cfg.WebAdminPassword)
	}
}

func TestLoadReadsRestoreSSHCommandPrefix(t *testing.T) {
	t.Setenv("RESTORE_SSH_COMMAND_PREFIX", "ssh -p 15227 truenas@example.test")
	cfg, err := Load()
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if cfg.RestoreSSHCommandPrefix != "ssh -p 15227 truenas@example.test" {
		t.Fatalf("RestoreSSHCommandPrefix got %q", cfg.RestoreSSHCommandPrefix)
	}
}

func TestLoadDerivesRestoreSSHCommandPrefixFromRailwayTCPProxy(t *testing.T) {
	t.Setenv("RESTORE_SSH_COMMAND_PREFIX", "")
	t.Setenv("RAILWAY_TCP_PROXY_DOMAIN", "example.proxy.rlwy.net")
	t.Setenv("RAILWAY_TCP_PROXY_PORT", "12345")
	cfg, err := Load()
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	want := "ssh -p 12345 [named_source]@example.proxy.rlwy.net"
	if cfg.RestoreSSHCommandPrefix != want {
		t.Fatalf("RestoreSSHCommandPrefix got %q want %q", cfg.RestoreSSHCommandPrefix, want)
	}
}

func TestLoadDefaultsRestoreSSHCommandPrefixWithoutRailwayTCPProxy(t *testing.T) {
	t.Setenv("RESTORE_SSH_COMMAND_PREFIX", "")
	t.Setenv("RAILWAY_TCP_PROXY_DOMAIN", "")
	t.Setenv("RAILWAY_TCP_PROXY_PORT", "")
	cfg, err := Load()
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if cfg.RestoreSSHCommandPrefix != "ssh [named_source]@<ssh-host>" {
		t.Fatalf("RestoreSSHCommandPrefix got %q", cfg.RestoreSSHCommandPrefix)
	}
}

func TestLoadDefaultsCatalogBackupIntervalToDaily(t *testing.T) {
	cfg, err := Load()
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if cfg.CatalogBackupInterval != 24*time.Hour {
		t.Fatalf("CatalogBackupInterval got %s", cfg.CatalogBackupInterval)
	}
}

func TestLoadReadsCatalogBackupInterval(t *testing.T) {
	t.Setenv("CATALOG_BACKUP_INTERVAL", "1h")
	cfg, err := Load()
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if cfg.CatalogBackupInterval.String() != "1h0m0s" {
		t.Fatalf("CatalogBackupInterval got %s", cfg.CatalogBackupInterval)
	}
}

func TestLoadReadsUploadThroughputLimit(t *testing.T) {
	t.Setenv("UPLOAD_THROUGHPUT_LIMIT_MBIT", "45")
	cfg, err := Load()
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if cfg.UploadThroughputLimitBytesPerSecond != 5_625_000 {
		t.Fatalf("UploadThroughputLimitBytesPerSecond got %d", cfg.UploadThroughputLimitBytesPerSecond)
	}
}

func TestParseThroughputLimitMbps(t *testing.T) {
	for _, test := range []struct {
		value string
		want  int64
	}{
		{value: "", want: 0},
		{value: "0", want: 0},
		{value: "0.5", want: 62_500},
		{value: "45", want: 5_625_000},
	} {
		got, err := ParseThroughputLimitMbps(test.value)
		if err != nil {
			t.Fatalf("ParseThroughputLimitMbps(%q): %v", test.value, err)
		}
		if got != test.want {
			t.Fatalf("ParseThroughputLimitMbps(%q) got %d want %d", test.value, got, test.want)
		}
	}
	for _, value := range []string{"nope", "-1", "NaN", "Inf", "0.05"} {
		if _, err := ParseThroughputLimitMbps(value); err == nil {
			t.Fatalf("ParseThroughputLimitMbps(%q) unexpectedly succeeded", value)
		}
	}
}

func TestLoadReadsMaxIncrementalChainDepth(t *testing.T) {
	t.Setenv("MAX_INCREMENTAL_CHAIN_DEPTH", "7")
	cfg, err := Load()
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if cfg.MaxIncrementalChainDepth != 7 {
		t.Fatalf("MaxIncrementalChainDepth got %d", cfg.MaxIncrementalChainDepth)
	}
}

func TestLoadDerivesStorageEncryptionKeyFromPassphrase(t *testing.T) {
	t.Setenv("STORAGE_ENCRYPTION_KEY", "correct horse battery staple")
	cfg, err := Load()
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if cfg.StorageEncryptionPassphrase != "correct horse battery staple" {
		t.Fatalf("StorageEncryptionPassphrase got %q", cfg.StorageEncryptionPassphrase)
	}
}

func TestLoadRejectsBlankStorageEncryptionPassphrase(t *testing.T) {
	if got := parseStorageEncryptionPassphrase("   "); got != "" {
		t.Fatalf("blank passphrase got %q, want empty", got)
	}
}

func TestLoadAcceptsRawLookingValuesAsPassphrases(t *testing.T) {
	rawLooking := "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	if got := parseStorageEncryptionPassphrase(rawLooking); got != rawLooking {
		t.Fatalf("raw-looking value got %q want unchanged passphrase", got)
	}
}

func TestLoadReadsCanonicalS3Env(t *testing.T) {
	t.Setenv("S3_BUCKET", "bucket-name")
	t.Setenv("S3_ENDPOINT", "https://storage.railway.app")
	t.Setenv("S3_REGION", "auto")
	t.Setenv("S3_ACCESS_KEY_ID", "access")
	t.Setenv("S3_SECRET_ACCESS_KEY", "secret")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if cfg.S3Bucket != "bucket-name" || cfg.S3Endpoint != "https://storage.railway.app" || cfg.S3Region != "auto" || cfg.S3AccessKeyID != "access" || cfg.S3SecretAccessKey != "secret" {
		t.Fatalf("unexpected s3 config: %+v", cfg)
	}
}
