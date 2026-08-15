package config

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/lawrencegripper/zfs-s3nd/internal/appsettings"
)

const (
	DefaultChunkSize                   = int64(64 * 1024 * 1024)
	DefaultStorageRootPrefix           = "zfs-s3nd/v1"
	DefaultCleanupWorkerInterval       = 30 * time.Second
	DefaultReconcileStaleAfter         = 10 * time.Minute
	DefaultAbandonedUploadCleanupAfter = 24 * time.Hour
	DefaultValidationSchedulerInterval = time.Hour
)

type Config struct {
	HTTPPort       string
	SSHPort        string
	SSHHostKeyPath string

	WebAdminPassword        string
	RestoreSSHCommandPrefix string

	DatabasePath string

	S3Endpoint                           string
	S3Region                             string
	S3Bucket                             string
	S3AccessKeyID                        string
	S3SecretAccessKey                    string
	S3ForcePathStyle                     bool
	StorageEncryptionPassphrase          string
	StorageRootPrefix                    string
	MaxIncrementalChainDepth             int64
	MaxIncrementalChainDepthEnvOverride  string
	UploadThroughputLimitBytesPerSecond  int64
	UploadThroughputLimitMbpsEnvOverride string
	CatalogBackupInterval                time.Duration
	CleanupWorkerInterval                time.Duration
	ReconcileStaleAfter                  time.Duration
	AbandonedUploadCleanupAfter          time.Duration
	ValidationInterval                   time.Duration
	ValidationLimit                      int64
	ShutdownGracePeriod                  time.Duration
}

func Load() (Config, error) {
	uploadThroughputOverride := nonEmptyEnv(appsettings.UploadThroughputLimitEnv)
	uploadThroughputValue := uploadThroughputOverride
	if uploadThroughputValue == "" {
		uploadThroughputValue = appsettings.DefaultUploadThroughputLimitMbps
	}
	uploadThroughputLimit, err := appsettings.ParseThroughputLimitMbps(uploadThroughputValue)
	if err != nil {
		return Config{}, fmt.Errorf("parse %s: %w", appsettings.UploadThroughputLimitEnv, err)
	}
	maxChainDepthOverride := nonEmptyEnv(appsettings.MaxIncrementalChainDepthEnv)
	maxChainDepthValue := maxChainDepthOverride
	if maxChainDepthValue == "" {
		maxChainDepthValue = appsettings.DefaultMaxIncrementalChainDepth
	}
	maxIncrementalChainDepth, err := appsettings.ParseMaxIncrementalChainDepth(maxChainDepthValue)
	if err != nil {
		return Config{}, fmt.Errorf("parse %s: %w", appsettings.MaxIncrementalChainDepthEnv, err)
	}
	validationLimit, err := envInt64("VALIDATION_LIMIT", 25)
	if err != nil {
		return Config{}, err
	}
	if validationLimit <= 0 {
		return Config{}, fmt.Errorf("VALIDATION_LIMIT must be greater than zero")
	}

	shutdownGrace, err := envDuration("SHUTDOWN_GRACE_PERIOD", 15*time.Second)
	if err != nil {
		return Config{}, err
	}
	catalogBackupInterval, err := envDuration("CATALOG_BACKUP_INTERVAL", 24*time.Hour)
	if err != nil {
		return Config{}, err
	}
	cleanupWorkerInterval, err := envDuration("CLEANUP_WORKER_INTERVAL", DefaultCleanupWorkerInterval)
	if err != nil {
		return Config{}, err
	}
	reconcileStaleAfter, err := envDuration("RECONCILE_STALE_AFTER", DefaultReconcileStaleAfter)
	if err != nil {
		return Config{}, err
	}
	abandonedUploadCleanupAfter, err := envDuration("ABANDONED_UPLOAD_CLEANUP_AFTER", DefaultAbandonedUploadCleanupAfter)
	if err != nil {
		return Config{}, err
	}
	validationInterval, err := envDuration("VALIDATION_INTERVAL", DefaultValidationSchedulerInterval)
	if err != nil {
		return Config{}, err
	}
	storageEncryptionPassphrase := parseStorageEncryptionPassphrase(env("STORAGE_ENCRYPTION_KEY", ""))

	databasePath := env("DATABASE_PATH", "./data/catalog.db")
	return Config{
		HTTPPort:                             env("HTTP_PORT", env("PORT", "3000")),
		SSHPort:                              env("SSH_PORT", "2222"),
		SSHHostKeyPath:                       env("SSH_HOST_KEY_PATH", filepath.Join(filepath.Dir(databasePath), "ssh_host_ed25519")),
		WebAdminPassword:                     env("WEB_ADMIN_PASSWORD", ""),
		RestoreSSHCommandPrefix:              env("RESTORE_SSH_COMMAND_PREFIX", defaultRestoreSSHCommandPrefix()),
		DatabasePath:                         databasePath,
		S3Endpoint:                           env("S3_ENDPOINT", ""),
		S3Region:                             env("S3_REGION", "us-east-1"),
		S3Bucket:                             env("S3_BUCKET", ""),
		S3AccessKeyID:                        env("S3_ACCESS_KEY_ID", ""),
		S3SecretAccessKey:                    env("S3_SECRET_ACCESS_KEY", ""),
		S3ForcePathStyle:                     envBool("S3_FORCE_PATH_STYLE", false),
		StorageEncryptionPassphrase:          storageEncryptionPassphrase,
		StorageRootPrefix:                    env("STORAGE_ROOT_PREFIX", DefaultStorageRootPrefix),
		MaxIncrementalChainDepth:             maxIncrementalChainDepth,
		MaxIncrementalChainDepthEnvOverride:  maxChainDepthOverride,
		UploadThroughputLimitBytesPerSecond:  uploadThroughputLimit,
		UploadThroughputLimitMbpsEnvOverride: uploadThroughputOverride,
		CatalogBackupInterval:                catalogBackupInterval,
		CleanupWorkerInterval:                cleanupWorkerInterval,
		ReconcileStaleAfter:                  reconcileStaleAfter,
		AbandonedUploadCleanupAfter:          abandonedUploadCleanupAfter,
		ValidationInterval:                   validationInterval,
		ValidationLimit:                      validationLimit,
		ShutdownGracePeriod:                  shutdownGrace,
	}, nil
}

func ParseThroughputLimitMbps(value string) (int64, error) {
	if strings.TrimSpace(value) == "" {
		return 0, nil
	}
	return appsettings.ParseThroughputLimitMbps(value)
}

func defaultRestoreSSHCommandPrefix() string {
	domain := strings.TrimSpace(os.Getenv("RAILWAY_TCP_PROXY_DOMAIN"))
	port := strings.TrimSpace(os.Getenv("RAILWAY_TCP_PROXY_PORT"))
	if domain != "" && port != "" {
		return fmt.Sprintf("ssh -p %s [named_source]@%s", port, domain)
	}
	return "ssh [named_source]@<ssh-host>"
}

func nonEmptyEnv(key string) string {
	return strings.TrimSpace(os.Getenv(key))
}

func env(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}

func parseStorageEncryptionPassphrase(value string) string {
	return strings.TrimSpace(value)
}

func envBool(key string, fallback bool) bool {
	value := os.Getenv(key)
	if value == "" {
		return fallback
	}
	parsed, err := strconv.ParseBool(value)
	if err != nil {
		return fallback
	}
	return parsed
}

func envDuration(key string, fallback time.Duration) (time.Duration, error) {
	parsed, err := time.ParseDuration(env(key, fallback.String()))
	if err != nil {
		return 0, fmt.Errorf("parse %s: %w", key, err)
	}
	return parsed, nil
}

func envInt64(key string, fallback int64) (int64, error) {
	value := os.Getenv(key)
	if value == "" {
		return fallback, nil
	}
	parsed, err := strconv.ParseInt(value, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("parse %s: %w", key, err)
	}
	return parsed, nil
}
