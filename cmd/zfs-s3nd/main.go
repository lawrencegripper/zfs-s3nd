package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/exec"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/lawrencegripper/zfs-s3nd/internal/appsettings"
	"github.com/lawrencegripper/zfs-s3nd/internal/catalog"
	"github.com/lawrencegripper/zfs-s3nd/internal/catalog/db"
	"github.com/lawrencegripper/zfs-s3nd/internal/catalogbackup"
	"github.com/lawrencegripper/zfs-s3nd/internal/cleanup"
	"github.com/lawrencegripper/zfs-s3nd/internal/config"
	"github.com/lawrencegripper/zfs-s3nd/internal/deletion"
	"github.com/lawrencegripper/zfs-s3nd/internal/ingest"
	"github.com/lawrencegripper/zfs-s3nd/internal/restore"
	"github.com/lawrencegripper/zfs-s3nd/internal/restoreplan"
	"github.com/lawrencegripper/zfs-s3nd/internal/sshserver"
	"github.com/lawrencegripper/zfs-s3nd/internal/storage"
	"github.com/lawrencegripper/zfs-s3nd/internal/validation"
	"github.com/lawrencegripper/zfs-s3nd/internal/web"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))
	if err := run(logger, os.Args[1:]); err != nil {
		logger.Error("fatal", "error", err)
		os.Exit(1)
	}
}

func run(logger *slog.Logger, args []string) error {
	cfg, err := config.Load()
	if err != nil {
		return err
	}

	if len(args) > 0 && args[0] == "restore-stream" {
		if len(args) != 2 {
			return fmt.Errorf("usage: zfs-s3nd restore-stream <manifest-key|snapshot-id>")
		}
		if strings.HasPrefix(args[1], "snap_") {
			cat, err := catalog.Open(cfg.DatabasePath)
			if err != nil {
				return err
			}
			defer cat.Close()
			if err := cat.Migrate(); err != nil {
				return err
			}
			return restoreSnapshot(logger, cfg, cat, args[1])
		}
		return restoreStream(logger, cfg, args[1])
	}
	if len(args) > 0 && args[0] == "restore-chain-to" {
		if len(args) != 3 {
			return fmt.Errorf("usage: zfs-s3nd restore-chain-to <snapshot-id> <zfs-target>")
		}
		cat, err := catalog.Open(cfg.DatabasePath)
		if err != nil {
			return err
		}
		defer cat.Close()
		if err := cat.Migrate(); err != nil {
			return err
		}
		return restoreSnapshotChainTo(logger, cfg, cat, args[1], args[2])
	}

	cat, err := catalog.Open(cfg.DatabasePath)
	if err != nil {
		return err
	}
	defer cat.Close()

	if err := cat.Migrate(); err != nil {
		return err
	}

	if len(args) > 0 {
		switch args[0] {
		case "serve":
			return serve(logger, cfg, cat)
		case "backup-sqlite":
			return backupSQLite(logger, cfg, cat)
		case "validate-due":
			return validateDue(logger, cfg, cat)
		case "validate-chain":
			if len(args) != 2 {
				return fmt.Errorf("usage: zfs-s3nd validate-chain <snapshot-id>")
			}
			return validateChain(logger, cfg, cat, args[1])
		default:
			return fmt.Errorf("unknown command %q", args[0])
		}
	}
	return serve(logger, cfg, cat)
}

func serve(logger *slog.Logger, cfg config.Config, cat *catalog.Catalog) error {
	ctx, stopBackground := context.WithCancel(context.Background())
	defer stopBackground()
	store, err := configuredStore(ctx, cfg, logger)
	if err != nil {
		return err
	}
	keyBuilder := storage.KeyBuilder{RootPrefix: cfg.StorageRootPrefix}
	repo := catalog.NewRepository(cat.DB())
	settingsManager, err := appsettings.New(ctx, cat.DB(), appsettings.Overrides{
		UploadThroughputLimitMbps: cfg.UploadThroughputLimitMbpsEnvOverride,
		MaxIncrementalChainDepth:  cfg.MaxIncrementalChainDepthEnvOverride,
	})
	if err != nil {
		return fmt.Errorf("load application settings: %w", err)
	}
	if cfg.ReconcileStaleAfter > 0 {
		cutoff := time.Now().Add(-cfg.ReconcileStaleAfter)
		result, err := repo.ReconcileStaleUploads(ctx, cutoff, 100)
		if err != nil {
			return fmt.Errorf("reconcile stale uploads: %w", err)
		}
		if result.StaleUploadsFailed > 0 {
			logger.Warn("reconciled stale uploads", "failed_uploads", result.StaleUploadsFailed, "operation_id", result.OperationID)
		}
		validationResult, err := validation.ReconcileStaleJobs(ctx, cat.DB(), cutoff)
		if err != nil {
			return fmt.Errorf("reconcile stale validation jobs: %w", err)
		}
		if validationResult.JobsFailed > 0 {
			logger.Warn("reconciled stale validation jobs", "failed_jobs", validationResult.JobsFailed)
		}
	}
	if cfg.AbandonedUploadCleanupAfter > 0 {
		result, err := (cleanup.AbandonedUploadCleaner{Repo: repo, Store: store}).Cleanup(ctx, time.Now().Add(-cfg.AbandonedUploadCleanupAfter), 100)
		if err != nil {
			return fmt.Errorf("cleanup abandoned uploads: %w", err)
		}
		if result.UploadsDeleted > 0 || result.ObjectsDeleted > 0 {
			logger.Info("cleaned abandoned uploads", "uploads_deleted", result.UploadsDeleted, "snapshots_deleted", result.SnapshotsDeleted, "objects_deleted", result.ObjectsDeleted)
		}
	}
	if cfg.ReconcileStaleAfter > 0 {
		startUploadReconciliationScheduler(ctx, logger, repo, cfg.ReconcileStaleAfter, time.Minute)
	}
	if cfg.CatalogBackupInterval > 0 {
		startCatalogBackupScheduler(ctx, logger, cat, repo, store, keyBuilder, cfg.CatalogBackupInterval)
	}
	deletion.StartWorker(ctx, logger, deletion.Worker{DB: cat.DB(), Store: store, Logger: logger, Limit: 10}, cfg.CleanupWorkerInterval)
	ingestSvc := ingest.Service{
		Repo:                     repo,
		Store:                    store,
		Keys:                     keyBuilder,
		ChunkSize:                config.DefaultChunkSize,
		MaxIncrementalChainDepth: cfg.MaxIncrementalChainDepth,
		MaxIncrementalChainDepthFunc: func() int64 {
			return settingsManager.Current().MaxIncrementalChainDepthValue
		},
	}

	addr := ":" + cfg.HTTPPort
	validationRunner := validation.Runner{DB: cat.DB(), Store: store, Checker: validation.ZstreamdumpChecker{}, Executor: "local"}
	startValidationScheduler(ctx, logger, validationRunner, cfg.ValidationInterval, cfg.ValidationLimit)
	server := web.New(addr, cat, logger, cfg.WebAdminPassword)
	server.SetRestoreSSHCommandPrefix(cfg.RestoreSSHCommandPrefix)
	server.SetStore(store)
	server.SetValidationRunner(validationRunner)
	server.SetSettingsManager(settingsManager)

	hostKey, err := sshserver.LoadOrCreateHostKey(cfg.SSHHostKeyPath)
	if err != nil {
		return err
	}
	sshSrv := &sshserver.Server{Addr: ":" + cfg.SSHPort, Signer: hostKey, DB: cat.DB(), Ingest: ingestSvc, Logger: logger, UploadRateBytesPerSecond: cfg.UploadThroughputLimitBytesPerSecond, UploadRateBytesPerSecondFunc: func() int64 {
		return settingsManager.Current().UploadThroughputLimitBytesPerSecond
	}, PostUploadValidation: func(snapshotID string) {
		go func() {
			if err := validationRunner.RunSnapshot(context.Background(), snapshotID); err != nil {
				logger.Warn("post-upload validation failed", "snapshot_id", snapshotID, "error", err)
				return
			}
			logger.Info("post-upload validation succeeded", "snapshot_id", snapshotID)
		}()
	}}

	errCh := make(chan error, 2)
	go func() {
		logger.Info("starting http server", "addr", addr)
		errCh <- server.ListenAndServe()
	}()
	go func() {
		errCh <- sshSrv.ListenAndServe()
	}()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	select {
	case sig := <-sigCh:
		logger.Info("shutdown signal received", "signal", sig.String())
		stopBackground()
		ctx, cancel := context.WithTimeout(context.Background(), cfg.ShutdownGracePeriod)
		defer cancel()
		if err := server.Shutdown(ctx); err != nil {
			logger.Warn("http shutdown failed", "error", err)
		}
		if err := sshSrv.Shutdown(ctx); err != nil {
			return fmt.Errorf("ssh shutdown: %w", err)
		}
		return nil
	case err := <-errCh:
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return err
	}
}

func startUploadReconciliationScheduler(ctx context.Context, logger *slog.Logger, repo *catalog.Repository, staleAfter, interval time.Duration) {
	logger.Info("starting upload reconciliation scheduler", "stale_after", staleAfter.String(), "interval", interval.String())
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				result, err := repo.ReconcileStaleUploads(ctx, time.Now().Add(-staleAfter), 100)
				if err != nil {
					logger.Error("scheduled upload reconciliation failed", "error", err)
					continue
				}
				if result.StaleUploadsFailed > 0 {
					logger.Warn("scheduled upload reconciliation marked stale uploads failed", "failed_uploads", result.StaleUploadsFailed, "operation_id", result.OperationID)
				}
			}
		}
	}()
}

func startCatalogBackupScheduler(ctx context.Context, logger *slog.Logger, cat *catalog.Catalog, repo *catalog.Repository, store storage.Store, keys storage.KeyBuilder, interval time.Duration) {
	logger.Info("starting catalog backup scheduler", "interval", interval.String())
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				result, err := (catalogbackup.Runner{Catalog: cat, Repo: repo, Store: store, Keys: keys}).Run(ctx)
				if err != nil {
					logger.Error("scheduled catalog backup failed", "error", err)
					continue
				}
				logger.Info("scheduled catalog backup completed", "operation_id", result.OperationID, "backup_id", result.BackupID, "object_key", result.ObjectKey, "metadata_object_key", result.MetadataObjectKey, "size_bytes", result.SizeBytes, "sha256", result.ChecksumSHA256)
			}
		}
	}()
}

func startValidationScheduler(ctx context.Context, logger *slog.Logger, runner validation.Runner, interval time.Duration, limit int64) {
	if interval <= 0 {
		return
	}
	logger.Info("starting validation scheduler", "interval", interval.String())
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				result, err := runner.RunDue(ctx, limit)
				if err != nil {
					logger.Error("scheduled validation failed", "error", err)
					continue
				}
				if result.Checked > 0 || result.Failed > 0 {
					logger.Info("scheduled validation completed", "checked", result.Checked, "succeeded", result.Succeeded, "failed", result.Failed)
				}
			}
		}
	}()
}

func configuredStore(ctx context.Context, cfg config.Config, logger *slog.Logger) (storage.Store, error) {
	if cfg.S3Bucket == "" {
		return nil, fmt.Errorf("S3_BUCKET is required; durable object storage must be configured")
	}
	base, err := storage.NewS3Store(ctx, storage.S3Config{Endpoint: cfg.S3Endpoint, Region: cfg.S3Region, Bucket: cfg.S3Bucket, AccessKeyID: cfg.S3AccessKeyID, SecretAccessKey: cfg.S3SecretAccessKey, ForcePathStyle: cfg.S3ForcePathStyle})
	if err != nil {
		return nil, err
	}
	if cfg.StorageEncryptionPassphrase == "" {
		return nil, fmt.Errorf("STORAGE_ENCRYPTION_KEY is required")
	}
	logger.Info("storage encryption enabled", "algorithm", "XChaCha20-Poly1305")
	return storage.NewEncryptedStore(base, cfg.StorageEncryptionPassphrase)
}

func restoreStream(logger *slog.Logger, cfg config.Config, manifestKey string) error {
	store, err := configuredStore(context.Background(), cfg, logger)
	if err != nil {
		return err
	}
	return restore.StreamSnapshot(context.Background(), store, manifestKey, os.Stdout)
}

func restoreSnapshot(logger *slog.Logger, cfg config.Config, cat *catalog.Catalog, snapshotID string) error {
	ctx := context.Background()
	store, err := configuredStore(ctx, cfg, logger)
	if err != nil {
		return err
	}
	plan, err := restoreplan.Build(ctx, catalog.NewRepository(cat.DB()), snapshotID)
	if err != nil {
		return err
	}
	if err := plan.ValidateStreamable(); err != nil {
		return err
	}
	_, _ = os.Stderr.WriteString(plan.ParentRequirement())
	if err := restore.StreamSnapshot(ctx, store, plan.ManifestKey(), os.Stdout); err != nil {
		return err
	}
	_, _ = os.Stderr.WriteString(plan.CLINextRestore())
	return nil
}

func restoreSnapshotChainTo(logger *slog.Logger, cfg config.Config, cat *catalog.Catalog, snapshotID, target string) error {
	ctx := context.Background()
	store, err := configuredStore(ctx, cfg, logger)
	if err != nil {
		return err
	}
	chain, err := db.New(cat.DB()).ListSnapshotRestoreChain(ctx, snapshotID)
	if err != nil {
		return err
	}
	if len(chain) == 0 {
		return fmt.Errorf("snapshot %s not found", snapshotID)
	}
	for _, item := range chain {
		if item.Status != "committed" || !item.ManifestObjectKey.Valid || item.ManifestObjectKey.String == "" {
			return fmt.Errorf("snapshot %s is not committed or has no manifest", item.Name)
		}
		logger.Info("restoring snapshot stream", "snapshot", item.Name, "target", target)
		if err := receiveManifestToZFS(ctx, store, item.ManifestObjectKey.String, target); err != nil {
			return err
		}
	}
	return nil
}

func receiveManifestToZFS(ctx context.Context, store storage.Store, manifestKey, target string) error {
	name := "zfs"
	args := []string{"recv", "-F", target}
	if os.Geteuid() != 0 {
		name = "sudo"
		args = []string{"-n", "zfs", "recv", "-F", target}
	}
	cmd := exec.CommandContext(ctx, name, args...)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	stdin, err := cmd.StdinPipe()
	if err != nil {
		return err
	}
	if err := cmd.Start(); err != nil {
		return err
	}
	streamErr := restore.StreamSnapshot(ctx, store, manifestKey, stdin)
	closeErr := stdin.Close()
	waitErr := cmd.Wait()
	if streamErr != nil {
		return streamErr
	}
	if closeErr != nil {
		return closeErr
	}
	return waitErr
}

func backupSQLite(logger *slog.Logger, cfg config.Config, cat *catalog.Catalog) error {
	ctx := context.Background()
	store, err := configuredStore(ctx, cfg, logger)
	if err != nil {
		return err
	}
	result, err := (catalogbackup.Runner{Catalog: cat, Repo: catalog.NewRepository(cat.DB()), Store: store, Keys: storage.KeyBuilder{RootPrefix: cfg.StorageRootPrefix}}).Run(ctx)
	if err != nil {
		return err
	}
	logger.Info("sqlite backup completed", "operation_id", result.OperationID, "backup_id", result.BackupID, "object_key", result.ObjectKey, "metadata_object_key", result.MetadataObjectKey, "size_bytes", result.SizeBytes, "sha256", result.ChecksumSHA256)
	return nil
}

func validateChain(logger *slog.Logger, cfg config.Config, cat *catalog.Catalog, snapshotID string) error {
	ctx := context.Background()
	store, err := configuredStore(ctx, cfg, logger)
	if err != nil {
		return err
	}
	if err := (validation.Validator{DB: cat.DB(), Store: store, Checker: validation.ZstreamdumpChecker{}}).ValidateChain(ctx, snapshotID); err != nil {
		return err
	}
	logger.Info("snapshot restore chain validation succeeded", "snapshot_id", snapshotID)
	return nil
}

func validateDue(logger *slog.Logger, cfg config.Config, cat *catalog.Catalog) error {
	ctx := context.Background()
	store, err := configuredStore(ctx, cfg, logger)
	if err != nil {
		return err
	}
	result, err := (validation.Runner{DB: cat.DB(), Store: store, Checker: validation.ZstreamdumpChecker{}, Executor: "local"}).RunDue(ctx, cfg.ValidationLimit)
	if err != nil {
		return err
	}
	logger.Info("due validation completed", "checked", result.Checked, "succeeded", result.Succeeded, "failed", result.Failed)
	return nil
}
