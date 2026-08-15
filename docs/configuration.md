# Configuration

The service reads configuration from environment variables at startup.

## Required storage settings

| Variable | Default | Description |
| --- | --- | --- |
| `S3_BUCKET` | none | Bucket used for stream chunks, manifests, and catalog backups. Required. |
| `STORAGE_ENCRYPTION_KEY` | none | Passphrase used to derive the chunk-encryption key. Required. |
| `S3_ENDPOINT` | AWS SDK default | S3-compatible endpoint. |
| `S3_REGION` | `us-east-1` | S3 region. |
| `S3_ACCESS_KEY_ID` | AWS credential chain | Static access key when required by the provider. |
| `S3_SECRET_ACCESS_KEY` | AWS credential chain | Static secret key when required by the provider. |
| `S3_FORCE_PATH_STYLE` | `false` | Use path-style bucket URLs. Commonly required by local S3 implementations. |
| `STORAGE_ROOT_PREFIX` | `zfs-s3nd/v1` | Prefix for objects written by the service. |

Keep `STORAGE_ENCRYPTION_KEY` outside the deployment platform as part of the recovery material. Existing chunks cannot be read with a different passphrase.

## Network and access

| Variable | Default | Description |
| --- | --- | --- |
| `HTTP_PORT` | `PORT`, then `3000` | Administration HTTP port. |
| `SSH_PORT` | `2222` | SSH ingest and restore port. |
| `UPLOAD_THROUGHPUT_LIMIT_MBIT` | `0` (disabled) | Server-enforced per-upload limit in decimal Mbps (minimum positive value `0.1`). Clients may request a lower limit through the allowlisted SSH environment request. |
| `WEB_ADMIN_PASSWORD` | unset | Administrator password supplied by the environment. When unset, first-run setup stores a password hash in SQLite. |
| `SSH_HOST_KEY_PATH` | beside `DATABASE_PATH` | Persistent Ed25519 SSH host-key path. |
| `RESTORE_SSH_COMMAND_PREFIX` | Railway TCP proxy when available, otherwise `ssh [named_source]@<ssh-host>` | Base SSH command shown by the UI. When unset, `RAILWAY_TCP_PROXY_DOMAIN` and `RAILWAY_TCP_PROXY_PORT` are detected automatically. Upload commands retain `[named_source]`; restore commands replace the username with `restore`. |

The HTTP health endpoint is public. Administration pages and API endpoints require the administrator session or a bearer token created in the UI.

## Catalog and lifecycle

| Variable | Default | Description |
| --- | --- | --- |
| `DATABASE_PATH` | `./data/catalog.db` | SQLite catalog path. |
| `CATALOG_BACKUP_INTERVAL` | `24h` | Interval for catalog backups. Set to `0s` to disable. |
| `MAX_INCREMENTAL_CHAIN_DEPTH` | `30` | Maximum incoming incremental-chain depth. Values at or below zero disable the limit. |
| `CLEANUP_WORKER_INTERVAL` | `30s` | Interval for queued object deletion. Set to `0s` to disable. |
| `RECONCILE_STALE_AFTER` | `10m` | Age after which active upload and validation records are considered stale. Set to `0s` to disable reconciliation. |
| `ABANDONED_UPLOAD_CLEANUP_AFTER` | `24h` | Retention period for failed or aborted upload objects. Set to `0s` to disable cleanup. |
| `SHUTDOWN_GRACE_PERIOD` | `15s` | Time allowed for HTTP and SSH shutdown. Long uploads require a matching platform drain window. |

Durations use Go duration syntax, for example `30s`, `10m`, or `24h`.

## Validation

| Variable | Default | Description |
| --- | --- | --- |
| `VALIDATION_INTERVAL` | `1h` | How often the due-validation scheduler runs. Set to `0s` to disable. |
| `VALIDATION_LIMIT` | `25` | Maximum snapshots selected by one scheduler run. Must be greater than zero. |

Validation behavior and its limits are described in [Validation and restore testing](validation.md).
