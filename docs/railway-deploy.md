# Railway deployment

This repo includes Railway IaC at `.railway/railway.ts`.

It defines:

- main Go service: `zfs-s3nd`
- Railway Volume mounted at `/data` for `catalog.db`
- Railway Bucket `zfs-s3nd-backups` for chunks, manifests, and catalog backups
- `/healthz` healthcheck
- TCP proxy declaration for SSH ingest on app port `2222`
- In-process hourly scheduler that runs due snapshot validation

## Plan/apply

```bash
railway link
railway config plan
railway config apply
```

## Public HTTP

`WEB_ADMIN_PASSWORD` is managed as a preserved Railway variable. Generate/set it once, then retrieve it when needed:

```bash
openssl rand -base64 24 | railway variable set WEB_ADMIN_PASSWORD --stdin --service zfs-s3nd
railway variables --service zfs-s3nd --json | jq -r .WEB_ADMIN_PASSWORD
```

When set, `WEB_ADMIN_PASSWORD` is the admin password for browser form login. When unset, the first visit opens `/setup` and stores a bcrypt password hash in SQLite. API tokens can be issued from the admin UI and used as `Authorization: Bearer <token>` for `/api/v1/*`. `/healthz` remains unauthenticated.

Deploy draining is configured with `RAILWAY_DEPLOYMENT_DRAINING_SECONDS=21600` and `SHUTDOWN_GRACE_PERIOD=5h55m`. On SIGTERM the app stops accepting new SSH sessions, logs active SQLite upload sessions, and waits for active uploads to finish before exiting, within that grace window.

## Storage encryption

Application-level storage encryption is always enabled. `STORAGE_ENCRYPTION_KEY` is a passphrase; the app derives a 32-byte XChaCha20-Poly1305 key with Argon2id at startup.

Set a strong passphrase:

```bash
openssl rand -base64 32 | railway variable set STORAGE_ENCRYPTION_KEY --stdin --service zfs-s3nd
```

The app encrypts ZFS stream chunk objects before writing them to the bucket and rejects unencrypted chunk objects on read. Snapshot manifests are intentionally stored as plaintext metadata for disaster recovery; they include encryption envelope/KDF metadata needed to decrypt chunks with the passphrase.

Keep `STORAGE_ENCRYPTION_KEY` somewhere safe outside Railway. Changing or losing it makes chunk data unrecoverable.

After apply, create or confirm a public HTTP domain for the `zfs-s3nd` service in Railway networking settings if one does not already exist. Then smoke test:

```bash
BASE_URL=https://your-service.up.railway.app ./scripts/railway-smoke.sh
```

## SSH TCP proxy

The app listens on `SSH_PORT=2222`; IaC declares a Railway TCP proxy for app port `2222`. Railway provides a hostname and public port through `RAILWAY_TCP_PROXY_DOMAIN` and `RAILWAY_TCP_PROXY_PORT`. When `RESTORE_SSH_COMMAND_PREFIX` is unset, the app uses those variables automatically for commands shown in the UI. Users then run:

```bash
zfs send pool/data@snap | ssh -p <railway-tcp-port> [named_source]@<railway-tcp-host>
zfs send -i @snap pool/data@next | ssh -p <railway-tcp-port> [named_source]@<railway-tcp-host>
```

The server parses the ZFS stream header to infer pool/dataset/snapshot and incremental GUID lineage. Railway sets `UPLOAD_THROUGHPUT_LIMIT_MBIT=45`, enforced independently for each upload by applying backpressure to the SSH channel. A client may request a lower limit with `-o SetEnv=ZFS_S3END_UPLOAD_THROUGHPUT_LIMIT_MBIT=<Mbps>` but cannot raise the server default. Piped OpenSSH sessions do not normally allocate a TTY; if local SSH config forces one, add `-T`.

Stream one stored snapshot with:

```bash
ssh -p <railway-tcp-port> restore@<railway-tcp-host> restore-stream <snapshot-id> | zfs recv -F restore/data
```

If that snapshot is incremental, `restore-stream` writes stderr before the stream with the parent snapshot that must already exist on the receive target. After the stream, if a committed child exists, it writes the next restore command and a `(plus n more)` count. ZFS receive also validates lineage and should reject an incremental stream applied out of order.

For a full dependency-chain restore, use the dashboard-generated command block or run the local helper where the app binary has bucket/catalog access:

```bash
zfs-s3nd restore-chain-to <snapshot-id> restore/data
```

## Incremental chain depth

`MAX_INCREMENTAL_CHAIN_DEPTH` defaults to `30` in Railway IaC. When an incremental upload would exceed this depth, the service rejects it before storing chunks and the client should send the same target snapshot as a full send to create a new anchor.

The `state` SSH command includes `chain_depth` so upload clients can switch to a full send before reaching the limit.

## Periodic validation

The main app runs an in-process validation scheduler every hour. It checks committed snapshots due for revalidation, records `validation_jobs`, updates the separate `stream_validation_status` and `chain_validation_status` fields, and shows recent validation status/errors on the dashboard.

Manual commands:

```bash
zfs-s3nd validate-chain <snapshot-id> # validate one restore chain
zfs-s3nd validate-due                 # validate committed snapshots due for recheck
```

## Catalog backups

The app service runs an in-process catalog backup scheduler every 24 hours by default so backups can use the same mounted `/data` volume as the main service. `CATALOG_BACKUP_INTERVAL` can override this if needed.

The binary also has a working one-shot command for manual use:

```bash
zfs-s3nd backup-sqlite
```

Both paths use SQLite `VACUUM INTO`, upload `.sqlite` and `.json` backup objects, and record `catalog_backups`/operation rows.

## Bucket env vars

The app uses one canonical set of S3-compatible storage env vars. Railway IaC maps bucket resource outputs into these names:

- `S3_BUCKET`
- `S3_ENDPOINT`
- `S3_REGION`
- `S3_ACCESS_KEY_ID`
- `S3_SECRET_ACCESS_KEY`
- `S3_FORCE_PATH_STYLE`
