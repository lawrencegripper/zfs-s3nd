# ZFS S3nd Plan

## Goal

Build a Railway-hosted backup target that lets users run `zfs send` from remote machines over SSH and stores the resulting ZFS send streams in Railway Buckets, with a SQLite catalog on a Railway Volume.

The service should provide:

- SSH-based authenticated ingest for `zfs send` streams
- Support for full and incremental snapshots
- Railway Bucket object storage for backup payloads
- SQLite catalog stored on a Railway Volume
- Minimal web UI showing datasets, snapshots, validation state, and usage
- Periodic validation that backups are structurally valid and, where possible, restorable
- Robust end-to-end test suite for fast local and CI iteration
- Railway Infrastructure as Code for reproducible deployment

## High-level architecture

```text
Remote ZFS host
  |
  | zfs send [-w] ... | ssh [named_source]@host
  v
Railway service
  |-- HTTPS web UI/API
  |-- SSH server on Railway TCP proxy
  |-- background scheduler/worker
  |-- SQLite catalog on Railway Volume
  |
  v
Railway Bucket / S3-compatible object storage
```

## Railway resources

Use Railway Infrastructure as Code in `.railway/railway.ts`.

Resources:

1. Main Go service
   - HTTP endpoint for UI/API
   - TCP proxy for embedded SSH server
   - Mounted Railway Volume for SQLite
   - Environment variables for bucket credentials and app config

2. Railway Bucket
   - S3-compatible object storage for ZFS send streams
   - Region should be chosen carefully because bucket region is immutable

3. Railway Volume
   - Mounted into the service, e.g. `/data`
   - Stores SQLite database and small local working files

4. Optional validation worker service
   - Same image, different start command
   - Can be scaled separately later

5. SQLite backup cron service
   - Same image, `backup-sqlite` command
   - Runs on a Railway cron schedule, e.g. hourly or daily
   - Dumps the SQLite catalog from the Railway Volume and writes it to the Railway Bucket

## Runtime stack

Use Go for the application data plane and control plane.

Reasons:

- Excellent long-lived TCP/SSH support
- Excellent streaming IO and backpressure behavior
- Mature S3 clients
- Mature SQLite options
- Easy single-binary Docker deployment on Railway
- Simpler/faster implementation than Rust while still being a good systems language for this workload

Likely libraries:

- HTTP/UI: standard `net/http`, server-rendered `html/template`, possibly HTMX for small dynamic updates
- SQLite: `github.com/mattn/go-sqlite3`
- S3 client: AWS SDK for Go v2 against Railway Buckets/RustFS
- SSH server: `golang.org/x/crypto/ssh`
- Tests: Go test + Docker Compose integration tests

Use TypeScript only for Railway Infrastructure as Code in `.railway/railway.ts` and, if useful, a small Railway Sandbox helper that calls `railwayapp/railway-ts-sdk`.

## Ingress model

### Web

Expose normal HTTP via Railway public networking.

The web UI should support:

- Dashboard
- Dataset list
- Dataset detail
- Snapshot detail
- Upload/session status
- Usage
- Validation jobs
- SSH key management
- Restore instructions

### SSH

Expose an embedded SSH server via Railway TCP proxy.

Users upload by piping a ZFS send stream to SSH with no remote command:

```bash
zfs send -w tank/data@snap1 | ssh backup.example.com -p $PORT
```

Incremental example:

```bash
zfs send -w -i tank/data@snap1 tank/data@snap2 \
  | ssh backup.example.com -p $PORT
```

The Railway TCP proxy gives a generated hostname and port. A custom DNS name can CNAME to the Railway proxy hostname, but the Railway-provided port still has to be used.

## SSH authentication and authorization

SSH handles:

- Public-key authentication
- Transport encryption
- Client identity

The service should:

- Store user SSH public keys in SQLite
- Match login key fingerprint to a user/account
- Disable shell access
- Disable port forwarding
- Disable arbitrary command execution
- Treat shell/no-command sessions as binary ZFS stream uploads
- Accept only a small explicit command grammar for metadata/restore helpers

Allowed explicit command shapes:

```text
state <pool> <dataset>
restore-stream <snapshot-id>
```

For commandless uploads, `source` comes from the SSH username, for example `[named_source]@host`. `pool`, `dataset`, `snapshot`, raw/compressed flags, and incremental GUID lineage are parsed from the ZFS stream header.

The tuple `(source, pool, dataset)` is the logical backup dataset identity. This allows different machines and different pools to push snapshots with the same dataset name without colliding.

Source, pool, dataset, and snapshot names must be validated and normalized before use in object keys.

## Storage model

The main service does not need to run `zfs recv` during ingest. It stores the raw ZFS send stream as ordered immutable chunk objects plus a final manifest.

A snapshot is committed only when:

1. all chunk rows are complete in SQLite,
2. all chunk objects exist in the bucket with expected size/checksum,
3. the final manifest object exists in the bucket with expected size/checksum,
4. the manifest contents match the SQLite chunk rows, and
5. the SQLite snapshot row is marked committed in a durable transaction.

Object keys should be understandable in a disaster-recovery scenario without the SQLite catalog. Use a stable human-readable layout with escaping, not opaque-only ids.

Naming rules:

- Prefix everything with `zfs-s3nd/v1/` so future formats can coexist.
- Preserve source, pool, dataset, and snapshot names in the path.
- Split dataset paths on `/` so the bucket browser resembles ZFS hierarchy.
- Percent-encode any path component outside a conservative safe set, e.g. `[A-Za-z0-9._:-]`.
- Use reserved marker directories beginning with `@`, e.g. `@snapshots`, `@validation`, `@catalog-backups`. ZFS dataset names cannot use `@` as a normal dataset path separator, so these are easy to distinguish.
- The `manifest.json` object is the commit marker. Chunks without a committed manifest are ignored by DR tooling.

Object key layout:

```text
zfs-s3nd/v1/sources/{source}/pools/{pool}/datasets/{dataset/path}/@dataset.json
zfs-s3nd/v1/sources/{source}/pools/{pool}/datasets/{dataset/path}/@snapshots/{snapshot}/manifest.json
zfs-s3nd/v1/sources/{source}/pools/{pool}/datasets/{dataset/path}/@snapshots/{snapshot}/chunks/000000000000.zfschunk
zfs-s3nd/v1/sources/{source}/pools/{pool}/datasets/{dataset/path}/@snapshots/{snapshot}/chunks/000000000001.zfschunk
zfs-s3nd/v1/sources/{source}/pools/{pool}/datasets/{dataset/path}/@snapshots/{snapshot}/@validation/{job_id}.json
zfs-s3nd/v1/@catalog-backups/{yyyy}/{mm}/{dd}/{timestamp}-{catalog_backup_id}.sqlite
zfs-s3nd/v1/@catalog-backups/{yyyy}/{mm}/{dd}/{timestamp}-{catalog_backup_id}.json
```

Example:

```text
zfs-s3nd/v1/sources/nas-home/pools/tank/datasets/photos/@snapshots/auto-2026-07-01/manifest.json
zfs-s3nd/v1/sources/nas-home/pools/tank/datasets/photos/@snapshots/auto-2026-07-01/chunks/000000000000.zfschunk
zfs-s3nd/v1/sources/nas-home/pools/tank/datasets/home/lg/@snapshots/hourly-001/manifest.json
```

SQLite should still store internal ids and normalized rows for fast queries, but the bucket path and manifest should be self-describing enough to restore or rebuild the catalog without SQLite.

Default chunk size should be configurable, probably 64MiB or 128MiB. Each chunk can itself use an S3 multipart upload internally if the S3 client/storage benefits from that, but the application-level backup format is many chunk objects plus a manifest, not one giant object.

### Persistence confirmation

The service should explicitly confirm writes at each layer before surfacing success to the SSH client/UI:

1. **Chunk object persisted**
   - upload chunk object
   - `HEAD` the object
   - verify object size and checksum/metadata where supported
   - record verified chunk row in SQLite

2. **Manifest object persisted**
   - write manifest after all chunks verify
   - `HEAD` the manifest object
   - verify size/checksum
   - compare manifest chunk list against SQLite chunk rows

3. **Catalog persisted**
   - commit SQLite transaction marking the snapshot committed
   - use SQLite WAL and an appropriate synchronous setting for Railway Volume durability
   - never acknowledge SSH success before the transaction commit returns

4. **Startup reconciliation**
   - on boot, scan non-terminal upload sessions
   - confirm bucket objects still match SQLite rows
   - finish safe commits or quarantine partial/orphaned data

The success response to the SSH client means: all chunks are in the bucket, the manifest is in the bucket, and the SQLite catalog commit completed.

Objects should be private by default.

Prefer encrypted ZFS sends where possible:

```bash
zfs send -w pool/encrypted@snap
```

This means the service can store backups without needing access to plaintext data.

## SQLite catalog

SQLite lives on the Railway Volume, e.g. `/data/catalog.db`.

Initial schema areas:

### Users and keys

- `admin_credentials`
- `ssh_keys`
- `sessions`

### Sources, pools, datasets, and snapshots

- `sources`
- `pools`
- `datasets`
- `snapshots`
- `snapshot_chunks`
- `snapshot_edges` or `parent_snapshot_id`

Source fields:

- id
- globally unique name
- description
- created at
- last seen at

Pool fields:

- id
- source id
- name, unique per source
- created at
- last seen at

Dataset fields:

- id
- source id
- pool id
- dataset path within pool
- display name
- created at
- last seen at
- unique `(source_id, pool_id, path)`

Snapshot fields:

- id
- dataset id
- snapshot name
- parent snapshot id, nullable
- manifest object key
- upload status
- validation status
- logical bytes if known
- stored bytes
- stream sha256/blake3 checksum
- chunk count
- created at
- upload started/completed at
- failure reason

Snapshot chunk fields:

- id
- snapshot id
- upload session id
- chunk index
- object key
- size bytes
- zfs stream offset start/end
- sha256/blake3 checksum
- status: `writing`, `uploaded`, `verified`, `failed`
- created at
- completed at

### Upload sessions

Track in-progress uploads separately from committed snapshots.

Fields:

- id
- dataset id
- target snapshot name
- optional base snapshot id
- status: `pending`, `uploading`, `writing_chunk`, `committing_manifest`, `committing_catalog`, `complete`, `failed`, `aborted`
- chunk size bytes
- current chunk index
- chunks completed
- bytes received
- stream checksum state/final checksum
- manifest staging object key
- manifest final object key
- started at
- last heartbeat at
- completed at
- failure reason

### Operations timeline

Keep operations and catalog-backup records so the dashboard can show current and historic activity.

- `operations`
- `catalog_backups`

Operation fields:

- id
- type: `upload`, `validation`, `reconciliation`, `cleanup`, `catalog_backup`
- status: `queued`, `running`, `succeeded`, `failed`
- source id, nullable
- pool id, nullable
- dataset id, nullable
- snapshot id, nullable
- upload session id, nullable
- validation job id, nullable
- started at
- updated at
- completed at
- summary
- failure reason

The dashboard shows both in-flight and recent historic operations.

Catalog backup fields:

- id
- operation id
- object key
- size bytes
- sqlite page count/page size if captured
- sha256/blake3 checksum
- started at
- completed at
- status: `running`, `succeeded`, `failed`
- failure reason

### Usage

Compute current stored bytes, snapshot counts, failed uploads, and validation activity directly from the catalog. Add rollup tables only if measured catalog size or query latency requires them.

### SQLite catalog backups

The SQLite catalog is operationally critical, so it should be backed up to the Railway Bucket periodically.

Use a dedicated Railway cron service running the same Go binary with a short-lived command:

```bash
zfs-s3nd backup-sqlite
```

Recommended schedule:

```text
17 * * * *      # hourly, UTC
```

Railway cron services are expected to run one task and exit. If a previous run is still active, Railway skips the next scheduled run, so the backup command must close DB/S3 handles and terminate.

Backup flow:

1. Open SQLite read-only or with a short read transaction.
2. Produce a consistent dump using SQLite backup API or `VACUUM INTO` to a temporary file on the Railway Volume.
3. Calculate checksum and size.
4. Upload to Railway Bucket under a catalog backup prefix.
5. `HEAD` the uploaded object and verify size/checksum metadata where supported.
6. Insert `catalog_backups` row and append `catalog_backup` operation events.
7. Delete local temporary dump.

Object layout:

```text
@catalog-backups/{yyyy}/{mm}/{dd}/{timestamp}-{catalog_backup_id}.sqlite
@catalog-backups/{yyyy}/{mm}/{dd}/{timestamp}-{catalog_backup_id}.json
```

Keep enough backups to recover from accidental catalog corruption. Initial retention can be simple, e.g. keep hourly backups for 48 hours and daily backups for 30 days, then make it configurable later.

### Validation

- `validation_jobs`
- `validation_results`

Fields:

- job id
- snapshot id or chain id
- type: `stream_check`, `restore_check`
- executor: `local`, `docker`, `railway_sandbox`, `external_vm`
- status
- logs/object key for logs
- started/completed at
- result summary

## Upload lifecycle

### Successful upload

1. SSH connection authenticates with public key.
2. Command is parsed and authorized.
3. Server validates dataset/snapshot/base.
4. Create `upload_session` row in SQLite.
5. Create a snapshot row in an uploading/pending state.
6. Stream SSH stdin into fixed-size chunk objects while:
   - counting bytes
   - updating stream checksum
   - computing per-chunk checksums
   - periodically updating heartbeat/progress
   - creating `snapshot_chunks` rows as chunks complete
7. Verify each uploaded chunk's object metadata/size/checksum.
8. Write a manifest object listing chunks, sizes, checksums, lineage, source/pool/dataset/snapshot names, and stream options.
9. In a SQLite transaction:
   - mark snapshot committed
   - mark upload complete
   - update usage counters
   - append operation events
10. Queue validation job.

### Failure mid-upload

Expected failures:

- Client disconnects
- Railway deploy/restart
- Network failure to bucket
- Chunk upload failure
- Manifest write failure
- SQLite transaction failure
- Process crash

Handling:

- Upload chunk objects under the snapshot id, but never mark the snapshot committed until the final manifest and catalog commit succeed
- Keep upload session status in SQLite
- Heartbeat active uploads
- On process startup, reconcile sessions:
  - `uploading`/`writing_chunk` with stale heartbeat -> mark `failed` or `aborted`
  - verify completed chunk objects still exist and match expected size/checksum
  - delete incomplete/uncommitted chunk objects after retention window
  - delete stale manifest staging objects after retention window
- Only expose snapshots as restorable after manifest + catalog commit
- Final chunk and manifest object keys should be immutable
- If commit fails after manifest upload, reconciler should either:
  - complete the catalog commit if enough metadata exists, or
  - move the manifest/chunks to orphan cleanup flow

For v1, interrupted SSH uploads should require retry from the client. Native ZFS receive resume tokens are not available because the service is storing streams rather than running `zfs recv` live.

## Incremental snapshots

Users specify the base snapshot explicitly:

```bash
zfs send -i pool/data@base pool/data@next \
  | ssh backup.example.com -p $PORT
```

The service should:

- Require `--base` for incremental uploads
- Verify the base exists in the catalog
- Store parent-child relationship
- Prevent duplicate snapshot names per dataset
- Show the chain in the UI
- Validate chains periodically, not only individual objects

The service cannot fully prove that an incremental stream matches the declared base without deeper stream inspection or restore testing. Restore validation is therefore important.

## Restore model

The UI should show exact restore commands.

Full restore concatenates the snapshot chunks in manifest order:

```bash
for key in $(jq -r '.chunks[].object_key' manifest.json); do
  aws s3 cp "s3://bucket/$key" -
done | zfs recv tank/restored
```

Incremental chain restore applies each snapshot manifest in lineage order:

```bash
restore_manifest full-manifest.json | zfs recv tank/restored
restore_manifest inc1-manifest.json | zfs recv tank/restored
restore_manifest inc2-manifest.json | zfs recv tank/restored
```

The UI can generate explicit commands for each chunk, plus a small shell helper function. Direct bucket downloads should be preferred over proxying restore data through the app.

## Validation strategy

There are two validation levels.

### Level 1: stream structure validation

Use `zstreamdump` where available to verify the object is a valid ZFS send stream.

This can run in Docker/local validation environments and may be possible in Railway if the binary can be installed without needing kernel ZFS support.

### Level 2: actual restore validation

Use Railway Sandboxes as the primary restore-validation executor.

Railway Sandboxes are short-lived, isolated Linux VMs that can be created on demand, controlled via the TypeScript SDK/CLI, executed against, and destroyed after the job. They start from a clean Debian base, support long-running commands, files API access, outbound networking, optional private networking, templates, forks, and checkpoints. They provide around 22GB of disk space, which is enough for MVP/sample restore checks and many small-to-medium chain validations.

Validation flow:

1. Create or reuse a prepared sandbox/checkpoint with ZFS tooling installed.
2. Download the relevant full + incremental manifests from Railway Bucket.
3. Stream each manifest's chunks from Railway Bucket in order.
4. Create a temporary file/vdev-backed ZFS pool inside the sandbox.
5. Run `zfs recv` for the full stream and then each incremental stream.
6. Run basic dataset checks, e.g. list snapshots, compare expected snapshot names, optionally checksum known fixture files.
7. Upload logs/results to the bucket or store summary in SQLite.
8. Destroy the sandbox aggressively to control cost.

Fallback options:

1. Local Docker/VM for development and CI where available
2. Stream-only `zstreamdump` validation when ZFS restore is not available
3. External VM runner only if Railway sandbox limitations block a needed restore scenario

Design validation executors behind an interface:

```text
ValidationExecutor
  - validateStream(snapshot)
  - validateRestore(chain)
```

Implement initially:

- `railway-sandbox` for production restore validation
- `local-docker` or local VM for development/CI
- `stream-only` fallback

Add later only if needed:

- `external-vm`

Validation scheduling:

- Validate every completed upload structurally
- Periodically sample full restore chains
- Prioritize newest snapshots
- Revalidate older backups on a schedule
- Surface stale validation in UI

## End-to-end test strategy

The project should have a robust test suite that allows quick loops locally and in CI.

### Test layers

1. Unit tests
   - command parsing
   - SSH key fingerprinting
   - object key generation
   - catalog transitions
   - usage calculation

2. Integration tests
   - SQLite migrations
   - S3/Railway Bucket adapter against RustFS
   - SSH server auth and command restrictions
   - upload session lifecycle
   - failure/reconciliation behavior

3. End-to-end tests
   - create temporary ZFS dataset/snapshots where available
   - run `zfs send | ssh ...`
   - verify object stored
   - verify catalog rows
   - restore into clean dataset when privileged ZFS is available

4. Failure injection tests
   - kill client mid-upload
   - kill server mid-upload
   - simulate S3 failure
   - simulate SQLite write failure
   - simulate chunk upload failure
   - simulate manifest write failure
   - verify cleanup/reconciliation
   - verify retry succeeds

### Local test environment

Use Docker Compose for fast local tests:

- app service
- RustFS as S3-compatible bucket
- test runner
- optional privileged ZFS test container or VM-backed runner

Because ZFS requires kernel support, tests should detect capabilities:

- If ZFS is available and privileged, run full restore tests
- Otherwise run stream/mock tests and mark restore tests skipped

### CI approach

Default CI:

- unit tests
- integration tests with RustFS
- SSH ingest tests using generated test streams/fixtures
- failure injection tests that do not require kernel ZFS

Privileged/nightly CI:

- full ZFS send/receive tests on a compatible runner or VM
- restore validation tests
- large upload tests
- incremental chain tests

### Test fixtures

Keep small ZFS send stream fixtures if licensing/portability is acceptable, or generate them in privileged test environments.

Fixture types:

- full stream
- incremental stream
- encrypted raw stream
- truncated stream
- corrupted stream

## Minimal UI plan

Use a server-rendered UI with minimal JavaScript.

Pages:

### Dashboard

- Total stored bytes
- Source count
- Pool count
- Dataset count
- Snapshot count
- Latest successful backup
- Failed/recent uploads
- In-flight uploads and validations
- Operation timeline with recent successes/failures/retries
- Catalog backup health and latest backup time
- Validation health
- Estimated bucket usage trend

### Sources / pools / datasets

- Source name, taken from the SSH username
- Pool name
- Dataset path
- Latest snapshot
- Snapshot count
- Stored bytes
- Last upload
- Last validation
- Health status

### Dataset detail

- Source, pool, and dataset path
- Snapshot timeline
- Incremental chain graph/list
- Object sizes
- Upload status
- Operation timeline filtered to this dataset
- Restore commands

### Operations / uploads

- Active uploads
- Active validations
- Queued jobs
- Failed uploads
- Failed validations
- Retry guidance
- Stale upload cleanup status
- Historical operation log

### Usage

- Total usage over time
- Per-dataset usage
- Ingest bytes per day
- Validation cost/activity

### Settings

- SSH public keys
- Bucket status
- Retention policy
- Catalog backup schedule/status
- Validation settings

## Security considerations

MVP auth model:

- Single admin user
- Web UI protected by an admin password/session
- SSH public keys stored in SQLite and managed by the admin UI

General security:

- SSH public-key auth only
- No password login
- No shell
- No arbitrary SSH exec
- No port forwarding
- Source identities remain isolated in catalog queries and object keys
- Private bucket objects
- Prefer raw encrypted ZFS sends (`zfs send -w`) for encrypted datasets
- Rate-limit failed SSH auth attempts
- Audit log key additions/deletions and upload events
- Validate dataset/snapshot names strictly
- Use least-privilege bucket credentials where Railway supports it

## Railway IaC notes

Use `.railway/railway.ts` and `railway config plan/apply`.

Relevant Railway IaC concepts:

- `defineRailway(...)`
- `service(name, config)`
- `volume(name, config)` mounted with `volumeMounts`
- `bucket(name, config)`
- environment variables
- service domains
- cron schedule for the SQLite backup service
- TCP proxy configured for the SSH listen port

Need to verify the exact current DSL for TCP proxy in IaC. If TCP proxy is not yet supported directly by IaC, document a one-time manual step or use Railway CLI/API after `config apply`.

## MVP phases

### Phase 1: local core — done

Done:

- Go app skeleton
- SQLite schema/migrations and sqlc-generated data access
- S3 adapter against RustFS/Railway Buckets
- Chunked snapshot object format and manifest writer
- Explicit manifest `version`/`format` gate for future breaking changes
- SSH server with SQLite-backed public-key auth
- `receive` command for full and incremental uploads
- `state` command returning committed remote snapshot chain as JSON for clients
- `restore-stream` command
- Unit/integration tests
- Docker + `zfs-fuse` full/incremental real ZFS send/restore roundtrip

### Phase 2: Railway deploy — mostly done

Done:

- Dockerfile for Go app
- `.railway/railway.ts` using Bun tooling
- Railway Volume for SQLite
- Railway Bucket integration
- HTTP domain
- TCP proxy for SSH
- Healthcheck
- Web form login plus API token Bearer auth
- In-process catalog backup scheduler, because a separate cron cannot share the mounted volume
- In-process scheduler for periodic due validation

Remaining:

- Apply/deploy latest IaC/app changes after review.
- Confirm post-deploy smoke, SSH ingest, scheduled validation, and dashboard state on Railway.

### Phase 3: robustness — mostly done

Done:

- Chunk upload lifecycle
- Manifest/catalog commit lifecycle
- Upload heartbeats/progress
- Startup stale-upload reconciler
- Periodic in-process stale-upload reconciler while the service is running
- Five-minute SSH upload stall timeout
- Cleanup of abandoned failed/aborted chunk/manifest objects
- Catalog backup command and scheduled backups
- Failure injection tests for chunk, manifest, and catalog commit failures
- Retry of failed/aborted snapshot names
- Dashboard active upload throughput
- Admin delete actions for snapshots/datasets that remove catalog rows and S3 objects via durable cleanup operations processed by an in-process worker

Remaining:

- Catalog backup retention/pruning.
- Snapshot/object retention policies.
- Quotas/alerts.

### Phase 4: incremental snapshots — done

Done:

- `--base` support
- Snapshot lineage in SQLite and manifests
- Chain UI
- Restore command generation
- Full + incremental E2E tests
- Zvol upload helper that discovers remote state over SSH and selects incremental bases without local state or direct S3 access
- Commandless SSH uploads: `zfs send ... | ssh [named_source]@target` uses the SSH username as the source, parses the ZFS stream BEGIN record server-side, and infers pool/dataset/snapshot, GUID lineage, raw/compressed flags, and incremental base.
- Chain restore: `restore-chain-to <snapshot-id> <zfs-target>` restores all dependent snapshots from the full base through the requested snapshot by invoking `zfs recv` per stream; `restore-stream <snapshot-id>` streams one stored snapshot with parent/next hints on stderr.
- Incremental chain depth cap via `MAX_INCREMENTAL_CHAIN_DEPTH` to force periodic full anchor uploads instead of unbounded chains.

### Phase 5: validation — partially done

Done:

- Chain-aware object/catalog/manifest validation core
- `zstreamdump` stream validation path
- GUID parsing and incremental `fromguid`/previous `toguid` compatibility check where available
- Asynchronous post-upload single-stream validation for newly committed SSH uploads, with scheduled full-chain validation retained as the backstop
- Validation job persistence
- Snapshot validation status in dashboard
- In-process periodic validation wiring

Remaining:

- Move actual validation execution out to Railway Sandbox/external runners rather than the app-local runner behind the trigger.
- Add full `zfs recv` restore validation in a sandbox/runner with enough ZFS support.
- Validation filtering/detail pages if the dashboard becomes noisy.

### Phase 6: encryption and DR metadata — partially done

Done:

- Mandatory app-level chunk encryption using XChaCha20-Poly1305.
- `STORAGE_ENCRYPTION_KEY` passphrase with Argon2id-derived key.
- Plaintext snapshot manifests for disaster recovery metadata.
- Manifest encryption envelope/KDF metadata for future compatibility.
- Unencrypted chunk objects rejected on read.

Remaining:

- Per-snapshot random salt/key derivation if we decide to move beyond the current fixed v1 KDF metadata.
- Key/passphrase rotation story.

### Phase 7: polish

Remaining:

- Retention policies
- Quotas
- Alerts
- More complete CLI/client helper UX
- Tailscale/private ingress option

## Current status

Working locally:

- `make test`
- `make integration`
- `make docker-build`
- `make zfs-roundtrip-docker`

The Docker roundtrip uses `zfs-fuse` and validates the full path:

```text
zfs send -> commandless SSH upload -> chunk objects + manifest in RustFS -> restore-chain-to -> zfs recv
```

Railway Sandbox ZFS notes:

- Sandbox creation and SSH access work.
- `zfsutils-linux` can be installed after enabling Debian `contrib`.
- `zstreamdump` is available from the package.
- Kernel ZFS restore is blocked by missing `6.13.9-railway` headers/modules, so `modprobe zfs` fails.
- `zfs-fuse` can be installed and started after ensuring `/run/lock` and `/var/lock/zfs` exist, but the repeatable Docker `zfs-fuse` harness is currently simpler and sufficient for local/CI roundtrip validation.

## Commandless send/restore UX tasks

Implemented from the UX simplification discussion:

1. Treat an SSH session with no exec command as an upload operation.
2. Reject PTY requests so binary streams are not corrupted; `-T` remains optional hygiene, not required for piped sends.
3. Parse the ZFS DRR_BEGIN header before ingesting chunks.
4. Store stream GUID metadata in manifests and SQLite.
5. Resolve incremental bases by matching incoming `fromguid` to a committed snapshot `toguid`.
6. Associate commandless uploads with a source name on the SSH key, with key name as a fallback.
7. Add `restore-stream <snapshot-id>` over SSH and CLI for a single stored stream, plus `restore-chain-to <snapshot-id> <zfs-target>` for local full-chain restores.
8. Update the ZFS roundtrip E2E rig to use commandless full/incremental sends and a chain-aware restore.
9. Validate that naive concatenation of independent send streams is not accepted by the current `zfs recv` rig; keep chain restore as sequential receives instead of emitting a non-portable concatenated stream.

Still optional/future:

- Teach the batch zvol uploader to become mostly a loop/scheduler around the one-line commandless upload path.
- Add idempotent duplicate `toguid` handling that can return success for already-uploaded streams without making retry UX confusing.
- Service-side compaction worker could restore a chain into a temporary ZFS pool and re-ingest a synthetic full anchor, but v1 relies on clients sending periodic full anchors.

## Deployment status

Railway project is linked and a production deployment has been created with:

- app service
- Railway Bucket
- Railway Volume
- public HTTP domain
- TCP proxy for SSH
- generated/preserved web admin password
- generated/preserved storage encryption passphrase

The latest code changes after the initial deployment still need to be applied/redeployed and smoke-tested.

## Open questions

1. Should validation execution move to Railway Sandbox first, or should we use an external ZFS-capable runner for true restore tests?
2. Do we want per-snapshot random salts/key derivation, or keep the current fixed v1 KDF metadata for simplicity?
3. What retention policy should be enforced for snapshots, abandoned objects, and catalog backups?
4. What alerting should exist for failed uploads, failed validation, stale backups, and storage usage?
5. Should the zvol uploader become a packaged installable client, or remain a repo script?
