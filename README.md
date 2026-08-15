# ZFS S3nd

ZFS S3nd receives `zfs send` streams over SSH and stores them in S3-compatible object storage. A SQLite catalog tracks datasets, snapshots, incremental chains, validation results, and restore commands.

It is a single-administrator service intended for a small self-hosted backup target. Railway deployment is included, but the storage interface works with other S3-compatible services.

## Project status

The project does not yet publish versioned releases or a stable catalog and object-layout compatibility policy. Review changes before upgrading an existing deployment, keep the encryption key outside the service, and run regular restore drills.

## What it does

- Receives full and incremental ZFS send streams over SSH
- Splits streams into encrypted chunks in S3-compatible object storage
- Tracks snapshot lineage in a SQLite catalog
- Checks chunk hashes, stream structure, and incremental GUID continuity
- Generates restore commands for complete snapshot chains
- Backs up the SQLite catalog to object storage
- Provides a server-rendered administration UI and API tokens

Validation checks stored bytes and ZFS stream structure. It does not replace a real `zfs recv` restore drill. See [Validation and restore testing](docs/validation.md).

## How it works

1. An authorized SSH key sends a ZFS stream to the service.
2. The service parses the stream header and identifies the source, pool, dataset, snapshot, and incremental base.
3. Stream chunks are encrypted and written to object storage.
4. A manifest and catalog transaction commit the snapshot.
5. Scheduled validation reads the stored chain and checks its hashes, structure, and lineage.

The SSH username identifies the source during upload. Snapshot IDs are globally unique; restore connections use the fixed SSH username `restore`.

## Requirements

Server:

- S3-compatible object storage
- Persistent storage for the SQLite catalog and SSH host key
- `zstreamdump` for validation
- An HTTP endpoint and an SSH TCP endpoint

Source and restore systems:

- OpenZFS command-line tools
- OpenSSH

## Deploy on Railway

The repository contains Railway IaC for the application, bucket, volume, HTTP health check, and SSH TCP proxy.

```bash
railway link
make railway-plan
make railway-apply
```

Before applying a fork, change the repository name in [`.railway/railway.ts`](.railway/railway.ts). Set `STORAGE_ENCRYPTION_KEY` and keep a copy outside Railway. Losing this value makes stored stream chunks unrecoverable.

Deployment details are in [Railway deployment](docs/railway-deploy.md). All environment variables are documented in [Configuration](docs/configuration.md).

## First backup

Open the HTTP endpoint, set the administrator password, and add an SSH public key. Then run on the source system:

```bash
zfs snapshot tank/photos@zs3-first
zfs send tank/photos@zs3-first \
  | ssh -p <ssh-port> nas-home@<ssh-host>
```

The SSH username, `nas-home` in this example, is recorded as the source name.

For an incremental send:

```bash
zfs snapshot tank/photos@zs3-next
zfs send -i tank/photos@zs3-first tank/photos@zs3-next \
  | ssh -p <ssh-port> nas-home@<ssh-host>
```

The service rejects an incremental stream when its base snapshot is missing or when the configured chain-depth limit would be exceeded.

## Restore

The dataset and snapshot pages show commands for the required restore chain. A single stream can be restored with:

```bash
ssh -p <ssh-port> restore@<ssh-host> \
  restore-stream <snapshot-id> \
  | zfs recv -F restore/photos
```

Incremental snapshots require the parent streams first. Use the ordered command list shown in the UI, or run the local helper where the binary has access to the catalog and bucket:

```bash
zfs-s3nd restore-chain-to <snapshot-id> restore/photos
```

Run restore drills against a disposable pool before relying on the service for recovery.

## Local development

The application requires S3-compatible storage even when run locally. The test compose file provides RustFS:

```bash
docker compose -f docker-compose.test.yml up -d rustfs create-bucket
mkdir -p data
DATABASE_PATH=./data/catalog.db \
HTTP_PORT=3000 \
S3_ENDPOINT=http://localhost:9000 \
S3_REGION=us-east-1 \
S3_BUCKET=zfs-s3nd-backups \
S3_ACCESS_KEY_ID=rustfsadmin \
S3_SECRET_ACCESS_KEY=rustfsadmin \
S3_FORCE_PATH_STYLE=true \
STORAGE_ENCRYPTION_KEY='local-development-passphrase' \
make run
```

Open <http://localhost:3000>. The SSH service listens on port `2222` by default.

Common checks:

```bash
make test                 # Go tests
make ui-test              # Playwright browser tests
make integration          # S3 integration tests against RustFS
make docker-build         # production-style image build
make zfs-roundtrip        # real send/receive test; requires ZFS and sudo
make zfs-roundtrip-docker # privileged zfs-fuse roundtrip
```

See [CONTRIBUTING.md](CONTRIBUTING.md) for the development workflow.

## Documentation

- [Configuration](docs/configuration.md)
- [Security and encryption](docs/security.md)
- [Validation and restore testing](docs/validation.md)
- [Railway deployment](docs/railway-deploy.md)
- [Implementation notes](docs/implementation.md)
- [Project plan and design notes](docs/plan.md)

## License

[MIT](LICENSE)
