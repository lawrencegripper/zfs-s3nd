# Contributing

## Development setup

Required tools:

- Go version from `go.mod`
- Docker and Docker Compose
- Bun for Railway IaC and Playwright tests
- ZFS and sudo only for the host roundtrip test

Run the normal checks with:

```bash
make test
make ui-test
make integration
make docker-build
```

`make test` installs the pinned sqlc version, regenerates `internal/catalog/db`, and runs the Go suite. Commit generated sqlc changes with the query that produced them.

The browser suite starts a test-only server with a temporary SQLite catalog and in-memory object store. Install Chromium separately with `make ui-test-install` if needed.

For changes to ingest, manifests, encryption, validation, deletion, or restore behavior, add a regression test that fails before the implementation change. Run a real roundtrip when the change can affect ZFS stream compatibility:

```bash
make zfs-roundtrip
# or
make zfs-roundtrip-docker
```

## Pull requests

Keep changes focused. Include:

- The problem being fixed
- Any catalog, manifest, object-layout, or configuration changes
- Tests run
- Deployment or recovery implications

Do not commit credentials, private SSH keys, database files, Playwright artifacts, or local object-store data.

## Generated code

Edit `queries/catalog.sql`, not `internal/catalog/db/catalog.sql.go` directly. Regenerate with:

```bash
make generate
```

Database schema changes belong in `migrations/`. This project currently treats the schema as pre-stable; document any migration that requires a fresh catalog.

## Security reports

Do not open a public issue for a vulnerability. Follow [SECURITY.md](SECURITY.md).
