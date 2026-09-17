# Validation and restore testing

The service records two validation states for a snapshot.

## Stream validation

Stream validation checks the selected snapshot without rereading its ancestors. It verifies:

- The snapshot is committed and has a manifest
- Manifest identity matches the catalog
- Manifest chunks match the catalog records
- Every chunk exists with the expected size and SHA-256 hash
- The concatenated stream has the expected size and SHA-256 hash
- `zstreamdump` accepts the stream and finds a `DRR_BEGIN` record
- Parsed stream GUIDs agree with catalog metadata when both are available
- An incremental stream's `fromguid` matches its recorded parent

## Chain validation

Chain validation starts at the full snapshot and checks each stream through the selected snapshot. In addition to the byte and structure checks above, it verifies:

- Every ancestor is committed
- Catalog parent references form an unbroken chain
- Manifest base-snapshot names match the catalog chain
- Each incremental `fromguid` matches the previous stream's `toguid`

The scheduler periodically reruns chain validation for committed snapshots and records each attempt in `validation_jobs`.

## What validation does not prove

`zstreamdump` checks stream structure and checksums, but validation does not run `zfs recv` into a real pool. A structurally valid stream may still fail on a restore host because of unsupported ZFS features, raw-encryption requirements, platform differences, receive-target state, or other receiver constraints.

A green chain-validation result therefore means that the stored bytes and recorded lineage are internally consistent. It is not proof that a particular restore host can receive the chain.

## Restore drills

Test recovery periodically against a disposable pool:

1. Choose a recent chain-valid snapshot in the UI.
2. Run the generated restore commands in order.
3. Confirm that `zfs recv` succeeds for the full stream and every incremental.
4. Mount or inspect the restored dataset.
5. Verify representative files or application-level checksums.
6. Record the date, target OpenZFS version, and result.

The repository includes two roundtrip harnesses:

```bash
make zfs-roundtrip        # host ZFS and sudo
make zfs-roundtrip-docker # privileged zfs-fuse container
```

Both create temporary file-backed pools, send full and incremental snapshots through the service, restore them, and compare file contents.

## Manual validation commands

```bash
zfs-s3nd validate-chain <snapshot-id>
zfs-s3nd validate-due
```

Validation reads every chunk in the selected chain and can incur object-storage transfer and CPU cost.
