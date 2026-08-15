# Security and encryption

## Storage encryption

ZFS stream chunks are encrypted before they are written to object storage. Each object uses XChaCha20-Poly1305 with a random nonce. The encryption key is derived from `STORAGE_ENCRYPTION_KEY` with Argon2id.

Snapshot manifests remain plaintext. They contain dataset and snapshot identity, incremental lineage, object keys, sizes, checksums, stream flags, and encryption-envelope parameters. They do not contain the encryption passphrase or decrypted stream data.

The live SQLite catalog is stored unencrypted on its persistent volume. Catalog backup objects are passed through the encrypted storage adapter.

## Recovery material

Recovery requires all of the following:

- The object-storage data
- `STORAGE_ENCRYPTION_KEY`
- A usable catalog backup, or enough manifests to reconstruct the required references

Store the encryption passphrase outside the deployment platform. Losing or changing it makes existing chunk objects unreadable. The service does not rotate or re-encrypt old objects when the value changes.

The SSH host key should also be on persistent storage. Replacing it does not affect backup contents, but clients will see a host-key change.

## Administrative access

The service has one administrator. Browser sessions use a signed cookie and unsafe form requests require a CSRF token. API tokens grant administrator access and should be created separately for each integration so they can be revoked independently.

SSH access is controlled by public-key fingerprint. The username identifies a source for uploads. Restore sessions use the fixed username `restore`; authorization still depends on possession of an accepted private key.

Use the platform firewall and private networking where practical. The SSH TCP endpoint and administration UI should not be treated as anonymous public services.

## Operational guidance

- Use a unique, randomly generated storage-encryption passphrase.
- Back up the passphrase separately from the bucket and Railway project.
- Restrict bucket credentials to the bucket used by this service.
- Keep the SQLite volume and catalog backups private.
- Revoke unused API tokens and SSH keys.
- Review failed authentication, upload, and validation operations.
- Perform restore drills into a disposable ZFS pool.

## Reporting vulnerabilities

See [SECURITY.md](../SECURITY.md).
