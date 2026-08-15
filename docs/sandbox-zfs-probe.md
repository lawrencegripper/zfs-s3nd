# Railway Sandbox ZFS Probe

Purpose: prove whether Railway Sandboxes can run the restore-validation path with real `zfs recv`.

The app will eventually create sandboxes via `railwayapp/railway-ts-sdk` using a project token and environment id. For now, the fastest manual probe is SSH:

```bash
ssh sandbox@railway.new
```

Then run the probe below inside the sandbox.

## Probe script

```bash
set -euxo pipefail

whoami
id
uname -a
cat /etc/os-release || true
df -h /
lsblk || true

# Use sudo if available; some sandbox sessions may already be root.
if command -v sudo >/dev/null 2>&1; then
  SUDO=sudo
else
  SUDO=""
fi

# Debian's zfsutils-linux is commonly in contrib/non-free components.
# This is safe in an ephemeral sandbox.
if [ -f /etc/apt/sources.list.d/debian.sources ]; then
  $SUDO sed -i 's/Components: main$/Components: main contrib non-free non-free-firmware/' /etc/apt/sources.list.d/debian.sources || true
fi
if [ -f /etc/apt/sources.list ]; then
  $SUDO sed -i 's/ main$/ main contrib non-free non-free-firmware/' /etc/apt/sources.list || true
fi

$SUDO apt-get update

# Try to install ZFS tooling and whatever headers are available for this kernel.
$SUDO apt-get install -y ca-certificates curl kmod lsb-release
$SUDO apt-get install -y linux-headers-$(uname -r) || true
$SUDO apt-get install -y zfsutils-linux zfs-dkms || true

command -v zfs
command -v zpool
zfs version || true
zpool version || true

# Try loading the kernel module.
$SUDO modprobe zfs || true
lsmod | grep -i '^zfs' || true

# Real file-backed pool send/receive test.
# Keep small so it fits easily inside the sandbox disk budget.
$SUDO rm -f /tmp/zfs-vdev /tmp/s1.zfs
truncate -s 512M /tmp/zfs-vdev

$SUDO zpool create -f zfstest /tmp/zfs-vdev
$SUDO zfs create zfstest/src
$SUDO sh -c 'echo hello-from-railway-sandbox > /zfstest/src/hello.txt'
$SUDO zfs snapshot zfstest/src@s1
$SUDO zfs send zfstest/src@s1 > /tmp/s1.zfs
$SUDO zfs recv zfstest/restored < /tmp/s1.zfs
cat /zfstest/restored/hello.txt
$SUDO zfs list -t all

$SUDO zpool destroy zfstest
rm -f /tmp/zfs-vdev /tmp/s1.zfs

echo SANDBOX_ZFS_PROBE_PASS
```

## Result interpretation

Pass:

- Script prints `SANDBOX_ZFS_PROBE_PASS`.
- We can use Railway Sandboxes for production restore validation.

Partial pass:

- `zstreamdump` works but `zpool create`/`zfs recv` fails.
- Use sandboxes for stream validation and keep restore validation behind another executor.

Fail:

- ZFS packages/modules cannot be installed or loaded.
- Keep `railway-sandbox` executor but initially mark it stream-only, or use an external privileged VM executor.

## SDK shape for later

From `railwayapp/railway-ts-sdk` examples:

```ts
import { Sandbox } from "railway";

// Reads RAILWAY_API_TOKEN and RAILWAY_ENVIRONMENT_ID, or pass them explicitly.
await using sandbox = await Sandbox.create({
  idleTimeoutMinutes: 10,
  env: {
    S3_ENDPOINT: "...",
    S3_BUCKET: "...",
  },
});

const result = await sandbox.exec("bash /tmp/validate.sh", {
  timeoutSec: 20 * 60,
  onStdout: chunk => process.stdout.write(chunk),
  onStderr: chunk => process.stderr.write(chunk),
});

if (result.exitCode !== 0) {
  throw new Error(`validation failed: ${result.exitCode}`);
}
```

We should use templates/checkpoints once the install path is known, so validation jobs do not pay the package-install cost every time.
