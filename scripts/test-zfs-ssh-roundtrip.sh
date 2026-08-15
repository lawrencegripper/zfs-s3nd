#!/usr/bin/env bash
set -euo pipefail

# Real end-to-end test:
#   temporary source zpool -> full + incremental zfs send -> SSH ingest -> RustFS
#   chunks/manifests -> restore-stream -> zfs recv into temporary restore zpool
#   -> compare data at each snapshot.
#
# Requires Linux with usable ZFS tooling/kernel module and passwordless sudo for
# zpool/zfs operations. This is intended for a capable dev machine, CI runner, or
# Railway Sandbox if ZFS support is confirmed.

require() {
  command -v "$1" >/dev/null 2>&1 || { echo "missing required command: $1" >&2; exit 2; }
}

require go
if [[ "${SKIP_COMPOSE:-}" != "1" ]]; then
  require docker
fi
require ssh
require ssh-keygen
require sqlite3
require zfs
require zpool
require truncate

SUDO=""
if [[ "$(id -u)" != "0" ]]; then
  SUDO="sudo -n"
  $SUDO true || { echo "passwordless sudo is required for zfs/zpool commands" >&2; exit 2; }
fi

ROOT="$(pwd)"
TMP="$(mktemp -d)"
SRC_POOL="zs3src$$"
DST_POOL="zs3dst$$"
APP_PID=""

cleanup() {
  set +e
  if [[ -n "${APP_PID}" ]]; then kill "${APP_PID}" >/dev/null 2>&1 || true; fi
  $SUDO zpool destroy "$SRC_POOL" >/dev/null 2>&1 || true
  $SUDO zpool destroy "$DST_POOL" >/dev/null 2>&1 || true
  if [[ "${SKIP_COMPOSE:-}" != "1" ]]; then
    DOCKER_CONFIG="$ROOT/.docker-config" docker compose -f docker-compose.test.yml down -v >/dev/null 2>&1 || true
  fi
  rm -rf "$TMP"
}
trap cleanup EXIT

start_zfs_fuse_if_needed() {
  if zpool list >/dev/null 2>&1; then
    return 0
  fi
  if ! command -v zfs-fuse >/dev/null 2>&1; then
    return 0
  fi
  mkdir -p /run/lock /var/lock/zfs 2>/dev/null || true
  zfs-fuse --no-daemon --no-kstat-mount --pidfile /tmp/zfs-fuse.pid >/tmp/zfs-fuse.out 2>/tmp/zfs-fuse.err &
  for _ in $(seq 1 40); do
    if zpool list >/dev/null 2>&1; then
      return 0
    fi
    sleep 0.25
  done
  echo "zfs-fuse failed to start" >&2
  cat /tmp/zfs-fuse.out /tmp/zfs-fuse.err >&2 || true
  return 1
}

start_zfs_fuse_if_needed

free_port() {
  python3 - <<'PY'
import socket
s = socket.socket()
s.bind(('127.0.0.1', 0))
print(s.getsockname()[1])
s.close()
PY
}

HTTP_PORT="$(free_port)"
SSH_PORT="$(free_port)"
DB_PATH="$TMP/catalog.db"
APP_BIN="$TMP/zfs-s3nd"
CLIENT_KEY="$TMP/client_ed25519"
SRC_IMG="$TMP/src.img"
DST_IMG="$TMP/dst.img"

make generate
go build -buildvcs=false -o "$APP_BIN" ./cmd/zfs-s3nd

if [[ "${SKIP_COMPOSE:-}" != "1" ]]; then
  DOCKER_CONFIG="$ROOT/.docker-config" docker compose -f docker-compose.test.yml up -d rustfs
  ./scripts/wait-port.sh 127.0.0.1 9000 30
  DOCKER_CONFIG="$ROOT/.docker-config" docker compose -f docker-compose.test.yml run --rm create-bucket
  ./scripts/wait-port.sh 127.0.0.1 9000 30
fi

ssh-keygen -q -t ed25519 -N '' -f "$CLIENT_KEY"

S3_ENV=(
  "S3_ENDPOINT=${S3_ENDPOINT:-http://localhost:9000}"
  "S3_REGION=us-east-1"
  "S3_BUCKET=zfs-s3nd-backups"
  "S3_ACCESS_KEY_ID=rustfsadmin"
  "S3_SECRET_ACCESS_KEY=rustfsadmin"
  "S3_FORCE_PATH_STYLE=true"
)

env \
  DATABASE_PATH="$DB_PATH" \
  STORAGE_ENCRYPTION_KEY="${STORAGE_ENCRYPTION_KEY:-a2tra2tra2tra2tra2tra2tra2tra2tra2tra2tra2s=}" \
  HTTP_PORT="$HTTP_PORT" \
  SSH_PORT="$SSH_PORT" \
  SSH_HOST_KEY_PATH="$TMP/ssh_host_ed25519" \
  "${S3_ENV[@]}" \
  "$APP_BIN" serve >"$TMP/app.out" 2>"$TMP/app.err" &
APP_PID=$!

for _ in $(seq 1 120); do
  if curl -fsS "http://127.0.0.1:$HTTP_PORT/healthz" >/dev/null 2>&1; then break; fi
  sleep 0.25
done
curl -fsS "http://127.0.0.1:$HTTP_PORT/healthz" >/dev/null

FINGERPRINT="$(ssh-keygen -lf "$CLIENT_KEY.pub" -E sha256 | awk '{print $2}')"
PUBLIC_KEY="$(cat "$CLIENT_KEY.pub")"
sqlite3 "$DB_PATH" <<SQL
INSERT INTO ssh_keys (id, name, public_key, fingerprint_sha256)
VALUES ('key_1', 'roundtrip', '$PUBLIC_KEY', '$FINGERPRINT');
SQL

truncate -s 512M "$SRC_IMG"
truncate -s 512M "$DST_IMG"
$SUDO zpool create -f "$SRC_POOL" "$SRC_IMG"
$SUDO zpool create -f "$DST_POOL" "$DST_IMG"
$SUDO zfs create "$SRC_POOL/data"
SRC_MOUNT="$($SUDO zfs get -H -o value mountpoint "$SRC_POOL/data")"
echo "hello from zfs s3end" | $SUDO tee "$SRC_MOUNT/hello.txt" >/dev/null
$SUDO zfs snapshot "$SRC_POOL/data@s1"
echo "hello from incremental snapshot" | $SUDO tee "$SRC_MOUNT/incremental.txt" >/dev/null
echo "hello from zfs s3end v2" | $SUDO tee "$SRC_MOUNT/hello.txt" >/dev/null
$SUDO zfs snapshot "$SRC_POOL/data@s2"

ssh_upload() {
  ssh \
    -o StrictHostKeyChecking=no \
    -o UserKnownHostsFile=/dev/null \
    -i "$CLIENT_KEY" \
    -p "$SSH_PORT" \
    truenas@127.0.0.1
}

parse_snapshot_id() {
  local output="$1"
  local id
  id="$(printf '%s\n' "$output" | sed -n 's/.*snapshot=\([^ ]*\).*/\1/p')"
  if [[ -z "$id" ]]; then
    echo "could not parse snapshot id from ssh output" >&2
    printf '%s\n' "$output" >&2
    cat "$TMP/app.err" >&2
    exit 1
  fi
  printf '%s\n' "$id"
}

SSH_OUTPUT_FULL="$($SUDO zfs send "$SRC_POOL/data@s1" | ssh_upload)"
echo "$SSH_OUTPUT_FULL"
SNAPSHOT_ID_FULL="$(parse_snapshot_id "$SSH_OUTPUT_FULL")"

SSH_OUTPUT_INC="$($SUDO zfs send -i "$SRC_POOL/data@s1" "$SRC_POOL/data@s2" | ssh_upload)"
echo "$SSH_OUTPUT_INC"
SNAPSHOT_ID_INC="$(parse_snapshot_id "$SSH_OUTPUT_INC")"

env \
  DATABASE_PATH="$DB_PATH" \
  STORAGE_ENCRYPTION_KEY="${STORAGE_ENCRYPTION_KEY:-a2tra2tra2tra2tra2tra2tra2tra2tra2tra2tra2s=}" \
  "${S3_ENV[@]}" \
  "$APP_BIN" restore-chain-to "$SNAPSHOT_ID_INC" "$DST_POOL/data"
DST_MOUNT="$($SUDO zfs get -H -o value mountpoint "$DST_POOL/data")"
$SUDO zfs list -t snapshot "$DST_POOL/data@s1" >/dev/null
RESTORED_S2="$($SUDO cat "$DST_MOUNT/hello.txt")"
RESTORED_INC="$($SUDO cat "$DST_MOUNT/incremental.txt")"
if [[ "$RESTORED_S2" != "hello from zfs s3end v2" ]]; then
  echo "restored s2 content mismatch: $RESTORED_S2" >&2
  exit 1
fi
if [[ "$RESTORED_INC" != "hello from incremental snapshot" ]]; then
  echo "restored incremental file mismatch: $RESTORED_INC" >&2
  exit 1
fi

$SUDO zfs list -t snapshot "$DST_POOL/data@s2" >/dev/null

echo "ZFS SSH incremental roundtrip PASS: full_snapshot=$SNAPSHOT_ID_FULL incremental_snapshot=$SNAPSHOT_ID_INC"
