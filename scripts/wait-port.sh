#!/usr/bin/env bash
set -euo pipefail
HOST="${1:?host}"
PORT="${2:?port}"
TIMEOUT="${3:-30}"
python3 - "$HOST" "$PORT" "$TIMEOUT" <<'PY'
import socket, sys, time
host, port, timeout = sys.argv[1], int(sys.argv[2]), float(sys.argv[3])
deadline = time.time() + timeout
last = None
while time.time() < deadline:
    s = socket.socket()
    s.settimeout(0.5)
    try:
        s.connect((host, port))
        s.close()
        sys.exit(0)
    except OSError as e:
        last = e
        time.sleep(0.25)
    finally:
        try: s.close()
        except Exception: pass
print(f"timed out waiting for {host}:{port}: {last}", file=sys.stderr)
sys.exit(1)
PY
