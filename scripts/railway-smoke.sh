#!/usr/bin/env bash
set -euo pipefail

: "${BASE_URL:?set BASE_URL to the Railway service URL}"

curl -fsS "$BASE_URL/healthz"
echo

if [[ -n "${SSH_HOST:-}" && -n "${SSH_PORT:-}" ]]; then
  timeout 10 bash -c "</dev/tcp/$SSH_HOST/$SSH_PORT" \
    && echo "ssh tcp proxy reachable: $SSH_HOST:$SSH_PORT"
fi
