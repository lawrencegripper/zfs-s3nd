#!/usr/bin/env bash
# Smoke test the HTTP API with an API token.
#
# Usage:
#   BASE_URL=https://... API_TOKEN=zs3_... ./scripts/api-smoke.sh
#
# Optional:
#   VALIDATION_LIMIT=1        Number of due snapshots to validate.
#   RUN_VALIDATION=0          Skip the validation trigger.
#   CURL_MAX_TIME_SECONDS=30  Per-request timeout.

set -euo pipefail

: "${BASE_URL:?set BASE_URL}"
: "${API_TOKEN:?set API_TOKEN}"

BASE_URL="${BASE_URL%/}"
VALIDATION_LIMIT="${VALIDATION_LIMIT:-1}"
RUN_VALIDATION="${RUN_VALIDATION:-1}"
CURL_MAX_TIME_SECONDS="${CURL_MAX_TIME_SECONDS:-30}"

TMPDIR="$(mktemp -d)"
trap 'rm -rf "$TMPDIR"' EXIT

LAST_BODY=""
LAST_STATUS=""

request() {
  local method="$1"
  local path="$2"
  local auth="${3:-auth}"
  local body="${4:-}"
  local out status args
  out="$TMPDIR/response-$(date +%s%N).body"
  args=(-sS --max-time "$CURL_MAX_TIME_SECONDS" -X "$method")
  if [[ "$auth" == "auth" ]]; then
    args+=(-H "Authorization: Bearer ${API_TOKEN}")
  fi
  if [[ -n "$body" ]]; then
    args+=(-H "Content-Type: application/json" --data "$body")
  fi
  status="$(curl "${args[@]}" -o "$out" -w '%{http_code}' "$BASE_URL$path" || true)"
  LAST_BODY="$out"
  LAST_STATUS="$status"
}

expect_status() {
  local want="$1"
  local label="$2"
  if [[ "$LAST_STATUS" != "$want" ]]; then
    echo "FAIL $label: HTTP $LAST_STATUS, expected $want" >&2
    sed -n '1,80p' "$LAST_BODY" >&2 || true
    exit 1
  fi
  echo "ok $label"
}

expect_json() {
  local label="$1"
  python3 -m json.tool "$LAST_BODY" >/dev/null || {
    echo "FAIL $label: response is not JSON" >&2
    sed -n '1,80p' "$LAST_BODY" >&2 || true
    exit 1
  }
}

expect_contains() {
  local needle="$1"
  local label="$2"
  if ! grep -q "$needle" "$LAST_BODY"; then
    echo "FAIL $label: missing $needle" >&2
    sed -n '1,80p' "$LAST_BODY" >&2 || true
    exit 1
  fi
}

json_get() {
  python3 - "$LAST_BODY" "$1" <<'PY'
import json, sys
with open(sys.argv[1]) as f:
    data = json.load(f)
expr = sys.argv[2]
try:
    value = eval(expr, {"__builtins__": {}}, {"data": data})
except Exception:
    value = ""
if value is None:
    value = ""
print(value)
PY
}

request GET /healthz none
expect_status 200 healthz
expect_json healthz

request GET /readyz none
expect_status 200 readyz
expect_json readyz

request GET /api/v1/dashboard none
expect_status 401 "dashboard rejects missing token"

request GET /metrics none
expect_status 401 "metrics rejects missing token"

request GET /metrics auth
expect_status 200 metrics
expect_contains '^zfs_s3end_committed_snapshots ' metrics
expect_contains '^zfs_s3end_active_uploads ' metrics

request GET /api/v1/dashboard auth
expect_status 200 dashboard
expect_json dashboard
SNAPSHOT_COUNT="$(json_get 'data.get("SnapshotCount", "")')"
echo "dashboard snapshots=$SNAPSHOT_COUNT"

request GET /api/v1/ssh-keys auth
expect_status 200 ssh-keys
expect_json ssh-keys

request GET /api/v1/api-tokens auth
expect_status 200 api-tokens
expect_json api-tokens

request GET /api/v1/datasets auth
expect_status 200 datasets
expect_json datasets
DATASET_ID="$(json_get '(data[0].get("dataset_id") if isinstance(data, list) and data else "")')"
if [[ -n "$DATASET_ID" ]]; then
  request GET "/api/v1/datasets/$DATASET_ID" auth
  expect_status 200 dataset-detail
  expect_json dataset-detail
  SNAPSHOT_ID="$(json_get '(data.get("Snapshots") or [{}])[0].get("id", "") if isinstance(data, dict) else ""')"
  if [[ -n "$SNAPSHOT_ID" ]]; then
    request GET "/api/v1/snapshots/$SNAPSHOT_ID" auth
    expect_status 200 snapshot-detail
    expect_json snapshot-detail
  else
    echo "skip snapshot-detail: dataset has no snapshots"
  fi
else
  echo "skip dataset-detail: no datasets"
fi

if [[ "$RUN_VALIDATION" == "1" ]]; then
  request POST "/api/v1/admin/validation/run?limit=$VALIDATION_LIMIT" auth
  expect_status 200 validation-run
  expect_json validation-run
  CHECKED="$(json_get 'data.get("checked", "")')"
  SUCCEEDED="$(json_get 'data.get("succeeded", "")')"
  FAILED="$(json_get 'data.get("failed", "")')"
  echo "validation checked=$CHECKED succeeded=$SUCCEEDED failed=$FAILED"
else
  echo "skip validation-run"
fi

echo "api smoke passed"
