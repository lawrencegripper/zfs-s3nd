#!/usr/bin/env bash
set -euo pipefail

export GOMODCACHE="${GOMODCACHE:-$PWD/.gomodcache}"
export GOCACHE="${GOCACHE:-$PWD/.gocache}"
export GOSUMDB="${GOSUMDB:-off}"
export DOCKER_CONFIG="${DOCKER_CONFIG:-$PWD/.docker-config}"

make generate
docker compose -f docker-compose.test.yml up -d rustfs
docker compose -f docker-compose.test.yml run --rm create-bucket
docker compose -f docker-compose.test.yml run --rm test-runner
