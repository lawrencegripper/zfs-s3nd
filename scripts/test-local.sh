#!/usr/bin/env bash
set -euo pipefail

export GOMODCACHE="${GOMODCACHE:-$PWD/.gomodcache}"
export GOCACHE="${GOCACHE:-$PWD/.gocache}"
export GOSUMDB="${GOSUMDB:-off}"

make generate
go test ./...
