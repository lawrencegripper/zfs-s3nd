.PHONY: test ui-test ui-test-ci ui-test-install ui-test-install-ci run docker-build docker-up docker-down integration zfs-roundtrip zfs-roundtrip-docker generate install-tools bun-install railway-plan railway-apply

export GOMODCACHE := $(CURDIR)/.gomodcache
export GOCACHE := $(CURDIR)/.gocache
export GOSUMDB := off
export DOCKER_CONFIG := $(CURDIR)/.docker-config
export BUN_INSTALL_CACHE_DIR := $(CURDIR)/.bun-cache
BUN ?= $(shell command -v bun 2>/dev/null || if [ -x "$(HOME)/.bun/bin/bun" ]; then echo "$(HOME)/.bun/bin/bun"; fi)

install-tools:
	GOBIN=$(CURDIR)/.bin go install github.com/sqlc-dev/sqlc/cmd/sqlc@v1.30.0

generate: install-tools
	$(CURDIR)/.bin/sqlc generate

test: generate
	go test ./...

ui-test-install: bun-install
	$(BUN) run ui:test:install

ui-test-install-ci: bun-install
	$(BUN) run ui:test:install-ci

ui-test: ui-test-install
	$(BUN) run ui:test

ui-test-ci: ui-test-install-ci
	$(BUN) run ui:test

run:
	go run ./cmd/zfs-s3nd serve

bun-install:
	@test -n "$(BUN)" || { echo "bun is required for Railway IaC tooling" >&2; exit 2; }
	$(BUN) install

railway-plan: bun-install
	$(BUN) run railway:plan

railway-apply: bun-install
	$(BUN) run railway:apply

docker-build:
	docker build -t zfs-s3nd:test .

docker-up:
	docker compose -f docker-compose.test.yml up --build

docker-down:
	docker compose -f docker-compose.test.yml down -v

integration: generate
	docker compose -f docker-compose.test.yml up -d rustfs
	docker compose -f docker-compose.test.yml run --rm create-bucket
	docker compose -f docker-compose.test.yml run --rm test-runner

zfs-roundtrip:
	./scripts/test-zfs-ssh-roundtrip.sh

zfs-roundtrip-docker: generate
	docker compose -f docker-compose.test.yml up -d rustfs
	docker compose -f docker-compose.test.yml run --rm create-bucket
	docker compose -f docker-compose.test.yml run --rm zfs-roundtrip
