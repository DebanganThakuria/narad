SHELL := /bin/bash

GO        ?= go
GOFUMPT   ?= gofumpt
GOIMPORTS ?= goimports

BIN_DIR  := bin
DATA_DIR := data
PKG      := github.com/debanganthakuria/narad
IMAGE    ?= ghcr.io/debanganthakuria/narad:dev
DOCKER_PLATFORM ?= linux/amd64
HOST_ARCH := $(shell uname -m)
ifeq ($(HOST_ARCH),arm64)
BUILD_PLATFORM ?= linux/arm64
else ifeq ($(HOST_ARCH),aarch64)
BUILD_PLATFORM ?= linux/arm64
else
BUILD_PLATFORM ?= linux/amd64
endif

PKGS       := ./...
TEST_FLAGS := -race -count=1

# Stamp the binary with the current commit so `narad version` is honest.
# Falls through to the runtime debug.BuildInfo path inside `narad version`
# when there's no commit yet.
GIT_REV ?= $(shell git rev-parse --short HEAD 2>/dev/null)
ifeq ($(strip $(GIT_REV)),)
LDFLAGS :=
else
LDFLAGS := -X 'main.version=dev+$(GIT_REV)'
endif

.DEFAULT_GOAL := help

.PHONY: help
help: ## Show this help.
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) \
		| awk 'BEGIN {FS = ":.*?## "} {printf "  \033[36m%-16s\033[0m %s\n", $$1, $$2}'

# ---- build / run ---------------------------------------------------------

.PHONY: build
build: ## Build the narad binary into bin/narad.
	@mkdir -p $(BIN_DIR)
	$(GO) build -ldflags "$(LDFLAGS)" -o $(BIN_DIR)/narad ./cmd/narad

.PHONY: docker-build
docker-build: ## Build a container image. Override with IMAGE=ghcr.io/owner/narad:tag.
	docker build --platform "$(DOCKER_PLATFORM)" --build-arg BUILDPLATFORM="$(BUILD_PLATFORM)" --build-arg GIT_REV="$(GIT_REV)" -t "$(IMAGE)" .

.PHONY: docker-push
docker-push: ## Push the container image built by docker-build.
	docker push "$(IMAGE)"

.PHONY: install
install: ## Install narad into $GOBIN.
	$(GO) install -ldflags "$(LDFLAGS)" ./cmd/narad

.PHONY: run-serve
run-serve: build ## Build and run `narad serve` with default flags.
	$(BIN_DIR)/narad serve

.PHONY: run-cli
run-cli: build ## Build and run `narad cli` with default flags.
	$(BIN_DIR)/narad cli

# ---- test / lint / format ------------------------------------------------

.PHONY: test
test: ## Run the full test suite with the race detector.
	$(GO) test $(TEST_FLAGS) $(PKGS)

.PHONY: vet
vet: ## Run go vet.
	$(GO) vet $(PKGS)

.PHONY: fmt
fmt: ## Auto-format the tree with gofumpt + goimports.
	$(GOFUMPT) -w .
	$(GOIMPORTS) -w .

.PHONY: fmt-check
fmt-check: ## Strict format check (no rewrites; suitable for CI).
	@out="$$($(GOFUMPT) -l .)"; \
		if [ -n "$$out" ]; then \
			echo "gofumpt: unformatted files:"; \
			echo "$$out" | sed 's/^/  /'; \
			echo "run: make fmt"; \
			exit 1; \
		fi
	@out="$$($(GOIMPORTS) -l .)"; \
		if [ -n "$$out" ]; then \
			echo "goimports: unsorted imports:"; \
			echo "$$out" | sed 's/^/  /'; \
			echo "run: make fmt"; \
			exit 1; \
		fi

.PHONY: tidy
tidy: ## go mod tidy.
	$(GO) mod tidy

.PHONY: check
check: fmt-check vet test ## Strict check: fmt-check + vet + test (no auto-fix).

.PHONY: local-cluster-e2e
local-cluster-e2e: ## Run a local 3-node cluster integration/load test. Pass ARGS='--topics 10 --messages 1000' to override.
	./scripts/local-cluster-e2e.sh $(ARGS)

.PHONY: local-cluster-chaos
local-cluster-chaos: ## Run local 3-node cluster test with rolling process restarts.
	./scripts/local-cluster-chaos.sh $(ARGS)

.PHONY: cluster-load
cluster-load: ## Run the integration/load driver against existing nodes. Use NARAD_NODES='http://host1,http://host2' ARGS='--topics 10 --messages 1000000'.
	@if [[ -z "$(NARAD_NODES)" ]]; then \
		echo "NARAD_NODES is required, for example: make cluster-load NARAD_NODES='http://narad:8080'"; \
		exit 2; \
	fi
	$(GO) run ./tests/integration --nodes "$(NARAD_NODES)" $(ARGS)

.PHONY: local-soak-cluster
local-soak-cluster: ## Start a persistent local 3-node cluster for soak/perf testing.
	./scripts/local-soak-cluster.sh

.PHONY: local-soak-stop
local-soak-stop: ## Stop the persistent local soak cluster, keeping data/logs.
	./scripts/local-soak-stop.sh

.PHONY: local-soak-launchd-start
local-soak-launchd-start: ## Start local Narad + tester under macOS launchd for long soak runs.
	./scripts/local-soak-launchd-start.sh

.PHONY: local-soak-launchd-stop
local-soak-launchd-stop: ## Stop the macOS launchd local Narad + tester soak services.
	./scripts/local-soak-launchd-stop.sh

.PHONY: local-soak-tester
local-soak-tester: ## Run the long-running Narad tester. Defaults to 50 msg/sec, 20ms consume wait, +/-10 msg/sec every 10m, capped at 100k.
	./scripts/local-soak-tester.sh $(ARGS)

.PHONY: local-monitoring-start
local-monitoring-start: ## Start local Prometheus and import the Grafana soak dashboard.
	./scripts/local-monitoring-start.sh

.PHONY: local-monitoring-stop
local-monitoring-stop: ## Stop local Prometheus. Pass ARGS='--grafana' to stop Homebrew Grafana too.
	./scripts/local-monitoring-stop.sh $(ARGS)

# ---- developer setup -----------------------------------------------------

.PHONY: tools-install
tools-install: ## Install gofumpt and goimports to $(go env GOPATH)/bin.
	$(GO) install mvdan.cc/gofumpt@latest
	$(GO) install golang.org/x/tools/cmd/goimports@latest

# ---- housekeeping --------------------------------------------------------

.PHONY: clean
clean: ## Remove build artefacts and runtime data.
	rm -rf $(BIN_DIR) $(DATA_DIR)
