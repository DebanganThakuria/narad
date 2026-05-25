SHELL := /bin/bash

GO        ?= go
GOFUMPT   ?= gofumpt
GOIMPORTS ?= goimports

BIN_DIR  := bin
DATA_DIR := data
PKG      := github.com/debanganthakuria/narad

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

# ---- developer setup -----------------------------------------------------

.PHONY: tools-install
tools-install: ## Install gofumpt and goimports to $(go env GOPATH)/bin.
	$(GO) install mvdan.cc/gofumpt@latest
	$(GO) install golang.org/x/tools/cmd/goimports@latest

# ---- housekeeping --------------------------------------------------------

.PHONY: clean
clean: ## Remove build artefacts and runtime data.
	rm -rf $(BIN_DIR) $(DATA_DIR)
