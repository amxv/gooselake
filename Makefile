.DEFAULT_GOAL := help

VERSION ?= latest
CONFIG ?= $(HOME)/.config/gg-runtime/runtime-server.toml
GOOSETOWER_CONFIG ?= $(HOME)/.config/gg-goosetower/goosetower.toml
SERVICE ?= gg-runtime.service
SCOPE ?= user
BASE_URL ?=
TOKEN ?=
DEV_RUNTIME_PORT ?= 18080
DEV_GOOSETOWER_PORT ?= 18090
DEV_GOOSEWEB_PORT ?= 13001

RUNTIME_BIN ?= $(HOME)/.local/share/gg-runtime/current/bin/gg-runtime-server
GOOSETOWER_BIN ?= $(HOME)/.local/share/gg-runtime/current/bin/gg-goosetower

.PHONY: help
help: ## Show available targets
	@echo "GG Runtime task runner"
	@echo ""
	@echo "Variables:"
	@echo "  VERSION=<tag>        (default: latest)"
	@echo "  CONFIG=<path>        (default: $(HOME)/.config/gg-runtime/runtime-server.toml)"
	@echo "  GOOSETOWER_CONFIG=<path> (default: $(HOME)/.config/gg-goosetower/goosetower.toml)"
	@echo "  SERVICE=<name>       (default: gg-runtime.service)"
	@echo "  SCOPE=<user|system>  (default: user)"
	@echo "  BASE_URL=<url>       (optional)"
	@echo "  TOKEN=<bearer>       (optional)"
	@echo "  DEV_RUNTIME_PORT=<port> (default: 18080)"
	@echo "  DEV_GOOSETOWER_PORT=<port> (default: 18090)"
	@echo "  DEV_GOOSEWEB_PORT=<port> (default: 13001)"
	@echo "  RUNTIME_BIN=<path>   (default: $(HOME)/.local/share/gg-runtime/current/bin/gg-runtime-server)"
	@echo "  GOOSETOWER_BIN=<path> (default: $(HOME)/.local/share/gg-runtime/current/bin/gg-goosetower)"
	@echo ""
	@awk 'BEGIN {FS = ":.*## "}; /^[a-zA-Z0-9_.-]+:.*## / {printf "  %-22s %s\n", $$1, $$2}' $(MAKEFILE_LIST)

.PHONY: install
install: ## Install release bundle into ~/.local (VERSION=latest by default)
	./scripts/install-runtime.sh $(VERSION)

.PHONY: install-source
install-source: ## Build and install from source into ~/.local
	./scripts/install-from-source.sh

.PHONY: upgrade
upgrade: ## Staged upgrade + symlink activation (VERSION=latest by default)
	./scripts/upgrade-runtime.sh $(VERSION)

.PHONY: preflight
preflight: ## Run deployment preflight checks (filesystem/config only)
	./scripts/preflight-runtime.sh --config "$(CONFIG)" --runtime-bin "$(RUNTIME_BIN)" --skip-http

.PHONY: preflight-http
preflight-http: ## Run deployment preflight checks including HTTP (requires BASE_URL and TOKEN)
	@if [ -z "$(BASE_URL)" ] || [ -z "$(TOKEN)" ]; then \
	  echo "BASE_URL and TOKEN are required for preflight-http"; \
	  exit 1; \
	fi
	./scripts/preflight-runtime.sh --config "$(CONFIG)" --runtime-bin "$(RUNTIME_BIN)" --base-url "$(BASE_URL)" --token "$(TOKEN)"

.PHONY: check-config
check-config: ## Validate runtime config via gg-runtime-server --check-config
	"$(RUNTIME_BIN)" --check-config --config "$(CONFIG)"

.PHONY: goosetower-check-config
goosetower-check-config: ## Validate Goosetower config via gg-goosetower --check-config
	"$(GOOSETOWER_BIN)" --check-config --config "$(GOOSETOWER_CONFIG)"

.PHONY: goosetower-preflight
goosetower-preflight: ## Run Goosetower deployment preflight checks
	./scripts/preflight-goosetower.sh --config "$(GOOSETOWER_CONFIG)" --goosetower-bin "$(GOOSETOWER_BIN)" $(if $(BASE_URL),--base-url "$(BASE_URL)") $(if $(TOKEN),--token "$(TOKEN)")

.PHONY: gooseweb-dev
gooseweb-dev: ## Start Gooseweb dev server
	VITE_GOOSETOWER_URL=ws://127.0.0.1:$(DEV_GOOSETOWER_PORT)/v1/realtime VITE_GOOSEWEB_DEV_TICKET_ROUTE_ENABLED=true bun run --cwd apps/gooseweb dev --host 127.0.0.1 --port $(DEV_GOOSEWEB_PORT)

.PHONY: dev
dev: ## Start local runtime, Goosetower, and Gooseweb together
	RUNTIME_PORT=$(DEV_RUNTIME_PORT) GOOSETOWER_PORT=$(DEV_GOOSETOWER_PORT) GOOSEWEB_PORT=$(DEV_GOOSEWEB_PORT) ./scripts/dev-gooseweb-stack.sh

.PHONY: gooseweb-build
gooseweb-build: ## Build Gooseweb app
	bun run --cwd apps/gooseweb build

.PHONY: gooseweb-typecheck
gooseweb-typecheck: ## Typecheck Gooseweb app
	bun run --cwd apps/gooseweb typecheck

.PHONY: gooseweb-check
gooseweb-check: ## Typecheck, test, and build Gooseweb app
	bun run --cwd apps/gooseweb typecheck
	bun run --cwd apps/gooseweb test
	bun run --cwd apps/gooseweb build

.PHONY: api-docs-refresh
api-docs-refresh: ## Regenerate runtime OpenAPI artifact from source
	./scripts/api-doc-sync.sh refresh

.PHONY: api-docs-status
api-docs-status: ## Show API/docs sync-relevant file status
	./scripts/api-doc-sync.sh status

.PHONY: api-docs-check
api-docs-check: ## Fail if API files changed without corresponding docs changes
	./scripts/api-doc-sync.sh check

.PHONY: lint-rust-file-lines
lint-rust-file-lines: ## Fail if any Rust file exceeds 1000 lines
	./scripts/check-rust-file-lines.sh 1000

.PHONY: check
check: ## Run repo-wide lint, format, API docs, workspace tests, and sidecar tests
	$(MAKE) lint-rust-file-lines
	cargo fmt --check
	cargo check -p goosetower
	cargo test -p goosetower
	cargo check --workspace
	cargo test --workspace
	cargo check --manifest-path sidecars/gg-mcp-server/Cargo.toml
	cargo test --manifest-path sidecars/gg-mcp-server/Cargo.toml
	$(MAKE) api-docs-check

.PHONY: vps-deploy
vps-deploy: ## One-command VPS deploy (upgrade + preflight + systemd enable/start)
	./scripts/deploy-vps.sh --version "$(VERSION)" --config "$(CONFIG)" --service "$(SERVICE)" --scope "$(SCOPE)" $(if $(BASE_URL),--base-url "$(BASE_URL)") $(if $(TOKEN),--token "$(TOKEN)")

.PHONY: vps-deploy-refresh
vps-deploy-refresh: ## VPS deploy and refresh service/env templates
	./scripts/deploy-vps.sh --version "$(VERSION)" --config "$(CONFIG)" --service "$(SERVICE)" --scope "$(SCOPE)" --refresh-unit-files $(if $(BASE_URL),--base-url "$(BASE_URL)") $(if $(TOKEN),--token "$(TOKEN)")

.PHONY: service-status
service-status: ## Show runtime service status via systemctl
	@if [ "$(SCOPE)" = "user" ]; then \
	  systemctl --user status "$(SERVICE)"; \
	else \
	  systemctl status "$(SERVICE)"; \
	fi

.PHONY: service-enable
service-enable: ## Reload, enable, and start runtime service via systemctl
	@if [ "$(SCOPE)" = "user" ]; then \
	  systemctl --user daemon-reload; \
	  systemctl --user enable --now "$(SERVICE)"; \
	else \
	  systemctl daemon-reload; \
	  systemctl enable --now "$(SERVICE)"; \
	fi

.PHONY: service-restart
service-restart: ## Restart runtime service via systemctl
	@if [ "$(SCOPE)" = "user" ]; then \
	  systemctl --user restart "$(SERVICE)"; \
	else \
	  systemctl restart "$(SERVICE)"; \
	fi

.PHONY: service-logs
service-logs: ## Tail runtime service logs via journalctl
	@if [ "$(SCOPE)" = "user" ]; then \
	  journalctl --user -u "$(SERVICE)" -f; \
	else \
	  journalctl -u "$(SERVICE)" -f; \
	fi
