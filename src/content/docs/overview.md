---
title: "Documentation overview"
description: "The full documentation map for Gooselake: setup, architecture, API, providers, operations, deployment, and release workflows."
order: 0
category: "Start Here"
summary: "Use this as the canonical guide map for the runtime."
---

Gooselake is a machine-side runtime for durable agent work. It exposes HTTP control APIs, replayable SSE event streams, provider-backed sessions, process/worktree services, team communication, and MCP sidecar plumbing from one host-owned control plane.

Use this Astro docs collection as the operating manual for the runtime. `src/content/docs/` is the single source of truth for both the docs website and the release-bundle Markdown docs.

## Start here

Install and run locally:

```bash
make install
cp "$HOME/.local/runtime-server.toml.example" ./runtime-server.toml
gg-runtime-server --check-config --config ./runtime-server.toml
gg-runtime-server --config ./runtime-server.toml
```

Deploy to a Linux VPS with staged releases and a systemd service:

```bash
make vps-deploy
```

Show every repo task:

```bash
make help
```

## Reading paths

### New operator

1. [Install Guide](/docs/install)
2. [Configuration Reference](/docs/configuration)
3. [Provider Guide](/docs/providers)
4. [Operations Runbook](/docs/operations)

### Frontend or API client builder

1. [API Guide](/docs/api)
2. [Endpoint Catalog](/docs/endpoint-catalog)
3. [Architecture](/docs/architecture)
4. [MCP and Sidecars](/docs/mcp-and-sidecars)

### Runtime contributor

1. [Architecture](/docs/architecture)
2. [Provider Guide](/docs/providers)
3. [API Doc Sync Workflow](/docs/api-doc-sync)
4. `crates/runtime-core/src/*`, `crates/runtime-server/src/http.rs`, and the provider crate you are changing.

## What the runtime owns

Gooselake is intentionally not just a token proxy. The server owns the pieces that have to survive browser refreshes, process restarts, provider differences, and multi-agent workflows:

- provider registry and provider-backed sessions
- normalized turn lifecycle
- durable event history in SQLite
- replay-first SSE streams
- runtime bearer authentication
- startup recovery and diagnostics
- Codex, Claude, and ACP provider adapters
- Claude and MCP sidecar process boundaries
- host process execution and logs
- managed git worktree creation, claims, releases, and cleanup
- team membership, direct messages, broadcasts, deliveries, retries, cancellation, and interrupts

## Repo map

| Path | Purpose |
| --- | --- |
| `crates/runtime-core` | Provider trait, session manager, event model, team comms traits, and shared records. |
| `crates/runtime-server` | Config, bootstrap composition root, HTTP/SSE routes, OpenAPI generation, and binary entrypoint. |
| `crates/runtime-store-sqlite` | Durable SQLite implementation for sessions, turns, approvals, events, teams, worktrees, and processes. |
| `crates/runtime-provider-codex` | Codex provider adapter and auth staging behavior. |
| `crates/runtime-provider-claude` | Claude provider adapter, Claude bridge integration, auth import/status flows, and GG MCP injection. |
| `crates/runtime-provider-acp` | ACP v1 stdio provider adapter. |
| `crates/runtime-tools` | Runtime process manager, MCP tool gateway, worktree service, team spawn workflow. |
| `sidecars/claude-bridge` | Bun/TypeScript bridge around Claude Code SDK behavior. |
| `sidecars/gg-mcp-server` | MCP server exposing `gg_*` tools that call back into the runtime gateway. |
| `openapi/runtime-server-openapi.yaml` | Generated route artifact. |
| `deploy/systemd` | Example systemd service and env files. |
| `examples/runtime-server.toml` | Full baseline config template. |

## API artifacts

- Generated OpenAPI artifact: [`openapi/runtime-server-openapi.yaml`](https://github.com/amxv/gooselake/blob/main/openapi/runtime-server-openapi.yaml)
- Public OpenAPI endpoint: `GET /openapi.yaml`
- Authenticated OpenAPI endpoint: `GET /v1/openapi.yaml`
- Sync helpers: `make api-docs-refresh`, `make api-docs-status`, `make api-docs-check`

## Command reference

| Command | Use |
| --- | --- |
| `make install` | Install the latest release bundle into `~/.local`. |
| `make install VERSION=v0.1.2` | Install a pinned release. |
| `make install-source` | Build and install from the current checkout. |
| `make upgrade` | Stage a release under `~/.local/share/gg-runtime/releases` and atomically update `current`. |
| `make vps-deploy` | Run the host install, preflight, systemd enable/start flow. |
| `make preflight` | Validate config, binary layout, and filesystem expectations without HTTP checks. |
| `make preflight-http BASE_URL=... TOKEN=...` | Run filesystem and HTTP preflight checks. |
| `make service-status` | Show systemd service status. |
| `make service-logs` | Follow service logs. |
| `make service-restart` | Restart the service. |
| `make api-docs-refresh` | Regenerate OpenAPI. |
| `make api-docs-check` | Ensure API changes were accompanied by doc changes. |

## Important constraints to remember

- `/v1/**` routes require `Authorization: Bearer <token>`.
- `GET /health` and `GET /openapi.yaml` are public.
- SSE streams replay first, then attach to live events.
- `after_seq` takes precedence over `Last-Event-ID` on stream endpoints.
- ACP v1 is stdio-only and agent-managed for auth.
- ACP `session/request_permission` is intentionally unsupported in v1 and fails the active turn clearly.
- Claude can use host-machine credentials or runtime-managed imported auth files.
- Codex auth is staged from the host `~/.gg/codex/auth.json` when available.
- The MCP gateway requires an active caller session; closed or failed caller sessions are rejected.
