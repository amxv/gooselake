---
title: "CLI and command runner"
description: "Understand the current gg-runtime-server command-line surface, Make targets, scripts, and what is still HTTP-only."
order: 3
category: "Start Here"
summary: "The practical command-line map for running, checking, installing, deploying, and documenting the runtime."
---

Gooselake has a command-line surface, but it is currently an **operator/server CLI**, not a polished interactive client CLI.

The main binary is:

```bash
gg-runtime-server
```

It starts the runtime server, validates config, and writes the generated OpenAPI artifact. Session creation, turn sending, event streaming, teams, processes, and worktrees are exposed through HTTP/SSE APIs rather than a separate first-party `gg` client command.

## `gg-runtime-server`

Start the runtime with default config resolution:

```bash
gg-runtime-server
```

Start with an explicit config file:

```bash
gg-runtime-server --config ./runtime-server.toml
```

Validate config and ensure data directories/auth material can be resolved:

```bash
gg-runtime-server --check-config --config ./runtime-server.toml
```

Write the OpenAPI artifact to the default path:

```bash
gg-runtime-server --write-openapi
```

Write the OpenAPI artifact to a custom path:

```bash
gg-runtime-server --write-openapi /tmp/runtime-server-openapi.yaml
```

The CLI intentionally rejects unknown arguments and rejects invalid flag combinations such as `--check-config` with `--write-openapi`.

## Make targets

The repo uses `make` as the human-friendly command runner. The most useful targets are:

```bash
make install
make install-source
make upgrade
make preflight
make preflight-http
make check-config
make api-docs-refresh
make api-docs-status
make api-docs-check
make vps-deploy
make vps-deploy-refresh
make service-status
make service-enable
make service-restart
make service-logs
```

Use `make help` to see the current list from the Makefile.

## Scripts behind the targets

The operational scripts live in `scripts/`:

| Script | Purpose |
| --- | --- |
| `install-runtime.sh` | install from a release artifact |
| `install-from-source.sh` | build/install from source checkout |
| `upgrade-runtime.sh` | staged upgrade path |
| `deploy-vps.sh` | VPS deployment helper |
| `preflight-runtime.sh` | local runtime preflight checks |
| `package-release.sh` | release bundle packaging |
| `api-doc-sync.sh` | generated OpenAPI/docs sync helper |

Most users should prefer Make targets unless they need to integrate the scripts into another automation system.

## HTTP remains the client surface

Today, these actions are HTTP/SSE-first:

- create/list/resume/close sessions
- send turns and respond to approvals
- replay or stream events
- create teams and send team messages
- spawn team members
- start/inspect/kill processes
- create/claim/release/cleanup worktrees
- invoke MCP gateway tools
- inspect diagnostics

The docs show `curl` examples because `curl` is the lowest-level truth. A future polished CLI should be a thin wrapper around the same API rather than a second runtime.

## Recommended shell environment

Most examples assume:

```bash
BASE_URL="http://127.0.0.1:8080"
TOKEN="replace-with-runtime-token"
AUTH=(-H "Authorization: Bearer $TOKEN")
```

Then protected requests look like:

```bash
curl "$BASE_URL/v1/health" "${AUTH[@]}"
```

## What a future client CLI should do

A first-class client CLI should not duplicate runtime logic. It should help humans compose the existing API:

- `gg session create --provider codex --model gpt-5.5`
- `gg turn send <session> --message ...`
- `gg events follow --session <session>`
- `gg team broadcast <team> --message ...`
- `gg process run --session <session> -- ...`
- `gg worktree claim <worktree> --session <session>`

The runtime already owns the hard parts. The CLI should mainly improve ergonomics.
