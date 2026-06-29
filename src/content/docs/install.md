---
title: "Install guide"
description: "Install Gooselake from a release artifact or from source, including service files, provider CLIs, upgrades, and bundle layout."
order: 2
category: "Start Here"
summary: "The complete install path for local machines and Linux hosts."
---

## Command Runner

Use the repo task runner for install/deploy operations:

```bash
make help
```

## Fast Path (Release Artifact)

Prereqs on host:
- `curl`
- `tar`
- provider CLIs you want to use (`codex`, `claude`)
- an ACP-compatible agent command if you want to enable ACP

Install latest release to `~/.local`:

```bash
make install
```

Or run directly from GitHub without cloning:

```bash
curl -fsSL https://raw.githubusercontent.com/amxv/gooselake/main/scripts/install-runtime.sh | \
  bash -s -- latest
```

Install a pinned version:

```bash
make install VERSION=v0.1.2
```

Only set `GG_RUNTIME_REPO` if you want to install from a fork or alternate repo. The default repo is `amxv/gooselake`:

```bash
GG_RUNTIME_REPO=owner/repo make install
```

Then:

```bash
export PATH="$HOME/.local/bin:$PATH"
cp "$HOME/.local/runtime-server.toml.example" ./runtime-server.toml
codex login
claude login
gg-runtime-server --check-config --config ./runtime-server.toml
gg-runtime-server --config ./runtime-server.toml
```

If you want ACP enabled in the first release, edit `./runtime-server.toml` before the config check:

```toml
[providers.acp]
enabled = true
command = "/absolute/path/to/your-acp-agent"
args = ["serve", "--stdio"]
transport = "stdio"
request_timeout_secs = 30
wait_timeout_secs = 300

[providers.acp.env]
# ACP_AGENT_API_TOKEN = "replace-if-your-agent-needs-it"
```

ACP install notes:
- only stdio transport is supported in v1; streamable HTTP ACP transport is not supported
- ACP auth is agent-managed in v1; there is no runtime ACP login, API-key import, JSON import, or logout flow
- `GET /v1/providers/acp/models` may return an empty list because ACP model selection can be session-scoped inside the configured agent
- ACP permission requests are unsupported in v1 and fail the active turn if the agent sends `session/request_permission`

## Staged Install for VPS (Recommended for Always-On)

For Linux VPS, prefer staged upgrades + symlink activation:

```bash
make upgrade
```

Default staged root:

```text
~/.local/share/gg-runtime/
  current -> releases/<version-timestamp>/
  releases/
```

This gives atomic activation and straightforward rollback.

For a single-command deploy flow on Linux VPS (upgrade + preflight + systemd start), use:

```bash
make vps-deploy
```

## Source Install (No Release Needed)

```bash
make install-source
```

## Preflight Checks

Before enabling a service, run:

```bash
make preflight CONFIG=./runtime-server.toml RUNTIME_BIN="$HOME/.local/bin/gg-runtime-server"
```

Or with an installed staged binary:

```bash
make preflight
```

Optional HTTP checks (running instance required):

```bash
make preflight-http BASE_URL="http://127.0.0.1:8080" TOKEN="$GG_RUNTIME_TOKEN"
```

## Install Layout

Runtime expects this relative layout by default:

```text
<install-root>/
  bin/gg-runtime-server
  sidecars/claude-bridge/claude-bridge
  sidecars/gg-mcp-server/gg-mcp-server
```

Additional deploy artifacts shipped in release/install bundle:

```text
<install-root>/
  deploy/systemd/gg-runtime.service.example
  deploy/systemd/gg-runtime.env.example
```

This allows starting `gg-runtime-server` without additional bridge path overrides and gives a baseline systemd unit for VPS deployment.

Release packaging note:
- the first ACP landing does not ship an ACP sidecar binary in the release bundle
- ACP runs the configured external agent command directly over stdio

## Related references

- [Configuration Reference](/docs/configuration)
- [Provider Guide](/docs/providers)
- [Operations Runbook](/docs/operations)
