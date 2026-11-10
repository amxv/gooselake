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
- provider CLIs you want to use (`codex`, `claude`); Codex must expose `codex app-server`
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

For an already-downloaded package, skip GitHub access and install it from an
explicit local archive into a chosen prefix:

```bash
GG_RUNTIME_ARCHIVE_PATH=/path/to/gg-runtime-linux-x86_64.tar.gz \
GG_RUNTIME_INSTALL_ROOT=/path/to/isolated/prefix \
  ./scripts/install-runtime.sh latest
```

The installer checks the expected platform-specific archive root, rejects
traversal and symbolic-link members, verifies required sidecars and embedded
harness/model/tool contract parity **before writing into the install prefix**.
Existing installed binaries are not changed when a candidate fails contract
verification. This does not authenticate the GitHub release publisher; verify
archive provenance separately when that matters.

The staged upgrade path supports the same local archive override through
`GG_RUNTIME_ARCHIVE_PATH` and uses `GG_RUNTIME_RELEASES_ROOT` to control the
staged release destination. The candidate's harness and semantic tool
contracts are checked **before switching** `current`; a failed contract check
does not change the active release symlink.

Only set `GG_RUNTIME_REPO` if you want to install from a fork or alternate repo. The default repo is `amxv/gooselake`:

```bash
GG_RUNTIME_REPO=owner/repo make install
```

Then:

```bash
export PATH="$HOME/.local/bin:$PATH"
cp "$HOME/.local/runtime-server.toml.example" ./runtime-server.toml
codex login
codex app-server --help >/dev/null
claude login
gg-runtime-server --check-config --config ./runtime-server.toml
gg-runtime-server --config ./runtime-server.toml
```

New packages include a `contracts/` directory containing
`runtime-contracts.json`, `gooselake-harness-v1.md`, and
`codex-model-policy.json`. The runtime binary can independently verify all
three against its embedded harness and three-tool semantic contract:

```bash
gg-runtime-server --verify-contracts "$HOME/.local/contracts/runtime-contracts.json"
```

Run `make check-release-layout` in a source checkout for an offline,
isolated install/tamper smoke. That test uses the compiled runtime CLI and
inert stand-in sidecar *files*; it does **not** validate live provider startup.

Codex command resolution checks `GG_CODEX_COMMAND`, then `~/.local/bin/codex`, `~/.volta/bin/codex`, `~/.bun/bin/codex`, then `PATH`. Set `GG_CODEX_COMMAND` to an absolute executable path when a service environment cannot see the host install.

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
- ACP `session/request_permission` routes through the durable operator approval
  workflow when it can be bound to an admitted turn and a safe offered choice;
  malformed or unrouteable requests fail closed instead of being auto-approved

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
