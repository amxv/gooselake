# Install Guide

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

Install latest release to `~/.local`:

```bash
make install
```

Or run directly from GitHub without cloning:

```bash
curl -fsSL https://raw.githubusercontent.com/amxv/gg-agent-runtime/main/scripts/install-runtime.sh | \
  bash -s -- latest
```

Install a pinned version:

```bash
make install VERSION=v0.1.0
```

Only set `GG_RUNTIME_REPO` if you want to install from a fork or alternate repo:

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
