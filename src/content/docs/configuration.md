---
title: Configuration
description: Configure server binding, bearer auth, data paths, providers, events, processes, worktrees, and sidecar overrides.
order: 6
category: Reference
summary: The practical config map for running Gooselake locally or as an always-on service.
---

## Config file

Run the server with:

```bash
gg-runtime-server --config ./runtime-server.toml
```

Validate first:

```bash
gg-runtime-server --check-config --config ./runtime-server.toml
```

The full reference is in `docs/CONFIGURATION.md`; the full template is `examples/runtime-server.toml`.

## Path resolution

Relative `data.root_dir` values are resolved relative to the config file directory. Other relative data paths sit under `data.root_dir`.

That means this config:

```toml
[data]
root_dir = ".gg-runtime"
sqlite_path = "runtime.sqlite3"
```

keeps state next to the config file, not necessarily next to the current shell directory.

## Important sections

- `[server]`: bind address and public base URL.
- `[auth]`: static bearer token or generated token file.
- `[data]`: SQLite, logs, and provider directories.
- `[providers]`: shared provider settings such as `claude_auth_mode`.
- `[providers.codex]`: Codex capacity and enable flag.
- `[providers.claude]`: Claude bridge capacity and enable flag.
- `[providers.acp]`: ACP stdio command, args, env, and timeouts.
- `[events]`: live and critical queue capacities.
- `[processes]`: runtime process execution limits.
- `[worktrees]`: managed git worktree behavior.

## Production posture

For a VPS:

- bind to `127.0.0.1` behind a reverse proxy
- set an explicit strong bearer token
- keep `data.root_dir` outside the release directory
- keep config under `~/.config/gg-runtime` or `/etc/gg-runtime`
- keep release bundles under `~/.local/share/gg-runtime` or `/opt/gg-runtime`
- run preflight before enabling systemd

## Common pitfall

`claude_auth_mode` belongs under `[providers]`, not below `[providers.acp.env]`.

```toml
[providers]
claude_auth_mode = "host_machine"

[providers.acp.env]
# only ACP agent env vars here
```
