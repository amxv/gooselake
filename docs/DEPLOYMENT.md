# Deployment Guide

## Command Runner

This repo exposes deploy/ops tasks via `make`:

```bash
make help
```

## Deployment Modes

### Host Install (Recommended)

Use this for Linux VPS and most serious always-on setups.

Why this is the default today:
- runtime is designed around host filesystem/process access
- provider login flows (`codex login`, `claude login`) are machine-native
- sidecar path discovery already matches release bundle layout
- lower operational complexity than container profile for current architecture

### Containerized Runtime (Advanced / Optional)

Container deployment is possible, but currently requires extra path/auth/tooling setup and should be treated as an advanced profile.

If you need maximum reproducibility over minimal complexity, use containers. Otherwise, prefer host install.

## Linux VPS Runbook (Systemd User Service)

### Automated Path (Recommended)

Use the orchestration script to perform upgrade + preflight + systemd enable/start in one command:

```bash
make vps-deploy
```

With post-start HTTP verification:

```bash
make vps-deploy BASE_URL="http://127.0.0.1:8080" TOKEN="$GG_RUNTIME_TOKEN"
```

The script is safe to rerun and supports refreshing unit templates:

```bash
make vps-deploy-refresh
```

For system-level units instead of user-level units:

```bash
make vps-deploy SCOPE=system SERVICE=gg-runtime.service
```

### 1. Install prerequisites

On the VPS:
- `curl`
- `tar`
- provider CLIs you plan to use (`codex`, `claude`)
- `systemd --user` available for your account

### 2. Install runtime as staged releases

```bash
make upgrade
```

Default staged layout:

```text
~/.local/share/gg-runtime/
  current -> releases/<version-timestamp>/
  releases/
```

Use `~/.local/share/gg-runtime/current/bin/gg-runtime-server` in service `ExecStart`.

### 3. Create config in a stable location

```bash
mkdir -p "$HOME/.config/gg-runtime"
cp "$HOME/.local/share/gg-runtime/current/runtime-server.toml.example" \
  "$HOME/.config/gg-runtime/runtime-server.toml"
```

For VPS use, set:
- explicit `auth.token` (recommended for stable automation)
- absolute `data.root_dir` for persistent state
- localhost bind unless intentionally exposed behind proxy

Example (edit your config accordingly):

```toml
[server]
bind_address = "127.0.0.1:8080"
public_base_url = "http://127.0.0.1:8080"

[auth]
mode = "static_bearer"
token = "replace-with-strong-token"

[data]
root_dir = "/home/<user>/.local/state/gg-runtime"
sqlite_path = "runtime.sqlite3"
logs_dir = "logs"
providers_dir = "providers"
```

### 4. Login providers on the host

```bash
codex login
claude login
```

### 5. Install systemd user unit

```bash
mkdir -p "$HOME/.config/systemd/user" "$HOME/.config/gg-runtime"
cp "$HOME/.local/share/gg-runtime/current/deploy/systemd/gg-runtime.service.example" \
  "$HOME/.config/systemd/user/gg-runtime.service"
cp "$HOME/.local/share/gg-runtime/current/deploy/systemd/gg-runtime.env.example" \
  "$HOME/.config/gg-runtime/runtime.env"
```

If you installed staged releases somewhere else, update `ExecStart` and config path in the unit file.

### 6. Run preflight checks

```bash
make preflight
```

### 7. Enable and start service

```bash
make service-enable
```

Optional: keep user services alive across logout/reboot cycles:

```bash
loginctl enable-linger "$USER"
```

### 8. Validate runtime health

```bash
TOKEN="replace-with-your-runtime-token"
BASE_URL="http://127.0.0.1:8080"

curl -fsS "$BASE_URL/health"
curl -fsS -H "Authorization: Bearer $TOKEN" "$BASE_URL/v1/health"
curl -fsS -H "Authorization: Bearer $TOKEN" "$BASE_URL/v1/diagnostics"
curl -fsS -H "Authorization: Bearer $TOKEN" "$BASE_URL/v1/diagnostics/providers"
```

## Logging and Troubleshooting

### Service logs (systemd)

```bash
make service-logs
```

### Runtime process logs (runtime-managed)

Logs for spawned runtime processes are stored under:

```text
${data.root_dir}/${data.logs_dir}/processes
```

This log plane is distinct from systemd journal logs.

### Common failure playbooks

1. Runtime crashes/restart loop
- inspect `journalctl --user -u gg-runtime.service`
- run config check directly:
  `make check-config`
- if start-limit triggered: `systemctl --user reset-failed gg-runtime.service`

2. Provider auth failures
- verify host login material: rerun `codex login` and/or `claude login`
- check auth status endpoints:
  - `/v1/providers/codex/auth/status`
  - `/v1/providers/claude/auth/status`

3. Sidecar/binary layout errors
- verify staged release layout under `current/` includes:
  - `bin/gg-runtime-server`
  - `sidecars/claude-bridge/claude-bridge`
  - `sidecars/gg-mcp-server/gg-mcp-server`

## Upgrade and Rollback

Upgrade to a new release (atomic symlink switch):

```bash
make upgrade
make service-restart
```

Or have script restart service automatically:

```bash
make vps-deploy-refresh
```

Rollback command is printed by the upgrade script. After rollback, restart service.

## Backup and Restore

Back up at minimum:
- `${data.root_dir}/${data.sqlite_path}`
- `${data.root_dir}/${data.providers_dir}`
- auth token file path (default `${data.root_dir}/auth/api-token` when `auth.token` is not set)

Recommended additional backups:
- `${data.root_dir}/${data.logs_dir}`
- worktree root (`worktrees.root_dir`) if you need to preserve claimed worktree state

Simple stop-and-copy backup flow:

```bash
systemctl --user stop gg-runtime.service
# copy backup artifacts
systemctl --user start gg-runtime.service
```

## Local Full-Filesystem Machine (Quick Path)

For local/personal use, the existing simple path remains valid:

```bash
make install
cp "$HOME/.local/runtime-server.toml.example" ./runtime-server.toml
codex login
claude login
gg-runtime-server --config ./runtime-server.toml
```

If you want always-on local behavior on Linux, you can still run the same `gg-runtime.service` unit model above.

## Security Notes

- Prefer binding to localhost and placing TLS/reverse proxy in front when exposing publicly.
- Use strong bearer tokens and rotate periodically.
- Keep runtime data directories permission-restricted.
- Keep `UMask=0077` in service config for conservative file permissions.
