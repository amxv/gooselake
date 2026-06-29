# Deployment Guide

## Command Runner

This repo exposes deploy/ops tasks via `make`:

```bash
make help
```


## Production stance

For the current architecture, the recommended production shape is a host install rather than a container-first deployment. Keep the release bundle replaceable, keep config and state outside the release directory, bind to localhost behind a reverse proxy, and run provider login/config steps on the same host that will execute agent work.

Detailed config behavior is documented in [Configuration Reference](./CONFIGURATION.md). Provider setup is documented in [Provider Guide](./PROVIDERS.md). Day-two checks are documented in [Operations Runbook](./OPERATIONS.md).

## Deployment Modes

### Host Install (Recommended)

Use this for Linux VPS and most serious always-on setups.

Why this is the default today:
- runtime is designed around host filesystem/process access
- provider login flows (`codex login`, `claude login`) are machine-native
- ACP in the first landing also assumes host execution because the runtime launches a configured ACP agent command over stdio
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
- an ACP-compatible agent command if you plan to enable ACP
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
- ACP `providers.acp.command`/`args`/`env` if ACP is enabled

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

ACP example block for that same config:

```toml
[providers.acp]
enabled = true
command = "/usr/local/bin/your-acp-agent"
args = ["serve", "--stdio"]
transport = "stdio"
request_timeout_secs = 30
wait_timeout_secs = 300

[providers.acp.env]
# ACP_AGENT_API_TOKEN = "replace-if-required-by-your-agent"
```

ACP deployment constraints in v1:
- only stdio transport is supported; streamable HTTP ACP transport is not supported
- auth is agent-managed; runtime exposes status only and does not provide ACP login/logout/import mutations
- ACP permission requests are unsupported in v1 and fail the active turn clearly

### 4. Login providers on the host

```bash
codex login
claude login
```

For ACP, there is no runtime login step in v1. Instead, ensure the configured ACP agent command starts successfully on the host and that any agent-specific environment variables are present in `[providers.acp.env]` or the systemd environment file.

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

### 9. Verify ACP end to end

Use this smoke runbook after enabling ACP in config:

1. Confirm provider registration:

```bash
curl -fsS -H "Authorization: Bearer $TOKEN" \
  "$BASE_URL/v1/providers"
```

2. Check ACP auth/config status:

```bash
curl -fsS -H "Authorization: Bearer $TOKEN" \
  "$BASE_URL/v1/providers/acp/auth/status"
```

Expected v1 shape:
- `mode` is typically `agent_managed` when the command is configured
- `mode` may be `not_configured`, `invalid_config`, or `disabled` when setup is incomplete

3. Check ACP model listing behavior:

```bash
curl -fsS -H "Authorization: Bearer $TOKEN" \
  "$BASE_URL/v1/providers/acp/models"
```

An empty list is valid in v1 because ACP model selection can be session-scoped inside the configured agent.

4. Create an ACP session:

```bash
SESSION_JSON=$(curl -fsS -X POST \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"provider":"acp"}' \
  "$BASE_URL/v1/sessions")

SESSION_ID=$(echo "$SESSION_JSON" | jq -r '.id')
```

5. Send a test turn:

```bash
curl -fsS -X POST \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"input":[{"type":"text","text":"Reply with the word ok."}]}' \
  "$BASE_URL/v1/sessions/$SESSION_ID/turns"
```

6. If the turn fails, inspect likely ACP-specific causes:
- `acp command is not configured`
- unsupported transport value other than `stdio`
- agent startup or stdio IO failure
- an ACP `session/request_permission` request, which is unsupported in v1 and intentionally fails the turn

### 10. Optional ignored real ACP smoke test

The repo includes an ignored runtime-server smoke test for machines that already have a working ACP agent command:

- required: `GG_ACP_SMOKE_COMMAND`
- optional: `GG_ACP_SMOKE_ARGS_JSON` as a JSON string array, for example `["serve","--stdio"]`
- optional: `GG_ACP_SMOKE_ENV_JSON` as a JSON object of string environment pairs
- optional: `GG_ACP_SMOKE_TIMEOUT_SECONDS`
- optional: `GG_ACP_SMOKE_DEBUG=1`

Exact command:

```bash
GG_ACP_SMOKE_COMMAND=/absolute/path/to/your-acp-agent \
GG_ACP_SMOKE_ARGS_JSON='["serve","--stdio"]' \
cargo test -p runtime-server ignored_real_acp -- --ignored --nocapture
```

What it verifies:
- ACP auth status route is reachable with the configured command
- the runtime can create an ACP session in a temporary cwd
- a deterministic prompt reaches `turn.completed`
- terminal assistant text is persisted in session events and the session transcript metadata

Keep expectations narrow for v1:
- the smoke does not assume remote ACP transport
- the smoke does not exercise runtime-managed ACP auth mutations
- the smoke does not require ACP permission-request support

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
  - `/v1/providers/acp/auth/status`
  - `/v1/providers/claude/auth/status`
- for ACP specifically, verify `providers.acp.command`, `providers.acp.args`, and `providers.acp.env`; the runtime does not mutate ACP credentials in v1

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

If you want ACP locally, enable `[providers.acp]`, point `command`/`args` at your ACP agent, and keep `transport = "stdio"`.

If you want always-on local behavior on Linux, you can still run the same `gg-runtime.service` unit model above.

## Security Notes

- Prefer binding to localhost and placing TLS/reverse proxy in front when exposing publicly.
- Use strong bearer tokens and rotate periodically.
- Keep runtime data directories permission-restricted.
- Keep `UMask=0077` in service config for conservative file permissions.
