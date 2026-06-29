# Provider Guide

Gooselake normalizes provider-specific execution behind the `RuntimeProvider` trait in `crates/runtime-core/src/provider.rs`. Clients create sessions and turns against the runtime; the runtime dispatches to Codex, Claude, or ACP and persists normalized records/events.

## Shared provider contract

Every provider adapter is expected to map its native lifecycle into the runtime contract:

- `metadata()` exposes `kind`, `display_name`, and enabled state.
- `healthcheck()` verifies the provider can be used at runtime bootstrap/diagnostics time.
- `list_models()` returns a provider catalog when the provider has one.
- `auth_status()` reports readiness/auth state when implemented.
- `create_session()` opens provider state for a runtime session.
- `resume_session()` reconnects runtime state to provider state after restart.
- `send_turn()` starts a turn.
- `wait_for_turn()` resolves terminal status and output.
- `interrupt_turn()` requests interruption.
- `respond_approval()` forwards approval decisions when supported.
- `close_session()` tears down provider session state.

The runtime persists the canonical `SessionRecord`, `TurnRecord`, `ApprovalRecord`, and `RuntimeEventRecord`; provider-specific references are stored as provider refs, not used as client-facing primary IDs.

## Provider IDs

Use these string values in API requests:

| Provider | API value | Crate |
| --- | --- | --- |
| Codex | `codex` | `crates/runtime-provider-codex` |
| Claude | `claude` | `crates/runtime-provider-claude` |
| ACP | `acp` | `crates/runtime-provider-acp` |

Example session creation:

```bash
curl -fsS -X POST \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"provider":"claude","model":"claude-sonnet-4-6","cwd":"/workspace/repo"}' \
  "$BASE_URL/v1/sessions"
```

## Codex

Codex is a host-machine provider. The runtime launches provider work with a staged Codex home.

### Models

`GET /v1/providers/codex/models` returns:

- `gpt-5.5`
- `gpt-5.4`
- `gpt-5.4-mini`

### Auth

Expected operator setup:

```bash
codex login
```

At runtime bootstrap, if `$HOME/.gg/codex/auth.json` exists, the server copies it into the runtime provider directory:

```text
<data.root_dir>/providers/codex/home/auth.json
```

`GET /v1/providers/codex/auth/status` reports whether that staged auth file exists and where `CODEX_HOME` is being used.

### Runtime behavior

- Session creation accepts `model`, `cwd`, `permission_mode`, and metadata.
- Turn sends can override `permission_mode`.
- Runtime approval gating can be used with `permission_mode = "require_approval"`.
- Startup recovery attempts to resume sessions with stored provider refs; failed resumes mark the session failed with a clear failure code.

## Claude

Claude runs through the runtime-owned Claude bridge sidecar. The Rust provider talks to `sidecars/claude-bridge`, and the bridge isolates Claude SDK/CLI behavior from the runtime server process.

### Models

`GET /v1/providers/claude/models` returns:

- `claude-sonnet-4-6`
- `claude-opus-4-8`
- `claude-haiku-4-5`

### Auth modes

Configured by `[providers].claude_auth_mode` or `GG_CLAUDE_AUTH_MODE`.

| Mode | Use when | Behavior |
| --- | --- | --- |
| `host_machine` | The machine already has Claude login material. | The bridge can use host-visible Claude config. Runtime-managed OAuth files and API key fallback are also considered. |
| `runtime_managed` | You want runtime-owned imported credentials/config. | Runtime-managed files are preferred; bridge overrides are considered when explicitly configured. |

Recommended local path:

```bash
claude login
curl -fsS -H "Authorization: Bearer $TOKEN" \
  "$BASE_URL/v1/providers/claude/auth/status"
```

Runtime-managed auth endpoints:

```bash
# API key
curl -fsS -X POST \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"api_key":"sk-ant-..."}' \
  "$BASE_URL/v1/providers/claude/auth/api-key"

# JSON import
curl -fsS -X POST \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d @claude-auth.json \
  "$BASE_URL/v1/providers/claude/auth/import-json"

# File upload
curl -fsS -X POST \
  -H "Authorization: Bearer $TOKEN" \
  -F "file=@claude-auth.json" \
  "$BASE_URL/v1/providers/claude/auth/import-file"

# Logout runtime-managed Claude auth
curl -fsS -X POST \
  -H "Authorization: Bearer $TOKEN" \
  "$BASE_URL/v1/providers/claude/auth/logout"
```

### GG MCP injection

Claude sessions are created/resumed with a GG MCP server configuration. The runtime builds:

- gateway URL: `<server.public_base_url>/v1/mcp`
- gateway token: runtime bearer token
- caller agent ID: runtime session ID
- MCP command: discovered bundled `gg-mcp-server`, or `GG_MCP_SERVER_PATH` override

That lets Claude call runtime-owned tools while preserving session/caller ownership.

## ACP

ACP is an external agent-provider integration over stdio. The runtime launches the configured agent command and maps ACP prompt/update behavior into the runtime turn model.

### Configuration

```toml
[providers.acp]
enabled = true
command = "/absolute/path/to/acp-agent"
args = ["serve", "--stdio"]
transport = "stdio"
request_timeout_secs = 30
wait_timeout_secs = 300

[providers.acp.env]
# Agent-specific environment goes here.
```

### Auth

ACP auth is agent-managed in v1. The runtime reports configuration state through:

```bash
curl -fsS -H "Authorization: Bearer $TOKEN" \
  "$BASE_URL/v1/providers/acp/auth/status"
```

Typical modes:

- `disabled`: provider is disabled.
- `invalid_config`: provider config is malformed.
- `not_configured`: provider is enabled but no command is configured.
- `agent_managed`: an ACP stdio agent command is configured; auth negotiation is delegated to that agent.

### Models

`GET /v1/providers/acp/models` can return `[]`. ACP model selection may be inside the configured agent or session config rather than a global runtime catalog.

### Current limitations

- `transport = "stdio"` only.
- No runtime ACP API-key import, JSON import, file import, or logout endpoints.
- ACP `session/request_permission` is not bridged into runtime approvals yet. If an ACP agent sends that request, the active turn fails with a clear unsupported error.

## Choosing a provider

| Situation | Best provider posture |
| --- | --- |
| You want the runtime to drive Codex CLI sessions on the host. | Enable Codex and run `codex login` on the host first. |
| You want Claude Code sessions with host credentials. | Enable Claude, keep `claude_auth_mode = "host_machine"`, run `claude login`. |
| You want imported/runtime-owned Claude credentials. | Enable Claude and set `claude_auth_mode = "runtime_managed"`. |
| You have an ACP-compatible coding agent. | Enable ACP with an absolute `command` and stdio args. |
| You are testing the runtime API without real providers. | Disable providers you do not have configured, but at least one provider must remain enabled. |

## Diagnostics

Provider-level checks:

```bash
curl -fsS -H "Authorization: Bearer $TOKEN" "$BASE_URL/v1/providers"
curl -fsS -H "Authorization: Bearer $TOKEN" "$BASE_URL/v1/diagnostics/providers"
curl -fsS -H "Authorization: Bearer $TOKEN" "$BASE_URL/v1/providers/codex/auth/status"
curl -fsS -H "Authorization: Bearer $TOKEN" "$BASE_URL/v1/providers/claude/auth/status"
curl -fsS -H "Authorization: Bearer $TOKEN" "$BASE_URL/v1/providers/acp/auth/status"
```

When diagnosing provider failures, check both the auth endpoint and the server logs. Many provider problems are not HTTP bugs; they are missing host credentials, bad sidecar paths, bad `public_base_url`, or an ACP command that cannot start under the service environment.
