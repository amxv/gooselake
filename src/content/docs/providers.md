---
title: "Provider guide"
description: "Configure and diagnose Codex, Claude, and ACP behind the shared Gooselake runtime provider contract."
order: 30
category: "Runtime Services"
summary: "Provider setup, models, auth behavior, and capability differences."
---

Gooselake treats providers like replaceable engines behind one cockpit. Codex, Claude, and ACP can have different native protocols, but clients should still create runtime sessions, send runtime turns, and read runtime events.

The shared contract lives in `crates/runtime-core/src/provider.rs` as the `RuntimeProvider` trait.

## Shared provider contract

Every provider adapter maps its native lifecycle into the runtime contract:

- `metadata()` exposes `kind`, `display_name`, and enabled state.
- `healthcheck()` verifies the provider can be used.
- `list_models()` returns a model catalog when the provider has one.
- `auth_status()` reports readiness/auth state when implemented.
- `create_session()` opens provider-backed work.
- `resume_session()` reconnects provider state to runtime state.
- `send_turn()` dispatches a turn.
- `wait_for_turn()` returns terminal turn results.
- `interrupt_turn()` stops active work when supported.
- `respond_approval()` forwards approval decisions when supported.
- `close_session()` closes provider-side state.

Trait defaults return unsupported for features a provider does not implement. That lets new providers be added incrementally.

## Provider IDs

Provider IDs accepted by the runtime:

| Provider | ID |
| --- | --- |
| Codex | `codex` |
| Claude | `claude` |
| ACP | `acp` |

Provider IDs are parsed case-insensitively after trimming.

## Codex

Codex is configured through the Codex provider adapter.

### Models

The current Codex model catalog includes:

- `gpt-5.5`
- `gpt-5.4`
- `gpt-5.4-mini`

Check the running server rather than hardcoding:

```bash
curl "$BASE_URL/v1/providers/codex/models" "${AUTH[@]}"
```

Model responses include provider-owned `reasoning_levels`. Codex models can
offer different levels, so clients should populate reasoning/effort selectors
from this field instead of assuming a fixed set.

### Auth

Codex auth is staged from host credentials into the runtime's provider data area. This keeps runtime execution isolated from the normal host config path while still using the logged-in host as the source.

Inspect status:

```bash
curl "$BASE_URL/v1/providers/codex/auth/status" "${AUTH[@]}"
```

### Runtime behavior

Codex sessions use runtime-owned IDs. Provider-specific session references are persisted as opaque provider refs. If a provider-side session disappears but refs exist, the runtime may attempt resume before retrying dispatch.

## Claude

Claude uses the Claude provider adapter plus the bundled Claude bridge sidecar.

### Models

The current Claude model catalog includes:

- `claude-sonnet-5`
- `claude-opus-4-8`
- `claude-fable-5`
- `claude-haiku-4-5`

Check:

```bash
curl "$BASE_URL/v1/providers/claude/models" "${AUTH[@]}"
```

Claude model responses also include `reasoning_levels` when the selected model
supports a global reasoning selector. Some Claude models may expose a smaller
set or no global selector.

### Auth modes

Claude supports two auth modes:

- `host_machine`: use host-machine credentials.
- `runtime_managed`: manage auth through runtime API endpoints.

Runtime-managed auth endpoints:

```bash
# API key
curl -X POST "$BASE_URL/v1/providers/claude/auth/api-key"   "${AUTH[@]}"   -H 'Content-Type: application/json'   -d '{"api_key":"..."}'

# JSON import
curl -X POST "$BASE_URL/v1/providers/claude/auth/import-json"   "${AUTH[@]}"   -H 'Content-Type: application/json'   -d '{"auth_json":{}}'

# File upload
curl -X POST "$BASE_URL/v1/providers/claude/auth/import-file"   "${AUTH[@]}"   -F "file=@claude-auth.json"

# Logout runtime-managed Claude auth
curl -X POST "$BASE_URL/v1/providers/claude/auth/logout" "${AUTH[@]}"
```

Inspect status:

```bash
curl "$BASE_URL/v1/providers/claude/auth/status" "${AUTH[@]}"
```

### GG MCP injection

Claude sessions can receive GG MCP tool configuration through the bridge path. This allows provider-side tool calls to call back into the runtime gateway when enabled.

## ACP

ACP is configured as an external stdio agent process. It is useful when an agent implements the Agent Client Protocol and can be driven by Gooselake as another provider.

### Configuration

ACP is disabled by default. A typical config shape:

```toml
[providers.acp]
enabled = true
command = "agent-command"
args = ["--stdio"]
request_timeout_secs = 120
wait_timeout_secs = 3600

[providers.acp.env]
# Agent-specific environment goes here.
```

### Auth

ACP auth is agent-managed. Gooselake can expose auth status if the configured ACP provider reports it, but it does not provide a universal login flow for arbitrary ACP agents.

Inspect:

```bash
curl "$BASE_URL/v1/providers/acp/auth/status" "${AUTH[@]}"
```

If ACP is not registered, provider-specific ACP routes return not-found behavior.

### Models

ACP model catalogs may be empty. Some ACP agents expose model choices through session config rather than provider-global lists.

### Current limitations

ACP permission requests do not map cleanly to Gooselake's current pre-dispatch approval model. Unsupported provider-originated permission requests should be treated as a provider capability limitation rather than a client bug.

## Choosing a provider

Use Codex or Claude when you want the built-in provider integrations. Use ACP when you want to bring an external ACP-compatible agent behind the same runtime API.

The best client design avoids branching deeply on provider. Branch for provider selection and provider-specific auth screens; rely on runtime sessions, turns, events, approvals, teams, processes, and worktrees everywhere else.

## Diagnostics

Useful endpoints:

```bash
curl "$BASE_URL/v1/providers" "${AUTH[@]}"
curl "$BASE_URL/v1/diagnostics/providers" "${AUTH[@]}"
curl "$BASE_URL/v1/providers/{provider}/models" "${AUTH[@]}"
```

Provider diagnostics should be the first stop before blaming sessions or clients.
