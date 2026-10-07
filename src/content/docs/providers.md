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
- `capabilities()` reports support, agent-managed behavior, or unsupported behavior for optional runtime features.
- `healthcheck()` verifies the provider can be used.
- `list_models()` returns a model catalog when the provider has one.
- `discover_models()` exposes provider-neutral model discovery without forcing external agents into a built-in catalog.
- `discover_skills()` exposes provider-neutral skill discovery where an adapter implements it.
- `auth_status()` reports readiness/auth state when implemented.
- typed create/resume policy carries permission intent, setting-source intent, system prompt, tool policy, harness slot, and current session preferences without clients branching on provider names.
- `send_turn()` dispatches a turn.
- `wait_for_turn()` returns terminal turn results.
- `interrupt_turn()` stops active work when supported.
- `respond_approval()` forwards approval decisions when supported.
- `close_session()` closes provider-side state.

Trait defaults return unsupported for features a provider does not implement. That lets new providers be added incrementally.

The v2 provider contract exposes those distinctions directly:

```bash
curl "$BASE_URL/v2/providers/codex/capabilities" "${AUTH[@]}"

curl -X POST "$BASE_URL/v2/providers/codex/models/discover" \
  "${AUTH[@]}" \
  -H 'Content-Type: application/json' \
  -d '{"setting_sources_intent":{"kind":"standard"},"force_refresh":false,"startup_mode":"cold"}'

curl -X POST "$BASE_URL/v2/providers/codex/skills/discover" \
  "${AUTH[@]}" \
  -H 'Content-Type: application/json' \
  -d '{"cwd":"/repo","setting_sources_intent":{"kind":"standard"},"force_refresh":false}'
```

Capability values are `supported`, `agent_managed`, or `unsupported`. Discovery modes are `catalog`, `agent_managed`, or `unsupported`. An empty result is therefore not ambiguous: check the reported mode before treating an empty catalog as “no models exist.”

Setting-source intent is typed. `standard` resolves to the user source and, when a working directory is present, the project/local sources as well. `explicit` names an ordered subset of `user`, `project`, and `local`; project/local sources require a cwd. `isolated` selects no ambient setting sources. Model discovery also accepts `startup_mode: "cold"` (default) or `"start_runtime"` so dynamic adapters can distinguish cache-only discovery from discovery that may start provider machinery.

## Provider IDs

Provider IDs accepted by the runtime:

| Provider | ID |
| --- | --- |
| Codex | `codex` |
| Claude | `claude` |
| ACP | `acp` |

Provider IDs are parsed case-insensitively after trimming.

## Codex

Codex is configured through the Codex provider adapter and runs through the Codex app-server JSON-RPC protocol. The host Codex CLI must expose `codex app-server`; there is no `codex exec` compatibility fallback.

### Models

The current Codex model catalog includes:

- `gpt-6-astra`
- `gpt-6.1-sol`
- `gpt-6-luna`

Check the running server rather than hardcoding:

```bash
curl "$BASE_URL/v1/providers/codex/models" "${AUTH[@]}"
```

Each current Codex model supports the curated `low`, `medium`, `high`,
`xhigh`, and `max` thinking-effort values. The v2 discovery response projects
these values as typed model capabilities. Clients should use the returned values
instead of inventing aliases.

### Auth

Codex auth is staged from host credentials into the runtime's provider data area. This keeps runtime execution isolated from the normal host config path while still using the logged-in host as the source.

Inspect status:

```bash
curl "$BASE_URL/v1/providers/codex/auth/status" "${AUTH[@]}"
```

### Runtime behavior

Each runtime Codex session owns a persistent app-server process and a deterministic provider home while the temporary caller-scoped GG MCP bridge remains in use. The provider-native `thread.id` is persisted separately from the runtime's opaque session ID and is reused through `thread/resume` after restart. Logical runtime turn IDs are likewise correlated with provider-native turn IDs instead of being inferred from process output.

Turn input remains structured at the provider boundary. Text, local images, and native skill references are sent as app-server input items instead of being flattened into a prompt. Current thinking effort is passed on `turn/start`. Provider-originated command/file approvals are correlated back into runtime approval records, and the adapter implements app-server interrupt, context-window observation, manual compaction, workspace rebind, and hard-fork/revert primitives.

Codex skill discovery is `catalog` mode and delegates to app-server `skills/list`. The result is therefore scoped to what the installed Codex runtime can discover for the requested working directory rather than a hardcoded Gooselake list.

## Claude

Claude uses the Claude provider adapter plus the bundled Claude bridge sidecar.

### Models

The current Claude model catalog includes:

- `claude-opus-5-5`
- `claude-fable-5-1`
- `claude-sonnet-5-5`

Check:

```bash
curl "$BASE_URL/v1/providers/claude/models" "${AUTH[@]}"
```

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

### Session policy, skills, and recovery

Claude's bundled bridge uses the Claude Agent SDK. Regular sessions preserve the Claude Code system preset while appending the caller's system instructions and the versioned GG harness once. Create and resume carry the same model, working directory, setting-source intent, tool allow/deny policy, harness version, and current thinking preference. Standard setting sources resolve to `user`, `project`, and `local` when a working directory exists; explicit and isolated sources are also supported.

Permission intent remains part of the durable recreation policy, but the bridge applies its effective mode to each SDK turn, where provider permission decisions actually occur. The manager supports revision-protected Claude permission and thinking-preference mutations for workspace-owned agents; these internal controls are not yet public `/v2` mutation endpoints. Provider-origin approvals, observed permission selection, and context-compaction signals are correlated to the admitted runtime turn rather than creating a second turn identity.

The Claude SDK usually supplies its canonical native session ID during the first turn, not at session creation. Gooselake persists that observed identity against the runtime session (including workspace-owned agents) before allowing the turn to become terminal. A final provider-identity read closes the event/wait race. Restart and resume use the durable canonical ID rather than the bridge's temporary session handle; missing or unverified identity requires recovery rather than guessing a replacement.

Claude model discovery is catalog-backed, and skill discovery asks the bridge for SDK-supported commands using the requested cwd and setting sources. Results reflect what the configured SDK installation discovers, not a hardcoded skill list. The bridge also supports evidence-backed workspace rebinding, context-window observations, manual compaction (`accepted` or `not_performed`), and native hard-fork/edit boundaries where the SDK supplies the required canonical session and history evidence. Operations that lack that evidence fail instead of guessing a new session identity or working directory. Live authentication and supported SDK behavior must be verified in each deployment environment.

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

ACP model catalogs are agent-managed. The v2 model discovery endpoint reports `mode: "agent_managed"` with no invented built-in entries; an empty list is valid because the configured ACP agent may own model selection privately.

Gooselake does not inject the tracked Golden Goose harness as hidden ACP user text. ACP owns its private model/system harness and receives the Gooselake collaboration surface through scoped GG MCP configuration. The capabilities response reports that distinction in the harness metadata.

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
curl "$BASE_URL/v2/providers/{provider}/capabilities" "${AUTH[@]}"
```

Provider diagnostics should be the first stop before blaming sessions or clients.
