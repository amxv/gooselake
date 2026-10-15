---
title: "MCP and sidecars"
description: "Understand how Gooselake uses sidecar processes for Claude bridge behavior and the bundled MCP tool server."
order: 34
category: "Runtime Services"
summary: "The boundary between the core runtime, provider bridges, and MCP tools."
---

Gooselake uses sidecars where protocol churn or provider integration details are better isolated from the core runtime server.

There are two bundled sidecars today:

- `sidecars/claude-bridge`: Claude bridge process used by the Claude provider.
- `sidecars/gg-mcp-server`: MCP server that can expose `gg_*` tools to provider sessions and call back into the runtime gateway.

The analogy is an adapter plug. The runtime keeps the wall socket stable; sidecars absorb the odd shapes of provider/tool protocols.

## Why sidecars exist

The runtime server owns durable state, provider orchestration, and HTTP/SSE APIs. Sidecars isolate integration surfaces that are better treated as process boundaries:

- provider SDK/CLI behavior
- JSON-lines bridge protocol details
- MCP server lifecycle
- environment injection
- provider-specific authentication plumbing
- tool-call callback wiring

This keeps `runtime-core` focused on records, invariants, events, and provider contracts.

## Claude bridge

The Claude provider communicates with the Claude bridge sidecar. The bridge exposes JSON-lines methods such as:

- `bridge.ping`
- `bridge.capabilities`
- `session.create`
- `session.resume`
- `session.send`
- `session.interrupt`
- `session.approval.respond`
- `session.wait`
- `session.supported_commands`
- `session.supported_models`
- `session.close`
- `bridge.shutdown`

The runtime provider adapter converts runtime session/turn operations into bridge requests and maps bridge responses back into runtime records and events.

## Model-facing GG tools and MCP server

The same three canonical tools are exposed through provider-native transports. **Codex** receives native dynamic-tool definitions at `thread/start`/`thread/resume`; its app-server's `item/tool/call` RPC is handled by the trusted adapter and dispatched directly to the runtime tool gateway. The adapter supplies the runtime session as caller and derives a stable invocation ID from the native tool call. Its staged configuration removes the legacy GG MCP server registration so tools cannot appear twice. Disabled tool families are omitted from native discovery using canonical gateway capabilities.

**Claude and ACP** use a per-session `gg-mcp-server` sidecar. Their model-facing path is:

```text
provider session -> gg-mcp-server -> POST /v2/mcp/invoke -> canonical runtime services
```

For MCP-backed providers, the provider adapter derives a **per-agent scoped MCP credential** from the runtime operator secret. The sidecar receives only that agent's credential and runtime session identity. The agent API derives the caller from the authenticated credential; callers cannot nominate another session, team or workspace in JSON. Archived agents, retired workspaces, closed sessions, and operator API bearer tokens cannot use this endpoint. Even if a custom/legacy gateway URL is configured, provider sessions never receive the raw operator credential: that misconfiguration fails authorization instead of falling back to operator access. Codex does not receive that bearer token: its in-process native bridge binds caller identity out-of-band and uses the same runtime authority.

`GET /v2/mcp/capabilities` and `POST /v2/mcp/invoke` require the scoped bearer token. The older `/v1/mcp` endpoints remain operator-authenticated compatibility interfaces; they are **not** used for provider sessions. The `v2` model-facing tool discovery contains **exactly these three tools when enabled**:

- **`gg_team`**: `mode=status` lists the caller's active workspace members and eligible managed worktrees; `add` creates an active workspace agent (optional `title`, `prompt`, `image_paths`, `model_preset`, and `worktree_name`) and delivers the supplied onboarding prompt or images through the canonical agent-message service; `remove` archives named workspace agents using `remove_agent_ids`; `assign_worktree` rebinds an existing member through the durable provider rebind path. Existing worktrees are preserved during removal and assignment; explicit cleanup requests (`preserve_worktrees=false` or `preserve_previous_worktree=false`) are rejected until safe cleanup is available.
- **`gg_message`**: `mode=direct` requires `agent_id` and `message` and delivers through the global agent-first direct-message service. `agent_id` accepts either the opaque session ID or the recipient's durable friendly alias (resolved through indexed authority, including across workspaces). `mode=broadcast` requires only `message` and broadcasts to active members of the caller's workspace (excluding the sender). Optional ordered `image_paths` are supported for eligible Codex and Claude recipients; the runtime rejects unsupported ACP image delivery rather than dropping files.
- **`gg_process`**: `mode=run` schedules a command and returns a durable `process_id`; `list` returns active workspace-visible processes; `status` inspects a specific `process_id`; `cancel` is limited to processes owned by the caller. Terminal completion is pushed to the agent automatically, so completion polling is prohibited. stdout/stderr log paths are available in process details after launch.

Mode-discriminated schemas reject unknown fields and combinations. The MCP sidecar mints a unique trusted invocation ID for each model tool call and carries it in a transport header, not model JSON; the native Codex bridge derives it from provider-native RPC call identity. Effectful HTTP gateway calls without a valid host invocation ID are rejected before mutation with `missing_invocation_id`. For `gg_team` mutations, the runtime journals an exact-input operation **before** native/provider changes and durably receipts successful results. Repeating the same host invocation and arguments returns the exact receipt without adding or removing another agent; reusing an ID with different arguments returns `conflict`. An interrupted operation without a confirmed receipt fails closed as `outcome_unresolved` and requires status inspection/operator reconciliation instead of redispatch. `gg_process` `run` likewise deduplicates durable process admissions by caller and invocation ID across restarts: replays return the original `process_id` and changed arguments conflict, without launching a second command.

Ambiguous mutating transport failures likewise return `outcome_unresolved` with `retryable=false`: **inspect authoritative status, do not blindly retry**. Tool responses use `{"ok":true,"result":...}` or `{"ok":false,"error":{"code":"...","message":"...","retryable":false}}`, with MCP `is_error` matching the envelope; Codex receives the same serialized envelope inside its native `contentItems` response.

The existing `[teams]` configuration determines whether team and message tools are enabled and the lead/non-lead member management permissions. Process tools are separately disabled with process configuration or `GG_MCP_ENABLE_PROCESS_TOOLS=0`. Disabled tools do not appear in model discovery.

The live runtime manages workspace member and messaging authority; the legacy `TeamRecord` endpoints remain available to the operator API for migration but are not the source of model workspace authority.

`gg_team` status returns the active roster in stable alias order with friendly identities, `role` (`lead` or `member`), `title` and `title_provenance`, creator provenance (`added_by` and `added_by_alias`), activity and the current provider-bound context remaining percentage. A missing or stale context-limit observation is `null`, never an estimate from recent transcript usage. The workspace lead may be absent (`leadless=true`); `lead_alias` is then `null`. Attached managed worktrees are identified by name, branch and routing state when the authoritative inventory is available. Eligible unassigned worktrees are sorted by name, branch and durable ID and limited to 20 entries; `available_worktrees_total` and `available_worktrees_truncated` show whether more exist. Basic roster status still works when Git worktree inventory is unavailable.

Incoming canonical agent messages arrive as model transport context, not a human user message. The compact `<agent_msg>` envelope names the sender's friendly alias, `kind="dm"` or `kind="broadcast"`, `context="workspace_team"` or `context="global_direct"`, and an ordered image-attachment count. Legacy migration-only context uses `legacy_team`. The envelope does not require the recipient to parse raw team IDs to coordinate with another agent.

## MCP environment

The MCP sidecar uses environment values such as:

```bash
GG_MCP_GATEWAY_URL="http://127.0.0.1:8080/v2/mcp"
GG_MCP_GATEWAY_TOKEN="<per-agent-scoped-credential>"
GG_MCP_CALLER_AGENT_ID="sess_codex_..."
GG_MCP_REQUIRE_TOOL_CALLER_AGENT_ID="1"
GG_MCP_ENABLE_PROCESS_TOOLS="1"
```

The exact environment is assembled by provider integration code when MCP tools are injected into a session.

The model never sets `GG_MCP_CALLER_AGENT_ID` or the credential; both come from its owning provider adapter, including on resume. `GG_MCP_ENABLE_PROCESS_TOOLS=0` hides `gg_process` without disabling workspace tools. The `v2` route derives identity exclusively from the signed scoped credential.

## Failure modes

Common MCP failures:

- caller session ID missing
- caller session is closed or unknown
- bearer token is wrong
- tool namespace is unsupported
- process ownership does not match caller session
- process tools are disabled in config
- team MCP tools are disabled in config
- caller is not an active workspace member
- non-lead caller is denied by the `gg_team` membership policy
- sidecar binary is missing from the release/source layout

Start with:

```bash
curl "$BASE_URL/v1/mcp/capabilities" "${AUTH[@]}"
curl "$BASE_URL/v1/diagnostics/providers" "${AUTH[@]}"
```

## Design boundary

Sidecars translate between provider tool transport and the runtime. Durable workspace membership, agent messages, worktrees and process state remain in canonical core services and SQLite. The provider-facing MCP tools do not create independent team state.
