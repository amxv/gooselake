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

## GG MCP server

The MCP sidecar lets provider sessions call runtime-backed tools. The high-level path is:

```text
provider session
  -> gg-mcp-server tool call
  -> POST /v1/mcp/invoke
  -> runtime tool gateway
  -> runtime service
```

The runtime gateway requires a caller session identity. Tool calls should be attributable to an active runtime session.

## Current gateway scope

The runtime MCP gateway is intentionally narrower than the full HTTP API. It exposes provider-safe control namespaces that are backed by the same runtime services used by HTTP:

- `gg_process`: run a process, get status, read logs, and kill a process
- `gg_team`: inspect team status, send direct/broadcast team messages, and add/remove team members

Use `/v1/mcp/capabilities` to inspect what the running server supports:

```bash
curl "$BASE_URL/v1/mcp/capabilities" "${AUTH[@]}"
```

Capabilities include `supportedNamespaces`, `tools`, `ggProcessEnabled`, `ggTeamEnabled`, and `ggTeamManagePermissions`. Team tools are listed only when team MCP is enabled. If disabled, `gg_team_*` calls return an `ok:false` envelope with `feature_disabled`.

The team MCP surface is:

- `gg_team_status`: returns lead/member status for teams where the caller is an active member.
- `gg_team_message`: sends direct messages or broadcasts. Use `recipient_agent_id: "broadcast"` for broadcast fanout; the sender is excluded by default.
- `gg_team_manage`: add one member when `remove_agent_ids` is absent, or remove one or more members when `remove_agent_ids` is present.

`gg_team` calls share the same underlying team services as HTTP routes. Messages create normal team message, delivery, and event records. Member add/remove operations use the runtime spawn, join, remove, worktree assignment, and cleanup services.

Agent-initiated membership management is governed by runtime team policy:

```toml
[teams]
enabled = true
non_lead_can_add_members = false
non_lead_can_remove_members = false
```

The team lead can add and remove members by default. Non-lead members can use `gg_team_manage` add/remove only when the matching policy flag is enabled. This policy applies to MCP-initiated membership control; authenticated HTTP team administration remains the client/human control plane.

Codex, Claude, and ACP sessions all use the bundled `gg-mcp-server` path when MCP is enabled, so `gg_process` and `gg_team` behavior is provider-agnostic. Providers inject caller identity into the sidecar environment or per-call metadata; the model-authored tool arguments are not trusted for caller identity.

## MCP environment

The MCP sidecar uses environment values such as:

```bash
GG_MCP_GATEWAY_URL="http://127.0.0.1:8080/v1/mcp/invoke"
GG_MCP_GATEWAY_TOKEN="..."
GG_MCP_CALLER_AGENT_ID="sess_codex_..."
GG_MCP_REQUIRE_TOOL_CALLER_AGENT_ID="1"
GG_MCP_ENABLE_PROCESS_TOOLS="1"
```

The exact environment is assembled by provider integration code when MCP tools are injected into a session.

When `GG_MCP_REQUIRE_TOOL_CALLER_AGENT_ID=1`, the sidecar requires caller identity per tool call. This lets a shared MCP server process safely serve different runtime sessions. `GG_MCP_ENABLE_PROCESS_TOOLS=0` hides `gg_process` tools without disabling the unified server or the team tool path.

## Failure modes

Common MCP failures:

- caller session ID missing
- caller session is closed or unknown
- bearer token is wrong
- tool namespace is unsupported
- process ownership does not match caller session
- process tools are disabled in config
- team MCP tools are disabled in config
- caller is not a team member
- non-lead caller is denied by `gg_team_manage` policy
- sidecar binary is missing from the release/source layout

Start with:

```bash
curl "$BASE_URL/v1/mcp/capabilities" "${AUTH[@]}"
curl "$BASE_URL/v1/diagnostics/providers" "${AUTH[@]}"
```

## Design boundary

Sidecars should not become alternate runtimes. They can translate, adapt, and bridge, but durable truth belongs in the core runtime and SQLite store.
