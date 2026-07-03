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

The current runtime MCP gateway is intentionally narrower than the full HTTP API. It primarily exposes the `gg_process` namespace:

- run process
- get process status
- read process logs
- kill process

Team and worktree services are first-class runtime HTTP APIs, but not every one of those operations is currently exposed through the runtime MCP gateway. Use `/v1/mcp/capabilities` to inspect what the running server supports:

```bash
curl "$BASE_URL/v1/mcp/capabilities" "${AUTH[@]}"
```

This distinction matters for technical accuracy: Gooselake has team/worktree runtime services, but the current MCP gateway should not be described as a complete mirror of all runtime services.

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

## Failure modes

Common MCP failures:

- caller session ID missing
- caller session is closed or unknown
- bearer token is wrong
- tool namespace is unsupported
- process ownership does not match caller session
- process tools are disabled in config
- sidecar binary is missing from the release/source layout

Start with:

```bash
curl "$BASE_URL/v1/mcp/capabilities" "${AUTH[@]}"
curl "$BASE_URL/v1/diagnostics/providers" "${AUTH[@]}"
```

## Design boundary

Sidecars should not become alternate runtimes. They can translate, adapt, and bridge, but durable truth belongs in the core runtime and SQLite store.
