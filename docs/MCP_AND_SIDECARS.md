# MCP and Sidecars

Gooselake uses sidecars to keep provider-specific and tool-protocol behavior outside the core HTTP server process.

There are two bundled sidecars today:

- `sidecars/claude-bridge`: Claude bridge process used by the Claude provider.
- `sidecars/gg-mcp-server`: MCP server exposing `gg_*` tools to provider sessions.

## Why sidecars exist

The runtime server owns durable state, provider orchestration, and HTTP/SSE APIs. Sidecars isolate integration surfaces that are better treated as process boundaries:

- provider SDK/runtime churn
- stdio protocols
- MCP tool schema generation
- per-provider environment setup
- process-level failure isolation

The release bundle preserves this layout:

```text
<install-root>/
  bin/gg-runtime-server
  sidecars/claude-bridge/claude-bridge
  sidecars/gg-mcp-server/gg-mcp-server
```

The runtime discovers bundled sidecars relative to the `gg-runtime-server` executable. `GG_MCP_SERVER_PATH` can override the MCP sidecar path.

## Claude bridge

The Claude provider does not embed all Claude SDK behavior directly into the Rust runtime. Instead, it starts a bridge process and talks to it using JSON lines.

Bridge methods include:

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

The bridge reports events with:

- `event`
- `seq`
- `sessionId`
- optional `turnId`
- provider payload

The Rust provider maps those events into runtime events and terminal turn results.

## GG MCP server

The MCP sidecar exposes tools to a provider session, then calls back into the runtime gateway:

```text
Provider session
  -> gg-mcp-server tool call
  -> POST <public_base_url>/v1/mcp/invoke
  -> runtime tool gateway
  -> process/team/worktree service
```

The runtime gateway routes are:

- `GET /v1/mcp/capabilities`
- `POST /v1/mcp/invoke`

`POST /v1/mcp/invoke` requires:

- `tool_name`
- `caller_agent_id`
- optional `namespace`
- optional `invocation_id`
- optional `args`

The caller agent ID is a runtime session ID. The runtime rejects calls from missing, closed, or failed sessions.

## MCP environment

The sidecar reads these variables:

| Variable | Meaning |
| --- | --- |
| `GG_MCP_GATEWAY_URL` | Runtime MCP gateway base URL, for example `http://127.0.0.1:8080/v1/mcp`. |
| `GG_MCP_GATEWAY_TOKEN` | Runtime bearer token. |
| `GG_MCP_CALLER_AGENT_ID` | Default caller session ID if the tool call omits hidden caller metadata. |
| `GG_MCP_REQUIRE_TOOL_CALLER_AGENT_ID` | If truthy, require explicit caller metadata. |
| `GG_MCP_ENABLE_PROCESS_TOOLS` | If `0`, `false`, or `off`, hide/disable `gg_process_*` tools. |

Provider adapters normally inject these automatically. Manual MCP runs must set them explicitly.

## Current tool families

The sidecar exposes runtime control-plane tools, including:

- `gg_ping`
- `gg_team_status`
- `gg_team_message`
- `gg_team_manage`
- `gg_markdown_open`
- `gg_process_run`
- `gg_process_status`
- `gg_process_kill`

Process tools are gated by `GG_MCP_ENABLE_PROCESS_TOOLS` and by runtime process configuration.

## Runtime gateway capabilities

`GET /v1/mcp/capabilities` is not just a static tool list. It lets the sidecar discover runtime-owned capabilities, including model-preset information for team management schemas.

Use:

```bash
curl -fsS -H "Authorization: Bearer $TOKEN" \
  "$BASE_URL/v1/mcp/capabilities"
```

## Failure modes

| Error | Meaning | Fix |
| --- | --- | --- |
| `backend_unavailable` from MCP tool result | Sidecar lacks gateway URL/token. | Check provider injection, `public_base_url`, and `GG_MCP_GATEWAY_*`. |
| `caller_agent_id is required` | Runtime did not receive caller identity. | Ensure provider session passes caller metadata or set `GG_MCP_CALLER_AGENT_ID`. |
| `caller session ... is not active` | Caller session is closed or failed. | Create/resume a valid runtime session before invoking tools. |
| Process tools unavailable | Process tools disabled in sidecar or runtime config. | Check `GG_MCP_ENABLE_PROCESS_TOOLS` and `[processes].enabled`. |
| Gateway request fails from sidecar | `public_base_url` is not reachable from provider/sidecar process. | Use a host-local URL such as `http://127.0.0.1:8080` for local sidecars. |

## Design boundary

The MCP sidecar should not become a second runtime. It is a tool-protocol façade. Durable truth remains in the server and SQLite store. That is why MCP tools call back into `/v1/mcp/*` instead of mutating sidecar-local state.
