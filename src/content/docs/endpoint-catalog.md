---
title: "Endpoint catalog"
description: "Browse the implemented runtime HTTP and SSE route surface, including public routes, bearer-protected routes, and provider-specific endpoints."
order: 70
category: "Reference"
summary: "A route-by-route catalog derived from the current server implementation."
---

This catalog tracks the currently implemented HTTP/SSE surface from runtime code.

Source references:
- [`crates/runtime-server/src/http/`](https://github.com/amxv/gooselake/blob/main/crates/runtime-server/src/http)
- [`openapi/runtime-server-openapi.yaml`](https://github.com/amxv/gooselake/blob/main/openapi/runtime-server-openapi.yaml)

Auth legend:
- `Public`: no bearer token
- `Bearer`: requires `Authorization: Bearer <token>`

## Runtime + Meta

- `GET /health` (Public)
- `GET /openapi.yaml` (Public)
- `GET /v1/health` (Bearer)
- `GET /v1/openapi.yaml` (Bearer)
- `GET /v1/version` (Bearer)

## Providers

- `GET /v1/providers` (Bearer)
- `GET /v1/providers/{provider}/models` (Bearer)
- `GET /v1/providers/codex/auth/status` (Bearer)
- `GET /v1/providers/acp/auth/status` (Bearer)
- `GET /v1/providers/claude/auth/status` (Bearer)
- `POST /v1/providers/claude/auth/api-key` (Bearer)
- `POST /v1/providers/claude/auth/import-json` (Bearer)
- `POST /v1/providers/claude/auth/import-file` (Bearer, `multipart/form-data`, field `file`)
- `POST /v1/providers/claude/auth/logout` (Bearer)

Provider behavior notes:
- `GET /v1/providers/{provider}/models` returns dynamic, provider-owned `reasoning_levels` using raw runtime capability tokens such as Codex `xhigh`.
- ACP v1 exposes only `GET /v1/providers/acp/auth/status` for auth. No ACP logout, API-key, JSON import, or file import routes are implemented.
- ACP auth status is agent-managed. The response reports configuration/readiness, not runtime-owned credentials.
- `GET /v1/providers/acp/models` may return an empty list because ACP model selection can be driven by session-scoped agent config.
- ACP permission requests are unsupported in v1 and fail the active turn clearly if an ACP agent requests them.

## Sessions

- `POST /v1/sessions` (Bearer)
- `GET /v1/sessions` (Bearer)
- `GET /v1/sessions/{session_id}` (Bearer)
- `POST /v1/sessions/{session_id}/resume` (Bearer)
- `POST /v1/sessions/{session_id}/close` (Bearer)
- `POST /v1/sessions/{session_id}/turns` (Bearer)
- `POST /v1/sessions/{session_id}/turns/{turn_id}/interrupt` (Bearer, returns `202`)
- `POST /v1/sessions/{session_id}/approvals/{approval_id}` (Bearer)
- `GET /v1/sessions/{session_id}/events` (Bearer)
- `GET /v1/sessions/{session_id}/events/stream` (Bearer, SSE)

Session event query parameters:
- replay: `after_seq`, `limit`
- stream: `after_seq`, `limit`, optional `Last-Event-ID` header fallback

## Global Runtime Events

- `GET /v1/events` (Bearer)
- `GET /v1/events/stream` (Bearer, SSE)

Global event query parameters:
- replay: `after_seq`, `limit`
- stream: `after_seq`, `limit`, optional `Last-Event-ID` header fallback

## Teams + Comms

- `POST /v1/teams` (Bearer)
- `GET /v1/teams` (Bearer)
- `GET /v1/teams/{team_id}` (Bearer)
- `DELETE /v1/teams/{team_id}` (Bearer, returns `204`)
- `POST /v1/teams/{team_id}/members` (Bearer)
- `POST /v1/teams/{team_id}/members/spawn` (Bearer)
- `DELETE /v1/teams/{team_id}/members/{agent_id}` (Bearer)
- `POST /v1/teams/{team_id}/lead` (Bearer)
- `POST /v1/teams/{team_id}/messages` (Bearer)
- `GET /v1/teams/{team_id}/messages` (Bearer)
- `POST /v1/teams/{team_id}/broadcasts` (Bearer)
- `GET /v1/teams/{team_id}/deliveries` (Bearer)
- `POST /v1/teams/{team_id}/deliveries/{delivery_id}/retry` (Bearer)
- `POST /v1/teams/{team_id}/messages/{message_id}/cancel` (Bearer)
- `GET /v1/teams/{team_id}/view` (Bearer)
- `GET /v1/teams/{team_id}/events` (Bearer)
- `GET /v1/teams/{team_id}/events/stream` (Bearer, SSE)
- `POST /v1/teams/{team_id}/interrupt-all` (Bearer)

Team query parameters:
- messages: `cursor`, `limit`
- deliveries: `message_id`, `recipient_agent_id`
- view: `message_cursor`, `message_limit`, `include_delivery_map`, `delivery_recipient_filter`
- events replay/stream: `after_seq`, `limit` (+ `Last-Event-ID` fallback for stream)

## Processes

- `POST /v1/processes` (Bearer)
- `GET /v1/processes` (Bearer)
- `GET /v1/processes/{process_id}` (Bearer)
- `GET /v1/processes/{process_id}/logs` (Bearer)
- `GET /v1/processes/{process_id}/events` (Bearer)
- `GET /v1/processes/{process_id}/events/stream` (Bearer, SSE)
- `POST /v1/processes/{process_id}/kill` (Bearer)

Process query parameters:
- list: `session_id`, `include_completed`
- get: `session_id`
- logs: `session_id`, `stream`, `head_lines`, `tail_lines`, `max_bytes`
- events replay/stream: `session_id`, `after_seq`, `limit` (+ `Last-Event-ID` fallback for stream)

## Worktrees

- `POST /v1/worktrees` (Bearer)
- `GET /v1/worktrees` (Bearer)
- `GET /v1/worktrees/{worktree_id}` (Bearer)
- `POST /v1/worktrees/{worktree_id}/claims` (Bearer)
- `POST /v1/worktrees/{worktree_id}/release` (Bearer)
- `POST /v1/worktrees/{worktree_id}/cleanup` (Bearer)

## Diagnostics

- `GET /v1/diagnostics` (Bearer)
- `GET /v1/diagnostics/providers` (Bearer)
- `GET /v1/diagnostics/comms` (Bearer)
- `GET /v1/diagnostics/processes` (Bearer)
- `GET /v1/diagnostics/worktrees` (Bearer)
- `GET /v1/diagnostics/recovery` (Bearer)
- `GET /v1/diagnostics/team-operations` (Bearer)

Diagnostics query parameters:
- team operations: `team_id`, `operation_id`

## MCP Gateway

- `GET /v1/mcp/capabilities` (Bearer)
- `POST /v1/mcp/invoke` (Bearer)

`POST /v1/mcp/invoke` request fields accepted by server handler:
- `namespace` (optional)
- `tool_name` (required, accepts alias `toolName`)
- `caller_agent_id` (required, accepts alias `callerAgentId`)
- `invocation_id` (optional, accepts alias `invocationId`)
- `args` (optional JSON value, defaults to `{}`)

## Notes on Contract Precision

The generated OpenAPI currently prioritizes endpoint/method coverage over strict schema typing. Treat this catalog + code as the reliable source for:
- endpoint existence and grouping
- auth requirements
- stream vs replay endpoints
- query parameter behavior

Treat exact JSON object field shapes as evolving unless typed in Rust handler input structs or runtime-core model definitions.
