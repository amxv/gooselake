---
title: "Endpoint catalog"
description: "Browse the implemented runtime HTTP and SSE route surface, including public routes, bearer-protected routes, and provider-specific endpoints."
order: 70
category: "Reference"
summary: "A route-by-route catalog derived from the current server implementation."
---

This catalog tracks the currently implemented Gooselake runtime HTTP/SSE surface
and the initial Goosetower browser gateway surface.

Source references:
- [`crates/runtime-server/src/http/`](https://github.com/amxv/gooselake/blob/main/crates/runtime-server/src/http)
- [`crates/goosetower/src/http/mod.rs`](https://github.com/amxv/gooselake/blob/main/crates/goosetower/src/http/mod.rs)
- [`crates/goosetower/src/gateway/mod.rs`](https://github.com/amxv/gooselake/blob/main/crates/goosetower/src/gateway/mod.rs)
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
- `GET /v1/bootstrap` (Bearer; coherent current records + runtime-issued
  `source_epoch` + global `high_watermark`)

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
- `GET /v1/providers/{provider}/models` returns `reasoning_levels` on each
  model when that provider/model exposes a reasoning or effort selector.
  Clients should treat this list as dynamic and provider-owned.
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

Bootstrap contract notes:

- `high_watermark` is `0` for an empty source and otherwise the greatest global
  runtime event row visible in the same SQLite snapshot as `records`.
- `source_epoch` belongs to the runtime database generation, not Goosetower
  configuration.
- Old or unreachable runtimes are unavailable for continuity-sensitive
  materialization; clients must not substitute a static epoch.
- Participating bootstrap tables are capped at 10,000 rows each; overflow is an
  explicit error, never a partial snapshot.

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

## Goosetower Browser Gateway

These routes are served by `gg-goosetower`, not `gg-runtime-server`.

- `GET /health` (Public)
- `GET /v1/health` (Bearer, Goosetower API token)
- `GET /v1/sources` (Bearer, Goosetower API token)
- `GET /v1/metrics` (Bearer, Goosetower API token)
- `GET /v1/debug/protocol` (Bearer, Goosetower API token, requires `debug.endpoints_enabled = true`)
- `GET /v1/debug/sources` (Bearer, Goosetower API token, requires `debug.endpoints_enabled = true`)
- `GET /v1/debug/subscriptions` (Bearer, Goosetower API token, requires `debug.endpoints_enabled = true`)
- `GET /v1/debug/materializer` (Bearer, Goosetower API token, requires `debug.endpoints_enabled = true`)
- `GET /v1/debug/audit` (Bearer, Goosetower API token, requires `debug.endpoints_enabled = true`)
- `POST /v1/dev/tickets` (Bearer, Goosetower API token, dev-only when `debug.endpoints_enabled = true`)
- `GET /v1/realtime?ticket={ticket}` (WebSocket upgrade, exact `Origin` allowlist, signed single-use ticket)

Realtime gateway notes:

- `/v1/realtime` accepts binary Protobuf `RealtimeEnvelope` frames from
  `proto/goosetower/v1/realtime.proto`.
- Tickets are short-lived and include issuer, audience, subject, workspace,
  scopes, allowed origins, expiry, issued-at time, and `jti`.
- The WebSocket upgrade rejects missing/invalid/replayed tickets and origins
  outside the exact configured allowlist.
- The server emits `Hello`, responds to `Ping` with `Pong`, enforces configured
  max message size, and supports in-band `AuthRefresh`.
- Subscriptions return snapshots and receive matching patches for board,
  approval inbox, session, team, process tail, ledger, fleet/source health, and
  worktree views.
- Commands return `CommandAccepted`, `CommandRejected`, or `CommandDuplicate`.
  V0 duplicate detection is in-memory with TTL only.
- Command payloads route to the configured Gooselake runtime for send turn,
  resolve approval, interrupt turn, direct/broadcast team message, spawn team
  member, retry/cancel delivery, kill process, and start process.
- `/v1/metrics` exposes in-process operational counters for gateway
  connections, source health, browser RTT, command latency, replay/resume,
  materializer reduce time, outbound lanes, coalescing, drops, and WebSocket
  backpressure state.

## Notes on Contract Precision

The generated OpenAPI currently prioritizes endpoint/method coverage over strict schema typing. Treat this catalog + code as the reliable source for:
- endpoint existence and grouping
- auth requirements
- stream vs replay endpoints
- query parameter behavior

Treat exact JSON object field shapes as evolving unless typed in Rust handler input structs or runtime-core model definitions.
