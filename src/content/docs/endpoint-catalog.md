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

## Workspace Authority + Durable Operations

- `POST /v2/workspaces` (Bearer, JSON; optional `Idempotency-Key` header)
- `GET /v2/workspaces` (Bearer)
- `GET /v2/workspaces/{workspace_id}` (Bearer)
- `POST /v2/workspaces/{workspace_id}/lead` (Bearer, JSON; optional `Idempotency-Key` header)
- `POST /v2/workspaces/{workspace_id}/interrupt` (Bearer; optional `Idempotency-Key` header)
- `POST /v2/workspaces/{workspace_id}/agents` (Bearer, JSON)
- `GET /v2/workspaces/{workspace_id}/agents` (Bearer; optional `lifecycle=active|archived|all`, defaults to `active`)
- `GET /v2/workspaces/{workspace_id}/agents/{agent_id}` (Bearer)
- `POST /v2/workspaces/{workspace_id}/agents/{agent_id}/archive` (Bearer, JSON)
- `POST /v2/workspaces/{workspace_id}/agents/{agent_id}/restore` (Bearer)
- `GET /v2/migrations/workspaces` (Bearer)
- `POST /v2/migrations/workspaces/preview` (Bearer)
- `POST /v2/migrations/workspaces/apply` (Bearer; optional `Idempotency-Key` header)
- `POST /v2/migrations/workspaces/resolutions/{subject_kind}/{subject_id}` (Bearer, JSON; optional `Idempotency-Key` header)
- `GET /v2/operations/{operation_id}` (Bearer)

Workspace registration canonicalizes the requested filesystem root and uses that canonical root as the durable uniqueness key. Repeated or concurrent registrations of the same root converge on one workspace identity. With `Idempotency-Key`, an exact retry replays the original terminal result; a different normalized request under the same key returns HTTP `409` without changing state.

Workspace leadership is nullable and revisioned. `POST /v2/workspaces/{workspace_id}/lead` accepts `lead_agent_id` (`string` or `null`) plus `expected_revision`. Non-null leads must be active members of that exact workspace, enforced by both service validation and SQLite triggers. Set/reassign/clear operations use compare-and-swap revision authority and durable operation identity. Archiving the current lead atomically clears leadership; restore never promotes it again. No workspace or legacy-team code path auto-elects a replacement lead.

Workspace membership authorization is leadless-aware: an active member can manage membership when no lead exists; once a lead is set, non-lead add/remove authority follows the configured add/remove flags. Lead assignment itself is reserved for the operator surface.

`POST /v2/workspaces/{workspace_id}/interrupt` snapshots the authoritative active roster and active turn IDs, records durable per-agent effect intent before provider calls, and returns sorted `interrupted_agent_ids` and `skipped_agent_ids`. Idle members are skipped, agents outside the workspace are never touched, and exact `Idempotency-Key` retries replay the original result without redispatch. Durable effect/event evidence supports restart recovery without blindly duplicating an interrupt whose provider outcome may already have crossed the execution boundary.

Workspace-agent creation is the canonical normal `/v2` agent path. The runtime reuses the opaque `sess_*` session ID as the public agent ID, assigns a globally unique durable friendly alias, and commits the session row, immutable workspace ownership, profile, active roster membership, and exact recreation policy in one SQLite transaction. The recreation policy contains provider, model, permission intent, setting sources, system prompt, allowed/disallowed tools, authoritative cwd, and harness-version slot. Provider-native session references remain separate from the public ID and alias.

Archiving removes the agent from the default active roster and makes its runtime session non-writable while preserving history, alias, immutable workspace ownership, provider identity, and recreation policy. `restore` reuses the same opaque agent ID and alias and resumes the provider with the persisted recreation policy. Archived history is available with `lifecycle=archived`; `lifecycle=all` returns both active and archived records.

The operation resource exposes durable transition, resource-claim, effect, outbox, and receipt evidence for the operation. Completed registration operations have released their live canonical-root claim, while the transition evidence retains the fence generation used for the commit.

Legacy workspace migration preview classifies every persisted legacy authority subject as `mapped`, `archived_history`, or `unresolved`. Deterministic mapping uses canonical repository and worktree evidence rather than display names. Apply consumes the persisted preview, creates workspace/session authority only for proven mappings, leaves unresolved subjects without guessed ownership, and reports `cutover_blocked: true` while any unresolved subject remains. The resolution endpoint lets an operator explicitly map an unresolved subject to an existing active workspace or archive it as history. Legacy `/v1` rows and read routes are not rewritten by this migration.

## Agent Messages + Deliveries

- `POST /v2/messages` (Bearer, JSON; optional `Idempotency-Key` header)
- `GET /v2/messages` (Bearer; optional `workspace_id`, `sender_agent_id`, `cursor`, `limit`)
- `GET /v2/messages/{message_id}/deliveries` (Bearer; optional `recipient_agent_id`)
- `POST /v2/messages/{message_id}/cancel` (Bearer)
- `POST /v2/deliveries/{delivery_id}/retry` (Bearer)

Canonical `/v2` messages are agent-first and do not require a legacy team ID. Direct routing can target any active/routable agent in the runtime: same-workspace direct messages use `workspace_team` context and cross-workspace/migration-compatible directs use `global_direct` context. Workspace broadcasts derive the sender workspace from canonical ownership, snapshot the active roster once, exclude the sender, and never fan out across workspace boundaries.

Message creation commits the canonical message and every recipient delivery atomically. `Idempotency-Key` replays an exact duplicate and returns `409` if the same scoped key is reused with different normalized message input. Delivery status is durable per recipient; failed/deferred deliveries can be retried, pending/deferred messages can be cancelled when every outstanding delivery remains cancellable, and startup recovery resumes pending/deferred canonical work without duplicating migrated legacy delivery injection.

`image_paths` accepts up to eight ordered PNG, JPEG, GIF, or WebP files that must be regular/readable and match a supported byte signature. The current v2 transport delivers them as native structured image input to Claude; Codex and ACP reject image-bearing v2 messages explicitly rather than silently flattening or dropping attachments. Validation errors are path-redacted.

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

Turn creation durably records the logical turn, input provenance/snapshot authority, dispatch
policy, and correlation before provider execution. Provider-native turn IDs remain separate from
the public logical `turn_id`. Approval responses cover both runtime pre-dispatch gates and durable
provider-originated approval requests.

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

Legacy `/v1` teams still require a non-null lead. Removing the current legacy lead is rejected until another lead is explicitly assigned; the runtime no longer promotes the oldest remaining member automatically.

Team query parameters:
- messages: `cursor`, `limit`
- deliveries: `message_id`, `recipient_agent_id`
- view: `message_cursor`, `message_limit`, `include_delivery_map`, `delivery_recipient_filter`
- events replay/stream: `after_seq`, `limit` (+ `Last-Event-ID` fallback for stream)

## Processes

- `POST /v1/processes` (Bearer)
- `GET /v1/processes` (Bearer)
- `GET /v1/processes/scheduler` (Bearer)
- `POST /v1/processes/scheduler` (Bearer)
- `POST /v1/processes/queue/reorder` (Bearer)
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

Process scheduler mutations are durable. `POST /v1/processes/scheduler` accepts optional `max_concurrent`, `workspace_max_concurrent`, `capture_limit_bytes`, `paused`, and `pause_reason`. Queue reorder accepts `process_id` plus exactly one of `before_process_id` or `after_process_id`.

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
