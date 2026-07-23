---
title: "API guide"
description: "Use the Gooselake HTTP and SSE API for sessions, turns, events, approvals, providers, processes, worktrees, teams, diagnostics, and MCP gateway calls."
order: 22
category: "Client Builders"
summary: "The human-facing API guide for building clients on top of the runtime."
---

This document is the human-facing guide to the runtime HTTP/SSE API.

Sources of truth:

- generated artifact: [`openapi/runtime-server-openapi.yaml`](https://github.com/amxv/gooselake/blob/main/openapi/runtime-server-openapi.yaml)
- route + handler code: [`crates/runtime-server/src/http/`](https://github.com/amxv/gooselake/blob/main/crates/runtime-server/src/http)
- OpenAPI generator: [`crates/runtime-server/src/openapi.rs`](https://github.com/amxv/gooselake/blob/main/crates/runtime-server/src/openapi.rs)
- shared runtime structs: [`crates/runtime-core/src`](https://github.com/amxv/gooselake/blob/main/crates/runtime-core/src)

If this guide disagrees with runtime behavior, treat server/core code as authoritative.

## Quick API start

```bash
BASE_URL="http://127.0.0.1:8080"
TOKEN="<runtime-bearer-token>"

curl -fsS "$BASE_URL/health"
curl -fsS -H "Authorization: Bearer $TOKEN" "$BASE_URL/v1/health"
```

Public routes:

- `GET /health`
- `GET /openapi.yaml`

Protected routes:

- all `/v1/**` routes require `Authorization: Bearer <token>`

Auth failure returns HTTP `401` with:

```json
{"error":"missing or invalid bearer token"}
```

## API surface by group

See [Endpoint Catalog](/docs/endpoint-catalog) for the full route list.

Top-level groups:

- Runtime/meta: health, version, OpenAPI, diagnostics
- Providers/auth: provider list/models plus Codex, Claude, and ACP auth endpoints
- Sessions: create/list/get/resume/close, turns, approvals, event replay/stream
- Global events: replay and stream
- Teams/comms: team lifecycle, spawn, messages, deliveries, retries, snapshots, interrupts
- Processes: run/list/get/logs/kill, replay and stream process events
- Worktrees: create/list/get/claim/release/cleanup
- MCP gateway: capabilities and invoke

## Request/response precision

The generated OpenAPI currently prioritizes route/method/content-type coverage. Many request and response bodies are represented as broad `JsonObject` schemas.

For exact JSON fields, use:

- handler input structs in `crates/runtime-server/src/http/`
- shared input/output structs in `crates/runtime-core/src/runtime.rs` and `crates/runtime-core/src/services.rs`
- durable record structs in `crates/runtime-core/src/state.rs`

## Sessions

Create a provider-backed runtime session:

```bash
SESSION_JSON=$(curl -fsS -X POST \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "provider":"codex",
    "model":"gpt-5.4-mini",
    "cwd":"/workspace/repo",
    "permission_mode":"default",
    "metadata":{"purpose":"docs smoke"}
  }' \
  "$BASE_URL/v1/sessions")

SESSION_ID=$(echo "$SESSION_JSON" | jq -r '.id')
```

`POST /v1/sessions` accepts:

| Field | Required | Notes |
| --- | --- | --- |
| `provider` | yes | `codex`, `claude`, or `acp`. |
| `model` | no | Provider-specific model ID. ACP can ignore global model catalogs. |
| `cwd` | no | Working directory for provider session. |
| `permission_mode` | no | Passed to providers; `require_approval` enables runtime approval gating where supported. |
| `metadata` | no | Arbitrary JSON object/value stored with the session. |

Send a turn:

```bash
TURN_JSON=$(curl -fsS -X POST \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"input":[{"type":"text","text":"Run ls and summarize the repo."}]}' \
  "$BASE_URL/v1/sessions/$SESSION_ID/turns")
```

`POST /v1/sessions/{session_id}/turns` accepts:

| Field | Required | Notes |
| --- | --- | --- |
| `input` | yes | Array of provider input objects. Text objects use `{ "type": "text", "text": "..." }`. |
| `expected_turn_id` | no | Optional client-side concurrency guard. |
| `permission_mode` | no | Optional per-turn permission override. |

The response is an accepted turn, not necessarily terminal output:

```json
{"session_id":"...","turn_id":"...","status":"accepted"}
```

Interrupt a turn:

```bash
curl -fsS -X POST -H "Authorization: Bearer $TOKEN" \
  "$BASE_URL/v1/sessions/$SESSION_ID/turns/$TURN_ID/interrupt"
```

Close a session:

```bash
curl -fsS -X POST \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"reason":"done"}' \
  "$BASE_URL/v1/sessions/$SESSION_ID/close"
```

## Approvals

When runtime approval gating is active, a pending approval is stored and emitted in the event stream. Respond with:

```bash
curl -fsS -X POST \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"decision":"accept"}' \
  "$BASE_URL/v1/sessions/$SESSION_ID/approvals/$APPROVAL_ID"
```

Accepted decision values:

- `accept`
- `accepted`
- `decline`
- `declined`
- `reject`
- `rejected`

The runtime normalizes to provider approval behavior when the provider supports it.

## SSE and replay model

SSE endpoints:

- `GET /v1/events/stream`
- `GET /v1/sessions/{session_id}/events/stream`
- `GET /v1/teams/{team_id}/events/stream`
- `GET /v1/processes/{process_id}/events/stream`

Replay endpoints:

- `GET /v1/events`
- `GET /v1/sessions/{session_id}/events`
- `GET /v1/teams/{team_id}/events`
- `GET /v1/processes/{process_id}/events`

Behavior:

- stream endpoints replay first, then deliver live events
- session/process streams subscribe before replay handoff to reduce missed events
- `after_seq` query param takes precedence
- otherwise stream endpoints use the `Last-Event-ID` header
- invalid `Last-Event-ID` returns HTTP `400`
- keepalive pings are sent every 10 seconds

SSE event envelope:

- `id`: runtime sequence id
- `event`: runtime event kind
- `data`: JSON-serialized `RuntimeEventRecord`

Example:

```bash
curl -N -H "Authorization: Bearer $TOKEN" \
  "$BASE_URL/v1/sessions/$SESSION_ID/events/stream?after_seq=0"
```

## Providers

Provider IDs:

- `codex`
- `claude`
- `acp`

Model catalogs:

- Codex: `gpt-5.6-sol`, `gpt-5.6-terra`, `gpt-5.6-luna`, `gpt-5.5`, `gpt-5.4`, `gpt-5.4-mini`, `gpt-5.3-codex-spark`
- Claude: `claude-sonnet-5`, `claude-opus-4-8`, `claude-fable-5`, `claude-haiku-4-5`
- ACP: can return an empty list because model selection can be session-scoped inside the configured agent

`GET /v1/providers/{provider}/models` returns `id`, `display_name`, and
provider-owned `reasoning_levels` for each model. Clients should use
`reasoning_levels` to populate reasoning-effort controls. The list can be empty
when a model does not expose a global selector.

Auth status examples:

```bash
curl -fsS -H "Authorization: Bearer $TOKEN" "$BASE_URL/v1/providers"
curl -fsS -H "Authorization: Bearer $TOKEN" "$BASE_URL/v1/providers/codex/auth/status"
curl -fsS -H "Authorization: Bearer $TOKEN" "$BASE_URL/v1/providers/claude/auth/status"
curl -fsS -H "Authorization: Bearer $TOKEN" "$BASE_URL/v1/providers/acp/auth/status"
```

ACP v1 notes:

- only `GET /v1/providers/acp/auth/status` exists for ACP auth
- ACP auth is agent-managed
- no ACP logout/API-key/import routes exist in v1
- ACP permission requests fail the active turn clearly

See [Provider Guide](/docs/providers) for full provider setup.

## Processes

Start a process:

```bash
PROCESS_JSON=$(curl -fsS -X POST \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"command":"echo hello","cwd":"/tmp","timeout_ms":30000}' \
  "$BASE_URL/v1/processes")
```

`POST /v1/processes` accepts:

| Field | Required | Notes |
| --- | --- | --- |
| `command` | yes | Command string. Shell behavior depends on `[processes].allow_shell`. |
| `cwd` | no | Working directory. |
| `timeout_ms` | no | Overrides configured default timeout. |
| `session_id` | no | Associates process ownership with a session. |

Read logs:

```bash
curl -fsS -H "Authorization: Bearer $TOKEN" \
  "$BASE_URL/v1/processes/$PROCESS_ID/logs?stream=stdout&tail_lines=100&max_bytes=65536"
```

Kill:

```bash
curl -fsS -X POST \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"reason":"operator stop"}' \
  "$BASE_URL/v1/processes/$PROCESS_ID/kill"
```

## Worktrees

Create:

```bash
curl -fsS -X POST \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "source_session_id":"'"$SESSION_ID"'",
    "repo_root":"/workspace/repo",
    "worktree_name":"feature-docs",
    "branch_prefix":"gg",
    "base_ref":"main",
    "run_init_script":false
  }' \
  "$BASE_URL/v1/worktrees"
```

Important request fields:

| Field | Notes |
| --- | --- |
| `source_session_id` | Session used as source/owner context. |
| `repo_root` | Optional repo root; implementation can infer from session cwd when available. |
| `worktree_name` | Stable human-readable worktree identity. |
| `branch_prefix` | Optional generated branch prefix. |
| `base_ref` | Optional base branch/ref. |
| `deletion_policy` | Optional cleanup policy override. |
| `run_init_script` | Whether to run configured init script. |
| `team_id` / `operation_id` | Optional team/spawn traceability fields. |

Claims, release, and cleanup are separate endpoints so ownership can be represented explicitly.

## Teams and comms

Create a team:

```bash
TEAM_JSON=$(curl -fsS -X POST \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"name":"Implementation Team","lead_agent_id":"'"$SESSION_ID"'","member_agent_ids":["'"$SESSION_ID"'"]}' \
  "$BASE_URL/v1/teams")
```

Send direct message:

```bash
curl -fsS -X POST \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "sender_agent_id":"'"$SESSION_ID"'",
    "recipient_agent_id":"'"$OTHER_SESSION_ID"'",
    "input":{"type":"text","text":"Review this patch."},
    "priority":"normal",
    "policy":"non_interrupting"
  }' \
  "$BASE_URL/v1/teams/$TEAM_ID/messages"
```

Send broadcast:

```bash
curl -fsS -X POST \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "sender_agent_id":"'"$SESSION_ID"'",
    "input":{"type":"text","text":"Status update?"},
    "include_sender":false
  }' \
  "$BASE_URL/v1/teams/$TEAM_ID/broadcasts"
```

Snapshot:

```bash
curl -fsS -H "Authorization: Bearer $TOKEN" \
  "$BASE_URL/v1/teams/$TEAM_ID/view?include_delivery_map=true&message_limit=50"
```

Team delivery fields make multi-agent coordination inspectable: `pending`, `deferred`, `injecting`, `injected`, `failed`, or `cancelled` states are stored as records and replayed through team events.

HTTP routes and MCP team tools use the same underlying team services. A team created through `POST /v1/teams` is immediately visible to `gg_team_status`; messages sent through `gg_team_message` create the same message, delivery, and event records as `POST /v1/teams/{team_id}/messages` and `POST /v1/teams/{team_id}/broadcasts`; members added or removed through `gg_team_manage` use the same spawn, join, remove, worktree assignment, and cleanup paths as the HTTP team/member endpoints.

## MCP gateway

Routes:

- `GET /v1/mcp/capabilities`
- `POST /v1/mcp/invoke`

Important constraints:

- MCP request body limit is `64 KiB`.
- `tool_name` is required, also accepts camelCase `toolName`.
- `caller_agent_id` is required, also accepts camelCase `callerAgentId`.
- `invocation_id` is optional, also accepts camelCase `invocationId`.
- closed/failed caller sessions are rejected with `400`.
- `namespace`, when present, must match the tool prefix. `gg_process` accepts `gg_process_*`; `gg_team` accepts `gg_team_*`.

`GET /v1/mcp/capabilities` reports the enabled GG tool namespaces and tool names:

- `gg_process`: `gg_process_run`, `gg_process_status`, `gg_process_logs`, `gg_process_kill`
- `gg_team`: `gg_team_status`, `gg_team_message`, `gg_team_manage`

Team MCP tools advertise under `gg_team` when the runtime team MCP policy is enabled. The same response includes `ggTeamManagePermissions` so agents can see whether non-lead members may add or remove team members through MCP, and `ggTeamModelPresets` so agents can discover user-friendly `model_preset` names for `gg_team_manage` add mode. If team MCP is disabled, team tools are omitted from capabilities and direct `gg_team_*` invocations return an `ok:false` envelope with `feature_disabled`.

`gg_team_status` returns a team/member snapshot for an active team member. Member rows include activity state, last team-message context, managed-worktree metadata, `added_by`, and `context_window_remaining_percentage`. The percentage is derived from persisted provider usage when usage includes a context-window size and token counts. Codex and Claude sessions can report it after completed turns with usage; ACP remains `null` unless the configured ACP agent emits compatible usage data.

`gg_team_message` sends direct messages or broadcasts by setting `recipient_agent_id` to a member id or `"broadcast"`. Optional `image_paths` are stored with the message and delivered as image input items for supported providers. `gg_team_manage` adds one member when `remove_agent_ids` is absent, and removes one or more members when `remove_agent_ids` is present. Add mode accepts optional `model_preset` and `image_paths`; selected presets set the spawned session provider/model and metadata, and add-mode images are attached to the canonical onboarding message. ACP image attachments are not modeled by this runtime yet; team MCP calls that would send `image_paths` to ACP sessions return an `unsupported_provider_images` error instead of dropping the attachment.

Agent-initiated membership management is configurable in runtime config:

```toml
[teams]
enabled = true
non_lead_can_add_members = false
non_lead_can_remove_members = false

[[teams.model_presets]]
name = "fast"
provider = "codex"
model = "gpt-5.4-mini"
thinking_effort = "low"
```

The lead can add and remove members by default. Non-lead members can use `gg_team_manage` add/remove only when the matching flag is enabled. This policy gates MCP-initiated membership control; authenticated HTTP team administration remains the human/client control plane.

Codex, Claude, and ACP provider sessions all receive the bundled `gg-mcp-server` configuration when enabled. The sidecar forwards provider tool calls to this gateway, so `gg_process` and `gg_team` behavior is provider-agnostic and uses the same success/error envelope across providers:

```json
{"ok":true,"result":{}}
```

```json
{"ok":false,"error":{"code":"unauthorized","message":"..."}}
```

Example:

```bash
curl -fsS -X POST \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "namespace":"gg_process",
    "tool_name":"gg_process_run",
    "caller_agent_id":"'"$SESSION_ID"'",
    "args":{"command":"echo from mcp"}
  }' \
  "$BASE_URL/v1/mcp/invoke"
```

See [MCP and Sidecars](/docs/mcp-and-sidecars) for sidecar details.

## Error mapping

Common behavior:

- validation errors: HTTP `400`, body `{"error":"..."}`
- auth failures: HTTP `401`, body `{"error":"missing or invalid bearer token"}`
- not found / unknown entities: HTTP `404`, body `{"error":"..."}`
- internal/io/bootstrap errors: HTTP `500`, body `{"error":"..."}`

Special status codes:

- `POST /v1/sessions/{session_id}/turns/{turn_id}/interrupt` returns `202 Accepted`
- `DELETE /v1/teams/{team_id}` returns `204 No Content`

## OpenAPI generation and sync

Regenerate artifact:

```bash
make api-docs-refresh
```

Review sync-relevant file changes:

```bash
make api-docs-status
```

Fail fast when API files changed without docs:

```bash
make api-docs-check
```

Workflow reference: [API Doc Sync Workflow](/docs/api-doc-sync)
