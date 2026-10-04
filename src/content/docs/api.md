---
title: "API guide"
description: "Use the Gooselake HTTP and SSE API for workspace authority, agent messaging, durable operations, sessions, turns, events, providers, processes, worktrees, diagnostics, and MCP gateway calls."
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
- all `/v2/**` routes require `Authorization: Bearer <token>`

Auth failure returns HTTP `401` with:

```json
{"error":"missing or invalid bearer token"}
```

## API surface by group

See [Endpoint Catalog](/docs/endpoint-catalog) for the full route list.

Top-level groups:

- Workspace authority: durable workspace registration, nullable/revisioned lead authority, workspace-wide turn interruption, workspace-owned agent create/list/get/archive/restore, legacy-authority migration, and operation inspection under `/v2`
- Agent messaging: agent-first direct messages across workspaces plus workspace-local broadcasts, delivery inspection, retry, and cancellation under `/v2`
- Runtime/meta: health, version, OpenAPI, diagnostics
- Providers/auth: legacy provider list/models/auth plus v2 capability, model-discovery, and skill-discovery endpoints
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
- workspace/operation structs in `crates/runtime-core/src/workspace.rs`
- workspace-agent identity/recreation structs in `crates/runtime-core/src/workspace_agent.rs`
- workspace lead, membership-policy, and interrupt structs in `crates/runtime-core/src/workspace_control.rs`
- agent message/delivery structs in `crates/runtime-core/src/agent_comms.rs`

## Workspace authority and durable operations

The `/v2` surface currently starts with workspace identity. A workspace is keyed by its canonical filesystem root, so concurrent or repeated registration of the same root converges on one durable `workspace_id`.

Register a workspace:

```bash
WORKSPACE_RESULT=$(curl -fsS -X POST \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -H "Idempotency-Key: register-my-repo" \
  -d '{
    "canonical_root":"/workspace/repo",
    "display_name":"My Repo"
  }' \
  "$BASE_URL/v2/workspaces")

WORKSPACE_ID=$(echo "$WORKSPACE_RESULT" | jq -r '.workspace.workspace_id')
OPERATION_ID=$(echo "$WORKSPACE_RESULT" | jq -r '.operation_id')
```

`POST /v2/workspaces` accepts:

| Field | Required | Notes |
| --- | --- | --- |
| `canonical_root` | yes | Must resolve to an existing directory. The runtime canonicalizes it before durable admission. |
| `display_name` | no | Defaults to the canonical root's final path component. Re-registering an existing root does not rename it. |

`Idempotency-Key` is optional, but recommended for retried mutations. The key is scoped to the authenticated operator principal. Repeating the same key with the same normalized input replays the exact terminal response, including the original `operation_id`. Reusing the same key with different normalized input returns HTTP `409` and does not mutate workspace state.

Read the durable state:

```bash
curl -fsS -H "Authorization: Bearer $TOKEN" "$BASE_URL/v2/workspaces"
curl -fsS -H "Authorization: Bearer $TOKEN" "$BASE_URL/v2/workspaces/$WORKSPACE_ID"
curl -fsS -H "Authorization: Bearer $TOKEN" "$BASE_URL/v2/operations/$OPERATION_ID"
```

The operation endpoint exposes the durable operation row plus current claims, transition evidence, effect evidence, outbox rows, and delivery receipts. Workspace registration itself is committed in one SQLite transaction with its operation transitions and canonical-root claim/fence bookkeeping, so a failed commit does not leave a half-created workspace or a falsely terminal operation.

Existing session/team/process/worktree APIs remain under `/v1`; `/v2` is introduced incrementally rather than changing `/v1` behavior in place. Canonical agent messaging is also available under `/v2` without requiring a legacy team ID.

### Workspace-owned agents

Normal `/v2` agent creation is always scoped to a registered workspace:

```bash
AGENT_RESULT=$(curl -fsS -X POST \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "provider":"claude",
    "model":"claude-sonnet-5-5",
    "permission_intent":{"kind":"provider_default"},
    "setting_sources_intent":{"kind":"explicit","sources":["user","project","local"]},
    "current_preferences":{},
    "system_prompt":"Work on the repository task.",
    "allowed_tools":["Read","Grep"],
    "disallowed_tools":["WebFetch"],
    "harness_version_slot":"default",
    "title":"Builder"
  }' \
  "$BASE_URL/v2/workspaces/$WORKSPACE_ID/agents")

AGENT_ID=$(echo "$AGENT_RESULT" | jq -r '.agent_id')
```

The runtime uses the opaque `sess_*` value as the durable public agent ID and allocates a separate friendly alias for display and human references. The alias does **not** replace provider-native session references. Creation commits the session, immutable workspace ownership, workspace profile, active roster membership, and the recreation policy atomically; there is no normal `/v2/agents` unowned-creation route and no separate join step.

The recreation policy is durable and is reused for startup recovery, lazy provider resume, and explicit restore. It currently includes provider, selected model, immutable launch policy (permission intent, setting-source intent, system prompt, allowed/disallowed tools, authoritative cwd, and harness-version slot), plus mutable current preferences such as thinking effort. Provider-native session identity is persisted separately from that recreation policy.

Read the roster or one agent:

```bash
curl -fsS -H "Authorization: Bearer $TOKEN" \
  "$BASE_URL/v2/workspaces/$WORKSPACE_ID/agents"

curl -fsS -H "Authorization: Bearer $TOKEN" \
  "$BASE_URL/v2/workspaces/$WORKSPACE_ID/agents/$AGENT_ID"
```

The list defaults to active agents. Use `?lifecycle=archived` for archived history or `?lifecycle=all` for both states.

Archive and restore without changing identity or policy:

```bash
curl -fsS -X POST \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"reason":"paused"}' \
  "$BASE_URL/v2/workspaces/$WORKSPACE_ID/agents/$AGENT_ID/archive"

curl -fsS -X POST \
  -H "Authorization: Bearer $TOKEN" \
  "$BASE_URL/v2/workspaces/$WORKSPACE_ID/agents/$AGENT_ID/restore"
```

An archived agent is removed from the default active roster and its underlying runtime session rejects new turns. History, workspace ownership, alias, recreation policy, and provider identity remain durable. Restore returns the same agent ID and alias and resumes the provider from the stored policy; it does not implicitly assign any future workspace role such as lead.

### Workspace lead and interrupt authority

Every workspace is allowed to be intentionally leadless. `GET /v2/workspaces/{workspace_id}` exposes nullable `lead_agent_id` plus the workspace `revision`; title and title provenance remain properties of the member profile and are never rewritten by lead changes.

Set or reassign the lead with an optimistic revision guard:

```bash
curl -fsS -X POST \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -H "Idempotency-Key: promote-builder" \
  -d "{\"lead_agent_id\":\"$AGENT_ID\",\"expected_revision\":0}" \
  "$BASE_URL/v2/workspaces/$WORKSPACE_ID/lead"
```

Clear leadership without electing a replacement:

```bash
curl -fsS -X POST \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -H "Idempotency-Key: clear-lead" \
  -d '{"lead_agent_id":null,"expected_revision":1}' \
  "$BASE_URL/v2/workspaces/$WORKSPACE_ID/lead"
```

`lead_agent_id`, when non-null, must name an active agent in the same workspace. SQLite enforces that invariant at commit time. A successful change increments the workspace revision; a stale `expected_revision` returns HTTP `409`. Exact retries under the same `Idempotency-Key` replay the original terminal result. Archiving the current lead clears leadership atomically and increments the workspace revision; restoring that agent leaves it as an ordinary active member. There is no oldest-member or other automatic lead election.

The runtime membership authority follows the same workspace policy as Golden Goose: while a workspace is leadless, any active member may add or remove members. Once a lead exists, the lead may manage membership and non-leads are governed independently by the configured `non_lead_can_add_members` and `non_lead_can_remove_members` policy flags. Lead assignment itself remains an operator-only control rather than a model/member action.

Interrupt every currently active turn owned by the workspace roster:

```bash
curl -fsS -X POST \
  -H "Authorization: Bearer $TOKEN" \
  -H "Idempotency-Key: stop-workspace-now" \
  "$BASE_URL/v2/workspaces/$WORKSPACE_ID/interrupt"
```

The response contains the durable `operation_id`, `workspace_id`, and deterministic `interrupted_agent_ids` / `skipped_agent_ids` sets. The runtime snapshots only active workspace members, records per-agent interrupt intent before provider calls, skips idle members, and cannot target a session outside the workspace. An exact idempotency-key retry replays the stored terminal result without issuing another provider interrupt. If a process dies after the provider accepted an interrupt but before the effect was finalized, recovery uses the durable `turn.interrupt_requested` event instead of blindly dispatching a duplicate; genuinely ambiguous effects remain non-terminal for explicit recovery rather than being guessed.

### Migrating legacy workspace authority

The migration surface turns existing `/v1` session/team/worktree evidence into explicit workspace authority without rewriting the legacy rows.

Preview the current classification:

```bash
curl -fsS -X POST \
  -H "Authorization: Bearer $TOKEN" \
  "$BASE_URL/v2/migrations/workspaces/preview"
```

The preview persists a diagnostic row for every legacy session, team, team member, managed worktree, and worktree claim. Each subject is classified as `mapped`, `archived_history`, or `unresolved`. Repository identity comes from canonical Git filesystem evidence, including the Git common directory shared by linked worktrees; display names are never used as authority.

Apply all deterministic results:

```bash
curl -fsS -X POST \
  -H "Authorization: Bearer $TOKEN" \
  -H "Idempotency-Key: migrate-legacy-workspaces" \
  "$BASE_URL/v2/migrations/workspaces/apply"
```

Run preview before apply; apply consumes the persisted preview rather than silently refreshing it. Apply creates durable workspace/session ownership and placeholder workspace-agent profiles only for subjects whose repository authority is proven. `unresolved` subjects receive no guessed ownership and keep `cutover_blocked: true` until they are explicitly resolved. Re-running apply is convergent; an exact `Idempotency-Key` retry replays the original terminal operation result without reclassifying legacy rows as a hidden side effect.

Read migration diagnostics:

```bash
curl -fsS -H "Authorization: Bearer $TOKEN" \
  "$BASE_URL/v2/migrations/workspaces"
```

An operator can explicitly map an unresolved subject to an existing active workspace:

```bash
curl -fsS -X POST \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -H "Idempotency-Key: resolve-session-123" \
  -d "{\"action\":\"map\",\"workspace_id\":\"$WORKSPACE_ID\"}" \
  "$BASE_URL/v2/migrations/workspaces/resolutions/session/session_123"
```

Or explicitly classify an unresolved historical subject as archived by sending `{ "action": "archive", "workspace_id": null }` to the same resolution route. Operator resolutions are persisted and are not overwritten by later preview refreshes.

The migration is additive: existing `/v1` session, team, process, and worktree rows and read routes remain intact throughout preview, apply, and explicit resolution.

### Agent-first messages and deliveries

`POST /v2/messages` creates one canonical message plus its per-recipient delivery rows. Direct messages address an active runtime agent by `recipient_agent_id` and do not require the sender and recipient to share a workspace. When both agents belong to the same workspace, the message records `workspace_team` context; a cross-workspace or migration-compatible direct records `global_direct` context instead of inventing legacy team ownership.

Send a direct message:

```bash
MESSAGE_RESULT=$(curl -fsS -X POST \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -H "Idempotency-Key: ask-agent-once" \
  -d "{
    \"mode\":\"direct\",
    \"sender_agent_id\":\"$AGENT_ID\",
    \"recipient_agent_id\":\"$OTHER_AGENT_ID\",
    \"input\":[{\"type\":\"text\",\"text\":\"Please inspect the failure.\"}],
    \"policy\":\"non_interrupting\"
  }" \
  "$BASE_URL/v2/messages")

MESSAGE_ID=$(echo "$MESSAGE_RESULT" | jq -r '.message.id')
DELIVERY_ID=$(echo "$MESSAGE_RESULT" | jq -r '.deliveries[0].id')
```

Broadcasts derive the sender's current canonical workspace from durable agent ownership; callers do not supply a workspace or team ID. The active roster is snapshotted once at send time, the sender is excluded, and later roster changes do not rewrite historical recipients. A leadless workspace and a workspace with no other active members are both valid broadcast states.

```bash
curl -fsS -X POST \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -H "Idempotency-Key: broadcast-once" \
  -d "{
    \"mode\":\"broadcast\",
    \"sender_agent_id\":\"$AGENT_ID\",
    \"input\":[{\"type\":\"text\",\"text\":\"New constraint: keep the API compatible.\"}]
  }" \
  "$BASE_URL/v2/messages"
```

The create body accepts `mode`, `sender_agent_id`, structured `input`, optional ordered `image_paths`, `priority`, `policy`, and `correlation_id`. Direct mode additionally accepts `recipient_agent_id` and `reply_to_message_id`; broadcast rejects those direct-only fields. `Idempotency-Key` is an HTTP header: an exact retry returns the existing message, while reusing the key with different normalized content returns HTTP `409`.

Image paths are validated before message admission: at most eight regular readable files, with PNG, JPEG, GIF, or WebP identified from file bytes. Ordering is preserved. The canonical v2 transport currently sends images natively to Claude sessions; Codex and ACP v2 message recipients reject image-bearing sends with an explicit unsupported error rather than dropping or flattening attachments. Validation errors identify the failing image index/reason without echoing the local path.

Inspect message history and durable delivery state:

```bash
curl -fsS -H "Authorization: Bearer $TOKEN" \
  "$BASE_URL/v2/messages?workspace_id=$WORKSPACE_ID&limit=100"

curl -fsS -H "Authorization: Bearer $TOKEN" \
  "$BASE_URL/v2/messages/$MESSAGE_ID/deliveries"
```

`GET /v2/messages` supports `workspace_id`, `sender_agent_id`, `cursor`, and bounded `limit` filters. Delivery state is per recipient and remains explicit across `pending`, `deferred`, `injecting`, `injected`, `failed`, and `cancelled` transitions. Retry is allowed from failed/deferred state, and cancellation applies only while all outstanding deliveries are still cancellable:

```bash
curl -fsS -X POST -H "Authorization: Bearer $TOKEN" \
  "$BASE_URL/v2/deliveries/$DELIVERY_ID/retry"

curl -fsS -X POST -H "Authorization: Bearer $TOKEN" \
  "$BASE_URL/v2/messages/$MESSAGE_ID/cancel"
```

Message creation and its recipient snapshot commit atomically. Startup recovery resumes canonical `pending`/`deferred` work, and migrated legacy delivery rows mirror canonical terminal transitions so `/v1` compatibility views cannot inject the same migrated delivery twice.

## Sessions

Create a provider-backed runtime session:

```bash
SESSION_JSON=$(curl -fsS -X POST \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "provider":"codex",
    "model":"gpt-6-astra",
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

The public turn endpoint accepts only those documented fields. Input provenance, durable
user-input snapshots, dispatch state, and provider-native turn identifiers are runtime-owned
authority and cannot be supplied by an API caller. A turn is durably admitted before any
provider dispatch begins. If the provider cannot prove that a failed dispatch never crossed its
execution boundary, the runtime retains the turn for reconciliation instead of blindly sending it
again.

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

Approvals are durable turn-scoped records. They can originate from the runtime's pre-dispatch
policy gate or from a provider after a turn has already been dispatched. Provider-originated
approval records retain their provider approval reference, optional tool-call correlation, and
request payload; the `origin` field distinguishes them from the runtime pre-dispatch gate.

When an approval is pending, it is stored and emitted in the event stream. Respond with:

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

For provider-originated approvals, the runtime durably records the decision as pending before
forwarding it to the provider. If the provider decision outcome becomes ambiguous, the turn is
left in a recovery-required state instead of assuming the decision succeeded or retrying it
blindly.

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

- Codex: `gpt-6-astra`, `gpt-6.1-sol`, `gpt-6-luna`
- Claude: `claude-opus-5-5`, `claude-fable-5-1`, `claude-sonnet-5-5`
- ACP: can return an empty list because model selection can be session-scoped inside the configured agent

`GET /v1/providers/{provider}/models` returns `id`, `display_name`, and
provider-owned `reasoning_levels` for each model. These are raw runtime
capability tokens, such as Codex `xhigh`, and clients should use them directly
when populating reasoning-effort controls. The list can be empty when a model
does not expose a global selector.

The v2 provider surface makes optional behavior explicit:

- `GET /v2/providers/{provider}/capabilities` returns provider capability support plus the tracked harness contract metadata.
- `POST /v2/providers/{provider}/models/discover` accepts optional `cwd`, typed `setting_sources_intent`, `force_refresh`, and `startup_mode` (`cold` or `start_runtime`). The response includes a discovery `mode` plus typed model descriptors.
- `POST /v2/providers/{provider}/skills/discover` accepts optional `cwd`, typed `setting_sources_intent`, and `force_refresh`, and returns a discovery `mode` plus provider-tagged skills.

`mode` is `catalog`, `agent_managed`, or `unsupported`. ACP model discovery is currently `agent_managed`, so its empty model array must not be interpreted as a built-in empty catalog. Skill discovery is currently unsupported by the shipping Codex, Claude, and ACP adapters; later adapters can implement it without changing the client contract.

`setting_sources_intent` is one of `{"kind":"standard"}`,
`{"kind":"explicit","sources":["user","project","local"]}`, or
`{"kind":"isolated"}`. Project/local sources require `cwd`.

Auth status examples:

```bash
curl -fsS -H "Authorization: Bearer $TOKEN" "$BASE_URL/v1/providers"
curl -fsS -H "Authorization: Bearer $TOKEN" "$BASE_URL/v1/providers/codex/auth/status"
curl -fsS -H "Authorization: Bearer $TOKEN" "$BASE_URL/v1/providers/claude/auth/status"
curl -fsS -H "Authorization: Bearer $TOKEN" "$BASE_URL/v1/providers/acp/auth/status"
curl -fsS -H "Authorization: Bearer $TOKEN" "$BASE_URL/v2/providers/codex/capabilities"
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

Admission is persisted before native spawn. The response returns promptly with a stable `process.process_id`; its initial status may be `queued`. Queue wait does not count against `timeout_ms`, which starts when execution actually begins.

Inspect or update the durable scheduler:

```bash
curl -fsS -H "Authorization: Bearer $TOKEN" \
  "$BASE_URL/v1/processes/scheduler"

curl -fsS -X POST \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"max_concurrent":32,"workspace_max_concurrent":{"ws_example":4},"paused":false}' \
  "$BASE_URL/v1/processes/scheduler"
```

Reorder queued work by providing exactly one of `before_process_id` or `after_process_id`:

```bash
curl -fsS -X POST \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"process_id":"proc_a","before_process_id":"proc_b"}' \
  "$BASE_URL/v1/processes/queue/reorder"
```

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

Session-owned terminal processes deliver a completion turn back to the owning model through durable turn admission. Completion delivery is tracked separately from terminal process state and survives restart. MCP callers should not poll `gg_process_status` waiting for completion; completion is pushed automatically. Queued cancellation atomically prevents launch and intentionally does not create a model completion turn.

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
model = "gpt-6-luna"
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
