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

- Workspace authority: durable workspace registration, nullable/revisioned lead authority, workspace-wide turn interruption, workspace-owned agent create/list/get/archive/restore, managed-worktree inventory and guarded reassignment, legacy-authority migration, and operation inspection under `/v2`
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

#### Read-only team hook configuration

`GET /v2/workspaces/{workspace_id}/hooks` is an **operator-authenticated,
read-only** view of versioned user/project hook configuration. An optional
`?event=pre_teammate_add` (or any of the five `post_*` event names) filters the
result. The JSON response includes `config_version`, `execution_enabled`, and
effective entries (`event_name`, normalized `name`, `command`, `timeout_ms`,
`scope`, `fingerprint`). **`execution_enabled` is currently `false`: hooks are not invoked by
workspace operations yet.** This is discovery only, not a feature switch or a
way for agents to run commands.

Configuration files live at `~/.agents/gg/team-hooks.json` for the runtime
operator and `<workspace-root>/.agents/gg/team-hooks.json` for the project.
Both use strict JSON `{ "version": 1, "hooks": { "pre_teammate_add":
[{ "name": "setup", "command": "./scripts/setup.sh", "timeout_ms": 15000 }]
} }` (`version` defaults to 1 if omitted). Missing files mean no hooks; invalid files fail closed. Supported events
are `pre_teammate_add`, `post_teammate_add`, `post_teammate_remove`,
`post_direct_message_send`, `post_broadcast_message_send`, and
`post_compaction`. Project entries override user entries of the same normalized
name *within the same event*; order is deterministic. This configuration does
not grant model tools any shell-execution or caller-identity capability.

To grant an exact resolved hook, the authenticated runtime operator can send
`PUT /v2/workspaces/{workspace_id}/hooks/approval` with
`{"expected_revision":0,"hooks":[{"event_name":"pre_teammate_add","name":"setup","fingerprint":"sha256:<exact hash from GET /hooks>"}]}`.
The current revision and approved command snapshot are returned by
`GET /v2/workspaces/{workspace_id}/hooks/approval`. A stale revision fails with
`409`. Submitting `"hooks":[]` revokes every grant, even when a project config
has since become invalid. Commands and timeouts come from the effective
configuration and cannot be supplied directly in the approval request. The
fingerprint binds the observed command, timeout, event, name, and origin scope;
an edit between discovery and approval is rejected with `409`.
An agent editing its project's configuration therefore cannot add approved
commands. Grants are workspace-local, and an admitted but unclaimed effect
cannot begin once its grant is revoked.

Hook effect intents, single-execution claims, exact terminal receipts, and
ambiguous-start `manual_review` classifications are persisted separately.
`GET /v2/workspaces/{workspace_id}/hooks/effects?limit=50` returns bounded
operator diagnostics (maximum 100 records). **Hook invocation and model context
injection are not yet active, even when an operator approves hooks.**

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
    "title":"Builder",
    "onboarding":{"prompt":"Implement the API endpoint","image_paths":[]}
  }' \
  "$BASE_URL/v2/workspaces/$WORKSPACE_ID/agents")

AGENT_ID=$(echo "$AGENT_RESULT" | jq -r '.agent_id')
```

The runtime uses the opaque `sess_*` value as the durable public agent ID and allocates a separate friendly alias for display and human references. The alias does **not** replace provider-native session references. Creation commits the session, immutable workspace ownership, workspace profile, active roster membership, recreation policy, **and a deterministic system onboarding notice** atomically; there is no normal `/v2/agents` unowned-creation route and no separate join step. The notice is created even without `onboarding` input. It includes the workspace name, new agent alias/title, and either the current lead alias or an explicit leadless statement, followed by any optional task prompt, native ordered images, and supported Codex skill invocation. The notice is an `automation_context` turn, never a forged user message or a lead-authored DM. ACP image attachments and unsupported skill formats are rejected before creation.

The creation response confirms the **queued intent**, not delivery. The runtime injects when the recipient is idle; busy recipients remain pending. A direct user turn before mandatory onboarding has reached the provider returns `409 Conflict`. Pending message deliveries are **deferred**, and pending process completions remain queued, rather than being marked as terminal provider failures; neither may overtake onboarding. Only the worker's exact claimed onboarding automation input may pass this admission fence. Dispatch outcomes are tracked against a stable correlation ID. Unknown provider outcomes require review, never automatic redelivery. Inspect with `GET /v2/workspaces/{workspace_id}/agents/{agent_id}/onboarding`; `state` is `pending`, `injecting`, `delivered`, `manual_review`, or `cancelled`, and `turn_id` is present only after admission/observation. The runtime retries safe pending work after restart. A notice cancelled before any turn admission because its recipient was archived is safely requeued with its original identity on restore; ambiguous or already-delivered onboarding is never resubmitted.

The recreation policy is durable and is reused for startup recovery, lazy provider resume, and explicit restore. It currently includes provider, selected model, immutable launch policy (permission intent, setting-source intent, system prompt, allowed/disallowed tools, authoritative cwd, and harness-version slot), plus mutable current preferences such as thinking effort. Provider-native session identity is persisted separately from that recreation policy.

Read the roster or one agent:

```bash
curl -fsS -H "Authorization: Bearer $TOKEN" \
  "$BASE_URL/v2/workspaces/$WORKSPACE_ID/agents"

curl -fsS -H "Authorization: Bearer $TOKEN" \
  "$BASE_URL/v2/workspaces/$WORKSPACE_ID/agents/$AGENT_ID"

curl -fsS -H "Authorization: Bearer $TOKEN" \
  "$BASE_URL/v2/workspaces/$WORKSPACE_ID/agents/$AGENT_ID/onboarding"
```

The list defaults to active agents. Use `?lifecycle=archived` for archived history or `?lifecycle=all` for both states.

### Provider observations and mutable session controls

After provider turns, Gooselake attempts to persist the provider-reported context window, token usage and remaining percentage. `GET /v2/agents/{agent_id}/context-limit` returns the current observation or JSON `null` if none exists for the current native attachment. Snapshots carry the provider native reference, optional canonical reference, agent revision, observation time, and source turn. A late observation cannot overwrite newer evidence or survive a rebind, reattachment, or revisioned policy update. `POST /v2/agents/{agent_id}/context-limit/refresh` requests fresh evidence; it never estimates usage from transcript text.

Use the current agent `revision` from `GET /v2/workspaces/{workspace_id}/agents/{agent_id}` when updating thinking preference or Claude permission intent. Both public mutations require `expected_revision`, return the new revision, and reject stale revisions with HTTP `409`. Changes are rejected during an active turn. Codex and Claude thinking preferences carry into later turns/resume; ACP retains its own preferences and does not support these mutations.

```bash
curl -fsS -H "Authorization: Bearer $TOKEN" \
  "$BASE_URL/v2/agents/$AGENT_ID/context-limit"

curl -fsS -X POST -H "Authorization: Bearer $TOKEN" \
  -H 'Content-Type: application/json' \
  -d '{"expected_revision":0,"current_preferences":{"thinking_effort":"high"}}' \
  "$BASE_URL/v2/agents/$AGENT_ID/preferences"

curl -fsS -X POST -H "Authorization: Bearer $TOKEN" \
  -H 'Content-Type: application/json' \
  -d '{"expected_revision":1,"permission_intent":{"kind":"inherit_provider_configuration"}}' \
  "$BASE_URL/v2/agents/$AGENT_ID/permission"
```

The permission endpoint is Claude-only. These controls do not rewrite earlier turns.

`POST /v2/agents/{agent_id}/turns` admits a new user-visible turn, preserving its typed `turn_settings` in the durable dispatch-policy snapshot even if the runtime has to recover an admitted-but-not-dispatched turn after restart. Explicit `thinking_effort` selects and durably updates the agent's revisioned current thinking preference **before turn admission**; subsequent turns inherit that preference until it is changed again. Omit it to use the existing preference without a revision change. Codex also accepts `codex_collaboration_mode: "default" | "plan"` on a specific turn, without turning that selection into permission or a persistent session default. Claude accepts thinking changes without Codex collaboration. Unsupported ACP preferences or cross-provider collaboration modes fail before turn admission.

```bash
curl -fsS -X POST -H "Authorization: Bearer $TOKEN" \
  -H 'Content-Type: application/json' \
  -d '{"input":[{"type":"text","text":"Review this change"}],"turn_settings":{"thinking_effort":"high","codex_collaboration_mode":"plan"}}' \
  "$BASE_URL/v2/agents/$AGENT_ID/turns"
```

`POST /v2/agents/{agent_id}/compact` requires a ready, active workspace-owned Codex or Claude agent. It durably admits a manager-owned `cmp_*` run before invoking the native provider, with optional `Idempotency-Key`. The result distinguishes `accepted` (native request accepted, **not yet confirmed complete**) from `not_performed`; an unknown provider outcome becomes `manual_review` and is never automatically resubmitted. `GET /v2/agents/{agent_id}/compactions/{run_id}` reports the exact durable operation. Provider completion events settle accepted runs to `completed`/`failed`. Until an outstanding or ambiguous run is settled, new turns, permission updates, and worktree rebinds are fenced. A crash does not erase or replay unresolved compaction. If evidence is lost, an operator can independently inspect native provider state and settle an ambiguous `manual_review` via `POST /v2/agents/{agent_id}/compactions/{run_id}/resolve` with JSON `{"phase":"failed","evidence":"..."}` or a verified `completed` result. This records external review evidence and never repeats native compaction; confirming completion requires the same native attachment/revision. The broader automatic/provider compaction and notice/hook unification remains a separate lifecycle expansion.

`POST /v2/agents/{agent_id}/turns/{turn_id}/edit-rerun` is the manager-owned historical edit path. It requires an idle workspace agent, a **completed user-visible** target turn with persisted input/native turn identity, and a provider advertising verified hard-fork support (Codex and Claude advertise this capability; ACP does not). Its body is `{"edited_snapshot":{"prompt_text":"Updated request","image_refs":[],"invocation":null}}`; optional `Idempotency-Key` makes exact retries return the original operation and rejects a different edit under the same key. The manager rebuilds native structured inputs from the edited snapshot, including ordered persisted image paths and a **Codex-only** skill invocation when supplied, rejecting missing files and unsupported invocation types. It records an `edit_*` operation **before** provider fork, verifies the provider-native child, atomically commits the child attachment, retained transcript prefix and superseded-turn lineage, then durably admits a **new logical user turn** with operation correlation. Previous turns remain durably available for audit; no source turn is overwritten. `completed` on the edit operation means the replacement turn was admitted, not that model work finished. The new turn's own state and provider dispatch outcome remain canonical.

`GET /v2/agents/{agent_id}/edits/{operation_id}` returns the durable operation, child identity and replacement turn (if any). A crash or uncertain provider fork is `manual_review`, **never automatically retried**. While an edit remains unresolved, ordinary new turns, native compaction and worktree reassignment are blocked. An operator who has independently established a verified child attachment with no admitted replacement turn can record evidence at `POST /v2/agents/{agent_id}/edits/{operation_id}/resolve` using `{"evidence":"Provider child verified, no replacement admitted"}`; this settles the operation as failed without repeating the native effect. Unknown native child identity cannot be dismissed by an unverified assertion and remains fenced for explicit external recovery. The provider-native edit path is capability-gated; unsupported providers do not silently emulate it. **A provider conversation fork does not undo filesystem writes, commands or external tool side effects from superseded turns.**

### Workspace worktree routes

`GET /v2/workspaces/{workspace_id}/worktrees` lists persisted managed worktrees for the workspace's canonical Git repository. Each item includes its logical ID, monotonically increasing persisted-record `revision`, name, branch, path, retention policy, active attached agent IDs, and `eligible_for_assignment` plus explicit `eligibility_blockers`. The response includes the canonical repository root, Git common directory, and repository fingerprint. A stale/missing path, mismatched native branch/repository, or conflicting claim is **not** considered eligible. The worktree revision changes on persisted record updates; it is independent of the workspace-agent revision and Git commit.

To create or reuse a named managed Git checkout without needing an existing source agent, use the workspace-scoped endpoint:

```bash
curl -fsS -X POST \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"worktree_name":"feature-search","branch_prefix":"gg","run_init_script":false}' \
  "$BASE_URL/v2/workspaces/$WORKSPACE_ID/worktrees"
```

Its response includes a durable worktree ID, `created`, and init-script status. Repeating the same repository, name and branch selection converges on the existing managed worktree. Worktree names must be Git-safe slugs. Managed worktrees are kept when their retention policy says to retain; no external/unmanaged checkout is implicitly deleted. If an init script fails after creating local files, cleanup will not force-delete them. A later request can recover that exact verified Git checkout without repeating the failed initialization.

When creating an agent, choose one of these `worktree` selectors in the `POST /v2/workspaces/{workspace_id}/agents` JSON request:

```json
{"provider":"codex","worktree":{"mode":"root"}}
{"provider":"codex","worktree":{"mode":"existing","worktree_id":"wt_existing"}}
{"provider":"codex","worktree":{"mode":"new","worktree_name":"feature-search","run_init_script":false}}
```

The `new` selector creates or reuses the managed checkout before creating the provider session. The provider cwd, workspace-agent recreation policy, session worktree ID, and owned managed-worktree claim are persisted consistently. `cwd` must not also be supplied with an explicit worktree selector. An agent-create failure may leave an unclaimed, retained checkout for later inspection and reuse rather than destructively rolling it back.

Operators can rebind an **idle**, active workspace agent to an already registered eligible managed worktree, or back to the workspace root (null worktree ID):

```bash
curl -fsS -X POST \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -H "Idempotency-Key: move-builder-linked" \
  -d '{"worktree_id":"wt_existing","expected_revision":0}' \
  "$BASE_URL/v2/workspaces/$WORKSPACE_ID/agents/$AGENT_ID/worktree"
```

`expected_revision` is the current workspace-agent revision, not the workspace revision. The agent's public identity and provider session identity remain stable. The runtime persists a worktree-rebind intent **before** calling a provider, and only after provider-native cwd/session evidence is verified does one SQLite transaction update the session cwd, workspace-agent recreation policy/revision, and managed-worktree associations. A repeat with the same `Idempotency-Key` and identical input returns the original completed operation without calling the provider again; conflicting input or revision fails with HTTP `409`.

Response fields include `operation` (ID, source/destination routes, `phase`, native evidence) and the updated `agent`. Inspect a durable in-flight or recovery-required operation with `GET /v2/workspaces/{workspace_id}/agents/{agent_id}/rebinds/{operation_id}`, or inspect its shared operation journal, resource claim, and transitions through `GET /v2/operations/{operation_id}`. If native dispatch or binding is ambiguous, its phase becomes `manual_review`: new turns, restores, archival, and further reassignments are blocked rather than replaying unknown provider work. An explicitly unsupported provider (including current ACP rebind) cannot finalize a changed cwd.

Previous managed checkouts are **preserved by default**. To request cleanup *after* verified native rebinding, add `"cleanup_previous_worktree":true` alongside `worktree_id` and `expected_revision`. This choice is part of the idempotency identity: reusing a key with a changed cleanup policy returns a conflict. The provider route changes first; cleanup can never roll back a verified cwd. The durable operation exposes `previous_cleanup_status` (`preserved`, `pending`, `deleted`, `retained_by_policy`, or `skipped_live_claims`) and an optional `previous_cleanup_diagnostic`. A dirty, unmerged, externally managed or unverifiable worktree remains intact and `pending`, never force-deleted. If cleanup is pending after a restart, or after resolving the safety issue, the operator can retry without re-dispatching provider work:

```bash
curl -fsS -X POST \
  -H "Authorization: Bearer $TOKEN" \
  "$BASE_URL/v2/workspaces/$WORKSPACE_ID/agents/$AGENT_ID/rebinds/$OPERATION_ID/cleanup"
```

Only checkout paths under the configured managed root with verified Git identity/branch, no uncommitted changes, and no unmerged branch commits are eligible for automatic deletion. This retry is a separate cleanup action, not a retry of provider rebinding: replaying the original `Idempotency-Key` does not implicitly retry pending native cleanup, even if the checkout has since become clean. Recovery from an ambiguous provider rebind still requires operator investigation and is not implicitly resolved by this endpoint.

Older agent creation requests without a `worktree` selector still accept a `cwd` inside the registered root. The legacy `/v1/worktrees` endpoints remain temporarily available to existing clients; the workspace-scoped endpoint is the preferred creation path. Unresolved provider rebinds still require explicit operator recovery rather than automatic guessing or replay.

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

### Durable workspace system notices

The runtime inserts lifecycle notifications in the same SQLite transaction that commits the corresponding authority: a newly assigned lead receives a lead-onboarding notice (including optional global then workspace `.agents/gg/lead-readme.md` instructions snapshotted at assignment); a former lead receives demotion/leadless context; the active lead can receive member additions, removals and restorations; a member receives a route-change notice only **after** their provider has proved the new working directory; and each actually interrupted agent can receive a workspace interruption notice when the operation is durably complete. A lead is not notified of their own membership changes or member changes they initiated. Leadless workspaces emit no lead-membership notifications. A no-op or idempotently replayed lead assignment does not create a second notice.

These notifications use the same stable `automation_context` turn admission and replay-safe dispatcher as the mandatory new-member onboarding. They are **not** messages impersonating a human user, and a provider dispatch with an unknown outcome is never blindly retried. Archiving a recipient cancels their pending notice; busy or unavailable recipients are deferred rather than force-dispatched. System notice policy changes affect newly admitted notices, not already-queued snapshots.

Operators can inspect and change the per-workspace policy with independent optimistic revision protection:

```bash
curl -fsS -H "Authorization: Bearer $TOKEN" \
  "$BASE_URL/v2/workspaces/$WORKSPACE_ID/notice-policy"

curl -fsS -X PUT \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"expected_revision":0,"policy":{"lead_transitions":true,"membership_to_lead":true,"worktree_reassignment":true,"workspace_interrupt":true}}' \
  "$BASE_URL/v2/workspaces/$WORKSPACE_ID/notice-policy"
```

All four lifecycle flags default to `true`; mandatory onboarding is not suppressible through this policy. The policy revision starts at `0`, increments after each successful change, and mismatched `expected_revision` returns HTTP `409`. It is independent of the workspace lead/roster revision. These operator endpoints require the ordinary `/v2` bearer token; models have no notice-policy mutation tool.

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

Image paths are validated before message admission: at most eight regular readable files, with PNG, JPEG, GIF, or WebP identified from file bytes. Ordering is preserved. The canonical v2 transport sends images as native structured provider input to Codex and Claude sessions. ACP still rejects image-bearing v2 messages explicitly rather than dropping or flattening attachments. Validation errors identify the failing image index/reason without echoing the local path.

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
forwarding it to the provider. A provider can reject an invalid local selection before any
native response is sent; the approval then stays pending so the caller can correct the choice.
For ACP, an explicit `payload.optionId` must match an offered native permission option and agree
with the `decision` (allow vs reject). If the provider decision outcome becomes ambiguous after
the response might have been sent, the turn is left in a recovery-required state instead of
assuming the decision succeeded or retrying it blindly.

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

`mode` is `catalog`, `agent_managed`, or `unsupported`. ACP model discovery is currently `agent_managed`, so its empty model array must not be interpreted as a built-in empty catalog. Codex skill discovery is `catalog` mode and delegates to its app-server `skills/list` method; Claude and ACP skill discovery remain unsupported by the shipping adapters.

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
- ACP provider-origin permission requests are recorded against the admitted logical turn and answered using one of the agent's offered options; invalid requests fail closed
- ACP image/audio/embedded context inputs require negotiated prompt capabilities; unsupported native/local file or skill inputs are rejected before dispatch instead of being flattened

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

Session-owned terminal processes deliver a completion turn back to the owning model through durable turn admission. Completion delivery is tracked separately from terminal process state and survives restart. Models should not poll `gg_process` with `mode=status` waiting for completion; completion is pushed automatically. Queued cancellation atomically prevents launch and intentionally does not create a model completion turn.

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

The `/v1/teams` HTTP routes and historical split MCP tools (`gg_team_status`, `gg_team_message`, `gg_team_manage`) are **operator-only compatibility surfaces** backed by the legacy team services. They remain available during migration but are not advertised to provider models. Current model agents instead use workspace-authoritative `gg_team` and `gg_message` operations through the scoped `/v2/mcp` endpoint or native Codex tools; the canonical workspace agent/message records are separate from legacy `TeamRecord` membership.

## MCP gateway

The **model-facing** scoped MCP API is separate from operator authentication:

- `GET /v2/mcp/capabilities`: authenticated with a per-agent bearer token derived by the runtime; returns active tool names and policy.
- `POST /v2/mcp/invoke`: authenticated with the same agent token. JSON contains `namespace` (optional), `tool_name` (required) and `args` (mode-discriminated JSON). `caller_agent_id`, `workspace_id`, `team_id`, and `invocation_id` are **not accepted** in the request body. Internal call correlation is supplied by the host via `x-gg-tool-invocation-id`.

The runtime resolves the trusted caller from the credential and rejects archived agents, retired workspaces, and closed sessions. The operator bearer token cannot be substituted. Requests are limited to **64 KiB**. The three model tools are `gg_team`, `gg_message`, and `gg_process`, omitted when their corresponding features are disabled. Native Codex sessions invoke these same tools through in-process `item/tool/call` RPCs with host-bound caller and invocation identity; Codex does not use the scoped MCP HTTP endpoint.

```json
{"namespace":"gg_message","tool_name":"gg_message","args":{"mode":"direct","agent_id":"sess_recipient","message":"Hello"}}
```

`gg_team` accepts `status`, `add`, `remove`, `assign_worktree`; `gg_message` accepts `direct` and `broadcast`; `gg_process` accepts `run`, `list`, `status`, `cancel`. Each mode validates its own fields and rejects unrelated/unknown fields. A `gg_team` add commits mandatory system onboarding with an optional prompt and ordered image attachments. The returned `onboarding.state` reports queue/dispatch status, not successful model delivery. Worktree cleanup on removal is not enabled via model tools yet. Processes notify their owner of terminal completion automatically.

The `gg_team` status response includes the optional friendly `lead_alias`, each active member's `alias`, `role`, `title_provenance`, creator provenance, activity and `context_remaining_pct`. The context field is `null` without a current provider-session-bound observation; it is never reconstructed from transcript estimates. Attached worktree name, branch and routing state are included where inventory can prove them. Eligible available worktrees are sorted deterministically and capped at 20, with `available_worktrees_total` and `available_worktrees_truncated` describing the full eligible set. The roster remains queryable when Git inventory is unavailable. Agent-message delivery context uses compact `<agent_msg>` envelopes (`dm|broadcast`, `workspace_team|global_direct`, attachment count), not a raw team-ID-bearing model prompt.

The legacy **operator-authenticated** `GET /v1/mcp/capabilities` and `POST /v1/mcp/invoke` remain temporarily for existing automation. Their previous request format includes `caller_agent_id` and optional `invocation_id`; provider sessions no longer receive operator bearer credentials and use `v2` exclusively.

Tool envelopes are compact and consistent:

```json
{"ok":true,"result":{}}
```

```json
{"ok":false,"error":{"code":"bad_request","message":"...","retryable":false}}
```

`gg_team` mutations first record a durable intent keyed to the trusted caller and invocation ID, then persist the exact successful response. Exact replays return the receipt; conflicting payloads return `conflict`. After a crash or ambiguous failure, an unreceipted operation returns non-retryable `outcome_unresolved` rather than silently applying the change twice. All ambiguous mutation transport failures use `outcome_unresolved` and must not be blindly retried. See [MCP and Sidecars](/docs/mcp-and-sidecars) for the provider wiring.

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
