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
- operator `/v2/**` routes require `Authorization: Bearer <token>`; `/v2/mcp/**` uses a separate agent-scoped credential

The operator bearer check requires exactly one `Authorization` header. Two
headers are unauthorized even if one (or both) contains the valid token;
`/v2` returns the typed `unauthorized` envelope and `/v1` keeps its legacy
error shape.

Legacy `/v1` auth failure returns HTTP `401` with:

```json
{"error":"missing or invalid bearer token"}
```

The operator `/v2` API instead uses a machine-readable error envelope for
application errors, authentication failures, malformed JSON/query requests,
and unsupported methods. For example, `401`:

```json
{
  "error": {
    "code": "unauthorized",
    "message": "missing or invalid operator bearer token",
    "retryable": false,
    "details": null
  },
  "request_id": "req_<server-generated-128-bit-hex>"
}
```

Every `/v2` operator response, including success responses, carries
`X-Request-ID`; error responses repeat the same ID in `request_id`. The server
creates these IDs and never trusts an incoming `X-Request-ID` as authority.
Generated OpenAPI now publishes that response header on ordinary 200s,
SSE stream handshakes, 202 recoverable retirement, typed 409 native conflicts,
412 revision conflicts and default operator errors. Keyset
`X-Next-Cursor`, resource `ETag` and durable `X-Operation-ID` coexist
within the same response header map when applicable. The separate
agent-scoped `/v2/mcp` gateway does **not** claim an operator request ID.
When a supported operator mutation creates, updates, or replays a durable
operation, its successful response also carries `X-Operation-ID` matching the
operation identifier in the JSON result. This includes registration, lead
changes, interruption, retirement, rebind and cleanup, migration apply and
resolution, and historical edit/review. A `202` retirement response exposes
the frozen operation ID but **does not** mean native provider settlement is
complete. Use `GET /v2/operations/{operation_id}` for the shared journal;
provider rebinds and historical edits also have their specialized operation
inspection routes. An ordinary unjournaled action must not invent an ID.
Common codes include `invalid_request`, `invalid_state`, `not_found`,
`conflict`, `outcome_unresolved`, `onboarding_pending`, `unsupported_capability`,
`idempotency_not_supported`, and `internal_error`. Provider-dispatch conflicts retain their typed code and
`details.dispatch_outcome`. Do not blindly repeat an effectful request after
an unknown native dispatch outcome; inspect its durable operation/turn state.
The agent-scoped `/v2/mcp` gateway uses its separate credential and model-tool
result contract, not the operator error envelope.

Workspace GET responses also carry a strong `ETag` containing the quoted
numeric `revision`, for example `ETag: "5"`. When setting workspace leadership,
send `If-Match: "5"` alongside JSON `expected_revision: 5`. A mismatched or
malformed precondition returns `412 revision_precondition_failed`; the actual
lead write still performs its authoritative transactional compare-and-set, so
concurrent changes cannot be overwritten by a stale request. The header is
optional for existing callers; `expected_revision` remains mandatory. The
same precondition applies to workspace retirement and to their respective
revisioned hook-approval and notice-policy GET/PUT contracts. Each revisioned
GET and successful PUT returns its resource's own `ETag`; policy revisions are
distinct from the workspace's revision.
`GET /v2/workspaces/{workspace_id}/agents/{agent_id}` likewise returns the
agent's own revision ETag. Agent preference and permission mutations accept
optional matching `If-Match` alongside mandatory `expected_revision`, and
successful mutations return the new agent-policy revision ETag. The service
still verifies the revision transactionally before committing policy changes.

Operator native-effect endpoints without a revision/`expected_revision` CAS
contract reject `If-Match` with nonretryable `400
revision_precondition_not_supported` **before** doing work. This includes
agent creation/archive/restore, managed worktree creation/cleanup, user turn
submission, manual compaction, context refresh and workspace interruption.
Sending a strong ETag does not silently protect an unrelated action.
This also applies to nonrevisioned registration, messaging and delivery
operations, migration preview/apply/resolution, provider model/skill discovery,
historical edit/review, compaction review and retirement reconciliation.
Only routes explicitly documented with a revision/`expected_revision`
contract accept `If-Match`.

`GET /v2/workspaces/{workspace_id}/agents` uses an indexed ascending
`(created_at, agent_id)` keyset, with optional `lifecycle=active|archived|all`
and `limit=1..100`. Supply the exact `X-Next-Cursor` from a previous page.
The continuation must refer to an existing agent in the **same workspace**
and requested lifecycle, with its exact creation timestamp; an unrelated,
stale, or fabricated cursor is rejected with HTTP 400 rather than silently
skipping a portion of the collection.

## API surface by group

See [Endpoint Catalog](/docs/endpoint-catalog) for the full route list.

Top-level groups:

- Operator capability discovery: `GET /v2/capabilities` reports the active
  runtime and API versions, versioned harness/tool contract, enabled model GG
  tools, worktree/process enablement, and *configured* providers' real
  capability flags. An ACP agent-managed catalog is not a static model list.
- Workspace authority: durable workspace registration, nullable/revisioned lead authority, workspace-wide turn interruption, workspace-owned agent create/list/get/archive/restore, managed-worktree inventory and guarded reassignment, legacy-authority migration, and operation inspection under `/v2`
- Agent messaging: agent-first direct messages across workspaces plus workspace-local broadcasts, delivery inspection, retry, and cancellation under `/v2`
- Runtime/meta: health, version, OpenAPI, diagnostics
- Providers/auth: legacy provider list/models/auth plus v2 capability, model-discovery, and skill-discovery endpoints
- Sessions: create/list/get/resume/close, turns, approvals, event replay/stream
- Global events: replay and stream
- Teams/comms: team lifecycle, spawn, messages, deliveries, retries, snapshots, interrupts
- Processes: indexed operator `/v2` history and single-process inspection;
  legacy `/v1` run/list/get/logs/kill and event replay/stream remain available
- Worktrees: create/list/get/claim/release/cleanup
- MCP gateway: capabilities and invoke

## Request/response precision

The generated OpenAPI combines route/method metadata from the **same declarative
Rust registration table as the live Axum router**
(`crates/runtime-server/src/http/route_table.rs`) with **Rust/serde-derived
OpenAPI 3.1 JSON Schemas** for the workspace registration, workspace list/get,
lead and retirement requests/results, retirement preflight, workspace
interruption, durable operation inspection/list, agent message/delivery and
runtime/provider capability/model/skill discovery contracts. Worktree
creation (including its workspace-scoped input), worktree inventory and rebind
request/operation inspection, agent archive input and onboarding notices,
revisioned notice policy, and redacted operator process history/detail now
also use the real Rust DTOs. Operator user-turn submission/acceptance,
nullable provider context snapshots and explicit refresh, durable compaction
status/review, and revisioned current-preference/permission writes are typed
from their actual public handler and runtime contracts. Workspace-agent
creation, list/get, archive/restore, provider-rebind results, operator reviews
of ambiguous create/archive/restore/policy effects, and historical
edit-and-rerun input/operation inspection now use those same Rust-derived
schemas. The migration status/preview/apply/resolution contracts, workspace
retirement manual-review request/receipt, redacted operator event list/full
event detail, effective hook inspection/approval, and operator hook effect
summary/full detail are also generated from their exact Rust DTOs. Scoped
model-facing `POST /v2/mcp/invoke` uses its strict, caller-free handler
request contract; tool result payloads remain intentionally extensible.
These schemas preserve the actual `serde` field names, required properties,
enums, nullability, nested records and unknown-field behavior.
For example, registration and lead/retirement requests reject unknown JSON
fields, while an optional lead may explicitly be `null`. Workspace and
operation lists return arrays; messages return a response object with a
`messages` array and `next_cursor`. Pagination still uses `X-Next-Cursor`
where documented, not an invented response wrapper.

The OpenAPI query catalog now derives parameter names, scalar/enum types,
accepted `limit` bounds and defaults from the real Axum `Query<T>` structs,
not handwritten copies of the handler inputs. Optional query parameters are
absent when omitted; JSON `null` is not a valid URL query value. The keyset
list operations for workspaces, managed worktrees, agents, operations,
processes, operator events, migration status/preview, hook effects and message
deliveries advertise the conditional `X-Next-Cursor` **response header**.
`GET /v2/messages` instead returns `next_cursor` in its JSON body. Event SSE
streams are not keyset lists: they expose `after_seq`, bounded `limit`, and
the optional `Last-Event-ID` header fallback, never `X-Next-Cursor`.

Two Bearer schemes are intentionally distinct in OpenAPI: `bearerAuth` is
the owner/operator token on operator and legacy routes, whereas
`agentScopedBearerAuth` is the runtime-issued per-agent credential for
`/v2/mcp/capabilities` and `/v2/mcp/invoke` only. Model-facing MCP cannot
borrow the operator credential. The invocation endpoint documents the
sidecar-supplied `x-gg-tool-invocation-id` header, with an exact identity
required for mutating tool modes; request JSON cannot impersonate a caller.

The `/v2` operator middleware serializes its error envelope from the same
Rust type used by the OpenAPI `OperatorErrorResponse` schema, including
`error.code`, `error.message`, `error.retryable`, nullable `error.details`,
and `request_id`. Typed `409` and revision-precondition `412` errors use this
schema; other application/framework failures are described by the operator
`default` error response. The model-facing `/v2/mcp` gateway is a separate
authentication and tool-result contract.

Provider discovery request schemas are derived from their actual custom
deserializer, including both canonical tagged `setting_sources_intent`
objects and still-accepted legacy arrays. Model discovery's `startup_mode`
enum and the response's supported/agent-managed/unsupported distinctions
are also typed. Both `/v2/providers/{provider}/models/discover` and
`skills/discover` publish a JSON request body in OpenAPI.

For `POST /v2/agents/{agent_id}/turns`, `input` is the required ordered
provider-native JSON block array; optional `turn_settings` carries typed
`thinking_effort` and per-turn `codex_collaboration_mode`. Internal provenance
fields are deliberately rejected. `GET /v2/agents/{agent_id}/context-limit`
returns either the exact current-provider observation or JSON `null`, never
an inferred estimate. Compaction run responses include their durable phase,
native result (accepted/not-performed where observed), and review metadata;
explicit review accepts only its documented `phase` and `evidence`. Current
preference/permission writes require `expected_revision` and return a revised
typed result. The permission-intent request schema retains the real parser's
canonical tagged object, legacy string and explicit null compatibility; it
does not claim legacy forms have been removed.

For workspace-agent creation, `provider` is required; the optional `worktree`
selector has `mode: root|existing|new` and mode-specific fields. The
`workspace_id` comes from the trusted URL path, not the request body.
Onboarding can contain user prompt, ordered image paths and typed
skill/invocation metadata. List/Get/Archive/Restore return the same durable
agent record shape (a list response is a JSON array, with pagination via
`X-Next-Cursor`). A provider-backed rebind returns a composite of the
durable rebind operation and an optional verified agent; the internal-only
`newly_completed` switch is never part of public JSON. The independent
creation/archive/restore/native-policy review inputs require the exact
evidence and provider identity fields documented in the endpoint catalog.
Historical edit-and-rerun accepts a strict `edited_snapshot` containing
`prompt_text`, persisted `image_refs` and optional skill `invocation`, not
an arbitrary transcript rewrite. Its durable result is inspectable by
operation ID and remains a distinct historical record.

Migration status and preview return the **same diagnostic shape**:
the complete classified totals plus a bounded page of per-subject evidence,
not proof that a later `apply` succeeded. Preview refreshes and persists
planning evidence but does not apply mappings; neither preview nor apply
accepts a JSON request body. Apply is a separate journaled operator mutation,
and a subject resolution body accepts only its typed `action` and optional
`workspace_id`.
The retirement reconcile endpoint requires independent effect/attachment
evidence and returns a durable review receipt; it does not accept prose as a
shortcut to delete outstanding resources. Operator event history returns an
indexed, redacted summary array, while full event payloads require explicit
single-event lookup. Hook approval accepts only revision and fingerprinted
selections, never shell commands; effective hook introspection and detailed
effect inspection are operator-only, not model-facing.

The two deliberately extensible scoped MCP response bodies (`GET
/v2/mcp/capabilities` and `POST /v2/mcp/invoke`) still use broad
`JsonObject` schemas because their actual handlers return dynamic
`serde_json::Value`. All other canonical `/v2` request/response paths
are wired to their Rust/serde-derived DTO schemas; do not invent static
tool-result payload DTOs for the model gateway.
The operator process list schema contains only its redacted eight-field
summary, while the process detail schema intentionally includes diagnostic
fields. A typed schema describes serde
serialization/deserialization, not every semantic validation rule: for example,
the message `mode` is accepted as a string and normalized/validated by its
handler, and retirement must pass durable state and revision checks. Refer to
the endpoint-specific guidance below for those constraints.

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

Unknown JSON fields are rejected with HTTP `422 invalid_request`,
rather than silently ignored. For example, registration cannot set
`lead_agent_id`; use the separately revisioned workspace lead endpoint.

`Idempotency-Key` is optional, but recommended for retried mutations. The key is scoped to the authenticated operator principal. Repeating the same key with the same normalized input replays the exact terminal response, including the original `operation_id`. Reusing the same key with different normalized input returns HTTP `409` and does not mutate workspace state.

### Durable managed-worktree creation

`POST /v2/workspaces/{workspace_id}/worktrees` accepts `worktree_name`, optional
`branch_prefix`, `base_ref`, `deletion_policy` and `run_init_script`. For retryable
operator automation, send an `Idempotency-Key`. By default, init scripts are
disabled unless `run_init_script` is explicitly `true`; omitted deletion policy
uses the server's configured default **for a new checkout**; when reusing an
existing managed checkout without a policy override, it preserves that
checkout's original retention setting. Explicitly requesting a conflicting
retention setting rejects before native-effect admission. Every creation commits the exact
requested Git checkout identity, **exclusive checkout-path and Git-branch
claims**, and native-effect intent **before** running Git or an init script. After successful
native work and a verified managed row, a second transaction records the exact
response (including the original `created` and `init_script_status`), finalizes
the effect, and releases both claims. On success, `X-Operation-ID` identifies the
durable operation. The same key with changed input returns HTTP `409` without
running another command.

If Git, an init script, or response persistence has an ambiguous outcome, the
operator receives HTTP `409` with `error.code: "outcome_unresolved"`,
`error.retryable: false`, `error.details.operation_id`, and the same
`X-Operation-ID` header when the durable operation is known. The operator must
consult `GET /v2/operations/{operation_id}`. The intent remains
`requested` or `manual_review`, without automatic native re-execution. The
two claims prevent a fresh key from overwriting uncertain effects on either the
same checkout or native Git branch, even after changing the managed root. Recover
only after independent verification; simply using a new
key is not a recovery procedure. Calls without `Idempotency-Key` also persist
native intent with a server-generated one-shot key and return `X-Operation-ID`
on success (or when reporting an unresolved native outcome), but cannot replay
the original receipt after a lost response.

Workspace and workspace-agent lists use bounded keyset pages with optional
`limit` (1–100; defaults to 50) and `cursor` query parameters. The response
remains a JSON array for client compatibility. When another page exists, the
server returns an `X-Next-Cursor` response header to pass back verbatim as the
next `cursor`; when the header is absent, the listing is complete. The cursor
uses the durable `(created_at, opaque_id)` order, not offsets that can shift
when agents or workspaces are added. The workspace-list cursor must match
the exact creation timestamp and ID of a currently existing workspace;
fabricated or removed anchors return HTTP 400 rather than silently skipping
the collection. Workspace-agent lists also accept
`lifecycle=active|archived|all` independently from pagination.

Read the durable state:

```bash
curl -fsS -H "Authorization: Bearer $TOKEN" "$BASE_URL/v2/workspaces"
curl -fsS -H "Authorization: Bearer $TOKEN" "$BASE_URL/v2/workspaces/$WORKSPACE_ID"
curl -fsS -H "Authorization: Bearer $TOKEN" "$BASE_URL/v2/operations?workspace_id=$WORKSPACE_ID&limit=50"
curl -fsS -H "Authorization: Bearer $TOKEN" "$BASE_URL/v2/operations/$OPERATION_ID"
```

`GET /v2/operations` returns newest-first durable operation summaries, optionally
filtered by exact `workspace_id`. It uses indexed `(created_at, operation_id)`
keyset pagination: `limit` defaults to 50 (maximum 100), and `X-Next-Cursor`
supplies the next page's cursor. The cursor must identify an existing
operation with the exact creation timestamp and, when filtering, the same
workspace; an unknown or mismatched continuation returns HTTP 400 instead of
silently skipping records. Each row contains only the operation ID,
workspace ID, kind, actor kind, phase, optional error code and timestamps.
It does not retrieve admission requests, idempotency credentials, terminal
results, claims, transitions or effect details for every row. The singular operation endpoint returns the full durable
operation row plus current claims, transition evidence, effect evidence, outbox
rows, and delivery receipts. Workspace registration itself is committed in one
SQLite transaction with its operation transitions and canonical-root claim/fence
bookkeeping, so a failed commit does not leave a half-created workspace or a
falsely terminal operation.

### Retiring and reactivating a workspace

`GET /v2/workspaces/{workspace_id}/retirement-preflight` is an authenticated, **read-only advisory snapshot**. It reports whether the workspace currently qualifies for zero-side-effect retirement and returns stable blocker codes, counts and descriptions for outstanding session ownership (including archived agents), processes, deliveries, notices, hooks, operations, migration conflicts, leadership and unrelated legacy activity in the same repository. Use this to identify what requires recovery or deliberate native-effect settlement, not as an authorization to forcibly close agents or delete files. A preview is not a lock or guarantee: `POST .../retire` rechecks all blockers in its own write transaction.

`POST /v2/workspaces/{workspace_id}/retire` is an operator-only **quiescent retirement** operation. Pass JSON `{"expected_revision":0}` (use the current revision from `GET /v2/workspaces/{workspace_id}`) and preferably an `Idempotency-Key`. The response contains the durable `operation_id` and the workspace in `retired` state. The operation, its claim/fence history, a retired-root identity receipt, and the lifecycle change commit **atomically**. Retrying the same key and exact input returns the original response without retiring the workspace again.

This endpoint is deliberately conservative: if *any* workspace session ownership or agent record exists (even archived), or a managed process, message delivery, lifecycle notice, hook effect, migration subject, or workspace operation needs settlement, it returns `409 Conflict` and **does not retire anything**. It also checks for unowned legacy provider sessions and live processes using the same filesystem root; an empty v2 roster alone is not sufficient proof. Use the retirement workflow below for a workspace with agents. The quiescent endpoint sends no provider requests and preserves worktrees.

`POST /v2/workspaces/{workspace_id}/retirement/advance` retires a populated workspace through a durable operation. Send `{"expected_revision":...}` with a **required `Idempotency-Key`**, retaining both for retries. Admission freezes new workspace work, snapshots agents, turns, processes, deliveries, hooks, notices and worktrees, and records effect intents before contacting providers. A turn that provably never reached its provider is cancelled locally, including its pending pre-dispatch approval. A native turn is interrupted and must reach a proven terminal state. Provider attachments are archived only after provider-specific close evidence is committed. Retained and external worktree files are preserved.

Queued processes that never launched are cancelled without creating a completion turn. Running processes receive operator cancellation; their terminal result remains durably pending while the owner is archived. **Reactivating the workspace alone does not deliver that result.** Explicitly restoring its owner makes the normal exactly-once completion worker eligible to deliver it. Undelivered messages and lifecycle context are cancelled by durable retirement receipts, and a stale worker or retry cannot resurrect a cancelled delivery.

**HTTP 200** means the returned operation is `completed` and the workspace is retired. **HTTP 202** means it remains `requested` or `manual_review`; inspect `GET /v2/operations/{operation_id}`. Repeating the original request and key advances only provable effects. Background workers continue safe admitted work, including after restart. Effects whose native outcome was left uncertain are quarantined for operator reconciliation rather than replayed. Neither an accepted request nor an archived-agent row proves native closure by itself.

A frozen retirement blocks further SQLite admission and native process launch authorization, including legacy-origin records. Its terminal tombstone continues fencing workspace routes until reactivation. Historical legacy work without proven ownership and provider attachments without verified native closure require explicit reconciliation. Completed close receipts can be reused for an unchanged archived attachment on a later retirement; explicitly restoring or changing that attachment requires fresh close evidence.

#### Reconciling an uncertain retirement

An operator can reconcile one exact quarantined effect through `POST /v2/workspaces/{workspace_id}/retirement/{operation_id}/reconcile`. This route requires operator bearer authentication and an `Idempotency-Key`; it is not available through the model tool surface. Read the effect ID and any recorded native identity from `GET /v2/operations/{operation_id}` before reviewing it. Inspect the current agent revision through `GET /v2/workspaces/{workspace_id}/agents/{agent_id}` and the exact session references, timestamp, and active turn through `GET /v1/sessions/{agent_id}`. The request includes `effect_id`, a bounded `evidence` explanation (8–2048 UTF-8 bytes), and one of these typed resolutions:

- `{"kind":"provider_closed","identity":{"agent_id":"...","provider":"codex","provider_session_ref":"...","canonical_provider_session_ref":null,"agent_revision":0,"session_updated_at":0,"active_turn_id":null}}` asserts that the operator independently verified closure of that exact native attachment. Replace every example value with the current recorded identity. Provider, native and canonical references, agent revision, session timestamp, and active logical turn must match; an old assertion cannot authorize a changed or restored attachment. Verified attachment closure can settle its exact snapshotted logical turn and pending approvals after a restart, while preserving any historical uncertainty about whether input was dispatched. Managed processes still require their own terminal evidence. This records an audited close receipt and archives the attachment without sending another native close request.
- `{"kind":"recheck_settled"}` asks the runtime to verify durable settlement facts for the exact turn, process, delivery, notice, or hook effect. The explanation alone cannot authorize a result. If the required proof is still missing, the receipt reports `settled:false` and the retirement stays quarantined.

The response is an immutable review receipt with `settled`, `operation_phase`, and `unresolved_effect_ids`. Repeating the same review key and input returns that receipt; changing the input with the same key returns a conflict. Once the uncertain effects are resolved, the original retirement operation returns to `requested` and its background worker continues the remaining admitted work. A successful review response does not itself mean the workspace is retired: inspect the original operation until it reaches `completed`. The resource fence remains in place throughout reconciliation.

Native close proof is provider-specific. Codex retirement waits for its isolated app-server child to exit. Claude retirement seals the session against new SDK subprocesses and waits for every child owned by that session to exit. ACP requires the negotiated close capability and a valid acknowledgement from the same connection. Unsupported, malformed, or timed-out outcomes remain recoverable `manual_review` state instead of being treated as successful closure.

After a proven completed retirement, `POST /v2/workspaces` with the **same canonical root** and a **new idempotency key** reactivates the existing workspace identity instead of minting another ID. It increments the revision and leaves leadership unset. Reactivation independently rechecks outstanding obligations under the registration write transaction: even with a valid retirement receipt, late legacy sessions, active processes, deliveries or unresolved work block reopening without consuming the idempotency key or discarding the retired identity. While retired, worktree inventory marks retained worktrees with the `workspace_not_active` assignment blocker. A retired flag without a valid terminal retirement operation cannot reactivate automatically. Reusing an old registration key returns its historical exact receipt and does not reactivate anything.

Existing session/team/process/worktree APIs remain under `/v1`; `/v2` is introduced incrementally rather than changing `/v1` behavior in place. Canonical agent messaging is also available under `/v2` without requiring a legacy team ID.

### Workspace-owned agents

#### Read-only team hook configuration

`GET /v2/workspaces/{workspace_id}/hooks` is an **operator-authenticated,
read-only** view of versioned user/project hook configuration. An optional
`?event=pre_teammate_add` (or any of the five `post_*` event names) filters the
result. The JSON response includes `config_version`, `execution_enabled`, `executable_events`, and
effective entries (`event_name`, normalized `name`, `command`, `timeout_ms`,
`scope`, `fingerprint`). `execution_enabled: true` means approved hooks can
execute, and `executable_events` lists **six live events**:
`pre_teammate_add`, `post_teammate_add`, `post_teammate_remove`,
`post_direct_message_send`, `post_broadcast_message_send`, and
`post_compaction`. Compaction hooks are admitted only upon durable
terminal completion, not a provider's provisional acceptance. Discovery never grants permission to run
commands or enables a new model-facing tool.

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
Include an `Idempotency-Key` on the `PUT` for retry safety. Approval and its
durable operation receipt commit atomically; the response returns the approved
revision's `ETag` and `X-Operation-ID` (inspectable at
`GET /v2/operations/{operation_id}`). Retrying the same key and selections
recovers the original approval and revision, even after effective hook config
changes or later operator revocation. Reusing the key for different selections
or another operation returns `409` without authorizing new commands. New
approvals are refused while workspace retirement is in progress.
An agent editing its project's configuration therefore cannot add approved
commands. Grants are workspace-local, and an admitted but unclaimed effect
cannot begin once its grant is revoked.

**Hook commands execute as the runtime OS user, not inside a security sandbox.**
Operator approval pins the command text and hook settings, not the contents of
scripts or other files that command subsequently reads. In particular, do not
approve a command that executes an agent-writable workspace script unless you
intend to trust changes to that script too. Revoking the approval prevents
future unclaimed executions; it cannot undo an already-started shell process.

An approved **pre-add hook** is admitted durably against a reserved member ID
before any provider session can launch. It blocks creation on failure or
invalid output. It can select only an existing canonical directory within the
workspace root, and cannot override an explicitly selected worktree route.
Operator-approved onboarding text is included in the new member's first,
system-provenance onboarding notice; optional caller text is admitted as a
separate scoped notice in the **same transaction** as member creation. Other
injection recipients and authorization/policy mutations are rejected. Crashed
or orphaned pre-add executions retain reviewable receipts and are never
automatically retried.
If multiple approved hooks propose conflicting working directories, member
creation fails rather than silently choosing one. Revoking an approval after
the script has run but before agent admission also prevents the provider
session and onboarding context from being committed as a workspace member.

An approved post hook is admitted *atomically* with its canonical workspace
membership, message or completed compaction mutation. A worker runs its bounded script once and
commits a terminal receipt before separately validating any optional output.
Message hooks wait until the message's durable delivery attempts have settled,
so their context cannot race ahead of a still-pending or deferred message.
Only a successful terminal script receipt makes context eligible for application;
a nonterminal effect cannot consume a prematurely marked context receipt.
Only active, authorized recipients named by the immutable event snapshot can
receive resulting automation-context notices. A transactionally receipted
notice prevents duplicate context on restart; a forged target rejects the
entire output. Operator revocation prevents still-unclaimed scripts from
executing, and old or ambiguous effects are never blindly replayed.

The hook command receives a bounded JSON object on stdin with `schema_version`,
`event_id`, `hook_event`, `workspace_id`, `actor_agent_id` (when an active member
initiated the event), `subject`, and `eligible_target_agent_ids`. It must exit
successfully and write a **single JSON object** to stdout (at most 16 KiB).
An empty `{}` means no context delivery. Optional output such as
`{"inject":{"caller":{"text":"..."},"targets":[{"agent_id":"sess_...","text":"..."}]}}`
is checked against the persisted actor, event recipients, current workspace
membership, and notice delivery order before it enters an agent turn. A
failed, timed-out, or invalid-output script never rolls back an already-committed
message or membership mutation. The operator-facing effect history reports
both script execution and the independently receipted output application.

Hook effect intents, single-execution claims, terminal script and context
application receipts, and ambiguous-start `manual_review` classifications
are persisted separately.
`GET /v2/workspaces/{workspace_id}/hooks/effects?limit=50` returns
newest-first, indexed operator effect summaries, with `limit` from 1 to 100
and an optional `cursor`. When another page exists, pass the response's
`X-Next-Cursor` as the next request's `cursor`. Summary rows expose event
identity, hook name, execution phase, attempts, application state and
timestamps, **not** shell commands, hook inputs or native receipts.
The continuation must refer to an actual effect in the selected workspace
with its original creation timestamp; a forged, removed or foreign effect
cursor returns HTTP 400 without scanning other workspaces. Inspect
one effect explicitly via
`GET /v2/workspaces/{workspace_id}/hooks/effects/{parent_event_id}/{event_name}/{hook_name}`
to retrieve its full approved hook, input and receipt. Both routes require
operator bearer authentication. Historical effect keys are immutable across
phase/application transitions, so changing those states does not reorder
pages. Post-compaction hooks follow the canonical provider-compaction lifecycle.

Normal `/v2` agent creation is always scoped to a registered workspace:

```bash
AGENT_RESULT=$(curl -fsS -X POST \
  -H "Authorization: Bearer $TOKEN" \
  -H "Idempotency-Key: create-builder-123" \
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

**Agent creation is an operator-native, write-ahead operation**, not a best-
effort provider session launch. Pass `Idempotency-Key` to retry the same exact
request safely: the runtime atomically reserves the operator, normalized
request hash, durable `op_agent_create_*` coordinate and one stable `sess_*`
agent identity **before any new Git checkout, approved pre-add hook or
provider session creation**. A newly created managed worktree has a separate
durable Git operation, whose derived child key/ID is linked to that parent's
frozen launch policy. After native provider acknowledgement, one SQLite
transaction commits the original session/profile/ownership, recreation policy,
worktree claim, pre-add hook application, mandatory onboarding snapshot,
exact operation receipt and claim release. The response includes the agent
revision `ETag` and `X-Operation-ID`. Reusing the exact caller key never
starts a second provider session or Git checkout, even after a crash or
lost HTTP reply; changing its request or target workspace returns `409`.
An omitted key still gets a server-generated, one-shot operation, but an
unkeyed HTTP retry cannot identify the original result.

If Git, native provider creation or the atomic receipt has an ambiguous
outcome, the original operation retains its exclusive native identity claim
and returns `409 outcome_unresolved`, including
`error.details.operation_id`. An unknown provider result **does not** create
an authoritative agent from a guessed session reference. Startup quarantines
unreceipted native create intents to `manual_review` and never re-executes
Git/hooks/provider calls for them. Inspect
`GET /v2/operations/{operation_id}` for the reserved agent ID, effect evidence
and frozen original launch snapshot.

For an independently verified provider outcome, resolve that exact intent via
`POST /v2/workspaces/{workspace_id}/agents/creations/{operation_id}/resolve`.
Supply `resolution` (`created` or `not_created`), substantive `evidence` (8–2048
bytes), the original `reserved_agent_id` and `provider`. For `created`, also
supply the independently observed `observed_provider_session_ref` and nullable
`observed_canonical_provider_session_ref`: a **durably frozen pre-provider
launch snapshot is required** to reconstruct approved session/agent/onboarding
authority, with no provider call. The restored agent stays in
`create_recovery_required` until a fresh startup successfully reattaches the
exact observed native identity. A mismatched/failed reattachment leaves it
fenced. Verified `not_created` permanently consumes the original key with
`409 operator_verified_not_created`, preserves any previously created Git
checkout for separate audited cleanup, and permits a distinct caller key for
a new creation. The evidence-only review rejects `Idempotency-Key`, `If-Match`
and extra JSON fields, and an identical review replays its stored receipt.

The agent creation and archive requests reject unknown fields before native
provider or lifecycle effects. Nested `current_preferences` and onboarding
skill invocation objects reject unknown properties too, so a misspelled
`thinking_effort` or invented skill option never silently selects a default;
the same rule applies to preference mutation requests.

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
  "$BASE_URL/v2/agents/$AGENT_ID"

curl -fsS -H "Authorization: Bearer $TOKEN" \
  "$BASE_URL/v2/workspaces/$WORKSPACE_ID/agents/$AGENT_ID/onboarding"
```

The list defaults to active agents. Use `?lifecycle=archived` for archived history or `?lifecycle=all` for both states.
`GET /v2/agents/{agent_id}` is a read-only global operator lookup for a
workspace-owned agent, including an archived agent. It returns the canonical
workspace identity and a strong revision `ETag`, just like the workspace-scoped
agent lookup; it does not create standalone agents.

### Provider observations and mutable session controls

After provider turns, Gooselake attempts to persist the provider-reported context window, token usage and remaining percentage. `GET /v2/agents/{agent_id}/context-limit` returns the current observation or JSON `null` if none exists for the current native attachment. Snapshots carry the provider native reference, optional canonical reference, agent revision, observation time, and source turn. A late observation cannot overwrite newer evidence or survive a rebind, reattachment, or revisioned policy update. `POST /v2/agents/{agent_id}/context-limit/refresh` requests fresh evidence; it never estimates usage from transcript text.

Context-limit refresh accepts **no options** (empty body or `{}`)
and rejects `Idempotency-Key` (no exact native-refresh receipt):
supplying custom refresh options returns `400 invalid_request` without querying the provider.

Use the current agent `revision` from `GET /v2/workspaces/{workspace_id}/agents/{agent_id}` when updating thinking preference or Claude permission intent. Both public mutations require `expected_revision`, return the new revision, and reject stale revisions with HTTP `409`. Changes are rejected during an active turn. Codex and Claude thinking preferences carry into later turns/resume; ACP retains its own preferences and does not support these mutations.

**For retry-safe preference and permission changes, send `Idempotency-Key`.**
Both routes *always* commit an immutable operator request, exact native-session
identity and exclusive agent claim to SQLite **before** calling the provider.
A confirmed provider acknowledgement and the updated agent policy/revision
are committed with the exact result in one transaction; `X-Operation-ID`
identifies the durable receipt. Replaying the original operator key and body
returns the original successful response without another provider call, even
after the agent revision advances. Reusing that key for a different request
returns `409 conflict`. A lost or ambiguous native acknowledgement is
`409 outcome_unresolved` with `error.details.operation_id`; the effect remains
under `manual_review`, blocking another native mutation, turn or provider
resume until independently reconciled. **Do not retry with a new key.**
On startup, any durable policy intent lacking a terminal native receipt is
quarantined as `manual_review` with its original claim retained. Recovery
does not guess whether the provider call happened or silently resubmit it.

### Resolve an uncertain native policy without a second provider call

Use `GET /v2/operations/{operation_id}` to inspect the **exact** original
provider/agent identity, expected agent revision, original session timestamp,
and effect intent. Independently examine that very same native provider
attachment and determine whether the originally requested preferences or
permission intent **actually applied** (`applied`) or **did not apply**
(`not_applied`). If this cannot be established, leave the operation in
`manual_review`: guessing can silently discard a provider-side change.

The operator-only
`POST /v2/agents/{agent_id}/native-policy/{operation_id}/resolve` accepts a
strict JSON object with `resolution`, `evidence` (8–2048 bytes), `provider`,
`provider_session_ref`, `canonical_provider_session_ref` (`null` when absent),
`expected_revision`, and `session_updated_at`. All identity fields must match
the original immutable native-effect intent and the **currently unchanged**
workspace/agent attachment. The optional strong `If-Match` must match
`expected_revision`. This endpoint deliberately rejects `Idempotency-Key`,
since the existing `operation_id` is the immutable review target; retry the
same review request to receive its original durable response. Different or
conflicting evidence is rejected. `X-Operation-ID` identifies the reviewed
operation. No native provider call is made by this endpoint.

An `applied` review atomically commits the exact originally requested policy,
increments its revision, and completes the original mutation operation. A
repeat of its **original** idempotency key subsequently returns the confirmed
success receipt without native redispatch. A `not_applied` review preserves
the previous policy/revision, marks the operation failed, and permanently
consumes the original key: repeat calls return nonretryable
`409 operator_verified_not_applied` with the original `operation_id`.
The operator evidence, prior uncertainty, final effect and terminal transition
remain in `GET /v2/operations/{operation_id}`.

**Both outcomes leave the agent's runtime session in
`policy_recovery_required`.** New turns and provider-native operations remain
blocked even after the review releases its exclusive resource claim. Restart
the runtime only after independent review. On the next startup, a successful
native resume using the reconciled durable policy transitions the session to
`ready`. If native reattachment fails, its recovery gate stays closed and
another startup can retry the same exact identity. A review receipt is *not*
permission to keep using a possibly stale in-process provider attachment.

Example after inspecting the original operation/effect and obtaining its
precise original identity (replace the placeholder values with the record's
actual values, not a different session):

```bash
curl -fsS -X POST "$BASE_URL/v2/agents/$AGENT_ID/native-policy/$OPERATION_ID/resolve" \
  -H "Authorization: Bearer $TOKEN" -H 'Content-Type: application/json' \
  -d '{"resolution":"applied",
       "evidence":"Verified the exact native session policy independently",
       "provider":"codex","provider_session_ref":"<original-native-session-ref>",
       "canonical_provider_session_ref":null,
       "expected_revision":0,"session_updated_at":123456789}'
```

Unkeyed `/v2` preference/permission requests now also create a durable one-shot
native intent using a server-generated key and return `X-Operation-ID`. This
prevents untraceable native effects and retains the same recovery quarantine
if their reply is lost. **An unkeyed HTTP retry is still not replay-safe**:
there is no caller key to retrieve the original receipt, so always provide one
for automation and consult the durable operation after an uncertain response.
The old direct internal manager mutation methods are not exposed on these
`/v2` operator routes; they have separate legacy semantics.
Provider-backed agent **archive and restore** now use separate native-effect
idempotency journals. Caller `Idempotency-Key` enables replay of the exact
original receipt across restart and lost HTTP responses, while omitted keys
create durable one-shot intents that cannot be identified by a later unkeyed
HTTP retry. Both transitions check unresolved native policy, user-turn,
restore and archive claims inside the SQLite write transaction. Independently
reviewed open attachments remain fenced until startup verifies exact native
reattachment, including when a stale manager or direct persistence caller
tries to skip the normal gate.

```bash
curl -fsS -H "Authorization: Bearer $TOKEN" \
  "$BASE_URL/v2/agents/$AGENT_ID/context-limit"

curl -fsS -X POST -H "Authorization: Bearer $TOKEN" \
  -H 'Content-Type: application/json' -H 'Idempotency-Key: policy-pref-001' \
  -d '{"expected_revision":0,"current_preferences":{"thinking_effort":"high"}}' \
  "$BASE_URL/v2/agents/$AGENT_ID/preferences"

curl -fsS -X POST -H "Authorization: Bearer $TOKEN" \
  -H 'Content-Type: application/json' -H 'Idempotency-Key: policy-permission-001' \
  -d '{"expected_revision":1,"permission_intent":{"kind":"inherit_provider_configuration"}}' \
  "$BASE_URL/v2/agents/$AGENT_ID/permission"
```

The permission endpoint is Claude-only. These controls do not rewrite earlier turns.

`POST /v2/agents/{agent_id}/turns` admits a new user-visible turn, preserving its typed `turn_settings` in the durable dispatch-policy snapshot even if the runtime has to recover an admitted-but-not-dispatched turn after restart. Explicit `thinking_effort` selects and durably updates the agent's revisioned current thinking preference **before turn admission**; subsequent turns inherit that preference until it is changed again. Omit it to use the existing preference without a revision change. Codex also accepts `codex_collaboration_mode: "default" | "plan"` on a specific turn, without turning that selection into permission or a persistent session default. Claude accepts thinking changes without Codex collaboration. Unsupported ACP preferences or cross-provider collaboration modes fail before turn admission.

For operator automation, include an optional `Idempotency-Key`. The trusted
operator, agent identity, normalized input, expected turn, permission override
and typed per-turn settings are reserved in **the same SQLite transaction as the
logical turn and provider-dispatch admission**. A successful request returns
`X-Operation-ID`, and an exact replay returns the original immutable
`session_id`, `turn_id`, and accepted status, even after that turn completes or
the agent is archived. Changing the key's input or mutation kind returns
`409`. A definitively **not-dispatched** provider failure is receipted without
allowing the same key to send again; a new key can safely retry once the
original turn is settled. If the native dispatch or acceptance receipt is
uncertain, `409 outcome_unresolved` supplies `error.details.operation_id` and
`X-Operation-ID` for `GET /v2/operations/{operation_id}`. Its exclusive
agent-dispatch claim remains held until independently reconciled; a new key
cannot bypass that fence. Do not blindly resend the model input. Calls without
a key retain their existing direct-turn behavior and cannot replay a lost
response by request identity.

The unresolved native-turn claim also blocks other provider-native mutations
on that agent: permission and preference changes, manual compaction, historical
edit/fork, worktree reassignment, archive, and explicit session close/resume.
The `409 outcome_unresolved` error identifies the original operation rather
than relying on a possibly stale in-memory active-turn marker. Exact replay of
an already completed, independently journaled mutation is still allowed; a
restore request that is already a no-op does not change the native attachment.

For keyed turns, an explicit `thinking_effort` must already match the agent's
current persisted preference; selecting a *different* effort would itself
invoke a separate native provider mutation before turn admission and is
therefore rejected. Update the agent's thinking preference through its
revisioned policy endpoint before submitting a keyed turn. Unkeyed turns retain
their existing optional thinking-selection behavior.

```bash
curl -fsS -X POST -H "Authorization: Bearer $TOKEN" \
  -H 'Content-Type: application/json' -H 'Idempotency-Key: review-change-001' \
  -d '{"input":[{"type":"text","text":"Review this change"}],"turn_settings":{"codex_collaboration_mode":"plan"}}' \
  "$BASE_URL/v2/agents/$AGENT_ID/turns"
```

`POST /v2/agents/{agent_id}/compact` requires a ready, active workspace-owned Codex or Claude agent. It durably admits a manager-owned `cmp_*` run before invoking the native provider, with optional `Idempotency-Key`. The result distinguishes `accepted` (native request accepted, **not yet confirmed complete**) from `not_performed`; an unknown provider outcome becomes `manual_review` and is never automatically resubmitted. `GET /v2/agents/{agent_id}/compactions/{run_id}` reports the exact durable operation. Manual and unsolicited provider/automatic compaction now share this lifecycle: `started`, `compacting`, `completed`, `failed`, or `cancelled` observations are attached to the exact provider-native session, with `source`, `confidence`, `trigger`, and optional before/after token counts. Automatic run IDs appear in durable `compaction.lifecycle_changed` session events. An observation may confirm completion before the provider returns synchronous manual acceptance; a late acceptance cannot roll back its terminal state. The provider's native session reference is checked at commit time, so delayed observations from a replaced attachment are rejected.

Manual compaction accepts **no options** (empty body or `{}` allowed).
A speculative `dry_run`,
`force`, or other JSON option returns `400 invalid_request` before durable
compaction admission or native provider invocation.

Compaction completion transactionally admits approved `post_compaction` hook intents and one stable system notice per selected recipient. The compacted member's active creator is subscribed by default unless their `creator_compaction_subscription` is `unsubscribed`; the workspace notice policy enables lead notifications by default and disables team-wide notifications by default. Lead/creator overlap is deduplicated. If no eligible lead, creator, or opted-in team member exists, no notice is injected. System notices use the same ordered, recoverable `automation_context` dispatcher as onboarding; hook commands still require the operator's exact trusted approval. Neither a failed compaction nor replayed terminal evidence emits a completion hook.

Until an outstanding or ambiguous run is settled, new turns, permission updates, and worktree rebinds are fenced. A crash does not erase or replay unresolved compaction; a started/compacting native run without terminal evidence is quarantined to `manual_review`. If evidence is lost, an operator can independently inspect native provider state and settle an ambiguous `manual_review` via `POST /v2/agents/{agent_id}/compactions/{run_id}/resolve` with JSON `{"phase":"failed","evidence":"..."}` or a verified `completed` result. This records external review evidence and never repeats native compaction; confirming completion requires the same native attachment/revision. Codex has session-level completion events without a native compaction item ID, so a bounded weak-alias deduplication window is used instead of claiming an exact native run identity. ACP remains unsupported for provider-native compaction.

`POST /v2/agents/{agent_id}/turns/{turn_id}/edit-rerun` is the manager-owned historical edit path. It requires an idle workspace agent, a **completed user-visible** target turn with persisted input/native turn identity, and a provider advertising verified hard-fork support (Codex and Claude advertise this capability; ACP does not). Its body is `{"edited_snapshot":{"prompt_text":"Updated request","image_refs":[],"invocation":null}}`; optional `Idempotency-Key` makes exact retries return the original operation and rejects a different edit under the same key. Unknown properties in `edited_snapshot`, its individual `image_refs`, or `invocation` are rejected with `422 invalid_request` **before any native fork or operation admission**. The manager rebuilds native structured inputs from the edited snapshot, including ordered persisted image paths and a **Codex-only** skill invocation when supplied, rejecting missing files and unsupported invocation types. It records an `edit_*` operation **before** provider fork, verifies the provider-native child, atomically commits the child attachment, retained transcript prefix and superseded-turn lineage, then durably admits a **new logical user turn** with operation correlation. Previous turns remain durably available for audit; no source turn is overwritten. `completed` on the edit operation means the replacement turn was admitted, not that model work finished. The new turn's own state and provider dispatch outcome remain canonical.

Manual compaction review and edit resolution use their existing `run_id` or
`operation_id` for inspection, **not** a new idempotency receipt. These POST
review routes reject `Idempotency-Key` with `400 idempotency_not_supported`
instead of silently ignoring a requested replay guarantee; after an uncertain
response, inspect the corresponding GET resource before deciding next steps.

`GET /v2/agents/{agent_id}/edits/{operation_id}` returns the durable operation, child identity and replacement turn (if any). A crash or uncertain provider fork is `manual_review`, **never automatically retried**. While an edit remains unresolved, ordinary new turns, native compaction and worktree reassignment are blocked. An operator who has independently established a verified child attachment with no admitted replacement turn can record evidence at `POST /v2/agents/{agent_id}/edits/{operation_id}/resolve` using `{"evidence":"Provider child verified, no replacement admitted"}`; this settles the operation as failed without repeating the native effect. Unknown native child identity cannot be dismissed by an unverified assertion and remains fenced for explicit external recovery. The provider-native edit path is capability-gated; unsupported providers do not silently emulate it. **A provider conversation fork does not undo filesystem writes, commands or external tool side effects from superseded turns.**

### Workspace worktree routes

`GET /v2/workspaces/{workspace_id}/worktrees` lists persisted managed worktrees for the workspace's canonical Git repository. Each item includes its logical ID, monotonically increasing persisted-record `revision`, name, branch, path, retention policy, active attached agent IDs, and `eligible_for_assignment` plus explicit `eligibility_blockers`. The response includes the canonical repository root, Git common directory, and repository fingerprint. A stale/missing path, mismatched native branch/repository, or conflicting claim is **not** considered eligible. The worktree revision changes on persisted record updates; it is independent of the workspace-agent revision and Git commit.

With SQLite storage, inventory reads one repository-scoped worktree/revision/active-claim snapshot instead of loading the full runtime history or querying each worktree revision separately. Historical repository-root path aliases are resolved to the same canonical Git root; released claims are excluded from the conflict check. Attached agent IDs remain derived from the **active workspace roster**, not claim ownership alone. The index-backed projection is read-only; Git identity and routing eligibility are reverified when inventory is requested.

The inventory's active-agent attachment map reads only indexed member IDs and durable routing CWDs; archived agents, unrelated workspace memberships, and unnecessary provider/profile metadata do not enter this projection. Native route eligibility and any conflicting live claim are still evaluated against the full authoritative attached-ID set, rather than just the `gg_team status` 100-member display page.

Inventory output is bounded with `limit=1..100` (default 50) and an optional
opaque `cursor`. When more records remain, follow the response's
`X-Next-Cursor` as `?cursor=...` on the next request. Ordering uses the stable
normalized worktree name and logical ID, so names that tie cannot skip or
repeat an entry. Entries inserted before an existing cursor do not get
replayed on later pages. Repository identity metadata remains present on
every page; worktree eligibility is recalculated from current Git evidence.
The opaque cursor must refer to an existing name/ID pair in this workspace's
currently visible managed Git inventory. A cursor from another repository or
from a worktree removed since the preceding page returns HTTP 400 rather
than silently skipping remaining candidates.

To create or reuse a named managed Git checkout without needing an existing source agent, use the workspace-scoped endpoint:

```bash
curl -fsS -X POST \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"worktree_name":"feature-search","branch_prefix":"gg","run_init_script":false}' \
  "$BASE_URL/v2/workspaces/$WORKSPACE_ID/worktrees"
```

Its response includes a durable worktree ID, `created`, and init-script status. Repeating the same repository, name and branch selection converges on the existing managed worktree. Worktree names must be Git-safe slugs. Managed worktrees are kept when their retention policy says to retain; no external/unmanaged checkout is implicitly deleted. If an init script fails after creating local files, cleanup will not force-delete them. A later request can recover that exact verified Git checkout without repeating the failed initialization.

Managed-worktree claim/release transitions write their critical `worktree.claimed`
or `worktree.released` event **in the same SQLite transaction** as the claim;
a journal-write failure rolls back the claim change, and a replay of an
already-effective transition does not overwrite its original timestamp or
append a duplicate event. A newly created or independently recovered Git
checkout likewise commits its managed record and `worktree.created` or
`worktree.recovered` event together. Git itself is a separate native effect:
an unknown post-Git database result must not be interpreted as permission to
run the same native operation again.

Native managed-worktree **cleanup** (`delete_on_last_claim` only) now admits a
durable system operation, checkout-path and branch reservations, and a
`worktree.cleanup_admitted` event **before** removing the checkout or branch.
The terminal receipt and `worktree.cleaned_up` event commit together, and a
completed cleanup replays the exact original response without running Git.
If a process dies or the terminal transaction fails after Git, the pending
operation remains visible through `GET /v2/operations/{operation_id}` and
subsequent calls **do not repeat Git**. When the exact checkout and Git branch
can both be independently proven absent, the runtime can settle that existing
operation from read-only evidence after restart; ambiguous, partial, foreign,
dirty or unverified effects remain fenced for operator review. Incomplete
native cleanup records `manual_review` and preserves reservations rather than
automatically retrying deletion. A typed `409 outcome_unresolved` is
non-retryable native authority, not a transient failure inviting a new key.
Claims cannot be attached to a checkout whose generation has an admitted
cleanup operation, even through another SQLite writer. This does **not**
authorize removal of preserved legacy data or unresolved migration state.

When creating an agent, choose one of these `worktree` selectors in the `POST /v2/workspaces/{workspace_id}/agents` JSON request:

```json
{"provider":"codex","worktree":{"mode":"root"}}
{"provider":"codex","worktree":{"mode":"existing","worktree_id":"wt_existing"}}
{"provider":"codex","worktree":{"mode":"new","worktree_name":"feature-search","run_init_script":false}}
```

The `new` selector validates deterministic agent settings, provider availability,
onboarding inputs and workspace lifecycle **before** creating a native Git
checkout. A registered but disabled provider is unavailable for agent creation,
including the root route; the final manager admission also rechecks enablement
before any provider-native session creation. The new checkout is admitted through the same durable write-ahead
Git operation journal as `POST /v2/workspaces/{workspace_id}/worktrees`, including
an operator-owned operation and exact native result; its ID is persisted as
`created_by_operation_id` on the managed-worktree record. A native Git outcome
that cannot be proven returns `409 outcome_unresolved` with the operation ID
for explicit inspection rather than automatically repeating Git. The agent
creation endpoint itself is **not yet** a durable idempotent provider-creation
operation; its worktree step uses a one-shot server key, not a replayable
caller key. The existing default retention policy for agent-created worktrees
remains `retain_on_last_claim` when no policy is supplied.

Until native provider-agent creation is journaled, callers must **not** assume
that an `Idempotency-Key` makes the entire agent request replay-safe. The
endpoint rejects that header with HTTP `400 idempotency_not_supported`, before
admitting a provider or native Git mutation; it does not silently ignore it.

The provider cwd, workspace-agent recreation policy, session worktree ID, and
owned managed-worktree claim are persisted consistently. `cwd` must not also
be supplied with an explicit worktree selector. An agent-create failure after
Git succeeds may still leave an unclaimed, retained checkout for inspection
and reuse rather than destructively rolling it back. Preflight cannot rule
out concurrent lifecycle changes or later provider/hook failures.

Operators can rebind an **idle**, active workspace agent to an already registered eligible managed worktree, or back to the workspace root (null worktree ID):

```bash
curl -fsS -X POST \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -H "Idempotency-Key: move-builder-linked" \
  -d '{"worktree_id":"wt_existing","expected_revision":0}' \
  "$BASE_URL/v2/workspaces/$WORKSPACE_ID/agents/$AGENT_ID/worktree"
```

`expected_revision` is the current workspace-agent revision, not the workspace revision. Optionally send the matching strong `If-Match: "0"` header; a missing header retains compatibility, but a weak, duplicate or mismatched precondition returns typed `412 revision_precondition_failed` **before** provider work or native intent. A successful verified rebind includes the resulting agent revision `ETag` and `X-Operation-ID`. The agent's public identity and provider session identity remain stable. The runtime persists a worktree-rebind intent **before** calling a provider, and only after provider-native cwd/session evidence is verified does one SQLite transaction update the session cwd, workspace-agent recreation policy/revision, and managed-worktree associations. A repeat with the same `Idempotency-Key` and identical input returns the original completed operation without calling the provider again; conflicting input or revision fails with HTTP `409`.

Response fields include `operation` (ID, source/destination routes, `phase`, native evidence) and the updated `agent`. Inspect a durable in-flight or recovery-required operation with `GET /v2/workspaces/{workspace_id}/agents/{agent_id}/rebinds/{operation_id}`, or inspect its shared operation journal, resource claim, and transitions through `GET /v2/operations/{operation_id}`. If native dispatch or binding is ambiguous, HTTP `409 reassignment_recovery_required` includes `error.details.dispatch_outcome: "unknown"`, `error.details.operation_id` and the exact `X-Operation-ID` header, including on a same-key retry, instead of requiring the caller to parse the ID from prose. Its phase becomes `manual_review`: new turns, restores, archival, and further reassignments are blocked rather than replaying unknown provider work. An explicitly unsupported provider (including current ACP rebind) cannot finalize a changed cwd.

Previous managed checkouts are **preserved by default**. To request cleanup *after* verified native rebinding, add `"cleanup_previous_worktree":true` alongside `worktree_id` and `expected_revision`. This choice is part of the idempotency identity: reusing a key with a changed cleanup policy returns a conflict. The provider route changes first; cleanup can never roll back a verified cwd. The durable operation exposes `previous_cleanup_status` (`preserved`, `pending`, `deleted`, `retained_by_policy`, or `skipped_live_claims`) and an optional `previous_cleanup_diagnostic`. A dirty, unmerged, externally managed or unverifiable worktree remains intact and `pending`, never force-deleted. If cleanup is pending after a restart, or after resolving the safety issue, the operator can retry without re-dispatching provider work:

```bash
curl -fsS -X POST \
  -H "Authorization: Bearer $TOKEN" \
  "$BASE_URL/v2/workspaces/$WORKSPACE_ID/agents/$AGENT_ID/rebinds/$OPERATION_ID/cleanup"
```

This explicit cleanup retry accepts **no options** (empty body or `{}`)
and rejects `Idempotency-Key` with `400 idempotency_not_supported`:
the original provider-rebind key cannot safely promise another Git deletion
receipt. Inspect the rebind by operation ID after an interrupted retry.
Unsupported `force` or other options fail before any Git deletion or record
change.

Only checkout paths under the configured managed root with verified Git identity/branch, no uncommitted changes, and no unmerged branch commits are eligible for automatic deletion. This retry is a separate cleanup action, not a retry of provider rebinding: replaying the original `Idempotency-Key` does not implicitly retry pending native cleanup, even if the checkout has since become clean. Recovery from an ambiguous provider rebind still requires operator investigation and is not implicitly resolved by this endpoint.

Older agent creation requests without a `worktree` selector still accept a `cwd` inside the registered root. The legacy `/v1/worktrees` endpoints remain temporarily available to existing clients; the workspace-scoped endpoint is the preferred creation path. Unresolved provider rebinds still require explicit operator recovery rather than automatic guessing or replay.

Archive and restore without changing identity or policy:

```bash
curl -fsS -X POST \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -H "Idempotency-Key: pause-agent-123" \
  -d '{"reason":"paused"}' \
  "$BASE_URL/v2/workspaces/$WORKSPACE_ID/agents/$AGENT_ID/archive"

curl -fsS -X POST \
  -H "Authorization: Bearer $TOKEN" \
  -H "Idempotency-Key: reopen-agent-123" \
  "$BASE_URL/v2/workspaces/$WORKSPACE_ID/agents/$AGENT_ID/restore"
```

Agent restore accepts **no options** (empty body or `{}`); policy or force
options return `400 invalid_request` before provider resume.

Agent archival uses a **native-close write-ahead operation**. Before contacting
the provider, the runtime persists immutable operator/agent/input identity,
the original native session references, session timestamp, agent revision and
an exclusive native-effect claim. A successful archive requires a strong
provider-close acknowledgement or verified native-process termination for the
exact attachment; updating an SQLite lifecycle field alone never proves that
the provider was closed. The native proof, session closure, roster/lead change,
membership notice, revision and exact operation result are committed in one
transaction. Exact caller-key retries return the same JSON, `ETag` and
`X-Operation-ID` without another provider call. An already archived agent
accepts a new key as a journaled no-effect operation, **not** new evidence
of historical native closure.

If the close acknowledgement is lost, the provider returns mismatching proof,
the atomic receipt fails, or the runtime crashes, the original operation is
quarantined in `manual_review` with its exclusive claim. The request returns
`409 outcome_unresolved` with `error.details.operation_id`; inspection through
`GET /v2/operations/{operation_id}` is required. Startup does not redispatch
the original close or implicitly resume its uncertain native attachment.

To reconcile independently verified provider state, use
`POST /v2/workspaces/{workspace_id}/agents/{agent_id}/archives/{operation_id}/resolve`
with `resolution` (`closed` or `not_closed`), 8–2048 bytes of explanatory
`evidence`, `provider`, the exact `original_provider_session_ref`, nullable
`original_canonical_provider_session_ref`, `expected_revision`, and
`session_updated_at` from the admitted operation. For `not_closed`, also
supply the independently observed `observed_provider_session_ref` and
nullable `observed_canonical_provider_session_ref`; omit both for `closed`.
This review endpoint never calls the provider; it accepts an optional matching
strong `If-Match` but not `Idempotency-Key`, and identical reviews return the
same durable receipt. Verified `closed` settles the original archive without
another native request. Verified `not_closed` consumes the original key with
nonretryable `409 operator_verified_not_closed`, preserves the active agent,
and sets `archive_recovery_required`: only a new startup that reattaches to
the independently observed native identity may reenable native work. A failed
or changed reattachment remains fenced until corrected.

Agent restoration is a provider-native operation protected by a **pre-dispatch
durable intent**, an exact per-agent resource claim, and a single SQLite
commit joining the provider acknowledgement, active roster, session reference,
revision, membership notices, and completed operation receipt. Send
an `Idempotency-Key` to make safe retries possible: an identical key returns
the original exact `ETag`, `X-Operation-ID` and JSON response without a second
provider resume; a new key for an already active agent admits a journaled
no-op. For compatibility, a missing key gets a generated one-shot key and
`X-Operation-ID`, but **a new unkeyed HTTP retry cannot identify the prior
attempt**. `409 outcome_unresolved` includes the original operation ID: use
`GET /v2/operations/{operation_id}` to inspect its durable intent/effect/claim.
An ambiguous provider response, failed atomic receipt or crash during the
native operation leaves `manual_review` and never automatically dispatches
another restore. An unresolved restore also fences other native turns,
settings, reassignment and lifecycle changes across concurrent instances.

After **independently verifying** the original native provider session, an
operator can resolve a quarantined restore without contacting the provider.
Send `POST /v2/workspaces/{workspace_id}/agents/{agent_id}/restores/{operation_id}/resolve`
with `resolution` (`resumed` or `not_resumed`), substantive `evidence`, exact
`provider`, `original_provider_session_ref`, nullable
`original_canonical_provider_session_ref`, `expected_revision`, and
`session_updated_at` from the original intent. For `resumed`, also supply the
independently verified `observed_provider_session_ref` and nullable
`observed_canonical_provider_session_ref`. This evidence-only endpoint accepts
optional matching strong `If-Match`, rejects `Idempotency-Key`, and replays an
identical review receipt. Verified `not_resumed` consumes the original key
permanently with `409 operator_verified_not_resumed`; a distinct key can start
a new restore. Verified `resumed` settles the durable roster/receipt but
requires **a fresh runtime startup with successful exact provider identity
reattachment** before any further native work. A changed attachment remains
fenced under `restore_recovery_required` until corrected.

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
Unknown lead mutation properties, including speculative election/force flags,
are rejected before altering lead authority or revision.

The runtime membership authority follows the same workspace policy as Golden Goose: while a workspace is leadless, any active member may add or remove members. Once a lead exists, the lead may manage membership and non-leads are governed independently by the configured `non_lead_can_add_members` and `non_lead_can_remove_members` policy flags. Lead assignment itself remains an operator-only control rather than a model/member action.

### Durable workspace system notices

The runtime inserts lifecycle notifications in the same SQLite transaction that commits the corresponding authority: a newly assigned lead receives a lead-onboarding notice (including optional global then workspace `.agents/gg/lead-readme.md` instructions snapshotted at assignment); a former lead receives demotion/leadless context; the active lead can receive member additions, removals and restorations; a member receives a route-change notice only **after** their provider has proved the new working directory; and each actually interrupted agent can receive a workspace interruption notice when the operation is durably complete. A lead is not notified of their own membership changes or member changes they initiated. Leadless workspaces emit no lead-membership notifications. A no-op or idempotently replayed lead assignment does not create a second notice.

These notifications use the same stable `automation_context` turn admission and replay-safe dispatcher as the mandatory new-member onboarding. They are **not** messages impersonating a human user, and a provider dispatch with an unknown outcome is never blindly retried. Archiving a recipient cancels their pending notice; busy or unavailable recipients are deferred rather than force-dispatched. System notice policy changes affect newly admitted notices, not already-queued snapshots.

Retirement records operator-visible `workspace.retirement_started`, verified `session.archived`, and `workspace.retired` events in the same transactions as their authority changes, with stable operation-based event IDs. An unresolved native close cannot produce an archived or retired event. The default `workspace_retirement` notice policy also preserves a notice snapshot for each active member when the retirement freeze is admitted. These notices are already `cancelled`, with reason `workspace_retirement_freezes_new_model_input`, zero delivery attempts, and no model turn: the freeze prevents new model input, and retirement must not reopen a provider merely to announce its closure. Setting `workspace_retirement:false` suppresses those future member notice snapshots; durable operator lifecycle events remain available.

Operators can inspect and change the per-workspace policy with independent optimistic revision protection:

```bash
curl -fsS -H "Authorization: Bearer $TOKEN" \
  "$BASE_URL/v2/workspaces/$WORKSPACE_ID/notice-policy"

curl -fsS -X PUT \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -H "Idempotency-Key: notice-policy-update-1" \
  -d '{"expected_revision":0,"policy":{"lead_transitions":true,"membership_to_lead":true,"worktree_reassignment":true,"workspace_interrupt":true,"workspace_retirement":true,"compaction_to_lead":true,"compaction_to_team":false}}' \
  "$BASE_URL/v2/workspaces/$WORKSPACE_ID/notice-policy"
```

The four existing lifecycle flags and `compaction_to_lead` default to `true`; `compaction_to_team` defaults to `false`. Mandatory onboarding is not suppressible through this policy. The policy revision starts at `0`, increments after each successful change, and mismatched `expected_revision` returns HTTP `409`. It is independent of the workspace lead/roster revision. These operator endpoints require the ordinary `/v2` bearer token; models have no notice-policy mutation tool.

For retry-safe notice-policy updates, include `Idempotency-Key` on the `PUT`
request. The policy compare-and-set, operation admission, exact response
receipt, and resource-claim release commit in **one SQLite transaction**.
Successful responses preserve the policy JSON and its strong revision `ETag`,
and add `X-Operation-ID` for `GET /v2/operations/{operation_id}`. Repeating
the same operator key with the same workspace, expected revision and policy
returns the original exact policy result and operation ID, even after a
subsequent policy change or runtime restart. Reusing the key for different
input, a different workspace, or another operation kind returns `409`
without mutating state. Distinct stale revisions still return `409`;
an optional mismatched `If-Match` returns `412`. Updates to retired or
retirement-frozen workspaces are rejected. Omitting the key retains normal
CAS semantics but does not provide lost-response replay.

Interrupt every currently active turn owned by the workspace roster:

```bash
curl -fsS -X POST \
  -H "Authorization: Bearer $TOKEN" \
  -H "Idempotency-Key: stop-workspace-now" \
  "$BASE_URL/v2/workspaces/$WORKSPACE_ID/interrupt"
```

Workspace interruption accepts **no options** (empty body or `{}`).
Nonempty JSON options such as
`{"dry_run":true}` returns `400 invalid_request` without interrupting turns
or allocating an operation.

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

Migration preview rejects `Idempotency-Key` with `400 idempotency_not_supported`: it refreshes diagnostic state instead of offering an exact replay receipt. Migration **apply** supports keyed durable replay.

The migration preview/apply POST routes accept **no options** (empty body or
`{}` allowed for older clients). Supplying
speculative options such as `{"dry_run":true}` or `{"force":true}` returns
`400 invalid_request` before preview writes, migration authority, or operation
admission. Prefer omitting the body.

Read migration diagnostics:

```bash
curl -fsS -H "Authorization: Bearer $TOKEN" \
  "$BASE_URL/v2/migrations/workspaces"
```

Both this status route and the preview accept `limit=1..100` (default 50)
and an opaque `cursor`. Follow `X-Next-Cursor` until exhausted to inspect
all subject diagnostics ordered by kind and immutable subject ID. The global
mapped, archived and unresolved counts, including `cutover_blocked`, always
describe the complete migration snapshot, not only the current page.

Migration subject resolution accepts only `action` (`map` or `archive`) and
optional `workspace_id`. Unknown JSON options, such as `force` or `dry_run`,
return `422 invalid_request` before durable mapping or operation admission.

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

`GET /v2/messages` supports `workspace_id`, `sender_agent_id`, `cursor`, and
`limit=1..100` (default 50). It returns a `messages` array and `next_cursor`
in the JSON response; pass the cursor unchanged to fetch the next page.
Message pages use an indexed ascending `(created_at, id)` keyset over durable
canonical messages, not an in-memory full-history scan. A cursor must refer to
an existing message matching the same optional workspace/sender filters or
the request is rejected; only the bounded page's JSON inputs are decoded.
`GET /v2/messages/{message_id}/deliveries` supports `recipient_agent_id`,
`cursor`, and `limit=1..100` (default 50). Delivery lists retain their JSON
array shape; when another page exists the response includes `X-Next-Cursor`,
based on stable delivery creation time and ID. The SQLite backend reads these
pages using the indexed message/time/ID keyset rather than loading every
delivery of a large broadcast into the HTTP request. Each cursor must match
the ID and exact creation time of a delivery in that message and optional
recipient filter; unknown, changed, or cross-message cursors return HTTP `400`
instead of skipping deliveries. Delivery state is per recipient
and remains explicit across `pending`, `deferred`, `injecting`, `injected`,
`failed`, and `cancelled` transitions. Retry is allowed from failed/deferred
state, and cancellation applies only while all outstanding deliveries are
still cancellable:

```bash
curl -fsS -X POST -H "Authorization: Bearer $TOKEN" \
  "$BASE_URL/v2/deliveries/$DELIVERY_ID/retry"

curl -fsS -X POST -H "Authorization: Bearer $TOKEN" \
  -H "Idempotency-Key: retry-$DELIVERY_ID" \
  "$BASE_URL/v2/deliveries/$DELIVERY_ID/retry"

curl -fsS -X POST -H "Authorization: Bearer $TOKEN" \
  -H "Idempotency-Key: cancel-$MESSAGE_ID" \
  "$BASE_URL/v2/messages/$MESSAGE_ID/cancel"
```

Message creation and its recipient snapshot commit atomically. Operator cancellation now atomically updates every pending/deferred delivery and its legacy mirrors together with an exact durable operation receipt; it cannot partially cancel a broadcast. The cancellation response retains its JSON delivery array and adds `X-Operation-ID` for inspection through `/v2/operations/{id}`. The optional `Idempotency-Key` returns the original receipt after a lost response; reusing the same operator key for different inputs or another operation-backed operator resource conflicts with HTTP 409. A stale delivery worker cannot revive a terminal cancelled delivery.

Message cancellation and delivery retry POST routes accept **no options**
(empty body or `{}` allowed for older clients). Nonempty JSON options, including `{"dry_run":true}`, fails with
`400 invalid_request` without cancelling deliveries or attempting a retry.

Retry only accepts failed/deferred canonical deliveries. Without an idempotency key, its original synchronous delivery-result response is preserved. With `Idempotency-Key`, retry atomically re-admits the delivery in `pending` state with its migrated legacy mirror and a durable completed operation receipt, then attempts provider delivery. The response remains a single delivery JSON object but describes the **exact pending queue admission**, not the later provider delivery result; `X-Operation-ID` points to `/v2/operations/{id}`. Replaying that key returns the exact admission response without a second native attempt, even if the delivery subsequently becomes `injected`, `deferred`, or `failed`. Inspect the delivery endpoint for its current state. If the process dies after durable admission, startup recovery owns pending work; an `injecting` state is not a license to blindly duplicate an ambiguous native send. Key reuse for another delivery or operator operation conflicts. Startup recovery resumes canonical `pending`/`deferred` work, and migrated legacy delivery rows mirror canonical terminal transitions so `/v1` compatibility views cannot inject the same migrated delivery twice.

Before dispatching a canonical delivery, the SQLite runtime acquires a transactional compare-and-swap claim from `pending`/`deferred` to `injecting`. The claim and migrated legacy mirror commit together, and the sender/recipient retirement fences are checked under the same database lock. Competing workers with the same prior status and update timestamp cannot both acquire a native dispatch claim; stale cache updates cannot revert `injecting` to `pending` or revive a terminal `injected`/`cancelled` delivery. An interrupted or externally ambiguous provider dispatch may still require independent admission reconciliation or operator review: this guarantee prevents duplicate status claims, not every possible provider-side duplicate after a crash.

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

Once the provider reports a terminal result, the runtime commits the terminal
turn, any still-attached session transition (including clearing the active turn
and updating transcript context), and one critical `turn.completed`,
`turn.interrupted`, or `turn.failed` journal event atomically in SQLite.
The deterministic terminal receipt survives restart and SSE replay; a journal
fault cannot leave a half-terminalized turn/session, and a lost receipt does
not authorize resending native work. A result from an older detached turn
cannot clear a newer active session. Provider wait errors and provider-native
approval transitions have **separate atomic recovery receipts**; the terminal
result's event does not substitute for either of them. For an active provider
wait error, the failed turn, detached session and critical `provider.error`
event are committed together. An ambiguous provider event-stream observation
instead preserves the active turn for recovery and commits its
`turn_recovery_required` session and critical
`provider.event_stream_unknown` receipt together. Journal failure rolls back
all affected state; neither outcome can blindly resend possibly executed
native work, and a later wait error cannot clear an existing quarantine.

Legacy `/v1` session **create, resume and close** also use separate guarded
SQLite transactions to couple their session projection and critical
`session.created`, `session.resumed` or `session.closed` receipt. SQLite event
faults roll back the corresponding session change; stale owners cannot
overwrite a concurrent session transition, and replay of an identical
committed receipt does not append an event. Following an acknowledged native
action, a missing/failed local receipt is an uncertain native-effect result,
**never authorization to automatically repeat the native provider call**.
The legacy native-create fallback attempts provider cleanup, but does not
claim exactly-once provider creation if its effect is uncertain. Closing an
already-closed session is state-preserving and does not call the provider
again. These lifecycle receipts do not assert authority over independent
legacy `/v1` team events or every nonterminal dispatch transition.

A **provider-native approval request** observed after turn dispatch now also
commits its pending approval identity, the waiting turn/session state, and one
critical `approval.requested` session event in a single SQLite transaction.
The native provider reference is the cross-process deduplication key:
repeated callbacks or a lost acknowledgement converge on the same approval
without duplicating the event. Journal faults roll back all the local state,
and legacy records are not given invented historical events. A later
**approval decision** still crosses a separate native provider-effect
boundary and requires its own reconciliation; a request receipt never grants
permission to retry that provider action.

Before sending a provider-native **approval decision**, the runtime now makes
an exclusive SQLite compare-and-swap from pending to \`decision_pending\`
and writes one critical \`approval.decision_reserved\` event in the **same
transaction**. That receipt means only **decision reserved**, not decision
executed: if the journal insert or reservation fails, the provider is not
called, and a lost reservation acknowledgement never authorizes resending.
Competing processes and restart retries cannot claim the already-pending
native effect again. Only a provider adapter's explicit **not dispatched**
proof allows an atomic release back to pending with its own
`approval.decision_not_dispatched` event. A **separate post-native SQLite
transaction** now commits a known successful provider acknowledgement,
`approval.resolved` critical receipt and the guarded active turn/session
resumption together. Ambiguous provider results retain their original
`decision_pending` reservation and commit recovery quarantine and
`approval.decision_unknown` receipt together, without making a potentially
executed native action retryable. Journal faults roll back the outcome
transaction, a duplicate/lost commit receipt is reconciled without a new
provider decision, and a provider turn already completed concurrently is not
resurrected or used to overwrite a newer session attachment. These rules do
not claim atomicity for legacy `/v1` team and other nonterminal dispatch
event families.

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

- `GET /v2/events/stream` (operator bearer, global committed journal)
- `GET /v2/workspaces/{workspace_id}/events/stream` (operator bearer, workspace scope)
- `GET /v2/agents/{agent_id}/events/stream` (operator bearer, canonical session scope)
- `GET /v2/processes/{process_id}/events/stream` (operator bearer, process scope)
- `GET /v1/events/stream`
- `GET /v1/sessions/{session_id}/events/stream`
- `GET /v1/teams/{team_id}/events/stream`
- `GET /v1/processes/{process_id}/events/stream`

Replay endpoints:

- `GET /v2/events` (operator history, bounded metadata pages)
- `GET /v2/events/{event_id}` (operator single-event details)
- `GET /v1/events`
- `GET /v1/sessions/{session_id}/events`
- `GET /v1/teams/{team_id}/events`
- `GET /v1/processes/{process_id}/events`

The new **operator-only** `GET /v2/events` lists lightweight, redacted event
metadata in **global row ID ascending** order. It accepts `cursor` (a
nonnegative global row ID), `limit` (1–100, default 50), and an optional
**paired** `scope` / `scope_id` filter. Valid scopes are `session`, `workspace`,
`team`, `process`, `worktree`, `system`, `turn`, `message`, and `operation`. The cursor is always the global
`row_id`, **including within scoped filters**; it is never the per-scope
`seq`. The `X-Next-Cursor` response header is present when further rows exist.
Positive cursors must identify a real event row in the selected global or
`scope`/`scope_id` collection; unknown, deleted, or cross-scope anchors return
HTTP `400` instead of silently skipping history. `cursor=0` is accepted as a
beginning-of-history sentinel.
Separate global and scoped SQLite indexes back the bounded reads.
`turn`, `message`, and `operation` select indexed subject links to canonical
parent-scoped journal rows rather than generating copies or new per-subject
sequence numbers. The response retains the event's **original** primary
`scope`, `scope_id`, and `seq`; these need not equal the requested subject
filter. New message send and operation admission/transition records are
published in the same durable transaction as the authority mutation. Earlier
events are linked where they already carry validated subject metadata.

Each summary includes only `row_id`, `event_id`, `scope`, `scope_id`, `seq`,
`kind`, `criticality`, and `created_at`. Payloads, provider/native metadata,
and per-session/turn IDs are not selected for list pages. Inspect the complete
authenticated event, including its payload, with
`GET /v2/events/{event_id}`. This is a read-only history contract, separate
from the four operator-only `/v2` SSE routes above. **History page `cursor` is
not an SSE resume token.** The global `/v2/events/stream` accepts `after_seq`
as a global row ID; resource streams use `after_seq` as their scoped sequence,
where an agent stream maps to the canonical session event scope. All four
allow `Last-Event-ID` when `after_seq` is omitted, require nonnegative cursors
and a stream page `limit` between 1 and 10000 (default 2000), and reject
unknown query fields or duplicate `Last-Event-ID` headers. They check the
referenced workspace, agent or managed process exists before opening a stream;
the operator bearer credential is required even for archived/terminal history.
The global `/v2/events/stream` additionally accepts paired `scope` and
`scope_id` to follow a single subject or primary scope. For `turn`, `message`,
and `operation` subjects the SSE event ID and resume cursor are global row IDs
across the original parent scopes; for other primary scopes, the cursor
remains the scope-local sequence. Resource-specific streams reject query-level
scope overrides. Existing `/v1` cursor behavior is unchanged.
Legacy `/v1` contracts remain unchanged during migration.

Behavior:

- stream endpoints replay first, then deliver live events
- session/global/team/process streams (including `/v2` global/workspace/agent/process) subscribe before journal replay, read bounded
  durable pages, and wake on committed SQLite event inserts instead of
  periodically polling while idle; lagged notifications catch up from the last
  delivered cursor without re-emitting it
- an instrumented real-SQLite stream regression verifies a single initial
  journal query, **zero additional reads across idle intervals longer than
  the old 250 ms polling cadence**, and one catch-up query after each
  independently committed event; 10-second network keepalives do not
  poll the database
- deterministic concurrency/restart tests prove independent SQLite writers
  cannot duplicate global or scope-local cursors, and fresh runtime stores
  can resume global, workspace, agent and process SSE streams after restart
  without replaying the last acknowledged event
- turn/message/operation subject-filtered streams use the same
  subscribe-before-replay handoff: one committed event can wake all three
  subscriptions while unrelated parent-scoped events remain excluded. A
  subject cursor is the last **delivered global row ID**, so gaps caused by
  events for other subjects are expected and reconnect skips the acknowledged
  boundary without losing a commit that raced initial replay
- a committed 320-event real SQLite burst proves a bounded SSE output queue
  and an overrun in-process notification ring cannot lose or duplicate event
  rows: the stream recovers all IDs in order from indexed journal pages
- process streams check owner/workspace visibility through the process manager
  before opening their scoped journal stream
- process queued/running/cancel-request/terminal authority commits its critical
  lifecycle journal event in the same SQLite transaction, with one deterministic
  event ID per transition; failed journal writes roll back the state mutation,
  and a legacy in-memory broadcast failure does not erase the durable SSE row.
  `process.output` samples remain intentionally droppable. Migrating an old
  database does not fabricate older lifecycle events.
  An authenticated native-process HTTP regression executes a disposable command,
  reopens its SQLite journal, and checks `queued` / `started` / `completed`
  process events in strictly increasing scoped order; `/v2` process SSE replay
  and `Last-Event-ID` resume deliver each committed row once, and an
  unauthenticated process stream is rejected.
- ordinary and Idempotency-Key-bound operator turn admission commits the
  `turn.admitted` critical event with the turn, admission snapshot, active
  session claim and operator native-effect intent in the **same SQLite
  transaction**; journal failure rolls back the whole admission before any
  provider send. The runtime's post-admission broadcast reuses the committed
  deterministic `evt_turn_<turn_id>_admitted` receipt rather than inserting
  another journal row. A real authenticated operator lost-response/replay
  regression checks one canonical event per logical turn. Historical turns
  are not backfilled, and other turn/approval events are audited separately.
- when policy requires approval *before* native turn dispatch, that same
  admission transaction commits the `waiting_for_approval` session and turn,
  pending approval and deterministic critical `approval.requested` receipt.
  On failure to insert this event, **the entire turn/approval/operator intent
  rolls back** before calling the provider. The in-process live bridge reuses
  the committed receipt without converting a transient event-lookup failure
  into a retryable admission. A real authenticated workspace interrupt/approval
  test verifies exactly one ordered `turn.admitted` / `approval.requested`
  pair for a waiting turn; provider-native approvals remain a separate audit.
- `after_seq` query param takes precedence
- otherwise stream endpoints use the `Last-Event-ID` header
- invalid `Last-Event-ID` returns HTTP `400`
- duplicate `Last-Event-ID` headers also return HTTP `400`, including with
  an explicit `after_seq`, rather than silently choosing a resume position
- keepalive pings are sent every 10 seconds
- stream journal failures emit a retryable `stream.error` without a
  replacement event ID, then close; reconnecting with the last successfully
  processed `Last-Event-ID` replays unread retained rows rather than skipping
  them (verified with an injected SQLite read fault)

The runtime currently **does not prune committed event-journal rows**. This is
intentional: pending operations, delivery recovery, audit history, and SSE
resume tokens depend on durable records; the per-scope sequence is derived from
the persisted journal tail. Do not manually delete old event rows to manage
storage. Any future retention policy must first persist monotone cursor
high-water marks and prove that no recovery-required evidence or live reconnect
cursor can be lost. A fresh runtime reconstructs global and per-scope replay
from the same indexed journal, rather than treating an in-memory stream as
history.

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

- Codex: `gpt-6-astra`, `gpt-6.1-sol`, `gpt-6-luna`, `gpt-6.1-terra`
- Claude: `claude-opus-5-5`, `claude-fable-5-2`, `claude-haiku-5-5`, `claude-sonnet-5-5`
- ACP: can return an empty list because model selection can be session-scoped inside the configured agent

`GET /v1/providers/{provider}/models` returns `id`, `display_name`, and
provider-owned `reasoning_levels` for each model. These are raw runtime
capability tokens, such as Codex `xhigh`, and clients should use them directly
when populating reasoning-effort controls. The list can be empty when a model
does not expose a global selector.

The model/skill discovery POST routes reject `Idempotency-Key` with
`400 idempotency_not_supported`. Discovery may probe or start a native
provider runtime, but does not journal an exact caller-replay receipt.

The v2 provider surface makes optional behavior explicit. Its individual
capability and discovery routes are available only for currently enabled
adapters: a registered but disabled provider returns typed `404 not_found`
without probing or starting its native discovery process.

- `GET /v2/providers/{provider}/capabilities` returns provider capability support plus the tracked harness contract metadata.
- `POST /v2/providers/{provider}/models/discover` accepts optional `cwd`, typed `setting_sources_intent`, `force_refresh`, and `startup_mode` (`cold` or `start_runtime`). The response includes a discovery `mode` plus typed model descriptors.
- `POST /v2/providers/{provider}/skills/discover` accepts optional `cwd`, typed `setting_sources_intent`, and `force_refresh`, and returns a discovery `mode` plus provider-tagged skills.

Discovery request objects reject unrecognized JSON properties with
`invalid_request`, so a misspelled option never silently falls back to a
default or triggers a provider call with unintended settings.

`mode` is `catalog`, `agent_managed`, or `unsupported`. ACP model discovery is currently `agent_managed`, so its empty model array must not be interpreted as a built-in empty catalog. Codex skill discovery is `catalog` mode and delegates to its app-server `skills/list` method; Claude and ACP skill discovery remain unsupported by the shipping adapters.

`setting_sources_intent` is one of `{"kind":"standard"}`,
`{"kind":"explicit","sources":["user","project","local"]}`, or
`{"kind":"isolated"}`. Project/local sources require `cwd`.
The tagged setting-source and permission-intent policy variants reject extra
properties, including a `sources` array on an `isolated` policy or a `mode`
on `provider_default`. Existing legacy setting-source arrays and permission
strings remain readable for migration compatibility.

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

The operator-only `/v2` process history is a bounded, indexed read surface:

```bash
curl -fsS -H "Authorization: Bearer $TOKEN" \
  "$BASE_URL/v2/processes?workspace_id=$WORKSPACE_ID&limit=50"
curl -fsS -H "Authorization: Bearer $TOKEN" \
  "$BASE_URL/v2/processes/$PROCESS_ID"
```

`GET /v2/processes` returns a JSON array, newest admitted process first, with
an optional exact `workspace_id` filter. `limit` defaults to 50 (1–100),
and an `X-Next-Cursor` header supplies the next keyset page when there are more
rows. The cursor uses `(admitted_at, process_id)` descending order; the database
uses separate global and workspace indexes instead of scanning all stored
commands or doing in-memory pagination. A cursor must match a real process
admission timestamp and the selected workspace when filtered; missing,
fabricated, or cross-workspace cursors return HTTP 400. Each list item includes only
`process_id`, `workspace_id`, `owner_agent_id`, `status`, `admitted_at`,
`started_at`, `ended_at`, and `completion_state`. Neither process commands,
native PIDs, invocation keys nor full log paths are loaded into list results.

`GET /v2/processes/{process_id}` deliberately returns the existing detailed
operator process record, including command and log locations, for explicit
diagnosis. Both endpoints require the operator bearer token. Native process
launch/cancel and scheduler controls remain under `/v1/processes` until the
corresponding `/v2` effectful operations have durable idempotency/recovery
semantics; `POST /v2/processes` is not supported.

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

Legacy team **create, join, member removal and lead change** now commit their
roster mutation together with one Critical canonical team event in an atomic
SQLite transaction. A failed journal insert does not leave a partially
committed team/member row or advance the in-process roster cache. A previous
exact durable receipt can be replayed after restart without a second event;
conflicting writer generations or reused event IDs fail closed. `/v1` HTTP
requests do **not** expose the keyed `/v2` operator idempotency contract, and
this guarantee does **not** extend to legacy team deletion, messages, delivery
transition notifications or native side effects; uncertain outcomes must
not be treated as permission to retry potentially executed effects.

## MCP gateway

The **model-facing** scoped MCP API is separate from operator authentication:

- `GET /v2/mcp/capabilities`: authenticated with a per-agent bearer token derived by the runtime; returns active tool names and policy.
- `POST /v2/mcp/invoke`: authenticated with the same agent token. JSON contains `namespace` (optional), `tool_name` (required) and `args` (mode-discriminated JSON). `caller_agent_id`, `workspace_id`, `team_id`, and `invocation_id` are **not accepted** in the request body. Internal call correlation is supplied by the host via `x-gg-tool-invocation-id`.

The runtime resolves the trusted caller from the credential and rejects archived agents, retired workspaces, and closed sessions. The operator bearer token cannot be substituted. Requests are limited to **64 KiB**. The three model tools are `gg_team`, `gg_message`, and `gg_process`, omitted when their corresponding features are disabled. Native Codex sessions invoke these same tools through in-process `item/tool/call` RPCs with host-bound caller and invocation identity; Codex does not use the scoped MCP HTTP endpoint.

The scoped gateway accepts exactly one agent bearer `Authorization` header;
duplicates return HTTP `401`. It also rejects multiple
`x-gg-tool-invocation-id` headers with a non-retryable
`invalid_invocation_id` tool error before dispatch, rather than choosing a
possibly ambiguous replay identity. The scoped gateway has its own error
contract and does not adopt the operator `/v2` request-ID envelope.

```json
{"namespace":"gg_message","tool_name":"gg_message","args":{"mode":"direct","agent_id":"sess_recipient","message":"Hello"}}
```

`gg_team` accepts `status`, `add`, `remove`, `assign_worktree`; `gg_message` accepts `direct` and `broadcast`; `gg_process` accepts `run`, `list`, `status`, `cancel`. Each mode validates its own fields and rejects unrelated/unknown fields. A `gg_team` add commits mandatory system onboarding with an optional prompt and ordered image attachments. The returned `onboarding.state` reports queue/dispatch status, not successful model delivery. Worktree cleanup on removal is not enabled via model tools yet. Processes notify their owner of terminal completion automatically.

The `gg_team` status response includes the optional friendly `lead_alias`, each active member's `alias`, `role`, `title_provenance`, creator provenance, activity and `context_remaining_pct`. The context field is `null` without a current provider-session-bound observation; it is never reconstructed from transcript estimates. Attached worktree name, branch and routing state are included where inventory can prove them. Eligible available worktrees are sorted deterministically and capped at 20, with `available_worktrees_total` and `available_worktrees_truncated` describing the full eligible set. The roster remains queryable when Git inventory is unavailable. Agent-message delivery context uses compact `<agent_msg>` envelopes (`dm|broadcast`, `workspace_team|global_direct`, attachment count), not a raw team-ID-bearing model prompt.

With SQLite, the model-facing roster is read from an indexed **alias-ordered first page of 100 active members**, with a separate indexed full active count. Lead and displayed creator aliases are resolved in one bounded lookup even if their agents fall outside the page; archived or foreign-workspace agents do not supply active aliases. All three results share one read snapshot. `members_total` and `members_truncated` still reflect the entire active workspace roster, not just the page.

Status renders at most 100 active member details. Their live session states are
captured under one bounded runtime read lock and current context observations
are fetched by a single indexed SQLite batch keyed by those agent IDs. The
same persisted native provider identity, agent revision and lifecycle fences
apply after a restart, so this optimization cannot revive stale context usage.

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

The following legacy error shapes apply to `/v1` endpoints. Operator
`/v2` routes instead return the typed `OperatorErrorResponse` envelope
described above; malformed or unknown-field JSON rejected by Axum
deserialization is HTTP `422 invalid_request`, while semantic validation
failures often return HTTP `400 invalid_request`.

Common legacy behavior:

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

Run a real, credential-free operator HTTP smoke against a disposable
loopback runtime with its own SQLite database:

```bash
make check-api-docs-smoke
```

The smoke covers workspace registration, bounded keyset pagination and
`X-Next-Cursor`, typed request errors, idempotent replay across restart,
strong revision `ETag`/stale `If-Match`, operator-versus-agent Bearer
separation, and event history/cursor rejection. The configured ACP
executable is an inert tripwire; it must **never be launched**. No
Codex/Claude/ACP session or device is exercised, and no installed
runtime/database is changed. This is **not** authenticated live-provider
acceptance.

Workflow reference: [API Doc Sync Workflow](/docs/api-doc-sync)
