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

Operator bearer routes require exactly one `Authorization` header; duplicate
values return `401` rather than choosing a proxy-dependent credential. The
agent-scoped `/v2/mcp/**` gateway has independent authentication.

## Runtime + Meta

- `GET /health` (Public)
- `GET /openapi.yaml` (Public)
- `GET /v2/capabilities` (Bearer; live aggregate API/harness/tool/provider capability snapshot, with actual enabled model-facing tools)
- `GET /v1/health` (Bearer)
- `GET /v1/openapi.yaml` (Bearer)
- `GET /v1/version` (Bearer)

Operator `/v2` responses carry a server-minted `X-Request-ID`. Error responses
contain `{ "error": { "code", "message", "retryable", "details" },
"request_id" }`, including framework-level 4xx rejections. Legacy `/v1`
error bodies and the separate agent-scoped `/v2/mcp` tool contract are unchanged.
Journaled operator mutations also return the exact durable `X-Operation-ID`
on success, including when a keyed request replays the original result.
`202` retirement responses identify a frozen operation, not native completion;
provider rebind and edit records retain their specialized detail routes.

## Workspace Authority + Durable Operations

- `POST /v2/workspaces` (Bearer, strict JSON `canonical_root` and optional `display_name`; unknown fields rejected; optional `Idempotency-Key` header)
- `GET /v2/workspaces` (Bearer; keyset pagination with optional `cursor`, `limit=1..100` default 50; `X-Next-Cursor` when another page exists; cursor must identify an existing workspace at its exact creation timestamp)
- `GET /v2/workspaces/{workspace_id}` (Bearer)
- `POST /v2/workspaces/{workspace_id}/retire` (Bearer, JSON `expected_revision`, optional `Idempotency-Key`; atomic retirement only when no session or asynchronous obligations exist)
- `POST /v2/workspaces/{workspace_id}/retirement/advance` (Bearer, JSON `expected_revision`, required `Idempotency-Key`; durable populated-workspace retirement, returning operation details)
- `POST /v2/workspaces/{workspace_id}/retirement/{operation_id}/reconcile` (operator Bearer, required `Idempotency-Key`; audited resolution of one exact quarantined effect using independently verified native closure or durable settlement facts)
- `GET /v2/workspaces/{workspace_id}/retirement-preflight` (Bearer; advisory, read-only blocker codes and counts; retirement independently rechecks every obligation)
- `GET /v2/workspaces/{workspace_id}/hooks` (Bearer; optional `event` filter, read-only effective hook configuration plus exact executable post-event capability list)
- `GET /v2/workspaces/{workspace_id}/hooks/approval` (Bearer; current operator-granted hook snapshot and revision)
- `PUT /v2/workspaces/{workspace_id}/hooks/approval` (Bearer, JSON; `expected_revision` and resolved event/name/fingerprint selections, optional `Idempotency-Key`, response `ETag` and `X-Operation-ID`; empty selection revokes all grants)
- `GET /v2/workspaces/{workspace_id}/hooks/effects` (Bearer; redacted, indexed newest-first hook effect summaries; `limit` from 1 to 100 and optional `cursor`, response `X-Next-Cursor`; cursor must identify an exact effect/time in this workspace)
- `GET /v2/workspaces/{workspace_id}/hooks/effects/{parent_event_id}/{event_name}/{hook_name}` (Bearer; full approved hook, input and receipt for one exact durable effect)
- `GET /v2/workspaces/{workspace_id}/worktrees` (Bearer; managed Git inventory, native eligibility, attached agents and per-record revisions; `limit=1..100` with `cursor` and `X-Next-Cursor` for name/ID-ordered pages; cursor must identify a currently present worktree in this repository)
- `POST /v2/workspaces/{workspace_id}/worktrees` (Bearer, JSON; create/reuse a named workspace managed worktree; optional `Idempotency-Key` durably admits Git/init intent and replays its exact result; `X-Operation-ID` on success and on `409 outcome_unresolved` with `error.details.operation_id`)
- `POST /v2/workspaces/{workspace_id}/lead` (Bearer, strict JSON; only `lead_agent_id` and `expected_revision` accepted; optional `Idempotency-Key` header)
- `GET /v2/workspaces/{workspace_id}/notice-policy` (Bearer; current lifecycle notice policy and revision)
- `PUT /v2/workspaces/{workspace_id}/notice-policy` (Bearer, JSON; compare-and-set policy by `expected_revision`; optional `Idempotency-Key` for durable exact retry replay; strong `ETag` and `X-Operation-ID` on success)
- `POST /v2/workspaces/{workspace_id}/interrupt` (Bearer; no request options (`{}` legacy-compatible); optional `Idempotency-Key` header)
- `POST /v2/workspaces/{workspace_id}/agents` (Bearer, strict JSON including native preferences and onboarding skill metadata; optional operator-scoped `Idempotency-Key`, generated one-shot key when omitted, strong agent `ETag` and exact `X-Operation-ID` for every success. Admission reserves immutable request+agent ID and durable native provider intent **before Git worktree creation, approved pre-add hooks or provider startup**. `worktree.mode=new` has its own separately journaled Git operation under a derived child key, linked to the parent launch snapshot. Successful provider attachment/session/profile/onboarding/worktree claim and parent operation receipt commit atomically. Exact-key replays never create another provider session or checkout. Unknown native Git/provider/SQL outcomes and crashes are quarantined in `manual_review` with `409 outcome_unresolved`, not blindly replayed)
- `POST /v2/workspaces/{workspace_id}/agents/creations/{operation_id}/resolve` (Bearer, strict JSON with `resolution: created|not_created`, 8–2048-byte independent `evidence`, exact originally reserved `reserved_agent_id`, `provider`, and—for `created` only—the observed `observed_provider_session_ref` and nullable `observed_canonical_provider_session_ref`. Rejects `Idempotency-Key` and unsupported `If-Match`; review makes no native provider/Git/shell calls. Verified `created` requires the immutable frozen launch policy, atomically reconstructs agent/onboarding, consumes original key as a completed exact result and fences `create_recovery_required` until a new startup verifies the exact provider attachment. Verified `not_created` consumes the original key with nonretryable `409 operator_verified_not_created` without deleting an independently created worktree)
- `GET /v2/workspaces/{workspace_id}/agents` (Bearer; optional `lifecycle=active|archived|all` defaults to `active`; keyset `cursor`, `limit=1..100` defaults to 50; `X-Next-Cursor` when more; cursor must match an existing agent in that workspace and lifecycle at its exact creation timestamp)
- `GET /v2/workspaces/{workspace_id}/agents/{agent_id}` (Bearer; strong agent-revision ETag)
- `GET /v2/agents/{agent_id}` (Bearer; global lookup of a workspace-owned agent, including archived history, with strong agent-revision ETag)
- `GET /v2/workspaces/{workspace_id}/agents/{agent_id}/onboarding` (Bearer; durable system notice, correlation and dispatch outcome)
- `POST /v2/workspaces/{workspace_id}/agents/{agent_id}/worktree` (Bearer, JSON; mandatory `expected_revision`, optional matching strong `If-Match` with typed `412` mismatch, optional `Idempotency-Key`, revisioned provider-evidence-backed rebind returning new agent `ETag` and `X-Operation-ID`; ambiguous native results return typed `409` with `error.details.operation_id` and `X-Operation-ID`, including same-key retry without redispatch)
- `GET /v2/workspaces/{workspace_id}/agents/{agent_id}/rebinds/{operation_id}` (Bearer, durable provider-rebind status)
- `POST /v2/workspaces/{workspace_id}/agents/{agent_id}/rebinds/{operation_id}/cleanup` (Bearer; no request options (`{}` legacy-compatible); rejects `Idempotency-Key` until an exact retry receipt exists; retry pending previous-worktree cleanup after verified rebind, without repeating provider dispatch)
- `POST /v2/workspaces/{workspace_id}/agents/{agent_id}/archive` (Bearer, strict JSON `{"reason":string|null}`; unknown properties rejected; optional operator-scoped `Idempotency-Key`, durable pre-close native intent, exact successful/no-op receipt and agent `ETag`/`X-Operation-ID` even on one-shot unkeyed requests. Only matching strong native close evidence permits atomic session+roster+lead+notice+receipt commit. An unknown/native-mismatched close, SQL receipt failure or crash retains the original exclusive claim in `manual_review`, returns `409 outcome_unresolved`, and never dispatches a second native close on same-key replay)
- `POST /v2/workspaces/{workspace_id}/agents/{agent_id}/archives/{operation_id}/resolve` (Bearer; strict JSON `resolution: closed|not_closed`, independently verified 8–2048-byte `evidence`, `provider`, exact `original_provider_session_ref`, nullable `original_canonical_provider_session_ref`, original `expected_revision` and `session_updated_at`. `not_closed` additionally requires observed provider/canonical native refs; `closed` must not include observed refs. Matching optional strong `If-Match`, rejects `Idempotency-Key`, durable exact review replay with `X-Operation-ID`. Verified `closed` atomically archives without another provider RPC; `not_closed` consumes original key with nonretryable `409 operator_verified_not_closed`, keeps the active agent fenced in `archive_recovery_required` until a fresh startup verifies its exact native reattachment)
- `POST /v2/workspaces/{workspace_id}/agents/{agent_id}/restore` (Bearer; no request options (`{}` legacy-compatible); optional operator-scoped `Idempotency-Key`, durable native resume intent and exact success/no-op receipt, agent `ETag`, `X-Operation-ID` even on a server-keyed unkeyed request. An unknown native resume, failed atomic SQL receipt, or crash yields durable `manual_review`, `409 outcome_unresolved` with operation ID, and never re-invokes the provider on a same-key replay)
- `POST /v2/workspaces/{workspace_id}/agents/{agent_id}/restores/{operation_id}/resolve` (Bearer, strict JSON with `resolution: resumed|not_resumed`, independent 8–2048-byte `evidence`, original `provider`, `original_provider_session_ref`, nullable `original_canonical_provider_session_ref`, `expected_revision`, `session_updated_at`; for `resumed` also provide verified `observed_provider_session_ref` and nullable `observed_canonical_provider_session_ref`. Optional matching strong `If-Match`, rejects `Idempotency-Key`, returns exact replayable review and `X-Operation-ID`. `not_resumed` consumes the original restore key with nonretryable `409 operator_verified_not_resumed`; `resumed` restores durable roster without re-dispatch but blocks native work until fresh startup verifies exact provider reattachment)
- `GET /v2/agents/{agent_id}/context-limit` (Bearer; `null` until provider evidence exists for the current attachment)
- `POST /v2/agents/{agent_id}/context-limit/refresh` (Bearer; no request options (`{}` legacy-compatible); rejects `Idempotency-Key`; refresh provider context with atomic identity validation)
- `POST /v2/agents/{agent_id}/preferences` (Bearer, JSON; revisioned Codex/Claude thinking preferences; every `/v2` write durably journals native intent and returns `X-Operation-ID`. Caller `Idempotency-Key` enables exact successful retry replay; when omitted, a generated one-shot key still fences ambiguous effects but cannot replay across HTTP retries. Ambiguous/crashed-startup outcome quarantines agent in `manual_review` without native retry and returns nonretryable `409 outcome_unresolved`)
- `POST /v2/agents/{agent_id}/permission` (Bearer, JSON; revisioned Claude permission selection; same durable native-effect journal and optional caller-key exact replay; unkeyed requests receive one-shot immutable operation IDs. Ambiguous outcome returns `409 outcome_unresolved` with durable agent claim)
- `POST /v2/agents/{agent_id}/native-policy/{operation_id}/resolve` (Bearer, strict JSON with `resolution: applied|not_applied`, 8–2048-byte `evidence`, exact `provider`, `provider_session_ref`, nullable `canonical_provider_session_ref`, `expected_revision`, `session_updated_at`; optional matching `If-Match`, rejects `Idempotency-Key`; operator must independently verify the original native attachment. Atomically settles an uncertain provider policy without dispatch, retains immutable evidence, releases exact claim and returns durable review receipt with `X-Operation-ID`. Applied commits the original requested policy and increments its revision; not-applied consumes original key with nonretryable `409 operator_verified_not_applied`. Both require a successful runtime restart/provider reattachment before further native work)
- `POST /v2/agents/{agent_id}/turns` (Bearer, JSON; durable user turn with optional `turn_settings` and operator-scoped `Idempotency-Key`; same key replays the original logical-turn receipt with `X-Operation-ID`, unknown native outcomes are nonretryable `409 outcome_unresolved`)
- `POST /v2/agents/{agent_id}/turns/{turn_id}/edit-rerun` (Bearer, strict nested snapshot/image/skill JSON, unknown properties rejected before native effects; optional `Idempotency-Key`; native hard-fork and new durable user turn)
- `GET /v2/agents/{agent_id}/edits/{operation_id}` (Bearer; inspect native edit admission, fork and replacement identity)
- `POST /v2/agents/{agent_id}/edits/{operation_id}/resolve` (Bearer, JSON; rejects `Idempotency-Key`; explicitly settle a verified child fork with no admitted replacement)
- `POST /v2/agents/{agent_id}/compact` (Bearer; no request options (`{}` legacy-compatible); optional `Idempotency-Key`; admit provider compaction before dispatch)
- `GET /v2/agents/{agent_id}/compactions/{run_id}` (Bearer; durable compaction request state)
- `POST /v2/agents/{agent_id}/compactions/{run_id}/resolve` (Bearer, JSON; rejects `Idempotency-Key`; operator-confirmed `completed` or `failed` with external evidence)
- `GET /v2/migrations/workspaces` (Bearer; bounded subject diagnostics, `limit=1..100`, `cursor`, `X-Next-Cursor`, full-state counts; cursor must identify a subject in the current preview)
- `POST /v2/migrations/workspaces/preview` (Bearer; no request options (`{}` legacy-compatible); rejects `Idempotency-Key`; refresh and page subject diagnostics with the same cursor contract)
- `POST /v2/migrations/workspaces/apply` (Bearer; no request options (`{}` legacy-compatible; dry-run/force rejected); optional `Idempotency-Key` header)
- `POST /v2/migrations/workspaces/resolutions/{subject_kind}/{subject_id}` (Bearer, strict JSON `action` and optional `workspace_id`; unsupported fields rejected before a mapping operation; optional `Idempotency-Key` header)
- `GET /v2/operations/{operation_id}` (Bearer)
- `GET /v2/operations` (Bearer; indexed newest-first durable operation summaries, optional `workspace_id`, `cursor`, `limit=1..100` default 50; `X-Next-Cursor` when more; cursor must match the exact operation ID/time in the selected workspace)

Workspace registration canonicalizes the requested filesystem root and uses that canonical root as the durable uniqueness key. Repeated or concurrent registrations of the same root converge on one workspace identity. With `Idempotency-Key`, an exact retry replays the original terminal result; a different normalized request under the same key returns HTTP `409` without changing state.

Native operator actions without a revisioned `expected_revision` contract
(including agent create/archive/restore, managed worktree creation/cleanup,
turn submission, compaction, context refresh and workspace interruption)
reject a supplied `If-Match` as `400 revision_precondition_not_supported`
before any native effect. A strong ETag is not silently ignored.
Registration, message submission/cancellation/delivery retry, migration
preview/apply/resolution, model/skill discovery, edit-and-rerun and its review,
compaction review, and retirement reconciliation follow the same rule: these
requests do not provide a revision compare-and-set guarantee.

For workspace managed-worktree creation, supply `Idempotency-Key` when a request might be retried. Every operator creation records native Git/init intent and exclusive checkout-path plus Git-branch claims before execution; without a caller key, a generated one-shot key does not permit exact retry replay. Init scripts require `run_init_script: true`; omitted deletion policy uses the configured default for new checkouts and preserves the original policy on an existing managed checkout. Explicitly requesting an incompatible policy is a preflight conflict, without native admission. A successful response includes `X-Operation-ID`, and a repeated caller key replays the original `created` and `init_script_status` values. Key reuse with different input returns `409`. If the Git or init outcome cannot be proven, HTTP `409 outcome_unresolved` includes the durable `X-Operation-ID` and `error.details.operation_id` (with `retryable: false`), while the effect remains fenced in a `requested` or `manual_review` operation. `GET /v2/operations/{operation_id}` exposes its evidence; do not retry the effect with a new key until it has been independently reconciled.

For a **quiescent** workspace with no associated session/agent authority or unsettled process, message, notice, hook, migration or workspace operation, `POST /v2/workspaces/{workspace_id}/retire` transactionally commits the retired state, durable operation receipt, identity tombstone and revision increment. This narrow endpoint rejects populated workspaces with `409`; use `/retirement/advance` for those. Registering the same canonical root after proven retirement with a new idempotency key reactivates that original workspace ID, still leadless, only after a new transactional safety check. Archived agents remain archived until explicitly restored.

`POST /v2/workspaces/{workspace_id}/retirement/advance` (Bearer, JSON `{"expected_revision":number}`, **required `Idempotency-Key`**) freezes new work and stores resource snapshots, effect intents and an exclusive workspace claim before dispatch. It locally cancels proven undispatched turns, interrupts native turns, settles processes and records verified provider closure before archival. Terminal process results remain pending until their archived owner is explicitly restored; merely reactivating the workspace does not deliver them. Durable delivery cancellations cannot be overwritten by stale workers. HTTP `200` returns a completed retirement; `202` returns the full operation details in `requested` or `manual_review`. Safe work resumes in background and after restart; ambiguous native outcomes require operator reconciliation without blind replay. Retained and external worktrees are preserved. Reusing the original operator/key/input returns the same durable operation.

For an uncertain effect, `POST /v2/workspaces/{workspace_id}/retirement/{operation_id}/reconcile` accepts `effect_id`, `evidence`, and a typed `resolution`: `provider_closed` requires the exact current native identity after independent operator verification; `recheck_settled` verifies durable terminal facts. It never reissues a native effect. The immutable receipt reports whether settlement succeeded and which effects remain unresolved. Once the uncertain effects are resolved, the original operation resumes in the background while its workspace fence remains active. Review success and terminal retirement are separate states; inspect `GET /v2/operations/{operation_id}` for completion.

Workspace leadership is nullable and revisioned. `POST /v2/workspaces/{workspace_id}/lead` accepts `lead_agent_id` (`string` or `null`) plus `expected_revision`. Non-null leads must be active members of that exact workspace, enforced by both service validation and SQLite triggers. Set/reassign/clear operations use compare-and-swap revision authority and durable operation identity. Archiving the current lead atomically clears leadership; restore never promotes it again. No workspace or legacy-team code path auto-elects a replacement lead.

Workspace membership authorization is leadless-aware: an active member can manage membership when no lead exists; once a lead is set, non-lead add/remove authority follows the configured add/remove flags. Lead assignment itself is reserved for the operator surface.

Team hook configuration discovery merges strict version-1 user and project files
by event and normalized hook name, with project overrides. Operator approval
stores immutable command snapshots using compare-and-set revision authority;
each operator change also commits a durable idempotent operation receipt. An
exact retry recovers the original approval despite later revision/config
changes; retirement freezes new grants. Unapproved config files cannot grant
shell execution. Five post-operation
events (`post_teammate_add`, `post_teammate_remove`, `post_direct_message_send`,
`post_broadcast_message_send`, `post_compaction`) now create immutable effect intents in their
canonical transaction. An approved worker executes each script at most once
and applies validated, recipient-scoped automation context with a separate
durable receipt. Approved `pre_teammate_add` scripts are admitted before
provider creation and may modify only validated workspace-local cwd and
onboarding/caller context, with a fail-closed, durable recovery boundary.
`execution_enabled: true` advertises active hooks and `executable_events`
enumerates exactly these six live events. Post-compaction effects are admitted
only when the native attachment-fenced compaction lifecycle commits completion. Neither discovery nor approval alone
launches commands.

Lifecycle notices are durable system/automation-context turns, not user messages. A lead promotion/demotion, eligible non-self lead-membership change, verified worktree reassignment, completed interruption or completed compaction queues its notice in the same transaction as the authoritative change; no-op/idempotent replay does not duplicate. The revisioned notice policy has five default-enabled toggles: `lead_transitions`, `membership_to_lead`, `worktree_reassignment`, `workspace_interrupt`, `compaction_to_lead`; `compaction_to_team` defaults to false. Compaction creator notifications also respect the member's persisted `creator_compaction_subscription`. It does not suppress mandatory onboarding. Stale policy revisions return `409`. Pending notices are injected only when safe, with an unknown provider outcome held for manual review rather than blindly retried.

`POST /v2/workspaces/{workspace_id}/interrupt` snapshots the authoritative active roster and active turn IDs, records durable per-agent effect intent before provider calls, and returns sorted `interrupted_agent_ids` and `skipped_agent_ids`. Idle members are skipped, agents outside the workspace are never touched, and exact `Idempotency-Key` retries replay the original result without redispatch. Durable effect/event evidence supports restart recovery without blindly duplicating an interrupt whose provider outcome may already have crossed the execution boundary.

Workspace-agent creation is the canonical normal `/v2` agent path. The runtime reuses the opaque `sess_*` session ID as the public agent ID, assigns a globally unique durable friendly alias, and commits the session row, immutable workspace ownership, profile, active roster membership, exact recreation policy, mandatory system onboarding notice, and managed-worktree claim (when selected) in one SQLite transaction. The optional `worktree` selector accepts `root`, `existing` (managed worktree ID) or `new` (named worktree). The recreation policy contains provider, model, permission intent, setting-source intent, mutable current preferences, system prompt, allowed/disallowed tools, authoritative cwd, and harness-version slot. Provider-native session references remain separate from both the recreation policy and the public ID/alias. `onboarding` accepts optional task text, native image paths, and Codex skill metadata; unknown provider outcomes remain `manual_review` rather than being automatically replayed. Use the onboarding GET route to inspect the durable status and admitted turn reference.

Archiving removes the agent from the default active roster and makes its runtime session non-writable while preserving history, alias, immutable workspace ownership, provider identity, and recreation policy. `restore` reuses the same opaque agent ID and alias and resumes the provider with the persisted recreation policy. Archived history is available with `lifecycle=archived`; `lifecycle=all` returns both active and archived records.
The archive/restore persistence transaction refuses unresolved native operator
policy/turn/restore claims or a reviewed policy/restore awaiting verified provider reattachment,
including when called outside the normal session manager path.

The operation resource exposes durable transition, resource-claim, effect, outbox, and receipt evidence for the operation. Completed registration operations have released their live canonical-root claim, while the transition evidence retains the fence generation used for the commit.

Legacy workspace migration preview classifies every persisted legacy authority subject as `mapped`, `archived_history`, or `unresolved`. Deterministic mapping uses canonical repository and worktree evidence rather than display names. Apply consumes the persisted preview, creates workspace/session authority only for proven mappings, leaves unresolved subjects without guessed ownership, and reports `cutover_blocked: true` while any unresolved subject remains. The resolution endpoint lets an operator explicitly map an unresolved subject to an existing active workspace or archive it as history. Legacy `/v1` rows and read routes are not rewritten by this migration.

## Agent Messages + Deliveries

- `POST /v2/messages` (Bearer, JSON; optional `Idempotency-Key` header)
- `GET /v2/messages` (Bearer; optional `workspace_id`, `sender_agent_id`, `cursor`, `limit=1..100` default 50; JSON `next_cursor`)
- `GET /v2/messages/{message_id}/deliveries` (Bearer; optional `recipient_agent_id`, `cursor`, `limit=1..100` default 50; `X-Next-Cursor` for subsequent array pages; cursor must match the exact delivery ID/time in that message and recipient filter)
- `POST /v2/messages/{message_id}/cancel` (Bearer; no request options (`{}` legacy-compatible); optional `Idempotency-Key`; response `X-Operation-ID`)
- `POST /v2/deliveries/{delivery_id}/retry` (Bearer; no request options (`{}` legacy-compatible); optional `Idempotency-Key`; `X-Operation-ID` only for keyed exact pending admission)

Message history is paged by the durable indexed `(created_at, id)` keyset, with stable cursor validation against the requested workspace/sender filters. Only selected page bodies are read and decoded, not the entire runtime message history.

Keyed operator retry atomically accepts a failed/deferred canonical delivery into `pending` along with its legacy mirror and records a completed operation receipt *before* attempting native delivery. The replayed JSON delivery is the original pending admission snapshot, not a claim about a later native outcome; a key replay does not send another turn. Unkeyed retry preserves the synchronous delivery-result semantics for compatibility. Read the delivery record again for its current state, and never infer success from the retry admission receipt alone.

Canonical delivery dispatch acquires a serialized durable `pending`/`deferred` → `injecting` status claim with matching legacy mirror, and rejects retired/frozen owners inside the claim. A stale worker cannot take the same status/version claim or rewrite an `injected`/`cancelled` terminal delivery. This guards competing native dispatch claims but does not replace provider-side admission reconciliation for crash-unknown outcomes.

Canonical `/v2` messages are agent-first and do not require a legacy team ID. Direct routing can target any active/routable agent in the runtime: same-workspace direct messages use `workspace_team` context and cross-workspace/migration-compatible directs use `global_direct` context. Workspace broadcasts derive the sender workspace from canonical ownership, snapshot the active roster once, exclude the sender, and never fan out across workspace boundaries.

Message creation commits the canonical message and every recipient delivery atomically. `Idempotency-Key` replays an exact duplicate and returns `409` if the same scoped key is reused with different normalized message input. Delivery status is durable per recipient; failed/deferred deliveries can be retried. Operator cancellation atomically cancels every recipient only if all remain pending/deferred, with an exact durable operation receipt at `/v2/operations/{operation_id}`. Repeating the same cancellation `Idempotency-Key` replays the original response even after a lost HTTP reply; different messages cannot reuse that key. Once a delivery is cancelled, a stale worker cannot overwrite its terminal cancellation. Startup recovery resumes pending/deferred canonical work without duplicating migrated legacy delivery injection.

`image_paths` accepts up to eight ordered PNG, JPEG, GIF, or WebP files that must be regular/readable and match a supported byte signature. The current v2 transport delivers them as native structured image input to Codex and Claude; ACP rejects image-bearing v2 messages explicitly rather than silently flattening or dropping attachments. Validation errors are path-redacted.

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
- `GET /v2/providers/{provider}/capabilities` (Bearer; only enabled providers; disabled or absent adapters return `404 not_found`)
- `POST /v2/providers/{provider}/models/discover` (Bearer; only enabled providers; unknown properties rejected; `Idempotency-Key` not supported; disabled adapters cannot be probed)
- `POST /v2/providers/{provider}/skills/discover` (Bearer; only enabled providers; unknown properties rejected; `Idempotency-Key` not supported; disabled adapters cannot be probed)

Provider behavior notes:
- `GET /v1/providers/{provider}/models` returns dynamic, provider-owned `reasoning_levels` using raw runtime capability tokens such as Codex `xhigh`.
- v2 capabilities distinguish `supported`, `agent_managed`, and `unsupported` optional behavior and include harness contract metadata.
- v2 discovery distinguishes `catalog`, `agent_managed`, and `unsupported`. Model discovery accepts cwd, typed setting-source intent, refresh intent, and `cold`/ `start_runtime` startup intent; skill discovery accepts cwd, typed setting-source intent, and refresh intent.
- Codex skill discovery is `catalog` mode and delegates to Codex app-server `skills/list`; Claude and ACP currently report skill discovery as unsupported.
- ACP v1 exposes only `GET /v1/providers/acp/auth/status` for auth. No ACP logout, API-key, JSON import, or file import routes are implemented.
- ACP auth status is agent-managed. The response reports configuration/readiness, not runtime-owned credentials.
- `GET /v1/providers/acp/models` may return an empty list because ACP model selection can be driven by session-scoped agent config.
- `POST /v2/providers/acp/models/discover` reports `agent_managed` and does not invent model entries for the external agent.
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

- `GET /v2/events/stream` (Bearer; global durable SSE, global SQLite row ID cursor)
- `GET /v2/workspaces/{workspace_id}/events/stream` (Bearer; workspace-scoped durable SSE, requires workspace record)
- `GET /v2/agents/{agent_id}/events/stream` (Bearer; agent/session-scoped durable SSE, requires workspace-owned agent)
- `GET /v2/processes/{process_id}/events/stream` (Bearer; process-scoped durable SSE, requires managed process)

The `/v2` streams accept optional `after_seq` (nonnegative), `limit=1..10000`
(default 2000), and `Last-Event-ID` fallback. The global stream ID is the
durable event row ID; the three resource streams use their own scoped event
sequence. The global `/v2/events/stream` additionally accepts optional paired
`scope` and `scope_id` filters (including derived `turn`, `message`, and
`operation` subjects); subject streams resume using the global row ID while
primary scoped streams use their own sequence. A subject-filtered event still
reports its original parent `scope` and `seq`, without a duplicate journal row.
Resource streams reject scope overrides. Do **not** feed the paginated `GET /v2/events` `cursor` into a scoped
SSE stream; unknown query options, duplicate/invalid `Last-Event-ID` and
nonexistent resource IDs fail before the stream opens. The operator bearer is
required. The older `/v1` routes remain during migration.

- `GET /v2/events` (Bearer; indexed global-row-ID ascending, optional paired `scope` and `scope_id` including derived turn/message/operation subjects, numeric `cursor`, `limit=1..100` default 50; `X-Next-Cursor` for next page, eight-field redacted rows; positive cursor must identify an existing row in the selected global/scoped collection; zero starts from the beginning)
- `GET /v2/events/{event_id}` (Bearer; full event payload/provider diagnostic metadata, exact immutable event ID)
- `GET /v1/events` (Bearer)
- `GET /v1/events/stream` (Bearer, SSE)

Global event query parameters:
- replay: `after_seq`, `limit`
- stream: `after_seq`, `limit`, optional `Last-Event-ID` header fallback

All legacy SSE streams reject multiple `Last-Event-ID` header values with
HTTP `400`, even when the request also supplies `after_seq`; a replay cursor
must never be inferred from ambiguous proxy-provided headers.

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

- `GET /v2/processes` (Bearer; indexed newest-first admitted-process summaries, optional `workspace_id`, `cursor`, `limit=1..100` default 50; `X-Next-Cursor` when more; cursor must match the exact process ID/admission time and selected workspace)
- `GET /v2/processes/{process_id}` (Bearer; detailed native process diagnosis, including command/log locations)
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

The `/v2` collection returns eight non-sensitive metadata fields and never
materializes process commands, native PIDs or logs during list reads. Launch,
cancel and scheduler writes still use `/v1/processes`; no unguarded effectful
`POST /v2/processes` alias is exposed.

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

## MCP gateway

- `GET /v2/mcp/capabilities` (per-agent scoped Bearer credential)
- `POST /v2/mcp/invoke` (per-agent scoped Bearer credential)
- `GET /v1/mcp/capabilities` (operator Bearer; legacy compatibility)
- `POST /v1/mcp/invoke` (operator Bearer; legacy compatibility)

The `v2` model-facing invoke body accepts optional `namespace`, required `tool_name` and mode-discriminated `args`. It rejects body-supplied agent or workspace identity. Trusted `x-gg-tool-invocation-id` request metadata is supplied by the sidecar host. Duplicate invocation-ID headers fail with a non-retryable `invalid_invocation_id` tool envelope before mutation; duplicate agent `Authorization` headers fail with HTTP `401`. The three current model tools are `gg_team` (`status|add|remove|assign_worktree`), `gg_message` (`direct|broadcast`) and `gg_process` (`run|list|status|cancel`). Both the scoped credential and the active canonical workspace agent record are checked on every call. The `v1` operator-only body still accepts historical `caller_agent_id` and split-tool names as a migration surface; providers do not use it.

Codex registers those same three canonical tools as provider-native dynamic tools, with capability-gated mode-specific JSON schemas and trusted in-process caller derivation. It does not use MCP for GG tools. Claude and ACP continue to use the scoped MCP endpoint above.

## Notes on Contract Precision

OpenAPI route coverage is expanded from the same compile-time declaration table
used by the Axum router, not scraped from route-builder source text. The
generated OpenAPI has Rust/serde-derived schemas for workspace registration,
lead/retirement, operation history and agent message/delivery requests/results;
it also has a typed operator error envelope and `default` error response on
regular operator JSON routes. Other endpoint bodies may remain broad
`JsonObject` schemas. The provider capability/model/skill discovery requests and
responses also have Rust-derived schemas, including tagged and legacy
setting-source input forms. Managed-worktree creation/inventory, rebind
request and operation-detail inspection, agent archive request and onboarding
notice, notice-policy GET/PUT and redacted process history/full process detail
now use real Rust-derived DTOs too. User-turn input/accepted receipt, context
observation or null, compaction lifecycle/review, and revisioned preference
and permission mutation contracts are also Rust-derived, including the
permission parser's canonical tagged, legacy-string and null input forms.
Workspace-agent create/list/get/archive/restore and composite rebind responses,
typed independent review requests/results for ambiguous native create/archive/
restore/policy effects, and historical edit-and-rerun request/operation
inspection are also linked to real Rust/serde DTOs. The agent list remains
an array with `X-Next-Cursor`, and a rebind response contains both an
operation and an optional verified agent, not merely the agent record.
Migration status/preview, apply receipts and typed subject resolutions, full
retirement reconciliation identity/evidence requests and receipts, redacted
operator event summaries versus full single-event records, workspace effective
hook/approval/effect DTOs, and the caller-free scoped MCP invocation *request*
now derive directly from their Rust handlers. MCP capability and tool-result
payloads are intentionally dynamic JSON, not invented fixed response DTOs.
Query parameters are projected from the actual Axum `Query<T>` DTOs with
their accepted scalar/enum types and the handler's 1–100 (or stream 1–10000)
limit ranges and defaults. The generated `X-Next-Cursor` response header is
limited to genuine keyset page handlers, not message JSON-body cursors or
SSE. OpenAPI declares separate `bearerAuth` owner/operator and
`agentScopedBearerAuth` model-scoped schemes; only `/v2/mcp/*` uses the
latter, and its invoke operation documents the trusted sidecar invocation-ID
header rather than admitting caller identity in JSON. The generated response
header catalog also includes server-minted `X-Request-ID` on successful
operator JSON and SSE, explicit 202/409/412 branches and default errors.
It shares a single header map with applicable page cursors, resource ETags
and operation IDs; scoped model MCP does not advertise this operator header.
Treat this catalog + code as the reliable source for:
- endpoint existence and grouping
- auth requirements
- stream vs replay endpoints
- query parameter behavior

For the two intentionally extensible `/v2/mcp` response bodies, use the
advertised scoped tool capabilities and runtime result contract rather than
inventing fixed fields from `JsonObject`. All other canonical `/v2` response
methods use Rust-derived schemas; semantic validation and authorization
rules remain defined by the handlers.
