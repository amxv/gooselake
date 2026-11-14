---
title: "Events and recovery"
description: "Understand replayable event streams, cursors, criticality, SSE handoff, startup recovery, and diagnostics."
order: 12
category: "Mental Model"
summary: "How Gooselake remembers what happened and comes back after restarts."
---

Events are the runtime's flight recorder. They let a client reconnect, rebuild context, and explain what happened even if the original UI tab disappeared.

For a contradictory **late** native terminal callback on an already-terminal
turn, an idle session's `terminal_conflict` failure now commits with one
Critical `provider.terminal_conflict` receipt in the **same guarded SQLite
transaction**. A failed receipt write leaves the durable session unchanged;
replaying the same observation cannot publish another live notification.
The transaction verifies the original terminal turn and current session owner,
so a different manager's newly attached or closed session cannot be overwritten
by an obsolete callback. Existing `provider.late_turn_outcome` observations
for newer active owners or closed sessions remain state-preserving; neither
path proves provider-native exactly-once execution. Historical turns lacking
independent terminal receipts are not retroactively certified.

## Event scopes

Runtime events are scoped. A scope is the logbook a record belongs to.

| Scope | What it describes |
| --- | --- |
| `session` | session and turn lifecycle |
| `workspace` | workspace authority, membership and lifecycle |
| `team` | team messages, deliveries, member operations |
| `process` | runtime-managed command lifecycle and sampled output |
| `worktree` | worktree creation, claim, release, cleanup |
| `system` | runtime-level events such as startup recovery |
| `turn` | indexed subject projection of events linked to a logical turn |
| `message` | indexed subject projection of messages and linked lifecycle events |
| `operation` | indexed subject projection of durable operation admission and transitions |

Primary scopes (`session`, `workspace`, `team`, `process`, `worktree`,
`system`) retain independent sequence numbers. `turn`, `message`, and
`operation` are secondary **subject projections**, not duplicate journal
events: their cursor is the stable global `row_id`, even for events originally
published under several different parent scopes. Returned records retain
their original `scope`, `scope_id`, `seq`, and `event_id`. SQLite indexes subject
links in the same transaction as the journal append and versioned migration
backfills older linked events without rewriting their primary history.
Message publication and durable operation admission/transitions now append
redacted events in their own authority transaction. Not every historical
operation or transition necessarily generated a journal event.

On startup, SQLite verifies that the subject projection table **and complete
append-trigger SQL** match their canonical migration 27 definitions, even when
that migration is already marked complete. Missing objects or weaker, same-name
replacements (including a no-op trigger or a table without its original
constraints) stop bootstrap rather than silently losing future replay links.
This constant-sized schema attestation does **not** rebuild subject rows,
verify every historical link, or rewrite retained journal evidence; damaged
databases need independent, offline inspection before any repair.

For an **isolated, consistently checkpointed offline SQLite copy**, use
`python3 scripts/audit-event-subjects.py --snapshot /path/to/offline-copy.sqlite3`
to compare retained canonical rows against stored subject links. The audit
reports missing, unexpected, and malformed/structured-ID evidence, plus a
separate indexed primary-scope sequence-continuity check, without
writing to the database or fabricating a historical event. It refuses SQLite
WAL/journal sidecars, symlink paths, or a projection schema that differs from
the checked-out migration 27 source; exit 1 reports discrepancies, exit 2
refuses an unsafe or unsupported snapshot. A clean audit certifies **only**
source/index parity, contiguous *retained* per-primary-scope sequences starting
at one, and the installed schema definition in that copy. The report includes
the **observed** first/last global row IDs and count of primary scopes but
deliberately returns `persisted_recovery_safe_row_id: null` and
`retention_authorized: false`: a consistent snapshot cannot reveal a missing
entire scope or tail, establish a recovery-safe persisted high-water mark, or
authorize pruning. This offline SQLite inspection also does **not** verify
live or cross-process SSE notification fan-out, completeness of event-family
publication, native effect settlement, or future live database integrity.

## Operator history vs live replay

`GET /v2/events` is an operator-authenticated, bounded history index. It can
list the entire durable journal or filter by an exact `scope` and `scope_id`
pair. The cursor is **always the global event row ID** (`row_id`), not the
per-scope `seq`, and `limit` defaults to 50 with a maximum of 100. Pass the
returned `X-Next-Cursor` header verbatim to retrieve another page.

The eight-field list projection excludes payloads and provider-native details
even for events with large/private diagnostic bodies. Inspect an exact event
with `GET /v2/events/{event_id}` to retrieve its full record and payload.
The v2 collection uses database indexes, not memory scans; it does not replace
the `/v1` SSE streams or change their sequence-number contract.
Subject-filtered summaries retain the original primary `scope` and may
therefore display a different scope from the requested subject filter.

The operator can subscribe to the same committed journal through
`GET /v2/events/stream`, `/v2/workspaces/{workspace_id}/events/stream`,
`/v2/agents/{agent_id}/events/stream`, and
`/v2/processes/{process_id}/events/stream`. These routes require the operator
bearer credential and an existing workspace/agent/process for resource
streams; archived agents and terminal processes remain inspectable. Use
`after_seq` (or the `Last-Event-ID` header) rather than the **history list's**
`cursor`. On the global stream it is a SQLite event row ID; on resource
streams it is a scoped sequence. Agent events use the canonical session scope.
The global operator stream additionally accepts paired `scope` and `scope_id`.
When following a `turn`, `message`, or `operation` subject, `after_seq` and
`Last-Event-ID` are **global row IDs**, never parent-scoped sequence numbers;
the resource-specific streams reject an overriding query scope.
`/v1` and `/v2` SSE clients preserve their last received IDs across reconnects.

## Critical and droppable events

Events have criticality.

Critical events describe state transitions that clients should not casually lose: sessions created, turns completed, deliveries failed, processes started, and so on.

Canonical agent-message creation commits its initial per-recipient delivery
snapshot together with the Critical `message.sent` event. Subsequent
pre-native delivery transitions persist Critical `agent_delivery.pending` /
`agent_delivery.deferred` / `agent_delivery.injecting` /
`agent_delivery.cancelled` generation receipts in the same SQLite transaction
as the status and any legacy mirror. A positively acknowledged delivery also
requires a matching admitted logical turn; its `injected` status, turn ID,
mirror and `agent_delivery.injected` receipt commit together. Inserting the
last event can fail without leaving partially acknowledged native results.
Unknown native effects remain `injecting` for operator reconciliation rather
than being automatically resent after restart. Old unjournaled generations
are not retrospectively certified by newer code.

Droppable events are useful but less sacred. Process output samples are the clearest example. They help live UIs feel alive, but the process log files remain the authoritative output source.

For provider-observed compaction, the committed `compaction.lifecycle_changed`
Critical event shares a SQLite transaction with the manager-owned compaction
run transition and, on completion, the subscribed system notices and approved
`post_compaction` hook intents. A failed journal insert rolls all of those
changes back; a replay resolved to the **same durable compaction run** does not
publish an extra event. Provider signals without strong native IDs still use
the documented bounded weak-alias deduplication window; this is not global
exactly-once native-event identity. Unknown native outcomes are **not**
automatically retried because a journal write failed.

Manual compaction request-state transitions (`accepted`, `not_performed`,
`manual_review` and other explicit store phases) likewise commit the Critical
`compaction.request_state_changed` receipt in the same SQLite transaction as
the changed run state. A failed event insert retains the prior state and
leaves uncertain native effects fenced; an exact phase replay emits nothing.
Initial native-request admission (`compaction.admitted`), restart quarantine
(`compaction.restart_quarantined` with the previous phase) and explicit
operator settlement (`compaction.review_resolved`) also publish Critical
receipts **in the same transaction** as their changed durable run state.
Startup quarantine is all-or-nothing across the pending batch; injected event
failure rolls back all affected phases, retaining native uncertainty until a
later authorized recovery attempt. A same-key admission replay, including one
that races between independent runtime processes, returns the existing run and
must not dispatch another native request. Operator review events omit the
review evidence itself, which remains on the protected durable run record;
review always requires external native-outcome verification. Journal replay is
authoritative; broadcasts may be lost without changing the persisted state.
This proof covers the named compaction mutation paths, not arbitrary native
effect exactly-once identity, legacy historical event backfills or safe
destructive retention.

Managed-process **critical** lifecycle events (`process.queued`,
`process.started`, `process.kill_requested` and terminal outcomes such as
`process.completed` / `process.canceled`) are inserted in the **same SQLite
transaction** as the corresponding durable managed-process state. Failed
journal inserts roll back that state transition, so no process may pass its
pre-execution launch gate without a committed `process.started` receipt.
Critical IDs are deterministic per process and lifecycle transition;
idempotent cancel/terminal retries never add duplicate journal rows. The
older in-memory broadcast is best-effort; replayable SQLite journal rows,
not broadcast delivery, are the authoritative source. Migration to this
journal coupling does **not** invent historical lifecycle events for process
states recorded before the migration. Sampled `process.output` remains
droppable, and complete captured output is kept in the process log files.

`turn.admitted` is likewise inserted in the **existing SQLite admission
transaction** alongside the new turn, its immutable admission snapshot, the
session's active-turn claim, any pending approval, and (for operator-keyed
requests) its durable operation/effect/claim intent. A critical journal
failure rolls back the whole admission **before provider dispatch**; it never
returns an ambiguous success with a missing admission event. The subsequent
in-memory broadcast resolves the same deterministic event ID, rather than
creating a duplicate critical row. If policy requires **pre-dispatch manual
approval**, the same transaction also commits the pending approval,
`waiting_for_approval` turn/session state and its own deterministic critical
`approval.requested` event. A journal failure rolls all of them back; a
temporary postcommit live-projection lookup failure does not invite another
provider send or admission. This applies to **pre-dispatch policy approval**,
not every provider-originated approval callback. Old persisted turns are not
backfilled with invented admission/approval events. Other session, native
approval and turn-transition event families retain distinct authority/recovery
boundaries; this is not a blanket claim that every legacy event source is
transactionally journaled.

After a **positively acknowledged provider turn send** (including an approved
pre-dispatch manual request), the canonical native-turn mapping, `dispatched`
admission, `in_progress` turn, `turn_running` session and deterministic Critical
`turn.started` receipt commit in **one SQLite transaction**. A failed event
insert rolls back all three projections. The pre-existing `dispatching` fence
then remains for restart/operator inspection; a lost acknowledgement of the
database commit can be reconciled against its stored receipt, never interpreted
as permission to send the native turn twice. Live broadcast follows commit
and is not authoritative. This covers normal and approved send acknowledgements;
other legacy/recovery callbacks have separate acceptance boundaries.

**Known no-send and unknown native dispatch** now have their own transactional
Critical receipts. A confirmed `not_dispatched` result commits its admission,
failed turn, cleared active session, all still-pending approval cancellations
and deterministic `turn.failed` event together. An ambiguous provider result
commits the `unknown` admission, quarantined nonterminal turn, still-attached
`turn_recovery_required` session and deterministic `turn.dispatch_unknown`
event in one transaction. A late event-insert fault rolls *every* affected
projection back to the original state; in particular, a former `dispatching`
fence stays non-retryable. Startup recovery of an in-flight `dispatching`
admission uses the same unknown quarantine without sending native work again.
Conflicting independent writers or a forged prior receipt fail closed; exact
unchanged committed receipts are inspectable after restart. Historical already-
`unknown` partial projections and other nonterminal state transitions are not
certified by this specific native-send authority.

A **local pre-dispatch policy decline** is a different, provider-free decision
boundary. A new pending policy gate now commits `approval.status=decline`,
its `not_dispatched` admission, interrupted logical turn, released ready
session, and both deterministic Critical session events `approval.resolved`
and `turn.interrupted` **in one guarded SQLite transaction**. A failure in
either event insert rolls the entire denial back to `waiting_for_approval`;
no native send occurs. Independently racing writers converge on the exact
pair of receipts, while stale authority, absent original `turn.admitted` /
`approval.requested` receipts or historical partial rows fail closed rather
than being silently repaired. A fresh SQLite reader can retrieve both event
IDs and the four matching projections. This does not certify the separate
pre-dispatch **acceptance** reservation or old native/legacy recovery paths.

An observed **provider terminal result** (`turn.completed`, `turn.interrupted`,
or `turn.failed`) now commits the terminal turn, the session's active-turn
release and transcript update when that turn is still attached, and its
critical session event in **one SQLite transaction**. Each logical turn has
one deterministic terminal-event receipt. A failed event insert or an
independently changed session rolls the full transition back; an ambiguous
commit can be inspected by that durable receipt on restart without invoking
the provider again. A late result for a superseded, no-longer-active turn may
terminalize only that historical turn, never the newer active session.
If a *different* terminal outcome arrives after the old turn was already
terminal, the manager also preserves a newer active attachment or an already
closed session instead of marking that unrelated session state `failed`.
It records the rejected stale observation as a critical
`provider.late_turn_outcome` with `state_preserved: true` and the reason
`newer_turn_active` or `session_already_closed`. An ordinary conflicting
observation while the session is idle still follows the existing
`terminal_conflict` failure path. This guard is not a cross-process callback
CAS guarantee or certification of the idle-conflict event-atomicity path.
Provider wait failures, nonterminal dispatch transitions and older legacy
team paths are **separate** event-authority cases; none is implied by this
terminal-result transaction. Provider wait failures and native approval
outcomes have their own receipts below.

An **active provider wait failure** now commits the failed turn, detached
failed session and deterministic critical `provider.error` journal receipt in
one guarded SQLite transaction. A lost or ambiguous **provider event stream**
instead commits a nonterminal `provider_event_recovery_required` turn, active
`turn_recovery_required` session and deterministic critical
`provider.event_stream_unknown` event in one transaction. Either critical
event fault rolls back both projections; a restart/lost local reply uses the
prior exact receipt rather than resending native work. Independently racing
terminal/wait/stream outcomes or a stale session owner cannot overwrite one
another. A later unsupported wait error cannot silently turn an **already
quarantined unknown native effect** into a terminal failure or authorize a
retry. Late callbacks for operator-closed, terminal or detached turns remain
state-preserving audit observations. Other session creation/closure/dispatch
boundaries and legacy `/v1` team paths have their own independent reviews.

For new legacy `/v1` native provider sessions, `session.created` now commits
with the persisted created session as **one critical SQLite transaction**.
Acknowledged explicit provider resume (`session.resumed`) and native/session
rollback closure (`session.closed`) likewise commit their guarded session
projection together with a matching critical event. Each create/close receipt
has a deterministic session event ID; resumes receive an operation-scoped
unique ID. SQLite journal faults roll back the entire local state update.
An acknowledged native create/resume/close whose local receipt fails is
reported as an **unknown post-native effect**, **not** automatic permission to
repeat the provider action; creation also attempts its existing native
cleanup, but unsuccessful cleanup is not treated as proof of no residual
provider activity. Concurrent/stale session writers fail closed, and a
previous identical SQLite commit receipt may be read again without creating
new events. An already-closed session does not dispatch another native close
or invent another closure event. These guarantees are for **session state and
critical lifecycle events**; they do not guarantee that native provider
creation itself is exactly-once when a transport response is lost, or certify
all legacy `/v1` team and nonterminal turn-transition event families.

Legacy `/v1` **team creation, member join/removal and explicit lead change**
now commit the exact roster state and its critical `team.created`,
`team.member_joined`, `team.member_left` or `team.lead_changed` event in a
single immediate SQLite transaction. A journal failure rolls back the roster
and the in-process team cache is updated only after a confirmed commit.
Each stored receipt is checked against the exact original mutation on a
restart/replay; stale or competing roster generations and reused event IDs
with different contents are rejected. Roster revisions advance strictly even
when multiple mutations happen within one clock millisecond. This does **not**
give the unkeyed `/v1` create HTTP request exactly-once transport semantics,
nor itself certify team **deletion**, delivery transitions or their separate
post-native event emissions. These require their own guarded transactions;
the implemented deletion and delivery boundaries are described below.

Legacy `/v1` **direct/broadcast message admission** separately commits the
complete message, immutable recipient snapshot, all initial `pending`
deliveries and their critical `team_message.created` plus one
`team_delivery.pending` event per recipient in one immediate SQLite
transaction **before** native recipient injection. If even the last pending
event insert fails, no message, deliveries or earlier journal events commit;
the in-process cache is not advanced. A keyed duplicate with exactly the
same intent reuses the persisted receipt across independent writers/restarts
with disposition `existing`, without dispatching native delivery twice.
Changed input against the same key, reused event IDs, missing members and
historical unjournaled rows fail closed. Replaying admission after a delivery
has been injected does **not** reset its status. This guarantees **initial
pre-native admission** only: subsequent delivery retries, post-native
cancellation, team deletion, status event transitions and any already
executed native effects require separate fencing and reconciliation; unkeyed
message creation POSTs do not have a lost-response exactly-once guarantee.

Historical low-level SQLite `upsert_team_message` calls cannot rewrite a
message sealed by the critical admission receipt, even when addressed through
its idempotency key. Low-level delivery updates can advance status/error/turn
fields, but cannot retarget a sealed delivery to another message, team,
recipient, provider or initial policy. Legacy **unjournaled** rows retain their
historical upsert compatibility; sealing them retroactively would invent
authority that was never committed.

Legacy `/v1` **pre-native message cancellation** is a separate all-or-nothing
SQLite transaction, permitted only while **every** delivery is still
`pending` or `deferred`. It changes the complete delivery set to `cancelled`
together with one Critical `team_delivery.cancelled` event per delivery and
one Critical `team_message.completed` event. A late event fault rolls back
all prior delivery updates and events, leaving the live cache unchanged.
An injecting/injected, missing, canonical-owned, or concurrently changed
delivery fails closed instead of partially cancelling the message. A
generation-specific pre-native injection guard prevents a queued worker
from sending a native turn when a concurrent cancellation already won.
Cancellation events record the exact previous status/revision and the
resulting delivery; a lost response or retry after restart verifies the
complete stored receipts without introducing a second cancellation or native
effect. A missing/altered receipt does **not** certify an older unjournaled
cancellation. This is **not** a guarantee for messages that have started
native injection, post-native state transitions, team deletion or retry.

Legacy delivery **native injection** now claims the exact pending/deferred
SQLite generation and appends one Critical `team_delivery.injecting` event
in a single immediate transaction, before any native interrupt or turn send.
Distinct service processes cannot both win the same generation; stale
direct-upsert paths cannot turn an outstanding injecting claim back into a
retryable row. Event insertion fault or a stale cancellation winner grants
no provider permission. If a native send returns an unproven error, the
durable `injecting` status remains a review fence, not automatic retry
permission. Old unjournaled pending rows cannot fabricate initial receipts.
The deterministic claim ID binds the delivery ID to its exact status revision;
sender/recipient sessions and team membership, and any workspace ownership
retirement fences, are verified under the same database lock.
Following a **positive runtime native-turn acknowledgement**, a separate
SQLite immediate transaction verifies the exact admitted turn's recipient,
`agent_message_delivery_transport` source, provider and message correlation,
then atomically changes the claimed delivery to `injected` with the accepted
turn ID and deterministic Critical `team_delivery.injected` receipt. If this
completed the entire recipient snapshot, the same commit emits one Critical
`team_message.completed` event with those terminal delivery states. A last
event fault undoes every delivery/result event write. Lost result commit
responses may be reconciled against the identical existing state and receipt,
without re-executing a native send. An unproven native outcome remains
durably `injecting`; an admitted waiting-for-approval turn is not evidence
that provider execution already happened. Pre-native legacy **deferral, recipient-unavailable failure, and explicit
retry** now use a separate generation-specific SQLite immediate transaction.
Each status/revision update and its deterministic Critical
`team_delivery.deferred`, `team_delivery.failed` or `team_delivery.pending`
receipt commit together. A last-recipient terminal failure also commits the
Critical `team_message.completed` snapshot in that transaction; an event
insertion fault rolls the status and every earlier event back. The native
injection claim verifies the latest generation receipt, not only the initial
pending admission, before granting dispatch. A stale/unjournaled historical
`deferred`/`failed` row or direct upsert is not a retry token. Explicit retry
may reopen a `deferred` recipient, or a `failed` recipient only while its
message has not been terminally completed; after a completed message,
operators must create a new message instead of rewriting its immutable
completion. An uncertain native `injecting` row is **never** eligible for
this pre-native retry path. Team deletion has its own separate Critical
transaction described below.

Legacy `/v1` **team deletion** commits the exact active team revision's
durable tombstone, every still-`pending`/`deferred` legacy recipient's
`cancelled` state, one Critical `team_delivery.cancelled` receipt per affected
recipient, all newly-terminal `team_message.completed` snapshots, and one
Critical `team.deleted` tombstone receipt in **one immediate SQLite
transaction**. A failure at even the final event insert rolls back the entire
state change; team, idempotency and delivery queue caches remain unchanged.
Cancellation checks the original Critical pending admission and latest
pre-native status generation and rejects unjournaled/forged transitions,
stale revisions, missing recipients, and canonical-owned deliveries.
`injecting` native outcomes are unresolved effects, so deleting a team with
one fails closed rather than pretending a cancellation is safe. A lost
commit reply or competing delete can verify the durable original timestamp,
complete recipient snapshot and all immutable receipts after restart and
converge on that one tombstone without new native work. This does not erase
historical deliveries or prove exactly-once provider execution; unkeyed
legacy HTTP create/message POSTs and Windows native process launch gating
remain separate constraints. Certified team records also cannot be restored
through the raw legacy SQLite team upsert.

A dispatched provider turn may also produce a **native approval request**.
Its pending approval record, waiting turn/session projections and critical
`approval.requested` event now form a separate **atomic SQLite commit**.
Uniqueness on the native provider's approval reference (per session/turn)
converges competing callbacks after a restart or a lost local response.
An event insertion fault leaves no partial pending approval, and duplicate
callbacks do not replay a second journal entry. Existing legacy approvals
without a journal receipt remain historical rather than backfilled with
invented request events. Native approval **resolution** is a separate
transaction boundary from the request receipt, as described below.

Before submitting any **provider-native approval decision**, an exclusive
SQLite transaction updates the pending approval to \`decision_pending\` and
commits the critical \`approval.decision_reserved\` receipt. This is a
**pre-native reservation**, not evidence that the provider executed the
decision. A failed journal insert means no reservation and no native call;
concurrent or restarted callers cannot use an existing reservation as
permission to submit a second native decision. Only an adapter that proves
the decision **was not dispatched** may atomically release it to pending
with `approval.decision_not_dispatched`. After a provider **acknowledges** a
native decision, its resolved approval and any still-attached turn/session
resumption are committed with one deterministic critical `approval.resolved`
receipt in a separate SQLite transaction. When a provider returns an
**ambiguous native outcome**, the original `decision_pending` reservation
remains fenced while the turn/session recovery quarantine and critical
`approval.decision_unknown` receipt commit together. An event insertion
failure rolls back the entire outcome transaction; it **never** authorizes
resending a possibly executed native approval. Restart or lost SQLite commit
responses are reconciled against exact reservation/decision receipts, and
independent writers cannot resolve competing outcomes. If a provider turn
completed while the decision acknowledgement was in flight, its already
terminal state and the newer session attachment are preserved rather than
reopened by the approval result. Missing/stale ownership fails closed; only
the provider adapter's proven-not-dispatched response can release the exact
attempt. These guarantees apply to new native approval outcomes, **not**
legacy missing receipts, other session lifecycle event families or every
`/v1` team event. Historical approvals are not given fabricated
reservation/no-effect/outcome events.

## Replay before live

A robust client reconnects like this:

1. remember the last event ID it processed
2. ask the runtime for events after that cursor
3. render the replayed backlog
4. follow the live stream

Session, global, team, workspace, agent, and process streams now subscribe to **committed SQLite event notifications** before reading the first replay page. Each stream reads ordered event rows from the durable journal in bounded pages, then waits for a commit notification instead of polling SQLite every 250 ms while idle. Notifications contain no event payload or authority: a delayed, coalesced, or lagged notification triggers an indexed catch-up query from the last delivered cursor. SQLite writes that roll back never wake subscribers. Legacy process streams verify process visibility through the process manager; operator streams verify bearer identity and resource existence before accessing a scoped journal.

The in-process commit notification channel is scoped to one runtime database and is not a cross-process replication bus. Clients must preserve the last received event ID and reconnect to replay after network/server failure. A journal-read error closes the SSE stream with a retryable `stream.error` event that does **not** advance the cursor. All streams retain their existing cursor contracts.

A deterministic injected journal-read fault regression proves `stream.error`
has no event ID, closes the failed subscription, and allows a new SQLite
stream to replay the unread committed event from the last acknowledged ID.

The idle behavior is covered by an instrumented real SQLite-store SSE test:
one initial journal read, **no further SELECT across an idle interval
longer than the former 250 ms polling cadence**, then exactly one indexed
catch-up read for each separately committed event. The stream returns to
the same no-query idle state after delivery; its 10-second SSE keepalive
is a network heartbeat, not a periodic database scan. This proof covers
the in-process committed-journal wakeup architecture, not cross-process
or cross-host event replication.

Concurrency/recovery regression exercises independent SQLite connections
contending for the same workspace/process scope, verifies unique gap-free
global and per-scope cursors after restart, and reopens fresh stream stores
to replay and reconnect across global, workspace, agent and process streams.
The append transaction serializes scope-sequence allocation; retained journal
rows remain the authority after restart, not the former stream's memory.

A bounded-output SSE lag stress regression holds back a client while 320
independent events commit, proves a separate notification subscriber overran
the 256-entry broadcast ring, then drains the stream and verifies all 320
global row IDs arrived exactly once and in journal order. Coalesced or lost
notification pulses never replace durable replay.

## Cursor rules

The runtime supports both explicit query cursors and the SSE `Last-Event-ID` header.

- `after_seq` query parameter wins when provided.
- otherwise `Last-Event-ID` is used.
- invalid `Last-Event-ID` values are rejected with `400`.
- duplicate `Last-Event-ID` headers return `400` (even if identical or an
  explicit `after_seq` is present); the runtime never guesses which resume
  cursor a proxy or client intended.
- session, team, and process stream IDs are scoped sequence numbers.
- global stream IDs are global runtime event row IDs.

The name `after_seq` is convenient but slightly overloaded. On scoped endpoints it means scoped sequence. On global endpoints it means the global event row ID.

## Startup recovery

Startup recovery is not a background hope. It is a real reconciliation pass during bootstrap.

The runtime hydrates state from SQLite, then checks nonterminal sessions, turns, approvals, provider resume status, running processes, deferred team deliveries, and worktree/claim records.

The recovery goal is simple:

> After a restart, every durable record should either continue safely or be marked clearly enough that an operator can understand the failure.

## Session recovery

For sessions that are not closed or failed, the runtime attempts to resume provider state when it has provider references. If provider resume fails, the session is marked failed with a recovery reason instead of being left half alive.

For active turns, recovery checks whether the turn is terminal, waiting for approval, missing, or still running. Depending on the case, it clears active state, keeps waiting for approval, marks the turn failed, or respawns a provider waiter.

Pending orphan approvals are declined during recovery because there is no safe owner left to approve.

## Process recovery

Runtime-managed processes are host OS processes. If the server restarts, it cannot assume an old child process is still safely owned by the current runtime instance.

On startup, `queued` processes remain queued. An interrupted
`launch_reserved` claim is safely requeued, using a distinct launch gate for
the next claim generation so a stale pre-launch wrapper cannot run the
command when a replacement attempt is authorized. On Unix, pre-launch shells
also exit if their runtime parent disappears or their bounded gate wait
expires. Previously `running` processes are reconciled only after verifying
their recorded OS PID **and start identity** before signaling the process
group; a mismatch fails closed, not by killing an unrelated reused PID.
Successfully reconciled work is recorded as `interrupted` with a terminal
journal event. Pending model completion delivery is recovered separately.

## Team delivery recovery

Team deliveries can be deferred when a recipient is busy. Startup recovery retries deferred deliveries for ready recipients so messages do not remain stuck forever just because the runtime restarted.

## Worktree recovery

The worktree service normalizes stored records, repairs duplicate worktree identities, rewrites claims when duplicates are merged, releases impossible claims, and enforces one active claim per session.

This is closer to a filesystem check than a normal startup hook. The runtime assumes old state may be messy and tries to make it coherent.

## Diagnostics to inspect

Use these endpoints when something feels wrong:

```bash
curl "$BASE_URL/v1/diagnostics" "${AUTH[@]}"
curl "$BASE_URL/v1/diagnostics/recovery" "${AUTH[@]}"
curl "$BASE_URL/v1/diagnostics/providers" "${AUTH[@]}"
curl "$BASE_URL/v1/diagnostics/comms" "${AUTH[@]}"
curl "$BASE_URL/v1/diagnostics/processes" "${AUTH[@]}"
curl "$BASE_URL/v1/diagnostics/worktrees" "${AUTH[@]}"
curl "$BASE_URL/v1/diagnostics/team-operations" "${AUTH[@]}"
```

The best operator habit is to read diagnostics before guessing.

## Inspect a legacy SQLite snapshot without changing it

Before considering a migration or removing compatibility paths, inventory a
**consistently checkpointed, isolated copy** of the original SQLite database:

```bash
python3 ./scripts/audit-legacy-snapshot.py --snapshot /path/to/offline-copy.sqlite3
```

This is a counts-only preflight, **not** the migration preview/apply endpoint.
It opens the snapshot with SQLite's immutable/read-only mode and explicitly
enables query-only access, runs fixed aggregation queries, and never starts
the runtime, providers, or native worktree/process actions. It refuses
symbolic links and files with SQLite `-wal`, `-shm`, or `-journal`
sidecars; create a *consistent SQLite backup* before using it rather than
copying a live WAL database with a plain file copy. An immutable SQLite
handle assumes the file cannot change while it is open, so **do not run
this command against a live database**.

The JSON report distinguishes observed counts from `null` (a schema table
or required column is unavailable, **not** an observed zero). It inventories
unresolved migration subjects, unowned live sessions, turns, approvals,
dispatches, operations, effects/outbox, message deliveries, processes and
completion notices, hook work, retirement fences, worktree claims, and legacy
team native spawn journals. For schema version 30 and newer it additionally
counts uncompleted caller-keyed spawn admissions: an absent older table is
`null`/unknown, not proof that no pending Git/provider effects exist.
It also reports the migration version and missing ordered migration steps.
Only aggregate counts and metric names are printed, not prompts, credentials,
file paths, or member identities.

The report **always** sets `cutover_authorized: false`, including when every
reported count is zero. Inspect the original data's ownership/legacy lineage,
review every unknown or outstanding obligation, rehearse ordered migrations
and restart on a separate writable copy, and obtain explicit cutover approval
before irreversible changes. The snapshot audit cannot establish those proofs.
