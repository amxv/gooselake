---
title: "Events and recovery"
description: "Understand replayable event streams, cursors, criticality, SSE handoff, startup recovery, and diagnostics."
order: 12
category: "Mental Model"
summary: "How Gooselake remembers what happened and comes back after restarts."
---

Events are the runtime's flight recorder. They let a client reconnect, rebuild context, and explain what happened even if the original UI tab disappeared.

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

Droppable events are useful but less sacred. Process output samples are the clearest example. They help live UIs feel alive, but the process log files remain the authoritative output source.

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

An observed **provider terminal result** (`turn.completed`, `turn.interrupted`,
or `turn.failed`) now commits the terminal turn, the session's active-turn
release and transcript update when that turn is still attached, and its
critical session event in **one SQLite transaction**. Each logical turn has
one deterministic terminal-event receipt. A failed event insert or an
independently changed session rolls the full transition back; an ambiguous
commit can be inspected by that durable receipt on restart without invoking
the provider again. A late result for a superseded, no-longer-active turn may
terminalize only that historical turn, never the newer active session.
Provider wait failures, nonterminal dispatch transitions, provider-native
approval callbacks/resolutions and older legacy team paths are **separate**
event-authority cases and are not certified atomic by this result path.

A dispatched provider turn may also produce a **native approval request**.
Its pending approval record, waiting turn/session projections and critical
`approval.requested` event now form a separate **atomic SQLite commit**.
Uniqueness on the native provider's approval reference (per session/turn)
converges competing callbacks after a restart or a lost local response.
An event insertion fault leaves no partial pending approval, and duplicate
callbacks do not replay a second journal entry. Existing legacy approvals
without a journal receipt remain historical rather than backfilled with
invented request events. Native approval **resolution**, including unknown
provider decision outcomes, is still a separate guarded recovery boundary
and is not proven atomic by the request receipt.

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
completion notices, hook work, retirement fences, and worktree claims.
It also reports the migration version and missing ordered migration steps.
Only aggregate counts and metric names are printed, not prompts, credentials,
file paths, or member identities.

The report **always** sets `cutover_authorized: false`, including when every
reported count is zero. Inspect the original data's ownership/legacy lineage,
review every unknown or outstanding obligation, rehearse ordered migrations
and restart on a separate writable copy, and obtain explicit cutover approval
before irreversible changes. The snapshot audit cannot establish those proofs.
