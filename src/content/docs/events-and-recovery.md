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
| `team` | team messages, deliveries, member operations |
| `process` | runtime-managed command lifecycle and sampled output |
| `worktree` | worktree creation, claim, release, cleanup |
| `system` | runtime-level events such as startup recovery |

Each scoped event has a scoped sequence number. Global event listing uses the database row ID as the cursor.

## Critical and droppable events

Events have criticality.

Critical events describe state transitions that clients should not casually lose: sessions created, turns completed, deliveries failed, processes started, and so on.

Droppable events are useful but less sacred. Process output samples are the clearest example. They help live UIs feel alive, but the process log files remain the authoritative output source.

## Replay before live

A robust client reconnects like this:

1. remember the last event ID it processed
2. ask the runtime for events after that cursor
3. render the replayed backlog
4. follow the live stream

For session and process streams, the server subscribes to live events before replaying backlog. That closes the classic handoff gap where an event could be written after replay starts but before live subscription begins.

Global and team streams use the store as the source of truth and poll for new rows after replay. They are reliable, but clients should still treat the replay cursor as the contract.

## Cursor rules

The runtime supports both explicit query cursors and the SSE `Last-Event-ID` header.

- `after_seq` query parameter wins when provided.
- otherwise `Last-Event-ID` is used.
- invalid `Last-Event-ID` values are rejected with `400`.
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

On startup, previously `running` or `queued` process records are marked failed and reported in startup recovery diagnostics.

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
