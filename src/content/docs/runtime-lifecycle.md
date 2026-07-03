---
title: "Runtime lifecycle"
description: "Follow sessions, turns, approvals, provider dispatch, terminal states, and close/resume behavior through the runtime."
order: 11
category: "Mental Model"
summary: "The end-to-end path from HTTP request to durable runtime state."
---

This page follows the core execution path. It is the best place to understand what happens after a client presses “send”.

## The short version

A normal turn moves like this:

```text
client request
  -> HTTP handler
  -> runtime session manager
  -> SQLite records
  -> provider adapter
  -> event append
  -> SSE/replay
  -> provider terminal result
  -> durable terminal state
```

The analogy is a workshop job ticket. The client writes a ticket, the runtime assigns it to a worker, the worker reports progress, and the runtime stamps the final result into the ledger.

## Create a session

A client creates a session with a provider name, optional model, optional working directory, optional permission mode, and optional metadata.

The runtime then:

1. looks up the provider adapter
2. allocates a runtime session ID
3. asks the provider to create its native session
4. stores provider references opaquely
5. persists the `SessionRecord`
6. appends `session.created`

The runtime ID is the stable public identity. Provider session references are private adapter details.

## Send a turn

A client sends a turn to an existing session.

The runtime checks:

- the session exists
- the session is not closed or failed
- no active turn already owns the session
- the provider is registered
- approval requirements can be represented

If the request is accepted, the runtime persists a `TurnRecord`, updates the session's active turn, appends a start or approval event, and returns an accepted response. It does not wait synchronously for the model to finish.

## The one-active-turn rule

Every session has at most one active turn. This is one of the most important design decisions in the codebase.

Without it, the runtime would need to solve ambiguous cases like:

- Which turn owns a tool call?
- Which message should receive a team delivery?
- Which process belongs to which in-flight job?
- Which interruption should win?

The one-active-turn rule keeps ownership tractable.

## Provider dispatch and fallback

Provider adapters implement the shared `RuntimeProvider` trait. After the runtime records the turn, it calls the provider's `send_turn` behavior.

There is an important reliability detail: when provider dispatch reports a missing provider-side session but the runtime has stored provider references, the runtime attempts to resume the provider session and retry dispatch. This lets the runtime heal from some provider process/session loss at the dispatch boundary.

## Approval flow

When a turn requires approval, the runtime records an `ApprovalRecord` and marks the turn/session as waiting for approval.

If the approval is accepted:

1. the approval becomes resolved
2. the turn moves to `in_progress`
3. the session moves to `turn_running`
4. the runtime appends `turn.started`
5. the provider waiter is spawned

If the approval is declined:

1. the approval becomes resolved
2. the turn is interrupted
3. the session becomes ready again
4. the runtime appends `turn.interrupted`

The important point is that approval state is durable. It is not just an in-memory prompt.

## Waiting for terminal result

After dispatch, the runtime spawns a background waiter for the provider's terminal result.

The provider eventually reports one of the runtime terminal outcomes:

- completed
- interrupted
- failed

The runtime then updates the turn, clears the session active turn, updates session status, stores usage/error data when present, and appends the terminal event.

Duplicate terminal results are treated idempotently when they agree. Conflicting terminal results are treated as protocol violations and fail the session closed instead of guessing.

## Interrupt a turn

Interrupt requests go through the runtime. The runtime delegates to the provider adapter and records the resulting state through the same durable path. Team delivery policies can also trigger interruptions when a message needs to be injected into a busy recipient.

## Resume a session

Session resume is explicit. A provider may return updated provider references. The runtime keeps its own session ID stable while refreshing provider details underneath it.

Startup recovery also uses provider resume to reconcile nonterminal sessions after a process restart.

## Close a session

Closing a session is different from forgetting it. The runtime persists closed state so later diagnostics and event replay can still explain what happened.

## Failure stance

Gooselake generally prefers to **fail closed into durable state**.

For example, if provider send fails after a turn request has been accepted internally, the runtime records a failed turn, restores or fails session state as appropriate, and appends a failure event. That is better than leaving the user with a spinner and no record.

## What clients should assume

Clients should assume:

- `POST /v1/sessions/{id}/turns` is an acceptance boundary, not a completion boundary.
- terminal information arrives through events and records.
- a session with an active turn cannot accept another normal turn.
- reconnecting clients should replay first, then follow live streams.
- provider-specific details should not be stored as client truth.
