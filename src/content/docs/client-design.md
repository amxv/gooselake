---
title: "Client design guide"
description: "Build thin frontends, local consoles, scripts, and future CLIs on top of Gooselake without duplicating runtime state."
order: 21
category: "Client Builders"
summary: "Patterns for UIs and automations that cooperate with the runtime instead of fighting it."
---

A Gooselake client should feel powerful, but it should stay thin. The runtime is the engine room; the client is the dashboard.

## Design principle

The client should ask the runtime what is true instead of inventing truth locally.

Good client state:

- selected session ID
- selected team ID
- last processed event cursor
- optimistic text currently being typed
- local display preferences

Bad client state:

- “this turn is probably complete”
- “this process is still running” without checking the runtime
- “this agent owns this worktree” without a claim record
- “this provider is authenticated” without provider diagnostics

## First screen

A useful first screen should show:

- protected `/v1/health`
- provider list and auth status
- recent sessions
- startup recovery summary
- diagnostics links

This gives an operator a cockpit view before they start work.

## Session screen

A session screen should be built from records plus events:

1. fetch the session record
2. replay session events from the last cursor
3. open the session stream
4. render terminal state from persisted turn/session records
5. show approvals when pending
6. show provider errors as durable facts, not transient toast messages

Do not rely on one long-lived browser connection as the only source of truth. It will eventually disconnect.

## Turn submission

When sending a turn, treat the response as “accepted for execution.” Then update the UI by following events and records.

A simple flow:

```text
POST /v1/sessions/{id}/turns
  -> show accepted turn ID
  -> follow /v1/sessions/{id}/events/stream
  -> update when turn terminal event arrives
```

If the session already has an active turn, the runtime will reject the request. The UI should show the active turn rather than hiding the reason.

## Event cursors

Persist cursors per stream. For example:

- global events cursor
- per-session cursor
- per-team cursor
- per-process cursor

On reconnect, pass `after_seq` or `Last-Event-ID`. Render replay first, then live events.

## Teams UI

A team UI should show three different objects separately:

- the team itself
- members and their session IDs
- messages and deliveries

Do not collapse “message sent” and “message delivered” into one state. A message can be created while delivery is pending, deferred, injected, failed, or cancelled.

## Process UI

For runtime processes, show both:

- lifecycle state from process records/events
- logs from `/v1/processes/{process_id}/logs`

Output events are sampled for live feedback. Logs are the source of truth for detailed output.

## Worktree UI

Show worktrees and claims separately.

A good worktree panel answers:

- where is the worktree on disk?
- which branch does it use?
- who created it?
- which sessions currently claim it?
- what is the cleanup policy?
- was cleanup blocked by active claims?

## Provider UI

Provider status should come from provider endpoints and diagnostics, not hardcoded assumptions.

Model catalogs differ by provider. ACP may return an empty model list depending on the agent. Claude and Codex expose known model catalogs through their adapters.

## Error handling

Prefer errors that tell the user which runtime invariant fired:

- missing bearer token
- provider not registered
- session already has active turn
- approval not pending
- process ownership mismatch
- worktree has active claim
- MCP caller session missing or inactive

Generic “request failed” messages make a durable runtime feel unreliable even when it is doing the right thing.

## Golden rule

A good client can be killed, restarted, and reconnected without changing the runtime's understanding of the work.
