---
title: Operator workflows
description: The workflows that matter once Gooselake is running real agent work on a real machine.
order: 4
category: Operator Workflows
summary: A field guide to the repeatable motions around sessions, execution, teams, worktrees, and recovery.
---

## Session lifecycle

A normal operator loop is:

1. create or reconnect to a session
2. send a turn
3. stream events
4. respond to approvals if needed
5. inspect terminal state
6. close or resume later

Sessions are durable runtime objects. Multiple clients can inspect the same underlying state.

## Event recovery

When a client disconnects, it should reconnect with the last sequence it saw:

```bash
curl -N -H "Authorization: Bearer $TOKEN" \
  "$BASE_URL/v1/sessions/$SESSION_ID/events/stream?after_seq=$LAST_SEQ"
```

The stream replays first, then continues live.

## Long-running execution

Use the process API or MCP process tools for background work. The runtime stores process records, log paths, sampled output events, exit status, timeout data, and ownership information.

That gives operators a place to ask: what ran, who started it, what did it output, and is it still alive?

## Worktree ownership

Worktrees are created, claimed, released, and cleaned up explicitly. That matters when multiple agents are operating on the same repo or when spawned teammates need isolated branches.

The important habit: do not treat a filesystem path as the ownership model. Treat the runtime worktree record and claims as the ownership model.

## Team coordination

A team workflow can include:

- a lead session
- manually joined members
- spawned members
- direct messages
- broadcasts
- deferred delivery
- retries
- cancellation
- team-wide interrupts

The runtime records delivery state, so a UI can show whether a teammate actually received a message or whether it is waiting behind an active turn.

## Recovery mindset

After a restart or interruption, start with runtime state:

```bash
curl -fsS -H "Authorization: Bearer $TOKEN" "$BASE_URL/v1/diagnostics/recovery"
curl -fsS -H "Authorization: Bearer $TOKEN" "$BASE_URL/v1/events?after_seq=0&limit=100"
```

Do not rely on operator memory when the runtime already has receipts.
