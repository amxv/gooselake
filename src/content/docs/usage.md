---
title: Usage model
description: Learn how clients, operators, and provider sessions interact with the runtime during daily use.
order: 3
category: Core Concepts
summary: A practical view of how consumers talk to the runtime and what the runtime is expected to own.
---

## Thin clients

A healthy Gooselake client focuses on:

- listing sessions
- creating turns
- rendering event streams
- showing approvals
- displaying process logs and worktree state
- surfacing provider diagnostics

The client should not own the truth of whether a turn is running, which worktree is claimed, or whether a team message was delivered. Those are runtime records.

## HTTP for commands, SSE for flow

Use HTTP for explicit actions:

- create a session
- send a turn
- respond to an approval
- start or kill a process
- create or release a worktree
- send a team message

Use SSE for state flow:

- global event timeline
- session events
- team events
- process events

This keeps command boundaries clear while making reconnects simple.

## Provider readiness before product work

Before building a UI feature around a provider, check readiness:

```bash
curl -fsS -H "Authorization: Bearer $TOKEN" "$BASE_URL/v1/providers"
curl -fsS -H "Authorization: Bearer $TOKEN" "$BASE_URL/v1/diagnostics/providers"
```

A provider that is registered but unauthenticated is a setup problem, not a frontend state problem.

## The frontend question

Ask this when adding client logic:

> Can the UI disappear right now without corrupting the session's truth?

If the answer is no, too much orchestration has leaked into the client.

## Good usage

- A desktop app that reconnects to the same machine-side session after restart.
- A web UI that renders replayed events before showing live output.
- An ops console that inspects provider, process, worktree, and recovery diagnostics.
- A multi-agent surface that renders delivery records instead of pretending messages are instant.

## Bad usage

- Treating the runtime as a token proxy while state lives in React.
- Hiding long-running host work behind a browser lifecycle.
- Building provider-specific session semantics into every client.
- Recreating team delivery state in local UI stores.
