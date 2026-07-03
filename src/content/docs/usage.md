---
title: "Usage model"
description: "Learn how clients, operators, scripts, and provider sessions interact with the runtime during daily use."
order: 20
category: "Client Builders"
summary: "A practical view of how consumers talk to the runtime and what the runtime owns."
---

Gooselake usage has two halves:

- clients send commands over HTTP
- clients learn what happened through records and replayable events

That split is the core product shape. HTTP is the steering wheel. Events are the dashboard and black-box recorder.

## Thin clients

A healthy Gooselake client focuses on presentation and user intent:

- list sessions
- create sessions
- send turns
- render event streams
- show approvals
- display process logs
- show worktree state
- surface team messages and deliveries
- expose diagnostics

The runtime should own the hard state: active turns, provider refs, event cursors, delivery state, process status, and cleanup policy.

## HTTP for commands, SSE for flow

Commands are ordinary HTTP requests:

```bash
curl -X POST "$BASE_URL/v1/sessions"   "${AUTH[@]}"   -H 'Content-Type: application/json'   -d '{"provider":"codex","model":"gpt-5.5"}'
```

Flow comes from events:

```bash
curl -N "$BASE_URL/v1/events/stream" "${AUTH[@]}"
```

A client should not assume a `POST` response is the final story. It is usually the start of a durable workflow.

## Provider readiness before product work

Before creating product UI around a provider, check:

```bash
curl "$BASE_URL/v1/providers" "${AUTH[@]}"
curl "$BASE_URL/v1/diagnostics/providers" "${AUTH[@]}"
```

Then check provider-specific auth status when needed:

```bash
curl "$BASE_URL/v1/providers/codex/auth/status" "${AUTH[@]}"
curl "$BASE_URL/v1/providers/claude/auth/status" "${AUTH[@]}"
curl "$BASE_URL/v1/providers/acp/auth/status" "${AUTH[@]}"
```

## The frontend question

Ask this for every feature:

> If the UI disappears right now, can the runtime still explain what happened?

If the answer is no, move more responsibility into runtime records/events or fetch more state from the runtime.

## Good usage

- Create a session, then store only the runtime session ID in the client.
- Send a turn, then follow events until terminal state.
- Replay events after reconnect before opening a live stream.
- Show pending approvals as durable runtime objects.
- Treat team deliveries as separate from messages.
- Read process logs for exact output.
- Inspect diagnostics before retrying failed operations.

## Bad usage

- Store provider-native session references in client state.
- Treat a turn request as completed just because the HTTP request returned.
- Hide runtime errors behind generic toast messages.
- Assume process output events contain full logs.
- Delete worktrees without checking active claims.
- Build one-off provider behavior into clients instead of using the provider abstraction.

## Client examples

A shell script, desktop UI, web dashboard, and future first-party CLI should all use the same runtime contract. That is the point of the control-plane boundary.

For design patterns, see [Client design guide](/docs/client-design). For route details, see [API guide](/docs/api) and [Endpoint catalog](/docs/endpoint-catalog).
