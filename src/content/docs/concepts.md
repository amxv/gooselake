---
title: Core concepts
description: Understand the runtime boundary, event model, provider abstraction, and host-owned execution model.
order: 3
category: Core Concepts
summary: The mental model for why the runtime exists and which responsibilities belong inside it.
---

## The central boundary

The important boundary is not between a React component and a model provider. It is between a client and the runtime.

Once that boundary is real, clients stop owning concerns they are bad at carrying:

- provider auth staging
- stream lifetime
- durable turn history
- process lifetime
- worktree and filesystem control
- recovery after disconnects
- team message delivery state

The client renders and controls. The runtime remembers and executes.

## Sessions are runtime objects

A session is a durable runtime record with a provider, status, model, cwd, metadata, provider refs, active turn, optional worktree, and timestamps.

Provider sessions can disappear or change shape. The runtime session ID remains the API identity that clients should use.

## Turns are asynchronous

Sending a turn accepts work and returns a runtime turn ID. Terminal output arrives later through stored records and events. That distinction is why SSE matters: the request starts the work; the stream explains what happened.

## Events are receipts

Runtime events are stored in SQLite and scoped by session, team, process, worktree, or system. They are replayable before live streaming starts.

That gives clients a simple reconnect strategy:

1. remember the last event sequence seen
2. reconnect with `after_seq`
3. replay missed events
4. continue live

## Providers are adapters

Codex, Claude, and ACP have different transports, auth assumptions, model catalogs, and turn semantics. Gooselake normalizes them behind one provider contract for sessions, turns, approvals, interrupts, close, and terminal results.

The UI should not become a switch statement for every provider quirk.

## Host work is first-class

The runtime is designed for agents that operate on the host machine:

- spawning background processes
- writing logs
- claiming git worktrees
- using repo cwd values
- routing MCP tools back into runtime services

If the host is where the work happens, the host is where durable control should live.

## Teams are transport, not roleplay

Gooselake models team communication as records and deliveries. Messages have senders, recipients, priority, policy, image paths, correlation IDs, idempotency keys, delivery state, retries, cancellation, and replayable events.

That turns multi-agent coordination into inspectable runtime behavior rather than hidden prompt glue.
