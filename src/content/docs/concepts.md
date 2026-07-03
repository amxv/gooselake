---
title: "Core concepts"
description: "Understand the runtime boundary, event model, provider abstraction, host-owned execution, teams, processes, and worktrees."
order: 10
category: "Mental Model"
summary: "The plain-English mental model for why the runtime exists and which responsibilities belong inside it."
---

Gooselake is easiest to understand if you stop thinking about it as a chat backend. It is closer to an operating room, an air traffic tower, or a factory floor: work enters through a controlled door, every important movement is recorded, and specialized workers do their jobs behind stable interfaces.

## The central boundary

The main boundary is between a **client** and the **runtime**.

A client might be a desktop app, browser UI, shell script, local dashboard, or future polished CLI. The runtime is the long-lived process that owns truth. Clients ask it to create sessions, send turns, stream events, inspect diagnostics, and manage host resources.

That boundary keeps fragile UI state away from responsibilities that need durability:

- provider auth and staged credentials
- stream lifetime and replay cursors
- session and turn state
- approvals and terminal turn results
- process lifetime and logs
- worktree ownership
- team messages and delivery state
- startup recovery

## Sessions are desks

A session is a runtime-owned workspace for one provider-backed agent. The public session ID belongs to Gooselake. Provider IDs, native session handles, and sidecar references are implementation details stored behind that ID.

The practical effect: clients do not need to know how Codex, Claude, or ACP name their sessions. Clients use the runtime ID and let the provider adapter translate.

## Turns are jobs on the desk

A turn is a unit of work sent to a session. Turns are asynchronous. Sending a turn means “accept this job and start working,” not “block until the answer is ready.”

The runtime enforces the important invariant: **one active turn per session**. This keeps ownership simple. A session cannot be asked to do two conflicting things at the same time unless the runtime has explicitly interrupted, completed, or failed the current turn.

## Approvals are gates

Some turns require approval before dispatch or continuation. Approvals are durable records, not transient prompts. If a process restarts while an approval is pending, startup recovery can still see that the session was waiting for a decision.

The API accepts common decision wording such as `accept`, `accepted`, `decline`, `declined`, `reject`, and `rejected`, but the runtime normalizes those decisions internally.

## Events are receipts

Events are not decorations for a UI. They are receipts.

When a session is created, a turn starts, an approval is requested, a team delivery is deferred, or a process emits output, the runtime records an event. Clients can replay those events later or subscribe to live streams.

A good UI treats events like a bank ledger: render from the ledger first, then follow the live tail.

## Providers are adapters

Codex, Claude, and ACP do not define Gooselake's architecture. They are adapters behind a shared provider contract.

That contract includes provider metadata, health checks, optional model catalogs, optional auth status, session creation/resume/close, turn send/wait/interrupt, and approval response behavior.

Provider-specific churn stays inside provider crates and sidecars. The client-facing runtime API should remain stable.

## SQLite is the runtime notebook

SQLite stores sessions, turns, approvals, teams, messages, deliveries, worktrees, process records, credentials, diagnostics, and runtime events.

This is a deliberate single-user runtime choice. The runtime gets durable local state without requiring an external database service. The tradeoff is that Gooselake is not trying to become a multi-tenant cloud platform.

## Host work is first-class

Gooselake is allowed to run real work on the host:

- start commands
- capture stdout/stderr logs
- create Git worktrees
- allocate claims on those worktrees
- let provider sessions call runtime tools through MCP

This is why the runtime belongs on the machine where work happens. It is not just forwarding prompts to a model.

## Teams are transport, not roleplay

A team is not just a prompt that says “pretend there are five agents.” Gooselake models team communication with real records:

- teams
- members
- messages
- deliveries
- delivery policies
- retries
- cancellation
- spawn operations
- diagnostics

That makes team coordination inspectable and recoverable. If a message cannot be delivered because a recipient is busy, the runtime can defer it instead of pretending nothing happened.

## Worktrees are rooms, claims are keys

A managed worktree is a room where an agent can work. A claim is the key that says which session currently owns it.

Separating the room from the key matters. The runtime can create a worktree once, claim it for a session, release the claim later, and decide whether cleanup is safe based on active claims and policy.

## Processes are jobs with logs

Runtime-managed processes are commands started by the runtime. Their logs are authoritative. Output events are useful for live UIs, but they may be sampled or truncated. If you need the full output, ask for logs.

## The frontend question

A healthy client design can answer this question:

> If the UI disappears right now, can the runtime still explain what happened?

If the answer is yes, the client is probably thin enough.
