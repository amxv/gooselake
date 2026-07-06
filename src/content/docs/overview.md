---
title: "Documentation overview"
description: "The full documentation map for Gooselake: setup, mental model, runtime services, client APIs, operations, deployment, and reference material."
order: 0
category: "Start Here"
summary: "Use this page as the map for the runtime manual."
---

Gooselake is a machine-side runtime for durable agent work. Think of it as an **air traffic control tower** for coding agents: clients ask for flights, providers fly the planes, and the runtime keeps the flight plan, radio log, runway state, crash reports, and replayable black-box recorder.

The important shift is that the UI is not the runtime. A desktop app, web console, local script, or future CLI can disappear without losing the truth of the work. The runtime owns provider sessions, turn execution, event history, process execution, worktrees, team communication, diagnostics, and recovery.

## What to read first

If you are new, read in this order:

1. [Setup](/docs/setup) — run the server and make the first authenticated request.
2. [Core concepts](/docs/concepts) — learn the vocabulary: sessions, turns, events, providers, teams, processes, and worktrees.
3. [Runtime lifecycle](/docs/runtime-lifecycle) — follow one turn through the system.
4. [Events and recovery](/docs/events-and-recovery) — understand replay, cursors, and startup reconciliation.
5. [CLI and command runner](/docs/cli) — learn what the current command-line surface actually is.

## Reading paths

### New operator

Start with [Setup](/docs/setup), then [Install guide](/docs/install), [Configuration reference](/docs/configuration), [Operations runbook](/docs/operations), and [Troubleshooting](/docs/troubleshooting).

You are trying to answer: **is the runtime healthy, authenticated, provider-ready, and recoverable after a restart?**

### Frontend, API, or CLI builder

Start with [Usage model](/docs/usage), [Client design guide](/docs/client-design), [API guide](/docs/api), [Endpoint catalog](/docs/endpoint-catalog), and [Events and recovery](/docs/events-and-recovery).

You are trying to build a thin client. The runtime should hold the hard state; your client should render it.

### Runtime contributor

Start with [Architecture](/docs/architecture), [Repo guide](/docs/repo-guide), [Provider guide](/docs/providers), [MCP and sidecars](/docs/mcp-and-sidecars), and [API doc sync workflow](/docs/api-doc-sync).

You are trying to change implementation without breaking the runtime contract.

## The docs map

### Start Here

- [Setup](/docs/setup): first local runtime loop.
- [Install guide](/docs/install): release and source install paths.
- [CLI and command runner](/docs/cli): current binary, flags, Make targets, and scripts.

### Mental Model

- [Core concepts](/docs/concepts): the big ideas in plain language.
- [Runtime lifecycle](/docs/runtime-lifecycle): how a session and turn move through the runtime.
- [Events and recovery](/docs/events-and-recovery): the event ledger and restart behavior.
- [Architecture](/docs/architecture): crates, sidecars, persistence, and boundaries.

### Runtime Services

- [Provider guide](/docs/providers): Codex, Claude, and ACP behind the shared provider contract.
- [Teams and comms](/docs/teams): durable team messages, deliveries, retries, and spawn.
- [Processes](/docs/processes): runtime-managed host commands and logs.
- [Worktrees](/docs/worktrees): managed Git workspaces, claims, and cleanup.
- [MCP and sidecars](/docs/mcp-and-sidecars): Claude bridge and the bundled MCP sidecar.

### Client Builders

- [Usage model](/docs/usage): how thin clients should use the runtime.
- [Client design guide](/docs/client-design): UI and automation patterns that fit the runtime.
- [API guide](/docs/api): practical HTTP/SSE examples.

### Operators

- [Configuration reference](/docs/configuration): every config section.
- [Operations runbook](/docs/operations): day-two commands.
- [Deployment guide](/docs/deployment): VPS/systemd-oriented deployment.
- [Goosetower and Gooseweb deployment](/docs/goosetower-deployment): browser gateway, Vercel, and WebSocket operations.
- [Security model](/docs/security): bearer auth, local trust, process execution, and provider credentials.
- [Troubleshooting](/docs/troubleshooting): common failure playbooks.

### Reference

- [Endpoint catalog](/docs/endpoint-catalog): route-by-route API surface.
- [Repo guide](/docs/repo-guide): where the implementation lives.
- [API doc sync workflow](/docs/api-doc-sync): how API docs stay synchronized with code.
- [Changelog](/docs/changelog): tagged release notes.

## What the runtime owns

Gooselake owns these responsibilities because clients are poor places to keep them:

- provider-backed sessions and opaque provider references
- one-active-turn session coordination
- durable turns, approvals, and terminal states
- replayable session/team/process/global events
- provider auth staging and provider readiness checks
- process execution and bounded log capture
- team messages, deliveries, retries, cancellation, and spawn operations
- managed worktree creation, claims, release, and cleanup
- diagnostics and startup recovery summaries
- MCP gateway calls that are tied back to an active runtime session

## What it does not try to be

Gooselake is not a multi-tenant SaaS backend, a hosted OAuth broker, or a universal model abstraction layer. It is intentionally a **host-owned runtime** for powerful machine work. SQLite, local provider credentials, process execution, and real filesystem access are features of that stance, not accidents.
