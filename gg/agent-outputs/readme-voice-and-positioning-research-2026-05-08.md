# README voice + positioning research (2026-05-08)

## TL;DR
If you want one sentence that lands:

**GG Runtime is an air traffic control tower for machine agents, not a chat widget with ambition problems.**

It earns that claim because the runtime owns cross-provider sessions/turns/events, durable replay, agent-to-agent delivery semantics, worktree/process lifecycle, and team operations, while the UI is just a client.

## The strongest narrative spine

Use this as the backbone of the rewrite:

1. Frontend-first agent apps feel fast until they become useful.
2. The moment they become useful, they inherit backend problems (state recovery, background execution, provider quirks, teammate coordination).
3. GG Runtime starts at that hard layer on purpose.
4. The “wow” is not just Codex + Claude support; it is that both are forced through one operational contract, then combined with real-time team coordination.

The key tone: not “AI future vibes,” more “we have seen this movie and skipped to the part where the architecture stops lying.”

## Punchy analogies that are actually accurate

Use these freely:

- **"It’s Kubernetes for agent turns, but with receipts."**
  Why accurate: runtime persists turn/session state, events, delivery states, and recovery metadata in SQLite rather than ephemeral UI memory.

- **"Your frontend is Slack; this runtime is the message bus plus scheduler plus incident log."**
  Why accurate: team messages become delivery records with state transitions, retries, cancellations, and event emission.

- **"Codex and Claude are aircraft types; the runtime is the control tower phrasebook."**
  Why accurate: provider-specific behavior is behind a shared trait + session/turn lifecycle.

- **"The browser tab is now a remote control, not the engine block."**
  Why accurate: long-running execution, process lifecycle, and replayable events are machine-side.

- **"It turns agent collaboration from roleplay into transport guarantees."**
  Why accurate: per-recipient delivery policies, deferred queues, idempotency keys, and retry semantics are explicit.

## Why frontend-first breaks down (repo-backed)

The runtime’s own route and model shape shows exactly what frontend-first apps eventually have to rebuild:

- Multiple durable domains: sessions, approvals, processes, worktrees, teams, deliveries, diagnostics, replay streams.
- Multiple stream scopes: global, session, process, team.
- Reconnect semantics with replay cursors and `last-event-id` support.
- Startup reconciliation and recovery behavior for sessions/turns/approvals/deferred deliveries.

Concrete proof points:

- One server route table already spans all these surfaces (`/v1/sessions/*`, `/v1/processes/*`, `/v1/worktrees/*`, `/v1/teams/*`, `/v1/events/*`) in [`crates/runtime-server/src/http.rs:49`](/Users/ashray/code/amxv/gg-agent-runtime/crates/runtime-server/src/http.rs:49).
- Session streams explicitly subscribe-before-replay to avoid handoff loss in [`crates/runtime-server/src/http.rs:475`](/Users/ashray/code/amxv/gg-agent-runtime/crates/runtime-server/src/http.rs:475).
- Runtime startup recovery reconciles stale runtime state in [`crates/runtime-core/src/runtime.rs:126`](/Users/ashray/code/amxv/gg-agent-runtime/crates/runtime-core/src/runtime.rs:126).

Translation for README voice: frontend-first works great right up to the moment you need correctness after disconnects, crashes, retries, and multi-agent contention.

## Why machine-side runtime is the right center of gravity

This repo is opinionated about where truth lives:

- Truth is append-only events + materialized state in SQLite, not browser component state.
- Provider auth + session refs are staged machine-side.
- Process/worktree side effects happen where the filesystem actually exists.

Concrete proof points:

- Runtime event storage and scoped sequence indexing live in SQLite schema (`runtime_events`, unique `(scope, scope_id, seq)`) at [`crates/runtime-store-sqlite/src/lib.rs:70`](/Users/ashray/code/amxv/gg-agent-runtime/crates/runtime-store-sqlite/src/lib.rs:70) and [`crates/runtime-store-sqlite/src/lib.rs:87`](/Users/ashray/code/amxv/gg-agent-runtime/crates/runtime-store-sqlite/src/lib.rs:87).
- Monotonic per-scope sequence assignment is explicit in [`crates/runtime-store-sqlite/src/lib.rs:277`](/Users/ashray/code/amxv/gg-agent-runtime/crates/runtime-store-sqlite/src/lib.rs:277).
- Runtime bootstrap wires providers, team comms, worktrees, process manager, and deferred-delivery recovery in one composition root at [`crates/runtime-server/src/bootstrap.rs:32`](/Users/ashray/code/amxv/gg-agent-runtime/crates/runtime-server/src/bootstrap.rs:32).

Use this line in README drafts:

"If your agents can mutate repos and spawn processes, the machine is the product boundary. We just made it explicit."

## Why the team messaging/comms layer is unusual and important

This is the strongest non-obvious differentiator. Most systems hand-wave agent communication as app-level chat. This runtime treats it like a transport system.

### What is technically interesting

- Delivery policies are first-class and explicit:
  - `non_interrupting`
  - `interrupt_after_tool_boundary`
  - `immediate_interrupt`
  - `start_new_turn_only`
  Defined in [`crates/runtime-core/src/team_comms.rs:20`](/Users/ashray/code/amxv/gg-agent-runtime/crates/runtime-core/src/team_comms.rs:20).

- Delivery lifecycle is explicit state machine (`pending`, `deferred`, `injecting`, `injected`, `failed`, `cancelled`) at [`crates/runtime-core/src/team_comms.rs:25`](/Users/ashray/code/amxv/gg-agent-runtime/crates/runtime-core/src/team_comms.rs:25).

- Message idempotency is built in (not an afterthought) with index keys and replay behavior at [`crates/runtime-core/src/team_comms.rs:388`](/Users/ashray/code/amxv/gg-agent-runtime/crates/runtime-core/src/team_comms.rs:388).

- Recipient injection is serialized by per-recipient guard locks, so deliveries do not trample active ownership at [`crates/runtime-core/src/team_comms.rs:539`](/Users/ashray/code/amxv/gg-agent-runtime/crates/runtime-core/src/team_comms.rs:539).

- Deferred messages automatically resume when turn boundaries are reached and on startup recovery (`resume_deferred_for_recipient`, `recover_startup_deferred_deliveries`) at [`crates/runtime-core/src/team_comms.rs:937`](/Users/ashray/code/amxv/gg-agent-runtime/crates/runtime-core/src/team_comms.rs:937) and [`crates/runtime-core/src/team_comms.rs:187`](/Users/ashray/code/amxv/gg-agent-runtime/crates/runtime-core/src/team_comms.rs:187).

- Team messages are transformed into injected turn inputs with explicit `<team_msg ...>` wrappers at [`crates/runtime-core/src/team_comms.rs:1948`](/Users/ashray/code/amxv/gg-agent-runtime/crates/runtime-core/src/team_comms.rs:1948).

- Team event replay/stream endpoints are first-class (`/v1/teams/{team_id}/events` + `/stream`) in [`crates/runtime-server/src/http.rs:134`](/Users/ashray/code/amxv/gg-agent-runtime/crates/runtime-server/src/http.rs:134).

### Why this matters in plain English

The runtime doesn’t just let agents "message" each other. It decides *when* to inject, *whether* to interrupt, *how* to defer, *how* to retry, and *what* happened when it failed.

That is operational comms, not chat UI.

## Why model-agnostic native agent layer matters

The core abstraction is intentionally provider-shaped but provider-agnostic.

- `ProviderKind` is normalized (`codex`, `claude`) in [`crates/runtime-core/src/provider.rs:9`](/Users/ashray/code/amxv/gg-agent-runtime/crates/runtime-core/src/provider.rs:9).
- `RuntimeProvider` trait defines the canonical contract (`create_session`, `send_turn`, `wait_for_turn`, approvals, interrupt, close) in [`crates/runtime-core/src/provider.rs:177`](/Users/ashray/code/amxv/gg-agent-runtime/crates/runtime-core/src/provider.rs:177).
- Runtime manager orchestrates against that contract, then appends normalized runtime events regardless of provider in [`crates/runtime-core/src/runtime.rs:1267`](/Users/ashray/code/amxv/gg-agent-runtime/crates/runtime-core/src/runtime.rs:1267).

This gives the writer a clean claim:

"Provider APIs change. Runtime contracts should not."

## How Codex and Claude become one shared operational layer

Codex and Claude are wildly different underneath, but this repo forces them into one session/turn lifecycle.

### Codex path

- Codex provider shells out to `codex exec ...`, tracks active turns, approvals, waiters, and terminal results in-process.
- It builds prompts from normalized turn input items and maps CLI outcomes into `ProviderTurnResult`.

Refs:

- Provider implementation starts at [`crates/runtime-provider-codex/src/lib.rs:430`](/Users/ashray/code/amxv/gg-agent-runtime/crates/runtime-provider-codex/src/lib.rs:430).
- `send_turn` path at [`crates/runtime-provider-codex/src/lib.rs:538`](/Users/ashray/code/amxv/gg-agent-runtime/crates/runtime-provider-codex/src/lib.rs:538).
- `wait_for_turn` path at [`crates/runtime-provider-codex/src/lib.rs:698`](/Users/ashray/code/amxv/gg-agent-runtime/crates/runtime-provider-codex/src/lib.rs:698).

### Claude path

- Claude provider talks JSON-RPC-ish over a bridge sidecar protocol.
- Bridge methods include `session.create`, `session.send`, `session.wait`, approval respond, interrupt, close.
- Provider maps bridge events/responses into the same `ProviderTurnResult` shape.

Refs:

- Provider impl starts at [`crates/runtime-provider-claude/src/lib.rs:1214`](/Users/ashray/code/amxv/gg-agent-runtime/crates/runtime-provider-claude/src/lib.rs:1214).
- `send_turn` at [`crates/runtime-provider-claude/src/lib.rs:1508`](/Users/ashray/code/amxv/gg-agent-runtime/crates/runtime-provider-claude/src/lib.rs:1508).
- `wait_for_turn` at [`crates/runtime-provider-claude/src/lib.rs:1610`](/Users/ashray/code/amxv/gg-agent-runtime/crates/runtime-provider-claude/src/lib.rs:1610).
- Bridge protocol method set in [`sidecars/claude-bridge/src/protocol.ts:3`](/Users/ashray/code/amxv/gg-agent-runtime/sidecars/claude-bridge/src/protocol.ts:3).

### The punchline

Different engine rooms, same cockpit instruments.

## Why real-time multi-agent supervision/control is compelling

The runtime already combines these powers under one roof:

- team member spawn with worktree assignment,
- automatic onboarding DM injection,
- operation journaling,
- rollback diagnostics on partial failure,
- interrupt-all active turns.

Refs:

- Spawn flow with journal stages and rollback hooks in [`crates/runtime-tools/src/lib.rs:2484`](/Users/ashray/code/amxv/gg-agent-runtime/crates/runtime-tools/src/lib.rs:2484).
- Rollback and diagnostic append path in [`crates/runtime-tools/src/lib.rs:1939`](/Users/ashray/code/amxv/gg-agent-runtime/crates/runtime-tools/src/lib.rs:1939).
- Team-wide interrupt endpoint plumbing at [`crates/runtime-server/src/http.rs:136`](/Users/ashray/code/amxv/gg-agent-runtime/crates/runtime-server/src/http.rs:136) and service logic at [`crates/runtime-core/src/team_comms.rs:1389`](/Users/ashray/code/amxv/gg-agent-runtime/crates/runtime-core/src/team_comms.rs:1389).

This is compelling because it feels like operational choreography, not API bingo.

## One important "don’t overclaim" note

Right now, the runtime HTTP team API is clearly first-class, but the current `RuntimeToolGateway` implementation is process-centric (`gg_process_*`) and rejects other tool namespaces.

Refs:

- Supported namespaces currently return only `gg_process` in [`crates/runtime-tools/src/lib.rs:1047`](/Users/ashray/code/amxv/gg-agent-runtime/crates/runtime-tools/src/lib.rs:1047).
- Namespace matcher only recognizes `gg_process` in [`crates/runtime-tools/src/lib.rs:1054`](/Users/ashray/code/amxv/gg-agent-runtime/crates/runtime-tools/src/lib.rs:1054).
- Meanwhile gg-mcp-server already exposes rich `gg_team_*` tool façades and serializes team calls per caller in [`sidecars/gg-mcp-server/src/server.rs:273`](/Users/ashray/code/amxv/gg-agent-runtime/sidecars/gg-mcp-server/src/server.rs:273) and [`sidecars/gg-mcp-server/src/server.rs:344`](/Users/ashray/code/amxv/gg-agent-runtime/sidecars/gg-mcp-server/src/server.rs:344).

How to phrase this safely in README:

"Team orchestration is a first-class runtime API today; MCP team/tool routing is evolving toward parity with the full HTTP surface."

## Suggested voice profile for README writer

Target voice: **smart friend who has done incident response at 2am and still has jokes left**.

- Confident, specific, slightly dry.
- No anthropomorphic AI hype.
- Emphasize failure modes and operational clarity.
- Keep metaphors mechanical, not mystical.

## Reusable copy snippets

- "Most agent stacks start as frontend demos and accidentally become distributed systems. GG Runtime skips the accident."
- "This runtime treats provider adapters as replaceable plumbing, not product architecture."
- "Agent-to-agent messaging is modeled like delivery infrastructure: policy, queueing, retries, cancellation, and audit events."
- "The browser reconnects; the runtime remembers."
- "Codex and Claude are different dialects. The runtime is the interpreter with a durable transcript."

## Evidence map (quick index)

- Core provider contract: [`crates/runtime-core/src/provider.rs:177`](/Users/ashray/code/amxv/gg-agent-runtime/crates/runtime-core/src/provider.rs:177)
- Runtime orchestration + recovery: [`crates/runtime-core/src/runtime.rs:126`](/Users/ashray/code/amxv/gg-agent-runtime/crates/runtime-core/src/runtime.rs:126)
- Team comms policy/state machine: [`crates/runtime-core/src/team_comms.rs:20`](/Users/ashray/code/amxv/gg-agent-runtime/crates/runtime-core/src/team_comms.rs:20)
- Team message injection wrapper: [`crates/runtime-core/src/team_comms.rs:1948`](/Users/ashray/code/amxv/gg-agent-runtime/crates/runtime-core/src/team_comms.rs:1948)
- HTTP/SSE surface (sessions/teams/processes/worktrees): [`crates/runtime-server/src/http.rs:49`](/Users/ashray/code/amxv/gg-agent-runtime/crates/runtime-server/src/http.rs:49)
- Replay-safe stream handoff: [`crates/runtime-server/src/http.rs:475`](/Users/ashray/code/amxv/gg-agent-runtime/crates/runtime-server/src/http.rs:475)
- SQLite durable events + scoped seq index: [`crates/runtime-store-sqlite/src/lib.rs:70`](/Users/ashray/code/amxv/gg-agent-runtime/crates/runtime-store-sqlite/src/lib.rs:70)
- Spawn/journal/rollback lifecycle: [`crates/runtime-tools/src/lib.rs:2484`](/Users/ashray/code/amxv/gg-agent-runtime/crates/runtime-tools/src/lib.rs:2484)
- Codex adapter impl: [`crates/runtime-provider-codex/src/lib.rs:430`](/Users/ashray/code/amxv/gg-agent-runtime/crates/runtime-provider-codex/src/lib.rs:430)
- Claude adapter + bridge integration: [`crates/runtime-provider-claude/src/lib.rs:1214`](/Users/ashray/code/amxv/gg-agent-runtime/crates/runtime-provider-claude/src/lib.rs:1214)
- Claude bridge protocol: [`sidecars/claude-bridge/src/protocol.ts:3`](/Users/ashray/code/amxv/gg-agent-runtime/sidecars/claude-bridge/src/protocol.ts:3)
