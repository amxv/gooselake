# Standalone Agent Runtime Implementation Plan

Date: 2026-05-05

Scope:
- Build a standalone Rust agent runtime service that can run independently of GG Desktop.
- Support a unified runtime across Codex and Clout/Claude-style provider adapters, with Codex and Claude as the concrete MVP providers.
- Expose the runtime over HTTP and SSE so any frontend or client can drive it.
- Reuse the desktop runtime under `tmp/gg-desktop/` as the seed implementation where it matches the target architecture.
- Follow `gg/design-doc-standalone.md` as the source of truth for behavior and architecture.

## State of Current System

The repo does not yet contain a standalone runtime crate, server binary, or HTTP API surface. The only implementation baseline is the desktop/Tauri runtime under `tmp/gg-desktop/src-tauri/src/agent_runtime/`, with sidecars under `tmp/gg-desktop/src-tauri/sidecars/`.

The desktop runtime already contains most of the backend subsystems the standalone service needs:
- Session and turn orchestration lives in `tmp/gg-desktop/src-tauri/src/agent_runtime/manager/`, centered on `AgentManager`.
- Provider abstraction is already explicit in `tmp/gg-desktop/src-tauri/src/agent_runtime/providers/mod.rs` and `providers/registry.rs`.
- Codex is already implemented as a pooled app-server provider under `providers/codex_app_server/`.
- Claude is already implemented as a Bun sidecar bridge provider under `providers/claude_code/` plus `sidecars/claude-bridge/`.
- Team lifecycle is already a backend service in `team/service.rs`.
- Team messaging and delivery coordination already exist in `comms/`.
- Managed worktree state and startup repair already exist in `managed_worktrees.rs`.
- Process execution already exists in `process_manager/`.
- The provider-facing tool boundary already exists in `mcp_tool_gateway/`, `gg_team_tools/`, `gg_process_tools/`, and `sidecars/gg-mcp-server/`.

The main mismatch is system boundary, not domain behavior:
- The runtime is exposed through Tauri commands in `tmp/gg-desktop/src-tauri/src/commands/agents/mod.rs` and desktop bindings in `tmp/gg-desktop/src-tauri/src/bindings.rs`, not HTTP/OpenAPI.
- Live updates are driven by in-process broadcast channels and replay buffers inside `AgentManager`, not a durable SSE stream.
- Persistence is split between JSON snapshots in `persistence/mod.rs` and a timeline-focused SQLite store in `timeline_store/mod.rs`, rather than one SQLite-first authoritative runtime store.
- Desktop-specific timeline projection and client recovery concerns are mixed into runtime-adjacent code paths and should not become the standalone API contract.

Operationally, the best extraction seams are already visible:
- `AgentManager` is the orchestration center to extract into a provider-agnostic runtime core.
- `AgentProvider` is already the correct adapter boundary to preserve.
- `CommsBroker`, `TeamService`, `ProcessManager`, and the worktree orchestration code are already service-level modules rather than UI glue.
- The Claude bridge and `gg-mcp-server` sidecars are already standalone process boundaries and should remain so.

## State of Ideal System

The target system is a single deployable runtime service, not a desktop backend. It should ship as one primary binary, expose a stable HTTP API, stream normalized runtime events over SSE, and persist runtime truth in SQLite.

At the architecture level, the ideal system has these properties:
- One public server process, for example `gg-runtime-server`.
- One stable HTTP/OpenAPI contract for commands and queries.
- One authoritative SSE event stream with replay for global and scoped subscriptions.
- One provider-agnostic runtime core that owns sessions, turns, approvals, events, teams, messages, deliveries, worktrees, processes, and recovery.
- Provider adapters for Codex and Claude, with provider-specific auth, sidecar/process management, event translation, and recovery hidden behind the adapter boundary.
- A runtime-owned MCP-compatible tool gateway for `gg_team_*`, `gg_process_*`, `gg_worktree_*`, and related internal tools.
- SQLite as the source of truth for state and replayable runtime events.
- First-class team/comms, process execution, and managed worktrees from MVP day one.
- Single-user bearer-token API auth, without multi-tenant SaaS concerns.

The clean target workspace should roughly follow the design doc split:
- `runtime-core`
- `runtime-store-sqlite`
- `runtime-provider-codex`
- `runtime-provider-claude`
- `runtime-tools`
- `runtime-server`
- `sidecars/gg-mcp-server`
- `sidecars/claude-bridge`

Behaviorally, the ideal system preserves the desktop runtime’s strengths while fixing its current shape:
- Keep provider routing, event normalization, turn coordination, delivery injection policy, process management, and worktree ownership semantics.
- Replace Tauri command handlers with public HTTP routes.
- Replace local-only event buses with durable event append plus SSE replay.
- Replace mixed JSON-plus-SQLite persistence with SQLite-first runtime truth.
- Keep timeline and UI projections as internal or client-side concerns, not public API contracts.

## Cross-provider Requirements

These requirements apply across all phases and should be treated as invariants, not optional polish:

- Runtime-owned IDs: runtime session IDs, turn IDs, approval IDs, message IDs, delivery IDs, process IDs, and worktree IDs must be assigned by runtime core. Provider refs remain opaque provider-owned identifiers.
- Provider-agnostic core state: sessions, turns, approvals, events, teams, messages, deliveries, worktrees, and processes must have normalized core models that do not leak provider-specific assumptions.
- One active turn rule: a session may have at most one active turn, regardless of provider. Late or conflicting provider terminal events must fail closed.
- Durable approvals: provider approval requests and responses must survive restart and remain auditable.
- Durable event replay: critical events must be persisted and replayable through SSE. High-volume deltas may be coalesced, but session/team/process state transitions may not be dropped.
- Tool caller identity: every provider-originated tool call must carry caller session identity and, when available, turn and tool call correlation.
- Cross-provider team delivery policy: `non_interrupting`, `interrupt_after_tool_boundary`, `immediate_interrupt`, and `start_new_turn_only` must be enforced in runtime core, not reimplemented differently per provider.
- Provider auth isolation: Codex and Claude should each use runtime-managed config/auth directories so the standalone service is self-contained and deployable on arbitrary hosts.
- Shared operational surfaces: both providers must integrate with the same process manager, team/comms services, worktree services, and event bus contracts.
- Recovery parity: provider crashes, restart recovery, deferred delivery resumption, and managed worktree normalization should produce consistent core outcomes even when provider-specific inspection differs.
- HTTP contract stability: the public API must not expose desktop-specific bindings, projection internals, or frontend reducer assumptions.

## Plan Phases

### Phase 1: Create the standalone workspace shell and runtime composition root

Files to read before starting:
- `gg/design-doc-standalone.md`
- `tmp/gg-desktop/src-tauri/src/agent_runtime/mod.rs`
- `tmp/gg-desktop/src-tauri/src/agent_runtime/manager/mod.rs`
- `tmp/gg-desktop/src-tauri/src/commands/agents/mod.rs`
- `tmp/gg-desktop/src-tauri/src/bindings.rs`

What to do:
- Create the new Rust workspace and crate layout for `runtime-core`, `runtime-store-sqlite`, `runtime-provider-codex`, `runtime-provider-claude`, `runtime-tools`, and `runtime-server`.
- Add a single `gg-runtime-server` binary crate with config loading, dependency wiring, and startup lifecycle.
- Define the initial configuration model for server bind address, public base URL, bearer token auth, data directories, provider settings, event queue limits, process limits, and worktree settings.
- Lift runtime composition out of the desktop/Tauri app model so the new server owns provider registry, store, tool gateway, process manager, team/comms services, and worktree services directly.
- Preserve the desktop runtime’s clean service seams, but do not carry over Tauri state wrappers or command bindings.
- Decide the MVP module boundaries early so later phases do not keep re-exporting desktop internals through temporary glue.

Validation strategy:
- `cargo check` passes for the new workspace and server composition root.
- Server starts successfully with placeholder providers and a stub health route.
- Config loading, data-dir creation, and bearer token bootstrap all work on a clean machine.
- Basic dependency graph is one-directional: server depends on core/store/providers/tools, not the reverse.

Risks / fallbacks:
- Risk: early crate boundaries become leaky and later phases start depending on desktop-only types.
- Fallback: if extraction pressure is high, keep a temporary compatibility module inside `runtime-core` that wraps desktop-derived types, but keep it private and schedule deletion before MVP completion.
- Risk: bootstrapping too much API surface before the core exists will harden the wrong abstractions.
- Fallback: keep Phase 1 limited to composition root, config, and skeleton routes only.

### Phase 2: Build the SQLite-first runtime store and durable event model

Files to read before starting:
- `gg/design-doc-standalone.md`
- `tmp/gg-desktop/src-tauri/src/agent_runtime/persistence/mod.rs`
- `tmp/gg-desktop/src-tauri/src/agent_runtime/timeline_store/mod.rs`
- `tmp/gg-desktop/src-tauri/src/agent_runtime/managed_worktrees.rs`
- `tmp/gg-desktop/src-tauri/src/agent_runtime/events.rs`
- Any Phase 1 workspace and config files that define data paths and startup wiring

What to do:
- Implement the SQLite schema proposed in the design doc for sessions, turns, approvals, runtime events, teams, team members, team messages, team deliveries, managed worktrees, managed worktree claims, processes, credentials, and diagnostics.
- Create a store layer with repositories for runtime state plus an append-oriented event log.
- Define the normalized event envelope used by the standalone runtime, with explicit scope, scope ID, session/team/turn linkage, criticality, provider metadata, and replay sequence numbers.
- Migrate desktop concepts rather than desktop persistence format: reuse the domain models and repair logic, but do not preserve the JSON snapshot architecture as the new source of truth.
- Keep timeline projection concerns out of the store contract. The durable store should capture runtime truth, not desktop rendering shape.
- Implement startup migrations and a first recovery bootstrap that hydrates sessions, teams, messages, deliveries, worktrees, and processes from SQLite.

Validation strategy:
- Store migrations apply on a fresh database and on a partially populated database without destructive resets.
- Repository tests cover insert, update, replay, idempotency, and restart hydration for core tables.
- Event sequences are monotonic and scoped correctly for global, session, team, and process streams.
- Recovery bootstrap can rehydrate sample session/team/worktree state created by tests.

Risks / fallbacks:
- Risk: trying to map desktop timeline rows directly into the public event log will overfit the standalone store to UI history.
- Fallback: keep a separate optional projection/materialization table if needed, but maintain the runtime event log as the authoritative public replay source.
- Risk: migration scope grows too large because every desktop persistence concern is carried forward.
- Fallback: keep MVP storage limited to the design doc entities and defer desktop-only artifacts such as UI projection caches.

### Phase 3: Extract runtime core state machines and session/turn APIs with Codex-first vertical slice

Files to read before starting:
- `tmp/gg-desktop/src-tauri/src/agent_runtime/session.rs`
- `tmp/gg-desktop/src-tauri/src/agent_runtime/manager/mod.rs`
- `tmp/gg-desktop/src-tauri/src/agent_runtime/manager/runtime_ops/construction.rs`
- `tmp/gg-desktop/src-tauri/src/agent_runtime/manager/session_api/session_lifecycle.rs`
- `tmp/gg-desktop/src-tauri/src/agent_runtime/manager/session_api/turn_management.rs`
- `tmp/gg-desktop/src-tauri/src/agent_runtime/providers/mod.rs`
- `tmp/gg-desktop/src-tauri/src/agent_runtime/providers/registry.rs`
- `tmp/gg-desktop/src-tauri/src/agent_runtime/providers/codex_app_server/mod.rs`
- `tmp/gg-desktop/src-tauri/src/agent_runtime/providers/codex_app_server/agent_provider_impl.rs`
- `tmp/gg-desktop/src-tauri/src/agent_runtime/providers/codex_app_server/transport.rs`
- Any Phase 2 core/store modules for sessions, turns, approvals, and events

What to do:
- Define the standalone `ProviderAdapter` trait and provider registry in the new core, using the existing `AgentProvider` contract as the seed.
- Extract runtime session and turn state machines from `RuntimeSession` and `AgentManager` into provider-agnostic core services.
- Port core turn coordination rules: create, resume, send turn, interrupt, approval response, close, wait, terminal reconciliation, and provider ownership validation.
- Implement the Codex adapter first using the existing transport pool, routing, auth transport separation, critical event handling, and interrupt reconciliation patterns.
- Keep the event sink/backpressure ideas from the desktop provider boundary, but convert them to durable event append plus stream fan-out rather than local-only broadcast.
- Define the first server routes for health, version, provider list/models, Codex auth, session CRUD, turn send, interrupt, approval response, and session event stream.

Validation strategy:
- Unit tests cover session and turn state transitions, duplicate terminal event handling, approval persistence, and interrupt reconciliation rules.
- Provider contract tests verify that Codex events map into normalized core events without session/turn ownership violations.
- End-to-end smoke test: authenticate Codex, create session, send input, stream events, interrupt a turn, resolve an approval, close the session.
- SSE session event stream replays from a saved cursor after reconnect.

Risks / fallbacks:
- Risk: `AgentManager` is large and the extraction may preserve too much desktop-internal shape.
- Fallback: extract state-machine behavior and provider wiring first, but defer non-essential projection features and desktop telemetry until later phases.
- Risk: Codex interrupt convergence logic is subtle and easy to regress.
- Fallback: preserve the existing inspection and delayed reconciliation behavior nearly verbatim for MVP, then simplify only after parity tests exist.

### Phase 4: Port the MCP tool gateway and process manager as first-class runtime services

Files to read before starting:
- `tmp/gg-desktop/src-tauri/src/agent_runtime/mcp_tool_gateway/mod.rs`
- `tmp/gg-desktop/src-tauri/src/agent_runtime/gg_process_tools/mod.rs`
- `tmp/gg-desktop/src-tauri/src/agent_runtime/gg_process_tools/router.rs`
- `tmp/gg-desktop/src-tauri/src/agent_runtime/process_manager/mod.rs`
- `tmp/gg-desktop/src-tauri/src/agent_runtime/process_manager/impl_api.rs`
- `tmp/gg-desktop/src-tauri/src/agent_runtime/process_manager/impl_runtime.rs`
- `tmp/gg-desktop/src-tauri/sidecars/gg-mcp-server/src/main.rs`
- `tmp/gg-desktop/src-tauri/sidecars/gg-mcp-server/src/gateway.rs`
- Any Phase 3 tool-routing and server auth modules

What to do:
- Move the MCP gateway into the standalone runtime so providers can call runtime-owned tools over a stable internal boundary.
- Preserve the caller identity rules from the desktop gateway: bearer token between sidecar and runtime, required caller agent ID, namespace routing, and bounded request sizes.
- Port the process manager as a standalone service with durable process records, stdout/stderr log files, process lifecycle events, timeouts, ownership, and kill controls.
- Expose public HTTP endpoints for process start/list/get/logs/kill, while also exposing the equivalent `gg_process_*` tool namespace through MCP.
- Make process output visible both through event streaming and durable log retrieval, with backpressure-aware sampling for stream consumers.
- Keep operator guardrails configurable, but default to a powerful host-runtime model as intended by the design doc.

Validation strategy:
- Tool invocation tests verify that MCP-originated `gg_process_*` calls resolve through the gateway with the correct caller identity and structured results.
- Process API tests cover spawn, completion, timeout, kill, retained logs, and log truncation semantics.
- End-to-end smoke test: provider session invokes `gg_process_run`, process emits runtime events, logs can be retrieved later over HTTP.
- Backpressure tests confirm stream output sampling does not lose process logs on disk.

Risks / fallbacks:
- Risk: provider-facing tool transport and public HTTP APIs drift semantically.
- Fallback: define one internal service interface and make both the MCP gateway and HTTP handlers call that shared service.
- Risk: process output overwhelms the event stream.
- Fallback: keep log files authoritative and coalesce stream output aggressively while preserving completion/status events.

### Phase 5: Port team lifecycle, comms broker, and team-focused HTTP/SSE surfaces

Files to read before starting:
- `tmp/gg-desktop/src-tauri/src/agent_runtime/team/service.rs`
- `tmp/gg-desktop/src-tauri/src/agent_runtime/comms/mod.rs`
- `tmp/gg-desktop/src-tauri/src/agent_runtime/comms/api_send.rs`
- `tmp/gg-desktop/src-tauri/src/agent_runtime/comms/api_delivery/queue.rs`
- `tmp/gg-desktop/src-tauri/src/agent_runtime/comms/api_delivery/injection.rs`
- `tmp/gg-desktop/src-tauri/src/agent_runtime/comms/api_delivery/lifecycle.rs`
- `tmp/gg-desktop/src-tauri/src/agent_runtime/comms/api_query.rs`
- `tmp/gg-desktop/src-tauri/src/agent_runtime/events.rs`
- `tmp/gg-desktop/src-tauri/src/agent_runtime/team_operations/messaging.rs`
- Any Phase 2 store modules for teams/messages/deliveries and Phase 3 session automation injection surfaces

What to do:
- Extract `TeamService` and `CommsBroker` into the standalone runtime core with SQLite-backed state and durable team events.
- Preserve the runtime’s existing team invariants: lead is always a member, sender/recipient must be in-team, broadcast expands to per-recipient deliveries, and delivery lifecycle is explicit.
- Implement team HTTP routes for create/list/get/join/remove/set lead/delete, direct message, broadcast, list messages, list deliveries, retry delivery, cancel message, team snapshot, team events, and interrupt-all.
- Implement global and team-scoped SSE streams with replay, so clients can rebuild team state without desktop-specific reducers.
- Keep delivery policy semantics in core: `start_new_turn_only`, `non_interrupting`, `interrupt_after_tool_boundary`, and `immediate_interrupt`.
- Reuse the recipient injection guard and delivery queue ideas from the desktop runtime, but persist delivery state transitions before and after injection attempts.

Validation strategy:
- Unit tests cover direct messaging, broadcast expansion, idempotency keys, delivery FSM transitions, queue blocking, retry, cancellation, and team membership validation.
- Integration tests verify team event replay, scoped SSE cursors, and reconnect correctness.
- End-to-end smoke test: create team, send direct message, send broadcast, observe deferred and injected deliveries, retry a failed delivery, inspect team snapshot.
- Regression tests verify provider-agnostic delivery behavior with at least one Codex-backed recipient session.

Risks / fallbacks:
- Risk: carrying over desktop team event shapes too literally will entangle the public API with UI history.
- Fallback: preserve semantic event kinds but reshape envelopes around the standalone runtime event log contract.
- Risk: delivery injection races become more visible once HTTP clients and provider sessions both act concurrently.
- Fallback: keep the recipient injection guard, explicit delivery states, and coordinator-owned interruption rules intact from the desktop runtime.

### Phase 6: Port managed worktrees, teammate spawning, and operation journaling

Files to read before starting:
- `tmp/gg-desktop/src-tauri/src/agent_runtime/managed_worktrees.rs`
- `tmp/gg-desktop/src-tauri/src/agent_runtime/gg_team_tools/router/manage.rs`
- `tmp/gg-desktop/src-tauri/src/agent_runtime/gg_team_tools/router/native_worktree.rs`
- `tmp/gg-desktop/src-tauri/src/agent_runtime/gg_team_tools/router/worktree_init.rs`
- `tmp/gg-desktop/src-tauri/src/agent_runtime/gg_team_tools/router/onboarding.rs`
- `tmp/gg-desktop/src-tauri/src/agent_runtime/team_operations/service.rs`
- `tmp/gg-desktop/src-tauri/src/agent_runtime/team_operations/native_worktree_cleanup.rs`
- `tmp/gg-desktop/src-tauri/src/agent_runtime/team_hooks/mod.rs`
- `tmp/gg-desktop/src-tauri/src/agent_runtime/persistence/mod.rs`
- Any Phase 5 team/comms service files and Phase 2 managed-worktree store modules

What to do:
- Port managed worktree records, claims, startup normalization, per-repo locking, and cleanup semantics into the standalone runtime.
- Add public worktree APIs for create/list/get/claim/release/cleanup, but treat teammate spawning as the primary user path.
- Port the teammate spawn orchestration from `gg_team_manage add`, including worktree planning, journal transitions, init-script execution, session creation, team join, onboarding message delivery, and rollback on failure.
- Move the current JSON journal concept into SQLite-backed operation journaling and diagnostics so restarts do not lose partial operation state.
- Preserve `.agents/gg/worktree-init.sh` as the default init hook path and keep the explicit distinction between created and reused worktrees.
- Keep cleanup conservative: removing a team member should release claims and attempt cleanup when eligible, but cleanup failure must not roll back membership removal.

Validation strategy:
- Unit tests cover managed worktree key normalization, claim conflict repair, startup hydration, and deletion-policy handling.
- Integration tests cover create-new-worktree spawn, reuse-existing-worktree spawn, join/remove member lifecycle, and rollback after init-script or session-create failure.
- End-to-end smoke test: create team, spawn teammate into a new worktree, verify onboarding delivery, remove teammate, verify cleanup behavior and diagnostics.
- Restart recovery test: crash between journal stages and confirm the server surfaces deterministic diagnostics and converges to a safe state.

Risks / fallbacks:
- Risk: spawn-member operations have the largest partial-failure surface in the system.
- Fallback: persist journal transitions aggressively and prefer safe retention of worktree artifacts over destructive cleanup when state is ambiguous.
- Risk: current desktop cleanup logic relies partly on reference scans and git heuristics.
- Fallback: keep the heuristics as a safety backstop, but treat persisted claim state as authoritative in the standalone service.

### Phase 7: Port the Claude provider, runtime-managed auth, and cross-provider parity

Files to read before starting:
- `tmp/gg-desktop/src-tauri/src/agent_runtime/providers/claude_code/mod.rs`
- `tmp/gg-desktop/src-tauri/src/agent_runtime/providers/claude_code/provider_impl/mod.rs`
- `tmp/gg-desktop/src-tauri/src/agent_runtime/providers/claude_code/provider_impl/agent_provider_impl.rs`
- `tmp/gg-desktop/src-tauri/src/agent_runtime/providers/claude_code/provider_impl/gg_mcp.rs`
- `tmp/gg-desktop/src-tauri/src/agent_runtime/providers/claude_code/bridge_runtime/`
- `tmp/gg-desktop/src-tauri/sidecars/claude-bridge/src/main.ts`
- `tmp/gg-desktop/src-tauri/sidecars/claude-bridge/src/protocol.ts`
- Any Phase 3 provider trait/core event mapping files and Phase 4 MCP gateway runtime files

What to do:
- Port the Claude adapter and Bun sidecar bridge into the standalone workspace, preserving bridge lifecycle management, stdout lane processing, heartbeat, request/response correlation, and per-session event sequencing.
- Implement runtime-managed Claude auth configuration using a dedicated runtime-owned config directory, not the operator’s ambient `~/.claude`.
- Add Claude auth endpoints for API key and `auth.json` import, plus logout/status and optional multipart upload support.
- Ensure the Claude bridge receives `CLAUDE_CONFIG_DIR` from runtime config and that `gg-mcp-server` bootstrap remains compatible with Claude SDK expectations.
- Normalize Claude events into the same core runtime event model used by Codex, including session start/resume, tool calls, approvals, deltas, turn completion, and provider recovery signals.
- After Claude lands, verify that team/comms, process tools, worktrees, and recovery behave consistently across mixed-provider teams.

Validation strategy:
- Provider contract tests verify Claude session create/resume/send/interrupt/approval/wait/close semantics against the normalized core trait.
- Auth tests cover API key config, `auth.json` import, logout, status reporting, and config-dir wiring.
- Mixed-provider integration tests cover a Codex lead and Claude teammate using shared team/comms and process services.
- End-to-end smoke test: import Claude auth, create Claude session, run tool call through MCP, send team message, remove session cleanly.

Risks / fallbacks:
- Risk: Claude bridge protocol or SDK behavior drifts more often than the Rust side can assume.
- Fallback: keep the Bun sidecar protocol narrow and versioned, and isolate compatibility fixes inside the sidecar instead of polluting core runtime logic.
- Risk: Claude auth file shape changes.
- Fallback: support importing a bundle of provider config files, but keep `auth.json` as the first-class MVP path.

### Phase 8: Recovery, diagnostics, API hardening, and acceptance demo

Files to read before starting:
- `gg/design-doc-standalone.md`
- `tmp/gg-desktop/src-tauri/src/agent_runtime/events.rs`
- `tmp/gg-desktop/src-tauri/src/agent_runtime/team_operations/diagnostics.rs`
- `tmp/gg-desktop/src-tauri/src/agent_runtime/provider_auth_fallback/`
- `tmp/gg-desktop/src-tauri/src/agent_runtime/compaction_notifications/`
- All standalone server/core/store/provider files created in Phases 1 through 7

What to do:
- Implement startup recovery for sessions, turns, deferred deliveries, worktrees, processes, and provider-side crash inspection.
- Add diagnostics endpoints for provider health, comms, team operations, process manager, worktrees, and recovery state.
- Finalize SSE replay semantics, cursor handling, error shapes, and auth enforcement across all public routes.
- Add comprehensive integration coverage for restart recovery, provider process crashes, interrupted turns, delivery retries, worktree cleanup failures, and lost client reconnects.
- Produce a minimal acceptance flow that proves the hosted-runtime thesis: a remote client authenticates providers, creates a lead session, creates a team, spawns a teammate in a worktree, streams events over SSE, runs a process, exchanges team messages, and cleans up.
- Document the deployment and operational expectations for a single-user runtime with full-machine access.

Validation strategy:
- Crash/restart tests verify that active sessions remain queryable, deferred deliveries resume, and worktree claims normalize correctly after restart.
- Diagnostics tests verify that the runtime exposes actionable state for provider failures, queued deliveries, and partial worktree cleanup.
- Security checks confirm bearer token enforcement, runtime-managed provider config dirs, and sensible defaults for dangerous host capabilities.
- Final acceptance demo passes end to end on a clean host or VPS using both Codex and Claude.

Risks / fallbacks:
- Risk: recovery semantics sprawl if each subsystem invents its own failure model.
- Fallback: route every subsystem through the same durable event and status model, then specialize only the provider-specific inspection/retry pieces.
- Risk: WebSocket or advanced convenience surfaces consume time without increasing MVP confidence.
- Fallback: keep REST plus SSE authoritative and defer WebSocket until after the acceptance demo succeeds.

## Recommended Implementation Order

Implement the phases in the order above, but keep two practical rules:
- Do not start Claude before the Codex-backed vertical slice, HTTP/SSE contract, and SQLite event model are stable.
- Do not start worktree-heavy teammate spawning before team/comms and process/tool routing are already functional in the standalone server.

This order preserves the shortest path to a usable hosted runtime:
1. Server shell and composition root.
2. SQLite-first store and event model.
3. Session/turn core plus Codex vertical slice.
4. Tool gateway and process manager.
5. Team/comms and team event streams.
6. Managed worktrees and teammate spawning.
7. Claude provider and mixed-provider parity.
8. Recovery, diagnostics, and acceptance demo.

## MVP Exit Criteria

The standalone runtime is ready for first real use when all of the following are true:
- A clean machine can start `gg-runtime-server` with a data directory and bearer token.
- Codex auth and Claude auth can be configured entirely through runtime APIs.
- A client can create sessions, send turns, resolve approvals, and stream normalized events over SSE.
- A client can create a team, send direct and broadcast messages, inspect deliveries, and retry/cancel as needed.
- A provider session can invoke `gg_process_*` tools successfully through the runtime-owned MCP boundary.
- A lead session can spawn a teammate into a managed git worktree, deliver onboarding, and remove that teammate with correct cleanup behavior.
- Restart recovery preserves queryable state and resumes deferred work without corrupting sessions, deliveries, or worktree claims.
- The acceptance demo from the design doc succeeds on a VPS-style deployment.
