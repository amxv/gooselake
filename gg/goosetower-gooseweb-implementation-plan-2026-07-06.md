# Goosetower + Gooseweb Implementation Plan

Date: 2026-07-06

## State of Current System

Gooselake is currently the Rust agent runtime and source of truth. The workspace is a Rust workspace with these current members:

- `crates/runtime-core`
- `crates/runtime-store-sqlite`
- `crates/runtime-provider-codex`
- `crates/runtime-provider-claude`
- `crates/runtime-provider-acp`
- `crates/runtime-tools`
- `crates/runtime-server`

The repository also contains an Astro documentation site at the repo root. There is no existing TanStack Start application and no existing Goosetower service crate.

Current runtime server shape:

- `crates/runtime-server` builds the `gg-runtime-server` binary.
- `crates/runtime-server/src/http/mod.rs` exposes protected `/v1` HTTP routes behind bearer auth.
- `crates/runtime-server/src/http/events.rs` exposes global and session event replay plus SSE streams.
- `crates/runtime-server/src/http/teams.rs` exposes team lifecycle, team message, delivery, team snapshot, team event replay, and team event SSE routes.
- `crates/runtime-server/src/http/processes.rs` exposes process lifecycle, log read, process event replay, and process event SSE routes.
- `crates/runtime-server/src/http/sessions.rs` exposes session lifecycle, turns, interrupts, and approvals.
- `crates/runtime-server/src/http/worktrees.rs` exposes managed worktree lifecycle and claims.
- `crates/runtime-server/src/http/diagnostics.rs` exposes provider, process, worktree, recovery, comms, and team operation diagnostics.
- `crates/runtime-server/src/openapi.rs` and `openapi/runtime-server-openapi.yaml` define and publish API docs for the runtime server.

Current durable state and event ledger:

- `crates/runtime-store-sqlite/src/schema.rs` stores sessions, turns, approvals, runtime events, teams, team members, team messages, team deliveries, managed worktrees, worktree claims, processes, credentials, team operation journal entries, team operation diagnostics, and diagnostics journal records.
- `crates/runtime-core/src/state.rs` defines the shared record types exported by `runtime-core`.
- `RuntimeEventRecord` has `row_id`, `event_id`, `scope`, `scope_id`, `session_id`, `team_id`, `turn_id`, `seq`, `kind`, `criticality`, `payload`, `provider`, `provider_seq`, and `created_at`.
- `RuntimeEventScope` already covers `session`, `team`, `process`, `worktree`, and `system`.
- `RuntimeEventCriticality` currently distinguishes `critical` and `droppable`.
- Event replay is sequence-numbered per scoped stream through `seq`, and global replay uses the SQLite row ID as the global cursor.
- SSE streaming uses replay-then-live handoff where available and honors `Last-Event-ID` through shared parser logic.

Current source-of-truth services:

- `RuntimeSessionManager` owns session, turn, approval, provider interaction, interrupt, startup recovery, event append, and session event broadcast behavior.
- `RuntimeTeamCommsService` owns teams, members, direct and broadcast messages, delivery records, delivery injection, retry/cancel behavior, team snapshots, team operation journal, and team event replay.
- `RuntimeProcessManager` owns local process execution, process records, stdout/stderr log files, log read, process event broadcast, process replay, and kill behavior.
- `RuntimeWorktreeService` owns managed worktree creation, reuse, claims, release, cleanup, and team member worktree integration.
- `RuntimeApp` composes the provider registry, store, tool gateway, process manager, team comms, and worktree services.

Current API/control coverage relevant to Gooseweb V0:

- Sessions: create, list, get, resume, close.
- Turns: send turn, interrupt turn.
- Approvals: respond to approval.
- Teams: create, list, get, delete, join member, spawn member, remove member, set lead, interrupt all.
- Messages: send direct, broadcast, list team messages.
- Deliveries: list, retry, cancel by message.
- Team view: current team snapshot.
- Runtime events: replay and SSE stream globally, by session, by team, and by process.
- Processes: start, list, get, read logs, kill.
- Worktrees: create, list, get, claim, release, cleanup.
- Provider state: provider list/model list and provider auth status routes.
- Diagnostics: recovery, providers, comms, processes, worktrees, team operations.

Current architecture/data-flow reconnaissance:

```text
Human or MCP client
  -> runtime-server HTTP bearer routes under /v1
  -> RuntimeApp / RuntimeSessionManager / RuntimeTeamCommsService / RuntimeProcessManager / RuntimeWorktreeService
  -> runtime-store-sqlite durable records and runtime_events ledger
  -> runtime-server replay endpoints and SSE streams
  -> providers and sidecars for Codex, Claude, ACP, gg-mcp-server, claude-bridge
```

Current limitations for the target product:

- There is no browser realtime gateway optimized for a desktop-class multi-agent UI.
- The runtime server exposes JSON HTTP/SSE, not a browser-facing binary Protobuf protocol.
- There is no server-side Goosetower materialized read model for fleet board rows, approval inbox rows, selected-session summaries, process tails, or cross-runtime source health.
- There is no connection-ticket flow for direct browser-to-VPS WebSocket authentication.
- There is no subscription/interest-management model for browser-visible board rows, pinned sessions, selected team, selected session detail, selected process tails, or approval inboxes.
- There are no gateway priority lanes for critical command acks, state patches, token streams, and bulk logs.
- There is no client-side Web Worker realtime core for Protobuf decode, replay, dedupe, coalescing, cursor persistence, or handoff into React state.
- There is no Gooseweb app shell, agent workspace, team workspace, Board, Inbox, Ledger, Fleet, Playbooks, or settings/admin UI.
- Multi-runtime source identity is not yet first-class in runtime events. Current events have provider fields, but not a gateway-owned `{source_id, source_epoch, source_seq}` cursor vector.

## State of Ideal System

The initial target system has three clear layers:

```text
Gooseweb browser app on Vercel
  -> direct wss://goosetower.example.com/v1/realtime?ticket=...
  -> Goosetower Rust service on VPS
  -> Gooselake runtime-server HTTP/SSE on VPS
  -> Gooselake runtime store, event ledger, providers, processes, teams, worktrees
```

Gooselake remains the source of truth:

- Sessions, turns, approvals, teams, team messages, team deliveries, processes, process logs, worktrees, provider state, runtime event ledger, replay, and recovery remain owned by Gooselake.
- Goosetower must not become a second execution database. It may cache/materialize state, but source-owned mutations are routed to Gooselake runtime APIs.
- Any source-of-truth behavior change in runtime APIs must update runtime API docs and OpenAPI artifacts through the repo's API doc sync workflow.

Goosetower is a separate Rust binary/service:

- It is a workspace member under a new crate, recommended as `crates/goosetower`.
- It runs as its own process, with its own config, bind address, auth settings, runtime registry, materialized read model, protocol codegen, and observability.
- It talks to one or more Gooselake runtime-server instances over HTTP/SSE.
- V0 targets one configured Gooselake runtime first.
- Later phases add multiple runtimes and RunPod-backed sources without changing the browser protocol shape.

Goosetower responsibilities:

- Browser realtime gateway over WebSocket.
- Public browser protocol over Protobuf frames.
- Runtime registry and source health tracking.
- Runtime HTTP command routing.
- Runtime SSE fan-in and replay-to-live handoff.
- Gateway-side materialized read models for Gooseweb views.
- Subscription management and per-client interest filtering.
- Priority lanes and outbound queue/backpressure policy.
- Idempotent command admission, command acks, command rejection, and duplicate handling.
- Short-lived connection-ticket validation and refresh.
- Origin allowlisting and message-level authorization hooks.
- Gateway audit events and command audit records.
- Stale, gap, reconnect, replay, and snapshot-resync signaling.
- Future fleet provisioning hooks for RunPod or other runtime sources.

Gooseweb responsibilities:

- A TanStack Start browser app, deployed to Vercel, serving the human operating experience only.
- A Web Worker realtime core that owns the WebSocket connection, Protobuf decode/encode, cursors, resume, dedupe, coalescing, command retry metadata, and state patches.
- A React/TanStack UI that consumes materialized client state through TanStack Store or `useSyncExternalStore`.
- A `shadcn/ui`-based component system for the app shell, navigation, forms, overlays, tables, alerts, and operating panels. Treat `shadcn/ui` source components as the default UI primitive layer rather than building ad hoc bespoke controls first.
- Virtualized logs, timelines, inboxes, ledger surfaces, and feeds.
- Desktop-class multi-agent operating workflows, not just passive dashboards.

Gooseweb V0 UI scope:

- Agent workspace:
  - Select session/agent.
  - View provider, model, status, cwd, worktree, active turn, failure state, context gauge when available, and recent activity.
  - Send a turn to the selected agent.
  - Interrupt an active turn.
  - View current turn stream, timeline events, tool calls, approval requests, and terminal states.

- Team workspace:
  - Select team.
  - View team lead, members, titles, member providers, member worktrees, delivery state, failed/deferred deliveries, and recent coordination events.
  - Send direct team messages.
  - Broadcast to a team.
  - Spawn teammates using the runtime team-member spawn API.
  - Remove teammates if allowed.
  - Retry/cancel failed or deferred deliveries.
  - Interrupt all team turns.

- Board:
  - Fleet-style view of active agents/sessions and team membership.
  - Row status, source health, provider/model, active turn, worktree, pending approvals, unread/error indicators, delivery indicators, process indicators, and latest activity.
  - Viewport-aware row subscriptions.

- Inbox:
  - Pending approvals across visible/allowed sessions.
  - Approval risk/context details.
  - Approve/reject with pending, accepted, rejected, stale, and resolved states.
  - Reject or disable dangerous approvals when source state is stale or replay has gaps.

- Ledger:
  - Runtime/gateway event feed with filters by source, scope, session, team, process, kind, criticality, and command ID.
  - Replay/staleness markers when the gateway uses snapshot resync.

- Fleet:
  - Runtime source registry, health, replay lag, last source sequence, last gateway sequence, source epoch, active sessions, process capacity, provider availability, and future RunPod source placeholders.

- Playbooks:
  - V0 can be a minimal command/message template surface.
  - Do not build a complex automation engine in the first slice.

- Settings/admin:
  - Connection status, gateway URL, runtime sources, auth/session state, protocol version, feature flags, and debug export.

Package/crate/app layout recommendation:

```text
Cargo.toml
crates/
  goosetower/
    Cargo.toml
    build.rs
    src/
      main.rs
      lib.rs
      config.rs
      http/
        mod.rs
        health.rs
        ticket.rs
        realtime.rs
        debug.rs
      auth/
        mod.rs
        tickets.rs
        origin.rs
        permissions.rs
      protocol/
        mod.rs
        codec.rs
        generated.rs
      runtime/
        mod.rs
        client.rs
        registry.rs
        sse.rs
        commands.rs
        replay.rs
      materializer/
        mod.rs
        state.rs
        reducers.rs
        snapshots.rs
        subscriptions.rs
      gateway/
        mod.rs
        connection.rs
        lanes.rs
        resume.rs
        commands.rs
        audit.rs
        metrics.rs
      tests/
        support.rs
proto/
  goosetower/
    v1/
      realtime.proto
      view.proto
      commands.proto
      common.proto
apps/
  gooseweb/
    package.json
    components.json
    app/
      routes/
      components/
      features/
      realtime/
      stores/
      worker/
      styles/
    public/
    vite.config.ts
    tsconfig.json
```

Recommended dependency direction:

- Rust gateway HTTP/WebSocket: `axum` with WebSocket support.
- Async/runtime: existing workspace Tokio/Tower style.
- Runtime HTTP client: `reqwest` or `hyper` client; prefer `reqwest` unless the repo already standardizes on a lower-level client by the time this is implemented.
- SSE client: `eventsource-stream`, `reqwest-eventsource`, or a small runtime-specific parser over streaming HTTP. Pick the crate after checking current docs with `webctx` if APIs have shifted.
- Protobuf Rust: `prost` and `prost-build`.
- Protobuf TS: `@bufbuild/protobuf` plus `protoc-gen-es`, or the current Buf-recommended equivalent after checking docs.
- Gooseweb: TanStack Start, React, `shadcn/ui`, TanStack Store or `useSyncExternalStore`, TanStack Virtual, and a dedicated Web Worker.

Wire protocol scope:

- Transport: WebSocket first.
- Encoding: binary Protobuf frames.
- Compatibility: versioned envelope with reserved field numbers and explicit protocol version negotiation.
- Required client-to-server messages:
  - `AuthRefresh`
  - `Ping`
  - `Resume`
  - `Subscribe`
  - `Unsubscribe`
  - `Ack`
  - `CommandSendTurn`
  - `CommandResolveApproval`
  - `CommandInterruptTurn`
  - `CommandSendTeamMessage`
  - `CommandBroadcastTeamMessage`
  - `CommandSpawnTeamMember`
  - `CommandRetryDelivery`
  - `CommandCancelDelivery`
  - `CommandKillProcess`
  - `CommandStartProcess` if the UI exposes process launch in V0

- Required server-to-client messages:
  - `Hello`
  - `Snapshot`
  - `Patch`
  - `Event`
  - `CommandAccepted`
  - `CommandRejected`
  - `CommandDuplicate`
  - `AuthExpiring`
  - `AuthRefreshed`
  - `ConnectionDegraded`
  - `SourceGapDetected`
  - `SourceGapFilled`
  - `SourceSnapshotResync`
  - `Error`
  - `Pong`

- Required envelope fields:
  - `protocol_version`
  - `message_id`
  - `message_kind`
  - `lane`
  - `gateway_seq`
  - `source_id`
  - `source_epoch`
  - `source_seq`
  - `scope`
  - `scope_id`
  - `entity_version`
  - `kind`
  - `command_id`
  - `happened_at`
  - `observed_at`
  - payload `oneof`

- Cursor model:
  - `gateway_seq` is Goosetower's observed merge order.
  - `{source_id, source_epoch, source_seq}` is the correctness cursor for runtime replay and gap detection.
  - Browser resume sends both the last applied gateway cursor and a source cursor vector.

Priority lanes:

- Lane 0 `critical`: command acks/rejections, approval requested/resolved, turn failed/interrupted/completed, auth expiring/revoked, source gaps.
- Lane 1 `state`: session status, turn phase, process status, team membership, delivery state, board row patches.
- Lane 2 `tokens`: assistant text deltas and status text where allowed.
- Lane 3 `bulk`: stdout/stderr samples, diagnostic logs, trace/debug data.

Subscription model:

- Always-on approval inbox subscription for the user's allowed workspace/sources.
- Board subscription with window/filter/sort parameters.
- Visible-row detail subscriptions for currently visible rows.
- Selected session detail subscription with optional token stream.
- Selected team detail subscription.
- Demand-driven process tail subscriptions.
- Ledger subscription with filters and explicit limits.

Auth/ticket flow:

1. User authenticates to Gooseweb through the chosen web auth provider.
2. Gooseweb server route or auth service mints a short-lived, single-use Goosetower ticket.
3. Ticket fields should include issuer, audience, subject/user ID, workspace ID, allowed origins, scopes/capabilities, expiry of roughly 30-60 seconds, and a `jti` nonce.
4. Browser opens `wss://goosetower.example.com/v1/realtime?ticket=...`.
5. Goosetower validates the ticket before accepting or immediately after upgrade with a strict auth timeout, validates the `Origin` header against an exact allowlist, and consumes/rejects replayed `jti` values.
6. Goosetower sends `Hello` with connection ID, server time, heartbeat interval, max message size, protocol version, and resume support.
7. Browser sends `Resume` if it has cursors; otherwise it sends initial subscriptions.
8. Goosetower revalidates permissions on every sensitive command, not only at connection time.
9. Goosetower emits `AuthExpiring`; Gooseweb fetches a new ticket through its authenticated app route and sends `AuthRefresh` in-band.

Cross-runtime requirements:

- Runtime sources must have stable `source_id`, `source_kind`, `base_url`, auth material, health state, capabilities, and optional workspace affinity.
- Runtime source boots must carry a `source_epoch` so sequence resets are detectable.
- Goosetower persists last consumed cursor per source.
- Goosetower preserves strict per-source event order.
- Goosetower assigns `gateway_seq` when it observes/accepts an event.
- Goosetower must not infer cross-source causality from `gateway_seq`.
- Commands route to the owning runtime/source for the target session, team, process, or worktree.
- If ownership is unknown, stale, or gapped, commands fail as `source_unavailable`, `source_stale`, or `ownership_unknown` instead of queueing blindly.
- Multi-runtime board views aggregate source-owned materialized views but keep source ownership visible to the UI.
- Gap detection is per source. Replay and snapshot resync are per source.
- RunPod sources later must appear as runtime registry entries with health, lease/provisioning metadata, capabilities, and replay limits; they should not require a new browser protocol.

Staged implementation order:

1. Define boundaries and protocol skeleton.
2. Add the separate Goosetower Rust service with health/config/runtime registry.
3. Add Goosetower runtime client and single-runtime materializer.
4. Add Protobuf WebSocket gateway with tickets, hello, heartbeat, subscriptions, snapshots, patches, commands, and lanes.
5. Add Gooseweb TanStack Start app and Worker realtime core.
6. Build V0 desktop-class UX over one runtime.
7. Harden reconnect/replay/gap/staleness behavior.
8. Add observability, packaging, and deployment wiring.
9. Add multi-runtime abstractions.
10. Add RunPod/fleet provisioning hooks.

## Plan Phases

### Phase 0: Baseline Confirmation And Plan Guardrails

#### Files to read before starting

- `AGENTS.md`
- `Cargo.toml`
- `package.json`
- `Makefile`
- `crates/runtime-server/Cargo.toml`
- `crates/runtime-server/src/main.rs`
- `crates/runtime-server/src/config.rs`
- `crates/runtime-server/src/bootstrap.rs`
- `crates/runtime-server/src/http/mod.rs`
- `crates/runtime-core/src/state.rs`
- `crates/runtime-store-sqlite/src/schema.rs`
- `gg/agent-outputs/realtime-stack-research.md`
- `gg/agent-outputs/realtime-design-research.md`

#### What to do

- Confirm the implementer is on `main` and not in a feature worktree unless the lead explicitly assigns one.
- Confirm the repo still has no existing `crates/goosetower`, `apps/gooseweb`, or `proto/goosetower` package.
- Confirm the runtime server remains the source of truth and no new code path writes session/team/process/worktree truth outside Gooselake runtime APIs.
- Confirm the initial implementation is general product infrastructure, not AMA-specific.
- Confirm the first target is one Gooselake runtime source.
- Confirm the first public browser transport is WebSocket, not WebTransport.
- Confirm Protobuf over WebSocket is the browser protocol.
- Confirm the client realtime core starts in TypeScript/Web Worker, not Rust/WASM.

#### Validation strategy

- Run quick non-mutating reconnaissance:
  - `git status --short`
  - `cargo metadata --no-deps`
  - `bun --version`
- Do not run full checks yet unless code has already been changed.
- Record any pre-existing dirty files and avoid touching unrelated changes.

#### Risks / fallbacks

- Risk: another agent adds overlapping packages while implementation starts.
- Fallback: keep package additions isolated and reconcile only if paths overlap.
- Risk: TanStack Start package APIs have changed.
- Fallback: use `webctx` to check current TanStack Start and Buf docs before scaffolding Gooseweb.

### Phase 1: Protocol And Package Skeleton

#### Files to read before starting

- `Cargo.toml`
- `crates/runtime-server/Cargo.toml`
- `crates/runtime-server/src/main.rs`
- `crates/runtime-server/src/config.rs`
- `package.json`
- `tsconfig.json`
- `bun.lock`

#### What to do

- Add `crates/goosetower` as a new Rust workspace crate and binary.
- Do not fold Goosetower into `crates/runtime-server`.
- Add a `gg-goosetower` binary entrypoint.
- Add initial modules:
  - `config`
  - `http`
  - `auth`
  - `protocol`
  - `runtime`
  - `materializer`
  - `gateway`
- Add `proto/goosetower/v1/` as the shared protocol schema directory.
- Add initial `.proto` files:
  - `common.proto`
  - `view.proto`
  - `commands.proto`
  - `realtime.proto`
- Add `prost-build` code generation through `crates/goosetower/build.rs`.
- Reserve field numbers in Protobuf messages from the start.
- Add minimal generated Rust module wiring under `crates/goosetower/src/protocol/generated.rs`.
- Add a package-level README only if the implementation lead asks for docs; otherwise keep this phase code-only plus protocol comments.
- Keep source-of-truth runtime API unchanged in this phase.

Recommended initial Protobuf concepts:

- `Envelope`
- `Lane`
- `Scope`
- `SourceCursor`
- `CursorVector`
- `Hello`
- `Ping`
- `Pong`
- `Resume`
- `Subscribe`
- `Unsubscribe`
- `Ack`
- `Snapshot`
- `Patch`
- `GatewayEvent`
- `Command`
- `CommandAccepted`
- `CommandRejected`
- `CommandDuplicate`
- `Error`
- Fleet, session, team, approval, process, worktree, source-health view models.

#### Validation strategy

- `cargo fmt --check`
- `cargo check -p goosetower`
- Add unit tests for protocol version constants and basic encode/decode if practical in this phase.
- If Protobuf generation creates artifacts, verify generated files are either intentionally committed or intentionally built into `OUT_DIR`; do not commit unstable generated files unless repo conventions require it.

#### Risks / fallbacks

- Risk: `prost-build` setup conflicts with workspace build assumptions.
- Fallback: keep codegen scoped to `goosetower/build.rs` and avoid workspace-wide build changes.
- Risk: TypeScript generation strategy is not yet chosen.
- Fallback: define `.proto` source now and defer TS generation to the Gooseweb phase, but avoid Rust-only schema assumptions.

### Phase 2: Goosetower Config, Runtime Registry, And Health Surface

#### Files to read before starting

- `crates/runtime-server/src/config.rs`
- `crates/runtime-server/src/main.rs`
- `crates/runtime-server/src/bootstrap.rs`
- `examples/runtime-server.toml`
- `deploy/systemd/gg-runtime.env.example`
- `deploy/systemd/gg-runtime.service.example`

#### What to do

- Implement `GoosetowerConfig` with:
  - server bind address
  - public base URL
  - allowed Gooseweb origins
  - ticket issuer/audience settings
  - ticket signing/verification settings
  - runtime source registry
  - WebSocket message limits
  - heartbeat interval
  - replay limits
  - materializer buffer sizes
  - lane queue sizes
  - debug endpoint enable flag
- Implement a static config-file runtime registry for V0:
  - `source_id`
  - `source_kind = "gooselake-runtime"`
  - `base_url`
  - bearer token or token file
  - enabled flag
  - display name
  - workspace ID
- Add CLI flags:
  - `--config <path>`
  - `--check-config`
- Add health endpoints:
  - unauthenticated or minimally authenticated `/health`
  - authenticated `/v1/health`
  - authenticated `/v1/sources`
- Add runtime source health checks against Gooselake `/v1/health` and `/v1/version`.
- Do not proxy all runtime APIs generically. Route only intentional gateway commands in later phases.

#### Validation strategy

- Unit-test config defaults, token-file resolution, origin allowlist parsing, and runtime registry parsing.
- Integration-test `/health` and `/v1/sources` with a mock runtime HTTP server.
- `cargo check -p goosetower`
- `cargo test -p goosetower config`

#### Risks / fallbacks

- Risk: auth material handling gets mixed with runtime-server bearer auth.
- Fallback: keep Goosetower browser auth/tickets separate from upstream runtime bearer tokens.
- Risk: V0 overdesigns dynamic registry storage.
- Fallback: use static config first; add durable registry later.

### Phase 3: Runtime HTTP Client And SSE Fan-In For One Source

#### Files to read before starting

- `crates/runtime-server/src/http/mod.rs`
- `crates/runtime-server/src/http/events.rs`
- `crates/runtime-server/src/http/sessions.rs`
- `crates/runtime-server/src/http/teams.rs`
- `crates/runtime-server/src/http/processes.rs`
- `crates/runtime-server/src/http/worktrees.rs`
- `crates/runtime-server/src/http/diagnostics.rs`
- `crates/runtime-core/src/state.rs`
- `crates/runtime-core/src/services.rs`
- `crates/runtime-store-sqlite/src/schema.rs`

#### What to do

- Implement a typed Gooselake runtime client in Goosetower for:
  - health/version/providers/provider auth status
  - sessions list/get/create/resume/close
  - send turn
  - interrupt turn
  - respond approval
  - teams list/get/view/create/join/spawn/remove/set lead/delete
  - send direct/broadcast team messages
  - list/retry/cancel deliveries
  - team interrupt all
  - processes list/get/logs/start/kill
  - worktrees list/get/create/claim/release/cleanup
  - diagnostics needed by Fleet/settings views
  - global events replay and SSE stream
  - session/team/process scoped replay if needed for targeted catch-up
- Implement upstream SSE ingest for one runtime source.
- Convert upstream `RuntimeEventRecord` into internal Goosetower source events:
  - `source_id`
  - `source_epoch`
  - `source_seq`
  - upstream `row_id`
  - upstream scoped `seq`
  - scope metadata
  - kind
  - criticality/lane
  - payload
- For V0, set `source_seq` to upstream global `row_id` for global stream fan-in.
- Preserve upstream scoped `seq` in the payload or metadata for scoped replay/debug.
- Add reconnect to upstream SSE with `Last-Event-ID`/`after_seq` based on last consumed source global row ID.
- Record source health:
  - live
  - replaying
  - stale
  - offline
  - gap_detected
- Do not yet expose browser WebSocket in this phase.

#### Validation strategy

- Mock runtime server tests:
  - client adds bearer token correctly
  - client decodes runtime records
  - client paginates replay with `after_seq`
  - SSE reconnect resumes after last source cursor
  - duplicate replay/live events are deduped
- Unit-test lane mapping from runtime event criticality/kind.
- `cargo test -p goosetower runtime`

#### Risks / fallbacks

- Risk: upstream global events stream currently polls the store every 250ms rather than using broadcast fanout.
- Fallback: V0 accepts this because provider/model latency will dominate; optimize runtime global event broadcast later only if measured.
- Risk: `seq` means scoped sequence for some endpoints and `row_id` means global cursor for global replay.
- Fallback: make cursor naming explicit in Goosetower and never conflate upstream scoped `seq` with global `row_id`.

### Phase 4: Materialized Read Models And Subscription Snapshots

#### Files to read before starting

- `crates/runtime-core/src/state.rs`
- `crates/runtime-core/src/services.rs`
- `crates/runtime-server/src/http/sessions.rs`
- `crates/runtime-server/src/http/teams.rs`
- `crates/runtime-server/src/http/processes.rs`
- `crates/runtime-server/src/http/worktrees.rs`
- `crates/runtime-server/src/http/diagnostics.rs`
- `crates/runtime-core/src/team_comms/service_impl.rs`
- `crates/runtime-core/src/team_comms/delivery.rs`

#### What to do

- Implement in-memory materialized state in Goosetower for one source.
- Bootstrap materialized state from runtime HTTP APIs:
  - sessions
  - teams and members
  - team view snapshots for selected/default teams
  - active/recent processes
  - worktrees
  - provider/auth status
  - diagnostics summary
  - global event cursor
- Reduce incoming source events into read models:
  - `FleetBoardView`
  - `AgentRowView`
  - `ApprovalInboxView`
  - `SessionDetailView`
  - `TeamWorkspaceView`
  - `ProcessTailView`
  - `LedgerView`
  - `SourceHealthView`
  - `WorktreeView`
- Define entity versions in the gateway materializer even if upstream records do not yet expose explicit versions.
- Build subscription snapshot functions:
  - board window/filter snapshot
  - approval inbox snapshot
  - selected session snapshot
  - selected team snapshot
  - process tail snapshot
  - ledger page snapshot
  - fleet/source health snapshot
- Build patch generation:
  - entity upsert
  - entity remove
  - list insert/remove/move
  - text/token append
  - log append/sample
  - source health/stale transition
- Keep raw upstream event payloads available for Ledger/debug but do not require the browser to derive the Board from raw events.

#### Validation strategy

- Reducer unit tests for each core upstream event kind currently emitted by runtime code.
- Snapshot tests for board, approval inbox, selected session, team view, and process tail materialization from seeded records.
- Tests for dedupe by source cursor.
- Tests for coalescing low-priority repeated row updates without losing terminal transitions.
- `cargo test -p goosetower materializer`

#### Risks / fallbacks

- Risk: some important UI fields are not present in runtime events.
- Fallback: materializer refreshes affected entities over HTTP after event hints. Later runtime events can be enriched without changing the browser protocol.
- Risk: materialized state grows unbounded.
- Fallback: use bounded ring buffers for ledger/log/tokens and demand-driven snapshots for deep history.

### Phase 5: Ticket Auth, WebSocket Gateway, Lanes, And Command Routing

#### Files to read before starting

- `crates/runtime-server/src/http/auth.rs`
- `crates/runtime-server/src/http/shared.rs`
- `crates/runtime-server/src/http/sessions.rs`
- `crates/runtime-server/src/http/teams.rs`
- `crates/runtime-server/src/http/processes.rs`
- `crates/runtime-server/src/http/worktrees.rs`
- `crates/runtime-core/src/services.rs`
- `crates/runtime-core/src/runtime/turns.rs`
- `crates/runtime-core/src/team_comms/service_impl.rs`

#### What to do

- Implement Goosetower ticket validation:
  - short expiry
  - audience
  - issuer
  - subject
  - workspace
  - scopes
  - allowed origin
  - nonce/JTI one-time use
- Implement exact `Origin` allowlist checks for WebSocket upgrade.
- Add a ticket-mint endpoint only if this repo is responsible for Gooseweb server auth in V0. Otherwise define the endpoint contract and use a dev-only static ticket issuer for local development.
- Add `/v1/realtime` WebSocket endpoint.
- Implement binary Protobuf frame decode/encode.
- Implement `Hello`, heartbeat `Ping/Pong`, max message size enforcement, auth timeout, and close codes.
- Implement per-connection state:
  - user/workspace/scopes
  - connection ID
  - subscriptions
  - cursor vector
  - last acked gateway seq
  - pending outbound queues per lane
  - backpressure counters
- Implement subscriptions:
  - subscribe/unsubscribe
  - snapshot-on-subscribe
  - patch fanout based on interest
  - viewport/window update
- Implement priority lane scheduler over one WebSocket.
- Implement command admission and routing with idempotency:
  - send turn
  - resolve approval
  - interrupt turn
  - direct team message
  - broadcast team message
  - spawn team member
  - retry delivery
  - cancel delivery
  - kill process
  - start process only if V0 UI needs it
- Implement command responses:
  - accepted
  - rejected
  - duplicate
- Include `command_id`, target, optional `base_entity_version`, and created-at client time on every command.
- Persist pending command IDs in memory for V0 with TTL. Do not add a durable Goosetower DB until needed.
- Emit gateway audit events for connection open/close, subscribe changes, auth refresh, command accepted/rejected/duplicate, source gap, and snapshot resync.

#### Validation strategy

- WebSocket integration tests:
  - rejects missing/expired/replayed tickets
  - rejects invalid origin
  - accepts valid ticket
  - sends hello
  - heartbeat closes dead connections
  - enforces max message size
  - subscribe returns snapshot
  - materializer patch reaches matching subscription only
  - command accepted maps to upstream runtime HTTP call
  - duplicate command returns original result
  - rejected command includes machine-readable reason
- Queue/lane tests:
  - critical messages are never dropped
  - state messages coalesce by entity
  - token/log messages coalesce or degrade under backpressure
- `cargo test -p goosetower gateway`

#### Risks / fallbacks

- Risk: browser WebSocket auth cannot use arbitrary Authorization headers.
- Fallback: use single-use URL ticket plus Origin validation and in-band refresh.
- Risk: command accepted is confused with command completed.
- Fallback: encode command lifecycle explicitly and make UI states distinguish accepted, forwarded, runtime-acknowledged, and terminal.
- Risk: in-memory command ID TTL loses duplicate detection on gateway restart.
- Fallback: acceptable for V0; add durable pending command storage when production reliability requires it.

### Phase 6: Reconnect, Replay, Gap Detection, And Staleness UX Signals

#### Files to read before starting

- `crates/runtime-server/src/http/events.rs`
- `crates/runtime-server/src/http/teams.rs`
- `crates/runtime-server/src/http/processes.rs`
- `crates/runtime-core/src/runtime/events.rs`
- `crates/runtime-store-sqlite/src/repository.rs`
- `crates/runtime-store-sqlite/src/store.rs`

#### What to do

- Implement browser `Resume` handling:
  - previous connection ID
  - last gateway seq
  - source cursor vector
  - active subscriptions
- Implement gateway replay from in-memory ring buffers when possible.
- Implement source replay from Gooselake global `/v1/events?after_seq=...` when gateway buffers are insufficient.
- Implement gap detection:
  - source epoch changed
  - source sequence jumps
  - source replay cannot fill requested range
  - upstream source unavailable past stale threshold
- Implement gap events:
  - `SourceGapDetected`
  - `SourceGapFilled`
  - `SourceSnapshotResync`
- Implement snapshot resync:
  - refresh affected materialized views from runtime HTTP
  - mark Ledger/session/team views with discontinuity metadata
  - keep source-owned destructive commands disabled while correctness is unknown
- Implement connection states:
  - connected
  - degraded
  - reconnecting
  - replaying
  - stale
  - offline
- Expose metrics counters internally even if final metrics export is later:
  - resume success
  - resume partial
  - resume rejected
  - replay events/bytes
  - replay time-to-live-tail
  - source stale age
  - gap count
  - snapshot resync count

#### Validation strategy

- Integration tests with fake runtime event streams:
  - clean reconnect resumes without duplicates
  - replay fills missing events
  - duplicate replay/live overlap dedupes
  - source gap pauses affected state
  - exhausted replay triggers snapshot resync
  - stale source disables risky command responses
- Browser Worker tests later must mirror these cases with fixture frames.
- `cargo test -p goosetower resume`

#### Risks / fallbacks

- Risk: Gooselake does not expose a compact whole-runtime snapshot endpoint.
- Fallback: compose snapshot from current list/get APIs for sessions, teams, processes, worktrees, providers, and diagnostics.
- Risk: token/log replay volume is too high.
- Fallback: replay critical/state first; use canonical turn/process state plus log range endpoints for bulk reconstruction.

### Phase 7: Gooseweb TanStack Start App Shell And Realtime Worker

#### Files to read before starting

- `package.json`
- `tsconfig.json`
- `bun.lock`
- `proto/goosetower/v1/common.proto`
- `proto/goosetower/v1/view.proto`
- `proto/goosetower/v1/commands.proto`
- `proto/goosetower/v1/realtime.proto`

#### What to do

- Use `webctx` to check current TanStack Start and Buf/Protobuf-ES setup before scaffolding if APIs are uncertain.
- Add `apps/gooseweb` as a separate TanStack Start app.
- Keep the existing Astro docs site intact.
- Initialize `shadcn/ui` inside `apps/gooseweb` using the Bun runner and current CLI guidance. Commit `components.json`, theme/global CSS wiring, aliases, and any required utility setup as part of the app foundation.
- Add package scripts for Gooseweb:
  - dev
  - build
  - typecheck
  - test if a test runner is introduced
  - proto generation
- Add TypeScript Protobuf generation from `proto/goosetower/v1`.
- Add the initial `shadcn/ui` dependency and component baseline needed for the app shell in later phases. At minimum, prepare the project to add components by CLI without manual path surgery.
- Implement `app/realtime/worker`:
  - WebSocket lifecycle
  - ticket input from main thread
  - binary Protobuf decode/encode
  - hello/heartbeat
  - resume and cursor persistence
  - subscribe/unsubscribe
  - command send with idempotency
  - dedupe by gateway and source cursor
  - rAF or timed patch coalescing handoff
  - lane-aware handling
  - degraded/stale state propagation
- Implement main-thread external store:
  - TanStack Store or `useSyncExternalStore`
  - narrow selectors
  - normalized entities by ID
  - visible subscription state
  - pending command state
- Add local development config:
  - Goosetower URL
  - dev ticket route or pasted dev ticket
  - feature flags
- Do not put raw durable tokens in browser local storage.
- Prefer `shadcn/ui` primitives for any shell scaffolding introduced in this phase instead of custom buttons, cards, dialogs, tabs, alerts, or sidebars.

#### Validation strategy

- `bun run typecheck` in `apps/gooseweb`.
- Unit tests for Worker reducer and cursor handling if test tooling is introduced.
- Fixture decode tests using binary frames generated by Rust tests or checked-in protocol fixtures.
- Manual local run:
  - Start Gooselake runtime.
  - Start Goosetower.
  - Start Gooseweb.
  - Verify browser connects, receives hello, subscribes, and renders initial snapshots.

#### Risks / fallbacks

- Risk: TanStack Store alpha APIs shift.
- Fallback: use `useSyncExternalStore` around a small hand-written external store and keep TanStack Store optional.
- Risk: Protobuf TS generation output style changes.
- Fallback: pin the generation package versions and document the command in `apps/gooseweb/package.json`.
- Risk: `shadcn/ui` CLI or generated config shape shifts.
- Fallback: verify current CLI/init flow with `webctx`, keep `components.json` and aliases explicit, and prefer CLI-managed component additions over hand-copied snippets.

### Phase 8: Gooseweb V0 Desktop-Class Operating Experience

#### Files to read before starting

- `apps/gooseweb/app/realtime/*`
- `apps/gooseweb/app/stores/*`
- `proto/goosetower/v1/view.proto`
- `proto/goosetower/v1/commands.proto`
- `crates/runtime-core/src/state.rs`
- `crates/runtime-core/src/services.rs`

#### What to do

- Build the primary shell as an operating workspace, not a marketing page.
- Standardize the Gooseweb surface on `shadcn/ui` primitives and composition. Use CLI-added source components for navigation, cards, tables, forms, overlays, separators, alerts, badges, tabs, sheets, drawers, and menus before introducing custom wrappers.
- Use a dense desktop layout:
  - left rail for Board, Inbox, Teams, Agents, Ledger, Fleet, Playbooks, Settings
  - entity list/sidebar
  - main detail workspace
  - right inspector/context panel where useful
  - persistent connection/source status
- Board:
  - virtualized rows
  - source/team/session filters
  - status, active turn, provider/model, pending approvals, process indicator, worktree, latest activity
  - selected row drives subscriptions
  - prefer `shadcn/ui` table, badge, input, select, tooltip, scroll-area, and sidebar/navigation primitives for row chrome and filters
- Agent workspace:
  - session selector
  - timeline
  - streaming current response
  - turn composer
  - interrupt action
  - approvals in context
  - process/worktree/provider/context indicators
  - use `shadcn/ui` cards, tabs, field/input patterns, dialogs/sheets, badges, alerts, and separators
- Team workspace:
  - team selector
  - member roster
  - lead controls
  - direct/broadcast composer
  - spawn teammate modal
  - delivery state list
  - retry/cancel controls
  - team event timeline
  - use `shadcn/ui` roster/table, dropdown menu, dialog, textarea/input, and status primitives
- Inbox:
  - global pending approvals
  - approve/reject controls
  - stale/source-gap warning
  - inline rejection feedback
  - prefer `shadcn/ui` alert, badge, button, dialog/alert-dialog, and empty-state patterns
- Ledger:
  - virtualized event feed
  - filters
  - cursor/replay markers
  - gateway audit events
  - use `shadcn/ui` scroll-area, table/list, badges, inputs/selects, and separators
- Fleet:
  - one runtime source in V0
  - health, stale age, replay lag, active sessions, provider auth statuses, process capacity
  - placeholders for future runtime add/provision actions
- Playbooks:
  - minimal command/message templates
  - no full automation engine in V0
- Settings:
  - gateway URL
  - protocol version
  - current user/workspace
  - feature flags
  - debug export
- Rendering requirements:
  - no React state update per token packet
  - use virtualization for logs, timelines, feeds, and ledger
  - keep approval controls outside heavy streaming subtrees
  - frame-batch visible token/log updates
  - show honest connected/degraded/reconnecting/replaying/stale/offline states
- Add any missing `shadcn/ui` components through the CLI as needed during this phase instead of copying registry markup manually.

#### Validation strategy

- `bun run typecheck` in `apps/gooseweb`.
- Browser smoke test with a real local Goosetower:
  - create/select a session
  - send a turn
  - interrupt a turn
  - resolve an approval if a provider/test provider can produce one
  - create/select a team
  - send direct and broadcast messages
  - spawn teammate
  - retry/cancel delivery
  - view process list/log tail
  - view worktree state
  - disconnect/reconnect and verify replay state
- Use Playwright or the available browser automation stack for screenshots and interaction verification once the app exists.
- Validate narrow subscriptions by watching Goosetower logs or test counters while scrolling/filtering the Board.

#### Risks / fallbacks

- Risk: provider availability makes approval/turn workflows hard to test deterministically.
- Fallback: add a Goosetower/browser dev fixture mode backed by a fake runtime server, not by fake UI state.
- Risk: desktop scope sprawls.
- Fallback: complete Board, Agent workspace, Team workspace, Inbox, and connection state before Ledger/Fleet/Playbooks polish.
- Risk: overly customized one-off UI drifts away from the shared component system.
- Fallback: keep `shadcn/ui` components as the visual and structural baseline, adding thin local wrappers only where Gooseweb needs domain-specific composition.

### Phase 9: End-To-End Command, Approval, Team, Process, And Worktree Hardening

#### Files to read before starting

- `crates/runtime-core/src/runtime/turns.rs`
- `crates/runtime-core/src/runtime/sessions.rs`
- `crates/runtime-core/src/team_comms/service_impl.rs`
- `crates/runtime-core/src/team_comms/delivery.rs`
- `crates/runtime-tools/src/process.rs`
- `crates/runtime-tools/src/worktree/service.rs`
- `crates/runtime-server/src/http/tests/*`
- `crates/runtime-tools/src/tests/*`
- `crates/runtime-core/src/team_comms/tests.rs`

#### What to do

- Audit V0 command mappings against runtime behavior:
  - command accepted vs runtime terminal state
  - approval resolved vs button pending state
  - team delivery queued/deferred/injected/failed/cancelled
  - process kill requested vs process exited
  - teammate spawn with worktree creation/reuse
  - worktree cleanup/release effects
- Add Goosetower integration tests that use existing runtime test support patterns where possible.
- Add Gooseweb UX for command rejection:
  - stay on object
  - revert pending marker only
  - show inline machine-readable reason mapped to human-readable copy
  - refresh stale entity where appropriate
- Add explicit command result reasons:
  - unauthorized
  - invalid_scope
  - invalid_target
  - stale_entity_version
  - source_unavailable
  - source_stale
  - source_gap
  - upstream_rejected
  - upstream_timeout
  - duplicate
- Add per-command telemetry fields to gateway audit.

#### Validation strategy

- Run targeted Rust tests:
  - `cargo test -p goosetower`
  - `cargo test -p runtime-core team_comms`
  - `cargo test -p runtime-tools`
  - `cargo test -p runtime-server http`
- Run Gooseweb typecheck.
- Manual E2E pass over the critical desktop workflows.

#### Risks / fallbacks

- Risk: runtime APIs lack idempotency for some command classes.
- Fallback: Goosetower provides best-effort idempotency at admission and returns duplicate results within TTL; add runtime-native idempotency only where duplicate side effects are observed.
- Risk: base entity versions are gateway-only.
- Fallback: use gateway materializer versions for optimistic concurrency in V0 and refresh from source on mismatch.

### Phase 10: Observability, Operations, And Deployment

#### Files to read before starting

- `Makefile`
- `scripts/install-runtime.sh`
- `scripts/install-from-source.sh`
- `scripts/upgrade-runtime.sh`
- `scripts/preflight-runtime.sh`
- `scripts/deploy-vps.sh`
- `deploy/systemd/gg-runtime.service.example`
- `deploy/systemd/gg-runtime.env.example`
- `crates/runtime-server/src/main.rs`
- `crates/runtime-server/src/config.rs`

#### What to do

- Add Goosetower operational config and examples:
  - config example
  - env example
  - systemd service example
  - local development example
- Add Makefile targets:
  - `goosetower-check-config`
  - `goosetower-preflight`
  - `gooseweb-dev`
  - `gooseweb-build`
  - `gooseweb-typecheck`
  - include `cargo check -p goosetower` and `cargo test -p goosetower` in broad checks once stable
- Add logs/metrics:
  - connection open/close count
  - active connections
  - source health
  - browser RTT
  - command accepted/rejected latency
  - upstream command latency
  - event ingest lag
  - materializer reduce time
  - outbound messages by lane
  - coalesced/dropped bulk messages
  - WebSocket buffered/backpressure state
  - resume success/gap/snapshot resync
- Add debug endpoints gated by auth/config:
  - decoded protocol version
  - active sources
  - active subscriptions
  - materializer summary
  - recent gateway audit
- Add deployment guidance:
  - Gooseweb on Vercel serves static/server app only.
  - Browser connects directly to Goosetower.
  - Goosetower runs on the VPS near Gooselake runtime.
  - Goosetower upstream runtime token is server-side only.
  - Configure TLS/proxy for WebSocket upgrade.
  - Use exact Origin allowlist for production and preview origins.

#### Validation strategy

- `cargo fmt --check`
- `cargo check --workspace`
- `cargo test --workspace`
- `cargo check --manifest-path sidecars/gg-mcp-server/Cargo.toml`
- `cargo test --manifest-path sidecars/gg-mcp-server/Cargo.toml`
- Gooseweb typecheck/build.
- Goosetower config check.
- Manual preflight against local Gooselake + Goosetower + Gooseweb.
- After broad Rust changes, run `make check` before push.

#### Risks / fallbacks

- Risk: broad `make check` becomes slow during early iteration.
- Fallback: run targeted checks per phase and reserve full `make check` for integration points and before push.
- Risk: WebSocket reverse proxy config breaks upgrades.
- Fallback: add preflight checks that verify 101 upgrade and heartbeat roundtrip.

### Phase 11: Multi-Runtime Readiness

#### Files to read before starting

- `crates/goosetower/src/runtime/registry.rs`
- `crates/goosetower/src/runtime/sse.rs`
- `crates/goosetower/src/materializer/*`
- `crates/goosetower/src/gateway/resume.rs`
- `crates/goosetower/src/gateway/commands.rs`
- `proto/goosetower/v1/common.proto`
- `proto/goosetower/v1/realtime.proto`

#### What to do

- Promote source identity from V0 single-source defaults into mandatory runtime registry data.
- Add multiple source connections in Goosetower:
  - independent health checks
  - independent SSE fan-in tasks
  - independent replay cursors
  - independent stale/gap state
- Persist source cursors in a small Goosetower store if restart-resume becomes required.
- Add source ownership indexes:
  - session owner
  - team owner
  - process owner
  - worktree owner
  - delivery owner
- Add cross-source materialized Board/Fleet views.
- Add source filtering in Gooseweb.
- Add source-specific stale/gap UI.
- Add command routing by owner source.
- Reject cross-source commands that need unsupported coordination.
- Do not implement horizontal gateway scaling until single gateway multi-source behavior is correct.

#### Validation strategy

- Fake two runtime sources with interleaved events.
- Test per-source order preservation.
- Test gateway sequence assignment.
- Test source-specific gap detection.
- Test command routes to the owning source.
- Test source stale state disables only affected commands/views.
- Test Board can filter and aggregate across two sources.

#### Risks / fallbacks

- Risk: team objects spanning runtimes require coordination not present in Gooselake.
- Fallback: keep each team owned by exactly one runtime source until a source coordinator exists.
- Risk: source identity is not present in upstream runtime events.
- Fallback: Goosetower assigns source identity at ingest from registry context.

### Phase 12: RunPod And Fleet Provisioning Hooks

#### Files to read before starting

- `crates/goosetower/src/runtime/registry.rs`
- `crates/goosetower/src/config.rs`
- `crates/goosetower/src/materializer/state.rs`
- `crates/goosetower/src/gateway/commands.rs`
- `apps/gooseweb/app/features/fleet/*`
- Any new provisioning docs/specs provided by the lead at that time

#### What to do

- Add runtime source lifecycle states:
  - configured
  - provisioning
  - booting
  - live
  - draining
  - stale
  - offline
  - failed
  - terminated
- Add provider/capability metadata:
  - provider kinds
  - models
  - process capacity
  - worktree capability
  - team capability
  - replay window
  - region
  - cost hints if available
- Add provisioning abstraction but keep implementation behind a trait:
  - static source provider
  - future RunPod source provider
- Add Fleet UI controls behind feature flags:
  - view source capacity
  - provision source
  - drain source
  - terminate source
  - inspect source logs/health
- Keep session/team/process ownership explicit. Do not auto-migrate live sessions between runtimes in this phase.

#### Validation strategy

- Unit-test source lifecycle state machine.
- Mock provisioning provider tests.
- Fleet UI tests with fake source states.
- E2E test that a newly provisioned fake source appears, becomes live, accepts a new session, and then drains.

#### Risks / fallbacks

- Risk: RunPod operational details are not available yet.
- Fallback: finish the provider trait and static/mock provider first; implement RunPod adapter after concrete credentials, image, networking, and runtime bootstrap requirements are known.
- Risk: browser UI implies unsupported migration or autoscaling.
- Fallback: label V0 Fleet actions as explicit admin operations and avoid automatic placement until policy exists.

### Phase 13: Mission-Control UI Redesign To Match Desktop Reference

#### Files to read before starting

- `apps/gooseweb/src/routes/index.tsx`
- `apps/gooseweb/src/routes/__root.tsx`
- `apps/gooseweb/src/styles/app.css`
- `apps/gooseweb/components.json`
- `apps/gooseweb/components/ui/*`
- `apps/gooseweb/app/stores/gooseweb-store.ts`
- `apps/gooseweb/app/realtime/*`
- the design reference image provided by the lead/user for this phase

#### What to do

- Redesign the Gooseweb operator surface to visually match the provided desktop reference as closely as practical without regressing existing realtime behavior.
- Keep the app `shadcn/ui`-based, but shift the composition, spacing, hierarchy, and chrome toward the reference:
  - full-height dark mission-control shell
  - left roster rail for agents/teams/projects with stacked cards and subtle active states
  - dominant center conversation/worklog pane with rounded container, muted header bar, dense prose blocks, and inline tool/result cards
  - right-side processes rail with compact segmented filters, running/completed states, process metadata, and kill/action affordances
  - persistent top-window/app chrome that reads like a desktop control surface rather than a browser dashboard
- Match the visual language of the reference:
  - near-black background with low-contrast layered panels
  - restrained grayscale palette with sparse accent color
  - larger rounded corners, inset surfaces, and soft separators instead of high-contrast borders
  - denser typography hierarchy tuned for long-form operational text
  - card treatments that feel like terminal/agent artifacts rather than generic SaaS widgets
- Rework the Gooseweb information architecture where needed so the main screen prioritizes:
  - active investigation or session narrative in the center
  - team/agent switching on the left
  - process operations and server state on the right
  - composer/action tray anchored at the bottom of the main pane
- Preserve or improve the existing V0 functionality from earlier phases:
  - realtime subscriptions
  - command actions
  - connection status
  - approval/inbox handling
  - fleet/process visibility
  - virtualization and frame-batched streaming behavior
- Introduce any additional `shadcn/ui` components through the CLI if the redesign needs them, rather than freehand recreating common primitives.
- Ensure the redesign still works on narrower laptop widths and has a coherent fallback layout for smaller screens, even though desktop is the priority target.

#### Validation strategy

- `bun run typecheck` in `apps/gooseweb`
- `bun run build` in `apps/gooseweb`
- Manual browser review against the reference image:
  - left roster rail hierarchy
  - center investigation/worklog pane
  - right processes rail
  - bottom composer/action bar
  - connection/process controls
- Capture before/after screenshots or a concise visual diff summary for review.

#### Risks / fallbacks

- Risk: redesign work regresses established command/realtime interactions while chasing visual parity.
- Fallback: treat behavior as non-negotiable, and isolate the redesign to layout, composition, styling, and presentation-layer state.
- Risk: copying the reference too literally creates mismatches with Gooseweb domain needs.
- Fallback: preserve the reference's structural cues and aesthetic language while adapting labels, panes, and controls to Gooseweb semantics.
- Risk: `shadcn/ui` defaults look too generic compared with the reference.
- Fallback: keep `shadcn/ui` as the primitive layer, but use custom layout composition, tokens, and panel treatments to achieve the darker mission-control feel.

## Cross-Runtime Requirements Summary

- Add second-source readiness before adding actual RunPod.
- Use `source_id`, `source_epoch`, and `source_seq` in all browser protocol messages from the beginning.
- Maintain a cursor vector, not a single source cursor.
- Keep per-source correctness order strict.
- Keep `gateway_seq` as display/merge order only.
- Make source ownership explicit for command routing.
- Treat unknown ownership as rejection, not best-effort broadcast.
- Represent source stale/gap state in both Goosetower protocol and Gooseweb UI.
- Build source snapshot resync before relying on multi-runtime operations.
- Defer cross-runtime team coordination until there is a real coordinator design.

## Auth And Ticket Flow Summary

- Gooseweb authenticates the user through the selected app auth provider.
- Gooseweb obtains a short-lived Goosetower ticket from a server route or auth service.
- Browser sends the ticket in the WebSocket URL because browser WebSocket APIs cannot set arbitrary Authorization headers.
- Goosetower validates ticket, exact Origin, expiry, audience, scopes, workspace, and one-time nonce.
- Goosetower never logs raw tickets or durable user credentials.
- Goosetower validates authorization on every sensitive command.
- Goosetower supports in-band auth refresh before ticket/session expiry.
- REST endpoints used by Gooseweb should use normal `Authorization` headers where browser fetch allows it.

## Wire Protocol Scope Summary

- V0 transport: WebSocket.
- V0 encoding: binary Protobuf.
- WebTransport: not in V0; revisit only after metrics show WebSocket limitations.
- SSE: keep for runtime fan-in, diagnostics, and degraded read-only fallback, not as the primary browser transport.
- Protocol is envelope-based, versioned, and generated for Rust and TypeScript.
- Browser receives materialized snapshots and patches, not only raw ledger events.
- Critical/state/token/bulk lanes are logical lanes over the same WebSocket.
- Commands are idempotent by `command_id`.
- Reconnect uses explicit resume with gateway and source cursors.

## Gooseweb UI Scope Summary

- The first screen should be the operating workspace.
- Board, Agent workspace, Team workspace, and Inbox are V0-critical.
- Ledger and Fleet should exist in useful minimal form for operations/debugging.
- Playbooks should start as templates, not a full automation engine.
- Settings/admin should expose connection, protocol, source, and debug state.
- Logs, ledgers, feeds, and timelines must be virtualized.
- Streaming token/log rendering must be frame-batched outside React packet rate.
- Connection state must be honest and visible.
- Stale/gapped sources must affect destructive command availability.

## Final Recommendation

Implement V0 as a separate `gg-goosetower` Rust service plus a separate `apps/gooseweb` TanStack Start app, with `shadcn/ui` as the default component system, Protobuf-over-WebSocket as the only primary browser protocol, and a TypeScript Web Worker as the client realtime core. Keep Gooselake as the only source of truth, use Goosetower for materialized realtime views and command routing, and finish the single-runtime path completely before enabling multi-runtime or RunPod behavior.
