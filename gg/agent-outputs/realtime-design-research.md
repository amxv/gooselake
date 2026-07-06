# Mission Control Realtime Design Research

Research date: 2026-07-06

Context: Mission Control is a browser app served from Vercel/TanStack Start. The browser connects directly to a Rust gateway on the VPS. The gateway sits in front of Gooselake runtime instances, which already expose HTTP/SSE, replayable sequence-numbered events, and primitives for sessions, turns, approvals, teams, deliveries, and processes.

## Executive Recommendations

1. Build Mission Control around a server-side materialized state gateway, not raw event fanout. The browser should render pre-shaped board/session/approval/process view models plus deltas. The durable event ledger remains the source of truth, but the realtime transport should ship UI-relevant patches.

2. Use one direct browser-to-VPS WebSocket for interactive Mission Control, backed by the existing HTTP/SSE replay semantics. Keep HTTP endpoints for bootstrap, audit/history, and fallback. Use WebSocket because commands and updates need one low-latency bidirectional control plane; keep SSE-compatible event IDs/cursors so replay is not transport-specific.

3. Split outbound traffic into priority lanes: critical command acks/rejections and approvals; state transitions; token deltas; logs/bulk output. Critical events must never be coalesced or dropped. Token/log streams should be coalesced per animation frame or under explicit byte/time budgets.

4. Add viewport/entity-based subscriptions. A fleet board should not subscribe to every token and process byte from every agent. The client should subscribe to a board query, visible rows, pinned sessions, selected session timelines, approval inbox, and explicit process log tails.

5. Use optimistic UI only for local intent, not for runtime truth. "Approve", "interrupt", and "send message" should show immediate local pending state keyed by a client command ID, then reconcile against server-accepted/rejected events. Never visually mark an approval resolved until the gateway sequences the acceptance.

6. Make fan-in cursors explicit. When the gateway aggregates multiple runtimes, every event needs both a gateway sequence and a per-source cursor. The gateway can define display order, but source order is the correctness boundary. Gap detection must be per source.

7. Authenticate direct browser-to-VPS connections with short-lived, single-use connection tickets plus Origin allowlisting. Browser WebSockets cannot set arbitrary Authorization headers. Do not put durable user tokens in the WebSocket URL. Use an ephemeral ticket in the URL for handshake rejection, then in-band refresh on the live socket.

8. Make connection state honest in the UI. Show connected, degraded, reconnecting, stale, and replaying states. Show stale age per board/source/session when possible. If a replay window is missed, say that a snapshot refresh occurred instead of pretending the stream was continuous.

## What Best-in-Class Systems Actually Do

### Figma: server-authoritative collaboration with a journal

Figma uses browser clients talking to servers over WebSockets. Its multiplayer service is authoritative: it validates, orders, resolves conflicts, and keeps hot document state in memory. The original protocol is deliberately simpler than full OT because Figma documents are object/property trees, not text documents. Property updates behave like server-ordered last-writer-wins registers, and the client applies local edits immediately while suppressing older conflicting server echoes to avoid flicker. Source: https://www.figma.com/blog/how-figmas-multiplayer-technology-works/

Figma later added a write-ahead journal. Notable numbers and mechanics:

- Checkpoints were every 30-60 seconds; journal entries reduced the failure data-loss target to less than 1 second.
- Clients send updates every 33ms, but journal writes are batched because the durable store does not need frame-level granularity.
- Each change gets a file sequence number; checkpoints also carry sequence numbers so recovery loads a checkpoint and replays newer journal entries.
- Figma reported more than 2.2B received changes per day and 95% of changes persisted within about 600ms.

Source: https://www.figma.com/blog/making-multiplayer-more-reliable/

Transfer to Mission Control:

- Use server authority for all runtime state: sessions, turns, approvals, deliveries, and process state.
- Keep hot materialized state in memory at the gateway for fast board deltas, but append durable events first or as part of the same accepted mutation path.
- Coalesce only where the domain permits it. Token deltas and stdout samples can be batched; approval requests, approval decisions, turn terminal states, team delivery state, and process exits cannot be silently coalesced away.
- A single in-memory owner per entity/source reduces split-brain risk. If the gateway is horizontally scaled later, use ownership/lease semantics per runtime/source/shard before allowing multiple gateways to write the same materialized state.

### Linear: local replicated read model plus deltas

Linear is known for a fast app feel. Public material confirms the sync-engine direction, and reverse-engineering shows a model where the client bootstraps a large local data set, stores it in IndexedDB, and applies `SyncAction` deltas with monotonically increasing IDs. The first bootstrap fetches core models needed for immediate render, while heavier models such as comments/history can be fetched partially after. Source: https://linear.app/now/scaling-the-linear-sync-engine and https://marknotfound.com/posts/reverse-engineering-linears-sync-magic

Transfer to Mission Control:

- Do not render the fleet board by replaying every historical event in React. Bootstrap a compact read model.
- Split bootstrap by product urgency:
  - First paint: active sessions, active turns, pending approvals, team membership, process status, recent activity.
  - Deferred: full transcripts, full process logs, old team deliveries, historical audit trails.
- Store the client-side materialized board state in an external store or IndexedDB-backed cache if the dataset grows. The browser should be able to restart and quickly resume from a known cursor, but the server remains authoritative.

### Discord Gateway: sequence numbers, resume, intents, heartbeats

Discord's Gateway is a persistent WebSocket protocol with opcodes, heartbeats, sequence numbers, and resume. Dispatch events include an `s` sequence number that clients must cache. On reconnect, the client sends `session_id` and last sequence; the Gateway replays missed events if the session is resumable. Discord also uses intents to reduce event volume and documents payload/rate limits, including 4096-byte outbound event payload limits and 120 client-sent gateway events per connection per 60 seconds. Source: https://docs.discord.com/developers/events/gateway

Transfer to Mission Control:

- Add explicit `hello`, `subscribe`, `resume`, `ack`, `ping`, `pong`, `command`, and `command_result` message classes instead of treating the socket as arbitrary JSON.
- The server should send sequence-bearing events and require the client to resume with the latest received cursor.
- Subscriptions should be declared as "intents" or query scopes, for example `approvals`, `board:{team_id}`, `session:{id}:summary`, `session:{id}:tokens`, `process:{id}:tail`.
- Add jittered reconnect and heartbeat behavior. A half-open WebSocket is common enough to design for, not an edge case.

### Google Docs and OT: useful mainly as a warning

Google Docs historically used operational transformation to make concurrent text edits converge. Google described operational transformation as one piece of making collaboration fast. Source: https://drive.googleblog.com/2010/09/whats-different-about-new-google-docs.html

Transfer to Mission Control:

- Do not import OT/CRDT complexity unless users are concurrently editing the same rich text/code document inside Mission Control.
- The Mission Control domain is mostly command/state/event synchronization. Server-authoritative command acceptance with idempotency is simpler and more appropriate.
- For draft text in a message composer, local-only editing is enough until the user sends. For collaborative prompt editing later, isolate that as a separate document-sync feature.

### Multiplayer netcode: per-client view models, deltas, prediction, backpressure

Valve's Source networking uses an authoritative server, tick-based simulation, snapshots, delta compression from the last acknowledged update, client-side prediction for local input, interpolation for remote entities, and bandwidth-aware update rates. It documents typical packet rates around 20-30 packets per second, default tickrates such as 66 Hz in some Source games, and a default interpolation period around 100ms. Source: https://developer.valvesoftware.com/wiki/Source_Multiplayer_Networking

Gaffer on Games shows the bandwidth logic behind snapshot compression: full 60 Hz snapshots of a sample physics world started around 17.37 Mbps, then delta compression and bit packing reduced bandwidth dramatically toward a 256 kbps target. The key architectural idea is not the bit packing; it is "encode this snapshot relative to a baseline the receiver has acknowledged." Source: https://gafferongames.com/post/snapshot_compression/

Transfer to Mission Control:

- Maintain per-client subscription state and last-acknowledged cursors. Do not compute one global outbound stream and hope every browser can keep up.
- Send deltas relative to each client's acknowledged view version where practical. If the client falls too far behind, send a compact replacement snapshot for that view.
- Use prediction/local echo only for the user's own commands, then reconcile against server authority.
- Add explicit degradation policy: slow clients keep critical and state events; token/log bulk is coalesced, sampled, or paused with a visible `stream.degraded` event.

### Trading UIs: simple hot paths, filtering, replay-to-live

Databento's OPRA post is a good reminder that high-throughput realtime systems often win by simplifying the hot path. They serve over 1.4M options tickers, cite OPRA bandwidth requirements around 37.3 Gbps, and emphasize a single-server/distributed-monolith design, efficient subscription filtering, binary encoding, replay from an intraday start time, and then seamless join to realtime. Source: https://databento.com/blog/real-time-tick-data

Google Cloud's market-data frontend pattern adapts Pub/Sub to WebSockets and notes that consumer broadband latency is often tens to hundreds of milliseconds, acceptable for visualization. Their sample chart updates at 500ms intervals rather than redrawing every inbound tick. Source: https://cloud.google.com/blog/topics/financial-services/building-real-time-streaming-pipelines-for-market-data

Transfer to Mission Control:

- Keep the gateway's hot path simple: ingest, append, materialize, filter, send.
- Avoid routing every token/log byte through a distributed queue unless there is a real scaling need. A VPS gateway can do a lot if memory ownership and subscription filtering are clean.
- "Replay then join realtime" should be a first-class gateway operation. A reconnecting browser should replay from cursor, catch up, and then switch to live without a separate client-side stitching algorithm.
- For charts/counters/throughput panels, update on a fixed visual cadence. Not every event needs a DOM paint.

## Proposed End-to-End Architecture

### Components

1. Browser client:
   - TanStack Start app shell from Vercel.
   - Direct `wss://gateway.example.com/v1/realtime?ticket=...` connection to VPS.
   - Local state store for materialized board views, selected session timeline, approval inbox, process tails, and pending commands.

2. Rust Mission Gateway on VPS:
   - Terminates WebSocket and REST/SSE.
   - Authenticates connection tickets and enforces Origin allowlist.
   - Maintains per-client subscriptions, cursors, outbound queues, and backpressure policy.
   - Aggregates one or more Gooselake runtimes.
   - Owns materialized read models for board/session/team/process views.

3. Gooselake runtime instances:
   - Existing HTTP/SSE/event-ledger primitives remain authoritative for runtime execution.
   - Each upstream runtime/source exposes sequence-numbered replay.
   - Later RunPod workers appear as additional sources with `source_id`, cursor, health, and capability metadata.

4. Durable stores:
   - Gooselake runtime DB remains source of truth for execution events.
   - Gateway may maintain a small local read-model DB/cache for fast bootstrap and cross-runtime aggregation.
   - Large logs/transcripts should be retained in runtime storage or object/log storage and referenced by event payloads when too large.

### Connection Flow

1. Browser loads app from Vercel.
2. The app obtains a short-lived realtime ticket from the authenticated app backend or auth service. The ticket should be scoped to user, workspace, gateway audience, allowed origins, and expiry under roughly 60 seconds.
3. Browser opens WebSocket directly to the VPS gateway with the ticket.
4. Gateway validates ticket, Origin, expiry, nonce/JTI, and user/workspace permissions before accepting.
5. Gateway sends:

```json
{
  "type": "hello",
  "connection_id": "conn_...",
  "server_time": "2026-07-06T14:00:00Z",
  "resume_supported": true,
  "heartbeat_ms": 15000,
  "max_client_message_bytes": 65536,
  "protocol_version": 1
}
```

6. Client sends `resume` if it has prior cursors; otherwise it sends initial subscriptions.
7. Gateway sends one or more snapshots, then deltas.

### Event Envelope

Every outbound event should carry enough metadata for replay, ordering, reconciliation, and debugging:

```json
{
  "type": "event",
  "lane": "critical|state|tokens|bulk",
  "gateway_seq": 184467,
  "source_id": "vps-main",
  "source_seq": 98231,
  "scope": "session|team|process|approval|runtime|board",
  "scope_id": "sess_123",
  "entity_version": 42,
  "kind": "approval.requested",
  "criticality": "critical",
  "command_id": "cmd_7N9...",
  "happened_at": "2026-07-06T14:00:01.123Z",
  "observed_at": "2026-07-06T14:00:01.127Z",
  "payload": {}
}
```

Key rule: `gateway_seq` is the gateway's observed merge order. `source_seq` is the source's correctness order. UI reducers may use `gateway_seq` for stream continuity, but source gap detection and source replay must use `source_id + source_seq`.

### Materialized Views

Do not make the browser derive everything from raw events during normal operation. The gateway should expose:

- Fleet board view: row per session/agent/member with status, current turn, provider, team, worktree, health, unread/error indicators, pending approval count, latest activity, and coarse token/log activity.
- Approval inbox: pending approvals sorted by urgency, session/team context, requested command/tool, risk level, age, and stale state.
- Session detail view: canonical turn timeline, current streaming assistant text, tool calls, approvals, artifacts, and terminal result.
- Process view: process status, command metadata, tail offsets, sampled output events, and links/cursors to authoritative logs.
- Team view: members, deliveries, pending messages, failed/deferred delivery counts, and recent coordination events.

The ledger is still needed for audit, replay, and reconstruction. The materialized view is the low-latency rendering contract.

## Subscription and Interest Management

Borrow the "area of interest" idea from games and "intents" from Discord.

Subscription examples:

```json
{
  "type": "subscribe",
  "request_id": "req_1",
  "subscriptions": [
    {"id": "approvals", "kind": "approval_inbox"},
    {"id": "fleet", "kind": "fleet_board", "team_id": "team_4", "window": {"offset": 0, "limit": 80}},
    {"id": "session-main", "kind": "session_detail", "session_id": "sess_a", "include_tokens": true},
    {"id": "process-tail", "kind": "process_tail", "process_id": "proc_b", "stream": "stderr", "tail_bytes": 32768}
  ]
}
```

Recommended subscription rules:

- The approval inbox is always active and critical.
- Board rows get summary events only. They should not receive token-by-token content.
- Visible rows can receive richer state than offscreen rows.
- The selected session receives token deltas, tool-call changes, and approval detail.
- Process logs are demand-driven by selected process/tail subscriptions.
- Team broadcast/delivery detail is demand-driven unless it affects unread, failed, deferred, or approval counts.

When the client scrolls or filters, it sends a subscription update. The gateway returns a snapshot for newly visible rows and stops sending detail for rows that leave the viewport.

## Priority Lanes

Use separate logical lanes over the same WebSocket. Implementation can be separate queues with weighted scheduling.

### Lane 0: critical

Includes:

- `approval.requested`
- `approval.resolved`
- `command.accepted`
- `command.rejected`
- `turn.failed`
- `turn.interrupted`
- `turn.completed`
- `auth.expiring`
- `auth.revoked`
- `source.gap_detected`

Policy:

- Flush immediately.
- Never coalesce.
- Never drop for slow clients.
- If the critical queue backs up, terminate or degrade the connection rather than silently losing events.

### Lane 1: state

Includes:

- session status changes
- turn started/progress phase changes
- team membership/delivery state
- process started/exited
- board summary patches

Policy:

- Microbatch for 16-50ms.
- Coalesce repeated updates to the same entity where only the latest summary matters.
- Preserve terminal and state-machine transition events.

### Lane 2: tokens

Includes:

- assistant text deltas
- reasoning/status text where allowed
- tool streaming summaries

Policy:

- Coalesce by `session_id + turn_id + content_stream_id`.
- Flush on `requestAnimationFrame` cadence client-side, and every 16-33ms or max byte threshold server-side.
- Preserve final full text in a canonical turn result; token events are a presentation stream.

### Lane 3: bulk/logs

Includes:

- stdout/stderr samples
- high-volume debug logs
- token usage counters
- trace/debug data

Policy:

- Demand-driven subscriptions only.
- Tail and sample by default.
- Drop/coalesce under backpressure with visible degradation.
- Authoritative log retrieval must be via range/tail endpoints, not only streamed events.

## Optimistic UI and Command Reconciliation

Commands that need local echo:

- approve/reject an approval
- interrupt a turn
- send user message / start turn
- send team message or broadcast
- retry/cancel a delivery
- kill a process

Use this lifecycle:

1. Client allocates `command_id` and records pending local intent.
2. Client updates the UI to pending state only:
   - approval button disabled, label "Approving..."
   - interrupt button disabled, row shows "Interrupt requested"
   - team message appears with pending marker
3. Client sends command over WebSocket or REST with `command_id` idempotency key and the `base_entity_version` it observed.
4. Gateway validates auth, entity state, source health, idempotency, and version constraints.
5. Gateway returns or emits one of:
   - `command.accepted` with `gateway_seq`
   - `command.rejected` with machine-readable reason
   - `command.duplicate` with the original result
6. Later authoritative state events update the entity:
   - `approval.resolved`
   - `turn.interrupting`
   - `turn.interrupted`
   - `team_message.created`
   - `delivery.injected`
   - `process.kill_requested`
   - `process.exited`

Rejected-command UX:

- Keep the user on the same object; do not toast-only critical failures.
- Revert only the pending local marker.
- Show the rejection reason inline on the command surface.
- If stale version is the reason, refresh that entity and explain that the underlying approval/turn changed.
- Allow retry only when the rejection is recoverable.

Important: an accepted command is not always completed work. For example, `interrupt` accepted means the gateway accepted and forwarded the interrupt, not that the agent has stopped. The UI should distinguish "requested", "forwarded", "acknowledged by runtime", and "terminal".

## Latency Engineering

Perceived latency in an agent UI is a chain:

1. User input to browser event handler.
2. Browser to gateway RTT.
3. Gateway auth/routing/admission.
4. Runtime/provider queueing.
5. Model/tool time to first event or token.
6. Gateway ingest/materialize/filter.
7. Transport flush to browser.
8. Browser parse/reducer work.
9. React render/commit.
10. Browser layout/paint.

For many agent interactions, provider time-to-first-token dominates. Once streaming begins, frontend rendering can dominate if every chunk triggers React state and Markdown reparsing.

Source-backed frontend rules:

- Chrome recommends appending streamed plain text with `append()`/`insertAdjacentText()` rather than rebuilding `textContent`/`innerHTML`; replacing accumulated HTML forces repeated parse and render work.
- For Markdown, Chrome recommends a streaming Markdown parser plus sanitizer rather than reparsing all accumulated chunks on every chunk.
- MDN documents `requestAnimationFrame` as the browser callback before the next repaint, usually matching display refresh rate such as 60Hz, 120Hz, or 144Hz.

Sources: https://developer.chrome.com/docs/ai/render-llm-responses and https://developer.mozilla.org/en-US/docs/Web/API/Window/requestAnimationFrame

Recommended client render strategy:

- Do not call React `setState` for every token.
- Buffer inbound token chunks in refs or an external store.
- Schedule one rAF flush per visible stream.
- Commit text deltas to the DOM or store at frame cadence.
- For Markdown, parse incrementally and sanitize accumulated content.
- Pause auto-scroll if the user has scrolled up; resume only when they are at the bottom.
- Virtualize long timelines and logs.
- Keep approval controls outside heavy transcript subtrees so critical actions do not wait behind token rendering.

Recommended server flush strategy:

- Critical lane: immediate.
- State lane: 16-50ms microbatch with per-entity coalescing.
- Tokens: 16-33ms or byte threshold, whichever comes first.
- Logs: sampled/tail windows; never unbounded push.

Metrics to capture from day one:

- Browser-to-gateway RTT from ping/pong.
- Command click to `command.accepted`.
- Command click to authoritative state transition.
- Turn start to first runtime event.
- Turn start to first visible token.
- Incoming messages per second by lane.
- Client reducer time and render frame budget.
- WebSocket buffered bytes.
- Dropped/coalesced bulk events by lane.
- Replay duration after reconnect.

## Fan-In Across Multiple Runtimes

The gateway will eventually aggregate the VPS runtime plus elastic RunPod workers. This is a distributed log merge problem.

Recommended source model:

```json
{
  "source_id": "runpod-us-west-3",
  "source_kind": "gooselake-runtime",
  "source_epoch": "boot_20260706_01",
  "last_source_seq": 123456,
  "health": "live|degraded|replaying|stale|offline",
  "replay_window": {"from_seq": 120000, "to_seq": 123456}
}
```

Merge rules:

- Preserve per-source order strictly.
- Assign `gateway_seq` when the gateway accepts/observes the event.
- Do not infer real-world causality across sources from `gateway_seq`.
- Include `source_epoch` to detect runtime restart/sequence reset.
- Maintain a cursor vector: `{source_id: {epoch, seq}}`.
- Persist the gateway's last consumed cursor per source.

Gap handling:

1. If next expected source sequence is missing, mark that source `gap_detected`.
2. Pause non-critical derived state for affected entities if correctness depends on the missing range.
3. Use the source replay endpoint to fill the gap.
4. If replay succeeds, emit `source.gap_filled` and resume.
5. If the replay window is gone, fetch a source snapshot/materialized state and emit `source.snapshot_resync`. The UI should show that continuity was broken but state is refreshed.

Cross-source commands:

- Route commands to the owning runtime/source for the target session/team/process.
- If ownership is unknown or stale, reject as `source_unavailable` rather than queueing blindly.
- For team views spanning sources, gateway materialization can aggregate, but source-owned mutations must remain source-routed or use an explicit coordinator.

## Auth for Direct Browser to VPS

Facts from sources:

- Browser WebSocket APIs do not allow custom Authorization headers.
- WebSockets are not protected by normal CORS enforcement in the same way as fetch; servers must validate `Origin`.
- OWASP calls out Cross-Site WebSocket Hijacking, message-level authorization, token rotation, message size limits, rate limiting, and heartbeat/backpressure controls.
- Cloudflare notes that WAF inspection generally covers the initial 101 upgrade request, not the established message stream, and that WebSockets can be terminated by infrastructure deploys or idle timeouts.

Sources: https://websocket.org/guides/authentication, https://cheatsheetseries.owasp.org/cheatsheets/WebSocket_Security_Cheat_Sheet.html, https://developers.cloudflare.com/network/websockets/

Recommended pattern:

1. User authenticates to the web app normally.
2. TanStack/Vercel server route or auth service issues a short-lived single-use realtime ticket:
   - `iss`: app auth issuer
   - `aud`: Mission gateway
   - `sub`: user ID
   - `workspace_id`
   - scopes/capabilities
   - `origin`: expected app origin
   - `exp`: 30-60 seconds
   - `jti`: nonce stored or derivable for one-time use
3. Browser opens `wss://gateway.example.com/v1/realtime?ticket=...`.
4. Gateway validates ticket before upgrade where possible or immediately after upgrade with a strict auth timeout.
5. Gateway validates `Origin` against an exact allowlist, for example `https://mission.example.com` and preview origins if intentionally enabled.
6. Gateway establishes connection capabilities and subscription permissions.
7. Client refreshes auth in-band before expiry using `auth.refresh` with a new ticket/token.
8. Gateway revalidates permissions on every sensitive command, not only at connection time.

Avoid:

- Long-lived JWTs in query parameters.
- Relying only on cookies for cross-origin WebSockets.
- Wildcard Origin checks.
- Trusting connection auth forever after the user logs out or loses permissions.
- Logging raw tickets, tokens, prompts, or command payloads in access logs.

REST CORS:

- For REST endpoints used by the Vercel app, allow exact origins only.
- Use `Authorization: Bearer ...` for REST fetches where browser headers are available.
- Keep WebSocket ticket auth separate from REST bearer auth.

## Failure and Reconnect UX

Good realtime products do not hide connectivity problems. They make stale data legible without making the user panic.

Connection states:

- `connected`: live stream active, heartbeat OK.
- `degraded`: connected but bulk lanes paused/coalesced or RTT high.
- `reconnecting`: transport down, retrying with backoff.
- `replaying`: connected and replaying missed events before live.
- `stale`: replay failed or a source is behind; data age shown.
- `offline`: no connection and no recent successful replay.

UI rules:

- Show a small global connection indicator with RTT/degraded status available on hover.
- Show per-source or per-team stale badges when only part of the fleet is affected.
- Pending approvals should show last-confirmed age. If stale beyond a threshold, require refresh before destructive approval decisions.
- Disable dangerous commands when the target source has a known gap or ownership is stale.
- Keep read-only browsing available against cached snapshots.
- During replay, keep the UI usable but mark it as catching up.
- If replay is impossible and a snapshot refresh is used, show "Refreshed from latest snapshot; some intermediate events may be absent from this view" in the relevant timeline/audit surface.

Reconnect protocol:

```json
{
  "type": "resume",
  "connection_id": "old_conn",
  "last_gateway_seq": 184467,
  "source_cursors": {
    "vps-main": {"epoch": "boot_a", "seq": 98231},
    "runpod-7": {"epoch": "boot_b", "seq": 1204}
  },
  "subscriptions": ["approvals", "fleet", "session-main"]
}
```

Gateway response:

- `resume.accepted`: replay starts.
- `resume.partial`: some sources replayed, others snapshot-refreshed.
- `resume.rejected`: full snapshot required.

The browser should persist cursors after every applied event, not only on clean close.

## Recommended Protocol Surface

### Client to server

- `auth.refresh`
- `ping`
- `subscribe`
- `unsubscribe`
- `resume`
- `ack`
- `command.send_turn`
- `command.resolve_approval`
- `command.interrupt_turn`
- `command.send_team_message`
- `command.retry_delivery`
- `command.cancel_delivery`
- `command.kill_process`

### Server to client

- `hello`
- `snapshot`
- `patch`
- `event`
- `command.accepted`
- `command.rejected`
- `command.duplicate`
- `auth.expiring`
- `auth.refreshed`
- `connection.degraded`
- `source.gap_detected`
- `source.gap_filled`
- `source.snapshot_resync`
- `error`
- `pong`

### Idempotency and dedupe

Every command should include:

```json
{
  "command_id": "cmd_01J...",
  "target": {"kind": "approval", "id": "apr_..."},
  "base_entity_version": 12,
  "created_at_client": "2026-07-06T14:00:01.100Z",
  "payload": {}
}
```

The gateway should retain command IDs long enough to survive reconnect and browser retries. Duplicate IDs return the original accepted/rejected result.

## Data Model Additions to Consider

For the gateway/read-model layer:

- `gateway_events`: gateway sequence, source ID, source sequence, source epoch, lane, scope, entity, kind, payload/payload reference.
- `source_cursors`: source ID, epoch, last consumed seq, health, replay window, last heartbeat.
- `client_sessions`: connection ID, user, workspace, subscriptions, last acked gateway seq, cursor vector, capabilities.
- `materialized_fleet_rows`: source/session/team summary with entity version.
- `pending_commands`: command ID, user, target, status, accepted/rejected seq, reason.
- `subscription_snapshots`: optional cache of snapshot version/hash per subscription.

For existing Gooselake runtime events, add or preserve:

- criticality/lane
- entity version
- source epoch
- idempotency command ID
- payload references for large logs/transcripts
- causality fields: `caused_by_command_id`, `turn_id`, `approval_id`, `delivery_id`

## Concrete MVP Design

MVP should avoid building every distributed-systems feature at once. Recommended cut:

1. Single VPS gateway, single Gooselake runtime source.
2. WebSocket protocol with ticket auth, Origin validation, heartbeat, command IDs, and cursors.
3. Bootstrap snapshots for:
   - fleet board
   - approval inbox
   - selected session detail
4. Lanes implemented as separate in-process queues over one socket.
5. Token coalescing at 16-33ms server-side plus rAF batching client-side.
6. Process logs as sampled stream plus authoritative range/tail endpoint.
7. Reconnect with `last_gateway_seq` and replay from existing runtime event ledger.
8. Visible connection states and stale/replay UI.

Do not build in MVP:

- Cross-source total ordering beyond gateway observed order.
- CRDT/OT editing.
- Binary encoding unless JSON overhead is measured as a problem.
- Horizontal gateway scaling before ownership/lease design exists.
- Full offline command queueing for dangerous commands.

## Later Scaling Path

When adding RunPod workers:

1. Introduce `source_id`, `source_epoch`, and per-source cursors before adding the second source.
2. Add source ownership metadata for sessions/teams/processes.
3. Add source replay/gap tests.
4. Add gateway materializer tests for out-of-order cross-source arrival.
5. Add per-client viewport subscriptions if not already present.
6. Add snapshot resync for a source whose replay window is exhausted.
7. Consider binary payloads only for high-volume token/log lanes if measurements justify it.

## Open Questions

- Will Mission Control own a new WebSocket gateway protocol, or should it initially use HTTP commands plus SSE directly? For lowest bidirectional latency and simpler auth refresh/subscriptions, I recommend a WebSocket gateway, but the runtime ledger should stay transport-agnostic.
- Will the gateway live in the same Rust binary as runtime-server or as a separate aggregation service? Same binary is simpler for VPS MVP; separate service may be cleaner once RunPod workers exist.
- What is the intended user/workspace auth issuer? The ticket model needs a trusted signer and JWKS/secret rotation plan.
- What replay retention is guaranteed per runtime source for high-volume token/log events? This decides when the UI can replay versus must snapshot-refresh.
- Are approval decisions allowed while source state is stale? I recommend no for destructive/high-risk approvals.

## Source Index

- Figma, "How Figma's multiplayer technology works": https://www.figma.com/blog/how-figmas-multiplayer-technology-works/
- Figma, "Making multiplayer more reliable": https://www.figma.com/blog/making-multiplayer-more-reliable/
- Linear, "Scaling the Linear Sync Engine": https://linear.app/now/scaling-the-linear-sync-engine
- Reverse-engineering Linear sync: https://marknotfound.com/posts/reverse-engineering-linears-sync-magic
- Discord Gateway docs: https://docs.discord.com/developers/events/gateway
- Google Docs collaboration blog: https://drive.googleblog.com/2010/09/whats-different-about-new-google-docs.html
- Valve Source multiplayer networking: https://developer.valvesoftware.com/wiki/Source_Multiplayer_Networking
- Gaffer on Games snapshot compression: https://gafferongames.com/post/snapshot_compression/
- WebSocket authentication guide: https://websocket.org/guides/authentication
- OWASP WebSocket Security Cheat Sheet: https://cheatsheetseries.owasp.org/cheatsheets/WebSocket_Security_Cheat_Sheet.html
- Cloudflare WebSockets docs: https://developers.cloudflare.com/network/websockets/
- Chrome streamed LLM rendering guidance: https://developer.chrome.com/docs/ai/render-llm-responses
- MDN `requestAnimationFrame`: https://developer.mozilla.org/en-US/docs/Web/API/Window/requestAnimationFrame
- Databento realtime tick data architecture: https://databento.com/blog/real-time-tick-data
- Google Cloud market-data WebSocket frontend: https://cloud.google.com/blog/topics/financial-services/building-real-time-streaming-pipelines-for-market-data
