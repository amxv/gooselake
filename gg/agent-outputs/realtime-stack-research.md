# Realtime Stack Research for Gooselake Board

Date: 2026-07-06

Context: browser Board console for live interaction with AI coding agents running on Gooselake. Existing runtime is Rust/Axum with HTTP and replayable sequence-numbered SSE event streams. Proposed addition is a Rust gateway on the same VPS that fans in runtime events and provides bidirectional browser connectivity. Frontend is TanStack Start on Vercel; browsers connect directly to the VPS gateway.

## Executive Recommendation

Build a custom self-hosted Rust gateway on the VPS. Use WebSocket as the production default transport, implement WebTransport as an optional fast path behind capability and network probing, and keep SSE as a one-way diagnostic/read-only fallback or replay endpoint. Use explicit sequence numbers and cursor-based replay at the application protocol layer for every transport.

Recommended package set:

- Gateway HTTP/WebSocket: `axum` WebSocket upgrade on the existing Tokio/Tower stack.
- Optional WebTransport: `wtransport` first; evaluate `web-transport-quinn`/`moq-dev/web-transport` if the desired API should expose Quinn-like primitives directly.
- Wire format: Protobuf with Rust `prost` and TypeScript `@bufbuild/protobuf`/`protoc-gen-es` for stable cross-language schemas. Consider MessagePack only for a faster MVP with looser contracts. Avoid rkyv on the public browser protocol.
- Browser state: plain TypeScript transport/decoder running in a Web Worker, posting coalesced patches to the UI. Do not start with a Rust/WASM client core.
- React rendering: TanStack Start + TanStack Store for narrow subscriptions, `@tanstack/react-virtual` for logs/feeds/lists, requestAnimationFrame batching, worker-side parsing, and canvas/WebGPU only for board surfaces with many moving/graphical objects.
- Off-the-shelf realtime infra: do not put Cloudflare Durable Objects, Convex, Electric, Zero, or Centrifugo on the critical path for this gateway. They solve valuable adjacent problems, but none beats a colocated Rust gateway for lowest latency to a Rust runtime on the same VPS.

## 1. Transport

### WebTransport in 2026

Browser support changed materially in 2026. MDN marks `WebTransport` as Baseline 2026 and says it became newly available across latest major browsers in March 2026; Can I Use reports Chrome 97+, Edge 98+, Firefox 114+, Safari 26.4+, and iOS Safari 26.4+ support. WebTransport provides HTTP/3/QUIC sessions, bidirectional and unidirectional streams, datagrams, stats, and backpressure through Streams APIs.

Pros:

- Best fit for lowest-latency advanced realtime: QUIC avoids TCP connection-level head-of-line blocking across independent streams.
- Supports unreliable datagrams for ephemeral state such as cursor positions, viewport telemetry, presence pings, or high-frequency transient agent progress.
- Supports multiple reliable streams, which maps well to independent channels: command replies, runtime events, logs, file diffs, terminal streams.
- Available in Web Workers, so parsing and protocol management can move off the main thread.

Cons and maturity notes:

- Safari/iOS support is new as of 26.4. Older iPhones and enterprise-managed browsers will miss it for a while.
- QUIC/UDP can be blocked by corporate networks, captive portals, and some middleboxes. A production app still needs WebSocket fallback.
- Rust server ecosystem is usable but not as boring as WebSocket. `wtransport` 0.7.1 is pure Rust, async-friendly, Quinn-based, and documented, but earlier ecosystem notes still describe WebTransport crates as less production-proven than WebSocket stacks.
- HTTP/3 termination on a VPS needs careful TLS/ALPN, UDP firewall, reverse proxy, and observability setup. It is easier to get wrong than WebSocket over HTTPS.

Rust crates:

- `wtransport`: pure Rust WebTransport over HTTP/3, built on `quinn`, `rustls`, and Tokio. Good first candidate for a custom gateway.
- `web-transport-quinn`: exposes a Quinn-like API and intentionally hides HTTP/3 internals. Useful if the gateway wants QUIC session semantics.
- `h3`/`quinn`: lower-level building blocks. Use directly only if the higher-level crates block required control.
- `socketioxide`: Rust Socket.IO server on Tower/Hyper/Axum. Mature convenience stack with polling and WebSocket transports, rooms, acks, and MsgPack parser, but it adds Socket.IO protocol overhead and is not the lowest-latency custom protocol path.

### WebSocket

MDN describes `WebSocket` as stable with good browser and server support, including Safari/iOS back to old versions. It is the practical baseline for bidirectional browser communication in 2026.

Pros:

- Universal browser support and proxy friendliness.
- Excellent Rust support through Axum/tungstenite ecosystem.
- Simple deployment behind normal TLS and reverse proxies.
- Good enough for live AI-agent streams, command submission, terminal-ish text, and most Board interactions.

Cons:

- Single ordered reliable TCP stream means head-of-line blocking if a large message or packet loss delays everything behind it.
- Browser `WebSocket` has no built-in backpressure; the app must monitor `bufferedAmount`, batch, drop low-priority messages, and avoid pushing more than the client can process.
- No native resume/replay semantics.

Rust crates:

- `axum` WebSocket extractor/upgrade: best fit because Gooselake already uses Rust/Axum.
- `tokio-tungstenite`/`tungstenite`: lower-level alternative if the gateway is not directly Axum.
- `socketioxide`: use only if rooms/acks/fallbacks and compatibility with Socket.IO clients matter more than custom protocol efficiency.

### SSE

SSE remains excellent for one-way streams and already matches Gooselake’s runtime model. It has built-in browser reconnection and `Last-Event-ID`, but it is not bidirectional.

Pros:

- HTTP-native, proxy-friendly, easy to inspect.
- Built-in EventSource reconnect and `Last-Event-ID` resume.
- Great for a replay/debug endpoint, audit stream, or low-complexity read-only console.

Cons:

- Server-to-client only. Browser commands need separate HTTP POST/fetch.
- Text-only event framing, so binary payloads need base64 or a separate endpoint.
- Less suitable as the primary bidirectional Board transport.

### Fallback Strategy

Implement a transport-neutral application protocol:

1. Browser probes `WebTransport` support and attempts a short connection with timeout.
2. If WebTransport is unavailable or fails, use WebSocket.
3. If WebSocket is blocked, expose read-only SSE plus POST commands as degraded mode.

Do not make WebTransport the only path until traffic confirms Safari 26.4+ adoption and UDP reachability for the user base. The first production version should ship WebSocket as default and WebTransport behind an opt-in or progressive rollout.

Sources:

- MDN WebTransport: https://developer.mozilla.org/en-US/docs/Web/API/WebTransport
- Can I Use WebTransport: https://caniuse.com/webtransport
- MDN WebSocket API: https://developer.mozilla.org/en-US/docs/Web/API/WebSockets_API
- `wtransport` docs: https://docs.rs/wtransport/latest/wtransport/
- `web-transport-quinn` docs: https://docs.rs/web-transport-quinn/latest/web_transport_quinn/
- `socketioxide` docs: https://docs.rs/socketioxide/latest/socketioxide/

## 2. Serialization

### Recommendation

Use Protobuf for the browser-facing protocol:

- Rust: `prost`
- TypeScript/browser: `@bufbuild/protobuf` and `@bufbuild/protoc-gen-es`
- Schema workflow: keep `.proto` files as the source of truth, generate Rust and TS types, version every envelope, and reserve field numbers aggressively.

Use JSON only for diagnostics and low-rate control endpoints. Use MessagePack only if the team wants a quick binary MVP without schema codegen. Do not use rkyv as the public browser protocol.

### rkyv

Pros:

- Extremely fast Rust-side zero-copy access in benchmark suites.
- Useful for Rust-to-Rust caches, snapshots, or gateway-internal event buffers.

Cons:

- Browser/TypeScript ergonomics are poor. You would likely need WASM or custom readers.
- Public protocol evolution and schema compatibility are harder than Protobuf.
- Zero-copy wins vanish if the browser must convert archived Rust layouts into JS objects anyway.

Recommendation: keep rkyv for internal Rust storage if needed, not for Rust-to-browser protocol.

### bincode and postcard

Pros:

- Simple with Serde, fast in Rust, compact.
- Good for controlled Rust-native endpoints or embedded-style messages.

Cons:

- No first-class TypeScript schema story.
- Versioning discipline is on the application.
- Browser decoders are not as standard as Protobuf/MessagePack.

Recommendation: not the main browser protocol.

### Protobuf

Pros:

- Strong cross-language contract and long-term compatibility story.
- `prost` is mature in Rust.
- `@bufbuild/protobuf` is modern TypeScript/ESM-first, generates plain typed objects and schema functions, and advertises Protobuf conformance.
- Easy to preserve unknown fields and evolve messages with optional fields, oneofs, and reserved field numbers.

Cons:

- Raw Rust benchmarks are not always fastest; `prost` decode/encode can trail rkyv, bincode, bitcode, etc.
- Requires codegen and schema discipline.
- `oneof` and 64-bit integer handling need explicit frontend conventions.

Recommendation: best tradeoff for Gooselake Board because schema sharing and protocol evolution matter more than winning a Rust-only benchmark.

### FlatBuffers

Pros:

- Zero-copy-ish access model and strong schema.
- Good for large structured binary data or game-like state snapshots.

Cons:

- More awkward TS ergonomics than Protobuf for normal app messages.
- Rust benchmark data shows access can be very fast, but validated upfront reads can be expensive.
- Overkill for AI agent event envelopes and command streams.

Recommendation: consider later only for large board snapshots or dense geometry/graph payloads.

### MessagePack

Pros:

- Very easy Rust/TS interoperability: Rust `rmp-serde`, browser `@msgpack/msgpack`.
- Smaller than JSON and handles binary.
- `@msgpack/msgpack` supports browsers, Node, Deno, Bun, streams, extension codecs, and TypeScript definitions.

Cons:

- Schema is implicit unless paired with a separate validator.
- More runtime shape checking in TS.
- Benchmarks and docs suggest JSON parse can be competitive for object-heavy payloads; MessagePack wins mostly when binary and compactness matter.

Recommendation: acceptable for a fast prototype or plugin messages, but Protobuf is better for a durable public protocol.

### JSON

Pros:

- Debuggable, native browser parse, trivial integration.
- Good for config, auth bootstrap, diagnostics, and low-rate commands.

Cons:

- Larger payloads, no binary types, weak schema unless layered with JSON Schema/Zod.
- High-frequency streams allocate many JS objects and strings.

Recommendation: keep for debug/control, not for the hot event stream once throughput matters.

Sources:

- Rust serialization benchmark, updated 2026-07-01: https://github.com/djkoloski/rust_serialization_benchmark
- Protobuf-ES: https://github.com/bufbuild/protobuf-es
- MessagePack JS: https://github.com/msgpack/msgpack-javascript

## 3. Rust-to-WASM Client Cores vs Plain TypeScript Decoders

### Recommendation

Start with plain TypeScript decoders in a Web Worker. Use WASM only for isolated hot loops after profiling proves TS decoding or state reduction is the bottleneck.

The browser pipeline should look like:

1. Network transport runs in a Worker where possible.
2. Decode Protobuf/MessagePack in TypeScript.
3. Maintain a normalized mutable core model in the Worker.
4. Emit compact UI patches to the main thread at animation-frame cadence.
5. React subscribes narrowly to visible slices.

WASM is not free. `wasm-bindgen` is excellent for Rust/JS interop, but calls across the JS/WASM boundary, string/object conversion, and copying between JS ArrayBuffers and WASM linear memory can erase performance gains. WASM is strongest when the data can stay in WASM memory and work is numeric/CPU-heavy: diff algorithms, compression, CRDT merge kernels, syntax parsing, binary patch application, graph layout, or canvas/WebGPU preparation.

For Gooselake Board, most messages are likely agent events, text deltas, command state, file paths, spans, diagnostics, and UI metadata. Those become JS objects for React/TanStack anyway. A WASM client core would add build complexity, async initialization, bundle loading, memory handoff rules, and harder debugging before proving a win.

Maturity notes:

- `wasm-bindgen` remains the default Rust-to-browser binding tool.
- Leptos/Dioxus are app frameworks, not just binary decoders. They do not fit a TanStack Start/React app unless the frontend strategy changes.
- Rust-generated TypeScript bindings can help for WASM APIs, but Protobuf schema generation already solves cross-language event types more cleanly.

Sources:

- wasm-bindgen guide: https://rustwasm.github.io/docs/wasm-bindgen/print.html
- wasm-bindgen browser support: https://rustwasm.github.io/docs/wasm-bindgen/reference/browser-support.html
- Grafbase note on JS/WASM overhead: https://grafbase.com/blog/getting-started-with-rust-and-webassembly

## 4. Rendering High-Frequency Streaming UIs with TanStack Start/React

### Recommendation

Use React for durable UI structure, not every inbound event. Put high-frequency transport, decode, coalescing, and state reduction outside React. React should render sampled/committed view state.

Recommended frontend packages and patterns:

- `@tanstack/react-virtual`: logs, agent event feeds, terminal history, issue lists, file trees, and chat-like streams.
- TanStack Store: client UI state with granular subscriptions. It is alpha, but it is a small reactive primitive aligned with TanStack Start.
- Alternative mature stores: Zustand or Jotai if the team wants a non-alpha state library; however TanStack Store fits the TanStack direction.
- TanStack Query: request/response and cacheable server data, not the hot realtime stream itself.
- Worker protocol core: transport decode, sequence tracking, dedupe, and patch generation.
- `requestAnimationFrame` batching: at most one visible-state commit per frame for fast streams.
- Backpressure: track client render lag and transport send queue; drop/coalesce low-priority ephemeral updates.

Rendering choices:

- DOM + virtualization: best for logs, event streams, chat, tables, panes, and inspectable UI.
- Canvas 2D: best for dense timeline visualizations, agent activity graphs, and thousands of simple moving items.
- WebGL/WebGPU: only when the Board becomes Figma-like: large graph/canvas, zoomable workspace, many objects, animated edges, dense minimaps. Keep UI chrome in React and render the scene separately.

React-specific guidance:

- Avoid `setState` per packet.
- Normalize by entity ID and sequence number.
- Maintain append-only ring buffers for logs with virtualized views.
- Split high-frequency transient state from durable state. For example, cursor/presence can be lossy and frame-sampled; terminal output and agent events must be ordered and replayable.
- Use `useSyncExternalStore`-style subscriptions or TanStack Store selectors so components subscribe to exact slices.

TanStack Virtual has specific support for chat/agent-style surfaces such as end anchoring, follow-on-append, scroll-to-end, and measuring streamed rows. This is directly relevant to AI agent logs and conversations.

Sources:

- TanStack Virtual: https://tanstack.com/virtual/latest
- TanStack Store: https://tanstack.com/store/latest
- TanStack libraries overview: https://tanstack.com/libraries

## 5. Off-the-Shelf Realtime Infra vs Custom Rust Gateway

### Recommendation

Use a custom Rust gateway for the core Board transport. The gateway is colocated with the Rust runtime on the same VPS, can consume replayable Gooselake SSE/HTTP directly, can expose the exact bidirectional protocol needed by Board, and can optimize latency without crossing another vendor runtime.

### Cloudflare Durable Objects

Pros:

- Strong stateful edge actor model.
- Built-in WebSockets, embedded SQLite, alarms, per-room/object coordination.
- Excellent for globally distributed rooms, collaborative docs, and reducing ops.

Cons:

- Adds an edge hop between browser and VPS runtime unless the runtime moves closer to Cloudflare.
- JavaScript/TypeScript runtime rather than Rust.
- Does not directly solve lowest latency to a single VPS-hosted Rust runtime.

Recommendation: valuable later for global fanout, presence, or hosted collaboration rooms; not the core gateway while runtime is VPS-local.

### PartyKit

Pros:

- Simple multiplayer/collaboration model.
- Used by tldraw and other collaborative apps.
- Good developer experience for room-style realtime.

Cons:

- JavaScript/edge platform abstraction.
- Less control over custom Rust protocol and colocated runtime fan-in.

Recommendation: not for this gateway; useful reference for room actor semantics.

### Centrifugo

Pros:

- Mature self-hosted realtime server with WebSocket/SSE/HTTP-streaming, client SDKs, JWT auth, presence, history, recovery, delta compression, and strong fanout.
- Explicitly supports stream history and recovery: bounded ordered sliding windows replay missed publications on reconnect.
- Language-agnostic backend integration.

Cons:

- Written in Go and introduces a separate broker/protocol layer.
- Great for pub/sub fanout, but Board needs tight command/event semantics with Gooselake runtime replay and custom prioritization.
- Custom gateway still needed for runtime-specific aggregation, authorization, and command routing unless Centrifugo becomes the gateway.

Recommendation: best off-the-shelf fallback if team wants to avoid hand-rolled connection management, but it probably will not beat a purpose-built Rust gateway for lowest latency and protocol fit.

### NATS WebSocket Bridge

Pros:

- High-performance messaging fabric with pub/sub, request/reply, JetStream, KV/object storage.
- NATS server supports WebSocket since 2.2, with binary frames, TLS, compression, and origin checking.

Cons:

- Browser clients must speak NATS protocol over binary WebSocket frames.
- Adds broker semantics and operational moving parts.
- Useful inside distributed systems, but heavy for browser-to-single-VPS gateway.

Recommendation: consider later if Gooselake becomes multi-node and needs internal event routing. Not first choice for browser protocol.

### Electric SQL and Zero

Pros:

- Excellent local-first/read-sync systems.
- Electric syncs Postgres shapes over HTTP; Zero syncs UI-needed data into a local normalized client store.
- Strong fit for app data views, issue lists, metadata, and offline-ish client reads.

Cons:

- They sync database state, not arbitrary low-latency bidirectional command streams from live agents.
- They do not replace a transport gateway for commands, terminals, and runtime event replay.

Recommendation: use later for durable Board data if the product grows a Postgres-backed state model; not for the hot agent stream.

### Convex

Pros:

- Reactive database and server functions with strong live-updating app ergonomics.
- Has TanStack Start quickstart and broad client library support.

Cons:

- Moves backend model into Convex, not colocated Rust gateway.
- Hosted/reactive DB is not the low-level transport bridge to Gooselake.

Recommendation: not for this architecture unless the product backend is intentionally rebuilt around Convex.

Sources:

- Cloudflare Durable Objects: https://www.cloudflare.com/products/durable-objects
- PartyKit docs: https://docs.partykit.io/
- Centrifugo main site: https://centrifugal.dev
- Centrifugo transports: https://centrifugal.dev/docs/transports/overview
- Centrifugo history/recovery: https://centrifugal.dev/docs/server/history_and_recovery
- NATS WebSocket docs: https://docs.nats.io/running-a-nats-service/configuration/websocket
- Electric Sync docs: https://electric-sql.com/docs/intro
- Zero docs: https://zero.rocicorp.dev/docs/introduction
- Convex docs: https://docs.convex.dev/home

## 6. Reconnect and Replay

### Recommendation

Make replay semantics transport-independent and explicit:

- Every server-to-client event has `stream_id`, `seq`, `server_time`, `type`, and payload.
- Every client command has `client_msg_id`, `idempotency_key`, optional `depends_on_seq`, and expected ack semantics.
- Browser persists the last applied cursor per stream in memory and periodically in IndexedDB/session storage.
- On reconnect, client sends `resume` with last applied cursor map.
- Server replies with either replayed events, a gap/snapshot-required response, or a fresh snapshot followed by live tail.
- Keep a per-board/per-agent ring buffer in the gateway and use Gooselake runtime’s replayable SSE stream as the source of truth where available.

For WebSocket/WebTransport:

- Reconnect with exponential backoff and jitter.
- Authenticate each reconnect.
- Resume subscriptions and stream cursors explicitly.
- Detect gaps before applying events.
- Dedupe by `(stream_id, seq)`.
- Keep command idempotency so browser retries do not duplicate actions.

For SSE:

- Use standard `id:` fields and honor `Last-Event-ID`.
- If EventSource is used for fallback, server should map `Last-Event-ID` to the same cursor/replay path.

Buffering policy:

- Gateway hot buffer: last N events and/or last T minutes per stream. Size by expected reconnect window, not infinite retention.
- Runtime replay: for longer gaps, use Gooselake’s sequence-numbered event streams.
- Snapshot path: for unrecoverable gaps, send compact current board/agent state then live events after a snapshot sequence watermark.

Operational concerns:

- Record `resume_success`, `resume_gap`, replay count, replay bytes, and time-to-live-tail metrics.
- Test network loss, tab sleep/wake, mobile backgrounding, deploy restarts, and duplicate reconnects.
- Treat reconnect as normal, not exceptional.

Sources:

- Last-Event-ID explainer: https://http.dev/last-event-id
- MDN Server-Sent Events reference via WebSocket page see-also: https://developer.mozilla.org/en-US/docs/Web/API/Server-sent_events
- WebSocket reconnection guide: https://websocket.org/guides/reconnection
- WebSocket production best practices: https://websocket.org/guides/best-practices/
- Centrifugo history/recovery model: https://centrifugal.dev/docs/server/history_and_recovery

## Proposed Initial Stack

Server:

- Rust/Tokio/Axum gateway process on the same VPS as Gooselake runtime.
- WebSocket endpoint: `/board/ws`.
- Replay/read endpoint: `/board/events` as SSE or HTTP stream for diagnostics and fallback.
- Optional WebTransport endpoint: `/board/wt`, shipped behind feature flag after WebSocket path is stable.
- `prost` generated protocol types, plus a small hand-written envelope/framing layer.
- Ring buffers keyed by board/session/agent stream.
- Runtime fan-in from Gooselake SSE with sequence preservation.

Client:

- TanStack Start app on Vercel.
- Browser connects directly to `wss://gateway.example.com/board/ws`.
- Worker owns connection, decode, resume, dedupe, coalescing, and state reduction.
- Main thread receives frame-batched patches.
- TanStack Store or `useSyncExternalStore` subscriptions feed React.
- TanStack Virtual for event feeds/logs.

Protocol:

- Binary Protobuf frames for hot path.
- JSON debug endpoint that can dump decoded envelopes.
- Versioned `Hello`, `Resume`, `Subscribe`, `Command`, `Ack`, `Event`, `Snapshot`, `Gap`, `Heartbeat`, and `Backpressure` messages.

Decision summary:

- Ship WebSocket first because it is universal, simple, and production-proven.
- Add WebTransport when the product has enough users to justify QUIC operational complexity or when packet loss/head-of-line blocking shows up in metrics.
- Use Protobuf because schema sharing and compatibility will matter more than max Rust-only decode speed.
- Keep React away from packet rate; update React at display rate.
- Keep self-hosted Rust gateway because the runtime is local, the protocol is specialized, and off-the-shelf realtime systems add a hop or abstraction mismatch.
