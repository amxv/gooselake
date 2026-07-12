# P04 replay, gap, restart, and disposable-context laboratory

This procedure is a verification gate over existing production paths. It does
not repair product behavior or treat IndexedDB as authority.

## Deterministic checks

Run the focused Tower resume tests:

```bash
cargo test -p goosetower gateway::tests::resume
```

They exercise gateway replay overlap/deduplication, a missing source cursor and
snapshot fallback, source-epoch replacement, and stale-command rejection
through the real gateway/materializer/runtime-client paths.

Run the production Worker socket and cursor checks:

```bash
bun run --cwd apps/gooseweb scripts/realtime-worker-smoke.ts
bun run --cwd apps/gooseweb scripts/p04-reconnect-lab-smoke.ts
```

The versioned cursor fixture is
`verification/gooseweb/fixtures/p04-reconnect-cursors-v1.json`. The smoke must
observe current product behavior exactly. A dangerous transition may pass the
product predicate only when the fixture maps it to a P06-P10 baseline; removing
that mapping or making a seeded failure look safe must fail the smoke.

## Supervisor-owned reconnect smoke

The later read-only browser review reuses the P03 headless `agent-browser`
procedure and the P02 public-contract fake source. The implementer starts no
stack or browser. Under the sole migration lease, the supervisor supplies one
exact clean head and the reviewer uses a fresh unique ephemeral Chromium
session.

1. Submit one deterministic turn and one Team Comms message through semantic UI
   controls. Record runtime, Tower, decoded-frame, Worker/store, and visible IDs.
2. Lose only the WebSocket. During loss, require an honest offline/stale state
   and reject risky commands; after reconnect, require resume from the recorded
   cursor and no duplicate visible identity.
3. Seed replay overlap, then a missing source sequence. Overlap must not
   duplicate. The gap must remain stale and must not reduce later state before
   targeted repair or snapshot fallback.
4. End that attempt, stop/release Tower, then start a new lease attempt with a
   new gateway/source epoch. An old IndexedDB cursor may accelerate detection
   but may not suppress the new authoritative snapshot.
5. Separately dispose the initial browser session/profile and open a second
   fresh ephemeral context with no prior browser cache, IndexedDB, session
   storage, CacheStorage, or service worker. Repeat the supported login flow and
   verify reconstruction from Tower.

Capture the P03 evidence set plus cursor transitions and recovery duration.
Known P04 baselines remain `blocked_not_approved`; they are corrections for
P06-P10, not reasons to compensate in this laboratory.

## Required outcomes

- Replay is bounded and overlap is deduplicated.
- Missing source continuity emits stale/gap evidence and snapshot fallback.
- Source and gateway generations are not conflated.
- Commands are rejected while their owning source is stale or unavailable.
- A fresh context reconstructs without old cache.
- Exactly one ordered semantic result is visible, or a manifest-mapped product
  baseline identifies the first divergent layer.

Any hidden gap, reduction beyond a gap, stale command admission, cache-dependent
fresh context, unmapped product defect, secret leak, or seeded failure reported
as safe fails the P04 gate.
