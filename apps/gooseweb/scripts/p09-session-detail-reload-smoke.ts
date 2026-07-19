import assert from "node:assert/strict";
import { create, fromBinary, toBinary } from "@bufbuild/protobuf";
import { EntityRefSchema, Lane, MessageKind } from "../src/gen/goosetower/v1/common_pb";
import {
  HelloSchema,
  RealtimeEnvelopeSchema,
  SourceGapDetectedSchema,
  SourceGapFilledSchema,
  SourceSnapshotResyncSchema,
  type RealtimeEnvelope
} from "../src/gen/goosetower/v1/realtime_pb";
import {
  PatchSchema,
  SnapshotSchema,
  ViewCoverageSchema,
  ViewOperation
} from "../src/gen/goosetower/v1/view_pb";
import { sourceEntityKey } from "../app/realtime/protocol/entities";
import type { WorkerOutbound } from "../app/realtime/types";
import { RealtimeWorkerCore } from "../app/realtime/worker/realtime-command-core";
import {
  getGoosewebSnapshot,
  getVisibleGoosewebSnapshot,
  resetGoosewebStoreForTests,
  setSubscription,
  updateGoosewebStore
} from "../app/stores/gooseweb-store";

const SOURCE = "p02-source";
const EPOCH = "p02-epoch-001";
const SESSION = "p02-session-001";
const GATEWAY = "p09-session-reload";
const CWD = "/p02/workspace";
const WORKTREE = "p02-worktree-001";

class FakeSocket {
  static readonly OPEN = 1;
  readyState = FakeSocket.OPEN;
  bufferedAmount = 0;
  binaryType = "";
  onopen: (() => void) | null = null;
  onmessage: ((event: { data: ArrayBuffer }) => void) | null = null;
  onerror: (() => void) | null = null;
  onclose: ((event: { code: number; reason: string }) => void) | null = null;
  readonly sent: Uint8Array[] = [];
  constructor(readonly url: string) { socket = this; }
  send(value: Uint8Array) { this.sent.push(value); }
  close() { this.readyState = 3; }
  open() { this.onopen?.(); }
  receive(envelope: RealtimeEnvelope) {
    const bytes = toBinary(RealtimeEnvelopeSchema, envelope);
    this.onmessage?.({ data: bytes.buffer.slice(bytes.byteOffset, bytes.byteOffset + bytes.byteLength) });
  }
}

let socket: FakeSocket | undefined;
Object.assign(globalThis, { WebSocket: FakeSocket });

resetGoosewebStoreForTests();
const liveCore = await connectedCore();
socket!.receive(detailPatch("live-session-detail"));
await flush();
const key = sourceEntityKey(SOURCE, SESSION);
const liveDetail = getGoosewebSnapshot().entities.sessionDetails[key]!;
const liveOwnership = ownershipProjection();
assert.deepEqual(liveOwnership, {
  detailCwd: CWD,
  detailWorktreeId: WORKTREE,
  sessionCwd: CWD,
  sourceId: SOURCE,
  sessionId: SESSION
});
await liveCore.handleMessage({ type: "disconnect" });

resetGoosewebStoreForTests();
const reloadCore = await connectedCore();
const requestId = latestRequestId();
socket!.receive(detailSnapshot("reload-session-detail", requestId));
await flush();
assert.deepEqual(ownershipProjection(), liveOwnership,
  "valid reload snapshot reconstructs the live patch cwd and worktree ownership");
assert.equal(getGoosewebSnapshot().subscriptions["session-detail"]?.status, "active");
socket!.receive(detailPatch("remove-detail", 5n, 5n, ViewOperation.REMOVE));
await flush();
assert.equal(getGoosewebSnapshot().entities.sessionDetails[key], undefined);
assert.equal(getGoosewebSnapshot().entities.sessions[key], undefined,
  "authoritative detail remove withdraws a detail-only render projection");
socket!.receive(detailSnapshot("restore-detail", requestId, 6n, 6n));
await flush();
socket!.receive(detailSnapshot("not-found-detail", requestId, 7n, 7n, true));
await flush();
assert.equal(getGoosewebSnapshot().entities.sessionDetails[key], undefined);
assert.equal(getGoosewebSnapshot().entities.sessions[key], undefined,
  "authoritative not-found snapshot cannot leave a ghost session");
await reloadCore.handleMessage({ type: "disconnect" });

resetGoosewebStoreForTests();
const summaryCore = await connectedCore();
socket!.receive(sessionSummaryPatch("summary", 3n, 3n));
socket!.receive(detailPatch("summary-detail", 4n, 4n));
await flush();
assert.equal(getGoosewebSnapshot().entities.sessions[key]?.cwd, CWD,
  "later detail authority wins exact operation order");
socket!.receive(detailPatch("summary-detail-remove", 5n, 5n, ViewOperation.REMOVE));
await flush();
assert.equal(getGoosewebSnapshot().entities.sessions[key]?.cwd, "/summary/workspace",
  "detail removal reveals independently authoritative session summary");
await summaryCore.handleMessage({ type: "disconnect" });

resetGoosewebStoreForTests();
let repairPublications = 0;
const repairErrors: string[] = [];
const repairCore = await connectedCore(
  () => { repairPublications += 1; },
  (message) => { repairErrors.push(message); }
);
socket!.receive(detailPatch("pre-gap-detail", 3n, 3n));
socket!.receive(sessionSummaryPatch("pre-gap-summary", 4n, 4n));
await flush();
assert.equal(getGoosewebSnapshot().entities.sessions[key]?.cwd, CWD,
  "selected detail remains the render authority when a summary follows it");
socket!.receive(gapDetected("forced-gap", 4n, 6n));
await flush();
assert.equal(getGoosewebSnapshot().connection, "stale");
assert.equal(getGoosewebSnapshot().staleSources[SOURCE], "gap_detected");
assert.equal(getGoosewebSnapshot().entities.sessionDetails[key]?.cwd, CWD,
  "gap detection retains the last coherent internal detail cache");
assert.equal(getGoosewebSnapshot().entities.sessions[key]?.cwd, CWD);
assert.equal(visibleOwnershipProjection().detailCwd, undefined,
  "gap detection withdraws untrusted selected detail from the visible projection");
socket!.receive(detailPatch(
  "frozen-later-detail",
  5n,
  6n,
  ViewOperation.UPSERT,
  body("/untrusted/later", "untrusted-worktree")
));
await flush();
assert.equal(getGoosewebSnapshot().entities.sessionDetails[key]?.cwd, CWD,
  "later detail authority cannot mutate the coherent cache while the source is frozen");
assert.equal(visibleOwnershipProjection().detailCwd, undefined,
  "later detail authority cannot leak into the visible projection while frozen");
const repairRequestId = latestRequestId();
const publicationsBeforeRecovery = repairPublications;
socket!.receive(gapFilled("gap-filled", 6n));
socket!.receive(sourceResync("source-resync", 6n, 6n));
assert.equal(latestRequestId(), repairRequestId,
  "source resync retains the current bounded repair request generation");
socket!.receive(detailSnapshot(
  "post-resync-detail",
  repairRequestId,
  7n,
  6n,
  false,
  body("/repaired/workspace", "repaired-worktree", "/repaired/tree")
));
socket!.receive(sessionSummaryPatch("post-resync-summary", 8n, 6n));
await flush();
assert.equal(repairPublications, publicationsBeforeRecovery + 1,
  "gap fill, resync, detail, and summary publish once in one bounded drain");
assert.equal(getGoosewebSnapshot().connection, "connected", repairErrors.join("\n"));
assert.deepEqual(repairErrors, []);
assert.equal(getGoosewebSnapshot().staleSources[SOURCE], undefined);
assert.deepEqual(ownershipProjection(), {
  detailCwd: "/repaired/workspace",
  detailWorktreeId: "repaired-worktree",
  sessionCwd: "/repaired/workspace",
  sourceId: SOURCE,
  sessionId: SESSION
}, "current-generation detail restores selected-session ownership without reload");
assert.equal(getGoosewebSnapshot().entities.sessions[key]?.worktreePath, "/repaired/tree");
assert.equal(getGoosewebSnapshot().entities.sessions[key]?.contextRemainingPercent, 42,
  "same-drain summary metadata enriches rather than replaces the detail projection");
await repairCore.handleMessage({ type: "disconnect" });

const detailUpsert = {
  operation: "upsert" as const,
  domain: "sessionDetails" as const,
  entityIds: [key],
  authoritative: true,
  payload: { [key]: liveDetail }
};
const detailRemove = {
  operation: "remove" as const,
  domain: "sessionDetails" as const,
  entityIds: [key],
  authoritative: true,
  payload: {}
};
const replaceDetailsEmpty = {
  operation: "replace" as const,
  domain: "sessionDetails" as const,
  entityIds: [],
  authoritative: true,
  payload: {}
};
resetGoosewebStoreForTests();
updateGoosewebStore({ entityOperations: [detailUpsert, replaceDetailsEmpty] });
assert.equal(getGoosewebSnapshot().entities.sessions[key], undefined,
  "authoritative replacement excluding a detail withdraws its projection");
updateGoosewebStore({ entityOperations: [replaceDetailsEmpty, detailUpsert] });
assert.equal(getGoosewebSnapshot().entities.sessions[key]?.cwd, CWD,
  "replace then upsert preserves exact ordered reduction");
updateGoosewebStore({ entityOperations: [detailUpsert, detailRemove] });
assert.equal(getGoosewebSnapshot().entities.sessions[key], undefined,
  "upsert then remove preserves exact ordered reduction");

const worktreeKey = sourceEntityKey(SOURCE, WORKTREE);
updateGoosewebStore({ entityOperations: [{
  operation: "upsert", domain: "worktrees", entityIds: [worktreeKey], authoritative: true,
  payload: { [worktreeKey]: {
    sourceId: SOURCE, worktreeId: WORKTREE, path: "/p02/worktree", label: "workspace"
  } }
}, detailUpsert] });
assert.equal(getGoosewebSnapshot().entities.sessions[key]?.worktreePath, "/p02/worktree");
const beforeNonAuthoritative = getGoosewebSnapshot().entities;
const beforeNonAuthoritativeContent = structuredClone(beforeNonAuthoritative);
updateGoosewebStore({ entityOperations: [
  {
    ...detailUpsert,
    authoritative: false,
    payload: { [key]: { ...liveDetail, cwd: "/optimistic/workspace" } }
  },
  {
    operation: "remove", domain: "sessions", entityIds: [key],
    authoritative: false, payload: {}
  },
  {
    operation: "remove", domain: "worktrees", entityIds: [worktreeKey],
    authoritative: false, payload: {}
  }
] });
assert.equal(getGoosewebSnapshot().entities, beforeNonAuthoritative,
  "non-authoritative operations preserve canonical entity identity");
assert.deepEqual(getGoosewebSnapshot().entities, beforeNonAuthoritativeContent,
  "non-authoritative operations preserve every canonical entity domain");
updateGoosewebStore({ entityOperations: [{
  operation: "remove", domain: "worktrees", entityIds: [worktreeKey],
  authoritative: true, payload: {}
}] });
assert.equal(getGoosewebSnapshot().entities.sessions[key]?.worktreePath, "",
  "removing worktree authority withdraws the detail-derived path fallback");

console.log("P09 session detail live/reload ownership converges");

async function connectedCore(
  onPublication?: () => void,
  onError?: (message: string) => void
): Promise<RealtimeWorkerCore> {
  const core = new RealtimeWorkerCore((message: WorkerOutbound) => {
    if (message.type === "state") {
      updateGoosewebStore(message.patch);
      onPublication?.();
    }
    if (message.type === "subscription-state") setSubscription(message.subscription);
    if (message.type === "error") onError?.(message.message);
  });
  await core.handleMessage({
    type: "connect", ticket: "ticket", goosetowerUrl: "ws://p02.invalid/v1/realtime"
  });
  socket!.open();
  socket!.receive(create(RealtimeEnvelopeSchema, {
    protocolVersion: 1,
    messageId: "hello",
    messageKind: MessageKind.HELLO,
    lane: Lane.CRITICAL,
    payload: { case: "hello", value: create(HelloSchema, {
      connectionId: "p09-session-reload",
      heartbeatIntervalMs: 60_000,
      protocolVersion: 1,
      resumeSupported: true,
      gatewayEpoch: GATEWAY,
      gatewayStartedAtUnixNs: 1n
    }) }
  }));
  await core.handleMessage({
    type: "subscribe",
    subscriptionId: "session-detail",
    viewKind: "session_detail",
    filters: { source_id: SOURCE, session_id: SESSION }
  });
  return core;
}

function detailPatch(
  messageId: string,
  gatewaySeq = 4n,
  sourceSeq = 4n,
  operation = ViewOperation.UPSERT,
  detailBody = body()
): RealtimeEnvelope {
  return create(RealtimeEnvelopeSchema, {
    protocolVersion: 1,
    messageId,
    messageKind: MessageKind.PATCH,
    lane: Lane.STATE,
    payload: { case: "patch", value: create(PatchSchema, {
      viewKind: "session_detail",
      schemaVersion: 1,
      operation,
      entity: create(EntityRefSchema, { entityId: SESSION }),
      cursor: cursor(gatewaySeq, sourceSeq),
      coverage: coverage(),
      body: operation === ViewOperation.REMOVE ? new TextEncoder().encode("null") : detailBody
    }) }
  });
}

function detailSnapshot(
  messageId: string,
  requestId: string,
  gatewaySeq = 4n,
  sourceSeq = 4n,
  notFound = false,
  detailBody = body()
): RealtimeEnvelope {
  return create(RealtimeEnvelopeSchema, {
    protocolVersion: 1,
    messageId,
    messageKind: MessageKind.SNAPSHOT,
    lane: Lane.STATE,
    payload: { case: "snapshot", value: create(SnapshotSchema, {
      viewKind: "session_detail",
      subscriptionId: "session-detail",
      requestId,
      schemaVersion: 1,
      operation: ViewOperation.REPLACE,
      notFound,
      cursor: cursor(gatewaySeq, sourceSeq),
      coverage: coverage(),
      body: notFound ? new TextEncoder().encode("null") : detailBody
    }) }
  });
}

function sessionSummaryPatch(
  messageId: string,
  gatewaySeq: bigint,
  sourceSeq: bigint
): RealtimeEnvelope {
  return create(RealtimeEnvelopeSchema, {
    protocolVersion: 1, messageId, messageKind: MessageKind.PATCH, lane: Lane.STATE,
    payload: { case: "patch", value: create(PatchSchema, {
      viewKind: "session_summary", schemaVersion: 1, operation: ViewOperation.UPSERT,
      entity: create(EntityRefSchema, { entityId: SESSION }),
      cursor: cursor(gatewaySeq, sourceSeq),
      coverage: create(ViewCoverageSchema, {
        domains: ["sessions"], entityIds: [SESSION], authoritative: true
      }),
      body: new TextEncoder().encode(JSON.stringify({
        source_id: SOURCE,
        session: {
          id: SESSION, provider: "codex", model: "gpt-5", status: "ready",
          cwd: "/summary/workspace", worktree_path: "/summary/tree", active_turn_id: null,
          metadata: { context_window: {
            remaining_percent: 42, window_tokens: 200_000, used_tokens: 116_000
          } }
        }
      }))
    }) }
  });
}

function gapDetected(
  messageId: string,
  lastSeenSeq: bigint,
  nextAvailableSeq: bigint
): RealtimeEnvelope {
  return create(RealtimeEnvelopeSchema, {
    protocolVersion: 1,
    messageId,
    messageKind: MessageKind.SOURCE_GAP_DETECTED,
    lane: Lane.CRITICAL,
    payload: { case: "sourceGapDetected", value: create(SourceGapDetectedSchema, {
      lastSeen: { sourceId: SOURCE, sourceEpoch: EPOCH, sourceSeq: lastSeenSeq },
      nextAvailable: { sourceId: SOURCE, sourceEpoch: EPOCH, sourceSeq: nextAvailableSeq }
    }) }
  });
}

function gapFilled(messageId: string, sourceSeq: bigint): RealtimeEnvelope {
  return create(RealtimeEnvelopeSchema, {
    protocolVersion: 1,
    messageId,
    messageKind: MessageKind.SOURCE_GAP_FILLED,
    lane: Lane.CRITICAL,
    payload: { case: "sourceGapFilled", value: create(SourceGapFilledSchema, {
      cursor: { sourceId: SOURCE, sourceEpoch: EPOCH, sourceSeq }
    }) }
  });
}

function sourceResync(
  messageId: string,
  gatewaySeq: bigint,
  sourceSeq: bigint
): RealtimeEnvelope {
  return create(RealtimeEnvelopeSchema, {
    protocolVersion: 1,
    messageId,
    messageKind: MessageKind.SOURCE_SNAPSHOT_RESYNC,
    lane: Lane.CRITICAL,
    payload: { case: "sourceSnapshotResync", value: create(SourceSnapshotResyncSchema, {
      sourceId: SOURCE,
      reason: "gap repair fallback",
      schemaVersion: 1,
      cursor: cursor(gatewaySeq, sourceSeq),
      coverage: create(ViewCoverageSchema, {
        domains: [
          "fleet_rows", "sessions", "session_details", "teams", "team_workspaces",
          "approvals", "processes", "worktrees", "sources"
        ],
        authoritative: true
      }),
      body: new TextEncoder().encode(JSON.stringify({ source_id: SOURCE }))
    }) }
  });
}

function cursor(gatewaySeq: bigint, sourceSeq: bigint) {
  return {
    gatewaySeq,
    gatewayEpoch: GATEWAY,
    gatewayStartedAtUnixNs: 1n,
    sources: [{ sourceId: SOURCE, sourceEpoch: EPOCH, sourceSeq }]
  };
}

function coverage() {
  return create(ViewCoverageSchema, {
    domains: ["session_details"],
    entityIds: [SESSION],
    authoritative: true
  });
}

function body(
  cwd = CWD,
  worktreeId = WORKTREE,
  worktreePath: string | null = null
): Uint8Array {
  return new TextEncoder().encode(JSON.stringify({
    source_id: SOURCE,
    session: {
      id: SESSION,
      provider: "codex",
      model: "gpt-5",
      status: "ready",
      cwd,
      worktree_id: worktreeId,
      worktree_path: worktreePath,
      active_turn_id: null
    },
    transcript: [],
    appended_text: "P09 deterministic terminal",
    latest_activity_unix_ms: 1_700_100_000_050
  }));
}

function latestRequestId(): string {
  for (const bytes of [...socket!.sent].reverse()) {
    const envelope = fromBinary(RealtimeEnvelopeSchema, bytes);
    if (envelope.payload.case === "subscribe" &&
      envelope.payload.value.subscriptionId === "session-detail") {
      return envelope.payload.value.requestId;
    }
  }
  throw new Error("missing session-detail subscribe request");
}

function ownershipProjection() {
  const state = getGoosewebSnapshot();
  const key = sourceEntityKey(SOURCE, SESSION);
  const detail = state.entities.sessionDetails[key];
  const session = state.entities.sessions[key];
  return {
    detailCwd: detail?.cwd,
    detailWorktreeId: detail?.worktreeId,
    sessionCwd: session?.cwd,
    sourceId: session?.sourceId,
    sessionId: session?.sessionId
  };
}

function visibleOwnershipProjection() {
  const state = getVisibleGoosewebSnapshot();
  const key = sourceEntityKey(SOURCE, SESSION);
  const detail = state.entities.sessionDetails[key];
  const session = state.entities.sessions[key];
  return {
    detailCwd: detail?.cwd,
    detailWorktreeId: detail?.worktreeId,
    sessionCwd: session?.cwd,
    sourceId: session?.sourceId,
    sessionId: session?.sessionId
  };
}

async function flush(): Promise<void> {
  await new Promise((resolve) => setTimeout(resolve, 24));
}
