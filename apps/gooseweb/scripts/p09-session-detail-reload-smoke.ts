import assert from "node:assert/strict";
import { create, fromBinary, toBinary } from "@bufbuild/protobuf";
import { EntityRefSchema, Lane, MessageKind } from "../src/gen/goosetower/v1/common_pb";
import {
  HelloSchema,
  RealtimeEnvelopeSchema,
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
await reloadCore.handleMessage({ type: "disconnect" });

console.log("P09 session detail live/reload ownership converges");

async function connectedCore(): Promise<RealtimeWorkerCore> {
  const core = new RealtimeWorkerCore((message: WorkerOutbound) => {
    if (message.type === "state") updateGoosewebStore(message.patch);
    if (message.type === "subscription-state") setSubscription(message.subscription);
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

function detailPatch(messageId: string): RealtimeEnvelope {
  return create(RealtimeEnvelopeSchema, {
    protocolVersion: 1,
    messageId,
    messageKind: MessageKind.PATCH,
    lane: Lane.STATE,
    payload: { case: "patch", value: create(PatchSchema, {
      viewKind: "session_detail",
      schemaVersion: 1,
      operation: ViewOperation.UPSERT,
      entity: create(EntityRefSchema, { entityId: SESSION }),
      cursor: cursor(),
      coverage: coverage(),
      body: body()
    }) }
  });
}

function detailSnapshot(messageId: string, requestId: string): RealtimeEnvelope {
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
      cursor: cursor(),
      coverage: coverage(),
      body: body()
    }) }
  });
}

function cursor() {
  return {
    gatewaySeq: 4n,
    gatewayEpoch: GATEWAY,
    gatewayStartedAtUnixNs: 1n,
    sources: [{ sourceId: SOURCE, sourceEpoch: EPOCH, sourceSeq: 4n }]
  };
}

function coverage() {
  return create(ViewCoverageSchema, {
    domains: ["session_details"],
    entityIds: [SESSION],
    authoritative: true
  });
}

function body(): Uint8Array {
  return new TextEncoder().encode(JSON.stringify({
    source_id: SOURCE,
    session: {
      id: SESSION,
      provider: "codex",
      model: "gpt-5",
      status: "ready",
      cwd: CWD,
      worktree_id: WORKTREE,
      worktree_path: null,
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

async function flush(): Promise<void> {
  await new Promise((resolve) => setTimeout(resolve, 24));
}
