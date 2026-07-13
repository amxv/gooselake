import assert from "node:assert/strict";
import { create, fromBinary, toBinary } from "@bufbuild/protobuf";
import { Lane, MessageKind } from "../src/gen/goosetower/v1/common_pb";
import {
  HelloSchema,
  RealtimeEnvelopeSchema,
  SourceGapFilledSchema,
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
const GATEWAY = "p09-materialized-gap";

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
  send(value: Uint8Array) { this.sent.push(value); }
  close() { this.readyState = 3; }
  open() { this.onopen?.(); }
  receive(envelope: RealtimeEnvelope) {
    const bytes = toBinary(RealtimeEnvelopeSchema, envelope);
    this.onmessage?.({ data: bytes.buffer.slice(bytes.byteOffset, bytes.byteOffset + bytes.byteLength) });
  }
  constructor(readonly url: string) { socket = this; }
}

let socket: FakeSocket | undefined;
Object.assign(globalThis, { WebSocket: FakeSocket });
resetGoosewebStoreForTests();
const posted: WorkerOutbound[] = [];
const core = new RealtimeWorkerCore((message) => {
  posted.push(message);
  if (message.type === "state") updateGoosewebStore(message.patch);
  if (message.type === "subscription-state") setSubscription(message.subscription);
});
await core.handleMessage({
  type: "connect", ticket: "ticket", goosetowerUrl: "ws://p02.invalid/v1/realtime"
});
socket!.open();
socket!.receive(create(RealtimeEnvelopeSchema, {
  protocolVersion: 1, messageId: "hello", messageKind: MessageKind.HELLO, lane: Lane.CRITICAL,
  payload: { case: "hello", value: create(HelloSchema, {
    connectionId: "materialized-gap", heartbeatIntervalMs: 60_000,
    protocolVersion: 1, resumeSupported: true,
    gatewayEpoch: GATEWAY, gatewayStartedAtUnixNs: 1n
  }) }
}));
for (const subscription of [
  { subscriptionId: "board", viewKind: "board", filters: { window: "0:120" } },
  { subscriptionId: "fleet", viewKind: "fleet", filters: {} }
]) await core.handleMessage({ type: "subscribe", ...subscription });

socket!.receive(snapshot("initial-board", "board", request("board"), 1n, 5n, boardBody("initial")));
socket!.receive(snapshot("initial-fleet", "fleet", request("fleet"), 2n, 5n, fleetBody("live")));
await flush();
const initial = structuredClone(getGoosewebSnapshot());
assert.equal(initial.connection, "connected");
assert.equal(Object.keys(getVisibleGoosewebSnapshot().entities.fleetRows).length, 1);

socket!.receive(sourceHealthPatch("gap-health", 3n, 5n, "gap_detected"));
await flush();
const held = getGoosewebSnapshot();
assert.equal(held.connection, "stale");
assert.equal(held.staleSources[SOURCE], "gap_detected");
assert.equal(held.cursor.gatewaySeq, 2n,
  "materialized gap detection freezes before cursor mutation");
assert.deepEqual(held.entities, initial.entities,
  "materialized gap detection freezes before entity mutation");
assert.deepEqual(held.loadedCoverage, {},
  "materialized gap detection invalidates prior visible authority");
assert.equal(Object.keys(getVisibleGoosewebSnapshot().entities.fleetRows).length, 0);
assert.equal(Object.keys(getVisibleGoosewebSnapshot().entities.sources).length, 0,
  "held gaps hide source-owned authority while safety is active");
const heldBoardRequest = request("board");
const heldFleetRequest = request("fleet");

socket!.receive(snapshot("prefill-board", "board", heldBoardRequest, 3n, 5n,
  boardBody("pre-fill")));
socket!.receive(snapshot("prefill-gap-fleet", "fleet", heldFleetRequest, 4n, 5n,
  fleetBody("gap_detected")));
await flush();
assert.equal(getGoosewebSnapshot().cursor.gatewaySeq, 2n);
assert.deepEqual(getGoosewebSnapshot().loadedCoverage, {});
assert.equal(getGoosewebSnapshot().subscriptions.board?.status, "subscribing",
  "snapshots before the unknown filled floor cannot activate or expose coverage");

socket!.receive(create(RealtimeEnvelopeSchema, {
  protocolVersion: 1, messageId: "filled-8", messageKind: MessageKind.SOURCE_GAP_FILLED,
  lane: Lane.CRITICAL,
  payload: { case: "sourceGapFilled", value: create(SourceGapFilledSchema, {
    cursor: { sourceId: SOURCE, sourceEpoch: EPOCH, sourceSeq: 8n }
  }) }
}));
await flush();
assert.equal(getGoosewebSnapshot().connection, "stale");
assert.equal(getGoosewebSnapshot().staleSources[SOURCE], "gap_detected");
assert.notEqual(request("board"), heldBoardRequest);
assert.notEqual(request("fleet"), heldFleetRequest,
  "the filled cursor rotates bounded subscriptions onto its authoritative floor");

socket!.receive(snapshot("repaired-board", "board", request("board"), 5n, 8n,
  boardBody("repaired")));
await flush();
assert.equal(getGoosewebSnapshot().connection, "stale",
  "one repaired domain cannot clear source-wide safety");
socket!.receive(snapshot("repaired-fleet", "fleet", request("fleet"), 6n, 8n,
  fleetBody("live")));
await flush();
const recovered = getGoosewebSnapshot();
assert.equal(recovered.connection, "connected");
assert.equal(recovered.staleSources[SOURCE], undefined);
assert.equal(recovered.cursor.sourceCursors[SOURCE]?.sourceSeq, 8n);
assert.equal(recovered.entities.fleetRows[sourceEntityKey(SOURCE, SESSION)]?.title, "repaired");
assert.equal(recovered.entities.sources[sourceEntityKey(SOURCE, SOURCE)]?.health, "live");
assert.equal(Object.keys(getVisibleGoosewebSnapshot().entities.fleetRows).length, 1);
assert.equal(posted.some((message) => message.type === "error"), false);
await core.handleMessage({ type: "disconnect" });

console.log("P09 materialized held-gap safety and authoritative recovery passed");

function snapshot(
  messageId: string, viewKind: "board" | "fleet", requestId: string,
  gatewaySeq: bigint, sourceSeq: bigint, body: Uint8Array
): RealtimeEnvelope {
  return create(RealtimeEnvelopeSchema, {
    protocolVersion: 1, messageId, messageKind: MessageKind.SNAPSHOT, lane: Lane.STATE,
    payload: { case: "snapshot", value: create(SnapshotSchema, {
      viewKind, subscriptionId: viewKind, requestId, schemaVersion: 1,
      operation: ViewOperation.REPLACE,
      cursor: { gatewaySeq, gatewayEpoch: GATEWAY, gatewayStartedAtUnixNs: 1n,
        sources: [{ sourceId: SOURCE, sourceEpoch: EPOCH, sourceSeq }] },
      coverage: create(ViewCoverageSchema, {
        domains: [viewKind === "board" ? "fleet_rows" : "sources"], authoritative: true
      }), body
    }) }
  });
}

function sourceHealthPatch(
  messageId: string, gatewaySeq: bigint, sourceSeq: bigint, state: string
): RealtimeEnvelope {
  return create(RealtimeEnvelopeSchema, {
    protocolVersion: 1, messageId, messageKind: MessageKind.PATCH, lane: Lane.CRITICAL,
    payload: { case: "patch", value: create(PatchSchema, {
      viewKind: "source_health", schemaVersion: 1, operation: ViewOperation.UPSERT,
      entity: { entityId: SOURCE },
      cursor: { gatewaySeq, gatewayEpoch: GATEWAY, gatewayStartedAtUnixNs: 1n,
        sources: [{ sourceId: SOURCE, sourceEpoch: EPOCH, sourceSeq }] },
      coverage: create(ViewCoverageSchema, {
        domains: ["sources"], entityIds: [SOURCE], authoritative: true
      }), body: fleetBody(state, false)
    }) }
  });
}

function boardBody(title: string): Uint8Array {
  return json({ rows: [{
    source_id: SOURCE, row_id: `${SOURCE}:${SESSION}`, session_id: SESSION,
    provider: "codex", model: "gpt-5", status: "ready", title,
    pending_approval_count: 0, latest_activity_unix_ms: 1_700_100_000_050
  }] });
}

function fleetBody(state: string, array = true): Uint8Array {
  const source = {
    source_id: SOURCE, source_epoch: EPOCH, display_name: "P02 deterministic",
    source_kind: "gooselake-runtime", provisioner_kind: "static", state,
    last_source_seq: 5, observed_at_unix_ms: 1_700_100_000_050,
    active_session_count: 1, active_process_count: 0, provider_kinds: ["codex"],
    models: ["gpt-5"], model_capabilities: [], supports_worktrees: true, supports_teams: true
  };
  return json(array ? [source] : source);
}

function request(subscriptionId: string): string {
  for (const bytes of [...socket!.sent].reverse()) {
    const envelope = fromBinary(RealtimeEnvelopeSchema, bytes);
    if (envelope.payload.case === "subscribe" &&
      envelope.payload.value.subscriptionId === subscriptionId) return envelope.payload.value.requestId;
  }
  throw new Error(`missing subscribe request for ${subscriptionId}`);
}

function json(value: unknown): Uint8Array {
  return new TextEncoder().encode(JSON.stringify(value));
}

async function flush(): Promise<void> {
  await new Promise((resolve) => setTimeout(resolve, 24));
}
