import assert from "node:assert/strict";
import { create, fromBinary, toBinary } from "@bufbuild/protobuf";
import { CommandRejectedSchema } from "../src/gen/goosetower/v1/commands_pb";
import { ErrorDetailSchema, Lane, MessageKind } from "../src/gen/goosetower/v1/common_pb";
import { HelloSchema, RealtimeEnvelopeSchema, type RealtimeEnvelope } from "../src/gen/goosetower/v1/realtime_pb";
import { PatchSchema, SnapshotSchema, ViewCoverageSchema, ViewOperation } from "../src/gen/goosetower/v1/view_pb";
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

const SOURCE = "local";
const EPOCH = "p09-normal-replay-epoch";
const GATEWAY = "p09-normal-replay-gateway";
const SESSION = "p09-recovered-lead";
const CWD = "/p09/recovered/workspace";
const WORKTREE = "p09-recovered-worktree";

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
let publications = 0;
const errors: string[] = [];
const core = new RealtimeWorkerCore((message: WorkerOutbound) => {
  if (message.type === "state") {
    publications += 1;
    updateGoosewebStore(message.patch);
  }
  if (message.type === "subscription-state") setSubscription(message.subscription);
  if (message.type === "error") errors.push(message.message);
});
await core.handleMessage({
  type: "connect", ticket: "ticket", goosetowerUrl: "ws://p09.invalid/v1/realtime"
});
socket!.open();
socket!.receive(hello());
for (const subscription of [
  { subscriptionId: "board", viewKind: "board", filters: {} },
  { subscriptionId: "fleet", viewKind: "fleet", filters: {} },
  {
    subscriptionId: "session-detail", viewKind: "session_detail",
    filters: { source_id: SOURCE, session_id: SESSION }
  }
]) await core.handleMessage({ type: "subscribe", ...subscription });

socket!.receive(snapshot("initial-board", "board", 1n, boardBody(false)));
socket!.receive(snapshot("initial-fleet", "fleet", 2n, fleetBody("replaying")));
socket!.receive(snapshot("initial-detail", "session-detail", 3n, json(null), true));
await flush();
assert.equal(getGoosewebSnapshot().entities.sources[sourceEntityKey(SOURCE, SOURCE)]?.health,
  "replaying");
assert.equal(getGoosewebSnapshot().staleSources[SOURCE], undefined);

await core.handleMessage({
  type: "command",
  command: {
    commandId: "start-lead", idempotencyKey: "start-lead", createdAtClientUnixMs: 1n,
    target: { scope: "source", scopeId: SOURCE, entityId: `source:${SOURCE}` },
    payload: { case: "createSession", value: {
      provider: "codex", model: "gpt-5", cwd: CWD, title: "Lead",
      permissionMode: "default", metadata: {}
    } }
  }
});
socket!.receive(commandRejected());
await flush();
const firstRepairRequests = requests();
assert.equal(getGoosewebSnapshot().connection, "stale");
assert.equal(getGoosewebSnapshot().staleSources[SOURCE], "source_stale");
assert.deepEqual(getGoosewebSnapshot().loadedCoverage, {});
assert.equal(getVisibleGoosewebSnapshot().entities.sources[sourceEntityKey(SOURCE, SOURCE)], undefined,
  "source_stale rejection withdraws prior authority before targeted repair");

socket!.receive(snapshot("replaying-board", "board", 4n, boardBody(false)));
socket!.receive(snapshot("replaying-fleet", "fleet", 5n, fleetBody("replaying")));
socket!.receive(snapshot("replaying-detail", "session-detail", 6n, json(null), true));
await flush();
assert.equal(getGoosewebSnapshot().staleSources[SOURCE], "source_stale",
  "bounded snapshots cannot clear command safety while source authority is replaying");

const requestsBeforeSnapshotFloor = requests();
socket!.receive(snapshot("prefloor-live-fleet", "fleet", 7n, fleetBody("live")));
await flush();
assert.deepEqual(requests(), requestsBeforeSnapshotFloor,
  "a Live snapshot from the pre-floor generation cannot rotate repair requests");
assert.equal(getGoosewebSnapshot().staleSources[SOURCE], "source_stale");
assert.equal(getVisibleGoosewebSnapshot().entities.sources[sourceEntityKey(SOURCE, SOURCE)], undefined,
  "a pre-floor Live snapshot remains frozen and cannot become command authority");

socket!.receive(sourceHealthPatch("source-live", 8n, "live"));
await flush();
const liveFloorRequests = requests();
assert.notDeepEqual(liveFloorRequests, firstRepairRequests,
  "current-generation live authority rotates every repair snapshot onto its floor");
assert.equal(getGoosewebSnapshot().staleSources[SOURCE], "source_live");
assert.equal(getVisibleGoosewebSnapshot().entities.sources[sourceEntityKey(SOURCE, SOURCE)], undefined,
  "accepted live-floor authority cannot leak into visible entities while repair remains frozen");

socket!.receive(sourceHealthPatch("duplicate-source-live", 9n, "live"));
await flush();
assert.deepEqual(requests(), liveFloorRequests,
  "a duplicate Live patch cannot rotate the accepted post-floor generation again");
assert.equal(getGoosewebSnapshot().staleSources[SOURCE], "source_live",
  "duplicate Live authority cannot clear stale before post-floor snapshots complete");

const wrongGeneration = snapshot("wrong-generation-board", "board", 10n, boardBody(true));
if (wrongGeneration.payload.case !== "snapshot" || !wrongGeneration.payload.value.cursor) {
  throw new Error("wrong-generation fixture lacks cursor");
}
wrongGeneration.payload.value.cursor.gatewayEpoch = "old-gateway";
socket!.receive(wrongGeneration);
await flush();
assert.equal(getGoosewebSnapshot().staleSources[SOURCE], "source_live");
assert.equal(getGoosewebSnapshot().entities.fleetRows[sourceEntityKey(SOURCE, SESSION)], undefined,
  "old-generation snapshots cannot materialize a repaired session");

const beforeRecoveryPublications = publications;
socket!.receive(snapshot("live-board", "board", 10n, boardBody(true)));
socket!.receive(snapshot("live-detail", "session-detail", 11n, detailBody()));
socket!.receive(snapshot("live-fleet", "fleet", 12n, fleetBody("live")));
await flush();
const recovered = getGoosewebSnapshot();
const sessionKey = sourceEntityKey(SOURCE, SESSION);
assert.equal(publications, beforeRecoveryPublications + 1,
  "one bounded live-floor drain publishes repaired board, detail, and health once");
assert.equal(recovered.connection, "connected", errors.join("\n"));
assert.equal(recovered.staleSources[SOURCE], undefined);
assert.equal(recovered.entities.sources[sourceEntityKey(SOURCE, SOURCE)]?.health, "live");
assert.equal(recovered.entities.fleetRows[sessionKey]?.sessionId, SESSION);
assert.equal(recovered.entities.sessionDetails[sessionKey]?.cwd, CWD);
assert.equal(recovered.entities.sessionDetails[sessionKey]?.worktreeId, WORKTREE);
assert.equal(recovered.entities.sessions[sessionKey]?.worktreePath, `${CWD}/tree`,
  "normal replaying-to-live recovery materializes selected detail without reload");
assert.deepEqual(errors, []);
await core.handleMessage({ type: "disconnect" });

console.log("P09 command rejection replaying-to-live authority repair passed");

function hello(): RealtimeEnvelope {
  return create(RealtimeEnvelopeSchema, {
    protocolVersion: 1, messageId: "hello", messageKind: MessageKind.HELLO, lane: Lane.CRITICAL,
    payload: { case: "hello", value: create(HelloSchema, {
      connectionId: "p09-normal-replay", heartbeatIntervalMs: 60_000,
      protocolVersion: 1, resumeSupported: true,
      gatewayEpoch: GATEWAY, gatewayStartedAtUnixNs: 1n
    }) }
  });
}

function commandRejected(): RealtimeEnvelope {
  return create(RealtimeEnvelopeSchema, {
    protocolVersion: 1, messageId: "source-stale-rejection",
    messageKind: MessageKind.COMMAND_REJECTED, lane: Lane.CRITICAL,
    payload: { case: "commandRejected", value: create(CommandRejectedSchema, {
      commandId: "start-lead", error: create(ErrorDetailSchema, {
        code: "source_stale", message: "runtime source local is stale", retryable: true
      })
    }) }
  });
}

function snapshot(
  messageId: string,
  subscriptionId: "board" | "fleet" | "session-detail",
  gatewaySeq: bigint,
  body: Uint8Array,
  notFound = false
): RealtimeEnvelope {
  const detail = subscriptionId === "session-detail";
  const viewKind = detail ? "session_detail" : subscriptionId;
  return create(RealtimeEnvelopeSchema, {
    protocolVersion: 1, messageId, messageKind: MessageKind.SNAPSHOT, lane: Lane.STATE,
    payload: { case: "snapshot", value: create(SnapshotSchema, {
      viewKind, subscriptionId, requestId: request(subscriptionId), schemaVersion: 1,
      operation: ViewOperation.REPLACE, notFound,
      cursor: cursor(gatewaySeq),
      coverage: create(ViewCoverageSchema, {
        domains: [detail ? "session_details" : subscriptionId === "board" ? "fleet_rows" : "sources"],
        entityIds: detail ? [SESSION] : [], authoritative: true
      }),
      body
    }) }
  });
}

function sourceHealthPatch(messageId: string, gatewaySeq: bigint, state: string): RealtimeEnvelope {
  return create(RealtimeEnvelopeSchema, {
    protocolVersion: 1, messageId, messageKind: MessageKind.PATCH, lane: Lane.CRITICAL,
    payload: { case: "patch", value: create(PatchSchema, {
      viewKind: "source_health", schemaVersion: 1, operation: ViewOperation.UPSERT,
      entity: { entityId: SOURCE }, cursor: cursor(gatewaySeq),
      coverage: create(ViewCoverageSchema, {
        domains: ["sources"], entityIds: [SOURCE], authoritative: true
      }),
      body: fleetBody(state, false)
    }) }
  });
}

function cursor(gatewaySeq: bigint) {
  return {
    gatewaySeq, gatewayEpoch: GATEWAY, gatewayStartedAtUnixNs: 1n,
    sources: [{ sourceId: SOURCE, sourceEpoch: EPOCH, sourceSeq: 5n }]
  };
}

function boardBody(includeSession: boolean): Uint8Array {
  return json({ rows: includeSession ? [{
    source_id: SOURCE, row_id: `${SOURCE}:${SESSION}`, session_id: SESSION,
    provider: "codex", model: "gpt-5", status: "ready", title: "Lead",
    pending_approval_count: 0, latest_activity_unix_ms: 1_700_100_000_050
  }] : [] });
}

function fleetBody(state: string, array = true): Uint8Array {
  const source = {
    source_id: SOURCE, source_epoch: EPOCH, display_name: "Local runtime",
    source_kind: "gooselake-runtime", provisioner_kind: "static", state,
    last_source_seq: 5, observed_at_unix_ms: 1_700_100_000_050,
    active_session_count: state === "live" ? 1 : 0, active_process_count: 0,
    provider_kinds: ["codex"], models: ["gpt-5"], model_capabilities: [],
    supports_worktrees: true, supports_teams: true
  };
  return json(array ? [source] : source);
}

function detailBody(): Uint8Array {
  return json({
    source_id: SOURCE,
    session: {
      id: SESSION, provider: "codex", model: "gpt-5", status: "ready",
      cwd: CWD, worktree_id: WORKTREE, worktree_path: `${CWD}/tree`, active_turn_id: null
    },
    transcript: [], appended_text: "ready", latest_activity_unix_ms: 1_700_100_000_050
  });
}

function requests(): Record<string, string> {
  return Object.fromEntries(["board", "fleet", "session-detail"].map((id) => [id, request(id)]));
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
