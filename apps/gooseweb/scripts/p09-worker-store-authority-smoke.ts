import assert from "node:assert/strict";
import { create, fromBinary, toBinary } from "@bufbuild/protobuf";
import { Lane, MessageKind } from "../src/gen/goosetower/v1/common_pb";
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
import {
  CURSOR_CACHE_NAMESPACE,
  isNewGatewayGeneration,
  mergeCursorVector,
  parsePersistedCursorState
} from "../app/realtime/cursors";
import { sourceEntityKey } from "../app/realtime/protocol/entities";
import { RealtimeWorkerCore } from "../app/realtime/worker/realtime-command-core";
import { mergeStorePatch } from "../app/realtime/worker/store-patch-batcher";
import {
  getGoosewebSnapshot,
  getVisibleGoosewebSnapshot,
  resetGoosewebStoreForTests,
  setSubscription,
  updateGoosewebStore
} from "../app/stores/gooseweb-store";
import type {
  EntityDomain,
  EntityOperation,
  WorkerOutbound
} from "../app/realtime/types";

const domains: readonly EntityDomain[] = [
  "fleetRows", "sessions", "sessionDetails", "teams", "teamWorkspaces",
  "approvals", "processes", "worktrees", "sources"
];

for (const domain of domains) {
  resetGoosewebStoreForTests();
  updateGoosewebStore({
    entityOperations: [
      operation(domain, "first", entity(domain, "first")),
      operation(domain, "second", entity(domain, "second"))
    ]
  });
  assert.deepEqual(Object.keys(getGoosewebSnapshot().entities[domain]), ["first", "second"],
    `${domain} must retain two same-drain siblings`);
}

resetGoosewebStoreForTests();
updateGoosewebStore({ entityOperations: [{
  operation: "replace", domain: "sessions", entityIds: [], authoritative: true,
  payload: { old: entity("sessions", "old"), kept: entity("sessions", "kept") }
}] });
updateGoosewebStore({ entityOperations: [
  operation("sessions", "new", entity("sessions", "new")),
  { operation: "remove", domain: "sessions", entityIds: ["old"], authoritative: true, payload: {} }
] });
assert.deepEqual(Object.keys(getGoosewebSnapshot().entities.sessions), ["kept", "new"],
  "replace then upsert then remove must preserve lifecycle order");

const originalWorkspace = {
  teamId: "team-1", sourceId: "source-1",
  messages: [{
    id: "old-message", teamId: "team-1", scope: "broadcast", senderAgentId: "a",
    recipientAgentIds: ["b"], text: "old", createdAtUnixMs: 1
  }],
  deliveries: [{
    id: "old-delivery", messageId: "old-message", teamId: "team-1",
    recipientAgentId: "b", provider: "codex", status: "pending", updatedAtUnixMs: 1
  }]
};
const replacementWorkspace = {
  ...originalWorkspace,
  messages: [{ ...originalWorkspace.messages[0]!, id: "new-message", text: "new" }],
  deliveries: []
};
resetGoosewebStoreForTests();
updateGoosewebStore({ entityOperations: [{
  operation: "replace", domain: "teamWorkspaces", entityIds: ["team-1"],
  authoritative: true, payload: { "team-1": originalWorkspace }
}, {
  operation: "replace", domain: "teamWorkspaces", entityIds: ["team-1"],
  authoritative: true, payload: { "team-1": replacementWorkspace }
}] });
assert.deepEqual(getGoosewebSnapshot().entities.teamWorkspaces["team-1"]?.messages
  .map((message) => message.id), ["new-message"]);
assert.deepEqual(getGoosewebSnapshot().entities.teamWorkspaces["team-1"]?.deliveries, [],
  "authoritative nested arrays replace absent covered entries");

updateGoosewebStore({ staleSourceOperations: [{
  operation: "add", sourceIds: ["source-1"], reasons: { "source-1": "gap" }
}] });
assert.deepEqual(getGoosewebSnapshot().staleSources, { "source-1": "gap" });
updateGoosewebStore({ staleSourceOperations: [{
  operation: "remove", sourceIds: ["source-1"], reasons: {}
}] });
assert.deepEqual(getGoosewebSnapshot().staleSources, {});
updateGoosewebStore({ staleSourceOperations: [{
  operation: "replace", sourceIds: ["source-2"], reasons: { "source-2": "resync" }
}] });
assert.deepEqual(getGoosewebSnapshot().staleSources, { "source-2": "resync" });

const beforeOverlay = getGoosewebSnapshot().entities;
updateGoosewebStore({ pendingCommands: {
  optimistic: {
    commandId: "optimistic", idempotencyKey: "optimistic", status: "sent",
    createdAtUnixMs: 1
  }
} });
assert.equal(getGoosewebSnapshot().entities, beforeOverlay,
  "optimistic command overlays cannot mutate authoritative entities");

const mergedPatch = mergeStorePatch(
  { entities: { sessions: { one: entity("sessions", "one") as never } } },
  { entities: { sessions: { two: entity("sessions", "two") as never } } }
);
assert.deepEqual(Object.keys(mergedPatch.entities?.sessions ?? {}), ["one", "two"],
  "bounded drain deep-merges compatibility entity siblings");

assert.match(CURSOR_CACHE_NAMESPACE, /authority-v2$/);
assert.throws(() => parsePersistedCursorState(JSON.stringify({
  gatewaySeq: "99", sourceCursors: {}
})), /incompatible/);
const persisted = parsePersistedCursorState(JSON.stringify({
  schema: "gooseweb-cursor/v2", gatewaySeq: "8", gatewayEpoch: "gateway-old",
  gatewayStartedAtUnixNs: "1",
  sourceCursors: { source: { sourceId: "source", sourceEpoch: "epoch-old", sourceSeq: "4" } }
}));
assert.equal(isNewGatewayGeneration(persisted, "gateway-new", 2n), true);
const restarted = mergeCursorVector(persisted, 1n, [{
  sourceId: "source", sourceEpoch: "epoch-new", sourceSeq: 1n
}], {
  replaceGateway: true, gatewayEpoch: "gateway-new", gatewayStartedAtUnixNs: 2n
});
assert.equal(restarted.gatewaySeq, 1n);
assert.equal(restarted.gatewayEpoch, "gateway-new",
  "new gateway generation accepts sequence below the persisted generation floor");

class FakeSocket {
  static readonly OPEN = 1;
  readyState = 0;
  bufferedAmount = 0;
  binaryType = "";
  onopen: (() => void) | null = null;
  onmessage: ((event: { data: ArrayBuffer }) => void) | null = null;
  onerror: (() => void) | null = null;
  onclose: ((event: { code: number; reason: string }) => void) | null = null;
  readonly sent: Uint8Array[] = [];
  constructor(readonly url: string) { sockets.push(this); }
  send(value: Uint8Array) { this.sent.push(value); }
  close() { this.readyState = 3; }
  open() { this.readyState = FakeSocket.OPEN; this.onopen?.(); }
  receive(envelope: RealtimeEnvelope) {
    const bytes = toBinary(RealtimeEnvelopeSchema, envelope);
    this.onmessage?.({ data: bytes.buffer.slice(bytes.byteOffset, bytes.byteOffset + bytes.byteLength) });
  }
}

const sockets: FakeSocket[] = [];
Object.assign(globalThis, { WebSocket: FakeSocket });
const posted: WorkerOutbound[] = [];
let publications = 0;
resetGoosewebStoreForTests();
const core = new RealtimeWorkerCore((message) => {
  posted.push(message);
  if (message.type === "state") {
    publications += 1;
    updateGoosewebStore(message.patch);
  }
  if (message.type === "subscription-state") setSubscription(message.subscription);
});
await core.handleMessage({
  type: "connect", ticket: "ticket", goosetowerUrl: "ws://127.0.0.1:18090/v1/realtime"
});
const socket = sockets[0]!;
socket.open();
socket.receive(hello("gateway-1", 1n));
await flush();
await core.handleMessage({
  type: "subscribe", subscriptionId: "session-detail", viewKind: "session_detail",
  filters: { source_id: "source-1", session_id: "session-a" }
});
const productionViews = productionRepairViews();
for (const view of productionViews) {
  await core.handleMessage({
    type: "subscribe",
    subscriptionId: view.subscriptionId,
    viewKind: view.viewKind,
    filters: view.filters
  });
}
const initialRequestId = latestSubscribeRequestId(socket, "session-detail");
socket.receive(snapshot("initial", initialRequestId, 1n, 4n, "session-a", "initial"));
for (const view of productionViews) {
  socket.receive(boundedSnapshot(
    `initial-${view.subscriptionId}`,
    view,
    latestSubscribeRequestId(socket, view.subscriptionId),
    1n,
    { sourceId: "source-1", sourceEpoch: "epoch-1", sourceSeq: 4n }
  ));
}
await flush();
assert.equal(getGoosewebSnapshot().subscriptions["session-detail"]?.status, "active",
  "subscription activates only after its valid snapshot");

const beforeSiblingPublications = publications;
socket.receive(patchFrame("sibling-a", 2n, 5n, "session-a", "updated-a"));
socket.receive(patchFrame("sibling-b", 3n, 5n, "session-b", "updated-b"));
await flush();
assert.equal(publications, beforeSiblingPublications + 1,
  "one bounded drain emits exactly one publication");
assert.equal(detail("session-a")?.appendedText, "updated-a");
assert.equal(detail("session-b")?.appendedText, "updated-b",
  "ordered same-frame siblings survive one publication");

updateGoosewebStore({ entityOperations: [
  operation("sessions", "source-1::session-a", {
    sourceId: "source-1", sessionId: "session-a", provider: "codex", status: "ready"
  }),
  operation("sessions", "source-1::session-b", {
    sourceId: "source-1", sessionId: "session-b", provider: "codex", status: "ready"
  }),
  operation("teamWorkspaces", "source-1::team-a", {
    sourceId: "source-1", teamId: "team-a", messages: [], deliveries: []
  }),
  operation("teamWorkspaces", "source-1::team-b", {
    sourceId: "source-1", teamId: "team-b", messages: [], deliveries: []
  }),
  operation("processes", "source-1::process-a", {
    sourceId: "source-1", processId: "process-a", status: "running"
  }),
  operation("processes", "source-1::process-b", {
    sourceId: "source-1", processId: "process-b", status: "running"
  }),
  operation("worktrees", "source-1::worktree-a", {
    sourceId: "source-1", worktreeId: "worktree-a", path: "/tmp/a"
  }),
  operation("worktrees", "source-1::worktree-b", {
    sourceId: "source-1", worktreeId: "worktree-b", path: "/tmp/b"
  })
] });
const coherentBeforeGap = structuredClone(getGoosewebSnapshot().entities);
socket.receive(patchFrame("jump", 4n, 7n, "session-a", "must-not-apply"));
await flush();
assert.deepEqual(getGoosewebSnapshot().entities, coherentBeforeGap,
  "same-epoch source jump freezes before entity mutation");
assert.equal(getGoosewebSnapshot().staleSources["source-1"], "source_cursor_gap");
assert.equal(getGoosewebSnapshot().subscriptions["session-detail"]?.status, "subscribing");
assert.equal(Object.keys(getVisibleGoosewebSnapshot().entities.sessions).length, 0);
assert.equal(Object.keys(getVisibleGoosewebSnapshot().entities.teamWorkspaces).length, 0);
assert.equal(Object.keys(getVisibleGoosewebSnapshot().entities.processes).length, 0);
assert.equal(Object.keys(getVisibleGoosewebSnapshot().entities.worktrees).length, 0,
  "same-epoch repair hides every uncovered source-owned domain before refill");
const repairRequestId = latestSubscribeRequestId(socket, "session-detail");
socket.receive(boundedSnapshot(
  "other-source-board-repair",
  productionViews[0]!,
  latestSubscribeRequestId(socket, productionViews[0]!.subscriptionId),
  4n,
  { sourceId: "source-2", sourceEpoch: "epoch-2", sourceSeq: 1n }
));
await flush();
assert.equal(getGoosewebSnapshot().staleSources["source-1"], "source_cursor_gap",
  "a valid bounded snapshot for another source cannot satisfy this source repair");

socket.receive(create(RealtimeEnvelopeSchema, {
  protocolVersion: 1, messageId: "gap-filled", messageKind: MessageKind.SOURCE_GAP_FILLED,
  lane: Lane.CONTROL, payload: { case: "sourceGapFilled", value: {
    cursor: { sourceId: "source-1", sourceEpoch: "epoch-1", sourceSeq: 7n }
  } }
}));
await flush();
assert.equal(getGoosewebSnapshot().staleSources["source-1"], "source_cursor_gap",
  "gap-filled signal cannot clear safety before authoritative repair");

socket.receive(snapshot("repair", repairRequestId, 5n, 7n, "session-a", "repaired"));
for (const view of productionViews) {
  socket.receive(boundedSnapshot(
    `repair-${view.subscriptionId}`,
    view,
    latestSubscribeRequestId(socket, view.subscriptionId),
    5n,
    { sourceId: "source-1", sourceEpoch: "epoch-1", sourceSeq: 7n }
  ));
}
await flush();
assert.equal(getGoosewebSnapshot().staleSources["source-1"], undefined,
  "gap fill plus every current bounded repair generation clears source safety");
assert.equal(getGoosewebSnapshot().connection, "connected");
assert.equal(getGoosewebSnapshot().subscriptions["session-detail"]?.status, "active");
assert.equal(detail("session-a")?.appendedText, "repaired");
assert.equal(getVisibleGoosewebSnapshot().entities.sessionDetails[
  sourceEntityKey("source-1", "session-b")
], undefined, "unrepaired sibling detail remains hidden");
assert.ok(getVisibleGoosewebSnapshot().entities.teamWorkspaces["source-1::team-a"]);
assert.equal(getVisibleGoosewebSnapshot().entities.teamWorkspaces["source-1::team-b"], undefined);
assert.ok(getVisibleGoosewebSnapshot().entities.processes["source-1::process-a"]);
assert.equal(getVisibleGoosewebSnapshot().entities.processes["source-1::process-b"], undefined);
assert.equal(Object.keys(getVisibleGoosewebSnapshot().entities.worktrees).length, 0,
  "bounded repair exposes selected coverage while uncovered siblings remain non-actionable");
assert.ok(getGoosewebSnapshot().entities.sessions["source-1::session-b"]);
assert.ok(getGoosewebSnapshot().entities.teamWorkspaces["source-1::team-b"]);
assert.ok(getGoosewebSnapshot().entities.processes["source-1::process-b"]);
assert.deepEqual(getGoosewebSnapshot().entities.worktrees, coherentBeforeGap.worktrees);
assert.equal(Object.keys(getVisibleGoosewebSnapshot().entities.sessions).length, 0);
assert.equal(Object.keys(getVisibleGoosewebSnapshot().entities.worktrees).length, 0,
  "successful bounded replay does not expose domains without repaired coverage");

const authorityBeforeMalformed = getGoosewebSnapshot();
socket.receive(patchFrame("unknown-operation", 6n, 8n, "session-a", "bad", {
  operation: ViewOperation.UNSPECIFIED
}));
socket.receive(patchFrame("unknown-version", 6n, 8n, "session-a", "bad", {
  schemaVersion: 2
}));
socket.receive(patchFrame("malformed", 6n, 8n, "session-a", "bad", {
  body: new TextEncoder().encode("{")
}));
await flush();
assert.equal(getGoosewebSnapshot().cursor.gatewaySeq, authorityBeforeMalformed.cursor.gatewaySeq);
assert.equal(detail("session-a")?.appendedText, "repaired",
  "unknown operation/version/malformed bodies fail before cursor or store mutation");

await core.handleMessage({ type: "disconnect" });
await flush();
console.log("P09 Worker/store authority smoke passed");

function operation(domain: EntityDomain, id: string, value: unknown): EntityOperation {
  return { operation: "upsert", domain, entityIds: [id], authoritative: true, payload: { [id]: value } };
}

function entity(domain: EntityDomain, id: string): unknown {
  if (domain === "sessionDetails") {
    return { sessionId: id, sourceId: "source", transcript: [], appendedText: id, latestActivityUnixMs: 1 };
  }
  if (domain === "teamWorkspaces") {
    return { teamId: id, sourceId: "source", messages: [], deliveries: [] };
  }
  return { sourceId: "source", id };
}

function hello(gatewayEpoch: string, startedAt: bigint): RealtimeEnvelope {
  return create(RealtimeEnvelopeSchema, {
    protocolVersion: 1, messageId: "hello", messageKind: MessageKind.HELLO, lane: Lane.CONTROL,
    payload: { case: "hello", value: create(HelloSchema, {
      connectionId: "connection", heartbeatIntervalMs: 60_000,
      gatewayEpoch, gatewayStartedAtUnixNs: startedAt
    }) }
  });
}

function coverage(entityId: string) {
  return create(ViewCoverageSchema, {
    domains: ["session_details"], entityIds: [entityId], authoritative: true
  });
}

function cursor(gatewaySeq: bigint, sourceSeq: bigint) {
  return {
    gatewaySeq, gatewayEpoch: "gateway-1", gatewayStartedAtUnixNs: 1n,
    sources: [{ sourceId: "source-1", sourceEpoch: "epoch-1", sourceSeq }]
  };
}

function sessionBody(sessionId: string, text: string): Uint8Array {
  return new TextEncoder().encode(JSON.stringify({
    source_id: "source-1", session: { id: sessionId, provider: "codex", status: "ready" },
    transcript: [], appended_text: text, latest_activity_unix_ms: 1
  }));
}

function snapshot(
  messageId: string,
  requestId: string,
  gatewaySeq: bigint,
  sourceSeq: bigint,
  sessionId: string,
  text: string
): RealtimeEnvelope {
  return create(RealtimeEnvelopeSchema, {
    protocolVersion: 1, messageId, messageKind: MessageKind.SNAPSHOT, lane: Lane.STATE,
    payload: { case: "snapshot", value: create(SnapshotSchema, {
      viewKind: "session_detail", schemaVersion: 1, operation: ViewOperation.REPLACE,
      subscriptionId: "session-detail", requestId, cursor: cursor(gatewaySeq, sourceSeq),
      coverage: coverage(sessionId), body: sessionBody(sessionId, text)
    }) }
  });
}

function patchFrame(
  messageId: string,
  gatewaySeq: bigint,
  sourceSeq: bigint,
  sessionId: string,
  text: string,
  override: { operation?: ViewOperation; schemaVersion?: number; body?: Uint8Array } = {}
): RealtimeEnvelope {
  return create(RealtimeEnvelopeSchema, {
    protocolVersion: 1, messageId, messageKind: MessageKind.PATCH, lane: Lane.STATE,
    payload: { case: "patch", value: create(PatchSchema, {
      viewKind: "session_detail", schemaVersion: override.schemaVersion ?? 1,
      operation: override.operation ?? ViewOperation.UPSERT,
      entity: { entityId: sessionId }, cursor: cursor(gatewaySeq, sourceSeq),
      coverage: coverage(sessionId), body: override.body ?? sessionBody(sessionId, text)
    }) }
  });
}

function latestSubscribeRequestId(socket: FakeSocket, subscriptionId: string): string {
  for (const bytes of [...socket.sent].reverse()) {
    const envelope = fromBinary(RealtimeEnvelopeSchema, bytes);
    if (envelope.payload.case === "subscribe" &&
      envelope.payload.value.subscriptionId === subscriptionId) {
      return envelope.payload.value.requestId;
    }
  }
  throw new Error("subscribe frame missing");
}

type RepairView = {
  readonly subscriptionId: string;
  readonly viewKind: string;
  readonly domain: string;
  readonly entityIds: readonly string[];
  readonly filters: Readonly<Record<string, string>>;
  readonly body: Uint8Array;
};

function boundedSnapshot(
  messageId: string,
  view: RepairView,
  requestId: string,
  gatewaySeq: bigint,
  source: { sourceId: string; sourceEpoch: string; sourceSeq: bigint }
): RealtimeEnvelope {
  return create(RealtimeEnvelopeSchema, {
    protocolVersion: 1, messageId, messageKind: MessageKind.SNAPSHOT, lane: Lane.STATE,
    payload: { case: "snapshot", value: create(SnapshotSchema, {
      viewKind: view.viewKind, schemaVersion: 1, operation: ViewOperation.REPLACE,
      subscriptionId: view.subscriptionId, requestId,
      cursor: {
        gatewaySeq, gatewayEpoch: "gateway-1", gatewayStartedAtUnixNs: 1n,
        sources: [source]
      },
      coverage: create(ViewCoverageSchema, {
        domains: [view.domain], entityIds: [...view.entityIds], authoritative: true
      }),
      body: view.body
    }) }
  });
}

function productionRepairViews(): RepairView[] {
  const json = (value: unknown) => new TextEncoder().encode(JSON.stringify(value));
  return [
    { subscriptionId: "board", viewKind: "board", domain: "fleet_rows", entityIds: [],
      filters: { window: "0:120" }, body: json({ rows: [] }) },
    { subscriptionId: "approvals", viewKind: "approval_inbox", domain: "approvals", entityIds: [],
      filters: { status: "pending" }, body: json({ approvals: [] }) },
    { subscriptionId: "fleet", viewKind: "fleet", domain: "sources", entityIds: [],
      filters: {}, body: json([{ source_id: "source-1", state: "healthy" }]) },
    { subscriptionId: "teams", viewKind: "teams", domain: "teams", entityIds: [],
      filters: {}, body: json({ total_rows: 0, teams: [], cursors: [] }) },
    { subscriptionId: "ledger", viewKind: "ledger", domain: "processes", entityIds: [],
      filters: { window: "0:120" }, body: json({ entities: {} }) },
    { subscriptionId: "team-detail", viewKind: "team_workspace", domain: "team_workspaces",
      entityIds: ["team-a"], filters: { source_id: "source-1", team_id: "team-a" },
      body: json({ source_id: "source-1", team: {
        id: "team-a", name: "Team A", lead_agent_id: "session-a"
      }, members: [], messages: [], deliveries: [] }) },
    { subscriptionId: "process-detail", viewKind: "process_tail", domain: "processes",
      entityIds: ["process-a"], filters: { source_id: "source-1", process_id: "process-a" },
      body: json({ source_id: "source-1", process: {
        source_id: "source-1", process_id: "process-a", status: "running",
        session_id: null, cwd: null, pid: null, command: null, started_at: 1,
        ended_at: null, exit_code: null, signal: null, stdout_bytes: null,
        stderr_bytes: null, stdout_truncated: null, stderr_truncated: null, version: 1
      }, stdout: [], stderr: [], samples: [] }) }
  ];
}

function detail(sessionId: string) {
  return getGoosewebSnapshot().entities.sessionDetails[sourceEntityKey("source-1", sessionId)];
}

async function flush(): Promise<void> {
  await new Promise((resolve) => setTimeout(resolve, 24));
}
