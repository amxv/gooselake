import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { resolve } from "node:path";
import { create, toBinary } from "@bufbuild/protobuf";
import { Lane, MessageKind } from "../src/gen/goosetower/v1/common_pb";
import { RealtimeEnvelopeSchema } from "../src/gen/goosetower/v1/realtime_pb";
import { PatchSchema, SnapshotSchema, ViewCoverageSchema, ViewOperation } from "../src/gen/goosetower/v1/view_pb";
import { EntityRefSchema } from "../src/gen/goosetower/v1/common_pb";
import { RealtimeWorkerCore } from "../app/realtime/worker/realtime-command-core";
import { sourceEntityKey } from "../app/realtime/protocol/entities";
import {
  getGoosewebSnapshot, getVisibleGoosewebSnapshot, updateGoosewebStore
} from "../app/stores/gooseweb-store";
import {
  currentSubscriptionRequestId, helloEnvelope, patchEnvelope, pongEnvelope, posted, sessionBodyFor,
  snapshotEnvelope, sockets, sourceResyncEnvelope, waitForPatchFlush
} from "./realtime-worker-smoke-fixtures";

export const core = new RealtimeWorkerCore((message) => {
  posted.push(message);
  if (message.type === "state") updateGoosewebStore(message.patch);
});
await core.handleMessage({
  type: "connect",
  goosetowerUrl: "ws://127.0.0.1:18090/v1/realtime",
  ticket: "first"
});
assert.equal(sockets.length, 1);
sockets[0]?.open();

await core.handleMessage({
  type: "connect",
  goosetowerUrl: "ws://127.0.0.1:18090/v1/realtime",
  ticket: "second"
});
assert.equal(sockets.length, 2);
sockets[1]?.open();
sockets[0]?.closeFromServer();

await waitForPatchFlush();
assert.equal(
  posted.some(
    (message) => message.type === "state" && message.patch.connection === "offline"
  ),
  false
);

sockets[1]?.closeFromServer();
await waitForPatchFlush();
assert.equal(
  posted.some(
    (message) => message.type === "state" && message.patch.connection === "offline"
  ),
  true
);

await core.handleMessage({
  type: "command",
  command: {
    commandId: "cmd_without_socket",
    idempotencyKey: "cmd_without_socket",
    target: {
      scope: "source",
      scopeId: "local",
      entityId: "source:local"
    },
    createdAtClientUnixMs: BigInt(Date.now()),
    payload: {
      case: "createSession",
      value: {
        provider: "codex",
        model: "gpt-5.4",
        cwd: "/tmp",
        title: "Socket unavailable test",
        permissionMode: "",
        metadata: {}
      }
    }
  }
});

assert.equal(
  posted.some(
    (message) =>
      message.type === "command-state" &&
      message.command.commandId === "cmd_without_socket" &&
      message.command.status === "rejected" &&
      message.command.errorCode === "socket_unavailable"
  ),
  true
);

await core.handleMessage({
  type: "connect",
  goosetowerUrl: "ws://127.0.0.1:18090/v1/realtime",
  ticket: "third"
});
assert.equal(sockets.length, 3);
sockets[2]?.open();
sockets[2]?.receive(toBinary(RealtimeEnvelopeSchema, helloEnvelope("gateway-1", 100n)));
await core.handleMessage({
  type: "subscribe", subscriptionId: "subscription-board", viewKind: "board", filters: {}
});
await core.handleMessage({
  type: "subscribe", subscriptionId: "subscription-session_detail",
  viewKind: "session_detail", filters: { session_id: "session-1", source_id: "source-1" }
});
await waitForPatchFlush();
assert.equal(
  posted.some(
    (message) => message.type === "state" && message.patch.connection === "connected"
  ),
  true
);

const nestedCursorSnapshot = snapshotEnvelope({
  messageId: "nested-cursor-snapshot",
  gatewaySeq: 41n,
  viewKind: "board",
  domain: "fleet_rows",
  sources: [
    { sourceId: "source-1", sourceEpoch: "epoch-1", sourceSeq: 17n },
    { sourceId: "source-2", sourceEpoch: "epoch-2", sourceSeq: 9n }
  ],
  body: new TextEncoder().encode(JSON.stringify({ rows: [] }))
});
sockets[2]?.receive(toBinary(RealtimeEnvelopeSchema, nestedCursorSnapshot));
await waitForPatchFlush();
assert.equal(posted.some((message) =>
  message.type === "state" &&
  message.patch.cursor?.sourceCursors["source-1"]?.sourceSeq === 17n
), true, "Worker must consume the canonical nested production cursor");
assert.equal(posted.some((message) =>
  message.type === "state" &&
  message.patch.cursor?.sourceCursors["source-2"]?.sourceSeq === 9n
), true, "Worker must atomically retain every source cursor");

export const selectedBody = new TextEncoder().encode(JSON.stringify({
  source_id: "source-1",
  session: { id: "session-1", provider: "codex", status: "ready" },
  transcript: [{ role: "assistant", text: "reloaded answer" }],
  appended_text: "",
  latest_activity_unix_ms: 200
}));
const selectedAtEqualAuthority = snapshotEnvelope({
  messageId: "selected-at-equal-authority",
  gatewaySeq: 41n,
  viewKind: "session_detail",
  domain: "session_details",
  entityIds: ["session-1"],
  sources: [{ sourceId: "source-1", sourceEpoch: "epoch-1", sourceSeq: 17n }],
  body: selectedBody
});
const detailOperationsBefore = posted.flatMap((message) =>
  message.type === "state" ? message.patch.entityOperations ?? [] : []
).length;
sockets[2]?.receive(toBinary(RealtimeEnvelopeSchema, selectedAtEqualAuthority));
await waitForPatchFlush();
const detailOperationsAfter = posted.flatMap((message) =>
  message.type === "state" ? message.patch.entityOperations ?? [] : []
).length;
assert.equal(detailOperationsAfter, detailOperationsBefore + 1,
  "equal source authority must not suppress a new scoped snapshot");
sockets[2]?.receive(toBinary(RealtimeEnvelopeSchema, selectedAtEqualAuthority));
await waitForPatchFlush();
assert.equal(posted.flatMap((message) =>
  message.type === "state" ? message.patch.entityOperations ?? [] : []
).length, detailOperationsAfter, "duplicate publication identity must be idempotent");

const staleAndFresh = snapshotEnvelope({
  messageId: "stale-and-fresh-vector",
  viewKind: "board",
  domain: "fleet_rows",
  sources: [
    { sourceId: "source-1", sourceEpoch: "epoch-1", sourceSeq: 16n },
    { sourceId: "source-2", sourceEpoch: "epoch-2", sourceSeq: 10n }
  ],
  body: new TextEncoder().encode(JSON.stringify({ rows: [] }))
});
sockets[2]?.receive(toBinary(RealtimeEnvelopeSchema, staleAndFresh));
await waitForPatchFlush();
assert.equal(posted.some((message) =>
  message.type === "state" &&
  message.patch.cursor?.sourceCursors["source-2"]?.sourceSeq === 10n
), false, "a mixed stale/fresh vector must not advance partially");

const reversedVector = snapshotEnvelope({
  messageId: "reversed-vector",
  gatewaySeq: 41n,
  viewKind: "board",
  domain: "fleet_rows",
  sources: [
    { sourceId: "source-2", sourceEpoch: "epoch-2", sourceSeq: 9n },
    { sourceId: "source-1", sourceEpoch: "epoch-1", sourceSeq: 17n }
  ],
  body: new TextEncoder().encode(JSON.stringify({ rows: [] }))
});
const operationsBeforeReverse = posted.flatMap((message) =>
  message.type === "state" ? message.patch.entityOperations ?? [] : []
).length;
sockets[2]?.receive(toBinary(RealtimeEnvelopeSchema, reversedVector));
await waitForPatchFlush();
assert.equal(posted.flatMap((message) =>
  message.type === "state" ? message.patch.entityOperations ?? [] : []
).length, operationsBeforeReverse + 1, "cursor vector order must not affect applicability");

const malformedAt18 = snapshotEnvelope({
  messageId: "malformed-at-18",
  gatewaySeq: 42n,
  viewKind: "session_detail",
  domain: "session_details",
  entityIds: ["session-1"],
  sources: [{ sourceId: "source-1", sourceEpoch: "epoch-1", sourceSeq: 18n }],
  body: new TextEncoder().encode("{}")
});
sockets[2]?.receive(toBinary(RealtimeEnvelopeSchema, malformedAt18));
await waitForPatchFlush();
const validAt18 = snapshotEnvelope({
  messageId: "valid-at-18",
  gatewaySeq: 42n,
  viewKind: "session_detail",
  domain: "session_details",
  entityIds: ["session-1"],
  sources: [{ sourceId: "source-1", sourceEpoch: "epoch-1", sourceSeq: 18n }],
  body: selectedBody
});
sockets[2]?.receive(toBinary(RealtimeEnvelopeSchema, validAt18));
await waitForPatchFlush();
assert.equal(posted.some((message) =>
  message.type === "state" &&
  message.patch.cursor?.sourceCursors["source-1"]?.sourceSeq === 18n
), true, "malformed detail must not advance the cursor before a valid same-authority frame");

await core.handleMessage({
  type: "subscribe", subscriptionId: "retired-session", viewKind: "session_detail",
  filters: { session_id: "session-1", source_id: "source-1" }
});
const retiredRequestId = currentSubscriptionRequestId("retired-session");
await core.handleMessage({ type: "unsubscribe", subscriptionId: "retired-session" });
const cursorBeforeInvalidProvenance = getGoosewebSnapshot().cursor.gatewaySeq;
const invalidProvenanceFrames = [
  snapshotEnvelope({
    messageId: "missing-provenance", gatewaySeq: 43n, viewKind: "session_detail",
    domain: "session_details", entityIds: ["session-1"], subscriptionId: "",
    requestId: "", sources: [{ sourceId: "source-1", sourceEpoch: "epoch-1", sourceSeq: 19n }],
    body: selectedBody
  }),
  snapshotEnvelope({
    messageId: "unknown-provenance", gatewaySeq: 43n, viewKind: "session_detail",
    domain: "session_details", entityIds: ["session-1"], subscriptionId: "unknown-session",
    requestId: "unknown-request", sources: [{ sourceId: "source-1", sourceEpoch: "epoch-1", sourceSeq: 19n }],
    body: selectedBody
  }),
  snapshotEnvelope({
    messageId: "retired-provenance", gatewaySeq: 43n, viewKind: "session_detail",
    domain: "session_details", entityIds: ["session-1"], subscriptionId: "retired-session",
    requestId: retiredRequestId, sources: [{ sourceId: "source-1", sourceEpoch: "epoch-1", sourceSeq: 19n }],
    body: selectedBody
  }),
  snapshotEnvelope({
    messageId: "old-request-provenance", gatewaySeq: 43n, viewKind: "session_detail",
    domain: "session_details", entityIds: ["session-1"],
    requestId: "superseded-request", sources: [{ sourceId: "source-1", sourceEpoch: "epoch-1", sourceSeq: 19n }],
    body: selectedBody
  }),
  snapshotEnvelope({
    messageId: "wrong-view-provenance", gatewaySeq: 43n, viewKind: "board",
    domain: "fleet_rows", subscriptionId: "subscription-session_detail",
    requestId: currentSubscriptionRequestId("subscription-session_detail"),
    sources: [{ sourceId: "source-1", sourceEpoch: "epoch-1", sourceSeq: 19n }],
    body: new TextEncoder().encode(JSON.stringify({ rows: [] }))
  })
];
for (const frame of invalidProvenanceFrames) {
  sockets[2]?.receive(toBinary(RealtimeEnvelopeSchema, frame));
}
await waitForPatchFlush();
assert.equal(getGoosewebSnapshot().cursor.gatewaySeq, cursorBeforeInvalidProvenance,
  "invalid subscription provenance must not advance or persist cursor authority");
const correctedAt19 = snapshotEnvelope({
  messageId: "missing-provenance", gatewaySeq: 43n, viewKind: "session_detail",
  domain: "session_details", entityIds: ["session-1"],
  sources: [{ sourceId: "source-1", sourceEpoch: "epoch-1", sourceSeq: 19n }],
  body: selectedBody
});
sockets[2]?.receive(toBinary(RealtimeEnvelopeSchema, correctedAt19));
await waitForPatchFlush();
assert.equal(getGoosewebSnapshot().cursor.sourceCursors["source-1"]?.sourceSeq, 19n,
  "corrected same-ID/same-authority snapshot applies after provenance rejection");

const operationsBeforeMissingAuthority = posted.flatMap((message) =>
  message.type === "state" ? message.patch.entityOperations ?? [] : []
).length;
const invalidAuthoritySnapshots = [
  create(RealtimeEnvelopeSchema, {
    protocolVersion: 1,
    messageId: "missing-cursor",
    messageKind: MessageKind.SNAPSHOT,
    lane: Lane.STATE,
    payload: { case: "snapshot", value: create(SnapshotSchema, {
      viewKind: "session_detail",
      schemaVersion: 1,
      operation: ViewOperation.REPLACE,
      coverage: create(ViewCoverageSchema, {
        domains: ["session_details"], entityIds: ["session-1"], authoritative: true
      }),
      body: selectedBody
    }) }
  }),
  snapshotEnvelope({
    messageId: "empty-sources",
    gatewaySeq: 43n,
    viewKind: "session_detail",
    domain: "session_details",
    entityIds: ["session-1"],
    sources: [],
    body: selectedBody
  }),
  snapshotEnvelope({
    messageId: "zero-source-seq",
    gatewaySeq: 43n,
    viewKind: "session_detail",
    domain: "session_details",
    entityIds: ["session-1"],
    sources: [{ sourceId: "source-1", sourceEpoch: "epoch-1", sourceSeq: 0n }],
    body: selectedBody
  }),
  snapshotEnvelope({
    messageId: "blank-source-epoch",
    gatewaySeq: 43n,
    viewKind: "session_detail",
    domain: "session_details",
    entityIds: ["session-1"],
    sources: [{ sourceId: "source-1", sourceEpoch: "", sourceSeq: 19n }],
    body: selectedBody
  })
];
for (const invalid of invalidAuthoritySnapshots) {
  sockets[2]?.receive(toBinary(RealtimeEnvelopeSchema, invalid));
}
const missingCursorPatch = create(RealtimeEnvelopeSchema, {
  protocolVersion: 1,
  messageId: "patch-missing-cursor",
  messageKind: MessageKind.PATCH,
  lane: Lane.STATE,
  gatewaySeq: 43n,
  payload: { case: "patch", value: create(PatchSchema, {
    viewKind: "session_detail",
    schemaVersion: 1,
    operation: ViewOperation.REPLACE,
    entity: create(EntityRefSchema, { entityId: "session-1" }),
    coverage: create(ViewCoverageSchema, {
      domains: ["session_details"], entityIds: ["session-1"], authoritative: true
    }),
    body: selectedBody
  }) }
});
sockets[2]?.receive(toBinary(RealtimeEnvelopeSchema, missingCursorPatch));
await waitForPatchFlush();
assert.equal(posted.flatMap((message) =>
  message.type === "state" ? message.patch.entityOperations ?? [] : []
).length, operationsBeforeMissingAuthority,
"versioned frames without complete canonical authority must not mutate entities");

const sessionSiblingFrames = [
  patchEnvelope({
    messageId: "session-fleet-19", gatewaySeq: 44n, sourceSeq: 19n,
    viewKind: "fleet_board", domain: "fleet_rows", entityId: "session-1",
    operation: ViewOperation.UPSERT,
    body: new TextEncoder().encode(JSON.stringify({
      source_id: "source-1", row_id: "session-1", session_id: "session-1",
      provider: "codex", status: "ready", latest_activity_unix_ms: 201
    }))
  }),
  patchEnvelope({
    messageId: "session-summary-19", gatewaySeq: 45n, sourceSeq: 19n,
    viewKind: "session_summary", domain: "sessions", entityId: "session-1",
    operation: ViewOperation.UPSERT,
    body: new TextEncoder().encode(JSON.stringify({
      source_id: "source-1",
      session: { id: "session-1", provider: "codex", status: "ready" }
    }))
  }),
  patchEnvelope({
    messageId: "session-detail-19", gatewaySeq: 46n, sourceSeq: 19n,
    viewKind: "session_detail", domain: "session_details", entityId: "session-1",
    operation: ViewOperation.REPLACE, body: selectedBody
  })
];
const operationsBeforeSiblings = posted.flatMap((message) =>
  message.type === "state" ? message.patch.entityOperations ?? [] : []
).length;
for (const frame of sessionSiblingFrames) sockets[2]?.receive(toBinary(RealtimeEnvelopeSchema, frame));
await waitForPatchFlush();
const operationsAfterSiblings = posted.flatMap((message) =>
  message.type === "state" ? message.patch.entityOperations ?? [] : []
).length;
assert.equal(operationsAfterSiblings, operationsBeforeSiblings + 3,
  "distinct gateway publications at one source cursor must all apply");
for (const frame of sessionSiblingFrames) sockets[2]?.receive(toBinary(RealtimeEnvelopeSchema, frame));
await waitForPatchFlush();
assert.equal(posted.flatMap((message) =>
  message.type === "state" ? message.patch.entityOperations ?? [] : []
).length, operationsAfterSiblings, "exact sibling replays must apply zero operations");

const emptyTeamBody = new TextEncoder().encode(JSON.stringify({
  source_id: "source-1",
  team: { id: "team-1", name: "Team", lead_agent_id: "session-1" },
  members: [], messages: [], deliveries: []
}));
const teamSiblingFrames = [
  patchEnvelope({
    messageId: "team-summary-20", gatewaySeq: 47n, sourceSeq: 20n,
    viewKind: "team_summary", domain: "teams", entityId: "team-1",
    operation: ViewOperation.UPSERT, body: emptyTeamBody
  }),
  patchEnvelope({
    messageId: "team-workspace-20", gatewaySeq: 48n, sourceSeq: 20n,
    viewKind: "team_workspace", domain: "team_workspaces", entityId: "team-1",
    operation: ViewOperation.REPLACE, body: emptyTeamBody
  })
];
const operationsBeforeTeamSiblings = posted.flatMap((message) =>
  message.type === "state" ? message.patch.entityOperations ?? [] : []
).length;
for (const frame of teamSiblingFrames) sockets[2]?.receive(toBinary(RealtimeEnvelopeSchema, frame));
await waitForPatchFlush();
assert.equal(posted.flatMap((message) =>
  message.type === "state" ? message.patch.entityOperations ?? [] : []
).length, operationsBeforeTeamSiblings + 2,
"team summary/workspace siblings at equal source authority must both apply");
await core.handleMessage({
  type: "subscribe", subscriptionId: "subscription-team_workspace",
  viewKind: "team_workspace", filters: { team_id: "team-1", source_id: "source-1" }
});
const activateTeamSubscription = snapshotEnvelope({
  messageId: "activate-team-subscription", gatewaySeq: 48n,
  viewKind: "team_workspace", domain: "team_workspaces", entityIds: ["team-1"],
  sources: [{ sourceId: "source-1", sourceEpoch: "epoch-1", sourceSeq: 20n }],
  body: emptyTeamBody
});
sockets[2]?.receive(toBinary(RealtimeEnvelopeSchema, activateTeamSubscription));
await waitForPatchFlush();

const highGatewayPatch = patchEnvelope({
  messageId: "pre-restart-high-gateway", gatewaySeq: 100n, sourceSeq: 21n,
  viewKind: "session_detail", domain: "session_details", entityId: "session-1",
  operation: ViewOperation.REPLACE, body: selectedBody
});
sockets[2]?.receive(toBinary(RealtimeEnvelopeSchema, highGatewayPatch));
await waitForPatchFlush();
assert.equal(getGoosewebSnapshot().cursor.gatewaySeq, 100n);

const epochMismatchPatch = patchEnvelope({
  messageId: "epoch-mismatch-patch", gatewaySeq: 101n, sourceSeq: 1n,
  sourceEpoch: "epoch-new", viewKind: "session_detail", domain: "session_details",
  entityId: "session-1", operation: ViewOperation.REPLACE, body: selectedBody
});
const operationsBeforeEpochMismatch = posted.flatMap((message) =>
  message.type === "state" ? message.patch.entityOperations ?? [] : []
).length;
sockets[2]?.receive(toBinary(RealtimeEnvelopeSchema, epochMismatchPatch));
await waitForPatchFlush();
assert.equal(posted.flatMap((message) =>
  message.type === "state" ? message.patch.entityOperations ?? [] : []
).length, operationsBeforeEpochMismatch, "patch epoch mismatch must not mutate state");
const scopedEpochSnapshot = snapshotEnvelope({
  messageId: "scoped-epoch-snapshot", gatewaySeq: 1n,
  viewKind: "session_detail", domain: "session_details", entityIds: ["session-1"],
  sources: [{ sourceId: "source-1", sourceEpoch: "epoch-new", sourceSeq: 1n }],
  body: selectedBody
});
sockets[2]?.receive(toBinary(RealtimeEnvelopeSchema, scopedEpochSnapshot));
await waitForPatchFlush();
assert.equal(posted.flatMap((message) =>
  message.type === "state" ? message.patch.entityOperations ?? [] : []
).length, operationsBeforeEpochMismatch,
"a scoped snapshot must not establish a new global source epoch");
assert.equal(getGoosewebSnapshot().cursor.sourceCursors["source-1"]?.sourceEpoch, "epoch-1");

export const sourceReplacementRecord: any = JSON.parse(readFileSync(resolve(
  import.meta.dir,
  "../../../verification/gooseweb/fixtures/p08-source-replacement-rust.json"
), "utf8"));
sourceReplacementRecord.source_id = "source-1";
export const sourceReplacementBody = new TextEncoder().encode(JSON.stringify(sourceReplacementRecord));
const epochResync = sourceResyncEnvelope({
  messageId: "epoch-source-resync", gatewaySeq: 1n,
  gatewayEpoch: "gateway-2", gatewayStartedAtUnixNs: 2n,
  sourceEpoch: "epoch-1", sourceSeq: 21n, body: sourceReplacementBody
});
const operationsBeforeWrongHelloResync = posted.flatMap((message) =>
  message.type === "state" ? message.patch.entityOperations ?? [] : []
).length;
sockets[2]?.receive(toBinary(RealtimeEnvelopeSchema, epochResync));
await waitForPatchFlush();
assert.equal(posted.flatMap((message) =>
  message.type === "state" ? message.patch.entityOperations ?? [] : []
).length, operationsBeforeWrongHelloResync,
"generation-changing resync must match the authenticated current Hello");
await core.handleMessage({
  type: "subscribe",
  subscriptionId: "reset-board-window",
  viewKind: "board",
  filters: { source_id: "source-1", offset: "0", limit: "100" }
});
await core.handleMessage({
  type: "subscribe",
  subscriptionId: "reset-approval-window",
  viewKind: "approval_inbox",
  filters: { source_id: "source-1", include_resolved: "false" }
});
await core.handleMessage({
  type: "subscribe", subscriptionId: "failed-process", viewKind: "process",
  filters: { process_id: "missing-process" }
});
for (const activation of [
  snapshotEnvelope({
    messageId: "activate-reset-board", subscriptionId: "reset-board-window",
    gatewaySeq: 100n, viewKind: "board", domain: "fleet_rows",
    sources: [{ sourceId: "source-1", sourceEpoch: "epoch-1", sourceSeq: 21n }],
    body: new TextEncoder().encode(JSON.stringify({ rows: [] }))
  }),
  snapshotEnvelope({
    messageId: "activate-reset-approval", subscriptionId: "reset-approval-window",
    gatewaySeq: 100n, viewKind: "approval_inbox", domain: "approvals",
    sources: [{ sourceId: "source-1", sourceEpoch: "epoch-1", sourceSeq: 21n }],
    body: new TextEncoder().encode(JSON.stringify({ approvals: [] }))
  })
]) {
  sockets[2]?.receive(toBinary(RealtimeEnvelopeSchema, activation));
}
await waitForPatchFlush();
const preseededFleetRows = Object.fromEntries(Array.from({ length: 101 }, (_, index) => [
  sourceEntityKey("source-1", `old-row-${index}`),
  {
    rowId: `old-row-${index}`,
    sourceId: "source-1",
    sessionId: `old-session-${index}`,
    provider: "codex",
    status: "ready",
    pendingApprovalCount: 0,
    latestActivityUnixMs: 1n
  }
]));
updateGoosewebStore({ entities: { fleetRows: preseededFleetRows } });
const entitiesBeforeOwnershipReset = structuredClone(getGoosewebSnapshot().entities);
await core.handleMessage({
  type: "connect",
  goosetowerUrl: "ws://127.0.0.1:18090/v1/realtime",
  ticket: "tower-restarted"
});
assert.equal(sockets.length, 4);
sockets[3]?.open();
sockets[3]?.receive(toBinary(RealtimeEnvelopeSchema, helloEnvelope("gateway-2", 2n)));
sockets[3]?.receive(toBinary(RealtimeEnvelopeSchema, epochResync));
await waitForPatchFlush();
assert.equal(getGoosewebSnapshot().cursor.gatewaySeq, 1n,
  "an explicit epoch reset must rebase the restarted Tower gateway watermark");
assert.equal(getGoosewebSnapshot().cursor.gatewayEpoch, "gateway-2");
assert.equal(getGoosewebSnapshot().cursor.sourceCursors["source-1"]?.sourceEpoch, "epoch-1");
assert.equal(getGoosewebSnapshot().lastError, undefined,
  "authenticated reconnect plus required resync must clear obsolete generation errors");
assert.deepEqual(getGoosewebSnapshot().entities, entitiesBeforeOwnershipReset,
  "ownership reset must preserve presentation data while marking its coverage unloaded");
assert.deepEqual(
  getGoosewebSnapshot().invalidatedSourceDomains["source-1"],
  ["fleetRows", "sessions", "sessionDetails", "teams", "teamWorkspaces",
    "approvals", "processes", "worktrees", "sources"],
  "reset alone must not represent unloaded source domains as authoritative emptiness"
);
assert.equal(getGoosewebSnapshot().connection, "connected",
  "an authoritative full-source reset clears stale safety while leaving old coverage invalidated");
sockets[3]?.receive(toBinary(RealtimeEnvelopeSchema, pongEnvelope("pong-during-pending-reset", 777n)));
await waitForPatchFlush();
assert.equal(getGoosewebSnapshot().connection, "connected",
  "PONG must not change the post-reset connection state");
await core.handleMessage({ type: "unsubscribe", subscriptionId: "subscription-board" });
assert.equal(getGoosewebSnapshot().connection, "connected",
  "unsubscribe does not alter the completed authoritative reset state");
const deletedSelectedSession = snapshotEnvelope({
  messageId: "deleted-selected-session",
  subscriptionId: "subscription-session_detail",
  gatewaySeq: 1n,
  viewKind: "session_detail",
  domain: "session_details",
  entityIds: ["session-1"],
  sources: [{ sourceId: "source-1", sourceEpoch: "epoch-1", sourceSeq: 21n }],
  body: new TextEncoder().encode("null"),
  gatewayEpoch: "gateway-2",
  gatewayStartedAtUnixNs: 2n,
  notFound: true
});
sockets[3]?.receive(toBinary(RealtimeEnvelopeSchema, deletedSelectedSession));
const deletedSelectedTeam = snapshotEnvelope({
  messageId: "deleted-selected-team",
  subscriptionId: "subscription-team_workspace",
  gatewaySeq: 1n,
  viewKind: "team_workspace",
  domain: "team_workspaces",
  entityIds: ["team-1"],
  sources: [{ sourceId: "source-1", sourceEpoch: "epoch-1", sourceSeq: 21n }],
  body: new TextEncoder().encode("null"),
  gatewayEpoch: "gateway-2",
  gatewayStartedAtUnixNs: 2n,
  notFound: true
});
sockets[3]?.receive(toBinary(RealtimeEnvelopeSchema, deletedSelectedTeam));
await waitForPatchFlush();
assert.equal(getVisibleGoosewebSnapshot().entities.sessionDetails[
  sourceEntityKey("source-1", "session-1")
], undefined,
  "explicit selected-entity absence retires stale detail without claiming malformed empty body");
assert.equal(getVisibleGoosewebSnapshot().entities.teamWorkspaces[
  sourceEntityKey("source-1", "team-1")
], undefined,
  "explicit selected-team absence retires stale Team Comms and recovery requirement");
assert.equal(Object.keys(getVisibleGoosewebSnapshot().entities.fleetRows).length, 0,
  "production read model must hide unloaded fleet rows");
assert.equal(Object.keys(getVisibleGoosewebSnapshot().entities.sessionDetails).length, 0,
  "production read model must hide unloaded session detail");
assert.equal(Object.keys(getVisibleGoosewebSnapshot().entities.teamWorkspaces).length, 0,
  "production read model must hide unloaded Team Comms");
assert.equal(Object.keys(getVisibleGoosewebSnapshot().entities.processes).length, 0,
  "production read model must hide unloaded processes");
assert.equal(Object.keys(getVisibleGoosewebSnapshot().entities.worktrees).length, 0,
  "production read model must hide unloaded worktrees");

const sourceTwoReplacementBody = new TextEncoder().encode(JSON.stringify({
  source_id: "source-2"
}));
const sourceTwoResync = sourceResyncEnvelope({
  messageId: "epoch-source-resync-source-2", gatewaySeq: 2n,
  gatewayEpoch: "gateway-2", gatewayStartedAtUnixNs: 2n,
  sourceId: "source-2", sourceEpoch: "epoch-2", sourceSeq: 9n,
  body: sourceTwoReplacementBody
});
const operationsBeforeSecondSource = posted.flatMap((message) =>
  message.type === "state" ? message.patch.entityOperations ?? [] : []
).length;
sockets[3]?.receive(toBinary(RealtimeEnvelopeSchema, sourceTwoResync));
await waitForPatchFlush();
assert.equal(posted.flatMap((message) =>
  message.type === "state" ? message.patch.entityOperations ?? [] : []
).length, operationsBeforeSecondSource,
"ownership resets must invalidate coverage without claiming empty entity replacements");
assert.deepEqual(
  getGoosewebSnapshot().invalidatedSourceDomains["source-2"],
  ["fleetRows", "sessions", "sessionDetails", "teams", "teamWorkspaces",
    "approvals", "processes", "worktrees", "sources"],
  "each source reset must record every consumed domain as unloaded"
);
sockets[3]?.receive(toBinary(RealtimeEnvelopeSchema, epochResync));
sockets[3]?.receive(toBinary(RealtimeEnvelopeSchema, sourceTwoResync));
await waitForPatchFlush();
assert.equal(posted.flatMap((message) =>
  message.type === "state" ? message.patch.entityOperations ?? [] : []
).length, operationsBeforeSecondSource,
"exact replay of each source reset publication must be suppressed");
for (const frame of [
  { ...deletedSelectedSession, messageId: "deleted-selected-session-after-all-resets", gatewaySeq: 2n },
  { ...deletedSelectedTeam, messageId: "deleted-selected-team-after-all-resets", gatewaySeq: 2n }
]) {
  sockets[3]?.receive(toBinary(RealtimeEnvelopeSchema, frame));
}
await waitForPatchFlush();

const boundedBoardRows = Array.from({ length: 100 }, (_, index) => ({
  row_id: `fresh-row-${index}`,
  source_id: "source-1",
  session_id: `fresh-session-${index}`,
  provider: "codex",
  model: null,
  status: "ready",
  title: null,
  team_id: null,
  worktree_path: null,
  pending_approval_count: 0,
  latest_activity_unix_ms: index + 1
}));
const boundedBoardRefill = snapshotEnvelope({
  messageId: "bounded-board-refill",
  subscriptionId: "reset-board-window",
  gatewaySeq: 1n,
  viewKind: "board",
  domain: "fleet_rows",
  sources: [{ sourceId: "source-1", sourceEpoch: "epoch-1", sourceSeq: 21n }],
  body: new TextEncoder().encode(JSON.stringify({ rows: boundedBoardRows })),
  gatewayEpoch: "gateway-2",
  gatewayStartedAtUnixNs: 2n
});
sockets[3]?.receive(toBinary(RealtimeEnvelopeSchema, boundedBoardRefill));
await waitForPatchFlush();
assert.equal(getGoosewebSnapshot().connection, "connected",
  "bounded refill preserves the completed reset while exposing only exact coverage");
assert.equal(Object.keys(getGoosewebSnapshot().entities.fleetRows).length > 100, true,
  "a 100-row window must not make a 101-row preseeded source appear to contain only 100 rows");
assert.ok(getGoosewebSnapshot().entities.fleetRows[sourceEntityKey("source-1", "old-row-100")],
  "out-of-window presentation data remains available but explicitly unloaded");
assert.equal(Object.keys(getVisibleGoosewebSnapshot().entities.fleetRows).length, 100,
  "production read model exposes only the exact loaded board window");
assert.equal(getVisibleGoosewebSnapshot().entities.fleetRows[
  sourceEntityKey("source-1", "old-row-100")
], undefined,
  "out-of-window stale row must not be presented or actionable");
assert.equal(
  getGoosewebSnapshot().loadedCoverage["source-1:fleetRows:reset-board-window"]?.entityIds.length,
  100,
  "bounded board refill must record its exact loaded window"
);
assert.equal(
  getGoosewebSnapshot().invalidatedSourceDomains["source-1"]?.includes("fleetRows"),
  true,
  "out-of-window fleet coverage must remain explicitly unloaded"
);
const emptyApprovalRefill = snapshotEnvelope({
  messageId: "empty-approval-window-refill",
  subscriptionId: "reset-approval-window",
  gatewaySeq: 1n,
  viewKind: "approval_inbox",
  domain: "approvals",
  sources: [{ sourceId: "source-1", sourceEpoch: "epoch-1", sourceSeq: 21n }],
  body: new TextEncoder().encode(JSON.stringify({ approvals: [] })),
  gatewayEpoch: "gateway-2",
  gatewayStartedAtUnixNs: 2n
});
sockets[3]?.receive(toBinary(RealtimeEnvelopeSchema, emptyApprovalRefill));
await waitForPatchFlush();
assert.equal(getGoosewebSnapshot().connection, "connected",
  "connection becomes live only after all required active coverage snapshots arrive");
assert.equal(
  getGoosewebSnapshot().loadedCoverage["source-1:approvals:reset-approval-window"]?.empty,
  true,
  "a valid empty snapshot is authoritative empty only for its exact approval window"
);
assert.equal(
  getGoosewebSnapshot().invalidatedSourceDomains["source-1"]?.includes("worktrees"),
  true,
  "unsubscribed domains must remain explicitly unloaded for later navigation refill"
);

const newEpochPatch = patchEnvelope({
  messageId: "new-epoch-patch", gatewaySeq: 3n, sourceSeq: 22n,
  gatewayEpoch: "gateway-2", gatewayStartedAtUnixNs: 2n,
  sourceEpoch: "epoch-1", viewKind: "session_detail", domain: "session_details",
  entityId: "session-1", operation: ViewOperation.REPLACE, body: selectedBody
});
sockets[3]?.receive(toBinary(RealtimeEnvelopeSchema, newEpochPatch));
await waitForPatchFlush();
assert.equal(posted.some((message) =>
    message.type === "state" &&
  message.patch.cursor?.gatewayEpoch === "gateway-2" &&
  message.patch.cursor?.sourceCursors["source-1"]?.sourceSeq === 22n
), true, "snapshot resync must establish the new epoch for later patches");
const sameEpochLowSnapshot = snapshotEnvelope({
  messageId: "same-epoch-low-snapshot", gatewaySeq: 2n,
  viewKind: "session_detail", domain: "session_details", entityIds: ["session-1"],
  gatewayEpoch: "gateway-2", gatewayStartedAtUnixNs: 2n,
  sources: [{ sourceId: "source-1", sourceEpoch: "epoch-1", sourceSeq: 22n }],
  body: selectedBody
});
sockets[3]?.receive(toBinary(RealtimeEnvelopeSchema, sameEpochLowSnapshot));
await waitForPatchFlush();
assert.equal(getGoosewebSnapshot().cursor.gatewaySeq, 3n,
  "an ordinary same-epoch snapshot must not regress gateway authority");

assert.ok(getVisibleGoosewebSnapshot().entities.sessionDetails[
  sourceEntityKey("source-1", "session-1")
],
  "a current patch must make a previously not-found detail visible");
