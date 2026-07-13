import assert from "node:assert/strict";
import { create, fromBinary, toBinary } from "@bufbuild/protobuf";
import { Lane, MessageKind } from "../src/gen/goosetower/v1/common_pb";
import { RealtimeEnvelopeSchema } from "../src/gen/goosetower/v1/realtime_pb";
import { ApprovalViewSchema, PatchSchema, SnapshotSchema, ViewCoverageSchema, ViewOperation } from "../src/gen/goosetower/v1/view_pb";
import { EntityRefSchema, SourceCursorSchema } from "../src/gen/goosetower/v1/common_pb";
import { sourceEntityKey } from "../app/realtime/protocol/entities";
import {
  getGoosewebSnapshot, getVisibleGoosewebSnapshot, resetGoosewebStoreForTests
} from "../app/stores/gooseweb-store";
import {
  patchEnvelope, posted, sessionBodyFor, snapshotEnvelope, sockets,
  sourceResyncEnvelope, waitForPatchFlush
} from "./realtime-worker-smoke-fixtures";
import {
  core, selectedBody, sourceReplacementBody, sourceReplacementRecord
} from "./realtime-worker-smoke-authority";

const livePatchFixtures = [
  patchEnvelope({
    messageId: "live-uncovered-row", gatewaySeq: 4n, sourceSeq: 23n,
    gatewayEpoch: "gateway-2", gatewayStartedAtUnixNs: 2n,
    viewKind: "fleet_board", domain: "fleet_rows", entityId: "live-row",
    operation: ViewOperation.UPSERT,
    body: new TextEncoder().encode(JSON.stringify({
      row_id: "live-row", source_id: "source-1", session_id: "live-session",
      provider: "codex", status: "ready", pending_approval_count: 0,
      latest_activity_unix_ms: 300
    }))
  }),
  patchEnvelope({
    messageId: "live-uncovered-team", gatewaySeq: 5n, sourceSeq: 24n,
    gatewayEpoch: "gateway-2", gatewayStartedAtUnixNs: 2n,
    viewKind: "team_summary", domain: "teams", entityId: "team-live",
    operation: ViewOperation.UPSERT,
    body: new TextEncoder().encode(JSON.stringify({
      source_id: "source-1",
      team: { id: "team-live", name: "Live Team", lead_agent_id: "session-1" },
      members: []
    }))
  }),
  patchEnvelope({
    messageId: "live-uncovered-approval", gatewaySeq: 6n, sourceSeq: 25n,
    gatewayEpoch: "gateway-2", gatewayStartedAtUnixNs: 2n,
    viewKind: "approval", domain: "approvals", entityId: "approval-live",
    operation: ViewOperation.UPSERT,
    body: toBinary(ApprovalViewSchema, create(ApprovalViewSchema, {
      sourceId: "source-1", approvalId: "approval-live", sessionId: "session-1",
      status: "pending", risk: "medium", summary: "Approve live patch"
    }))
  }),
  patchEnvelope({
    messageId: "live-uncovered-workspace", gatewaySeq: 7n, sourceSeq: 26n,
    gatewayEpoch: "gateway-2", gatewayStartedAtUnixNs: 2n,
    viewKind: "team_workspace", domain: "team_workspaces", entityId: "team-live",
    operation: ViewOperation.REPLACE,
    body: new TextEncoder().encode(JSON.stringify({
      source_id: "source-1",
      team: { id: "team-live", name: "Live Team", lead_agent_id: "session-1" },
      members: [], messages: [], deliveries: []
    }))
  })
];
for (const frame of livePatchFixtures) {
  sockets[3]?.receive(toBinary(RealtimeEnvelopeSchema, frame));
}
await waitForPatchFlush();
assert.ok(getVisibleGoosewebSnapshot().entities.fleetRows[sourceEntityKey("source-1", "live-row")]);
assert.ok(getVisibleGoosewebSnapshot().entities.teams[sourceEntityKey("source-1", "team-live")]);
assert.ok(getVisibleGoosewebSnapshot().entities.approvals[sourceEntityKey("source-1", "approval-live")]);
assert.ok(getVisibleGoosewebSnapshot().entities.teamWorkspaces[sourceEntityKey("source-1", "team-live")]);

for (const [index, [viewKind, domain, entityId]] of [
  ["fleet_board", "fleet_rows", "live-row"],
  ["team_summary", "teams", "team-live"],
  ["approval", "approvals", "approval-live"],
  ["team_workspace", "team_workspaces", "team-live"],
  ["session_detail", "session_details", "session-1"]
].entries()) {
  sockets[3]?.receive(toBinary(RealtimeEnvelopeSchema, patchEnvelope({
    messageId: `remove-live-${index}`, gatewaySeq: BigInt(8 + index),
    sourceSeq: BigInt(27 + index), gatewayEpoch: "gateway-2", gatewayStartedAtUnixNs: 2n,
    viewKind, domain, entityId, operation: ViewOperation.REMOVE, body: new Uint8Array()
  })));
}
await waitForPatchFlush();
assert.equal(getVisibleGoosewebSnapshot().entities.fleetRows[sourceEntityKey("source-1", "live-row")], undefined);
assert.equal(getVisibleGoosewebSnapshot().entities.teams[sourceEntityKey("source-1", "team-live")], undefined);
assert.equal(getVisibleGoosewebSnapshot().entities.approvals[sourceEntityKey("source-1", "approval-live")], undefined);
assert.equal(getVisibleGoosewebSnapshot().entities.teamWorkspaces[sourceEntityKey("source-1", "team-live")], undefined);
assert.equal(getVisibleGoosewebSnapshot().entities.sessionDetails[sourceEntityKey("source-1", "session-1")], undefined);

await core.handleMessage({
  type: "subscribe", subscriptionId: "process-tail-live", viewKind: "process_tail",
  filters: { process_id: "process-live", source_id: "source-1" }
});
const processTailBody = new TextEncoder().encode(JSON.stringify({
  source_id: "source-1",
  process: {
    source_id: "source-1", process_id: "process-live", status: "running",
    command: ["make", "check"], session_id: null, pid: null, cwd: null,
    started_at: 1, ended_at: null, exit_code: null, signal: null,
    stdout_bytes: null, stderr_bytes: null, stdout_truncated: null,
    stderr_truncated: null, version: 1
  },
  stdout: [], stderr: [], samples: []
}));
const processSnapshot = snapshotEnvelope({
  messageId: "process-tail-live", subscriptionId: "process-tail-live",
  gatewaySeq: 12n, viewKind: "process_tail", domain: "processes",
  entityIds: ["process-live"],
  sources: [{ sourceId: "source-1", sourceEpoch: "epoch-1", sourceSeq: 31n }],
  body: processTailBody, gatewayEpoch: "gateway-2", gatewayStartedAtUnixNs: 2n
});
sockets[3]?.receive(toBinary(RealtimeEnvelopeSchema, processSnapshot));
await waitForPatchFlush();
assert.equal([...posted].reverse().find((message) =>
  message.type === "subscription-state" &&
  message.subscription.subscriptionId === "process-tail-live"
)?.type === "subscription-state" ? ([...posted].reverse().find((message) =>
    message.type === "subscription-state" &&
    message.subscription.subscriptionId === "process-tail-live"
  ) as Extract<WorkerOutbound, { type: "subscription-state" }>).subscription.status : undefined,
"active");
assert.ok(getVisibleGoosewebSnapshot().entities.processes[sourceEntityKey("source-1", "process-live")]);

const cursorBeforeMalformedAbsence = getGoosewebSnapshot().cursor.gatewaySeq;
const wrongSourceProcessBody = new TextEncoder().encode(JSON.stringify({
  source_id: "source-2",
  process: {
    source_id: "source-2", process_id: "process-live", status: "running",
    command: ["wrong"], session_id: null, pid: null, cwd: null,
    started_at: 1, ended_at: null, exit_code: null, signal: null,
    stdout_bytes: null, stderr_bytes: null, stdout_truncated: null,
    stderr_truncated: null, version: 1
  },
  stdout: [], stderr: [], samples: []
}));
sockets[3]?.receive(toBinary(RealtimeEnvelopeSchema, snapshotEnvelope({
  messageId: "process-tail-wrong-source", subscriptionId: "process-tail-live",
  gatewaySeq: 13n, viewKind: "process_tail", domain: "processes",
  entityIds: ["process-live"],
  sources: [{ sourceId: "source-1", sourceEpoch: "epoch-1", sourceSeq: 32n }],
  body: wrongSourceProcessBody, gatewayEpoch: "gateway-2", gatewayStartedAtUnixNs: 2n
})));
sockets[3]?.receive(toBinary(RealtimeEnvelopeSchema, snapshotEnvelope({
  messageId: "duplicate-not-found-id", subscriptionId: "process-tail-live",
  gatewaySeq: 13n, viewKind: "process_tail", domain: "processes",
  entityIds: ["process-live", "process-live"],
  sources: [{ sourceId: "source-1", sourceEpoch: "epoch-1", sourceSeq: 32n }],
  body: new TextEncoder().encode("null"), notFound: true,
  gatewayEpoch: "gateway-2", gatewayStartedAtUnixNs: 2n
})));
sockets[3]?.receive(toBinary(RealtimeEnvelopeSchema, snapshotEnvelope({
  messageId: "nondetail-not-found", subscriptionId: "reset-board-window",
  gatewaySeq: 13n, viewKind: "board", domain: "fleet_rows", entityIds: ["live-row"],
  sources: [{ sourceId: "source-1", sourceEpoch: "epoch-1", sourceSeq: 32n }],
  body: new TextEncoder().encode("null"), notFound: true,
  gatewayEpoch: "gateway-2", gatewayStartedAtUnixNs: 2n
})));
for (const [index, malformed] of [
  { schemaVersion: 0 },
  { schemaVersion: 2 },
  { operation: ViewOperation.REMOVE },
  { domains: ["teams"] },
  { domains: ["processes", "teams"] },
  { body: new TextEncoder().encode("{}") }
].entries()) {
  sockets[3]?.receive(toBinary(RealtimeEnvelopeSchema, snapshotEnvelope({
    messageId: `malformed-not-found-${index}`, subscriptionId: "process-tail-live",
    gatewaySeq: 13n, viewKind: "process_tail", domain: "processes",
    entityIds: ["process-live"],
    sources: [{ sourceId: "source-1", sourceEpoch: "epoch-1", sourceSeq: 32n }],
    body: malformed.body ?? new TextEncoder().encode("null"), notFound: true,
    schemaVersion: malformed.schemaVersion, operation: malformed.operation,
    domains: malformed.domains, gatewayEpoch: "gateway-2", gatewayStartedAtUnixNs: 2n
  })));
}
await waitForPatchFlush();
assert.equal(getGoosewebSnapshot().cursor.gatewaySeq, cursorBeforeMalformedAbsence,
  "malformed not-found frames must not advance canonical authority");
assert.ok(getVisibleGoosewebSnapshot().entities.processes[sourceEntityKey("source-1", "process-live")]);

const missingProcess = snapshotEnvelope({
  messageId: "process-tail-not-found", subscriptionId: "process-tail-live",
  gatewaySeq: 13n, viewKind: "process_tail", domain: "processes",
  entityIds: ["process-live"],
  sources: [{ sourceId: "source-1", sourceEpoch: "epoch-1", sourceSeq: 32n }],
  body: new TextEncoder().encode("null"), notFound: true,
  gatewayEpoch: "gateway-2", gatewayStartedAtUnixNs: 2n
});
sockets[3]?.receive(toBinary(RealtimeEnvelopeSchema, missingProcess));
await waitForPatchFlush();
assert.equal(getVisibleGoosewebSnapshot().entities.processes[
  sourceEntityKey("source-1", "process-live")
], undefined,
  "an exact process-tail not-found snapshot removes only the selected process");

const malformedProcessBodies: unknown[] = [
  { error: "failed" },
  { ...JSON.parse(new TextDecoder().decode(processTailBody)), process: {
    ...JSON.parse(new TextDecoder().decode(processTailBody)).process, status: undefined
  } },
  { ...JSON.parse(new TextDecoder().decode(processTailBody)), process: {
    ...JSON.parse(new TextDecoder().decode(processTailBody)).process, command: undefined
  } },
  { ...JSON.parse(new TextDecoder().decode(processTailBody)), process: {
    ...JSON.parse(new TextDecoder().decode(processTailBody)).process, exit_code: "bad"
  } },
  { ...JSON.parse(new TextDecoder().decode(processTailBody)), stdout: null },
  { ...JSON.parse(new TextDecoder().decode(processTailBody)), samples: [{ stream: "stdout" }] },
  { ...JSON.parse(new TextDecoder().decode(processTailBody)), process: {
    ...JSON.parse(new TextDecoder().decode(processTailBody)).process, process_id: "other-process"
  } }
];
const cursorBeforeMalformedProcess = getGoosewebSnapshot().cursor.gatewaySeq;
for (const [index, body] of malformedProcessBodies.entries()) {
  sockets[3]?.receive(toBinary(RealtimeEnvelopeSchema, snapshotEnvelope({
    messageId: `malformed-process-tail-${index}`, subscriptionId: "process-tail-live",
    gatewaySeq: 14n, viewKind: "process_tail", domain: "processes",
    entityIds: ["process-live"],
    sources: [{ sourceId: "source-1", sourceEpoch: "epoch-1", sourceSeq: 33n }],
    body: new TextEncoder().encode(JSON.stringify(body)),
    gatewayEpoch: "gateway-2", gatewayStartedAtUnixNs: 2n
  })));
}
await waitForPatchFlush();
assert.equal(getGoosewebSnapshot().cursor.gatewaySeq, cursorBeforeMalformedProcess,
  "malformed current process-tail bodies must not advance authority");

await core.handleMessage({
  type: "subscribe", subscriptionId: "collision-teams", viewKind: "teams", filters: {}
});
const strictTeamsBody = {
  teams: [
    { source_id: "source-1", team_id: "team-collision", name: "A", lead_member_id: "a" },
    { source_id: "source-2", team_id: "team-collision", name: "B", lead_member_id: "b" }
  ],
  total_rows: 2,
  cursors: [
    { source_id: "source-1", source_epoch: "epoch-1", source_seq: 33 },
    { source_id: "source-2", source_epoch: "epoch-2", source_seq: 9 }
  ]
};
const malformedTeamsBodies: unknown[] = [
  {}, { error: "failed" }, { teams: null, total_rows: 0, cursors: [] },
  { teams: {}, total_rows: 0, cursors: [] },
  { teams: [{ source_id: "source-1" }], total_rows: 1, cursors: [] },
  { ...strictTeamsBody, teams: [strictTeamsBody.teams[0], strictTeamsBody.teams[0]] },
  { ...strictTeamsBody, teams: [
    { source_id: "source-3", team_id: "rogue", name: "Rogue", lead_member_id: "r" }
  ], total_rows: 1 }
];
const cursorBeforeMalformedTeams = getGoosewebSnapshot().cursor.gatewaySeq;
for (const [index, body] of malformedTeamsBodies.entries()) {
  sockets[3]?.receive(toBinary(RealtimeEnvelopeSchema, snapshotEnvelope({
    messageId: `malformed-teams-${index}`, subscriptionId: "collision-teams",
    gatewaySeq: 14n, viewKind: "teams", domain: "teams",
    sources: [
      { sourceId: "source-1", sourceEpoch: "epoch-1", sourceSeq: 33n },
      { sourceId: "source-2", sourceEpoch: "epoch-2", sourceSeq: 9n }
    ],
    body: new TextEncoder().encode(JSON.stringify(body)),
    gatewayEpoch: "gateway-2", gatewayStartedAtUnixNs: 2n
  })));
}
await waitForPatchFlush();
assert.equal(getGoosewebSnapshot().cursor.gatewaySeq, cursorBeforeMalformedTeams,
  "malformed teams lists must not advance authority");

const collisionTeams = snapshotEnvelope({
  messageId: "collision-teams-valid", subscriptionId: "collision-teams",
  gatewaySeq: 14n, viewKind: "teams", domain: "teams",
  sources: [
    { sourceId: "source-1", sourceEpoch: "epoch-1", sourceSeq: 33n },
    { sourceId: "source-2", sourceEpoch: "epoch-2", sourceSeq: 9n }
  ],
  body: new TextEncoder().encode(JSON.stringify(strictTeamsBody)),
  gatewayEpoch: "gateway-2", gatewayStartedAtUnixNs: 2n
});
sockets[3]?.receive(toBinary(RealtimeEnvelopeSchema, collisionTeams));
await waitForPatchFlush();
assert.equal(getGoosewebSnapshot().entities.teams[sourceEntityKey("source-1", "team-collision")]?.name, "A");
assert.equal(getGoosewebSnapshot().entities.teams[sourceEntityKey("source-2", "team-collision")]?.name, "B");

for (const [index, body] of [
  { source_id: "source-2", team: { id: "rogue", name: "Rogue", lead_agent_id: "b" }, members: [] },
  { team: { id: "rogue", name: "Rogue", lead_agent_id: "b" }, members: [] }
].entries()) {
  sockets[3]?.receive(toBinary(RealtimeEnvelopeSchema, patchEnvelope({
    messageId: `source-mismatch-patch-${index}`, gatewaySeq: 15n, sourceSeq: 34n,
    gatewayEpoch: "gateway-2", gatewayStartedAtUnixNs: 2n,
    viewKind: "team_summary", domain: "teams", entityId: "rogue",
    operation: ViewOperation.UPSERT,
    body: new TextEncoder().encode(JSON.stringify(body))
  })));
}
const multiSourcePatch = patchEnvelope({
  messageId: "multi-source-entity-patch", gatewaySeq: 15n, sourceSeq: 34n,
  gatewayEpoch: "gateway-2", gatewayStartedAtUnixNs: 2n,
  viewKind: "team_summary", domain: "teams", entityId: "rogue",
  operation: ViewOperation.UPSERT,
  body: new TextEncoder().encode(JSON.stringify({
    source_id: "source-1", team: { id: "rogue", name: "Rogue", lead_agent_id: "a" }, members: []
  }))
});
if (multiSourcePatch.payload.case === "patch" && multiSourcePatch.payload.value.cursor) {
  multiSourcePatch.payload.value.cursor.sources.push(create(SourceCursorSchema, {
    sourceId: "source-2", sourceEpoch: "epoch-2", sourceSeq: 9n
  }));
}
sockets[3]?.receive(toBinary(RealtimeEnvelopeSchema, multiSourcePatch));
await waitForPatchFlush();
assert.equal(getGoosewebSnapshot().cursor.gatewaySeq, 14n,
  "body/cursor source mismatch and multi-source entity patches must not advance authority");

sockets[3]?.receive(toBinary(RealtimeEnvelopeSchema, patchEnvelope({
  messageId: "remove-source-a-collision", gatewaySeq: 15n, sourceSeq: 34n,
  gatewayEpoch: "gateway-2", gatewayStartedAtUnixNs: 2n,
  viewKind: "team_summary", domain: "teams", entityId: "team-collision",
  operation: ViewOperation.REMOVE, body: new Uint8Array()
})));
await waitForPatchFlush();
assert.equal(getGoosewebSnapshot().entities.teams[sourceEntityKey("source-1", "team-collision")], undefined);
assert.equal(getGoosewebSnapshot().entities.teams[sourceEntityKey("source-2", "team-collision")]?.name, "B");

const scopedSubscriptions = [
  ["scoped-session-1", "session_detail", { session_id: "scoped-1", source_id: "source-1" }],
  ["scoped-session-2", "session_detail", { session_id: "scoped-2", source_id: "source-1" }],
  ["scoped-team-1", "team_workspace", { team_id: "scoped-team-1", source_id: "source-1" }],
  ["scoped-team-2", "team_workspace", { team_id: "scoped-team-2", source_id: "source-1" }],
  ["scoped-process-1", "process_tail", { process_id: "scoped-process-1", source_id: "source-1" }],
  ["scoped-process-2", "process_tail", { process_id: "scoped-process-2", source_id: "source-1" }]
] as const;
for (const [subscriptionId, viewKind, filters] of scopedSubscriptions) {
  await core.handleMessage({ type: "subscribe", subscriptionId, viewKind, filters });
}
const scopedProcessBody = (processId: string) => new TextEncoder().encode(JSON.stringify({
  source_id: "source-1",
  process: {
    source_id: "source-1", process_id: processId, status: "running", command: ["true"],
    session_id: null, pid: null, cwd: null, started_at: 1, ended_at: null,
    exit_code: null, signal: null, stdout_bytes: null, stderr_bytes: null,
    stdout_truncated: null, stderr_truncated: null, version: 1
  },
  stdout: [], stderr: [], samples: []
}));
const scopedFrames = [
  snapshotEnvelope({
    messageId: "scoped-session-1", subscriptionId: "scoped-session-1", gatewaySeq: 15n,
    viewKind: "session_detail", domain: "session_details", entityIds: ["scoped-1"],
    sources: [{ sourceId: "source-1", sourceEpoch: "epoch-1", sourceSeq: 34n }],
    body: sessionBodyFor("source-1", "scoped-1", "one"),
    gatewayEpoch: "gateway-2", gatewayStartedAtUnixNs: 2n
  }),
  snapshotEnvelope({
    messageId: "scoped-session-2", subscriptionId: "scoped-session-2", gatewaySeq: 15n,
    viewKind: "session_detail", domain: "session_details", entityIds: ["scoped-2"],
    sources: [{ sourceId: "source-1", sourceEpoch: "epoch-1", sourceSeq: 34n }],
    body: sessionBodyFor("source-1", "scoped-2", "two"),
    gatewayEpoch: "gateway-2", gatewayStartedAtUnixNs: 2n
  }),
  ...["scoped-team-1", "scoped-team-2"].map((teamId) => snapshotEnvelope({
    messageId: teamId, subscriptionId: teamId, gatewaySeq: 15n,
    viewKind: "team_workspace", domain: "team_workspaces", entityIds: [teamId],
    sources: [{ sourceId: "source-1", sourceEpoch: "epoch-1", sourceSeq: 34n }],
    body: new TextEncoder().encode(JSON.stringify({
      source_id: "source-1", team: { id: teamId, name: teamId, lead_agent_id: "lead" },
      members: [], messages: [], deliveries: []
    })), gatewayEpoch: "gateway-2", gatewayStartedAtUnixNs: 2n
  })),
  ...["scoped-process-1", "scoped-process-2"].map((processId) => snapshotEnvelope({
    messageId: processId, subscriptionId: processId, gatewaySeq: 15n,
    viewKind: "process_tail", domain: "processes", entityIds: [processId],
    sources: [{ sourceId: "source-1", sourceEpoch: "epoch-1", sourceSeq: 34n }],
    body: scopedProcessBody(processId), gatewayEpoch: "gateway-2", gatewayStartedAtUnixNs: 2n
  }))
];
for (const frame of scopedFrames) sockets[3]?.receive(toBinary(RealtimeEnvelopeSchema, frame));
await waitForPatchFlush();
for (const [domain, ids] of [
  ["sessionDetails", ["scoped-1", "scoped-2"]],
  ["teamWorkspaces", ["scoped-team-1", "scoped-team-2"]],
  ["processes", ["scoped-process-1", "scoped-process-2"]]
] as const) {
  for (const id of ids) assert.ok(getGoosewebSnapshot().entities[domain][sourceEntityKey("source-1", id)]);
}
const scopedSessionOneRefresh = { ...scopedFrames[0]!, messageId: "scoped-session-1-refresh" };
sockets[3]?.receive(toBinary(RealtimeEnvelopeSchema, scopedSessionOneRefresh));
sockets[3]?.receive(toBinary(RealtimeEnvelopeSchema, snapshotEnvelope({
  messageId: "scoped-session-1-remove", subscriptionId: "scoped-session-1", gatewaySeq: 15n,
  viewKind: "session_detail", domain: "session_details", entityIds: ["scoped-1"],
  sources: [{ sourceId: "source-1", sourceEpoch: "epoch-1", sourceSeq: 34n }],
  body: new TextEncoder().encode("null"), notFound: true,
  gatewayEpoch: "gateway-2", gatewayStartedAtUnixNs: 2n
})));
await waitForPatchFlush();
assert.equal(getGoosewebSnapshot().entities.sessionDetails[sourceEntityKey("source-1", "scoped-1")], undefined);
assert.ok(getGoosewebSnapshot().entities.sessionDetails[sourceEntityKey("source-1", "scoped-2")]);
for (const [subscriptionId] of scopedSubscriptions) {
  await core.handleMessage({ type: "unsubscribe", subscriptionId });
}

const legacyPatch = create(RealtimeEnvelopeSchema, {
  protocolVersion: 1, messageId: "legacy-v0-patch", messageKind: MessageKind.PATCH,
  lane: Lane.STATE, gatewaySeq: 16n, sourceId: "source-1", sourceEpoch: "epoch-1",
  sourceSeq: 35n,
  payload: { case: "patch", value: create(PatchSchema, {
    viewKind: "team", schemaVersion: 0, operation: ViewOperation.UNSPECIFIED,
    entity: create(EntityRefSchema, { entityId: "legacy-team" }),
    body: new TextEncoder().encode(JSON.stringify({
      source_id: "source-1",
      team: { id: "legacy-team", name: "Legacy", lead_agent_id: "lead" }, members: []
    }))
  }) }
});
sockets[3]?.receive(toBinary(RealtimeEnvelopeSchema, legacyPatch));
await waitForPatchFlush();
assert.ok(getGoosewebSnapshot().entities.teams[sourceEntityKey("source-1", "legacy-team")],
  "v0 patch must use bounded top-level source authority fallback");
const legacySnapshot = create(RealtimeEnvelopeSchema, {
  protocolVersion: 1, messageId: "legacy-v0-snapshot", messageKind: MessageKind.SNAPSHOT,
  lane: Lane.STATE, gatewaySeq: 17n, sourceId: "source-1", sourceEpoch: "epoch-1",
  sourceSeq: 36n,
  payload: { case: "snapshot", value: create(SnapshotSchema, {
    viewKind: "session", schemaVersion: 0, operation: ViewOperation.UNSPECIFIED,
    body: sessionBodyFor("source-1", "legacy-session", "legacy")
  }) }
});
sockets[3]?.receive(toBinary(RealtimeEnvelopeSchema, legacySnapshot));
await waitForPatchFlush();
assert.ok(getGoosewebSnapshot().entities.sessions[
  sourceEntityKey("source-1", "legacy-session")
], "v0 snapshot alias must use bounded top-level source authority fallback");
const legacyWrongSource = structuredClone(legacyPatch);
legacyWrongSource.messageId = "legacy-v0-wrong-source";
legacyWrongSource.gatewaySeq = 18n;
legacyWrongSource.sourceSeq = 37n;
if (legacyWrongSource.payload.case === "patch") {
  legacyWrongSource.payload.value.body = new TextEncoder().encode(JSON.stringify({
    source_id: "source-2",
    team: { id: "legacy-wrong", name: "Wrong", lead_agent_id: "lead" }, members: []
  }));
  legacyWrongSource.payload.value.entity = create(EntityRefSchema, { entityId: "legacy-wrong" });
}
sockets[3]?.receive(toBinary(RealtimeEnvelopeSchema, legacyWrongSource));
await waitForPatchFlush();
assert.equal(getGoosewebSnapshot().cursor.gatewaySeq, 17n,
  "v0 top-level A authority with B body must fail before mutation");

const largeSourceBody = new TextEncoder().encode(JSON.stringify({ source_id: "source-big" }));
for (const [index, sourceSeq] of [
  9_007_199_254_740_991n,
  9_007_199_254_740_992n,
  9_223_372_036_854_775_807n
].entries()) {
  const reset = sourceResyncEnvelope({
    messageId: `large-source-seq-${index}`,
    gatewaySeq: BigInt(40 + index),
    gatewayEpoch: "gateway-2",
    gatewayStartedAtUnixNs: 2n,
    sourceId: "source-big",
    sourceEpoch: "epoch-big",
    sourceSeq,
    body: largeSourceBody
  });
  sockets[3]?.receive(toBinary(RealtimeEnvelopeSchema, reset));
  await waitForPatchFlush();
  assert.equal(
    getGoosewebSnapshot().cursor.sourceCursors["source-big"]?.sourceSeq,
    sourceSeq,
    "protobuf BigInt source authority must apply without JSON numeric duplication"
  );
}

const invalidResyncRecords = Array.from({ length: 4 }, () => structuredClone(sourceReplacementRecord));
invalidResyncRecords[0]!.source_id = "wrong-source";
invalidResyncRecords[1]!.source_seq = "9007199254740992";
invalidResyncRecords[2]!.source_seq = -1;
invalidResyncRecords[3]!.source_seq = "9.1e15";
const operationsBeforeInvalidResync = posted.flatMap((message) =>
  message.type === "state" ? message.patch.entityOperations ?? [] : []
).length;
for (const [index, invalidRecord] of invalidResyncRecords.entries()) {
  const invalidFrame = sourceResyncEnvelope({
    messageId: `invalid-source-resync-${index}`,
    gatewaySeq: BigInt(7 + index),
    gatewayEpoch: "gateway-2",
    gatewayStartedAtUnixNs: 2n,
    sourceEpoch: "epoch-1",
    sourceSeq: 21n,
    body: new TextEncoder().encode(JSON.stringify(invalidRecord))
  });
  sockets[3]?.receive(toBinary(RealtimeEnvelopeSchema, invalidFrame));
}
await waitForPatchFlush();
assert.equal(getGoosewebSnapshot().cursor.gatewayEpoch, "gateway-2",
  "malformed source replacements must not advance gateway authority");
assert.equal(posted.flatMap((message) =>
  message.type === "state" ? message.patch.entityOperations ?? [] : []
).length, operationsBeforeInvalidResync,
"malformed ownership-reset authority must not mutate the store");

const delayedOldEpochPatch = patchEnvelope({
  messageId: "delayed-old-generation", gatewaySeq: 101n, sourceSeq: 999n,
  gatewayEpoch: "gateway-1", gatewayStartedAtUnixNs: 100n,
  sourceEpoch: "epoch-1", viewKind: "session_detail", domain: "session_details",
  entityId: "session-1", operation: ViewOperation.REPLACE, body: selectedBody
});
const operationsBeforeOldEpoch = posted.flatMap((message) =>
  message.type === "state" ? message.patch.entityOperations ?? [] : []
).length;
sockets[3]?.receive(toBinary(RealtimeEnvelopeSchema, delayedOldEpochPatch));
await waitForPatchFlush();
assert.equal(posted.flatMap((message) =>
  message.type === "state" ? message.patch.entityOperations ?? [] : []
).length, operationsBeforeOldEpoch, "delayed old-epoch patch must not flip authority");
const delayedOldResync = sourceResyncEnvelope({
  messageId: "delayed-old-generation-resync", gatewaySeq: 1n,
  gatewayEpoch: "gateway-1", gatewayStartedAtUnixNs: 100n,
  sourceEpoch: "epoch-1", sourceSeq: 21n, body: sourceReplacementBody
});
sockets[3]?.receive(toBinary(RealtimeEnvelopeSchema, delayedOldResync));
await waitForPatchFlush();
assert.equal(getGoosewebSnapshot().cursor.gatewayEpoch, "gateway-2",
  "a delayed old gateway generation resync must not roll authority back");
assert.equal(posted.flatMap((message) =>
  message.type === "state" ? message.patch.entityOperations ?? [] : []
).length, operationsBeforeOldEpoch, "a delayed old resync must not mutate entities");
sockets[3]?.receive(new Uint8Array([0xff]));
await waitForPatchFlush();
assert.equal(posted.some((message) =>
  message.type === "error" &&
  message.retryable === false &&
  message.message.startsWith("Realtime protocol error:")
), true, "malformed outer frames must fail safely");
const sentBeforeCommand = sockets[3]?.sent.length ?? 0;

await core.handleMessage({
  type: "command",
  command: {
    commandId: "cmd_with_socket",
    idempotencyKey: "cmd_with_socket",
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
        title: "Socket write test",
        permissionMode: "",
        metadata: {}
      }
    }
  }
});

assert.equal((sockets[3]?.sent.length ?? 0) > sentBeforeCommand, true);
const sentCommandFrame = sockets[3]?.sent.at(-1);
assert.ok(sentCommandFrame instanceof Uint8Array);
const sentCommandEnvelope = fromBinary(RealtimeEnvelopeSchema, sentCommandFrame);
assert.equal(sentCommandEnvelope.payload.case, "command");
assert.equal(sentCommandEnvelope.payload.value.payload.case, "createSession");
assert.equal(
  posted.some(
    (message) =>
      message.type === "command-state" &&
      message.command.commandId === "cmd_with_socket" &&
      message.command.status === "sent"
  ),
  true
);

const sentBeforeFallbackCommand = sockets[3]?.sent.length ?? 0;
await core.handleMessage({
  type: "command",
  command: {
    commandId: "cmd_with_fallback",
    idempotencyKey: "cmd_with_fallback",
    createdAtClientUnixMs: BigInt(Date.now()),
    fallbackCreateSession: {
      provider: "codex",
      model: "gpt-5.4",
      cwd: "/tmp",
      title: "Fallback payload test",
      permissionMode: "",
      metadata: {}
    },
    target: {
      scope: "source",
      scopeId: "local",
      entityId: "source:local"
    }
  } as never
});
assert.equal((sockets[3]?.sent.length ?? 0) > sentBeforeFallbackCommand, true);
const fallbackCommandFrame = sockets[3]?.sent.at(-1);
assert.ok(fallbackCommandFrame instanceof Uint8Array);
const fallbackCommandEnvelope = fromBinary(
  RealtimeEnvelopeSchema,
  fallbackCommandFrame
);
assert.equal(fallbackCommandEnvelope.payload.case, "command");
assert.equal(fallbackCommandEnvelope.payload.value.payload.case, "createSession");

const sentBeforeImageTurnCommand = sockets[3]?.sent.length ?? 0;
await core.handleMessage({
  type: "command",
  command: {
    commandId: "cmd_image_turn",
    idempotencyKey: "cmd_image_turn",
    target: {
      scope: "session",
      scopeId: "session_1",
      entityId: "session_1"
    },
    createdAtClientUnixMs: BigInt(Date.now()),
    payload: {
      case: "sendTurn",
      value: {
        sessionId: "session_1",
        text: "Inspect this image",
        input: [
          { type: "text", text: "Inspect this image" },
          {
            type: "image",
            mediaType: "image/png",
            data: "iVBORw0KGgo="
          }
        ]
      }
    }
  }
});
assert.equal((sockets[3]?.sent.length ?? 0) > sentBeforeImageTurnCommand, true);
const imageTurnFrame = sockets[3]?.sent.at(-1);
assert.ok(imageTurnFrame instanceof Uint8Array);
const imageTurnEnvelope = fromBinary(RealtimeEnvelopeSchema, imageTurnFrame);
assert.equal(imageTurnEnvelope.payload.case, "command");
assert.equal(imageTurnEnvelope.payload.value.payload.case, "sendTurn");
const imageTurnPayload = imageTurnEnvelope.payload.value.payload.value;
assert.equal(imageTurnPayload.input.length, 2);
assert.equal(imageTurnPayload.input[1]?.type, "image");
assert.equal(imageTurnPayload.input[1]?.mediaType, "image/png");
assert.equal(imageTurnPayload.input[1]?.data, "iVBORw0KGgo=");

const sentBeforeJoinCommand = sockets[3]?.sent.length ?? 0;
await core.handleMessage({
  type: "command",
  command: {
    commandId: "cmd_join_team_member",
    idempotencyKey: "cmd_join_team_member",
    target: {
      scope: "team",
      scopeId: "team_1",
      entityId: "team_1"
    },
    createdAtClientUnixMs: BigInt(Date.now()),
    payload: {
      case: "joinTeamMember",
      value: {
        teamId: "team_1",
        agentId: "session_2",
        title: "Second agent",
        addedBy: "session_1"
      }
    }
  }
});
assert.equal((sockets[3]?.sent.length ?? 0) > sentBeforeJoinCommand, true);
const joinCommandFrame = sockets[3]?.sent.at(-1);
assert.ok(joinCommandFrame instanceof Uint8Array);
const joinCommandEnvelope = fromBinary(RealtimeEnvelopeSchema, joinCommandFrame);
assert.equal(joinCommandEnvelope.payload.case, "command");
assert.equal(joinCommandEnvelope.payload.value.payload.case, "joinTeamMember");

sockets[3]?.closeFromServer();
await waitForPatchFlush();

resetGoosewebStoreForTests();
