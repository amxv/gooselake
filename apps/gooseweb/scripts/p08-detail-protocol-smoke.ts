import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { create, fromBinary, toBinary } from "@bufbuild/protobuf";
import { RealtimeEnvelopeSchema } from "../src/gen/goosetower/v1/realtime_pb";
import {
  EntityRefSchema,
  Lane,
  MessageKind
} from "../src/gen/goosetower/v1/common_pb";
import {
  PatchSchema,
  SnapshotSchema,
  ViewCoverageSchema,
  ViewOperation
} from "../src/gen/goosetower/v1/view_pb";
import {
  decodePatch,
  decodeSnapshot,
  ProtocolDecodeError
} from "../app/realtime/protocol/entities";
import {
  getGoosewebSnapshot,
  resetGoosewebStoreForTests,
  updateGoosewebStore
} from "../app/stores/gooseweb-store";
import {
  cursorStateToProto,
  mergeCursorVector,
  shouldApplyCursorVector
} from "../app/realtime/cursors";

const encoder = new TextEncoder();
const coverage = (domain: string, entityId: string) => create(ViewCoverageSchema, {
  domains: [domain],
  entityIds: [entityId],
  authoritative: true
});
const cursorA = { sourceId: "A", sourceEpoch: "epoch-A", sourceSeq: 17n };
const cursorB = { sourceId: "B", sourceEpoch: "epoch-B", sourceSeq: 9n };
const vectorState = mergeCursorVector(
  { gatewaySeq: 0n, sourceCursors: {} },
  41n,
  [cursorA, cursorB]
);
assert.deepEqual(
  cursorStateToProto(vectorState).sources.map((source) => source.sourceId).sort(),
  ["A", "B"],
  "resume cursor retains the complete source vector"
);
assert.equal(shouldApplyCursorVector(vectorState, 0n, [cursorB, cursorA], true), true);
assert.equal(shouldApplyCursorVector(vectorState, 0n, [
  { ...cursorA, sourceSeq: 16n },
  { ...cursorB, sourceSeq: 10n }
], true), false);
assert.equal(shouldApplyCursorVector(vectorState, 0n, [cursorA, cursorA], true), false);
assert.equal(shouldApplyCursorVector(vectorState, 0n, [
  { sourceId: "", sourceEpoch: "epoch", sourceSeq: 1n }
], true), false);

const sessionBody = (
  sessionId: string,
  transcript: readonly string[],
  appendedText = ""
) => encoder.encode(JSON.stringify({
  source_id: "source-1",
  session: { id: sessionId, provider: "codex", status: "ready" },
  transcript: transcript.map((text) => ({ role: "assistant", text })),
  appended_text: appendedText,
  latest_activity_unix_ms: 200
}));

const corpus = JSON.parse(readFileSync(
  new URL("../../../verification/gooseweb/fixtures/p08-detail-frame-corpus.json", import.meta.url),
  "utf8"
)) as { frames: Array<{ name: string; producer: "typescript" | "rust"; base64: string }> };
const corpusEnvelope = fromBinary(
  RealtimeEnvelopeSchema,
  Uint8Array.from(Buffer.from(corpus.frames[0]!.base64, "base64"))
);
assert.equal(corpusEnvelope.payload.case, "snapshot");
assert.equal(corpusEnvelope.payload.value.cursor?.sources[0]?.sourceSeq, 17n);
const corpusPatch = decodeSnapshot(corpusEnvelope.payload.value);
assert.equal(corpusPatch.entityOperations[0]?.entityIds[0], "session-1");
const typescriptCorpusFrames = [
  create(RealtimeEnvelopeSchema, {
    protocolVersion: 1,
    messageId: "ts-session-replace",
    messageKind: MessageKind.SNAPSHOT,
    lane: Lane.STATE,
    payload: { case: "snapshot", value: create(SnapshotSchema, {
      viewKind: "session_detail",
      schemaVersion: 1,
      operation: ViewOperation.REPLACE,
      cursor: { sources: [
        { sourceId: "source-1", sourceEpoch: "epoch-1", sourceSeq: 17n },
        { sourceId: "source-2", sourceEpoch: "epoch-2", sourceSeq: 9n }
      ] },
      coverage: coverage("session_details", "session-1"),
      body: sessionBody("session-1", ["reloaded answer"])
    }) }
  }),
  create(RealtimeEnvelopeSchema, {
    protocolVersion: 1,
    messageId: "ts-session-remove",
    messageKind: MessageKind.PATCH,
    lane: Lane.STATE,
    payload: { case: "patch", value: create(PatchSchema, {
      viewKind: "session_detail",
      schemaVersion: 1,
      operation: ViewOperation.REMOVE,
      entity: create(EntityRefSchema, { entityId: "session-1" }),
      cursor: { sources: [{ sourceId: "source-1", sourceEpoch: "epoch-1", sourceSeq: 19n }] },
      coverage: coverage("session_details", "session-1"),
      body: encoder.encode("null")
    }) }
  }),
  create(RealtimeEnvelopeSchema, {
    protocolVersion: 1,
    messageId: "ts-version-skew",
    messageKind: MessageKind.SNAPSHOT,
    lane: Lane.STATE,
    payload: { case: "snapshot", value: create(SnapshotSchema, {
      viewKind: "session_detail",
      schemaVersion: 2,
      operation: ViewOperation.REPLACE,
      coverage: coverage("session_details", "session-1"),
      body: sessionBody("session-1", ["reloaded answer"])
    }) }
  })
];
for (const frame of typescriptCorpusFrames) {
  const fixture = corpus.frames.find((item) => item.name === ({
    "ts-session-replace": "session_replace_multi_source",
    "ts-session-remove": "session_remove",
    "ts-version-skew": "version_skew"
  } as Record<string, string>)[frame.messageId]);
  assert.equal(fixture?.producer, "typescript");
  assert.equal(
    Buffer.from(toBinary(RealtimeEnvelopeSchema, frame)).toString("base64"),
    fixture?.base64,
    `TypeScript encoder drift for ${frame.messageId}`
  );
}
const skewEnvelope = fromBinary(
  RealtimeEnvelopeSchema,
  Buffer.from(corpus.frames.find((item) => item.name === "version_skew")!.base64, "base64")
);
assert.equal(skewEnvelope.payload.case, "snapshot");
assert.throws(() => decodeSnapshot(skewEnvelope.payload.value), ProtocolDecodeError);

const teamBody = (messages: readonly string[]) => encoder.encode(JSON.stringify({
  source_id: "source-1",
  team: { id: "team-1", name: "Operators", lead_agent_id: "session-1" },
  members: [],
  messages: messages.map((id, index) => ({
    id,
    team_id: "team-1",
    scope: "broadcast",
    sender_agent_id: "session-1",
    recipient_agent_ids: ["session-2"],
    input: [{ type: "text", text: `message ${index}` }],
    created_at: 100 + index
  })),
  deliveries: messages.map((id, index) => ({
    id: `delivery-${id}`,
    message_id: id,
    team_id: "team-1",
    recipient_agent_id: "session-2",
    provider: "codex",
    status: "injected",
    updated_at: 100 + index
  }))
}));

resetGoosewebStoreForTests();
const initial = decodeSnapshot(create(SnapshotSchema, {
  viewKind: "team_workspace",
  schemaVersion: 1,
  operation: ViewOperation.REPLACE,
  coverage: coverage("team_workspaces", "team-1"),
  body: teamBody(["stale-message", "current-message"])
}));
updateGoosewebStore(initial);
assert.deepEqual(
  getGoosewebSnapshot().entities.teamWorkspaces["team-1"]?.messages.map((item) => item.id),
  ["stale-message", "current-message"]
);

const repair = decodeSnapshot(create(SnapshotSchema, {
  viewKind: "team_workspace",
  schemaVersion: 1,
  operation: ViewOperation.REPLACE,
  coverage: coverage("team_workspaces", "team-1"),
  body: teamBody(["current-message"])
}));
updateGoosewebStore(repair);
assert.deepEqual(
  getGoosewebSnapshot().entities.teamWorkspaces["team-1"]?.messages.map((item) => item.id),
  ["current-message"],
  "authoritative replacement must remove stale Team Comms detail"
);

const session = decodeSnapshot(create(SnapshotSchema, {
  viewKind: "session_detail",
  schemaVersion: 1,
  operation: ViewOperation.REPLACE,
  coverage: coverage("session_details", "session-1"),
  body: sessionBody("session-1", ["persisted prompt"], "terminal answer")
}));
updateGoosewebStore(session);
assert.equal(
  getGoosewebSnapshot().entities.sessionDetails["session-1"]?.transcript
    .some((row) => row.text === "terminal answer"),
  true,
  "fresh snapshot must reconstruct minimal terminal session detail"
);

const remove = decodePatch(create(PatchSchema, {
  viewKind: "team_workspace",
  schemaVersion: 1,
  operation: ViewOperation.REMOVE,
  coverage: coverage("team_workspaces", "team-1")
}));
updateGoosewebStore(remove);
assert.equal(getGoosewebSnapshot().entities.teamWorkspaces["team-1"], undefined);

resetGoosewebStoreForTests();
updateGoosewebStore(decodeSnapshot(create(SnapshotSchema, {
  viewKind: "team_workspace",
  schemaVersion: 1,
  operation: ViewOperation.REPLACE,
  coverage: coverage("team_workspaces", "team-1"),
  body: teamBody(["stale-message"])
})));
const emptyWorkspace = decodeSnapshot(create(SnapshotSchema, {
  viewKind: "team_workspace",
  schemaVersion: 1,
  operation: ViewOperation.REPLACE,
  coverage: coverage("team_workspaces", "team-1"),
  body: teamBody([])
}));
updateGoosewebStore(emptyWorkspace);
assert.deepEqual(getGoosewebSnapshot().entities.teamWorkspaces["team-1"]?.messages, []);
assert.deepEqual(getGoosewebSnapshot().entities.teamWorkspaces["team-1"]?.deliveries, []);

const malformedBodies = [
  encoder.encode("{}"),
  encoder.encode(JSON.stringify({ error: "not found" })),
  encoder.encode(JSON.stringify({
    source_id: "source-1",
    team: { id: "team-1" },
    members: [],
    messages: "bad",
    deliveries: []
  }))
];
for (const body of malformedBodies) {
  assert.throws(() => decodeSnapshot(create(SnapshotSchema, {
    viewKind: "team_workspace",
    schemaVersion: 1,
    operation: ViewOperation.REPLACE,
    coverage: coverage("team_workspaces", "team-1"),
    body
  })), ProtocolDecodeError);
}
assert.throws(() => decodeSnapshot(create(SnapshotSchema, {
  viewKind: "session_detail",
  schemaVersion: 1,
  operation: ViewOperation.REPLACE,
  coverage: coverage("session_details", "session-A"),
  body: sessionBody("session-B", [])
})), ProtocolDecodeError, "coverage A/body B must fail");
assert.throws(() => decodePatch(create(PatchSchema, {
  viewKind: "session_detail",
  schemaVersion: 1,
  operation: ViewOperation.REPLACE,
  entity: create(EntityRefSchema, { entityId: "session-B" }),
  coverage: coverage("session_details", "session-A"),
  body: sessionBody("session-A", [])
})), ProtocolDecodeError, "entity ref must agree with coverage");
assert.throws(() => decodeSnapshot(create(SnapshotSchema, {
  viewKind: "session_detail",
  schemaVersion: 1,
  operation: ViewOperation.REPLACE,
  coverage: create(ViewCoverageSchema, {
    domains: ["session_details"],
    entityIds: ["session-A", "session-A"],
    authoritative: true
  }),
  body: sessionBody("session-A", [])
})), ProtocolDecodeError, "duplicate coverage IDs must fail");

resetGoosewebStoreForTests();
const replaceA = decodeSnapshot(create(SnapshotSchema, {
  viewKind: "session_detail",
  schemaVersion: 1,
  operation: ViewOperation.REPLACE,
  coverage: coverage("session_details", "session-A"),
  body: sessionBody("session-A", ["A"])
}));
const replaceB = decodeSnapshot(create(SnapshotSchema, {
  viewKind: "session_detail",
  schemaVersion: 1,
  operation: ViewOperation.REPLACE,
  coverage: coverage("session_details", "session-B"),
  body: sessionBody("session-B", ["B"])
}));
updateGoosewebStore({
  entityOperations: [...replaceA.entityOperations, ...replaceB.entityOperations]
});
assert.deepEqual(
  Object.keys(getGoosewebSnapshot().entities.sessionDetails).sort(),
  ["session-A", "session-B"],
  "same-flush scoped replacements retain both payloads"
);
const upsertB = decodePatch(create(PatchSchema, {
  viewKind: "session_detail",
  schemaVersion: 1,
  operation: ViewOperation.UPSERT,
  entity: create(EntityRefSchema, { entityId: "session-B" }),
  coverage: coverage("session_details", "session-B"),
  body: sessionBody("session-B", ["B2"])
}));
resetGoosewebStoreForTests();
updateGoosewebStore({
  entityOperations: [...replaceA.entityOperations, ...upsertB.entityOperations]
});
assert.deepEqual(
  Object.keys(getGoosewebSnapshot().entities.sessionDetails).sort(),
  ["session-A", "session-B"],
  "same-flush replace+upsert retains both scoped payloads"
);
const removeB = decodePatch(create(PatchSchema, {
  viewKind: "session_detail",
  schemaVersion: 1,
  operation: ViewOperation.REMOVE,
  entity: create(EntityRefSchema, { entityId: "session-B" }),
  coverage: coverage("session_details", "session-B")
}));
updateGoosewebStore({
  entityOperations: [...upsertB.entityOperations, ...removeB.entityOperations]
});
assert.equal(
  getGoosewebSnapshot().entities.sessionDetails["session-B"],
  undefined,
  "same-flush upsert+remove preserves operation order"
);
const replaceTeam1 = decodeSnapshot(create(SnapshotSchema, {
  viewKind: "team_workspace",
  schemaVersion: 1,
  operation: ViewOperation.REPLACE,
  coverage: coverage("team_workspaces", "team-1"),
  body: teamBody(["team-1-message"])
}));
const team2Body = teamBody(["team-2-message"]);
const team2Json = JSON.parse(new TextDecoder().decode(team2Body));
team2Json.team.id = "team-2";
for (const message of team2Json.messages) message.team_id = "team-2";
for (const delivery of team2Json.deliveries) delivery.team_id = "team-2";
const replaceTeam2 = decodeSnapshot(create(SnapshotSchema, {
  viewKind: "team_workspace",
  schemaVersion: 1,
  operation: ViewOperation.REPLACE,
  coverage: coverage("team_workspaces", "team-2"),
  body: encoder.encode(JSON.stringify(team2Json))
}));
resetGoosewebStoreForTests();
updateGoosewebStore({
  entityOperations: [...replaceTeam1.entityOperations, ...replaceTeam2.entityOperations]
});
assert.deepEqual(
  Object.keys(getGoosewebSnapshot().entities.teamWorkspaces).sort(),
  ["team-1", "team-2"],
  "same-flush team replacements retain both payloads"
);

assert.throws(
  () => decodeSnapshot(create(SnapshotSchema, {
    viewKind: "future_detail",
    schemaVersion: 1,
    operation: ViewOperation.REPLACE,
    body: encoder.encode("{}")
  })),
  ProtocolDecodeError
);
assert.throws(
  () => decodeSnapshot(create(SnapshotSchema, {
    viewKind: "session_detail",
    schemaVersion: 1,
    operation: ViewOperation.REPLACE,
    coverage: coverage("session_details", "session-1"),
    body: encoder.encode("{broken")
  })),
  ProtocolDecodeError
);
assert.throws(
  () => decodeSnapshot(create(SnapshotSchema, {
    viewKind: "session_detail",
    schemaVersion: 2,
    operation: ViewOperation.REPLACE,
    coverage: coverage("session_details", "session-1"),
    body: encoder.encode("{}")
  })),
  ProtocolDecodeError
);
assert.throws(
  () => decodeSnapshot(create(SnapshotSchema, {
    viewKind: "session_detail",
    schemaVersion: 1,
    operation: ViewOperation.REPLACE,
    body: encoder.encode("{}")
  })),
  ProtocolDecodeError,
  "versioned frames must declare authoritative coverage"
);

console.log("P08 detail replacement protocol smoke passed");
