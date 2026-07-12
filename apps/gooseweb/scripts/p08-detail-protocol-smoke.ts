import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { create, fromBinary } from "@bufbuild/protobuf";
import { RealtimeEnvelopeSchema } from "../src/gen/goosetower/v1/realtime_pb";
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

const encoder = new TextEncoder();
const coverage = (domain: string, entityId: string) => create(ViewCoverageSchema, {
  domains: [domain],
  entityIds: [entityId],
  authoritative: true
});

const corpus = JSON.parse(readFileSync(
  new URL("../../../verification/gooseweb/fixtures/p08-detail-frame-corpus.json", import.meta.url),
  "utf8"
)) as { frames: Array<{ base64: string; sourceSeq: number; entityId: string }> };
const corpusEnvelope = fromBinary(
  RealtimeEnvelopeSchema,
  Uint8Array.from(Buffer.from(corpus.frames[0]!.base64, "base64"))
);
assert.equal(corpusEnvelope.payload.case, "snapshot");
assert.equal(corpusEnvelope.payload.value.cursor?.sources[0]?.sourceSeq, 17n);
const corpusPatch = decodeSnapshot(corpusEnvelope.payload.value);
assert.equal(corpusPatch.entityMutations[0]?.entityIds[0], "session-1");

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
  body: encoder.encode(JSON.stringify({
    source_id: "source-1",
    session: { id: "session-1", provider: "codex", status: "ready" },
    transcript: [{ role: "user", text: "persisted prompt" }],
    appended_text: "terminal answer",
    latest_activity_unix_ms: 200
  }))
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
