import assert from "node:assert/strict";
import { create, fromBinary, toBinary } from "@bufbuild/protobuf";
import { Lane, MessageKind } from "../src/gen/goosetower/v1/common_pb";
import { RealtimeEnvelopeSchema } from "../src/gen/goosetower/v1/realtime_pb";
import { SnapshotSchema } from "../src/gen/goosetower/v1/view_pb";
import { decodeSnapshot } from "../app/realtime/protocol/entities";
import { getGoosewebSnapshot, updateGoosewebStore } from "../app/stores/gooseweb-store";
import {
  firstDivergentLayer,
  observeFrame,
  observeStore,
  redactedObserver,
  stableDigest,
  type LayerEvidence
} from "./support/p02-observers";

const fixedWorkspace = {
  team: {
    id: "p02-team-001",
    source_id: "p02-source",
    name: "P02 deterministic team",
    lead_agent_id: "p02-session-001",
    member_agent_ids: ["p02-session-001"],
    updated_at_unix_ms: 1_700_100_000_000
  },
  messages: [{
    id: "p02-message-001",
    team_id: "p02-team-001",
    scope: "broadcast",
    sender_agent_id: "p02-session-001",
    recipient_agent_ids: ["p02-session-001"],
    input: [{ type: "text", text: "P02 deterministic team action" }],
    created_at: 1_700_100_000_020
  }],
  deliveries: []
};

const snapshot = create(SnapshotSchema, {
  viewKind: "team_workspace",
  body: new TextEncoder().encode(JSON.stringify(fixedWorkspace))
});
const envelope = create(RealtimeEnvelopeSchema, {
  protocolVersion: 1,
  messageId: "p02-frame-001",
  messageKind: MessageKind.SNAPSHOT,
  lane: Lane.STATE,
  gatewaySeq: 7n,
  sourceId: "p02-source",
  sourceEpoch: "p02-epoch-001",
  sourceSeq: 3n,
  payload: { case: "snapshot", value: snapshot }
});
const decoded = fromBinary(RealtimeEnvelopeSchema, toBinary(RealtimeEnvelopeSchema, envelope));
assert.equal(decoded.payload.case, "snapshot");
const patch = decodeSnapshot(decoded.payload.value);
updateGoosewebStore({ entities: patch.entities });

const frameObserver = observeFrame(decoded);
const storeObserver = observeStore(getGoosewebSnapshot());
assert.equal(JSON.stringify(frameObserver).includes("p02-frame-001"), true);
assert.equal(JSON.stringify(storeObserver).includes("P02 deterministic team action"), true);

const secretProbe = redactedObserver({
  authorization: "Bearer p02-secret",
  nested: { ticket: "ticket=should-not-leak", safe: "preserved" },
  imageData: "data:image/png;base64,AAAA"
});
assert.deepEqual(secretProbe, {
  authorization: "[redacted]",
  nested: { ticket: "[redacted]", safe: "preserved" },
  imageData: "[redacted]"
});

const good = stableDigest({ id: "p02-message-001" });
const bad = stableDigest({ id: "wrong-message" });
const seeded: LayerEvidence[] = [
  { layer: "fake/runtime", available: true, expectedDigest: good, actualDigest: good },
  { layer: "goosetower/materialized", available: true, expectedDigest: good, actualDigest: good },
  { layer: "goosetower/frame", available: true, expectedDigest: good, actualDigest: bad },
  { layer: "gooseweb/worker-store", available: true, expectedDigest: good, actualDigest: bad }
];
assert.equal(firstDivergentLayer(seeded), "goosetower/frame");
assert.notEqual(firstDivergentLayer(seeded), "gooseweb/worker-store");
assert.throws(() => firstDivergentLayer(seeded.slice(1)), /exactly four layers/);
assert.throws(
  () => firstDivergentLayer(seeded.map((item, index) => index === 2 ? { ...item, available: false, expectedDigest: undefined, actualDigest: undefined } : item)),
  /evidence unavailable/
);
assert.throws(
  () => firstDivergentLayer(seeded.map((item, index) => index === 2 ? { ...item, available: false } : item)),
  /unavailable evidence was inferred/
);

console.log("P02 frame/store observers and first-divergence localization passed");
