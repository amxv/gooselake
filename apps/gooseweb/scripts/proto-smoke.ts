import { create, fromBinary, toBinary } from "@bufbuild/protobuf";
import {
  Lane,
  MessageKind
} from "../src/gen/goosetower/v1/common_pb";
import {
  RealtimeEnvelopeSchema
} from "../src/gen/goosetower/v1/realtime_pb";
import {
  SourceHealthViewSchema,
  SnapshotSchema as ViewSnapshotSchema
} from "../src/gen/goosetower/v1/view_pb";

const sourceHealth = create(SourceHealthViewSchema, {
  sourceId: "local-runtime",
  displayName: "Local Runtime",
  sourceKind: "gooselake-runtime",
  health: "live",
  observedAtUnixMs: BigInt(1_783_355_700_000)
});

const snapshot = create(ViewSnapshotSchema, {
  viewKind: "source-health",
  body: toBinary(SourceHealthViewSchema, sourceHealth)
});

const envelope = create(RealtimeEnvelopeSchema, {
  protocolVersion: 1,
  messageId: "fixture-source-health",
  messageKind: MessageKind.SNAPSHOT,
  lane: Lane.STATE,
  gatewaySeq: 1n,
  sourceId: "local-runtime",
  sourceEpoch: "epoch-1",
  sourceSeq: 1n,
  payload: {
    case: "snapshot",
    value: snapshot
  }
});

const decodedEnvelope = fromBinary(
  RealtimeEnvelopeSchema,
  toBinary(RealtimeEnvelopeSchema, envelope)
);

if (decodedEnvelope.payload.case !== "snapshot") {
  throw new Error("expected snapshot envelope");
}

const decodedSnapshot = fromBinary(
  ViewSnapshotSchema,
  toBinary(ViewSnapshotSchema, decodedEnvelope.payload.value)
);

const decodedSource = fromBinary(
  SourceHealthViewSchema,
  decodedSnapshot.body
);

if (
  decodedSource.sourceId !== "local-runtime" ||
  decodedSource.health !== "live" ||
  decodedEnvelope.gatewaySeq !== 1n
) {
  throw new Error("protobuf binary round trip did not preserve fixture values");
}

console.log("protobuf binary smoke fixture passed");
