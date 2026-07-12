import assert from "node:assert/strict";
import {
  firstDivergentLayer,
  redactedObserver,
  stableDigest,
  type LayerEvidence
} from "./support/p02-observers";
import { create } from "@bufbuild/protobuf";
import { RealtimeEnvelopeSchema } from "../src/gen/goosetower/v1/realtime_pb";
import { MessageKind } from "../src/gen/goosetower/v1/common_pb";
import { PatchSchema } from "../src/gen/goosetower/v1/view_pb";
import { createGoosewebVerificationObserver } from "../app/realtime/verification-observer";
import { getGoosewebSnapshot } from "../app/stores/gooseweb-store";

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

const servedObserver = createGoosewebVerificationObserver(getGoosewebSnapshot);
for (let index = 0; index < 130; index += 1) {
  servedObserver.recordFrame(create(RealtimeEnvelopeSchema, {
    gatewaySeq: BigInt(index + 1),
    messageKind: MessageKind.PATCH,
    payload: {
      case: "patch",
      value: create(PatchSchema, {
        viewKind: "session_detail",
        body: new TextEncoder().encode(JSON.stringify({
          authorization: "Bearer browser-secret",
          text: "P02 deterministic terminal"
        }))
      })
    }
  }));
}
servedObserver.recordWorkerOutput({ type: "state", patch: {} }, getGoosewebSnapshot());
const servedCapture = servedObserver.capture(getGoosewebSnapshot());
if (servedCapture.frames.length !== 128 || servedCapture.frame_count !== 130) {
  throw new Error("served browser observer must be bounded while retaining total count");
}
const servedJson = JSON.stringify(servedCapture);
if (servedJson.includes("browser-secret") || !servedJson.includes("[redacted]")) {
  throw new Error("served browser observer must redact secrets at capture time");
}

console.log("P02 observer redaction and strict first-divergence guards passed");
