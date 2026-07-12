import assert from "node:assert/strict";
import {
  firstDivergentLayer,
  redactedObserver,
  stableDigest,
  type LayerEvidence
} from "./support/p02-observers";

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

console.log("P02 observer redaction and strict first-divergence guards passed");
