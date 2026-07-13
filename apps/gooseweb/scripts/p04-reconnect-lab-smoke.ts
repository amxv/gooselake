import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { resolve } from "node:path";
import {
  isNewGatewayGeneration,
  mergeCursorVector,
  shouldApplyCursor
} from "../app/realtime/cursors";
import type { CursorState, SourceCursorState } from "../app/realtime/types";

type FixtureCursor = {
  readonly gateway_seq: string;
  readonly gateway_epoch: string;
  readonly gateway_started_at_unix_ns: string;
  readonly source_epoch: string;
  readonly source_seq: string;
};

type FixtureCase = {
  readonly id: string;
  readonly current: FixtureCursor;
  readonly next: FixtureCursor;
  readonly safe_to_apply: boolean;
  readonly current_product_applies: boolean;
  readonly baseline_defect_id: string | null;
};

const fixturePath = resolve(
  import.meta.dir,
  "../../../verification/gooseweb/fixtures/p04-reconnect-cursors-v1.json"
);
const fixture = JSON.parse(readFileSync(fixturePath, "utf8")) as {
  schema_revision: string;
  cases: FixtureCase[];
};

assert.equal(
  fixture.schema_revision,
  "gooseweb-p04-reconnect-cursors/v1"
);
assert.deepEqual(
  fixture.cases.map((entry) => entry.id),
  [
    "replay-overlap",
    "missing-source-cursor",
    "source-epoch-change",
    "tower-restart-gateway-sequence-reset"
  ]
);

const detectedBaselines = new Set<string>();
for (const entry of fixture.cases) {
  const currentSource = sourceCursor(entry.current);
  const current: CursorState = {
    gatewaySeq: BigInt(entry.current.gateway_seq),
    gatewayEpoch: entry.current.gateway_epoch,
    gatewayStartedAtUnixNs: BigInt(entry.current.gateway_started_at_unix_ns),
    sourceCursors: { local: currentSource }
  };
  const nextSource = sourceCursor(entry.next);
  const generationChanged = isNewGatewayGeneration(
    current,
    entry.next.gateway_epoch,
    BigInt(entry.next.gateway_started_at_unix_ns)
  );
  const observed = generationChanged || shouldApplyCursor(
    current,
    BigInt(entry.next.gateway_seq),
    nextSource
  );

  assert.equal(observed, entry.current_product_applies, entry.id);
  if (observed !== entry.safe_to_apply) {
    assert.ok(entry.baseline_defect_id, `${entry.id} must map its unsafe result`);
    detectedBaselines.add(entry.baseline_defect_id);
    continue;
  }

  assert.equal(entry.baseline_defect_id, null, `${entry.id} has a false baseline`);
  if (observed) {
    const merged = mergeCursorVector(current, BigInt(entry.next.gateway_seq), [nextSource], {
      replaceGateway: generationChanged,
      gatewayEpoch: entry.next.gateway_epoch,
      gatewayStartedAtUnixNs: BigInt(entry.next.gateway_started_at_unix_ns)
    });
    assert.equal(merged.gatewaySeq, BigInt(entry.next.gateway_seq));
    assert.deepEqual(merged.sourceCursors.local, nextSource);
  }
}

assert.deepEqual([...detectedBaselines], []);

console.log(
  "P04 cursor lab passed: source-gap and gateway-generation baselines closed"
);

function sourceCursor(cursor: FixtureCursor): SourceCursorState {
  return {
    sourceId: "local",
    sourceEpoch: cursor.source_epoch,
    sourceSeq: BigInt(cursor.source_seq)
  };
}
