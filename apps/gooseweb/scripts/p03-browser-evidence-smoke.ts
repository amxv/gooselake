import assert from "node:assert/strict";
import { mkdirSync, rmSync, writeFileSync } from "node:fs";
import { resolve } from "node:path";
import {
  applySchemaFile,
  readJson,
  stackConfigurationHash,
  validateManifest,
  validateManifestRegistry,
  validateP03EvidenceArtifact,
  validateP03EvidenceLinkage,
  validateP03BrowserEvidence,
  type Json,
  type RecordJson
} from "../../../verification/gooseweb/validator/validate";

const evidence = readJson("verification/gooseweb/validator/fixtures/valid-p03-browser-evidence.json");
const manifest = readJson("verification/gooseweb/manifests/p03-headless-agent-browser.json");
const p02Manifest = readJson("verification/gooseweb/manifests/p02-fake-source-observers.json");
const registry = readJson("verification/gooseweb/manifest-registry.json");

validateP03BrowserEvidence(evidence);
validateManifest(manifest);
validateManifestRegistry(registry);
applySchemaFile("verification/gooseweb/schemas/p03-browser-evidence.schema.json", evidence);

const seededFailures: readonly [string, (value: RecordJson) => RecordJson][] = [
  ["console", (value) => change(value, "captures.console.unexpected_failures", 1)],
  ["network", (value) => change(value, "captures.network.unexpected_failures", 1)],
  ["WebSocket", (value) => change(value, "captures.websocket.unexpected_failures", 1)],
  ["wrong head", (value) => change(value, "served_head_sha", "3".repeat(40))],
  ["wrong viewport", (value) => change(value, "viewports.2.width", 519)],
  ["composer outside viewport", (value) => change(value, "viewports.1.composer_inside_viewport", false)],
  ["fixture leakage", (value) => change(value, "fixture_leakage.fixture_markers_found", 1)],
  ["stale context", (value) => change(value, "reconstruction.fresh_context_nonce", valueAt(value, "reconstruction.old_context_nonce"))],
  ["headed mode", (value) => change(value, "browser.execution_mode", "headed")],
  ["non-real Chromium", (value) => change(value, "browser.real_local_chromium", false)],
  ["Chrome/User-Agent version mismatch", (value) => change(value, "browser.user_agent", "Mozilla/5.0 HeadlessChrome/149.0.0.0 Safari/537.36")]
];

for (const [name, seed] of seededFailures) {
  assert.throws(() => validateP03BrowserEvidence(seed(evidence)), undefined, `seeded ${name} failure unexpectedly passed`);
}

assertExactBaselinePreservation(manifest, p02Manifest);
assert.throws(
  () => assertExactBaselinePreservation(change(manifest, "baseline_detected.0.defect_id", "BASE-REPLACED"), p02Manifest),
  undefined,
  "replaced P03 baseline defect unexpectedly passed"
);
assert.throws(
  () => assertExactBaselinePreservation(change(manifest, "baseline_detected.1.defect_id", valueAt(manifest, "baseline_detected.0.defect_id")), p02Manifest),
  undefined,
  "duplicate P03 baseline defect unexpectedly passed"
);
assert.deepEqual(manifest.known_defects, [], "P03 verification infrastructure must have no known defects");
validateStandardEvidenceLinkage();
console.log(`P03 headless browser evidence contract passed (${seededFailures.length + 4} seeded failures rejected)`);

function validateStandardEvidenceLinkage(): void {
  const descriptor = linkedStandardDescriptor();
  const linked = linkedP03Evidence(descriptor);
  validateP03EvidenceLinkage(descriptor, linked, manifest);
  const root = resolve(import.meta.dir, "../../../tmp/gg/p03-browser-evidence-smoke");
  rmSync(root, { recursive: true, force: true });
  mkdirSync(root, { recursive: true });
  try {
    assert.throws(
      () => validateP03EvidenceArtifact(descriptor, manifest, root),
      undefined,
      "missing P03 browser evidence artifact unexpectedly passed"
    );
    writeFileSync(resolve(root, "p03-browser-evidence.json"), `${JSON.stringify(linked, null, 2)}\n`);
    validateP03EvidenceArtifact(descriptor, manifest, root);
    writeFileSync(resolve(root, "p03-browser-evidence.json"), `${JSON.stringify(change(linked, "browser.session_name", "gooseweb-p03-1111111-a1-other-1234abcd"), null, 2)}\n`);
    assert.throws(
      () => validateP03EvidenceArtifact(descriptor, manifest, root),
      undefined,
      "tuple-mismatched P03 browser evidence artifact unexpectedly passed"
    );
  } finally {
    rmSync(root, { recursive: true, force: true });
  }
}

function linkedStandardDescriptor(): RecordJson {
  const result = readJson("verification/gooseweb/validator/fixtures/valid-evidence-run.json");
  const standardBrowser = {
    mechanism: "agent-browser",
    execution_mode: "headless",
    headed_mode_prohibited: true,
    fresh_unique_session_required: true,
    chromium_binary: "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome",
    chromium_version: "150.0.7871.115",
    profile_policy: "fresh_ephemeral",
    session_name: "gooseweb-p03-1111111-a1-reviewer-1234abcd"
  };
  let linked = change(result, "phase_id", "P03");
  linked = change(linked, "attempt", 1);
  linked = change(linked, "candidate_head_sha", "1".repeat(40));
  linked = change(linked, "served_head_sha", "1".repeat(40));
  linked = change(linked, "candidate_tree_sha", "2".repeat(40));
  linked = change(linked, "served_tree_sha", "2".repeat(40));
  linked = change(linked, "browser", standardBrowser);
  linked = change(linked, "review.browser_session", standardBrowser.session_name);
  linked = change(linked, "lease.phase_id", "P03");
  linked = change(linked, "lease.owner_identity", "p03-supervisor");
  linked = change(linked, "metrics", { status: "captured", reason: "P03 procedure overhead recorded separately from product budgets.", artifacts: ["p03-browser-evidence.json"] });
  linked = change(linked, "p03_browser_evidence", "p03-browser-evidence.json");
  const stack = linked.stack as RecordJson;
  stack.configuration_sha256 = stackConfigurationHash(stack);
  return linked;
}

function linkedP03Evidence(descriptor: RecordJson): RecordJson {
  let linked = structuredClone(evidence);
  for (const key of ["phase_id", "attempt", "candidate_head_sha", "served_head_sha", "candidate_tree_sha", "served_tree_sha"]) {
    linked = change(linked, key, descriptor[key]!);
  }
  const standardBrowser = descriptor.browser as RecordJson;
  linked = change(linked, "browser.session_name", standardBrowser.session_name!);
  linked = change(linked, "browser.binary_path", standardBrowser.chromium_binary!);
  linked = change(linked, "browser.version", standardBrowser.chromium_version!);
  linked = change(linked, "supervisor_attachment.lease_id", valueAt(descriptor, "lease.lease_id"));
  linked = change(linked, "supervisor_attachment.lease_sequence", valueAt(descriptor, "lease.sequence"));
  linked = change(linked, "supervisor_attachment.supervisor_identity", valueAt(descriptor, "lease.owner_identity"));
  linked = change(linked, "supervisor_attachment.stack_configuration_sha256", valueAt(descriptor, "stack.configuration_sha256"));
  return linked;
}

function assertExactBaselinePreservation(p03: RecordJson, p02: RecordJson): void {
  const p02Scenario = valueAt(p02, "scenario.stable_scenario_id");
  const p03Scenario = valueAt(p03, "scenario.stable_scenario_id");
  const expected = structuredClone(p02.baseline_detected as Json[]);
  const actual = structuredClone(p03.baseline_detected as Json[]).map((entry) => {
    const record = entry as RecordJson;
    assert.equal(record.scenario_id, p03Scenario, "P03 baseline scenario rewrite");
    record.scenario_id = p02Scenario;
    return record;
  });
  assert.deepEqual(actual, expected, "P03 baseline defect IDs and correction/downstream/evidence mappings changed");
  const ids = actual.map((entry) => (entry as RecordJson).defect_id);
  assert.equal(new Set(ids).size, ids.length, "P03 baseline defect IDs must be unique");
}

function change(source: RecordJson, path: string, value: Json): RecordJson {
  const copy = structuredClone(source);
  const parts = path.split(".");
  let cursor: RecordJson | Json[] = copy;
  for (const part of parts.slice(0, -1)) {
    cursor = Array.isArray(cursor) ? cursor[Number(part)] as RecordJson : cursor[part] as RecordJson;
  }
  const key = parts.at(-1)!;
  if (Array.isArray(cursor)) cursor[Number(key)] = value;
  else cursor[key] = value;
  return copy;
}

function valueAt(source: RecordJson, path: string): Json {
  let cursor: Json = source;
  for (const part of path.split(".")) cursor = Array.isArray(cursor) ? cursor[Number(part)]! : (cursor as RecordJson)[part]!;
  return cursor;
}
