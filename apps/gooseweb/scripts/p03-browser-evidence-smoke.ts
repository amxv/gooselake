import assert from "node:assert/strict";
import {
  applySchemaFile,
  readJson,
  validateManifest,
  validateManifestRegistry,
  validateP03BrowserEvidence,
  type Json,
  type RecordJson
} from "../../../verification/gooseweb/validator/validate";

const evidence = readJson("verification/gooseweb/validator/fixtures/valid-p03-browser-evidence.json");
const manifest = readJson("verification/gooseweb/manifests/p03-headless-agent-browser.json");
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
  ["fixture leakage", (value) => change(value, "fixture_leakage.fixture_markers_found", 1)],
  ["stale context", (value) => change(value, "reconstruction.fresh_context_nonce", valueAt(value, "reconstruction.old_context_nonce"))],
  ["headed mode", (value) => change(value, "browser.execution_mode", "headed")],
  ["non-real Chromium", (value) => change(value, "browser.real_local_chromium", false)]
];

for (const [name, seed] of seededFailures) {
  assert.throws(() => validateP03BrowserEvidence(seed(evidence)), undefined, `seeded ${name} failure unexpectedly passed`);
}

assert.equal((manifest.baseline_detected as Json[]).length, 10, "P03 must preserve the ten finite P02 baseline entries");
assert.deepEqual(manifest.known_defects, [], "P03 verification infrastructure must have no known defects");
console.log(`P03 headless browser evidence contract passed (${seededFailures.length} seeded failures rejected)`);

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
