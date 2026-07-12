import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { mkdirSync, rmSync, writeFileSync } from "node:fs";
import { resolve } from "node:path";
import {
  applySchemaFile,
  readJson,
  stackConfigurationHash,
  validateBrowserCaptures,
  validateManifest,
  validateManifestRegistry,
  validateP03EvidenceArtifact,
  validateP03EvidenceLinkage,
  validateP03HardReloadNetworkLinkage,
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
validateP03BrowserEvidence(unavailableIdentity(evidence));

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
  ["Chrome/User-Agent version mismatch", (value) => change(value, "browser.user_agent", "Mozilla/5.0 HeadlessChrome/149.0.0.0 Safari/537.36")],
  ["malformed non-reduced headless UA", (value) => change(value, "browser.user_agent", "Mozilla/5.0 HeadlessChrome/150.0.7871.115 Safari/537.36")],
  ["non-headless Chrome UA", (value) => change(value, "browser.user_agent", "Mozilla/5.0 Chrome/150.0.0.0 Safari/537.36")],
  ["reduced UA token mismatch", (value) => change(value, "browser.user_agent_reduction.token_version", "149.0.0.0")],
  ["reduced UA mode mismatch", (value) => change(value, "browser.user_agent_reduction.mode", "full_version")],
  ["forged binary path", (value) => change(value, "browser.binary_path", "/Applications/Firefox.app/Contents/MacOS/firefox")],
  ["forged binary full version", (value) => change(value, "browser.version", "150.0.9999.1")],
  ["high-entropy full version mismatch", (value) => change(value, "browser.user_agent_data.full_version_list.1.version", "150.0.9999.1")],
  ["high-entropy Chrome brand missing", (value) => change(value, "browser.user_agent_data.full_version_list", [{ brand: "Not A Browser", version: "99.0.0.0" }])],
  ["available high-entropy list empty", (value) => change(value, "browser.user_agent_data.full_version_list", [])],
  ["unavailable API retained version list", (value) => change(unavailableIdentity(value), "browser.user_agent_data.full_version_list", [{ brand: "Google Chrome", version: "150.0.7871.115" }])],
  ["unavailable API omitted reason", (value) => change(unavailableIdentity(value), "browser.user_agent_data.unavailable_reason", "")],
  ["unavailable fallback wrong major", (value) => change(unavailableIdentity(value), "browser.user_agent", "Mozilla/5.0 HeadlessChrome/149.0.0.0 Safari/537.36")],
  ["navigator.webdriver missing", (value) => omit(value, "browser.navigator_webdriver")],
  ["navigator.webdriver false", (value) => change(value, "browser.navigator_webdriver", false)],
  ["semantic journey missing", (value) => omit(value, "journey")],
  ["roster selection unproven", (value) => change(value, "journey.selected_roster_control.selected", false)],
  ["duplicate command cardinality", (value) => change(value, "journey.action.command_count", 2)],
  ["descriptor submitted text/hash mismatch", (value) => change(value, "journey.action.submitted_text", "forged deterministic action")],
  ["authority chain missing", (value) => omit(value, "authority_chain")],
  ["observed authority missing instance", (value) => change(value, "authority_chain.1.missing_count", 1)],
  ["authority correlation mismatch", (value) => change(value, "authority_chain.2.correlation_id", "other-action")],
  ["authority semantic identity missing", (value) => change(value, "authority_chain.0.semantic_identity", "")],
  ["authority cursor missing", (value) => change(value, "authority_chain.1.cursor_or_version", "")],
  ["authority content mismatch", (value) => change(value, "authority_chain.3.content_sha256", "0".repeat(64))],
  ["first divergence mismatch", (value) => change(value, "first_divergent_layer", "Goosetower")],
  ["reload missing row", (value) => change(value, "reconstruction.ordinary_reload.missing_count", 1)],
  ["initial prior nonce non-null", (value) => change(value, "reconstruction.initial_prior_context_nonce", "stale-context")],
  ["CacheStorage not cleared", (value) => change(value, "reconstruction.cache_storage_cleared", false)],
  ["service workers not unregistered", (value) => change(value, "reconstruction.service_workers_unregistered", false)],
  ["network reconstruction linkage missing", (value) => omit(value, "network_reconstruction")],
  ["network reconstruction segment order", (value) => change(value, "network_reconstruction.ordered_segments", ["initial_load", "hard_reload", "ordinary_reload", "fresh_context"])],
  ["hard reload command success without navigation", (value) => change(value, "hard_reload_capability.navigation_observed", false)],
  ["hard reload header command failed", (value) => change(value, "hard_reload_capability.header_application_command_succeeded", false)],
  ["hard reload reload command failed", (value) => change(value, "hard_reload_capability.reload_command_succeeded", false)],
  ["hard reload missing document traffic", (value) => change(value, "hard_reload_capability.document_request_count", 0)],
  ["hard reload missing dev-ticket traffic", (value) => change(value, "hard_reload_capability.dev_ticket_request_count", 0)],
  ["hard reload document headers unproven", (value) => change(value, "hard_reload_capability.headers_observed_on_document", false)],
  ["hard reload dev-ticket headers unproven", (value) => change(value, "hard_reload_capability.headers_observed_on_dev_ticket", false)],
  ["hard reload reconstruction unobserved", (value) => change(value, "hard_reload_capability.observable_reconstruction", false)],
  ["hard reload cache revalidation unproven", (value) => change(value, "hard_reload_capability.cache_bypass_revalidation_evidenced", false)],
  ["hard reload cleanup failed", (value) => change(value, "hard_reload_capability.header_cleanup_command_succeeded", false)],
  ["hard reload headers remain active", (value) => change(value, "hard_reload_capability.temporary_headers_active_after_cleanup", true)],
  ["hard reload blocker retained on pass", (value) => change(value, "hard_reload_capability.blocker_reason", "reload gesture produced no navigation")],
  ["wrong hard reload mechanism", (value) => change(value, "hard_reload_capability.mechanism", "keyboard_gesture")]
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
validateSegmentedNetworkCapture();
validateStandardEvidenceLinkage();
console.log(`P03 headless browser evidence contract passed (${seededFailures.length + 43} seeded failures rejected)`);

function validateSegmentedNetworkCapture(): void {
  const capture = fullReconstructionNetworkCapture();
  const consoleCapture: RecordJson = {
    schema_revision: "gooseweb-console-capture/v3",
    capture_source: "agent-browser console",
    unfiltered: true,
    messages: []
  };
  validateBrowserCaptures(consoleCapture, capture, manifest);
  validateP03HardReloadNetworkLinkage(evidence, capture);
  const segmentFailures: readonly [string, (value: RecordJson) => RecordJson][] = [
    ["missing reconstruction segment", (value) => omit(value, "segments.2")],
    ["extra reconstruction segment", (value) => change(value, "segments.4", structuredClone((value.segments as Json[])[3]!))],
    ["duplicated reconstruction segment", (value) => change(value, "segments.2.segment_id", "ordinary_reload")],
    ["reordered reconstruction segments", (value) => change(value, "segments", [...(value.segments as Json[])].reverse())],
    ["filtered reconstruction capture", (value) => change(value, "unfiltered", false)],
    ["cross-origin reconstruction request", (value) => change(value, "raw_http.0.same_origin", false)],
    ["unexpected reconstruction status", (value) => change(value, "raw_http.0.status", 201)],
    ["segment-misattributed reconstruction traffic", (value) => change(value, "raw_http.0.segment_id", "ordinary_reload")],
    ["missing reconstruction request", (value) => omit(value, "raw_http.2")],
    ["extra reconstruction endpoint", (value) => change(value, "raw_http.1", request("initial_load", "GET", "/api/unexpected", 200, "api"))],
    ["query-bearing reconstruction document", (value) => change(value, "raw_http.0.query_keys", ["fixture"])],
    ["forged reconstruction raw count", (value) => change(value, "segments.0.raw_request_count", 99)],
    ["duplicated optional favicon", (value) => change(value, "raw_http.1", request("initial_load", "GET", "/favicon.ico", 404, "other", "BASE-P01-FAVICON-NOT-FOUND"))],
    ["wrong reconstruction trigger", (value) => change(value, "segments.1.trigger", "agent_browser_press_meta_shift_r")],
    ["wrong reconstruction context generation", (value) => change(value, "segments.3.context_generation", 1)],
    ["incomplete reconstruction start boundary", (value) => change(value, "segments.2.capture_started_before_trigger", false)],
    ["incomplete reconstruction end boundary", (value) => change(value, "segments.2.capture_ended_after_observable_state", false)],
    ["hard reload document header missing", (value) => change(value, "raw_http.7.request_cache_control", "absent")],
    ["hard reload dev-ticket header missing", (value) => change(value, "raw_http.9.request_pragma", "absent")],
    ["temporary headers leaked before hard reload", (value) => change(value, "raw_http.0.request_cache_control", "no-cache")],
    ["temporary headers leaked after cleanup", (value) => change(value, "raw_http.10.request_pragma", "no-cache")]
  ];
  for (const [name, seed] of segmentFailures) {
    assert.throws(() => validateBrowserCaptures(consoleCapture, seed(capture), manifest), undefined, `seeded ${name} unexpectedly passed`);
  }
  assert.throws(
    () => validateP03HardReloadNetworkLinkage(change(evidence, "hard_reload_capability.document_request_count", 0), capture),
    undefined,
    "hard-reload descriptor/network count mismatch unexpectedly passed"
  );
  assert.throws(
    () => validateP03HardReloadNetworkLinkage(evidence, change(capture, "segments.2.trigger", "agent_browser_reload")),
    undefined,
    "hard-reload descriptor/network mechanism mismatch unexpectedly passed"
  );
}

function fullReconstructionNetworkCapture(): RecordJson {
  const definitions = [
    ["initial_load", "initial_supervisor_url_attachment", 1],
    ["ordinary_reload", "agent_browser_reload", 1],
    ["hard_reload", "agent_browser_no_cache_headers_reload", 1],
    ["fresh_context", "second_unique_ephemeral_session_open", 2]
  ] as const;
  const rawHttp: RecordJson[] = [];
  const segments = definitions.map(([segmentId, trigger, contextGeneration], index) => {
    const requests = [
      request(segmentId, "GET", "/", 200, "document", "", [], index === 2 ? "no-cache" : "absent"),
      request(segmentId, "GET", `/assets/app-${index}.js`, 200, "module", "", ["v"], index === 2 ? "no-cache" : "absent"),
      request(segmentId, "POST", "/api/dev-ticket", 200, "api", "", [], index === 2 ? "no-cache" : "absent")
    ];
    if (index === 0) requests.push(request(segmentId, "GET", "/favicon.ico", 404, "other", "BASE-P01-FAVICON-NOT-FOUND", [], "absent"));
    rawHttp.push(...requests);
    return {
      segment_id: segmentId,
      trigger,
      context_generation: contextGeneration,
      complete: true,
      capture_started_before_trigger: true,
      capture_ended_after_observable_state: true,
      raw_request_count: requests.length
    };
  });
  return {
    schema_revision: "gooseweb-network-capture/v4",
    capture_source: "agent-browser network requests",
    unfiltered: true,
    segments,
    raw_http: rawHttp,
    websocket: {
      availability: "available",
      events: [{ event: "open", code: 0 }],
      inference_prohibited: false,
      reason: "",
      baseline_defect_id: ""
    }
  };
}

function request(
  segmentId: string,
  method: string,
  path: string,
  status: number,
  resourceType: string,
  baselineDefectId = "",
  queryKeys: readonly string[] = [],
  cacheDirective: "absent" | "no-cache" | "not_inspected" = "not_inspected"
): RecordJson {
  return {
    segment_id: segmentId,
    method,
    path,
    query_keys: [...queryKeys],
    status,
    resource_type: resourceType,
    same_origin: true,
    request_cache_control: cacheDirective,
    request_pragma: cacheDirective,
    baseline_defect_id: baselineDefectId
  };
}

function validateStandardEvidenceLinkage(): void {
  const descriptor = linkedStandardDescriptor();
  const linked = linkedP03Evidence(descriptor);
  validateP03EvidenceLinkage(descriptor, linked, manifest);
  assert.throws(
    () => validateP03EvidenceLinkage(descriptor, linked, omit(manifest, "scenario.actions.0.expected_submitted_text")),
    undefined,
    "missing manifest canonical submitted text unexpectedly passed"
  );
  assert.throws(
    () => validateP03EvidenceLinkage(descriptor, linked, change(manifest, "scenario.actions.0.expected_submitted_text", "wrong manifest action")),
    undefined,
    "wrong manifest canonical submitted text unexpectedly passed"
  );
  const internallyConsistentForgery = withSubmittedText(linked, "forged deterministic action");
  validateP03BrowserEvidence(internallyConsistentForgery);
  assert.throws(
    () => validateP03EvidenceLinkage(descriptor, internallyConsistentForgery, manifest),
    undefined,
    "internally consistent non-manifest submitted text unexpectedly passed"
  );
  assert.throws(
    () => validateP03EvidenceLinkage(descriptor, change(linked, "browser.binary_path", "/Applications/Chromium.app/Contents/MacOS/Chromium"), manifest),
    undefined,
    "manifest/standard-forged Chromium binary path unexpectedly passed"
  );
  let forgedFullVersion = change(linked, "browser.version", "150.0.9999.1");
  forgedFullVersion = change(forgedFullVersion, "browser.user_agent_data.full_version_list.0.version", "150.0.9999.1");
  forgedFullVersion = change(forgedFullVersion, "browser.user_agent_data.full_version_list.1.version", "150.0.9999.1");
  assert.throws(
    () => validateP03EvidenceLinkage(descriptor, forgedFullVersion, manifest),
    undefined,
    "manifest/standard-forged Chrome full version unexpectedly passed"
  );
  const cascade = cascadingDivergence(linked);
  validateP03EvidenceLinkage(descriptor, cascade, manifest);
  assert.throws(
    () => validateP03EvidenceLinkage(descriptor, change(cascade, "first_divergent_layer", "Gooseweb React"), manifest),
    undefined,
    "incorrect earliest layer in cascading divergence unexpectedly passed"
  );
  assert.throws(
    () => validateP03EvidenceLinkage(descriptor, change(linked, "journey.action.control.accessible_name", "Forged composer"), manifest),
    undefined,
    "manifest-mismatched semantic control unexpectedly passed"
  );
  assert.throws(
    () => validateP03EvidenceLinkage(descriptor, change(linked, "authority_chain.0.artifact", "report.md"), manifest),
    undefined,
    "wrong authority artifact unexpectedly passed"
  );
  let forgedDivergence = change(linked, "authority_chain.1.status", "baseline_divergent");
  forgedDivergence = change(forgedDivergence, "authority_chain.1.missing_count", 1);
  forgedDivergence = change(forgedDivergence, "authority_chain.1.baseline_defect_id", "BASE-NOT-REGISTERED");
  forgedDivergence = change(forgedDivergence, "first_divergent_layer", "Goosetower");
  assert.throws(
    () => validateP03EvidenceLinkage(descriptor, forgedDivergence, manifest),
    undefined,
    "unmapped first-divergence baseline unexpectedly passed"
  );
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
    writeAuthorityArtifacts(root, linked);
    validateP03EvidenceArtifact(descriptor, manifest, root);
    const artifactMismatchSeeds: readonly [string, string, Json][] = [
      ["correlation", "correlation_id", "forged-correlation"],
      ["semantic identity", "semantic_identity", "forged:identity"],
      ["cursor", "cursor_or_version", "forged:cursor"],
      ["content", "content_sha256", "0".repeat(64)],
      ["cardinality", "observed_instances", 2],
      ["discrepancy count", "missing_count", 1]
    ];
    for (const [name, key, forgedValue] of artifactMismatchSeeds) {
      writeAuthorityArtifacts(root, linked, { layerIndex: 1, key, value: forgedValue });
      assert.throws(
        () => validateP03EvidenceArtifact(descriptor, manifest, root),
        undefined,
        `forged authority artifact ${name} unexpectedly passed`
      );
    }
    writeAuthorityArtifacts(root, linked);
    rmSync(resolve(root, "tower-state.redacted.json"));
    assert.throws(
      () => validateP03EvidenceArtifact(descriptor, manifest, root),
      undefined,
      "missing parsed authority artifact unexpectedly passed"
    );
    writeAuthorityArtifacts(root, linked);
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

function writeAuthorityArtifacts(
  root: string,
  p03Evidence: RecordJson,
  mutate?: { readonly layerIndex: number; readonly key: string; readonly value: Json }
): void {
  const entries = p03Evidence.authority_chain as RecordJson[];
  entries.slice(0, 3).forEach((entry, index) => {
    let observation: RecordJson = {
      schema_revision: "gooseweb-p03-authority-observation/v1",
      phase_id: "P03",
      attempt: p03Evidence.attempt!,
      layer: entry.layer!,
      correlation_id: entry.correlation_id!,
      semantic_identity: entry.semantic_identity!,
      cursor_or_version: entry.cursor_or_version!,
      content_sha256: entry.content_sha256!,
      status: entry.status!,
      observed_instances: entry.observed_instances!,
      missing_count: entry.missing_count!,
      duplicate_count: entry.duplicate_count!,
      order_errors: entry.order_errors!,
      baseline_defect_id: entry.baseline_defect_id!
    };
    if (mutate?.layerIndex === index) observation = change(observation, mutate.key, mutate.value);
    writeFileSync(resolve(root, String(entry.artifact)), `${JSON.stringify(observation, null, 2)}\n`);
  });
}

function unavailableIdentity(source: RecordJson): RecordJson {
  let result = change(source, "browser.user_agent_data.availability", "unavailable");
  result = change(result, "browser.user_agent_data.full_version_list", []);
  return change(result, "browser.user_agent_data.unavailable_reason", "navigator.userAgentData.getHighEntropyValues is unavailable in this agent-browser Chrome context");
}

function withSubmittedText(source: RecordJson, submittedText: string): RecordJson {
  let result = change(source, "journey.action.submitted_text", submittedText);
  const hash = createHash("sha256").update(submittedText).digest("hex");
  for (const index of [0, 1, 2, 3]) result = change(result, `authority_chain.${index}.content_sha256`, hash);
  return result;
}

function cascadingDivergence(source: RecordJson): RecordJson {
  let result = structuredClone(source);
  for (const index of [1, 2, 3]) {
    result = change(result, `authority_chain.${index}.status`, "baseline_divergent");
    result = change(result, `authority_chain.${index}.observed_instances`, 0);
    result = change(result, `authority_chain.${index}.missing_count`, 1);
    result = change(result, `authority_chain.${index}.baseline_defect_id`, "BASE-P01-TEAM-COMMS-EMPTY");
  }
  return change(result, "first_divergent_layer", "Goosetower");
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

function omit(source: RecordJson, path: string): RecordJson {
  const copy = structuredClone(source);
  const parts = path.split(".");
  let cursor: RecordJson | Json[] = copy;
  for (const part of parts.slice(0, -1)) {
    cursor = Array.isArray(cursor) ? cursor[Number(part)] as RecordJson : cursor[part] as RecordJson;
  }
  const key = parts.at(-1)!;
  if (Array.isArray(cursor)) cursor.splice(Number(key), 1);
  else delete cursor[key];
  return copy;
}

function valueAt(source: RecordJson, path: string): Json {
  let cursor: Json = source;
  for (const part of path.split(".")) cursor = Array.isArray(cursor) ? cursor[Number(part)]! : (cursor as RecordJson)[part]!;
  return cursor;
}
