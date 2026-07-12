import { createHash } from "node:crypto";
import { existsSync, readFileSync, readdirSync, realpathSync, statSync } from "node:fs";
import { dirname, resolve, sep } from "node:path";
import { execFileSync } from "node:child_process";
import { inflateSync } from "node:zlib";
import { fileURLToPath } from "node:url";

export const APPROVED_PLAN_SHA256 =
  "521073ac7551df15d814b1e84d1be47ec9e80289728d07c3dbab8c5b2b1b3b2c";

const root = resolve(dirname(fileURLToPath(import.meta.url)), "../../..");
const PHASE = /^P(0[0-9]|[1-4][0-9]|5[0-6])$/;
const CORRECTION = /^P(0[6-9]|10)$/;
const RANGE_OR_SHORTHAND = /(?:P\d{2}\s*[-–—]\s*P\d{2}|P\d{2}\s*\.\.\s*P\d{2}|\+|\ball\b)/i;
const SECRET_KEY = /(?:credential|password|passwd|api[_-]?key|bearer[_-]?token|authorization|csrf(?:[_-]?(?:token|value))?|set[_-]?cookie|cookie|ticket(?:[_-]?(?:secret|value))?|provider[_-]?auth|secret[_-]?config|private[_-]?key|raw[_-]?image|image[_-]?data)/i;
const SECRET_VALUE = /(?:authorization\s*[:=]\s*bearer\s+\S+|(?:ticket|token|csrf|cookie|api[_-]?key|password)=[^&\s]+|-----BEGIN [A-Z ]+PRIVATE KEY-----|data:image\/[a-z0-9.+-]+;base64,)/i;
const REDACTED = /^(?:\[?redacted\]?|omitted|not captured|none|empty)$/i;

export type Json = null | boolean | number | string | Json[] | { [key: string]: Json };
export type RecordJson = { [key: string]: Json };
type Schema = RecordJson;

export function readJson(path: string): RecordJson {
  return JSON.parse(readFileSync(resolve(root, path), "utf8")) as RecordJson;
}

export function sha256(path: string): string {
  return hashBytes(readFileSync(resolve(root, path)));
}

export function validateManifest(value: RecordJson): void {
  applySchemaFile("verification/gooseweb/schemas/acceptance-manifest.schema.json", value);
  const scenario = object(value.scenario, "scenario");
  equal(scenario.stable_scenario_id, value.manifest_id, "stable scenario/manifest identity");
  if (!PHASE.test(string(scenario.phase_id, "manifest phase")) || scenario.phase_id === "P00") fail("manifest phase must be P01-P56");
  const layers = array(scenario.authority_chain, "authority_chain").map((entry) => object(entry, "authority layer").layer);
  equal(JSON.stringify(layers), JSON.stringify(["Gooselake", "Goosetower", "Gooseweb Worker/store", "Gooseweb React"]), "ordered authority chain");
  const actionCount = array(scenario.actions, "scenario actions").length;
  const cardinality = object(scenario.cardinality, "scenario cardinality");
  equal(cardinality.browser_actions, actionCount, "browser action cardinality matches actions");
  equal(cardinality.commands, actionCount, "command cardinality matches actions");
  for (const [name, stateValue] of Object.entries(object(value.states, "states"))) {
    const state = object(stateValue, `states.${name}`);
    if (state.applicability === "required") requireText(state.expectation, `states.${name}.expectation`);
    if (state.applicability === "not_applicable") requireText(state.reason, `states.${name}.reason`);
  }
  array(value.baseline_detected, "baseline_detected").forEach((entry, index) => validateBaseline(object(entry, `baseline[${index}]`)));
  ensureUnique(array(value.baseline_detected, "baseline_detected").map((entry) => string(object(entry, "baseline").defect_id, "defect_id")), "baseline defect IDs");
  scanSecrets(value, "manifest", false);
}

export function validateP03BrowserEvidence(value: RecordJson): void {
  applySchemaFile("verification/gooseweb/schemas/p03-browser-evidence.schema.json", value);
  equal(value.candidate_head_sha, value.served_head_sha, "P03 candidate/served head");
  equal(value.candidate_tree_sha, value.served_tree_sha, "P03 candidate/served tree");
  const browser = object(value.browser, "P03 browser");
  equal(browser.mechanism, "agent-browser", "P03 browser mechanism");
  equal(browser.engine, "chrome", "P03 browser engine");
  equal(browser.execution_mode, "headless", "P03 browser mode");
  equal(browser.headed_cli_value, false, "P03 explicit headed CLI value");
  equal(browser.headed_environment, "absent", "P03 headed environment");
  equal(browser.headed_config, "absent", "P03 headed config");
  equal(browser.real_local_chromium, true, "P03 real local Chromium identity");
  equal(browser.profile_policy, "fresh_ephemeral", "P03 fresh profile policy");
  equal(browser.persistent_state_loaded, false, "P03 persistent state policy");
  equal(browser.navigator_webdriver, true, "P03 navigator.webdriver automation proof");
  if (!/HeadlessChrome\/[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+/.test(string(browser.user_agent, "P03 browser user agent"))) {
    fail("P03 user agent does not prove headless Chromium");
  }
  const userAgentVersion = /HeadlessChrome\/([0-9]+\.[0-9]+\.[0-9]+\.[0-9]+)/.exec(string(browser.user_agent, "P03 browser user agent"))?.[1];
  equal(userAgentVersion, browser.version, "P03 Chrome binary/User-Agent full version");
  for (const [name, captureValue] of Object.entries(object(value.captures, "P03 captures"))) {
    const capture = object(captureValue, `P03 ${name} capture`);
    equal(capture.complete, true, `P03 ${name} capture completeness`);
    equal(capture.unexpected_failures, 0, `P03 ${name} unexpected failures`);
    equal(capture.redacted_at_capture, true, `P03 ${name} capture redaction`);
  }
  const reconstruction = object(value.reconstruction, "P03 reconstruction");
  equal(reconstruction.initial_prior_context_nonce, null, "P03 initial prior-context nonce");
  equal(reconstruction.fresh_context_prior_nonce, null, "P03 fresh prior-context nonce");
  for (const key of ["old_context_disposed", "indexeddb_cleared", "cookies_cleared", "local_storage_cleared", "session_storage_cleared", "cache_storage_cleared", "service_workers_unregistered"]) {
    equal(reconstruction[key], true, `P03 reconstruction ${key}`);
  }
  equal(reconstruction.remaining_cache_names, 0, "P03 remaining CacheStorage names");
  equal(reconstruction.remaining_service_workers, 0, "P03 remaining service workers");
  if (reconstruction.old_context_nonce === reconstruction.fresh_context_nonce) {
    fail("P03 stale browser context nonce was reused");
  }
  for (const key of ["ordinary_reload", "hard_reload", "navigation_away_back", "websocket_reconnect", "fresh_context"]) {
    validateP03IntegrityResult(object(reconstruction[key], `P03 reconstruction ${key}`), `P03 reconstruction ${key}`);
  }
  const viewportRecords = array(value.viewports, "P03 viewports").map((viewport) => object(viewport, "P03 viewport"));
  const viewports = viewportRecords.map((viewport) => viewport.id);
  equal(JSON.stringify(viewports), JSON.stringify(["1440x1000", "820x1000", "520x900"]), "P03 ordered viewport matrix");
  const dimensions = [[1440, 1000], [820, 1000], [520, 900]];
  viewportRecords.forEach((viewport, index) => {
    equal(viewport.width, dimensions[index]![0], `P03 viewport ${index} width`);
    equal(viewport.height, dimensions[index]![1], `P03 viewport ${index} height`);
    equal(viewport.screenshot, true, `P03 viewport ${index} screenshot`);
    equal(viewport.horizontal_overflow, false, `P03 viewport ${index} overflow`);
    equal(viewport.composer_inside_viewport, true, `P03 viewport ${index} composer geometry`);
    equal(viewport.primary_action_inside_viewport, true, `P03 viewport ${index} primary action`);
    equal(viewport.critical_actions_reachable, true, `P03 viewport ${index} critical actions`);
  });
  const leakage = object(value.fixture_leakage, "P03 fixture leakage");
  equal(leakage.default_development, "pass", "P03 default fixture leakage");
  equal(leakage.production_build, "pass", "P03 production fixture leakage");
  equal(leakage.query_flags_present, false, "P03 fixture query flags");
  equal(leakage.fixture_markers_found, 0, "P03 fixture markers");
  const journey = object(value.journey, "P03 semantic journey");
  const roster = object(journey.selected_roster_control, "P03 selected roster control");
  equal(roster.role, "button", "P03 roster role");
  requireText(roster.accessible_name, "P03 selected roster accessible name");
  for (const key of ["visible", "enabled", "selected"]) equal(roster[key], true, `P03 roster ${key}`);
  const action = object(journey.action, "P03 semantic action");
  requireText(action.submitted_text, "P03 submitted deterministic action text");
  for (const key of ["browser_submission_count", "command_count", "visible_submission_count"]) equal(action[key], 1, `P03 exact-once ${key}`);
  const authority = array(value.authority_chain, "P03 authority chain").map((entry) => object(entry, "P03 authority entry"));
  const layers = ["Gooselake", "Goosetower", "Gooseweb Worker/store", "Gooseweb React"];
  const authorityArtifacts = ["runtime-state.redacted.json", "tower-state.redacted.json", "store-state.redacted.json", "screenshots/1440x1000.png"];
  equal(JSON.stringify(authority.map((entry) => entry.layer)), JSON.stringify(layers), "P03 ordered authority chain");
  const divergent = authority.filter((entry) => entry.status === "baseline_divergent");
  const correlationId = string(authority[0]!.correlation_id, "P03 authority correlation ID");
  authority.forEach((entry) => equal(entry.correlation_id, correlationId, "P03 cross-layer correlation ID"));
  const submittedTextHash = hashBytes(Buffer.from(string(action.submitted_text, "P03 submitted deterministic action text")));
  if (value.first_divergent_layer === null) {
    equal(divergent.length, 0, "P03 absent first divergence");
  } else {
    equal(divergent.length, 1, "P03 single first divergence");
    equal(divergent[0]!.layer, value.first_divergent_layer, "P03 first divergent layer");
  }
  authority.forEach((entry, index) => {
    equal(entry.artifact, authorityArtifacts[index], `P03 ${layers[index]} artifact`);
    requireText(entry.semantic_identity, `P03 ${layers[index]} semantic identity`);
    requireText(entry.cursor_or_version, `P03 ${layers[index]} cursor/version`);
    equal(entry.content_sha256, submittedTextHash, `P03 ${layers[index]} submitted-content hash`);
    if (entry.status === "observed") {
      equal(entry.observed_instances, 1, `P03 ${layers[index]} observed cardinality`);
      equal(entry.missing_count, 0, `P03 ${layers[index]} missing count`);
      equal(entry.duplicate_count, 0, `P03 ${layers[index]} duplicate count`);
      equal(entry.order_errors, 0, `P03 ${layers[index]} order errors`);
      equal(entry.baseline_defect_id, null, `P03 ${layers[index]} baseline absence`);
    } else {
      requireText(entry.baseline_defect_id, `P03 ${layers[index]} divergent baseline`);
      const discrepancy = integer(entry.missing_count, "P03 divergent missing count") + integer(entry.duplicate_count, "P03 divergent duplicate count") + integer(entry.order_errors, "P03 divergent order errors");
      if (discrepancy === 0) fail(`P03 ${layers[index]} divergence has no measured discrepancy`);
    }
  });
  scanSecrets(value, "P03 browser evidence", false);
}

function validateP03IntegrityResult(result: RecordJson, label: string): void {
  requireText(result.artifact, `${label} artifact`);
  const missing = integer(result.missing_count, `${label} missing count`);
  const duplicates = integer(result.duplicate_count, `${label} duplicate count`);
  const orderErrors = integer(result.order_errors, `${label} order errors`);
  if (result.status === "pass") {
    equal(missing, 0, `${label} missing count`);
    equal(duplicates, 0, `${label} duplicate count`);
    equal(orderErrors, 0, `${label} order errors`);
    equal(result.baseline_defect_id, null, `${label} baseline absence`);
  } else {
    requireText(result.baseline_defect_id, `${label} divergent baseline`);
    if (missing + duplicates + orderErrors === 0) fail(`${label} divergence has no measured discrepancy`);
  }
}

export function validateP03EvidenceLinkage(
  descriptor: RecordJson,
  p03Evidence: RecordJson,
  candidateManifest: RecordJson
): void {
  validateP03BrowserEvidence(p03Evidence);
  for (const key of ["phase_id", "attempt", "candidate_head_sha", "served_head_sha", "candidate_tree_sha", "served_tree_sha"]) {
    equal(p03Evidence[key], descriptor[key], `P03/standard evidence ${key}`);
  }
  const browser = object(p03Evidence.browser, "P03 linked browser");
  const standardBrowser = object(descriptor.browser, "standard linked browser");
  const manifestBrowser = object(candidateManifest.browser_contract, "P03 manifest browser contract");
  equal(browser.mechanism, standardBrowser.mechanism, "P03 browser mechanism linkage");
  equal(browser.session_name, standardBrowser.session_name, "P03 browser session linkage");
  equal(browser.execution_mode, standardBrowser.execution_mode, "P03 browser mode linkage");
  equal(browser.binary_path, standardBrowser.chromium_binary, "P03 browser binary linkage");
  equal(browser.version, standardBrowser.chromium_version, "P03 browser version linkage");
  equal(browser.profile_policy, standardBrowser.profile_policy, "P03 browser profile linkage");
  equal(browser.execution_mode, manifestBrowser.execution_mode, "P03 manifest browser mode");
  equal(browser.binary_path, manifestBrowser.chromium_binary, "P03 manifest browser binary");
  equal(browser.version, manifestBrowser.chromium_version, "P03 manifest browser version");
  equal(browser.profile_policy, manifestBrowser.profile_policy, "P03 manifest browser profile");

  const journey = object(p03Evidence.journey, "P03 linked journey");
  const action = object(journey.action, "P03 linked action");
  const manifestActions = array(object(candidateManifest.scenario, "P03 manifest scenario").actions, "P03 manifest actions").map((entry) => object(entry, "P03 manifest action"));
  const manifestAction = manifestActions.find((entry) => entry.id === action.manifest_action_id);
  if (!manifestAction) fail("P03 journey action is absent from the active manifest");
  equal(action.command, manifestAction.command, "P03 journey command linkage");
  equal(action.command_count, manifestAction.expected_command_count, "P03 exact command cardinality linkage");
  for (const key of ["role", "accessible_name"]) {
    equal(object(action.control, "P03 action control")[key], object(manifestAction.control, "P03 manifest control")[key], `P03 control ${key} linkage`);
    equal(object(action.submit, "P03 action submit")[key], object(manifestAction.submit, "P03 manifest submit")[key], `P03 submit ${key} linkage`);
  }

  const attachment = object(p03Evidence.supervisor_attachment, "P03 supervisor attachment");
  const lease = object(descriptor.lease, "P03 linked lease");
  const stack = object(descriptor.stack, "P03 linked stack");
  equal(attachment.lease_id, lease.lease_id, "P03 lease ID linkage");
  equal(attachment.lease_sequence, lease.sequence, "P03 lease sequence linkage");
  equal(attachment.supervisor_identity, lease.owner_identity, "P03 supervisor identity linkage");
  equal(attachment.stack_configuration_sha256, stack.configuration_sha256, "P03 stack configuration linkage");
  equal(attachment.clean_tree, descriptor.clean_tree, "P03 clean-tree linkage");
  equal(attachment.hot_reload, descriptor.hot_reload, "P03 hot-reload linkage");

  const standardArtifacts = new Set<string>([
    string(descriptor.runtime_state_redacted, "P03 standard runtime artifact"),
    string(descriptor.tower_state_redacted, "P03 standard Tower artifact"),
    string(descriptor.store_state_redacted, "P03 standard store artifact"),
    string(descriptor.network, "P03 standard network artifact"),
    string(descriptor.websocket, "P03 standard WebSocket artifact"),
    string(descriptor.report, "P03 standard report artifact"),
    ...array(descriptor.screenshots, "P03 standard screenshots").map((item) => string(item, "P03 standard screenshot"))
  ]);
  for (const entryValue of array(p03Evidence.authority_chain, "P03 linked authority chain")) {
    const entry = object(entryValue, "P03 linked authority entry");
    if (!standardArtifacts.has(string(entry.artifact, "P03 authority artifact"))) fail("P03 authority artifact is absent from the standard evidence run");
    if (entry.baseline_defect_id !== null) resolveManifestBaseline(candidateManifest, string(entry.baseline_defect_id, "P03 authority baseline"), "P03 authority");
  }
  const reconstruction = object(p03Evidence.reconstruction, "P03 linked reconstruction");
  for (const key of ["ordinary_reload", "hard_reload", "navigation_away_back", "websocket_reconnect", "fresh_context"]) {
    const result = object(reconstruction[key], `P03 linked reconstruction ${key}`);
    if (!standardArtifacts.has(string(result.artifact, "P03 reconstruction artifact"))) fail(`P03 ${key} artifact is absent from the standard evidence run`);
    if (result.baseline_defect_id !== null) resolveManifestBaseline(candidateManifest, string(result.baseline_defect_id, "P03 reconstruction baseline"), `P03 reconstruction ${key}`);
  }

  const screenshots = array(p03Evidence.viewports, "P03 linked viewports").map((viewport) => object(viewport, "P03 linked viewport").screenshot_path);
  equal(JSON.stringify(screenshots), JSON.stringify(descriptor.screenshots), "P03 viewport screenshot linkage");
  const completion = object(p03Evidence.completion, "P03 completion");
  equal(completion.baseline_detected_count, array(candidateManifest.baseline_detected, "P03 manifest baselines").length, "P03 baseline count linkage");
  equal(JSON.stringify(completion.known_defects), JSON.stringify(candidateManifest.known_defects), "P03 known-defect linkage");
  equal(object(descriptor.metrics, "P03 metrics").status, "captured", "P03 procedure overhead metrics");
}

export function validateP03EvidenceArtifact(
  descriptor: RecordJson,
  candidateManifest: RecordJson,
  evidenceRoot: string
): void {
  equal(descriptor.phase_id, "P03", "P03 artifact phase");
  const relative = string(descriptor.p03_browser_evidence, "P03 browser evidence artifact");
  equal(relative, "p03-browser-evidence.json", "P03 fixed evidence artifact path");
  const path = safeChild(evidenceRoot, relative);
  if (!existsSync(path) || !statSync(path).isFile()) fail("referenced P03 browser evidence file missing: p03-browser-evidence.json");
  let p03Evidence: RecordJson;
  try { p03Evidence = JSON.parse(readFileSync(path, "utf8")) as RecordJson; }
  catch { fail("P03 browser evidence artifact is not valid JSON"); }
  validateP03EvidenceLinkage(descriptor, p03Evidence!, candidateManifest);
}

export function validateManifestRegistry(value: RecordJson): void {
  applySchemaFile("verification/gooseweb/schemas/manifest-registry.schema.json", value);
  const entries = array(value.active_manifests, "active manifests").map((entry) => object(entry, "active manifest"));
  ensureUnique(entries.map((entry) => string(entry.phase_id, "active manifest phase")), "active manifest phases");
  ensureUnique(entries.map((entry) => string(entry.path, "active manifest path")), "active manifest paths");
  for (const entry of entries) {
    const manifest = validateManifestFileTuple(entry);
    equal(object(manifest.scenario, "registered manifest scenario").phase_id, entry.phase_id, "registered manifest phase");
  }
  scanSecrets(value, "manifest registry", false);
}

export function validatePhaseGraphSeed(value: RecordJson): void {
  applySchemaFile("verification/gooseweb/schemas/phase-graph-seed.schema.json", value);
  const authority = object(value.authority_split, "authority split");
  equal(authority.tracked_role, "immutable_phase_graph_candidate_intent_and_lifecycle_seed", "tracked authority role");
  equal(authority.effective_state_rule, "seed_overlaid_by_validated_append_only_external_attestations", "effective state authority");
  equal(authority.post_review_tracked_mutation_prohibited, true, "post-review tracked mutation policy");
  const phases = array(value.phases, "phases").map((entry) => object(entry, "phase"));
  const ids = phases.map((entry) => string(entry.phase_id, "phase_id"));
  const expected = Array.from({ length: 57 }, (_, index) => `P${String(index).padStart(2, "0")}`);
  equal(JSON.stringify(ids), JSON.stringify(expected), "ordered P00-P56 phase IDs");
  const graph = new Map<string, string[]>();
  const stateById = new Map<string, string>();
  for (const entry of phases) {
    const id = string(entry.phase_id, "phase_id");
    const prerequisites = array(entry.prerequisites, `${id}.prerequisites`).map((dep) => string(dep, `${id} dependency`));
    for (const dep of prerequisites) {
      if (!PHASE.test(dep) || RANGE_OR_SHORTHAND.test(dep)) fail(`${id} has malformed or shorthand dependency ${dep}`);
      if (phaseNumber(dep) >= phaseNumber(id)) fail(`${id} depends on same/later phase ${dep}`);
    }
    graph.set(id, prerequisites);
    stateById.set(id, string(entry.state, `${id}.state`));
    validatePhaseHistory(entry);
  }
  if (!graph.get("P21")?.includes("P05")) fail("P21 must include P05");
  equal(JSON.stringify(graph.get("P56")), JSON.stringify(expected.slice(1, 56)), "P56 exact P01-P55 prerequisites");
  proveAcyclic(graph);
  for (const [id, prerequisites] of graph) {
    if (stateById.get(id) !== "blocked" && prerequisites.some((dep) => stateById.get(dep) !== "cleared")) fail(`${id} advanced before prerequisites cleared`);
  }
  const leases = array(value.seed_lease_history, "seed_lease_history").map((item) => object(item, "seed lease"));
  validateLeaseHistory(leases);
  const clearances = array(value.seed_clearance_history, "seed_clearance_history").map((item) => object(item, "seed clearance history entry"));
  validateLedgerCorrespondence(phases, leases, clearances);
  scanSecrets(value, "phase graph seed", false);
}

export interface LifecycleState {
  readonly effectiveStates: Readonly<Record<string, string>>;
  readonly eligiblePhases: readonly string[];
}

interface StoredAttestation {
  readonly path: string;
  readonly sha256: string;
  readonly document: RecordJson;
}

export function validateLifecycleStore(): LifecycleState {
  const storeRoot = safeLifecycleStoreRoot();
  const currentBytes = readFileSync(safeChild(storeRoot, "current.json"));
  const currentDocument = JSON.parse(currentBytes.toString("utf8")) as RecordJson;
  applySchemaFile("verification/gooseweb/schemas/lifecycle-current.schema.json", currentDocument);
  const current = object(currentDocument.current, "canonical lifecycle tip");
  const attestationsRoot = safeChild(storeRoot, "attestations");
  if (!existsSync(attestationsRoot) || !statSync(attestationsRoot).isDirectory()) fail("authoritative lifecycle attestations directory is missing");
  const files = readdirSync(attestationsRoot).filter((name) => name.endsWith(".json")).sort();
  if (files.length === 0) fail("authoritative lifecycle store has no attestation history");
  const nodes = new Map<string, StoredAttestation>();
  for (const file of files) {
    const path = `attestations/${file}`;
    if (!/^attestations\/gooseweb-lifecycle-[0-9]{6}-[a-f0-9]{64}\.json$/.test(path)) fail("lifecycle attestation filename is not sequence/hash addressed");
    const bytes = readFileSync(safeChild(storeRoot, path));
    const hash = hashBytes(bytes);
    let document: RecordJson;
    try { document = JSON.parse(bytes.toString("utf8")) as RecordJson; } catch { fail("stored lifecycle attestation is not JSON"); }
    applySchemaFile("verification/gooseweb/schemas/lifecycle-attestation.schema.json", document);
    const sequence = integer(document.attestation_sequence, "stored attestation sequence");
    const id = string(document.attestation_id, "stored attestation ID");
    equal(id, `gooseweb-lifecycle-${String(sequence).padStart(6, "0")}`, "stored attestation ID/sequence");
    equal(path, `attestations/${id}-${hash}.json`, "stored attestation filename/raw-byte hash");
    nodes.set(path, { path, sha256: hash, document });
  }

  const genesis: StoredAttestation[] = [];
  const children = new Map<string, StoredAttestation[]>();
  for (const node of nodes.values()) {
    if (node.document.predecessor === null) {
      equal(node.document.attestation_sequence, 1, "genesis attestation sequence");
      genesis.push(node);
      continue;
    }
    const predecessor = object(node.document.predecessor, "stored predecessor");
    const predecessorPath = string(predecessor.path, "stored predecessor path");
    const parent = nodes.get(predecessorPath);
    if (!parent) fail("stored lifecycle predecessor file is missing");
    equal(predecessor.sha256, parent.sha256, "stored predecessor raw-byte hash");
    equal(node.document.attestation_sequence, integer(parent.document.attestation_sequence, "predecessor sequence") + 1, "stored attestation sequence continuity");
    equal(JSON.stringify(node.document.phase_graph_seed), JSON.stringify(parent.document.phase_graph_seed), "immutable phase graph seed across lifecycle chain");
    if (time(node.document.generated_at) < time(parent.document.generated_at)) fail("lifecycle successor generated_at moves backward");
    const priorAttempts = array(parent.document.attempts, "predecessor attempts");
    equal(JSON.stringify(array(node.document.attempts, "successor attempts").slice(0, priorAttempts.length)), JSON.stringify(priorAttempts), "append-only attempt prefix");
    const siblings = children.get(predecessorPath) ?? [];
    siblings.push(node);
    children.set(predecessorPath, siblings);
  }
  if (genesis.length !== 1) fail("authoritative lifecycle store must contain exactly one genesis");
  for (const successors of children.values()) if (successors.length > 1) fail("authoritative lifecycle chain fork detected");
  const visited = new Set<string>();
  const chain: StoredAttestation[] = [];
  let tip = genesis[0]!;
  while (true) {
    if (visited.has(tip.path)) fail("authoritative lifecycle chain cycle detected");
    visited.add(tip.path);
    chain.push(tip);
    const successor = children.get(tip.path)?.[0];
    if (!successor) break;
    tip = successor;
  }
  if (visited.size !== nodes.size) fail("authoritative lifecycle store contains disconnected history");
  equal(current.path, tip.path, "canonical current pointer/tip path");
  equal(current.sha256, tip.sha256, "canonical current pointer raw-byte hash");
  equal(current.attestation_id, tip.document.attestation_id, "canonical current pointer attestation ID");
  equal(current.attestation_sequence, tip.document.attestation_sequence, "canonical current pointer sequence");
  let effective: LifecycleState | undefined;
  for (const node of chain) effective = validateLifecycleTip(node.document);
  return effective!;
}

function validateLifecycleTip(attestation: RecordJson): LifecycleState {
  const seedReference = object(attestation.phase_graph_seed, "phase graph seed reference");
  const seedPath = string(seedReference.path, "phase graph seed path");
  const seedHead = string(seedReference.candidate_head_sha, "phase graph seed candidate head");
  let seedBytes: Buffer;
  try { seedBytes = execFileSync("git", ["show", `${seedHead}:${seedPath}`], { cwd: root, stdio: ["ignore", "pipe", "pipe"] }); }
  catch { fail("phase graph seed is absent from its declared candidate head"); }
  equal(hashBytes(seedBytes), seedReference.blob_sha256, "phase graph seed candidate blob hash");
  let seed: RecordJson;
  try { seed = JSON.parse(seedBytes.toString("utf8")) as RecordJson; } catch { fail("candidate phase graph seed is not JSON"); }
  validatePhaseGraphSeed(seed);
  equal(seed.schema_revision, seedReference.schema_revision, "phase graph seed revision");
  const seedLeases = array(seed.seed_lease_history, "seed leases").map((item) => object(item, "seed lease"));
  const lastSeedLease = seedLeases.at(-1);
  if (!lastSeedLease) fail("phase graph seed must contain the migration lease checkpoint");
  equal(seedReference.last_seed_lease_id, lastSeedLease.lease_id, "last seed lease ID");
  equal(seedReference.last_seed_lease_sequence, lastSeedLease.sequence, "last seed lease sequence");
  equal(seedReference.last_seed_release_at, lastSeedLease.released_at, "last seed lease release");

  const externalLeases: RecordJson[] = [];
  const latestExternal = new Map<string, { status: string; leaseId: string }>();
  const attempts = array(attestation.attempts, "lifecycle attempts").map((item) => object(item, "lifecycle attempt"));
  for (const attempt of attempts) {
    const evidenceRoot = safeEvidenceRoot(string(attempt.evidence_root, "attested evidence root"));
    const descriptorPath = safeChild(evidenceRoot, "evidence-run.json");
    equal(hashBytes(readFileSync(descriptorPath)), attempt.evidence_descriptor_sha256, "attested evidence descriptor hash");
    const descriptor = JSON.parse(readFileSync(descriptorPath, "utf8")) as RecordJson;
    validateEvidence(descriptor, { checkFiles: true });
    const outcome = object(attempt.outcome, "attested outcome reference");
    const outcomePath = safeChild(evidenceRoot, string(outcome.record, "attested outcome record"));
    equal(hashBytes(readFileSync(outcomePath)), outcome.sha256, "attested outcome record hash");
    const record = JSON.parse(readFileSync(outcomePath, "utf8")) as RecordJson;
    equal(attempt.status, record.schema_revision === "gooseweb-exact-head-clearance/v4" ? "cleared" : "changes_required", "attestation/outcome status");
    if (attempt.status === "cleared") validateClearance(record);
    else validateReviewOutcome(record);
    const phase = string(record.phase_id, "outcome phase");
    const head = string(record.candidate_head_sha, "outcome candidate head");
    equal(attempt.evidence_root, `tmp/gg/gooseweb-migration/${phase}/${head.slice(0, 7)}/attempt-${integer(record.attempt, "outcome attempt")}/`, "attempt evidence root");
    equal(descriptor.phase_id, record.phase_id, "attested descriptor/outcome phase");
    equal(descriptor.attempt, record.attempt, "attested descriptor/outcome attempt");
    equal(descriptor.candidate_head_sha, record.candidate_head_sha, "attested descriptor/outcome head");
    const descriptorManifest = object(descriptor.manifest, "attested descriptor manifest");
    const outcomeManifest = object(record.manifest, "attested outcome manifest");
    equal(descriptorManifest.copy, "manifest.json", "attested manifest copy path");
    for (const key of ["path", "revision", "sha256"]) equal(descriptorManifest[key], outcomeManifest[key], `attested descriptor/outcome manifest ${key}`);
    let outcomeSeedBytes: Buffer;
    try { outcomeSeedBytes = execFileSync("git", ["show", `${head}:${seedPath}`], { cwd: root, stdio: ["ignore", "pipe", "pipe"] }); }
    catch { fail("outcome candidate does not contain the immutable phase graph seed"); }
    equal(hashBytes(outcomeSeedBytes), seedReference.blob_sha256, "outcome candidate phase graph seed hash");
    const lease = cloneRecord(object(record.lease, "outcome lease"));
    lease.reviewer = { browser_session: object(record.browser, "outcome browser").session_name! };
    lease.stack = record.stack!;
    externalLeases.push(lease);
    latestExternal.set(phase, { status: string(attempt.status, "attempt status"), leaseId: string(lease.lease_id, "external lease ID") });
    const terminalAt = attempt.status === "cleared" ? object(record.clearance, "clearance").issued_at : record.recorded_at;
    if (time(attestation.generated_at) < time(terminalAt)) fail("lifecycle attestation predates its outcome");
  }
  validateLeaseHistory([...seedLeases, ...externalLeases]);

  const phases = array(seed.phases, "seed phases").map((item) => object(item, "seed phase"));
  const effectiveStates: Record<string, string> = Object.fromEntries(phases.map((phase) => [string(phase.phase_id, "phase ID"), string(phase.state, "seed state")]));
  const claims: RecordJson[] = [];
  for (const [phase, latest] of latestExternal) {
    effectiveStates[phase] = latest.status === "cleared" ? "cleared" : "changes_required";
    if (latest.status === "cleared") claims.push({ phase_id: phase, state: "cleared", lease_id: latest.leaseId });
  }
  claims.sort((a, b) => phaseNumber(string(a.phase_id, "claim phase")) - phaseNumber(string(b.phase_id, "claim phase")));
  equal(JSON.stringify(attestation.claimed_effective_states), JSON.stringify(claims), "claimed effective phase states");
  const eligiblePhases = phases
    .filter((phase) => phase.phase_id !== "P00" && effectiveStates[string(phase.phase_id, "phase ID")] !== "cleared")
    .filter((phase) => array(phase.prerequisites, "phase prerequisites").every((dep) => effectiveStates[string(dep, "prerequisite")] === "cleared"))
    .map((phase) => string(phase.phase_id, "eligible phase"));
  equal(JSON.stringify(attestation.eligible_phases), JSON.stringify(eligiblePhases), "claimed eligible phases");
  scanSecrets(attestation, "lifecycle attestation", false);
  return { effectiveStates, eligiblePhases };
}

export interface EvidenceOptions {
  readonly checkFiles?: boolean;
  readonly expected?: RecordJson;
}

export function validateEvidence(value: RecordJson, options: EvidenceOptions = {}): void {
  applySchemaFile("verification/gooseweb/schemas/evidence-run.schema.json", value);
  const phase = string(value.phase_id, "evidence phase");
  const sha7 = string(value.sha7, "sha7");
  const attempt = integer(value.attempt, "attempt");
  const head = string(value.candidate_head_sha, "candidate head");
  if (!/^[a-f0-9]{40}$/.test(string(value.base_sha, "evidence base"))) fail("evidence base must be exact SHA");
  equal(value.reviewed_range, `${value.base_sha}..${head}`, "evidence reviewed range");
  equal(value.candidate_head_sha, value.served_head_sha, "evidence candidate/served head");
  equal(value.candidate_tree_sha, value.served_tree_sha, "evidence candidate/served tree");
  validateLease(object(value.lease, "evidence lease"));
  equal(object(value.lease, "evidence lease").phase_id, value.phase_id, "evidence lease phase");
  const evidenceStack = object(value.stack, "evidence stack");
  equal(evidenceStack.configuration_sha256, stackConfigurationHash(evidenceStack), "evidence stack hash");
  const evidenceReview = object(value.review, "evidence review");
  const evidenceBrowser = object(value.browser, "evidence browser");
  equal(evidenceReview.browser_session, evidenceBrowser.session_name, "evidence reviewer/browser session");
  if (!head.startsWith(sha7)) fail("evidence sha7 does not match candidate head");
  equal(value.root, `tmp/gg/gooseweb-migration/${phase}/${sha7}/attempt-${attempt}/`, "evidence root convention");
  validateManifestTuple(object(value.manifest, "evidence manifest"), phase);
  const candidateManifest = validateManifestAtGitHead(object(value.manifest, "evidence manifest"), head);
  equal(object(candidateManifest.scenario, "candidate evidence manifest scenario").phase_id, phase, "candidate manifest/evidence phase");
  if (phase === "P03") equal(value.p03_browser_evidence, "p03-browser-evidence.json", "P03 fixed evidence artifact path");
  validateBrowser(object(value.browser, "evidence browser"));
  if (options.expected) {
    for (const key of ["phase_id", "attempt", "base_sha", "reviewed_range", "candidate_head_sha", "candidate_tree_sha", "served_head_sha", "served_tree_sha", "clean_tree", "hot_reload"]) equal(value[key], options.expected[key], `expected evidence ${key}`);
    equal(JSON.stringify(value.manifest), JSON.stringify(options.expected.manifest), "expected evidence manifest");
    equal(JSON.stringify(value.browser), JSON.stringify(options.expected.browser), "expected browser tuple");
    for (const key of ["lease", "stack", "review"]) equal(JSON.stringify(value[key]), JSON.stringify(options.expected[key]), `expected evidence ${key} tuple`);
    equal(JSON.stringify(value.review_outcome), JSON.stringify(options.expected.review_outcome), "expected review outcome tuple");
  }
  const prohibited = array(object(value.redaction, "redaction").prohibited, "prohibited redaction categories");
  const required = ["credentials", "cookies", "CSRF values", "bearer tokens", "realtime tickets/query secrets", "provider auth", "raw image bytes", "secret config", "private keys"];
  equal(JSON.stringify(prohibited), JSON.stringify(required), "complete prohibited redaction vocabulary");
  equal(
    JSON.stringify(value.screenshots),
    JSON.stringify(["screenshots/1440x1000.png", "screenshots/820x1000.png", "screenshots/520x900.png"]),
    "exact required viewport screenshots"
  );
  if (options.checkFiles !== false) validateEvidenceFiles(value);
  scanSecrets(value, "evidence descriptor", false);
}

export interface ClearanceOptions {
  readonly expected?: RecordJson;
  readonly verifyGit?: boolean;
}

export function validateClearance(value: RecordJson, options: ClearanceOptions = {}): void {
  applySchemaFile("verification/gooseweb/schemas/exact-head-clearance.schema.json", value);
  if (!/^[a-f0-9]{40}$/.test(string(value.base_sha, "clearance base"))) fail("clearance base must be exact SHA");
  equal(value.candidate_head_sha, value.served_head_sha, "candidate/served head");
  equal(value.candidate_tree_sha, value.served_tree_sha, "candidate/served tree");
  equal(value.reviewed_range, `${value.base_sha}..${value.candidate_head_sha}`, "reviewed base/head range");
  if (time(object(value.clearance, "clearance").issued_at) < time(object(value.lease, "lease").released_at)) fail("clearance was issued before lease release");
  if (options.verifyGit !== false) {
    execFileSync("git", ["cat-file", "-e", `${string(value.base_sha, "base SHA")}^{commit}`], { cwd: root });
    const actualTree = execFileSync("git", ["rev-parse", `${string(value.candidate_head_sha, "candidate head")}^{tree}`], { cwd: root, encoding: "utf8", stdio: ["ignore", "pipe", "pipe"] }).trim();
    equal(value.candidate_tree_sha, actualTree, "candidate commit/tree relationship");
    try {
      execFileSync("git", ["merge-base", "--is-ancestor", string(value.base_sha, "base SHA"), string(value.candidate_head_sha, "candidate head")], { cwd: root, stdio: "ignore" });
    } catch {
      fail("declared base is not an ancestor of candidate head");
    }
    const mergeBase = execFileSync("git", ["merge-base", string(value.base_sha, "base SHA"), string(value.candidate_head_sha, "candidate head")], { cwd: root, encoding: "utf8" }).trim();
    equal(mergeBase, value.base_sha, "exact reviewed merge base");
  }
  validateManifestTuple(object(value.manifest, "clearance manifest"), string(value.phase_id, "clearance phase"));
  const candidateManifest = validateManifestAtGitHead(object(value.manifest, "clearance manifest"), string(value.candidate_head_sha, "candidate head"));
  equal(object(candidateManifest.scenario, "candidate manifest scenario").phase_id, value.phase_id, "candidate manifest/clearance phase");
  const lease = object(value.lease, "lease");
  validateLease(lease);
  equal(lease.phase_id, value.phase_id, "lease/clearance phase");
  const stack = object(value.stack, "stack");
  const ports = [stack.runtime_port, stack.tower_port, stack.gooseweb_port];
  ensureUnique(ports.map(String), "stack ports");
  equal(stack.configuration_sha256, stackConfigurationHash(stack), "stack configuration hash");
  const review = object(value.review, "review");
  if (review.implementer_identity === review.reviewer_identity) fail("reviewer and implementer must be distinct");
  const browser = object(value.browser, "clearance browser");
  validateBrowser(browser);
  equal(browser.session_name, review.browser_session, "browser/reviewer session name");
  const clearanceBaselines = array(value.baseline_detected, "baseline").map((entry) => object(entry, "baseline entry"));
  clearanceBaselines.forEach(validateBaseline);
  ensureUnique(clearanceBaselines.map((entry) => string(entry.defect_id, "clearance defect ID")), "clearance baseline defect IDs");
  equal(JSON.stringify(clearanceBaselines), JSON.stringify(candidateManifest.baseline_detected), "clearance/manifest baseline register");
  equal(object(value.clearance, "clearance").recipient_identity, "parallel_otter", "exact lead recipient identity");
  validateManifestClearancePolicy(candidateManifest, object(value.clearance, "clearance"));
  if (options.expected) {
    const expected = options.expected;
    for (const key of ["phase_id", "attempt", "base_sha", "reviewed_range", "candidate_head_sha", "candidate_tree_sha", "served_head_sha", "served_tree_sha", "clean_tree", "hot_reload"]) equal(value[key], expected[key], `expected ${key}`);
    for (const key of ["manifest", "lease", "stack", "review", "browser", "clearance", "baseline_detected"]) equal(JSON.stringify(value[key]), JSON.stringify(expected[key]), `expected ${key} tuple`);
  }
  scanSecrets(value, "clearance", false);
}

export function validateClearanceHistory(records: RecordJson[]): void {
  records.forEach((record) => validateClearance(record));
  const leases = records.map((record) => object(record.lease, "clearance lease"));
  validateLeaseHistory(leases, leases.length ? integer(leases[0]!.sequence, "first clearance lease sequence") : 1);
  ensureUnique(records.map((record) => string(object(record.review, "review").browser_session, "browser session")), "browser session names");
}

export function validateBrowserCaptures(consoleCapture: RecordJson, networkCapture: RecordJson, manifest?: RecordJson): void {
  const consoleAllowlist = readJson("verification/gooseweb/allowlists/console.json");
  const networkAllowlist = readJson("verification/gooseweb/allowlists/network.json");
  applySchemaInline(consoleCaptureSchema(), consoleCapture, "console capture");
  applySchemaInline(networkCaptureSchema(), networkCapture, "network capture");
  equal(consoleAllowlist.schema_revision, "gooseweb-console-allowlist/v6", "console allowlist revision");
  equal(networkAllowlist.schema_revision, "gooseweb-network-allowlist/v4", "network allowlist revision");
  const consoleBoundary = object(consoleAllowlist.capture_boundary, "console capture boundary");
  equal(consoleBoundary.source, "unfiltered agent-browser console output after document.readyState complete plus one second", "console capture source");
  equal(consoleBoundary.filtering, "none", "console filtering");
  equal(consoleBoundary.normalization, "none", "console normalization");
  equal(consoleBoundary.warnings_errors_exceptions_always_fail, true, "console failure policy");
  const capturedMessages = array(consoleCapture.messages, "console messages");
  if (capturedMessages.some((item) => ["warn", "error"].includes(string(object(item, "console message").level, "console level")))) fail("console capture contains warning/error/exception");
  const variants = array(consoleAllowlist.permitted_exact_variants, "console variants").map((item) => object(item, "console variant"));
  ensureUnique(variants.map((variant) => string(variant.variant_id, "console variant ID")), "console variant IDs");
  for (const variant of variants) if (array(variant.messages, "variant messages").some((item) => ["warn", "error"].includes(string(object(item, "variant message").level, "variant console level")))) fail("console allowlist variant contains warning/error");
  const benignLiterals: RecordJson[] = [
    { level: "debug", message: "[vite] connecting..." },
    { level: "debug", message: "[vite] connected." },
    { level: "info", message: "%cDownload the React DevTools for a better development experience: https://react.dev/link/react-devtools font-weight:bold" }
  ];
  const expectedPowerSet = Array.from({ length: 8 }, (_, mask) => benignLiterals.filter((_, index) => (mask & (1 << index)) !== 0)).map(multisetSignature).sort();
  const actualPowerSet = variants.map((variant) => multisetSignature(array(variant.messages, "variant messages"))).sort();
  equal(JSON.stringify(actualPowerSet), JSON.stringify(expectedPowerSet), "exact benign console literal power set");
  const matches = variants.filter((variant) => sameMultiset(capturedMessages, array(variant.messages, "variant messages")));
  if (matches.length !== 1) fail("console capture does not match exactly one finite benign variant");
  const boundary = object(networkAllowlist.capture_boundary, "network capture boundary");
  equal(boundary.source, "unfiltered agent-browser network requests including successes", "network capture source");
  equal(boundary.raw_capture_retained, true, "raw network retention");
  equal(boundary.query_values_retained, false, "network query-value redaction");
  equal(boundary.query_keys_retained, true, "network query-key retention");
  equal(boundary.failure_filtering_prohibited, true, "network failure filtering policy");
  equal(boundary.unclassified_filtering_prohibited, true, "unclassified network filtering policy");
  equal(boundary.ignored_status_min, 200, "ignored success status minimum");
  equal(boundary.ignored_status_max, 399, "ignored success status maximum");
  equal(JSON.stringify(boundary.ignored_same_origin_success_resource_types), JSON.stringify(["stylesheet", "font", "script", "module"]), "exact ignored static resource types");
  const ignoredTypes = new Set(array(boundary.ignored_same_origin_success_resource_types, "ignored resource types").map((item) => string(item, "resource type")));
  const evaluated = array(networkCapture.raw_http, "raw HTTP capture").filter((item) => {
    const request = object(item, "HTTP request");
    const ignorableStatus = integer(request.status, "HTTP status") >= Number(boundary.ignored_status_min) && integer(request.status, "HTTP status") <= Number(boundary.ignored_status_max);
    return !(request.same_origin === true && ignorableStatus && ignoredTypes.has(string(request.resource_type, "resource type")));
  }).map((item) => {
    const request = object(item, "HTTP request");
    return { method: request.method!, path: request.path!, status: request.status!, resource_type: request.resource_type!, baseline_defect_id: request.baseline_defect_id! } as RecordJson;
  });
  for (const item of array(networkCapture.raw_http, "raw HTTP capture")) {
    const request = object(item, "HTTP request");
    const failed = integer(request.status, "HTTP status") >= 400;
    const baselineId = string(request.baseline_defect_id, "HTTP baseline ID");
    if (!failed && baselineId) fail("successful HTTP request cannot carry a failure baseline");
    if (failed) {
      if (!manifest) fail("failed HTTP request requires the validated manifest");
      const baseline = resolveManifestBaseline(manifest, baselineId, "HTTP failure");
      equal(baseline.scenario_id, object(manifest.scenario, "manifest scenario").stable_scenario_id, "HTTP baseline scenario");
    }
  }
  const baseHttp: RecordJson[] = [
    { method: "GET", path: "/", status: 200, resource_type: "document", baseline_defect_id: "" },
    { method: "POST", path: "/api/dev-ticket", status: 200, resource_type: "api", baseline_defect_id: "" }
  ];
  const faviconFailure: RecordJson = { method: "GET", path: "/favicon.ico", status: 404, resource_type: "other", baseline_defect_id: "BASE-P01-FAVICON-NOT-FOUND" };
  const httpVariants = array(networkAllowlist.permitted_exact_evaluated_http_variants, "HTTP variants").map((item) => object(item, "HTTP variant"));
  ensureUnique(httpVariants.map((variant) => string(variant.variant_id, "HTTP variant ID")), "HTTP variant IDs");
  const expectedHttpVariants = [baseHttp, [...baseHttp, faviconFailure]].map(multisetSignature).sort();
  const actualHttpVariants = httpVariants.map((variant) => multisetSignature(array(variant.requests, "HTTP variant requests"))).sort();
  equal(JSON.stringify(actualHttpVariants), JSON.stringify(expectedHttpVariants), "exact evaluated HTTP variant set");
  const httpMatches = httpVariants.filter((variant) => sameMultiset(evaluated, array(variant.requests, "HTTP variant requests")));
  if (httpMatches.length !== 1) fail("evaluated HTTP activity does not match exactly one finite variant");
  const websocket = object(networkCapture.websocket, "WebSocket capture");
  if (websocket.availability === "available") exactMultiset(array(websocket.events, "WebSocket events"), array(networkAllowlist.exact_websocket_events, "WebSocket allowlist"), "WebSocket events");
  else {
    equal(websocket.inference_prohibited, true, "unavailable WebSocket inference policy");
    requireText(websocket.reason, "unavailable WebSocket reason");
    const defectId = string(websocket.baseline_defect_id, "unavailable WebSocket baseline mapping");
    requireText(defectId, "unavailable WebSocket baseline mapping");
    if (!manifest) fail("unavailable WebSocket capture requires the validated manifest");
    const baseline = resolveManifestBaseline(manifest, defectId, "unavailable WebSocket");
    equal(baseline.scenario_id, object(manifest.scenario, "manifest scenario").stable_scenario_id, "WebSocket baseline scenario");
  }
  scanSecrets(consoleCapture, "console capture", false);
  scanSecrets(networkCapture, "network capture", false);
}

export function applySchemaFile(schemaPath: string, value: Json): void {
  applySchemaInline(readJson(schemaPath), value, schemaPath);
}

export function applySchemaInline(schema: Schema, value: Json, label: string): void {
  validateSchemaNode(schema, value, label);
}

export function stackConfigurationHash(stack: RecordJson): string {
  const canonical: RecordJson = {};
  for (const key of ["dev_dir", "runtime_port", "tower_port", "gooseweb_port", "source_configuration", "branch", "mode"]) canonical[key] = stack[key] ?? null;
  return hashBytes(Buffer.from(JSON.stringify(canonical)));
}

function validateEvidenceFiles(descriptor: RecordJson): void {
  const evidenceRoot = safeEvidenceRoot(string(descriptor.root, "evidence root"));
  const outcome = object(descriptor.review_outcome, "review outcome descriptor");
  const textKeys = ["environment", "console", "network", "websocket", "runtime_state_redacted", "tower_state_redacted", "store_state_redacted", "checks", "report"];
  const relativePaths = [
    string(object(descriptor.manifest, "manifest").copy, "manifest copy"),
    ...textKeys.map((key) => string(descriptor[key], key)),
    string(outcome.record, "review outcome record"),
    ...array(descriptor.screenshots, "screenshots").map((item) => string(item, "screenshot"))
  ];
  if (descriptor.phase_id === "P03") relativePaths.push(string(descriptor.p03_browser_evidence, "P03 browser evidence"));
  ensureUnique(relativePaths, "evidence paths");
  for (const relative of relativePaths) {
    const path = safeChild(evidenceRoot, relative);
    if (!existsSync(path) || !statSync(path).isFile()) fail(`referenced evidence file missing: ${relative}`);
    if (relative.endsWith(".png")) {
      const expected = /([0-9]+)x([0-9]+)\.png$/.exec(relative);
      const dimensions = decodePngDimensions(readFileSync(path));
      if (!expected || dimensions.width !== Number(expected[1]) || dimensions.height !== Number(expected[2])) fail(`screenshot dimensions do not match ${relative}`);
      continue;
    }
    const content = readFileSync(path, "utf8");
    if (SECRET_VALUE.test(content)) fail(`secret-bearing referenced evidence: ${relative}`);
    if (relative.endsWith(".json")) scanSecrets(JSON.parse(content) as Json, `evidence file ${relative}`, false);
  }
  const manifestCopy = JSON.parse(readFileSync(safeChild(evidenceRoot, string(object(descriptor.manifest, "manifest").copy, "manifest copy")), "utf8")) as RecordJson;
  validateManifest(manifestCopy);
  equal(object(manifestCopy.scenario, "manifest scenario").phase_id, descriptor.phase_id, "evidence manifest phase");
  equal(
    hashBytes(readFileSync(safeChild(evidenceRoot, string(object(descriptor.manifest, "manifest").copy, "manifest copy")))),
    object(descriptor.manifest, "manifest").sha256,
    "evidence manifest copy hash"
  );
  if (descriptor.phase_id === "P03") {
    validateP03EvidenceArtifact(descriptor, manifestCopy, evidenceRoot);
  }
  const consoleCapture = JSON.parse(readFileSync(safeChild(evidenceRoot, string(descriptor.console, "console")), "utf8")) as RecordJson;
  const networkCapture = JSON.parse(readFileSync(safeChild(evidenceRoot, string(descriptor.network, "network")), "utf8")) as RecordJson;
  validateBrowserCaptures(consoleCapture, networkCapture, manifestCopy);
  const environment = JSON.parse(readFileSync(safeChild(evidenceRoot, string(descriptor.environment, "environment")), "utf8")) as RecordJson;
  for (const key of ["phase_id", "attempt", "base_sha", "reviewed_range", "candidate_head_sha", "candidate_tree_sha", "served_head_sha", "served_tree_sha", "clean_tree", "hot_reload"]) equal(environment[key], descriptor[key], `environment/${key}`);
  equal(environment.plan_sha256, APPROVED_PLAN_SHA256, "environment plan hash");
  equal(environment.manifest_sha256, object(descriptor.manifest, "manifest").sha256, "environment manifest hash");
  equal(environment.browser_session, object(descriptor.browser, "browser").session_name, "environment browser session");
  equal(environment.browser_execution_mode, object(descriptor.browser, "browser").execution_mode, "environment browser mode");
  for (const key of ["chromium_binary", "chromium_version", "profile_policy"]) equal(environment[key], object(descriptor.browser, "browser")[key], `environment browser ${key}`);
  for (const key of ["lease", "stack", "review"]) equal(JSON.stringify(environment[key]), JSON.stringify(descriptor[key]), `environment ${key} tuple`);
  const websocketCapture = JSON.parse(readFileSync(safeChild(evidenceRoot, string(descriptor.websocket, "websocket")), "utf8")) as RecordJson;
  equal(JSON.stringify(websocketCapture), JSON.stringify(networkCapture.websocket), "network/WebSocket capture linkage");
  const outcomeRecord = JSON.parse(readFileSync(safeChild(evidenceRoot, string(outcome.record, "review outcome record")), "utf8")) as RecordJson;
  if (outcome.status === "cleared") validateClearance(outcomeRecord);
  else if (outcome.status === "changes_required") validateReviewOutcome(outcomeRecord);
  else fail("unknown review outcome status");
  equal(outcomeRecord.status === "changes_required" ? "changes_required" : "cleared", outcome.status, "review outcome record/status");
  for (const key of ["phase_id", "attempt", "base_sha", "reviewed_range", "candidate_head_sha", "candidate_tree_sha", "served_head_sha", "served_tree_sha", "clean_tree", "hot_reload"]) equal(outcomeRecord[key], descriptor[key], `evidence/outcome ${key}`);
  for (const key of ["lease", "stack", "review", "browser"]) equal(JSON.stringify(outcomeRecord[key]), JSON.stringify(descriptor[key]), `evidence/outcome ${key} tuple`);
  const evidenceManifest = object(descriptor.manifest, "evidence manifest");
  const outcomeManifest = object(outcomeRecord.manifest, "outcome manifest");
  for (const key of ["path", "revision", "sha256"]) equal(outcomeManifest[key], evidenceManifest[key], `evidence/outcome manifest ${key}`);
}

function validatePhaseHistory(phase: RecordJson): void {
  const id = string(phase.phase_id, "phase ID");
  const history = array(phase.history, `${id}.history`).map((item) => object(item, "transition"));
  if (id === "P00" && phase.state === "cleared" && history.length === 0) return;
  if (history.length === 0) {
    if (phase.state !== "blocked") fail(`${id} non-blocked state lacks history`);
    return;
  }
  const allowed = new Set(["blocked>implementing", "implementing>candidate_ready_for_review", "candidate_ready_for_review>under_review", "under_review>changes_required", "under_review>cleared", "changes_required>implementing"]);
  let priorTo: Json | undefined;
  let priorTimestamp = -Infinity;
  history.forEach((entry, index) => {
    equal(entry.sequence, index + 1, `${id} transition sequence`);
    if (!allowed.has(`${entry.from}>${entry.to}`)) fail(`${id} illegal transition ${entry.from}>${entry.to}`);
    if (priorTo !== undefined) equal(entry.from, priorTo, `${id} contiguous transition history`);
    const timestamp = time(entry.at);
    if (timestamp < priorTimestamp) fail(`${id} transition timestamps move backward`);
    priorTimestamp = timestamp;
    priorTo = entry.to;
  });
  equal(priorTo, phase.state, `${id} current state/history`);
}

function validateLedgerCorrespondence(phases: RecordJson[], leases: RecordJson[], clearances: RecordJson[]): void {
  const leaseIds = new Set(leases.map((lease) => string(lease.lease_id, "lease ID")));
  for (const phase of phases) {
    const id = string(phase.phase_id, "phase ID");
    const history = array(phase.history, "history").map((item) => object(item, "transition"));
    for (const transition of history) {
      if (transition.lease_id !== null && !leaseIds.has(string(transition.lease_id, "transition lease ID"))) fail(`${id} transition references unknown lease`);
    }
    if (phase.state === "cleared" && id !== "P00" && !clearances.some((entry) => entry.phase_id === id && entry.status === "cleared")) fail(`${id} cleared without clearance history`);
  }
  for (const clearance of clearances) {
    const leaseId = string(clearance.lease_id, "clearance lease ID");
    if (!leaseIds.has(leaseId)) fail("clearance references unknown lease");
    const phase = phases.find((entry) => entry.phase_id === clearance.phase_id);
    if (!phase) fail("clearance references unknown phase");
    if (!array(phase.history, "phase history").some((entry) => object(entry, "transition").lease_id === leaseId)) fail("clearance lease absent from phase history");
    const lease = leases.find((entry) => entry.lease_id === leaseId)!;
    if (lease.outcome !== "cleared") fail("clearance references a lease that did not clear");
    equal(clearance.candidate_head_sha, object(lease.candidate, "lease candidate").head_sha, "clearance/lease head");
    equal(clearance.candidate_tree_sha, object(lease.candidate, "lease candidate").tree_sha, "clearance/lease tree");
    equal(clearance.manifest_sha256, object(lease.manifest, "lease manifest").sha256, "clearance/lease manifest");
  }
  for (const lease of leases) {
    const phase = phases.find((entry) => entry.phase_id === lease.phase_id);
    if (!phase) fail("lease references unknown phase");
    const transitions = array(phase.history, "lease phase history").map((entry) => object(entry, "transition"));
    const opened = transitions.find((entry) => entry.to === "under_review" && entry.lease_id === lease.lease_id);
    if (!opened) fail("lease has no matching under_review transition");
    equal(opened.at, lease.acquired_at, "lease acquisition/phase transition timestamp");
    const closed = transitions.find((entry) => ["changes_required", "cleared"].includes(String(entry.to)) && entry.lease_id === lease.lease_id);
    if (!closed || time(closed.at) < time(lease.released_at)) fail("lease release has no matching terminal review transition");
  }
}

function validateLeaseHistory(leases: RecordJson[], startSequence = 1): void {
  let lastSequence = startSequence - 1;
  const ids = new Set<string>();
  const browserSessions = new Set<string>();
  for (const [index, lease] of leases.entries()) {
    validateLease(lease);
    const id = string(lease.lease_id, "lease ID");
    const sequence = integer(lease.sequence, "lease sequence");
    equal(sequence, startSequence + index, "globally contiguous lease sequence");
    equal(id, `gooseweb-migration-${String(sequence).padStart(6, "0")}`, "lease ID/sequence");
    const prior = object(lease.prior_lease_termination_evidence, "prior lease termination");
    if (sequence === 1) equal(prior.status, "no_prior_lease", "genesis lease prior status");
    else equal(prior.status, "terminated_and_cleaned", "non-genesis prior termination status");
    if (ids.has(id) || sequence <= lastSequence) fail("lease IDs must be unique and sequences globally monotonic");
    ids.add(id); lastSequence = sequence;
    const session = string(object(lease.reviewer, "lease reviewer").browser_session, "lease browser session");
    if (browserSessions.has(session)) fail("ledger browser session names must be globally unique");
    browserSessions.add(session);
    ensureUnique([lease.stack].flatMap((item) => {
      const stack = object(item, "lease stack");
      return [stack.runtime_port, stack.tower_port, stack.gooseweb_port].map(String);
    }), "lease stack ports");
    if (sequence > 1 && !string(object(lease.prior_lease_termination_evidence, "prior lease termination").reference, "prior reference").includes(`gooseweb-migration-${String(sequence - 1).padStart(6, "0")}`)) fail("lease does not reference prior lease termination");
  }
  const sorted = [...leases].sort((a, b) => time(a.acquired_at) - time(b.acquired_at));
  for (let index = 1; index < sorted.length; index += 1) {
    if (time(sorted[index]!.acquired_at) < time(sorted[index - 1]!.released_at)) fail("lease intervals overlap across phase/P56 records");
  }
}

function validateLease(lease: RecordJson): void {
  const prior = object(lease.prior_lease_termination_evidence, "prior lease termination");
  requireText(prior.reference, "prior termination reference");
  const process = object(lease.managed_process, "managed process");
  const acquired = time(lease.acquired_at), started = time(process.started_at), stopped = time(process.stopped_at), cleaned = time(process.cleanup_completed_at), released = time(lease.released_at);
  if (!(acquired <= started && started < stopped && stopped <= cleaned && cleaned <= released)) fail("lease release must follow managed-process stop and cleanup");
}

export function validateReviewOutcome(record: RecordJson): void {
  applySchemaFile("verification/gooseweb/schemas/review-outcome.schema.json", record);
  equal(record.reviewed_range, `${record.base_sha}..${record.candidate_head_sha}`, "non-clearance reviewed range");
  equal(record.candidate_head_sha, record.served_head_sha, "non-clearance candidate/served head");
  equal(record.candidate_tree_sha, record.served_tree_sha, "non-clearance candidate/served tree");
  const lease = object(record.lease, "non-clearance lease");
  validateLease(lease);
  equal(lease.phase_id, record.phase_id, "non-clearance lease phase");
  const stack = object(record.stack, "non-clearance stack");
  equal(stack.configuration_sha256, stackConfigurationHash(stack), "non-clearance stack hash");
  const review = object(record.review, "non-clearance review");
  const browser = object(record.browser, "non-clearance browser");
  equal(review.browser_session, browser.session_name, "non-clearance reviewer/browser session");
  validateManifestTuple(object(record.manifest, "non-clearance manifest"), string(record.phase_id, "non-clearance phase"));
  if (time(record.recorded_at) < time(lease.released_at)) fail("changes-required outcome was recorded before lease release");
  validateGitRecord(record);
  const candidateManifest = validateManifestAtGitHead(object(record.manifest, "non-clearance manifest"), string(record.candidate_head_sha, "candidate head"));
  equal(object(candidateManifest.scenario, "candidate manifest scenario").phase_id, record.phase_id, "candidate manifest/non-clearance phase");
}

function validateManifestAtGitHead(tuple: RecordJson, head: string): RecordJson {
  let bytes: Buffer;
  try { bytes = execFileSync("git", ["show", `${head}:${string(tuple.path, "manifest path")}`], { cwd: root, stdio: ["ignore", "pipe", "pipe"] }); }
  catch { fail("candidate head does not contain declared manifest path"); }
  equal(hashBytes(bytes), tuple.sha256, "candidate manifest blob hash");
  let candidate: RecordJson;
  try { candidate = JSON.parse(bytes.toString("utf8")) as RecordJson; } catch { fail("candidate manifest blob is not JSON"); }
  validateManifest(candidate);
  equal(candidate.manifest_revision, tuple.revision, "candidate manifest blob revision");
  validateActiveManifestAtGitHead(tuple, string(object(candidate.scenario, "candidate manifest scenario").phase_id, "candidate manifest phase"), head);
  return candidate;
}

function validateActiveManifestAtGitHead(tuple: RecordJson, phase: string, head: string): void {
  let bytes: Buffer;
  try { bytes = execFileSync("git", ["show", `${head}:verification/gooseweb/manifest-registry.json`], { cwd: root, stdio: ["ignore", "pipe", "pipe"] }); }
  catch { fail("candidate head does not contain the authoritative manifest registry"); }
  let registry: RecordJson;
  try { registry = JSON.parse(bytes.toString("utf8")) as RecordJson; } catch { fail("candidate manifest registry is not JSON"); }
  applySchemaFile("verification/gooseweb/schemas/manifest-registry.schema.json", registry);
  const entries = array(registry.active_manifests, "candidate active manifests").map((entry) => object(entry, "candidate active manifest"));
  ensureUnique(entries.map((entry) => string(entry.phase_id, "candidate active manifest phase")), "candidate active manifest phases");
  const matches = entries.filter((entry) => entry.phase_id === phase);
  if (matches.length !== 1) fail(`candidate ${phase} must have exactly one authoritative active manifest`);
  for (const key of ["path", "revision", "sha256"]) equal(tuple[key], matches[0]![key], `candidate active ${phase} manifest ${key}`);
}

export function validateGitRecord(record: RecordJson): void {
  execFileSync("git", ["cat-file", "-e", `${string(record.base_sha, "base SHA")}^{commit}`], { cwd: root, stdio: "ignore" });
  const tree = execFileSync("git", ["rev-parse", `${string(record.candidate_head_sha, "candidate head")}^{tree}`], { cwd: root, encoding: "utf8", stdio: ["ignore", "pipe", "pipe"] }).trim();
  equal(record.candidate_tree_sha, tree, "Git candidate tree");
  try { execFileSync("git", ["merge-base", "--is-ancestor", string(record.base_sha, "base SHA"), string(record.candidate_head_sha, "candidate head")], { cwd: root, stdio: "ignore" }); }
  catch { fail("declared base is not an ancestor of candidate head"); }
  equal(execFileSync("git", ["merge-base", string(record.base_sha, "base SHA"), string(record.candidate_head_sha, "candidate head")], { cwd: root, encoding: "utf8" }).trim(), record.base_sha, "exact reviewed merge base");
}

function decodePngDimensions(bytes: Buffer): { width: number; height: number } {
  if (bytes.length < 45 || bytes.subarray(0, 8).toString("hex") !== "89504e470d0a1a0a") fail("screenshot is not a complete PNG");
  let offset = 8;
  let width = 0, height = 0, bitDepth = 0, colorType = -1, interlace = -1;
  let sawHeader = false, sawEnd = false;
  const idat: Buffer[] = [];
  while (offset + 12 <= bytes.length) {
    const length = bytes.readUInt32BE(offset);
    const end = offset + 12 + length;
    if (end > bytes.length) fail("PNG chunk is truncated");
    const type = bytes.subarray(offset + 4, offset + 8);
    const data = bytes.subarray(offset + 8, offset + 8 + length);
    if (crc32(Buffer.concat([type, data])) !== bytes.readUInt32BE(offset + 8 + length)) fail("PNG chunk CRC is invalid");
    const name = type.toString("ascii");
    if (!sawHeader) {
      if (name !== "IHDR" || length !== 13) fail("PNG does not begin with a valid IHDR");
      width = data.readUInt32BE(0); height = data.readUInt32BE(4); bitDepth = data[8]!; colorType = data[9]!; interlace = data[12]!;
      if (width < 1 || height < 1 || data[10] !== 0 || data[11] !== 0 || interlace !== 0) fail("PNG IHDR is unsupported or invalid");
      sawHeader = true;
    } else if (name === "IHDR") fail("PNG has duplicate IHDR");
    else if (name === "IDAT") idat.push(Buffer.from(data));
    else if (name === "IEND") { if (length !== 0) fail("PNG IEND is invalid"); sawEnd = true; offset = end; break; }
    offset = end;
  }
  if (!sawHeader || !sawEnd || idat.length === 0 || offset !== bytes.length) fail("PNG is missing IDAT/IEND or has trailing bytes");
  let decoded: Buffer;
  try { decoded = inflateSync(Buffer.concat(idat)); } catch { fail("PNG IDAT is not decodable"); }
  const channels = ({ 0: 1, 2: 3, 3: 1, 4: 2, 6: 4 } as Record<number, number>)[colorType];
  if (!channels || ![0, 2, 4, 6].includes(colorType) || bitDepth !== 8) fail("PNG color type/bit depth is invalid");
  const scanline = width * channels + 1;
  if (decoded.length !== scanline * height) fail("PNG decoded payload has wrong dimensions");
  for (let row = 0; row < height; row += 1) if (decoded[row * scanline]! > 4) fail("PNG scanline filter is invalid");
  return { width, height };
}

function crc32(bytes: Uint8Array): number {
  let crc = 0xffffffff;
  for (const byte of bytes) {
    crc ^= byte;
    for (let bit = 0; bit < 8; bit += 1) crc = (crc >>> 1) ^ (crc & 1 ? 0xedb88320 : 0);
  }
  return (crc ^ 0xffffffff) >>> 0;
}

function validateManifestTuple(tuple: RecordJson, expectedPhase?: string): RecordJson {
  const manifest = validateManifestFileTuple(tuple);
  if (expectedPhase) {
    equal(object(manifest.scenario, "manifest scenario").phase_id, expectedPhase, "manifest/record phase");
    const active = activeManifestForPhase(expectedPhase);
    for (const key of ["path", "revision", "sha256"]) equal(tuple[key], active[key], `active ${expectedPhase} manifest ${key}`);
  }
  return manifest;
}

function validateManifestFileTuple(tuple: RecordJson): RecordJson {
  const path = string(tuple.path, "manifest path");
  const manifestsRoot = resolve(root, "verification/gooseweb/manifests");
  if (!/^verification\/gooseweb\/manifests\/[a-z0-9][a-z0-9._/-]*\.json$/.test(path) || path.includes("..") || path.includes("//")) fail("manifest path is outside safe source-controlled manifest root");
  const absolute = resolve(root, path);
  if (!absolute.startsWith(`${manifestsRoot}${sep}`) || !existsSync(absolute) || !statSync(absolute).isFile()) fail("manifest path is missing or escapes manifest root");
  integer(tuple.revision, "manifest revision");
  equal(tuple.sha256, hashBytes(readFileSync(absolute)), "manifest hash");
  const manifest = JSON.parse(readFileSync(absolute, "utf8")) as RecordJson;
  validateManifest(manifest);
  equal(manifest.manifest_revision, tuple.revision, "manifest revision");
  return manifest;
}

function activeManifestForPhase(phase: string): RecordJson {
  const registry = readJson("verification/gooseweb/manifest-registry.json");
  validateManifestRegistry(registry);
  const matches = array(registry.active_manifests, "active manifests")
    .map((entry) => object(entry, "active manifest"))
    .filter((entry) => entry.phase_id === phase);
  if (matches.length !== 1) fail(`${phase} must have exactly one authoritative active manifest`);
  return matches[0]!;
}

export function validateClearancePhasePolicy(phase: string, baselines: RecordJson[], clearance: RecordJson, productClearance: string): void {
  const number = phaseNumber(phase);
  if (number <= 5) {
    equal(clearance.scope, "verification_infrastructure_only", "P01-P05 clearance scope");
    equal(clearance.product_approved, false, "P01-P05 product approval");
    if (productClearance === "approved") fail("P01-P05 infrastructure clearance cannot carry an approved product manifest");
  } else if (number === 56) {
    if (baselines.length !== 0) fail("P56 clearance requires empty baseline register");
    equal(clearance.scope, "integration_release", "P56 clearance scope");
    equal(clearance.product_approved, true, "P56 product approval");
    equal(productClearance, "approved", "P56 manifest product clearance");
  } else {
    if (baselines.length !== 0) fail("P06-P55 product clearance requires empty baseline register");
    equal(clearance.scope, "product_phase", "product phase clearance scope");
    equal(clearance.product_approved, true, "product phase approval");
    equal(productClearance, "approved", "product phase manifest clearance");
  }
}

export function validateManifestClearancePolicy(candidateManifest: RecordJson, clearance: RecordJson): void {
  validateManifest(candidateManifest);
  const scenario = object(candidateManifest.scenario, "candidate manifest scenario");
  const baselines = array(candidateManifest.baseline_detected, "candidate manifest baselines").map((entry) => object(entry, "candidate manifest baseline"));
  validateClearancePhasePolicy(
    string(scenario.phase_id, "candidate manifest phase"),
    baselines,
    clearance,
    string(scenario.product_clearance, "manifest product clearance")
  );
}

function validateBrowser(browser: RecordJson): void {
  if (browser.execution_mode !== "headless" || browser.headed_mode_prohibited !== true) fail("headed browser evidence is prohibited");
  requireText(browser.chromium_binary, "Chromium binary");
  requireText(browser.chromium_version, "Chromium version");
}

function validateBaseline(entry: RecordJson): void {
  if (!CORRECTION.test(string(entry.owning_correction_phase, "correction phase"))) fail("baseline correction phase must be P06-P10");
  equal(entry.product_scenario_status, "blocked_not_approved", "baseline product status");
}

function resolveManifestBaseline(manifest: RecordJson, defectId: string, label: string): RecordJson {
  requireText(defectId, `${label} baseline ID`);
  const matches = array(manifest.baseline_detected, "manifest baselines").map((item) => object(item, "baseline")).filter((entry) => entry.defect_id === defectId);
  if (matches.length !== 1) fail(`${label} baseline does not resolve exactly once in manifest`);
  validateBaseline(matches[0]!);
  return matches[0]!;
}

function validateSchemaNode(schema: Schema, value: Json, path: string): void {
  if (schema.const !== undefined && !deepEqual(value, schema.const)) fail(`${path} violates const`);
  if (schema.enum !== undefined && !array(schema.enum, `${path} enum`).some((item) => deepEqual(item, value))) fail(`${path} is outside enum`);
  if (schema.type !== undefined) {
    const types = Array.isArray(schema.type) ? schema.type : [schema.type];
    if (!types.some((item) => matchesType(value, String(item)))) fail(`${path} has wrong type`);
  }
  if (typeof value === "string") {
    if (typeof schema.minLength === "number" && value.length < schema.minLength) fail(`${path} is too short`);
    if (typeof schema.pattern === "string" && !new RegExp(schema.pattern).test(value)) fail(`${path} does not match pattern`);
    if (schema.format === "date-time" && !Number.isFinite(Date.parse(value))) fail(`${path} is not date-time`);
  }
  if (typeof value === "number" && typeof schema.minimum === "number" && value < schema.minimum) fail(`${path} is below minimum`);
  if (typeof value === "number" && typeof schema.maximum === "number" && value > schema.maximum) fail(`${path} is above maximum`);
  if (Array.isArray(value)) {
    if (typeof schema.minItems === "number" && value.length < schema.minItems) fail(`${path} has too few items`);
    if (typeof schema.maxItems === "number" && value.length > schema.maxItems) fail(`${path} has too many items`);
    if (schema.uniqueItems === true) ensureUnique(value.map((item) => JSON.stringify(item)), path);
    if (schema.items) value.forEach((item, index) => validateSchemaNode(object(schema.items, `${path}.items schema`), item, `${path}[${index}]`));
  }
  if (value && typeof value === "object" && !Array.isArray(value)) {
    const record = value as RecordJson;
    const properties = schema.properties ? object(schema.properties, `${path}.properties`) : {};
    for (const key of schema.required ? array(schema.required, `${path}.required`) : []) if (!(string(key, "required key") in record)) fail(`${path} omitted required field ${key}`);
    if (schema.additionalProperties === false) for (const key of Object.keys(record)) if (!(key in properties)) fail(`${path} has unexpected field ${key}`);
    for (const [key, childSchema] of Object.entries(properties)) if (key in record) validateSchemaNode(object(childSchema, `${path}.${key} schema`), record[key]!, `${path}.${key}`);
  }
}

function consoleCaptureSchema(): Schema {
  return { type: "object", additionalProperties: false, required: ["schema_revision", "capture_source", "unfiltered", "messages"], properties: { schema_revision: { const: "gooseweb-console-capture/v3" }, capture_source: { const: "agent-browser console" }, unfiltered: { const: true }, messages: { type: "array", items: { type: "object", additionalProperties: false, required: ["level", "message"], properties: { level: { enum: ["debug", "info", "warn", "error"] }, message: { type: "string", minLength: 1 } } } } } };
}

function networkCaptureSchema(): Schema {
  const httpItem = { type: "object", additionalProperties: false, required: ["method", "path", "query_keys", "status", "resource_type", "same_origin", "baseline_defect_id"], properties: { method: { type: "string", minLength: 1 }, path: { type: "string", pattern: "^/[^?]*$" }, query_keys: { type: "array", uniqueItems: true, items: { type: "string", minLength: 1 } }, status: { type: "integer", minimum: 100, maximum: 599 }, resource_type: { enum: ["document", "api", "stylesheet", "font", "script", "module", "websocket", "other"] }, same_origin: { type: "boolean" }, baseline_defect_id: { type: "string" } } };
  const wsEvent = { type: "object", additionalProperties: false, required: ["event", "code"], properties: { event: { enum: ["open", "close"] }, code: { type: "integer", minimum: 0, maximum: 4999 } } };
  return { type: "object", additionalProperties: false, required: ["schema_revision", "capture_source", "unfiltered", "raw_http", "websocket"], properties: { schema_revision: { const: "gooseweb-network-capture/v3" }, capture_source: { const: "agent-browser network requests" }, unfiltered: { const: true }, raw_http: { type: "array", items: httpItem }, websocket: { type: "object", additionalProperties: false, required: ["availability", "events", "inference_prohibited", "reason", "baseline_defect_id"], properties: { availability: { enum: ["available", "unavailable"] }, events: { type: "array", items: wsEvent }, inference_prohibited: { type: "boolean" }, reason: { type: "string" }, baseline_defect_id: { type: "string" } } } } };
}


function exactMultiset(actual: Json[], expected: Json[], label: string): void {
  const normalize = (items: Json[]) => items.map((item) => JSON.stringify(item)).sort();
  equal(JSON.stringify(normalize(actual)), JSON.stringify(normalize(expected)), `exact ${label} allowlist`);
}

function sameMultiset(actual: Json[], expected: Json[]): boolean {
  return multisetSignature(actual) === multisetSignature(expected);
}

function multisetSignature(items: Json[]): string { return JSON.stringify(items.map((item) => JSON.stringify(item)).sort()); }

function scanSecrets(value: Json, path: string, allowVocabulary: boolean): void {
  if (Array.isArray(value)) { value.forEach((item, index) => scanSecrets(item, `${path}[${index}]`, allowVocabulary)); return; }
  if (value && typeof value === "object") {
    for (const [key, item] of Object.entries(value)) {
      if (SECRET_KEY.test(key) && typeof item === "string" && item.trim() && !REDACTED.test(item)) fail(`secret-bearing field ${path}.${key}`);
      scanSecrets(item, `${path}.${key}`, allowVocabulary || key === "prohibited");
    }
    return;
  }
  if (typeof value === "string" && !allowVocabulary && SECRET_VALUE.test(value)) fail(`secret-bearing value at ${path}`);
}

function safeEvidenceRoot(relative: string): string {
  if (!relative.startsWith("tmp/gg/gooseweb-migration/") || relative.includes("..")) fail("unsafe evidence root");
  const path = resolve(root, relative);
  if (!existsSync(path)) fail("evidence root does not exist");
  return realpathSync(path);
}

function safeLifecycleStoreRoot(): string {
  const path = resolve(root, "tmp/gg/gooseweb-migration/lifecycle");
  if (!existsSync(path) || !statSync(path).isDirectory()) fail("authoritative lifecycle store is missing");
  return realpathSync(path);
}

function safeChild(parent: string, relative: string): string {
  if (relative.startsWith("/") || relative.includes("..")) fail(`unsafe evidence path ${relative}`);
  const path = resolve(parent, relative);
  if (!path.startsWith(`${parent}${sep}`)) fail(`evidence path escapes root: ${relative}`);
  if (existsSync(path)) {
    const real = realpathSync(path);
    if (!real.startsWith(`${parent}${sep}`)) fail(`evidence symlink escapes root: ${relative}`);
  }
  return path;
}

function proveAcyclic(graph: Map<string, string[]>): void {
  const visiting = new Set<string>(), visited = new Set<string>();
  const visit = (id: string): void => { if (visiting.has(id)) fail(`dependency cycle at ${id}`); if (visited.has(id)) return; visiting.add(id); for (const dep of graph.get(id) ?? []) visit(dep); visiting.delete(id); visited.add(id); };
  for (const id of graph.keys()) visit(id);
}

function matchesType(value: Json, type: string): boolean {
  if (type === "null") return value === null;
  if (type === "array") return Array.isArray(value);
  if (type === "object") return Boolean(value) && typeof value === "object" && !Array.isArray(value);
  if (type === "integer") return typeof value === "number" && Number.isInteger(value);
  return typeof value === type;
}

function deepEqual(a: Json, b: Json): boolean { return JSON.stringify(a) === JSON.stringify(b); }
function hashBytes(value: Uint8Array): string { return createHash("sha256").update(value).digest("hex"); }
function cloneRecord(value: RecordJson): RecordJson { return structuredClone(value); }
function ensureUnique(values: string[], label: string): void { if (new Set(values).size !== values.length) fail(`${label} must be unique`); }
function object(value: Json | undefined, label: string): RecordJson { if (!value || typeof value !== "object" || Array.isArray(value)) fail(`${label} must be an object`); return value as RecordJson; }
function array(value: Json | undefined, label: string): Json[] { if (!Array.isArray(value)) fail(`${label} must be an array`); return value; }
function string(value: Json | undefined, label: string): string { if (typeof value !== "string") fail(`${label} must be a string`); return value; }
function integer(value: Json | undefined, label: string): number { if (typeof value !== "number" || !Number.isInteger(value)) fail(`${label} must be an integer`); return value; }
function requireText(value: Json | undefined, label: string): void { if (!string(value, label).trim()) fail(`${label} must not be empty`); }
function equal(actual: Json | undefined, expected: Json | undefined, label: string): void { if (!deepEqual(actual ?? null, expected ?? null)) fail(`${label} changed: expected ${JSON.stringify(expected)}, received ${JSON.stringify(actual)}`); }
function phaseNumber(id: string): number { return Number(id.slice(1)); }
function time(value: Json | undefined): number { const parsed = Date.parse(string(value, "timestamp")); if (!Number.isFinite(parsed)) fail("invalid timestamp"); return parsed; }
function fail(message: string): never { throw new Error(message); }
