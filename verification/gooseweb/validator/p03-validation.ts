import { existsSync, readFileSync, statSync } from "node:fs";
import {
  applySchemaFile,
  array,
  equal,
  fail,
  hashBytes,
  integer,
  object,
  resolveManifestBaseline,
  requireText,
  safeChild,
  scanSecrets,
  string,
  validateP03ReconstructionNetworkLinkage,
  type RecordJson
} from "./validate";

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
  const binaryPath = string(browser.binary_path, "P03 browser binary path");
  if (!binaryPath.startsWith("/") || !/(?:Google Chrome|Chromium)(?:\.app)?\//.test(binaryPath)) fail("P03 browser binary path is not a local Chrome/Chromium executable");
  const binaryVersion = string(browser.version, "P03 browser binary version");
  const binaryMajor = binaryVersion.split(".")[0];
  const reducedMatch = /HeadlessChrome\/([0-9]+\.0\.0\.0)(?:\s|;)/.exec(string(browser.user_agent, "P03 browser user agent"));
  if (!reducedMatch) fail("P03 user agent does not prove reduced headless Chromium");
  const reduction = object(browser.user_agent_reduction, "P03 user-agent reduction");
  equal(reduction.mode, "reduced_major_only", "P03 user-agent reduction mode");
  equal(reduction.token_version, reducedMatch[1], "P03 reduced user-agent token");
  equal(reducedMatch[1]!.split(".")[0], binaryMajor, "P03 Chrome binary/reduced-UA major version");
  const userAgentData = object(browser.user_agent_data, "P03 user-agent data");
  const fullVersionList = array(userAgentData.full_version_list, "P03 fullVersionList").map((entry) => object(entry, "P03 fullVersionList entry"));
  if (userAgentData.availability === "available") {
    equal(userAgentData.unavailable_reason, "", "P03 available user-agent-data reason");
    const chromeBrands = fullVersionList.filter((entry) => /^(?:Chromium|Google Chrome)$/.test(string(entry.brand, "P03 browser brand")));
    if (chromeBrands.length === 0) fail("P03 high-entropy identity has no Chrome/Chromium brand");
    for (const entry of chromeBrands) equal(entry.version, binaryVersion, `P03 high-entropy ${entry.brand} full version`);
    if (binaryPath.includes("Google Chrome") && !chromeBrands.some((entry) => entry.brand === "Google Chrome")) fail("P03 Google Chrome binary lacks Google Chrome high-entropy brand");
  } else {
    equal(fullVersionList.length, 0, "P03 unavailable fullVersionList");
    requireText(userAgentData.unavailable_reason, "P03 unavailable user-agent-data reason");
  }
  for (const [name, captureValue] of Object.entries(object(value.captures, "P03 captures"))) {
    const capture = object(captureValue, `P03 ${name} capture`);
    equal(capture.complete, true, `P03 ${name} capture completeness`);
    equal(capture.unexpected_failures, 0, `P03 ${name} unexpected failures`);
    equal(capture.redacted_at_capture, true, `P03 ${name} capture redaction`);
  }
  const networkReconstruction = object(value.network_reconstruction, "P03 network reconstruction");
  equal(networkReconstruction.artifact, "network.json", "P03 reconstruction network artifact");
  equal(networkReconstruction.capture_schema_revision, "gooseweb-network-capture/v4", "P03 reconstruction network schema");
  equal(networkReconstruction.unfiltered_raw_requests_retained, true, "P03 reconstruction raw network retention");
  equal(
    JSON.stringify(networkReconstruction.ordered_segments),
    JSON.stringify(["initial_load", "ordinary_reload", "fresh_context"]),
    "P03 ordered reconstruction network segments"
  );
  const ordinaryReload = object(value.ordinary_reload_capability, "P03 ordinary-reload capability");
  equal(ordinaryReload.mechanism, "agent_browser_reload", "P03 ordinary-reload mechanism");
  equal(ordinaryReload.command, "reload", "P03 ordinary-reload command");
  for (const key of ["command_succeeded", "navigation_observed", "capture_complete", "semantic_state_observed"]) equal(ordinaryReload[key], true, `P03 ordinary-reload ${key}`);
  equal(ordinaryReload.document_request_count, 1, "P03 ordinary-reload document request count");
  equal(ordinaryReload.dev_ticket_request_count, 1, "P03 ordinary-reload dev-ticket request count");
  const freshIsolation = object(value.fresh_context_isolation, "P03 fresh-context isolation");
  equal(freshIsolation.artifact, "fresh-context-observation.json", "P03 fresh-context observation artifact");
  for (const key of ["navigation_observed", "capture_complete", "semantic_state_observed"]) equal(freshIsolation[key], true, `P03 fresh-context ${key}`);
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
  for (const key of ["ordinary_reload", "navigation_away_back", "websocket_reconnect", "fresh_context"]) {
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
    if (divergent.length === 0) fail("P03 declares a first divergence without measured divergent layers");
    const earliest = authority.find((entry) => entry.status === "baseline_divergent")!;
    equal(earliest.layer, value.first_divergent_layer, "P03 earliest divergent layer");
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
  requireText(manifestAction.expected_submitted_text, "P03 manifest expected submitted text");
  equal(action.submitted_text, manifestAction.expected_submitted_text, "P03 manifest submitted-text linkage");
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
    string(descriptor.p03_fresh_context_observation, "P03 standard fresh-context observation"),
    ...array(descriptor.screenshots, "P03 standard screenshots").map((item) => string(item, "P03 standard screenshot"))
  ]);
  for (const entryValue of array(p03Evidence.authority_chain, "P03 linked authority chain")) {
    const entry = object(entryValue, "P03 linked authority entry");
    if (!standardArtifacts.has(string(entry.artifact, "P03 authority artifact"))) fail("P03 authority artifact is absent from the standard evidence run");
    if (entry.baseline_defect_id !== null) resolveManifestBaseline(candidateManifest, string(entry.baseline_defect_id, "P03 authority baseline"), "P03 authority");
  }
  const reconstruction = object(p03Evidence.reconstruction, "P03 linked reconstruction");
  for (const key of ["ordinary_reload", "navigation_away_back", "websocket_reconnect", "fresh_context"]) {
    const result = object(reconstruction[key], `P03 linked reconstruction ${key}`);
    if (!standardArtifacts.has(string(result.artifact, "P03 reconstruction artifact"))) fail(`P03 ${key} artifact is absent from the standard evidence run`);
    if (result.baseline_defect_id !== null) resolveManifestBaseline(candidateManifest, string(result.baseline_defect_id, "P03 reconstruction baseline"), `P03 reconstruction ${key}`);
  }

  const screenshots = array(p03Evidence.viewports, "P03 linked viewports").map((viewport) => object(viewport, "P03 linked viewport").screenshot_path);
  equal(JSON.stringify(screenshots), JSON.stringify(descriptor.screenshots), "P03 viewport screenshot linkage");
  equal(object(p03Evidence.network_reconstruction, "P03 linked network reconstruction").artifact, descriptor.network, "P03 reconstruction network artifact linkage");
  equal(object(p03Evidence.fresh_context_isolation, "P03 linked fresh-context isolation").artifact, descriptor.p03_fresh_context_observation, "P03 fresh-context observation artifact linkage");
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
  validateP03FreshContextArtifact(descriptor, p03Evidence!, candidateManifest, evidenceRoot);
  const authority = array(p03Evidence!.authority_chain, "P03 parsed authority chain").map((entry) => object(entry, "P03 authority claim"));
  for (const claim of authority.slice(0, 3)) {
    const relativeArtifact = string(claim.artifact, "P03 authority artifact path");
    const artifactPath = safeChild(evidenceRoot, relativeArtifact);
    if (!existsSync(artifactPath) || !statSync(artifactPath).isFile()) fail(`referenced P03 authority artifact missing: ${relativeArtifact}`);
    let observation: RecordJson;
    try { observation = JSON.parse(readFileSync(artifactPath, "utf8")) as RecordJson; }
    catch { fail(`P03 authority artifact is not valid JSON: ${relativeArtifact}`); }
    applySchemaFile("verification/gooseweb/schemas/p03-authority-observation.schema.json", observation!);
    equal(observation!.phase_id, descriptor.phase_id, `P03 ${claim.layer} artifact phase`);
    equal(observation!.attempt, descriptor.attempt, `P03 ${claim.layer} artifact attempt`);
    for (const key of ["layer", "correlation_id", "semantic_identity", "cursor_or_version", "content_sha256", "status", "observed_instances", "missing_count", "duplicate_count", "order_errors", "baseline_defect_id"]) {
      equal(observation![key], claim[key], `P03 ${claim.layer} artifact/${key}`);
    }
    scanSecrets(observation!, `P03 authority artifact ${relativeArtifact}`, false);
  }
}

export function validateP03FreshContextArtifact(
  descriptor: RecordJson,
  p03Evidence: RecordJson,
  candidateManifest: RecordJson,
  evidenceRoot: string
): RecordJson {
  const relative = string(descriptor.p03_fresh_context_observation, "P03 fresh-context observation artifact");
  equal(relative, "fresh-context-observation.json", "P03 fixed fresh-context observation path");
  const path = safeChild(evidenceRoot, relative);
  if (!existsSync(path) || !statSync(path).isFile()) fail("referenced P03 fresh-context observation file missing");
  let observation: RecordJson;
  try { observation = JSON.parse(readFileSync(path, "utf8")) as RecordJson; }
  catch { fail("P03 fresh-context observation artifact is not valid JSON"); }
  applySchemaFile("verification/gooseweb/schemas/p03-fresh-context-observation.schema.json", observation!);
  for (const key of ["phase_id", "attempt", "candidate_head_sha", "candidate_tree_sha"]) equal(observation![key], descriptor[key], `P03 fresh-context artifact/${key}`);
  equal(observation!.approved_plan_sha256, object(candidateManifest.approved_plan, "P03 manifest plan").sha256, "P03 fresh-context artifact/manifest plan");
  const standardBrowser = object(descriptor.browser, "P03 standard browser");
  equal(observation!.initial_session_name, standardBrowser.session_name, "P03 fresh-context initial session linkage");
  const freshSession = string(observation!.fresh_session_name, "P03 fresh session");
  if (freshSession === standardBrowser.session_name) fail("P03 fresh-context observation reused initial session");
  const expectedPrefix = `gooseweb-p03-${string(descriptor.candidate_head_sha, "P03 candidate head").slice(0, 7)}-a${integer(descriptor.attempt, "P03 attempt")}-`;
  if (!freshSession.startsWith(expectedPrefix)) fail("P03 fresh-context observation session is not bound to candidate/attempt");
  equal(freshSession, standardBrowser.fresh_session_name, "P03 fresh-context standard session linkage");
  const observedBrowser = object(observation!.browser, "P03 fresh-context browser");
  equal(observedBrowser.mechanism, standardBrowser.mechanism, "P03 fresh-context browser mechanism");
  equal(observedBrowser.execution_mode, standardBrowser.execution_mode, "P03 fresh-context browser mode");
  equal(observedBrowser.binary_path, standardBrowser.chromium_binary, "P03 fresh-context Chrome binary");
  equal(observedBrowser.version, standardBrowser.chromium_version, "P03 fresh-context Chrome version");
  equal(observedBrowser.real_local_chromium, true, "P03 fresh-context real Chromium");
  equal(object(observation!.profile, "P03 fresh profile").policy, standardBrowser.profile_policy, "P03 fresh-context profile policy");
  const profile = object(observation!.profile, "P03 fresh profile");
  equal(profile.profile_id, standardBrowser.fresh_profile_id, "P03 fresh-context standard profile linkage");
  equal(profile.context_id, standardBrowser.fresh_context_id, "P03 fresh-context standard context linkage");
  const preLogin = object(observation!.pre_login, "P03 fresh pre-login probe");
  equal(preLogin.prior_context_nonce, null, "P03 fresh pre-login nonce");
  const transition = object(observation!.transition, "P03 fresh transition");
  const descriptorIsolation = object(p03Evidence.fresh_context_isolation, "P03 descriptor fresh isolation");
  for (const key of ["navigation_observed", "capture_complete", "semantic_state_observed"]) equal(transition[key], descriptorIsolation[key], `P03 fresh artifact/descriptor ${key}`);
  const reconstruction = object(object(p03Evidence.reconstruction, "P03 reconstruction").fresh_context, "P03 fresh reconstruction");
  equal(JSON.stringify(observation!.reconstruction), JSON.stringify(reconstruction), "P03 fresh artifact/reconstruction linkage");
  if (reconstruction.baseline_defect_id !== null) resolveManifestBaseline(candidateManifest, string(reconstruction.baseline_defect_id, "P03 fresh baseline"), "P03 fresh-context artifact");
  equal(observation!.fresh_context_nonce, object(p03Evidence.reconstruction, "P03 reconstruction").fresh_context_nonce, "P03 fresh artifact nonce linkage");
  equal(observation!.stale_first_context_state_detected, object(p03Evidence.reconstruction, "P03 reconstruction").stale_context_detected, "P03 fresh artifact stale-state linkage");
  equal(object(observation!.disposal, "P03 fresh disposal").initial_context_disposed, object(p03Evidence.reconstruction, "P03 reconstruction").old_context_disposed, "P03 initial disposal linkage");
  scanSecrets(observation!, "P03 fresh-context observation", false);
  return observation!;
}
