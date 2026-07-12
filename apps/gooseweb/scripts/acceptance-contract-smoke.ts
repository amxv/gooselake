import assert from "node:assert/strict";
import { mkdirSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { resolve } from "node:path";
import { deflateSync } from "node:zlib";
import {
  APPROVED_PLAN_SHA256,
  applySchemaFile,
  readJson,
  sha256,
  stackConfigurationHash,
  validateBrowserCaptures,
  validateClearance,
  validateClearanceHistory,
  validateEvidence,
  validateGitRecord,
  validateLedger,
  validateManifest,
  validateManifestClearancePolicy,
  validateManifestRegistry,
  validateReviewOutcome,
  type Json,
  type RecordJson
} from "../../../verification/gooseweb/validator/validate";

const root = resolve(import.meta.dir, "../../..");
const P01_MANIFEST_PATH = "verification/gooseweb/manifests/p01-team-comms-live.json";
const VALIDATOR_MANIFEST_PATH = "verification/gooseweb/validator/fixtures/manifests/validator-p01-empty.json";
const ALTERNATE_MANIFEST_PATH = "verification/gooseweb/manifests/validator-alternate.json";
const P01_BASE_SHA = "ca88bfe56719f69fe59151372e0d5aa76b2c92ab";
const manifest = readJson(P01_MANIFEST_PATH);
const validatorManifest = readJson(VALIDATOR_MANIFEST_PATH);
const manifestRegistry = readJson("verification/gooseweb/manifest-registry.json");
const ledger = readJson("verification/gooseweb/ledger/phase-state.json");
const clearance = readJson("verification/gooseweb/validator/fixtures/valid-clearance.json");
const evidence = readJson("verification/gooseweb/validator/fixtures/valid-evidence-run.json");
const validNonClearance = readJson("verification/gooseweb/validator/fixtures/valid-review-outcome.json");
const consoleCapture: RecordJson = {
  schema_revision: "gooseweb-console-capture/v3",
  capture_source: "agent-browser console",
  unfiltered: true,
  messages: [
    { level: "debug", message: "[vite] connecting..." },
    { level: "debug", message: "[vite] connected." }
  ]
};
const networkCapture: RecordJson = {
  schema_revision: "gooseweb-network-capture/v3",
  capture_source: "agent-browser network requests",
  unfiltered: true,
  raw_http: [
    { method: "GET", path: "/", query_keys: [], status: 200, resource_type: "document", same_origin: true, baseline_defect_id: "" },
    { method: "GET", path: "/src/styles.css", query_keys: ["v"], status: 200, resource_type: "stylesheet", same_origin: true, baseline_defect_id: "" },
    { method: "GET", path: "/node_modules/react.js", query_keys: ["v"], status: 200, resource_type: "module", same_origin: true, baseline_defect_id: "" },
    { method: "GET", path: "/fonts/geist.woff2", query_keys: [], status: 200, resource_type: "font", same_origin: true, baseline_defect_id: "" },
    { method: "POST", path: "/api/dev-ticket", query_keys: [], status: 200, resource_type: "api", same_origin: true, baseline_defect_id: "" },
    { method: "GET", path: "/favicon.ico", query_keys: [], status: 404, resource_type: "other", same_origin: true, baseline_defect_id: "BASE-P01-FAVICON-NOT-FOUND" }
  ],
  websocket: {
    availability: "available",
    events: [{ event: "open", code: 0 }],
    inference_prohibited: false,
    reason: "",
    baseline_defect_id: ""
  }
};

assert.equal(sha256("tmp/gg/golden-goose-gooseweb-migration-implementation-plan.md"), APPROVED_PLAN_SHA256, "immutable amended plan changed");
validateManifest(manifest);
validateManifest(validatorManifest);
validateManifestRegistry(manifestRegistry);
const reusableP02Manifest = change(change(change(change(manifest, "manifest_id", "GW-P02-GENERIC-001"), "scenario.stable_scenario_id", "GW-P02-GENERIC-001"), "scenario.phase_id", "P02"), "baseline_detected", []);
validateManifest(change(reusableP02Manifest, "scenario.product_clearance", "pending"));
const approvedP06Manifest = manifestForPhase("P06", "approved");
const approvedP56Manifest = manifestForPhase("P56", "approved");
validateManifestClearancePolicy(change(reusableP02Manifest, "scenario.product_clearance", "pending"), { scope: "verification_infrastructure_only", product_approved: false });
validateManifestClearancePolicy(approvedP06Manifest, { scope: "product_phase", product_approved: true });
validateManifestClearancePolicy(approvedP56Manifest, { scope: "integration_release", product_approved: true });
const laterPhaseBase = "d7da340c94f4cb34692a122696717e72f357fac1";
let laterPhaseEvidence = change(evidence, "phase_id", "P02");
laterPhaseEvidence = change(laterPhaseEvidence, "lease.phase_id", "P02");
laterPhaseEvidence = change(laterPhaseEvidence, "base_sha", laterPhaseBase);
laterPhaseEvidence = change(laterPhaseEvidence, "reviewed_range", `${laterPhaseBase}..${evidence.candidate_head_sha}`);
applySchemaFile("verification/gooseweb/schemas/evidence-run.schema.json", laterPhaseEvidence);
validateGitRecord({ base_sha: laterPhaseBase, candidate_head_sha: evidence.candidate_head_sha!, candidate_tree_sha: evidence.candidate_tree_sha! });
validateLedger(ledger);
validateClearance(clearance, { expected: clearance, verifyGit: true });
applySchemaFile("verification/gooseweb/schemas/exact-head-clearance.schema.json", change(clearance, "baseline_detected", []));
validateEvidence(evidence, { checkFiles: false, expected: evidence });
validateReviewOutcome(validNonClearance);
validateBrowserCaptures(consoleCapture, networkCapture, manifest);
validateBrowserCaptures(change(consoleCapture, "messages", [
  { level: "debug", message: "[vite] connecting..." },
  { level: "info", message: "%cDownload the React DevTools for a better development experience: https://react.dev/link/react-devtools font-weight:bold" }
]), networkCapture, manifest);
validateBrowserCaptures(change(consoleCapture, "messages", [
  { level: "debug", message: "[vite] connecting..." },
  { level: "debug", message: "[vite] connected." },
  { level: "info", message: "%cDownload the React DevTools for a better development experience: https://react.dev/link/react-devtools font-weight:bold" }
]), networkCapture, manifest);
validateBrowserCaptures(consoleCapture, change(networkCapture, "websocket", { availability: "unavailable", events: [], inference_prohibited: true, reason: "agent-browser exposes no redacted frame capture", baseline_defect_id: "BASE-P01-WEBSOCKET-OBSERVER-UNAVAILABLE" }), manifest);
validateSchemasAgainstDocuments();
validateReferencedEvidence();

const negativeCases: [string, () => void][] = [
  ["changed approved plan SHA", () => validateManifest(change(manifest, "approved_plan.sha256", "0".repeat(64)))],
  ["changed plan path", () => validateManifest(change(manifest, "approved_plan.path", "wrong-plan.md"))],
  ["headed manifest mode", () => validateManifest(change(manifest, "browser_contract.execution_mode", "headed"))],
  ["missing Chromium version", () => validateManifest(omit(manifest, "browser_contract.chromium_version"))],
  ["empty scenario actions", () => validateManifest(change(manifest, "scenario.actions", []))],
  ["missing authority chain", () => validateManifest(omit(manifest, "scenario.authority_chain"))],
  ["changed cardinality", () => validateManifest(change(manifest, "scenario.cardinality.commands", 2))],
  ["missing responsive assertion", () => validateManifest(change(manifest, "responsive.assertions", []))],
  ["missing required state expectation", () => validateManifest(omit(manifest, "states.loading.expectation"))],
  ["fixture correctness enabled", () => validateManifest(change(manifest, "fixtures.product_correctness_use", true))],
  ["extra nested manifest field", () => validateManifest(change(manifest, "scenario.forged", true))],
  ["baseline missing scenario", () => validateManifest(omit(manifest, "baseline_detected.0.scenario_id"))],
  ["baseline missing divergent layer", () => validateManifest(omit(manifest, "baseline_detected.0.first_divergent_layer"))],
  ["baseline missing evidence", () => validateManifest(change(manifest, "baseline_detected.0.evidence_references", []))],
  ["baseline missing repair phase", () => validateManifest(omit(manifest, "baseline_detected.0.owning_correction_phase"))],
  ["baseline missing downstream gates", () => validateManifest(change(manifest, "baseline_detected.0.affected_downstream_gates", []))],
  ["baseline labeled product approval", () => validateManifest(change(manifest, "baseline_detected.0.product_scenario_status", "approved"))],
  ["known defects nonempty", () => validateManifest(change(manifest, "known_defects", [{ id: "defect" }]))],
  ["manifest phase P00", () => validateManifest(change(manifest, "scenario.phase_id", "P00"))],
  ["stale manifest schema revision", () => validateManifest(change(manifest, "schema_revision", "gooseweb-acceptance-manifest/v3"))],
  ["future manifest schema revision", () => validateManifest(change(manifest, "schema_revision", "gooseweb-acceptance-manifest/v999"))],
  ["duplicate active manifest phase", () => validateManifestRegistry(change(manifestRegistry, "active_manifests.1", clone((manifestRegistry.active_manifests as Json[])[0])))],
  ["validator fixture is not an evidence manifest path", () => applySchemaFile("verification/gooseweb/schemas/exact-head-clearance.schema.json", change(clearance, "manifest.path", VALIDATOR_MANIFEST_PATH))],
  ["same-phase manifest cannot replace active manifest", () => withAlternateManifest((path, hash) => {
    let substituted = change(clearance, "manifest.path", path);
    substituted = change(substituted, "manifest.revision", 1);
    substituted = change(substituted, "manifest.sha256", hash);
    substituted = change(substituted, "baseline_detected", []);
    validateClearance(substituted, { verifyGit: false });
  })],
  ["not applicable without reason", () => validateManifest(omit(manifest, "non_applicable.rollback.reason"))],
  ["changed clearance base", () => validateClearance(change(clearance, "base_sha", "a".repeat(40)), { expected: clearance })],
  ["changed reviewed range", () => validateClearance(change(clearance, "reviewed_range", `${P01_BASE_SHA}..${"a".repeat(40)}`), { expected: clearance })],
  ["changed candidate HEAD", () => validateClearance(change(clearance, "candidate_head_sha", "a".repeat(40)), { expected: clearance })],
  ["cross-head manifest blob", () => {
    let stale = change(clearance, "candidate_head_sha", "7adbae8b4cdf368b7b7122a81ad6a8cf30cdd7d0");
    stale = change(stale, "served_head_sha", "7adbae8b4cdf368b7b7122a81ad6a8cf30cdd7d0");
    stale = change(stale, "candidate_tree_sha", "2c964761367fa4d5fb516bc6130c1af509cfea7d");
    stale = change(stale, "served_tree_sha", "2c964761367fa4d5fb516bc6130c1af509cfea7d");
    stale = change(stale, "reviewed_range", `${P01_BASE_SHA}..7adbae8b4cdf368b7b7122a81ad6a8cf30cdd7d0`);
    validateClearance(stale);
  }],
  ["candidate head missing authoritative registry", () => {
    let stale = change(clearance, "candidate_head_sha", "973a5771c54650946ece3b1d9016e0788c522087");
    stale = change(stale, "served_head_sha", "973a5771c54650946ece3b1d9016e0788c522087");
    stale = change(stale, "candidate_tree_sha", "592e4389b9be8f980c4deabd397ceb79e718bafb");
    stale = change(stale, "served_tree_sha", "592e4389b9be8f980c4deabd397ceb79e718bafb");
    stale = change(stale, "reviewed_range", `${P01_BASE_SHA}..973a5771c54650946ece3b1d9016e0788c522087`);
    validateClearance(stale);
  }],
  ["nonexistent Git range head", () => {
    const forged = change(change(change(clearance, "candidate_head_sha", "a".repeat(40)), "served_head_sha", "a".repeat(40)), "reviewed_range", `${P01_BASE_SHA}..${"a".repeat(40)}`);
    validateClearance(forged, { verifyGit: true });
  }],
  ["nonexistent phase base", () => validateGitRecord({ base_sha: "a".repeat(40), candidate_head_sha: evidence.candidate_head_sha!, candidate_tree_sha: evidence.candidate_tree_sha! })],
  ["non-ancestor phase base", () => validateGitRecord({ base_sha: evidence.candidate_head_sha!, candidate_head_sha: "d7da340c94f4cb34692a122696717e72f357fac1", candidate_tree_sha: "fb4ccf08803c9ba76a944ec870a8dfe1e7a33c3e" })],
  ["changed candidate tree", () => validateClearance(change(clearance, "candidate_tree_sha", "b".repeat(40)), { expected: clearance })],
  ["changed served head", () => validateClearance(change(clearance, "served_head_sha", "c".repeat(40)), { expected: clearance })],
  ["changed served tree", () => validateClearance(change(clearance, "served_tree_sha", "c".repeat(40)), { expected: clearance })],
  ["changed manifest path", () => validateClearance(change(clearance, "manifest.path", "wrong.json"))],
  ["manifest path traversal", () => validateClearance(change(clearance, "manifest.path", "verification/gooseweb/manifests/../secret.json"))],
  ["manifest path double separator schema", () => applySchemaFile("verification/gooseweb/schemas/exact-head-clearance.schema.json", change(clearance, "manifest.path", "verification/gooseweb/manifests/a//b.json"))],
  ["manifest path empty segment schema", () => applySchemaFile("verification/gooseweb/schemas/review-outcome.schema.json", change(validNonClearance, "manifest.path", "verification/gooseweb/manifests//empty.json"))],
  ["manifest phase mismatch", () => validateClearance(change(clearance, "phase_id", "P02"))],
  ["changed manifest hash", () => validateClearance(change(clearance, "manifest.sha256", "f".repeat(64)))],
  ["changed manifest revision", () => validateClearance(change(clearance, "manifest.revision", 1))],
  ["changed lease", () => validateClearance(change(clearance, "lease.sequence", 3), { expected: clearance })],
  ["changed stack configuration", () => validateClearance(change(clearance, "stack.runtime_port", 19999), { expected: clearance })],
  ["forged stack configuration hash", () => validateClearance(change(clearance, "stack.configuration_sha256", "f".repeat(64)))],
  ["changed reviewer tuple", () => validateClearance(change(clearance, "review.reviewer_identity", "replacement"), { expected: clearance })],
  ["changed browser mode", () => validateClearance(change(clearance, "browser.execution_mode", "headed"))],
  ["changed browser version", () => validateClearance(change(clearance, "browser.chromium_version", "149.0.0.0"), { expected: clearance })],
  ["missing prior termination evidence", () => validateClearance(omit(clearance, "lease.prior_lease_termination_evidence.reference"))],
  ["dirty tree", () => validateClearance(change(clearance, "clean_tree", false))],
  ["hot reload evidence", () => validateClearance(change(clearance, "hot_reload", true))],
  ["release before stop/cleanup", () => validateClearance(change(clearance, "lease.released_at", "2026-07-12T10:10:00.000Z"))],
  ["clearance issued before release", () => validateClearance(change(clearance, "clearance.issued_at", "2026-07-12T10:19:59.000Z"))],
  ["reviewer implementer overlap", () => validateClearance(change(clearance, "review.reviewer_identity", "p01-implementer"))],
  ["approval routed to implementer", () => validateClearance(change(clearance, "review.final_approval_routed_to_implementer", true))],
  ["wrong clearance recipient", () => validateClearance(change(clearance, "clearance.recipient_role", "supervisor"))],
  ["substituted lead recipient", () => validateClearance(change(clearance, "clearance.recipient_identity", "someone_else"))],
  ["missing clearance identity", () => validateClearance(omit(clearance, "clearance.recipient_identity"))],
  ["changed clearance attempt", () => validateClearance(change(clearance, "attempt", 4), { expected: clearance })],
  ["changed nested clearance tuple", () => validateClearance(change(clearance, "clearance.issued_at", "2026-07-12T10:21:00.000Z"), { expected: clearance })],
  ["empty clearance baselines", () => validateClearance(change(clearance, "baseline_detected", []))],
  ["omitted clearance baseline", () => validateClearance(change(clearance, "baseline_detected", (clearance.baseline_detected as Json[]).slice(0, -1)))],
  ["duplicated clearance baseline", () => validateClearance(change(clearance, "baseline_detected.6", clone((clearance.baseline_detected as Json[])[0])))],
  ["substituted clearance baseline owner", () => validateClearance(change(clearance, "baseline_detected.0.owning_correction_phase", "P10"))],
  ["reordered clearance baselines", () => validateClearance(change(clearance, "baseline_detected", [...(clearance.baseline_detected as Json[])].reverse()))],
  ["P01 approved manifest under infrastructure clearance", () => validateManifestClearancePolicy(change(manifest, "scenario.product_clearance", "approved"), { scope: "verification_infrastructure_only", product_approved: false })],
  ["P06 nonempty baseline policy", () => validateManifestClearancePolicy(change(approvedP06Manifest, "baseline_detected", manifest.baseline_detected), { scope: "product_phase", product_approved: true })],
  ["P06 pending manifest product approval", () => validateManifestClearancePolicy(change(approvedP06Manifest, "scenario.product_clearance", "pending"), { scope: "product_phase", product_approved: true })],
  ["P06 blocked manifest product approval", () => validateManifestClearancePolicy(change(approvedP06Manifest, "scenario.product_clearance", "blocked_expected_honest_failure"), { scope: "product_phase", product_approved: true })],
  ["P56 nonempty baseline policy", () => validateManifestClearancePolicy(change(approvedP56Manifest, "baseline_detected", manifest.baseline_detected), { scope: "integration_release", product_approved: true })],
  ["P56 pending manifest product approval", () => validateManifestClearancePolicy(change(approvedP56Manifest, "scenario.product_clearance", "pending"), { scope: "integration_release", product_approved: true })],
  ["P56 blocked manifest product approval", () => validateManifestClearancePolicy(change(approvedP56Manifest, "scenario.product_clearance", "blocked_expected_honest_failure"), { scope: "integration_release", product_approved: true })],
  ["evidence head/sha7 mismatch", () => validateEvidence(change(evidence, "sha7", "2222222"), { checkFiles: false })],
  ["evidence headed mode", () => validateEvidence(change(evidence, "browser.execution_mode", "headed"), { checkFiles: false })],
  ["incomplete prohibited vocabulary", () => validateEvidence(change(evidence, "redaction.prohibited", ["credentials"]), { checkFiles: false })],
  ["missing evidence candidate tree", () => validateEvidence(omit(evidence, "candidate_tree_sha"), { checkFiles: false })],
  ["secret-bearing descriptor", () => validateEvidence(change(evidence, "redaction.bearer_token", "live-secret"), { checkFiles: false })],
  ["unexpected console message", () => validateBrowserCaptures(change(consoleCapture, "messages.2", { level: "error", message: "boom" }), networkCapture, manifest)],
  ["missing expected console message", () => validateBrowserCaptures(change(consoleCapture, "messages", []), networkCapture, manifest)],
  ["unexpected HTTP failure cannot be filtered", () => validateBrowserCaptures(consoleCapture, change(networkCapture, "raw_http.6", { method: "GET", path: "/missing", query_keys: [], status: 404, resource_type: "module", same_origin: true, baseline_defect_id: "" }), manifest)],
  ["failed HTTP with nonexistent baseline", () => validateBrowserCaptures(consoleCapture, change(networkCapture, "raw_http.5.baseline_defect_id", "BASE-DOES-NOT-EXIST"), manifest)],
  ["successful HTTP with failure baseline", () => validateBrowserCaptures(consoleCapture, change(networkCapture, "raw_http.0.baseline_defect_id", "BASE-P01-FAVICON-NOT-FOUND"), manifest)],
  ["cross-origin static success cannot be filtered", () => validateBrowserCaptures(consoleCapture, change(networkCapture, "raw_http.1.same_origin", false), manifest)],
  ["query-bearing HTTP path", () => validateBrowserCaptures(consoleCapture, change(networkCapture, "raw_http.0.path", "/?ticket=secret"), manifest)],
  ["unexpected WebSocket close", () => validateBrowserCaptures(consoleCapture, change(networkCapture, "websocket.events.1", { event: "close", code: 1006 }), manifest)],
  ["unavailable WebSocket without baseline", () => validateBrowserCaptures(consoleCapture, change(networkCapture, "websocket", { availability: "unavailable", events: [], inference_prohibited: true, reason: "not exposed", baseline_defect_id: "" }), manifest)],
  ["unavailable WebSocket with nonexistent baseline", () => validateBrowserCaptures(consoleCapture, change(networkCapture, "websocket", { availability: "unavailable", events: [], inference_prohibited: true, reason: "not exposed", baseline_defect_id: "BASE-DOES-NOT-EXIST" }), manifest)],
  ["dependency shorthand", () => validateLedger(change(ledger, "phases.3.prerequisites.0", "P01-P02"))],
  ["same/later dependency", () => validateLedger(change(ledger, "phases.7.prerequisites.0", "P07"))],
  ["P21 missing P05", () => validateLedger(change(ledger, "phases.21.prerequisites", ["P18", "P20"]))],
  ["P56 missing prerequisite", () => validateLedger(change(ledger, "phases.56.prerequisites", phaseIds(1, 54)))],
  ["phase advanced before prerequisite", () => validateLedger(change(ledger, "phases.2.state", "candidate_ready_for_review"))],
  ["illegal phase transition", () => validateLedger(change(ledger, "phases.1.history.5.from", "blocked"))],
  ["phase state/history mismatch", () => validateLedger(change(ledger, "phases.1.state", "cleared"))],
  ["backward phase transition timestamp", () => validateLedger(change(ledger, "phases.1.history.7.at", "2026-07-12T05:40:00.000Z"))],
  ["unknown transition lease", () => validateLedger(change(ledger, "phases.1.history.2.lease_id", "gooseweb-migration-999999"))],
  ["duplicate ledger lease", () => validateLedger(change(ledger, "lease_history.1", clone((ledger.lease_history as Json[])[0])))],
  ["nonmonotonic ledger lease", () => validateLedger(ledgerWithLease({ sequence: 1 }))],
  ["overlapping ledger lease", () => validateLedger(ledgerWithLease({ acquired_at: "2026-07-12T05:30:00.000Z" }))],
  ["P56 integration lease overlap", () => validateLedger(ledgerWithLease({ phase_id: "P56", acquired_at: "2026-07-12T05:30:00.000Z" }))],
  ["clearance references unknown lease", () => validateLedger(change(ledger, "clearance_history.0", ledgerClearance("gooseweb-migration-999999")))],
  ["duplicate clearance browser session", () => validateClearanceHistory([clearance, laterClearance({ browser_session: "gooseweb-p01-review-attempt-3-headless" })])]
];

for (const [name, run] of negativeCases) assert.throws(run, undefined, `negative fixture unexpectedly passed: ${name}`);
console.log(`Gooseweb acceptance contract v8 passed (${negativeCases.length} negative cases)`);

function validateSchemasAgainstDocuments(): void {
  applySchemaFile("verification/gooseweb/schemas/acceptance-manifest.schema.json", manifest);
  applySchemaFile("verification/gooseweb/schemas/manifest-registry.schema.json", manifestRegistry);
  applySchemaFile("verification/gooseweb/schemas/phase-state-ledger.schema.json", ledger);
  applySchemaFile("verification/gooseweb/schemas/exact-head-clearance.schema.json", clearance);
  applySchemaFile("verification/gooseweb/schemas/evidence-run.schema.json", evidence);
}

function validateReferencedEvidence(): void {
  const directory = resolve(root, String(evidence.root));
  rmSync(directory, { recursive: true, force: true });
  mkdirSync(resolve(directory, "screenshots"), { recursive: true });
  const files: Record<string, string | Uint8Array> = {
    "manifest.json": readFileSync(resolve(root, P01_MANIFEST_PATH)),
    "environment.json": JSON.stringify({ phase_id: evidence.phase_id, attempt: evidence.attempt, base_sha: evidence.base_sha, reviewed_range: evidence.reviewed_range, candidate_head_sha: evidence.candidate_head_sha, candidate_tree_sha: evidence.candidate_tree_sha, served_head_sha: evidence.served_head_sha, served_tree_sha: evidence.served_tree_sha, clean_tree: evidence.clean_tree, hot_reload: evidence.hot_reload, lease: evidence.lease, stack: evidence.stack, review: evidence.review, plan_sha256: APPROVED_PLAN_SHA256, manifest_sha256: (evidence.manifest as RecordJson).sha256, browser_session: (evidence.browser as RecordJson).session_name, browser_execution_mode: (evidence.browser as RecordJson).execution_mode, chromium_binary: (evidence.browser as RecordJson).chromium_binary, chromium_version: (evidence.browser as RecordJson).chromium_version, profile_policy: (evidence.browser as RecordJson).profile_policy, redaction: "omitted" }),
    "console.json": JSON.stringify(consoleCapture),
    "network.json": JSON.stringify(networkCapture),
    "websocket.json": JSON.stringify((networkCapture.websocket as RecordJson)),
    "runtime-state.redacted.json": JSON.stringify({ credentials: "redacted", sessions: 0 }),
    "tower-state.redacted.json": JSON.stringify({ tickets: "redacted", teams: 0 }),
    "store-state.redacted.json": JSON.stringify({ messages: 0 }),
    "checks.json": JSON.stringify({ status: "pass" }),
    "report.md": "# Redacted acceptance report\n",
    "exact-head-clearance.json": JSON.stringify(clearance)
  };
  for (const [path, content] of Object.entries(files)) writeFileSync(resolve(directory, path), content);
  for (const viewport of ["1440x1000", "820x1000", "520x900"]) {
    const [width, height] = viewport.split("x").map(Number);
    writeFileSync(resolve(directory, `screenshots/${viewport}.png`), createPng(width!, height!));
  }
  try {
    validateEvidence(evidence, { checkFiles: true, expected: evidence });
    const rejectedEvidence = change(evidence, "review_outcome", { status: "changes_required", record: "review-outcome.json" });
    const nonClearance = validNonClearance;
    writeFileSync(resolve(directory, "review-outcome.json"), JSON.stringify(nonClearance));
    validateEvidence(rejectedEvidence, { checkFiles: true, expected: rejectedEvidence });
    withAlternateManifest((path, hash) => {
      let alternateManifestOutcome = change(nonClearance, "manifest.path", path);
      alternateManifestOutcome = change(alternateManifestOutcome, "manifest.revision", 1);
      alternateManifestOutcome = change(alternateManifestOutcome, "manifest.sha256", hash);
      writeFileSync(resolve(directory, "review-outcome.json"), JSON.stringify(alternateManifestOutcome));
      assert.throws(() => validateEvidence(rejectedEvidence, { checkFiles: true }), undefined, "changes-required evidence accepted a non-active same-phase manifest");
    });
    writeFileSync(resolve(directory, "review-outcome.json"), JSON.stringify({ ...nonClearance, attempt: 4 }));
    assert.throws(() => validateEvidence(rejectedEvidence, { checkFiles: true }), undefined, "cross-attempt outcome unexpectedly passed");
    writeFileSync(resolve(directory, "review-outcome.json"), JSON.stringify(change(nonClearance, "review.reviewer_identity", "other-reviewer")));
    assert.throws(() => validateEvidence(rejectedEvidence, { checkFiles: true }), undefined, "cross-reviewer outcome unexpectedly passed");
    let foreignBrowser = change(nonClearance, "review.browser_session", "other-headless-session");
    foreignBrowser = change(foreignBrowser, "browser.session_name", "other-headless-session");
    writeFileSync(resolve(directory, "review-outcome.json"), JSON.stringify(foreignBrowser));
    assert.throws(() => validateEvidence(rejectedEvidence, { checkFiles: true }), undefined, "cross-browser outcome unexpectedly passed");
    writeFileSync(resolve(directory, "review-outcome.json"), JSON.stringify(change(nonClearance, "lease.lease_id", "gooseweb-migration-999999")));
    assert.throws(() => validateEvidence(rejectedEvidence, { checkFiles: true }), undefined, "cross-lease outcome unexpectedly passed");
    let foreignStack = change(nonClearance, "stack.runtime_port", 19101);
    foreignStack = change(foreignStack, "stack.configuration_sha256", stackConfigurationHash(foreignStack.stack as RecordJson));
    writeFileSync(resolve(directory, "review-outcome.json"), JSON.stringify(foreignStack));
    assert.throws(() => validateEvidence(rejectedEvidence, { checkFiles: true }), undefined, "cross-stack outcome unexpectedly passed");
    const nonexistent = "a".repeat(40);
    let forgedGit = change(nonClearance, "candidate_head_sha", nonexistent);
    forgedGit = change(forgedGit, "served_head_sha", nonexistent);
    forgedGit = change(forgedGit, "reviewed_range", `${P01_BASE_SHA}..${nonexistent}`);
    assert.throws(() => validateReviewOutcome(forgedGit), undefined, "non-clearance with nonexistent Git head unexpectedly passed");
    writeFileSync(resolve(directory, "review-outcome.json"), JSON.stringify(nonClearance));
    writeFileSync(resolve(directory, "review-outcome.json"), JSON.stringify({ ...nonClearance, recorded_at: "2026-07-12T10:19:59.000Z" }));
    assert.throws(() => validateEvidence(rejectedEvidence, { checkFiles: true }), undefined, "pre-release changes-required outcome unexpectedly passed");
    writeFileSync(resolve(directory, "review-outcome.json"), JSON.stringify(nonClearance));
    const rejectedPointingAtClearance = change(rejectedEvidence, "review_outcome.record", "exact-head-clearance.json");
    assert.throws(() => validateEvidence(rejectedPointingAtClearance, { checkFiles: true }), undefined, "changes-required evidence accepted a clearance record");
    const clearedPointingAtRejection = change(evidence, "review_outcome.record", "review-outcome.json");
    assert.throws(() => validateEvidence(clearedPointingAtRejection, { checkFiles: true }), undefined, "cleared evidence accepted a non-clearance record");
    const headerOnly = Buffer.alloc(24);
    Buffer.from("89504e470d0a1a0a", "hex").copy(headerOnly, 0);
    headerOnly.write("IHDR", 12, "ascii");
    headerOnly.writeUInt32BE(1440, 16); headerOnly.writeUInt32BE(1000, 20);
    writeFileSync(resolve(directory, "screenshots/1440x1000.png"), headerOnly);
    assert.throws(() => validateEvidence(evidence, { checkFiles: true }), undefined, "header-only PNG unexpectedly passed");
    writeFileSync(resolve(directory, "screenshots/1440x1000.png"), createPng(1440, 1000));
    writeFileSync(resolve(directory, "runtime-state.redacted.json"), JSON.stringify({ note: "Authorization: Bearer live-secret" }));
    assert.throws(() => validateEvidence(evidence, { checkFiles: true }), undefined, "referenced secret unexpectedly passed");
    rmSync(resolve(directory, "network.json"));
    assert.throws(() => validateEvidence(evidence, { checkFiles: true }), undefined, "missing referenced evidence unexpectedly passed");
  } finally {
    rmSync(directory, { recursive: true, force: true });
  }
}

function withAlternateManifest(run: (path: string, hash: string) => void): void {
  const absolute = resolve(root, ALTERNATE_MANIFEST_PATH);
  writeFileSync(absolute, readFileSync(resolve(root, VALIDATOR_MANIFEST_PATH)));
  try {
    run(ALTERNATE_MANIFEST_PATH, sha256(ALTERNATE_MANIFEST_PATH));
  } finally {
    rmSync(absolute, { force: true });
  }
}

function manifestForPhase(phase: "P06" | "P56", productClearance: "approved" | "pending" | "blocked_expected_honest_failure"): RecordJson {
  const id = `GW-${phase}-VALIDATOR-001`;
  let result = change(manifest, "manifest_id", id);
  result = change(result, "scenario.stable_scenario_id", id);
  result = change(result, "scenario.phase_id", phase);
  result = change(result, "scenario.product_clearance", productClearance);
  return change(result, "baseline_detected", []);
}

function laterClearance(overrides: Record<string, unknown>): RecordJson {
  const next = clone(clearance);
  const lease = next.lease as RecordJson;
  lease.lease_id = "gooseweb-migration-000003";
  lease.sequence = 3;
  lease.acquired_at = "2026-07-12T10:30:00.000Z";
  lease.released_at = "2026-07-12T10:50:00.000Z";
  const process = lease.managed_process as RecordJson;
  process.started_at = "2026-07-12T10:31:00.000Z";
  process.stopped_at = "2026-07-12T10:48:00.000Z";
  process.cleanup_completed_at = "2026-07-12T10:49:00.000Z";
  const review = next.review as RecordJson;
  review.browser_session = String(overrides.browser_session ?? "gooseweb-p01-review-attempt-4-headless");
  return next;
}

function ledgerWithLease(overrides: Record<string, Json>): RecordJson {
  const next = clone(ledger);
  const lease = clone((next.lease_history as RecordJson[])[0]!);
  lease.lease_id = "gooseweb-migration-000002";
  lease.sequence = overrides.sequence ?? 2;
  lease.phase_id = overrides.phase_id ?? "P02";
  lease.acquired_at = overrides.acquired_at ?? "2026-07-12T06:00:00.000Z";
  lease.released_at = "2026-07-12T06:20:00.000Z";
  const prior = lease.prior_lease_termination_evidence as RecordJson;
  prior.status = "terminated_and_cleaned";
  prior.reference = "gooseweb-migration-000001 termination and cleanup";
  const process = lease.managed_process as RecordJson;
  process.started_at = "2026-07-12T06:01:00.000Z";
  process.stopped_at = "2026-07-12T06:18:00.000Z";
  process.cleanup_completed_at = "2026-07-12T06:19:00.000Z";
  const reviewer = lease.reviewer as RecordJson;
  reviewer.browser_session = "gooseweb-p02-review-attempt-1-headless";
  (next.lease_history as RecordJson[]).push(lease);
  return next;
}

function ledgerClearance(leaseId: string): RecordJson {
  return { phase_id: "P01", lease_id: leaseId, clearance_path: "clearance.json", candidate_head_sha: "1".repeat(40), candidate_tree_sha: "2".repeat(40), manifest_sha256: "3".repeat(64), status: "cleared" };
}

function phaseIds(first: number, last: number): string[] { return Array.from({ length: last - first + 1 }, (_, index) => `P${String(first + index).padStart(2, "0")}`); }
function createPng(width: number, height: number): Buffer {
  const signature = Buffer.from("89504e470d0a1a0a", "hex");
  const ihdr = Buffer.alloc(13); ihdr.writeUInt32BE(width, 0); ihdr.writeUInt32BE(height, 4); ihdr[8] = 8; ihdr[9] = 0;
  const raw = Buffer.alloc((width + 1) * height);
  const idat = deflateSync(raw);
  return Buffer.concat([signature, pngChunk("IHDR", ihdr), pngChunk("IDAT", idat), pngChunk("IEND", Buffer.alloc(0))]);
}
function pngChunk(type: string, data: Buffer): Buffer { const name = Buffer.from(type); const chunk = Buffer.alloc(12 + data.length); chunk.writeUInt32BE(data.length, 0); name.copy(chunk, 4); data.copy(chunk, 8); chunk.writeUInt32BE(testCrc32(Buffer.concat([name, data])), 8 + data.length); return chunk; }
function testCrc32(bytes: Uint8Array): number { let crc = 0xffffffff; for (const byte of bytes) { crc ^= byte; for (let bit = 0; bit < 8; bit += 1) crc = (crc >>> 1) ^ (crc & 1 ? 0xedb88320 : 0); } return (crc ^ 0xffffffff) >>> 0; }
function clone<T>(value: T): T { return structuredClone(value); }
function change(source: RecordJson, path: string, value: unknown): RecordJson { const result = clone(source); const parts = path.split("."); let current: any = result; for (const part of parts.slice(0, -1)) current = current[index(part)]; current[index(parts.at(-1)!)] = value; return result; }
function omit(source: RecordJson, path: string): RecordJson { const result = clone(source); const parts = path.split("."); let current: any = result; for (const part of parts.slice(0, -1)) current = current[index(part)]; delete current[index(parts.at(-1)!)]; return result; }
function index(part: string): string | number { return /^\d+$/.test(part) ? Number(part) : part; }

assert.equal(stackConfigurationHash(clearance.stack as RecordJson), (clearance.stack as RecordJson).configuration_sha256);
