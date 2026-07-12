import assert from "node:assert/strict";
import { mkdirSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { resolve } from "node:path";
import {
  APPROVED_PLAN_SHA256,
  MANIFEST_PATH,
  applySchemaFile,
  readJson,
  sha256,
  stackConfigurationHash,
  validateBrowserCaptures,
  validateClearance,
  validateClearanceHistory,
  validateEvidence,
  validateLedger,
  validateManifest,
  type Json,
  type RecordJson
} from "../../../verification/gooseweb/validator/validate";

const root = resolve(import.meta.dir, "../../..");
const manifest = readJson(MANIFEST_PATH);
const ledger = readJson("verification/gooseweb/ledger/phase-state.json");
const clearance = readJson("verification/gooseweb/validator/fixtures/valid-clearance.json");
const evidence = readJson("verification/gooseweb/validator/fixtures/valid-evidence-run.json");
const consoleCapture: RecordJson = {
  schema_revision: "gooseweb-console-capture/v2",
  messages: [
    { level: "debug", message: "[vite] connecting..." },
    { level: "info", message: "React DevTools development informational message" }
  ]
};
const networkCapture: RecordJson = {
  schema_revision: "gooseweb-network-capture/v2",
  http: [
    { method: "GET", path: "/", status: 200 },
    { method: "POST", path: "/api/dev-ticket", status: 200 }
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
validateLedger(ledger);
validateClearance(clearance, { expected: clearance, verifyGit: true });
validateEvidence(evidence, { expected: evidence });
validateBrowserCaptures(consoleCapture, networkCapture);
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
  ["not applicable without reason", () => validateManifest(omit(manifest, "non_applicable.rollback.reason"))],
  ["changed candidate HEAD", () => validateClearance(change(clearance, "candidate_head_sha", "a".repeat(40)), { expected: clearance })],
  ["changed candidate tree", () => validateClearance(change(clearance, "candidate_tree_sha", "b".repeat(40)), { expected: clearance })],
  ["changed served head", () => validateClearance(change(clearance, "served_head_sha", "c".repeat(40)), { expected: clearance })],
  ["changed served tree", () => validateClearance(change(clearance, "served_tree_sha", "c".repeat(40)), { expected: clearance })],
  ["changed manifest path", () => validateClearance(change(clearance, "manifest.path", "wrong.json"))],
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
  ["reviewer implementer overlap", () => validateClearance(change(clearance, "review.reviewer_identity", "p01-implementer"))],
  ["approval routed to implementer", () => validateClearance(change(clearance, "review.final_approval_routed_to_implementer", true))],
  ["wrong clearance recipient", () => validateClearance(change(clearance, "clearance.recipient_role", "supervisor"))],
  ["missing clearance identity", () => validateClearance(omit(clearance, "clearance.recipient_identity"))],
  ["evidence head/sha7 mismatch", () => validateEvidence(change(evidence, "sha7", "2222222"))],
  ["evidence headed mode", () => validateEvidence(change(evidence, "browser.execution_mode", "headed"))],
  ["incomplete prohibited vocabulary", () => validateEvidence(change(evidence, "redaction.prohibited", ["credentials"]))],
  ["missing evidence candidate tree", () => validateEvidence(omit(evidence, "candidate_tree_sha"))],
  ["secret-bearing descriptor", () => validateEvidence(change(evidence, "redaction.bearer_token", "live-secret"))],
  ["unexpected console message", () => validateBrowserCaptures(change(consoleCapture, "messages.2", { level: "error", message: "boom" }), networkCapture)],
  ["missing expected console message", () => validateBrowserCaptures(change(consoleCapture, "messages", []), networkCapture)],
  ["unexpected HTTP", () => validateBrowserCaptures(consoleCapture, change(networkCapture, "http.2", { method: "GET", path: "/missing", status: 404 }))],
  ["query-bearing HTTP path", () => validateBrowserCaptures(consoleCapture, change(networkCapture, "http.0.path", "/?ticket=secret"))],
  ["unexpected WebSocket close", () => validateBrowserCaptures(consoleCapture, change(networkCapture, "websocket.events.1", { event: "close", code: 1006 }))],
  ["unavailable WebSocket without baseline", () => validateBrowserCaptures(consoleCapture, change(networkCapture, "websocket", { availability: "unavailable", events: [], inference_prohibited: true, reason: "not exposed", baseline_defect_id: "" }))],
  ["dependency shorthand", () => validateLedger(change(ledger, "phases.3.prerequisites.0", "P01-P02"))],
  ["same/later dependency", () => validateLedger(change(ledger, "phases.7.prerequisites.0", "P07"))],
  ["P21 missing P05", () => validateLedger(change(ledger, "phases.21.prerequisites", ["P18", "P20"]))],
  ["P56 missing prerequisite", () => validateLedger(change(ledger, "phases.56.prerequisites", phaseIds(1, 54)))],
  ["phase advanced before prerequisite", () => validateLedger(change(ledger, "phases.2.state", "candidate_ready_for_review"))],
  ["illegal phase transition", () => validateLedger(change(ledger, "phases.1.history.5.from", "blocked"))],
  ["phase state/history mismatch", () => validateLedger(change(ledger, "phases.1.state", "cleared"))],
  ["unknown transition lease", () => validateLedger(change(ledger, "phases.1.history.2.lease_id", "gooseweb-migration-999999"))],
  ["duplicate ledger lease", () => validateLedger(change(ledger, "lease_history.1", clone((ledger.lease_history as Json[])[0])))],
  ["nonmonotonic ledger lease", () => validateLedger(ledgerWithLease({ sequence: 1 }))],
  ["overlapping ledger lease", () => validateLedger(ledgerWithLease({ acquired_at: "2026-07-12T05:30:00.000Z" }))],
  ["P56 integration lease overlap", () => validateLedger(ledgerWithLease({ phase_id: "P56", acquired_at: "2026-07-12T05:30:00.000Z" }))],
  ["clearance references unknown lease", () => validateLedger(change(ledger, "clearance_history.0", ledgerClearance("gooseweb-migration-999999")))],
  ["duplicate clearance browser session", () => validateClearanceHistory([clearance, laterClearance({ browser_session: "gooseweb-p01-review-attempt-3-headless" })])]
];

for (const [name, run] of negativeCases) assert.throws(run, undefined, `negative fixture unexpectedly passed: ${name}`);
console.log(`Gooseweb acceptance contract v2 passed (${negativeCases.length} negative cases)`);

function validateSchemasAgainstDocuments(): void {
  applySchemaFile("verification/gooseweb/schemas/acceptance-manifest.schema.json", manifest);
  applySchemaFile("verification/gooseweb/schemas/phase-state-ledger.schema.json", ledger);
  applySchemaFile("verification/gooseweb/schemas/exact-head-clearance.schema.json", clearance);
  applySchemaFile("verification/gooseweb/schemas/evidence-run.schema.json", evidence);
}

function validateReferencedEvidence(): void {
  const directory = resolve(root, String(evidence.root));
  rmSync(directory, { recursive: true, force: true });
  mkdirSync(resolve(directory, "screenshots"), { recursive: true });
  const files: Record<string, string | Uint8Array> = {
    "manifest.json": readFileSync(resolve(root, MANIFEST_PATH)),
    "environment.json": JSON.stringify({ phase_id: evidence.phase_id, candidate_head_sha: evidence.candidate_head_sha, candidate_tree_sha: evidence.candidate_tree_sha, plan_sha256: APPROVED_PLAN_SHA256, manifest_sha256: (evidence.manifest as RecordJson).sha256, browser_session: (evidence.browser as RecordJson).session_name, browser_execution_mode: (evidence.browser as RecordJson).execution_mode, chromium_binary: (evidence.browser as RecordJson).chromium_binary, chromium_version: (evidence.browser as RecordJson).chromium_version, profile_policy: (evidence.browser as RecordJson).profile_policy, redaction: "omitted" }),
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
    const png = Buffer.alloc(24);
    Buffer.from("89504e470d0a1a0a", "hex").copy(png, 0);
    png.write("IHDR", 12, "ascii");
    png.writeUInt32BE(width!, 16);
    png.writeUInt32BE(height!, 20);
    writeFileSync(resolve(directory, `screenshots/${viewport}.png`), png);
  }
  try {
    validateEvidence(evidence, { checkFiles: true, expected: evidence });
    writeFileSync(resolve(directory, "runtime-state.redacted.json"), JSON.stringify({ note: "Authorization: Bearer live-secret" }));
    assert.throws(() => validateEvidence(evidence, { checkFiles: true }), undefined, "referenced secret unexpectedly passed");
    rmSync(resolve(directory, "network.json"));
    assert.throws(() => validateEvidence(evidence, { checkFiles: true }), undefined, "missing referenced evidence unexpectedly passed");
  } finally {
    rmSync(directory, { recursive: true, force: true });
  }
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
function clone<T>(value: T): T { return structuredClone(value); }
function change(source: RecordJson, path: string, value: unknown): RecordJson { const result = clone(source); const parts = path.split("."); let current: any = result; for (const part of parts.slice(0, -1)) current = current[index(part)]; current[index(parts.at(-1)!)] = value; return result; }
function omit(source: RecordJson, path: string): RecordJson { const result = clone(source); const parts = path.split("."); let current: any = result; for (const part of parts.slice(0, -1)) current = current[index(part)]; delete current[index(parts.at(-1)!)]; return result; }
function index(part: string): string | number { return /^\d+$/.test(part) ? Number(part) : part; }

assert.equal(stackConfigurationHash(clearance.stack as RecordJson), (clearance.stack as RecordJson).configuration_sha256);
