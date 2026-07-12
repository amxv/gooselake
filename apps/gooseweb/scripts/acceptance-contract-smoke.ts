import assert from "node:assert/strict";
import {
  APPROVED_PLAN_SHA256,
  MANIFEST_PATH,
  readJson,
  sha256,
  validateClearance,
  validateClearanceHistory,
  validateEvidence,
  validateLedger,
  validateManifest,
  type RecordJson
} from "../../../verification/gooseweb/validator/validate";

const manifest = readJson(MANIFEST_PATH);
const ledger = readJson("verification/gooseweb/ledger/phase-state.json");
const clearance = readJson("verification/gooseweb/validator/fixtures/valid-clearance.json");
const evidence = readJson("verification/gooseweb/validator/fixtures/valid-evidence-run.json");

assert.equal(
  sha256("tmp/gg/golden-goose-gooseweb-migration-implementation-plan.md"),
  APPROVED_PLAN_SHA256,
  "immutable approved plan changed"
);
validateManifest(manifest);
validateLedger(ledger);
validateClearance(clearance, clearance);
validateEvidence(evidence);
validateAllowlists();
validateSchemas();

const negativeCases: [string, () => void][] = [
  ["changed approved plan SHA", () => validateManifest(change(manifest, "approved_plan.sha256", "0".repeat(64)))],
  ["changed candidate HEAD", () => validateClearance(change(clearance, "candidate_head_sha", "a".repeat(40)), clearance)],
  ["changed candidate tree", () => validateClearance(change(clearance, "candidate_tree_sha", "b".repeat(40)), clearance)],
  ["changed served head", () => validateClearance(change(clearance, "served_head_sha", "c".repeat(40)), clearance)],
  ["changed manifest path", () => validateClearance(change(clearance, "manifest.path", "wrong.json"))],
  ["changed manifest hash", () => validateClearance(change(clearance, "manifest.sha256", "f".repeat(64)))],
  ["changed manifest revision", () => validateClearance(change(clearance, "manifest.revision", 2))],
  ["changed lease", () => validateClearance(change(clearance, "lease.sequence", 2), clearance)],
  ["changed stack configuration", () => validateClearance(change(clearance, "stack.runtime_port", 19999), clearance)],
  ["missing prior termination evidence", () => validateClearance(omit(clearance, "lease.prior_lease_termination_evidence.reference"))],
  ["dirty tree", () => validateClearance(change(clearance, "clean_tree", false))],
  ["hot reload evidence", () => validateClearance(change(clearance, "hot_reload", true))],
  ["release before stop/cleanup", () => validateClearance(change(clearance, "lease.released_at", "2026-07-12T10:10:00.000Z"))],
  ["reviewer implementer overlap", () => validateClearance(change(clearance, "review.reviewer_identity", "p01-implementer"))],
  ["approval routed to implementer", () => validateClearance(change(clearance, "review.final_approval_routed_to_implementer", true))],
  ["wrong clearance recipient", () => validateClearance(change(clearance, "clearance.recipient_role", "supervisor"))],
  ["omitted required field", () => validateManifest(omit(manifest, "ownership.goosetower"))],
  ["not applicable without reason", () => validateManifest(omit(manifest, "non_applicable.rollback.reason"))],
  ["baseline missing scenario", () => validateManifest(omit(manifest, "baseline_detected.0.scenario_id"))],
  ["baseline missing divergent layer", () => validateManifest(omit(manifest, "baseline_detected.0.first_divergent_layer"))],
  ["baseline missing evidence", () => validateManifest(change(manifest, "baseline_detected.0.evidence_references", []))],
  ["baseline missing repair phase", () => validateManifest(omit(manifest, "baseline_detected.0.owning_correction_phase"))],
  ["baseline missing downstream gates", () => validateManifest(change(manifest, "baseline_detected.0.affected_downstream_gates", []))],
  ["baseline labeled product approval", () => validateManifest(change(manifest, "baseline_detected.0.product_scenario_status", "approved"))],
  ["known defects nonempty", () => validateManifest(change(manifest, "known_defects", [{ id: "defect" }]))],
  ["secret-bearing descriptor", () => validateEvidence(change(evidence, "redaction.bearer_token", "live-secret"))],
  ["dependency shorthand", () => validateLedger(change(ledger, "phases.3.prerequisites.0", "P01-P02"))],
  ["malformed dependency ID", () => validateLedger(change(ledger, "phases.3.prerequisites.0", "phase-one"))],
  ["same/later dependency", () => validateLedger(change(ledger, "phases.7.prerequisites.0", "P07"))],
  ["dependency cycle", () => validateLedger(change(ledger, "phases.1.prerequisites.0", "P01"))],
  ["P21 missing P05", () => validateLedger(change(ledger, "phases.21.prerequisites", ["P18", "P20"]))],
  ["P56 missing one exact prerequisite", () => validateLedger(change(ledger, "phases.56.prerequisites", (ledger.phases as RecordJson[]).slice(1, 55).map((entry) => entry.phase_id)))],
  ["duplicate lease ID", () => validateClearanceHistory([clearance, laterClearance({ lease_id: "gooseweb-migration-000001" })])],
  ["nonmonotonic lease sequence", () => validateClearanceHistory([clearance, laterClearance({ sequence: 1 })])],
  ["overlapping phase leases", () => validateClearanceHistory([clearance, laterClearance({ acquired_at: "2026-07-12T10:15:00.000Z" })])],
  ["P56 integration overlap", () => validateClearanceHistory([clearance, laterClearance({ phase_id: "P56", acquired_at: "2026-07-12T10:15:00.000Z" })])],
  ["nonunique browser session", () => validateClearanceHistory([clearance, laterClearance({ browser_session: "gooseweb-p01-review-attempt-1" })])]
];

for (const [name, run] of negativeCases) {
  assert.throws(run, undefined, `negative fixture unexpectedly passed: ${name}`);
}

console.log(`Gooseweb acceptance contract passed (${negativeCases.length} negative cases)`);

function laterClearance(overrides: Record<string, unknown>): RecordJson {
  const next = clone(clearance);
  next.phase_id = (overrides.phase_id as string | undefined) ?? "P02";
  const lease = next.lease as RecordJson;
  lease.lease_id = (overrides.lease_id as string | undefined) ?? "gooseweb-migration-000002";
  lease.sequence = (overrides.sequence as number | undefined) ?? 2;
  lease.acquired_at = (overrides.acquired_at as string | undefined) ?? "2026-07-12T10:30:00.000Z";
  lease.released_at = "2026-07-12T10:50:00.000Z";
  const process = lease.managed_process as RecordJson;
  process.started_at = "2026-07-12T10:31:00.000Z";
  process.stopped_at = "2026-07-12T10:48:00.000Z";
  process.cleanup_completed_at = "2026-07-12T10:49:00.000Z";
  const review = next.review as RecordJson;
  review.browser_session = (overrides.browser_session as string | undefined) ?? "gooseweb-p02-review-attempt-1";
  return next;
}

function validateAllowlists(): void {
  const consoleList = readJson("verification/gooseweb/allowlists/console.json");
  const networkList = readJson("verification/gooseweb/allowlists/network.json");
  assert.equal(consoleList.schema_revision, "gooseweb-console-allowlist/v1");
  assert.deepEqual(consoleList.exact_messages, []);
  assert.equal(networkList.schema_revision, "gooseweb-network-allowlist/v1");
  for (const entry of networkList.exact_expected as RecordJson[]) {
    assert.equal(typeof entry.method, "string");
    assert.match(String(entry.path), /^\//);
    assert.equal(typeof entry.status, "number");
    assert.doesNotMatch(String(entry.path), /[.*+?^${}()|[\]\\]/);
  }
}

function validateSchemas(): void {
  const paths = [
    "acceptance-manifest.schema.json", "exact-head-clearance.schema.json",
    "evidence-run.schema.json", "phase-state-ledger.schema.json"
  ];
  for (const path of paths) {
    const schema = readJson(`verification/gooseweb/schemas/${path}`);
    assert.equal(schema.$schema, "https://json-schema.org/draft/2020-12/schema");
    assert.match(String(schema.$id), /\/v1$/);
    assert.equal(schema.additionalProperties, false);
  }
}

function clone<T>(value: T): T { return structuredClone(value); }

function change(source: RecordJson, path: string, value: unknown): RecordJson {
  const result = clone(source);
  const parts = path.split(".");
  let current: any = result;
  for (const part of parts.slice(0, -1)) current = current[Number.isInteger(Number(part)) ? Number(part) : part];
  current[parts.at(-1)!] = value;
  return result;
}

function omit(source: RecordJson, path: string): RecordJson {
  const result = clone(source);
  const parts = path.split(".");
  let current: any = result;
  for (const part of parts.slice(0, -1)) current = current[Number.isInteger(Number(part)) ? Number(part) : part];
  delete current[parts.at(-1)!];
  return result;
}
