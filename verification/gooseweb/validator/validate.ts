import { createHash } from "node:crypto";
import { readFileSync } from "node:fs";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";

export const APPROVED_PLAN_SHA256 =
  "d3e55db431e8827ffbd7c4f7e41193a7fb77a99fdb446ea5dcf1fdfdc2b232b8";
export const MANIFEST_PATH =
  "verification/gooseweb/manifests/p01-team-comms-live.json";

const root = resolve(dirname(fileURLToPath(import.meta.url)), "../../..");
const PHASE = /^P(0[0-9]|[1-4][0-9]|5[0-6])$/;
const CORRECTION = /^P(0[6-9]|10)$/;
const RANGE_OR_SHORTHAND = /(?:P\d{2}\s*[-–—]\s*P\d{2}|P\d{2}\s*\.\.\s*P\d{2}|\+|\ball\b)/i;
const SECRET_KEY = /(?:password|passwd|api[_-]?key|bearer[_-]?token|csrf[_-]?(?:token|value)|cookie|ticket[_-]?(?:secret|value)|provider[_-]?auth|secret[_-]?config)/i;
const SECRET_VALUE = /(?:authorization:\s*bearer\s+\S+|(?:ticket|token|csrf|cookie|api_key)=[^&\s]+|-----BEGIN [A-Z ]+PRIVATE KEY-----)/i;

export type Json = null | boolean | number | string | Json[] | { [key: string]: Json };
export type RecordJson = { [key: string]: Json };

export function readJson(path: string): RecordJson {
  return JSON.parse(readFileSync(resolve(root, path), "utf8")) as RecordJson;
}

export function sha256(path: string): string {
  return createHash("sha256")
    .update(readFileSync(resolve(root, path)))
    .digest("hex");
}

export function validateManifest(value: RecordJson): void {
  required(value, [
    "schema_revision", "manifest_id", "manifest_revision", "approved_plan",
    "scenario", "ownership", "reconstruction", "responsive", "states",
    "fixtures", "allowlists", "baseline_detected", "known_defects",
    "non_applicable", "provenance"
  ], "manifest");
  equal(value.schema_revision, "gooseweb-acceptance-manifest/v1", "manifest schema");
  const plan = object(value.approved_plan, "approved_plan");
  equal(plan.sha256, APPROVED_PLAN_SHA256, "approved plan SHA");
  equal(plan.path, "tmp/gg/golden-goose-gooseweb-migration-implementation-plan.md", "approved plan path");
  equal(value.manifest_revision, 1, "manifest revision");

  const ownership = object(value.ownership, "ownership");
  required(ownership, ["gooselake", "goosetower", "gooseweb", "auth_account_state", "protocol_generated_code", "api_narrative_docs"], "ownership");
  for (const [name, entry] of Object.entries(ownership)) {
    requireReason(object(entry, `ownership.${name}`), `ownership.${name}`);
  }
  const nonApplicable = object(value.non_applicable, "non_applicable");
  required(nonApplicable, ["schema_migration", "backfill", "compatibility", "rollback", "performance", "security", "data_retention_gc", "deployment", "provenance_reuse"], "non_applicable");
  for (const [name, entry] of Object.entries(nonApplicable)) {
    requireReason(object(entry, `non_applicable.${name}`), `non_applicable.${name}`);
  }

  const baseline = array(value.baseline_detected, "baseline_detected");
  baseline.forEach((entry, index) => validateBaseline(object(entry, `baseline[${index}]`)));
  const defects = array(value.known_defects, "known_defects");
  if (defects.length !== 0) fail("known_defects must be empty at infrastructure clearance");
  scanSecrets(value);
}

export function validateLedger(value: RecordJson): void {
  required(value, ["schema_revision", "approved_plan_sha256", "phases", "lease_history", "clearance_history"], "ledger");
  equal(value.schema_revision, "gooseweb-phase-state-ledger/v1", "ledger schema");
  equal(value.approved_plan_sha256, APPROVED_PLAN_SHA256, "ledger plan SHA");
  const phases = array(value.phases, "phases").map((entry) => object(entry, "phase"));
  if (phases.length !== 57) fail("ledger must enumerate P00-P56 exactly");
  const ids = phases.map((entry) => string(entry.phase_id, "phase_id"));
  const expected = Array.from({ length: 57 }, (_, index) => `P${String(index).padStart(2, "0")}`);
  equal(JSON.stringify(ids), JSON.stringify(expected), "ordered phase IDs");
  const graph = new Map<string, string[]>();
  for (const entry of phases) {
    required(entry, ["phase_id", "prerequisites", "state", "state_reason", "history"], `phase ${entry.phase_id}`);
    const id = string(entry.phase_id, "phase_id");
    if (!PHASE.test(id)) fail(`malformed phase ID ${id}`);
    const prerequisites = array(entry.prerequisites, `${id}.prerequisites`).map((dep) => string(dep, `${id} dependency`));
    if (new Set(prerequisites).size !== prerequisites.length) fail(`${id} has duplicate dependencies`);
    for (const dep of prerequisites) {
      if (!PHASE.test(dep) || RANGE_OR_SHORTHAND.test(dep)) fail(`${id} has malformed or shorthand dependency ${dep}`);
      if (phaseNumber(dep) >= phaseNumber(id)) fail(`${id} depends on same/later phase ${dep}`);
    }
    graph.set(id, prerequisites);
  }
  if (!graph.get("P21")?.includes("P05")) fail("P21 must include P05");
  const p56 = graph.get("P56") ?? [];
  const exactP56 = expected.slice(1, 56);
  equal(JSON.stringify(p56), JSON.stringify(exactP56), "P56 exact P01-P55 prerequisites");
  proveAcyclic(graph);
  validateClearanceHistory(array(value.clearance_history, "clearance_history").map((item) => object(item, "clearance")));
  scanSecrets(value);
}

export function validateEvidence(value: RecordJson): void {
  required(value, ["schema_revision", "phase_id", "sha7", "attempt", "root", "manifest", "environment", "screenshots", "console", "network", "websocket", "runtime_state_redacted", "tower_state_redacted", "store_state_redacted", "checks", "metrics", "report", "clearance", "redaction"], "evidence");
  equal(value.schema_revision, "gooseweb-evidence-run/v1", "evidence schema");
  const phase = string(value.phase_id, "evidence phase");
  const sha7 = string(value.sha7, "sha7");
  const attempt = number(value.attempt, "attempt");
  equal(value.root, `tmp/gg/gooseweb-migration/${phase}/${sha7}/attempt-${attempt}/`, "evidence root convention");
  if (array(value.screenshots, "screenshots").length < 3) fail("evidence needs all three viewport screenshots");
  const redaction = object(value.redaction, "redaction");
  equal(redaction.capture_time, true, "capture-time redaction");
  scanSecrets(value);
}

export function validateClearance(value: RecordJson, expected?: RecordJson): void {
  required(value, ["schema_revision", "phase_id", "attempt", "base_sha", "candidate_head_sha", "candidate_tree_sha", "served_head_sha", "clean_tree", "hot_reload", "manifest", "approved_plan_sha256", "lease", "stack", "review", "baseline_detected", "known_defects", "clearance"], "clearance");
  equal(value.schema_revision, "gooseweb-exact-head-clearance/v1", "clearance schema");
  equal(value.approved_plan_sha256, APPROVED_PLAN_SHA256, "clearance plan SHA");
  equal(value.clean_tree, true, "clean tree");
  equal(value.hot_reload, false, "hot reload evidence");
  equal(value.candidate_head_sha, value.served_head_sha, "candidate/served head");
  for (const key of ["candidate_head_sha", "candidate_tree_sha", "base_sha"]) {
    if (!/^[a-f0-9]{40}$/.test(string(value[key], key))) fail(`${key} must be an exact SHA`);
  }
  const manifest = object(value.manifest, "clearance manifest");
  equal(manifest.path, MANIFEST_PATH, "manifest path");
  equal(manifest.revision, 1, "manifest revision");
  equal(manifest.sha256, sha256(MANIFEST_PATH), "manifest hash");
  if (expected) {
    for (const key of ["candidate_head_sha", "candidate_tree_sha", "served_head_sha"]) equal(value[key], expected[key], `expected ${key}`);
    equal(JSON.stringify(value.manifest), JSON.stringify(expected.manifest), "expected manifest tuple");
    equal(JSON.stringify(value.lease), JSON.stringify(expected.lease), "expected lease tuple");
    equal(JSON.stringify(value.stack), JSON.stringify(expected.stack), "expected stack tuple");
  }
  validateLease(object(value.lease, "lease"));
  const stack = object(value.stack, "stack");
  required(stack, ["dev_dir", "runtime_port", "tower_port", "gooseweb_port", "source_configuration", "branch", "mode", "configuration_sha256"], "stack");
  const review = object(value.review, "review");
  required(review, ["implementer_identity", "reviewer_identity", "reviewer_role", "browser_mechanism", "browser_session", "concurrent_mutation_build_format_generation", "findings_route", "final_approval_routed_to_implementer", "replacement_reviewer_full_rerun_required"], "review");
  if (review.implementer_identity === review.reviewer_identity) fail("reviewer and implementer must be distinct");
  equal(review.reviewer_role, "read_only", "reviewer role");
  equal(review.browser_mechanism, "agent-browser", "sole browser mechanism");
  equal(review.concurrent_mutation_build_format_generation, false, "reviewer/implementer overlap");
  equal(review.final_approval_routed_to_implementer, false, "approval route to implementer");
  const final = object(value.clearance, "clearance routing");
  equal(final.recipient_role, "lead", "clearance recipient");
  equal(final.product_approved, false, "P01 product approval");
  array(value.baseline_detected, "baseline").forEach((entry) => validateBaseline(object(entry, "baseline entry")));
  if (array(value.known_defects, "known_defects").length !== 0) fail("known_defects must be empty at clearance");
  scanSecrets(value);
}

export function validateClearanceHistory(records: RecordJson[]): void {
  const leaseIds = new Set<string>();
  const sessions = new Set<string>();
  let lastSequence = 0;
  for (const record of records) {
    validateClearance(record);
    const lease = object(record.lease, "lease");
    const id = string(lease.lease_id, "lease ID");
    const sequence = number(lease.sequence, "lease sequence");
    if (leaseIds.has(id) || sequence <= lastSequence) fail("lease IDs must be unique and sequences globally monotonic");
    leaseIds.add(id); lastSequence = sequence;
    const session = string(object(record.review, "review").browser_session, "browser session");
    if (sessions.has(session)) fail("browser session names must be globally unique");
    sessions.add(session);
  }
  const sorted = [...records].sort((a, b) => time(object(a.lease, "lease").acquired_at) - time(object(b.lease, "lease").acquired_at));
  for (let index = 1; index < sorted.length; index += 1) {
    const prior = object(sorted[index - 1]!.lease, "prior lease");
    const current = object(sorted[index]!.lease, "current lease");
    if (time(current.acquired_at) < time(prior.released_at)) fail("lease intervals overlap across phase/integration clearance records");
  }
}

function validateLease(lease: RecordJson): void {
  required(lease, ["lease_id", "sequence", "owner_role", "owner_identity", "acquired_at", "released_at", "prior_lease_termination_evidence", "managed_process"], "lease");
  if (!/^gooseweb-migration-\d{6,}$/.test(string(lease.lease_id, "lease ID"))) fail("lease ID is not globally sortable");
  if (number(lease.sequence, "lease sequence") < 1) fail("lease sequence must be positive");
  equal(lease.owner_role, "supervisor", "lease owner role");
  const prior = object(lease.prior_lease_termination_evidence, "prior lease termination");
  required(prior, ["status", "reference"], "prior lease termination");
  if (!string(prior.reference, "prior termination reference").trim()) fail("missing prior managed-process termination evidence");
  const process = object(lease.managed_process, "managed process");
  required(process, ["identity", "started_at", "stopped_at", "cleanup_completed_at", "termination_evidence"], "managed process");
  const acquired = time(lease.acquired_at), started = time(process.started_at), stopped = time(process.stopped_at), cleaned = time(process.cleanup_completed_at), released = time(lease.released_at);
  if (!(acquired <= started && started < stopped && stopped <= cleaned && cleaned <= released)) fail("lease release must follow managed-process stop and cleanup");
}

function validateBaseline(entry: RecordJson): void {
  required(entry, ["scenario_id", "first_divergent_layer", "evidence_references", "owning_correction_phase", "affected_downstream_gates", "product_scenario_status"], "baseline entry");
  if (!CORRECTION.test(string(entry.owning_correction_phase, "correction phase"))) fail("baseline correction phase must be P06-P10");
  if (array(entry.evidence_references, "baseline evidence").length === 0 || array(entry.affected_downstream_gates, "downstream gates").length === 0) fail("baseline entry needs evidence and downstream gates");
  equal(entry.product_scenario_status, "blocked_not_approved", "baseline product status");
}

function proveAcyclic(graph: Map<string, string[]>): void {
  const visiting = new Set<string>(), visited = new Set<string>();
  const visit = (id: string): void => {
    if (visiting.has(id)) fail(`dependency cycle at ${id}`);
    if (visited.has(id)) return;
    visiting.add(id); for (const dep of graph.get(id) ?? []) visit(dep); visiting.delete(id); visited.add(id);
  };
  for (const id of graph.keys()) visit(id);
}

function scanSecrets(value: Json, path = "root"): void {
  if (Array.isArray(value)) { value.forEach((item, index) => scanSecrets(item, `${path}[${index}]`)); return; }
  if (value && typeof value === "object") {
    for (const [key, item] of Object.entries(value)) {
      if (SECRET_KEY.test(key) && typeof item === "string" && item.trim()) fail(`secret-bearing field ${path}.${key}`);
      scanSecrets(item, `${path}.${key}`);
    }
    return;
  }
  if (typeof value === "string" && SECRET_VALUE.test(value)) fail(`secret-bearing value at ${path}`);
}

function required(value: RecordJson, keys: string[], label: string): void { for (const key of keys) if (!(key in value)) fail(`${label} omitted required field ${key}`); }
function requireReason(value: RecordJson, label: string): void { required(value, ["reason"], label); if (!string(value.reason, `${label}.reason`).trim()) fail(`${label} needs an explicit reason`); }
function object(value: Json | undefined, label: string): RecordJson { if (!value || typeof value !== "object" || Array.isArray(value)) fail(`${label} must be an object`); return value as RecordJson; }
function array(value: Json | undefined, label: string): Json[] { if (!Array.isArray(value)) fail(`${label} must be an array`); return value; }
function string(value: Json | undefined, label: string): string { if (typeof value !== "string") fail(`${label} must be a string`); return value; }
function number(value: Json | undefined, label: string): number { if (typeof value !== "number" || !Number.isFinite(value)) fail(`${label} must be a number`); return value; }
function equal(actual: Json | undefined, expected: Json | undefined, label: string): void { if (actual !== expected) fail(`${label} changed: expected ${String(expected)}, received ${String(actual)}`); }
function phaseNumber(id: string): number { return Number(id.slice(1)); }
function time(value: Json | undefined): number { const parsed = Date.parse(string(value, "timestamp")); if (!Number.isFinite(parsed)) fail("invalid timestamp"); return parsed; }
function fail(message: string): never { throw new Error(message); }
