import { create } from "@bufbuild/protobuf";
import { SourceCursorSchema } from "../../../src/gen/goosetower/v1/common_pb";
import type { SourceSnapshotResync } from "../../../src/gen/goosetower/v1/realtime_pb";
import {
  ApprovalViewSchema,
  FleetRowViewSchema,
  ProcessViewSchema,
  SessionViewSchema,
  SourceHealthViewSchema,
  TeamMemberViewSchema,
  TeamViewSchema,
  WorktreeViewSchema
} from "../../../src/gen/goosetower/v1/view_pb";
import type { EntityDomain } from "../types";
import type { EntityPatch } from "./entities";
import { ProtocolDecodeError } from "./protocol-error";

const SOURCE_REPLACEMENT_DOMAINS = [
  "fleet_rows", "sessions", "session_details", "teams", "team_workspaces",
  "approvals", "processes", "worktrees", "sources"
] as const;

export function decodeSourceSnapshotResync(resync: SourceSnapshotResync): EntityPatch {
  if (resync.schemaVersion !== 1) {
    throw new ProtocolDecodeError(`unsupported source resync schema ${resync.schemaVersion}`);
  }
  const sourceId = requireString(resync.sourceId, "source_resync.source_id");
  const authority = resync.cursor;
  if (!authority || authority.sources.length !== 1) {
    throw new ProtocolDecodeError("source resync must carry one canonical source cursor");
  }
  const sourceAuthority = authority.sources[0];
  if (!sourceAuthority || sourceAuthority.sourceId !== sourceId) {
    throw new ProtocolDecodeError("source resync cursor disagrees with source identity");
  }
  requireCoverage(resync);
  const body = parseBody(resync.body);
  if (requireString(body.source_id, "source_resync.source_id") !== sourceId) {
    throw new ProtocolDecodeError("source resync body disagrees with source identity");
  }

  const fleetRows = decodeUnique(body.fleet_rows, "fleet_rows", (item, field) => {
    const row = ownedRecord(item, sourceId, field);
    const id = requireString(row.row_id, `${field}.row_id`);
    requireString(row.session_id, `${field}.session_id`);
    requireNullableString(row.team_id, `${field}.team_id`);
    requireNullableString(row.title, `${field}.title`);
    requireString(row.provider, `${field}.provider`);
    requireNullableString(row.model, `${field}.model`);
    requireString(row.status, `${field}.status`);
    requireNullableString(row.worktree_path, `${field}.worktree_path`);
    requireNumber(row.pending_approval_count, `${field}.pending_approval_count`);
    requireNumber(row.latest_activity_unix_ms, `${field}.latest_activity_unix_ms`);
    return [id, create(FleetRowViewSchema, {
      rowId: id,
      sourceId,
      sessionId: row.session_id as string,
      teamId: nullableString(row.team_id),
      provider: row.provider as string,
      model: nullableString(row.model),
      status: row.status as string,
      title: nullableString(row.title),
      worktreePath: nullableString(row.worktree_path),
      pendingApprovalCount: row.pending_approval_count as number,
      latestActivityUnixMs: BigInt(row.latest_activity_unix_ms as number)
    })];
  });

  const sessions = decodeUnique(body.sessions, "sessions", (item, field) => {
    const summary = ownedRecord(item, sourceId, field);
    const session = strictRecord(summary.session, `${field}.session`);
    const id = requireString(session.id, `${field}.session.id`);
    requireString(session.provider, `${field}.session.provider`);
    requireNullableString(session.model, `${field}.session.model`);
    requireString(session.status, `${field}.session.status`);
    requireNullableString(session.cwd, `${field}.session.cwd`);
    requireNullableString(session.worktree_path, `${field}.session.worktree_path`);
    requireNullableString(session.active_turn_id, `${field}.session.active_turn_id`);
    return [id, create(SessionViewSchema, {
      sourceId,
      sessionId: id,
      provider: session.provider as string,
      model: nullableString(session.model),
      status: session.status as string,
      cwd: nullableString(session.cwd),
      worktreePath: nullableString(session.worktree_path),
      activeTurnId: nullableString(session.active_turn_id)
    })];
  });
  requireEmptyArray(body.session_details, "source_resync.session_details");

  const teams = decodeUnique(body.teams, "teams", (item, field) => {
    const summary = ownedRecord(item, sourceId, field);
    const team = strictRecord(summary.team, `${field}.team`);
    const id = requireString(team.id, `${field}.team.id`);
    requireString(team.name, `${field}.team.name`);
    requireString(team.lead_agent_id, `${field}.team.lead_agent_id`);
    const members = requireArray(summary.members, `${field}.members`).map((memberItem, index) => {
      const memberView = strictRecord(memberItem, `${field}.members[${index}]`);
      const member = strictRecord(memberView.member, `${field}.members[${index}].member`);
      if (requireString(member.team_id, `${field}.members[${index}].member.team_id`) !== id) {
        throw new ProtocolDecodeError("source resync team member belongs to another team");
      }
      const memberId = requireString(member.agent_id, `${field}.members[${index}].member.agent_id`);
      requireNullableString(member.title, `${field}.members[${index}].member.title`);
      const session = memberView.session === null
        ? undefined
        : strictRecord(memberView.session, `${field}.members[${index}].session`);
      if (session) {
        requireString(session.id, `${field}.members[${index}].session.id`);
        requireString(session.provider, `${field}.members[${index}].session.provider`);
        requireNullableString(session.model, `${field}.members[${index}].session.model`);
        requireString(session.status, `${field}.members[${index}].session.status`);
      }
      return create(TeamMemberViewSchema, {
        memberId,
        sessionId: session ? session.id as string : memberId,
        title: nullableString(member.title),
        provider: session ? session.provider as string : "",
        model: session ? nullableString(session.model) : "",
        status: session ? session.status as string : ""
      });
    });
    return [id, create(TeamViewSchema, {
      sourceId,
      teamId: id,
      name: team.name as string,
      leadMemberId: team.lead_agent_id as string,
      members
    })];
  });
  requireEmptyArray(body.team_workspaces, "source_resync.team_workspaces");

  const approvals = decodeUnique(body.approvals, "approvals", (item, field) => {
    const approval = ownedRecord(item, sourceId, field);
    const id = requireString(approval.approval_id, `${field}.approval_id`);
    requireString(approval.session_id, `${field}.session_id`);
    requireString(approval.turn_id, `${field}.turn_id`);
    requireString(approval.risk, `${field}.risk`);
    requireString(approval.status, `${field}.status`);
    requireString(approval.summary, `${field}.summary`, true);
    return [id, create(ApprovalViewSchema, {
      sourceId,
      approvalId: id,
      sessionId: approval.session_id as string,
      turnId: approval.turn_id as string,
      risk: approval.risk as string,
      status: approval.status as string,
      summary: approval.summary as string
    })];
  });

  const processes = decodeUnique(body.processes, "processes", (item, field) => {
    const process = ownedRecord(item, sourceId, field);
    const id = requireString(process.process_id, `${field}.process_id`);
    requireString(process.status, `${field}.status`);
    requireNullableFiniteNumber(process.exit_code, `${field}.exit_code`);
    if (!("command" in process)) throw new ProtocolDecodeError(`${field}.command is required`);
    return [id, create(ProcessViewSchema, {
      sourceId,
      processId: id,
      status: process.status as string,
      command: typeof process.command === "string"
        ? process.command
        : JSON.stringify(process.command),
      exitCode: process.exit_code === null ? 0 : process.exit_code as number
    })];
  });

  const worktrees = decodeUnique(body.worktrees, "worktrees", (item, field) => {
    const worktree = ownedRecord(item, sourceId, field);
    const id = requireString(worktree.worktree_id, `${field}.worktree_id`);
    requireString(worktree.worktree_root, `${field}.worktree_root`);
    requireString(worktree.branch_name, `${field}.branch_name`, true);
    requireString(worktree.status, `${field}.status`);
    return [id, create(WorktreeViewSchema, {
      sourceId,
      worktreeId: id,
      path: worktree.worktree_root as string,
      branch: worktree.branch_name as string,
      status: worktree.status as string
    })];
  });

  const sourceHealth = decodeSourceHealth(body.source_health, sourceId);
  if (
    sourceHealth.cursor?.sourceEpoch !== sourceAuthority.sourceEpoch ||
    sourceHealth.cursor.sourceSeq !== sourceAuthority.sourceSeq
  ) {
    throw new ProtocolDecodeError("source health authority disagrees with resync cursor");
  }
  const payloads = {
    fleetRows,
    sessions,
    sessionDetails: {},
    teams,
    teamWorkspaces: {},
    approvals,
    processes,
    worktrees,
    sources: { [sourceId]: sourceHealth }
  };
  return {
    entityOperations: (Object.keys(payloads) as EntityDomain[]).map((domain) => ({
      operation: "replace",
      domain,
      entityIds: [],
      authoritative: true,
      sourceId,
      payload: payloads[domain]
    }))
  };
}

function decodeSourceHealth(value: unknown, sourceId: string) {
  const source = ownedRecord(value, sourceId, "source_resync.source_health");
  const epoch = requireString(source.source_epoch, "source_resync.source_health.source_epoch");
  const seq = requireNumber(source.last_source_seq, "source_resync.source_health.last_source_seq");
  requireString(source.display_name, "source_resync.source_health.display_name");
  requireString(source.source_kind, "source_resync.source_health.source_kind");
  requireString(source.provisioner_kind, "source_resync.source_health.provisioner_kind");
  requireString(source.state, "source_resync.source_health.state");
  requireNumber(source.observed_at_unix_ms, "source_resync.source_health.observed_at_unix_ms");
  requireNumber(source.active_session_count, "source_resync.source_health.active_session_count");
  requireNumber(source.active_process_count, "source_resync.source_health.active_process_count");
  const providers = requireStringArray(source.provider_kinds, "source_resync.source_health.provider_kinds");
  const models = requireStringArray(source.models, "source_resync.source_health.models");
  requireNullableNumber(source.process_capacity, "source_resync.source_health.process_capacity");
  requireBoolean(source.supports_worktrees, "source_resync.source_health.supports_worktrees");
  requireBoolean(source.supports_teams, "source_resync.source_health.supports_teams");
  requireNullableNumber(source.replay_window_events, "source_resync.source_health.replay_window_events");
  requireNullableNumber(source.replay_window_ms, "source_resync.source_health.replay_window_ms");
  requireNullableString(source.region, "source_resync.source_health.region");
  requireNullableString(source.cost_hint, "source_resync.source_health.cost_hint");
  const capabilities = requireArray(
    source.model_capabilities,
    "source_resync.source_health.model_capabilities"
  ).map((item, index) => {
    const capability = strictRecord(item, `source_resync.source_health.model_capabilities[${index}]`);
    return {
      provider: requireString(capability.provider, `source_resync.source_health.model_capabilities[${index}].provider`),
      model: requireString(capability.model, `source_resync.source_health.model_capabilities[${index}].model`),
      displayName: requireString(capability.display_name, `source_resync.source_health.model_capabilities[${index}].display_name`),
      reasoningLevels: requireStringArray(capability.reasoning_levels, `source_resync.source_health.model_capabilities[${index}].reasoning_levels`)
    };
  });
  return create(SourceHealthViewSchema, {
    sourceId,
    displayName: source.display_name as string,
    sourceKind: source.source_kind as string,
    health: source.state as string,
    cursor: create(SourceCursorSchema, { sourceId, sourceEpoch: epoch, sourceSeq: BigInt(seq) }),
    observedAtUnixMs: BigInt(source.observed_at_unix_ms as number),
    lifecycle: source.state as string,
    provisionerKind: source.provisioner_kind as string,
    providerKinds: providers,
    models,
    modelCapabilities: capabilities,
    activeSessionCount: source.active_session_count as number,
    activeProcessCount: source.active_process_count as number,
    processCapacity: source.process_capacity === null ? 0 : source.process_capacity as number,
    supportsWorktrees: source.supports_worktrees as boolean,
    supportsTeams: source.supports_teams as boolean,
    replayWindowEvents: source.replay_window_events === null ? 0n : BigInt(source.replay_window_events as number),
    replayWindowMs: source.replay_window_ms === null ? 0n : BigInt(source.replay_window_ms as number),
    region: nullableString(source.region),
    costHint: nullableString(source.cost_hint)
  });
}

function requireCoverage(resync: SourceSnapshotResync): void {
  const coverage = resync.coverage;
  if (
    !coverage?.authoritative || coverage.entityIds.length !== 0 ||
    coverage.domains.length !== SOURCE_REPLACEMENT_DOMAINS.length ||
    SOURCE_REPLACEMENT_DOMAINS.some((domain, index) => coverage.domains[index] !== domain)
  ) {
    throw new ProtocolDecodeError("source resync lacks exact authoritative source coverage");
  }
}

function parseBody(bytes: Uint8Array): Record<string, unknown> {
  try {
    return strictRecord(JSON.parse(new TextDecoder().decode(bytes)), "source_resync");
  } catch (error) {
    if (error instanceof ProtocolDecodeError) throw error;
    throw new ProtocolDecodeError("malformed source resync JSON body");
  }
}

function decodeUnique(
  value: unknown,
  field: string,
  decode: (item: unknown, field: string) => readonly [string, unknown]
): Record<string, unknown> {
  const result: Record<string, unknown> = {};
  for (const [index, item] of requireArray(value, `source_resync.${field}`).entries()) {
    const [id, entity] = decode(item, `source_resync.${field}[${index}]`);
    if (id in result) throw new ProtocolDecodeError(`duplicate ${field} entity ID ${id}`);
    result[id] = entity;
  }
  return result;
}

function ownedRecord(value: unknown, sourceId: string, field: string): Record<string, unknown> {
  const record = strictRecord(value, field);
  if (requireString(record.source_id, `${field}.source_id`) !== sourceId) {
    throw new ProtocolDecodeError(`${field} belongs to a different source`);
  }
  return record;
}

function strictRecord(value: unknown, field: string): Record<string, unknown> {
  if (!value || typeof value !== "object" || Array.isArray(value)) {
    throw new ProtocolDecodeError(`${field} must be an object`);
  }
  return value as Record<string, unknown>;
}

function requireArray(value: unknown, field: string): unknown[] {
  if (!Array.isArray(value)) throw new ProtocolDecodeError(`${field} must be an array`);
  return value;
}

function requireEmptyArray(value: unknown, field: string): void {
  if (requireArray(value, field).length !== 0) {
    throw new ProtocolDecodeError(`${field} must be empty and repaired by scoped subscription`);
  }
}

function requireString(value: unknown, field: string, allowEmpty = false): string {
  if (typeof value !== "string" || (!allowEmpty && value.length === 0)) {
    throw new ProtocolDecodeError(`${field} must be a string`);
  }
  return value;
}

function requireNullableString(value: unknown, field: string): void {
  if (value !== null && typeof value !== "string") {
    throw new ProtocolDecodeError(`${field} must be a string or null`);
  }
}

function requireNumber(value: unknown, field: string): number {
  if (typeof value !== "number" || !Number.isFinite(value) || value < 0) {
    throw new ProtocolDecodeError(`${field} must be a nonnegative number`);
  }
  return value;
}

function requireNullableNumber(value: unknown, field: string): void {
  if (value !== null) requireNumber(value, field);
}

function requireNullableFiniteNumber(value: unknown, field: string): void {
  if (value !== null && (typeof value !== "number" || !Number.isFinite(value))) {
    throw new ProtocolDecodeError(`${field} must be a number or null`);
  }
}

function requireBoolean(value: unknown, field: string): void {
  if (typeof value !== "boolean") throw new ProtocolDecodeError(`${field} must be a boolean`);
}

function requireStringArray(value: unknown, field: string): string[] {
  const array = requireArray(value, field);
  if (!array.every((item) => typeof item === "string")) {
    throw new ProtocolDecodeError(`${field} must contain only strings`);
  }
  return array as string[];
}

function nullableString(value: unknown): string {
  return typeof value === "string" ? value : "";
}
