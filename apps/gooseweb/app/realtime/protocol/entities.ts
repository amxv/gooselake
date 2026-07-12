import { create, fromBinary } from "@bufbuild/protobuf";
import { SourceCursorSchema } from "../../../src/gen/goosetower/v1/common_pb";
import {
  ApprovalViewSchema,
  FleetRowViewSchema,
  ProcessViewSchema,
  SessionViewSchema,
  SourceHealthViewSchema,
  TeamMemberViewSchema,
  TeamViewSchema,
  WorktreeViewSchema,
  ViewOperation,
  type Patch,
  type Snapshot
} from "../../../src/gen/goosetower/v1/view_pb";
import type { EntityDomain, EntityMutation, NormalizedEntityPatch } from "../types";

export type EntityPatch = {
  readonly entities: NormalizedEntityPatch;
  readonly entityMutations: readonly EntityMutation[];
};

type DecodedEntities = { readonly entities: NormalizedEntityPatch };

export function decodeSnapshot(snapshot: Snapshot): EntityPatch {
  const operation = operationFromFrame(snapshot.schemaVersion, snapshot.operation, "replace");
  if (operation !== "replace") {
    throw new ProtocolDecodeError(`snapshot operation must be replace, received ${operation}`);
  }
  requireDeclaredCoverage(snapshot.schemaVersion, snapshot.coverage);
  return withMutation(
    decodeViewBody(snapshot.viewKind, snapshot.body),
    snapshot.viewKind,
    operation,
    snapshot.coverage?.domains,
    snapshot.coverage?.entityIds,
    snapshot.coverage?.authoritative ?? snapshot.schemaVersion === 0
  );
}

export function decodePatch(patch: Patch): EntityPatch {
  const operation = operationFromFrame(patch.schemaVersion, patch.operation, "upsert");
  requireDeclaredCoverage(patch.schemaVersion, patch.coverage);
  const entityIds = patch.coverage?.entityIds.length
    ? patch.coverage.entityIds
    : patch.entity?.entityId ? [patch.entity.entityId] : [];
  const decoded = operation === "remove"
    ? { entities: {} }
    : decodeViewBody(patch.viewKind, patch.body);
  return withMutation(
    decoded,
    patch.viewKind,
    operation,
    patch.coverage?.domains,
    entityIds,
    patch.coverage?.authoritative ?? patch.schemaVersion === 0
  );
}

function requireDeclaredCoverage(
  schemaVersion: number,
  coverage: Snapshot["coverage"]
): void {
  if (
    schemaVersion === 1 &&
    (!coverage?.authoritative || coverage.domains.length === 0)
  ) {
    throw new ProtocolDecodeError("versioned view frame lacks authoritative coverage");
  }
}

export class ProtocolDecodeError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "ProtocolDecodeError";
  }
}

function operationFromFrame(
  schemaVersion: number,
  operation: ViewOperation,
  legacyOperation: EntityMutation["operation"]
): EntityMutation["operation"] {
  if (schemaVersion === 0 && operation === ViewOperation.UNSPECIFIED) {
    return legacyOperation;
  }
  if (schemaVersion !== 1) {
    throw new ProtocolDecodeError(`unsupported view schema version ${schemaVersion}`);
  }
  switch (operation) {
    case ViewOperation.REPLACE: return "replace";
    case ViewOperation.UPSERT: return "upsert";
    case ViewOperation.REMOVE: return "remove";
    default: throw new ProtocolDecodeError("view operation is unspecified or unknown");
  }
}

function withMutation(
  decoded: DecodedEntities,
  viewKind: string,
  operation: EntityMutation["operation"],
  declaredDomains: readonly string[] | undefined,
  entityIds: readonly string[] | undefined,
  authoritative: boolean
): EntityPatch {
  // Ledger remains a presentation-only bounded compatibility view until its
  // normalized browser domain lands. It is deliberately named here so future
  // unknown kinds still fail closed instead of becoming empty state.
  if (viewKind === "ledger") {
    return { entities: decoded.entities, entityMutations: [] };
  }
  const domain = domainForViewKind(viewKind);
  if (!domain) {
    throw new ProtocolDecodeError(`unknown view kind ${viewKind}`);
  }
  if (declaredDomains?.length && !declaredDomains.includes(domainToWire(domain))) {
    throw new ProtocolDecodeError(`coverage does not declare ${domainToWire(domain)}`);
  }
  return {
    entities: decoded.entities,
    entityMutations: [{
      operation,
      domain,
      entityIds: entityIds?.length ? entityIds : Object.keys(decoded.entities[domain] ?? {}),
      authoritative
    }]
  };
}

function domainForViewKind(viewKind: string): EntityDomain | undefined {
  switch (viewKind) {
    case "board": case "fleet-row": case "board-row": return "fleetRows";
    case "session": case "session_summary": return "sessions";
    case "session_detail": return "sessionDetails";
    case "team": case "team_summary": case "teams": return "teams";
    case "team_workspace": case "team_stream": return "teamWorkspaces";
    case "approval": case "approval_inbox": return "approvals";
    case "process": return "processes";
    case "worktree": case "worktrees": return "worktrees";
    case "source-health": case "source_health": case "source": case "fleet": return "sources";
    default: return undefined;
  }
}

function domainToWire(domain: EntityDomain): string {
  return domain.replace(/[A-Z]/g, (letter) => `_${letter.toLowerCase()}`);
}

function decodeViewBody(viewKind: string, body: Uint8Array): DecodedEntities {
  if (viewKind === "ledger") {
    const value = decodeJsonViewBody(viewKind, body);
    return value ?? { entities: {} };
  }
  if (!domainForViewKind(viewKind)) {
    throw new ProtocolDecodeError(`unknown view kind ${viewKind}`);
  }
  const jsonPatch = decodeJsonViewBody(viewKind, body);
  if (jsonPatch) {
    return jsonPatch;
  }

  switch (viewKind) {
    case "fleet-row":
    case "board-row": {
      const row = fromBinary(FleetRowViewSchema, body);
      return { entities: { fleetRows: { [row.rowId]: row } } };
    }
    case "session":
    case "session_summary": {
      const session = fromBinary(SessionViewSchema, body);
      return { entities: { sessions: { [session.sessionId]: session } } };
    }
    case "team":
    case "team_summary": {
      const team = fromBinary(TeamViewSchema, body);
      return { entities: { teams: { [team.teamId]: team } } };
    }
    case "approval": {
      const approval = fromBinary(ApprovalViewSchema, body);
      return { entities: { approvals: { [approval.approvalId]: approval } } };
    }
    case "process": {
      const process = fromBinary(ProcessViewSchema, body);
      return { entities: { processes: { [process.processId]: process } } };
    }
    case "worktree": {
      const worktree = fromBinary(WorktreeViewSchema, body);
      return { entities: { worktrees: { [worktree.worktreeId]: worktree } } };
    }
    case "source-health":
    case "source": {
      const source = fromBinary(SourceHealthViewSchema, body);
      return { entities: { sources: { [source.sourceId]: source } } };
    }
    default:
      throw new ProtocolDecodeError(`view kind ${viewKind} has no binary decoder`);
  }
}

function decodeJsonViewBody(
  viewKind: string,
  body: Uint8Array
): DecodedEntities | undefined {
  const text = new TextDecoder().decode(body);
  let value: unknown;
  try {
    value = JSON.parse(text);
  } catch {
    if (/^\s*[\[{]/.test(text)) {
      throw new ProtocolDecodeError(`malformed ${viewKind} JSON body`);
    }
    return undefined;
  }

  if (!value || typeof value !== "object") {
    throw new ProtocolDecodeError(`malformed ${viewKind} JSON body`);
  }

  switch (viewKind) {
    case "board": {
      const rows = arrayFrom((value as { rows?: unknown }).rows);
      return {
        entities: {
          fleetRows: Object.fromEntries(
            rows.map((row) => {
              const entity = normalizeFleetRow(row);
              return [entity.rowId, entity];
            })
          )
        }
      };
    }
    case "approval_inbox": {
      const approvals = arrayFrom((value as { approvals?: unknown }).approvals);
      return {
        entities: {
          approvals: Object.fromEntries(
            approvals.map((approval) => {
              const entity = normalizeApproval(approval);
              return [entity.approvalId, entity];
            })
          )
        }
      };
    }
    case "source_health":
    case "fleet":
    case "source-health":
    case "source": {
      if (Array.isArray(value)) {
        return {
          entities: {
            sources: Object.fromEntries(
              value.map((item) => {
                const source = normalizeSource(item);
                return [source.sourceId, source];
              })
            )
          }
        };
      }
      const source = normalizeSource(value);
      return { entities: { sources: { [source.sourceId]: source } } };
    }
    case "team":
    case "team_summary": {
      const team = normalizeTeam(value);
      return team ? { entities: { teams: { [team.teamId]: team } } } : { entities: {} };
    }
    case "session":
    case "session_summary": {
      const session = normalizeSession(value);
      return session
        ? { entities: { sessions: { [session.sessionId]: session } } }
        : { entities: {} };
    }
    case "session_detail": {
      const detail = normalizeSessionDetail(value);
      return detail
        ? { entities: { sessionDetails: { [detail.sessionId]: detail } } }
        : { entities: {} };
    }
    case "team_workspace":
    case "team_stream": {
      const workspace = normalizeTeamWorkspace(value);
      const team = normalizeTeam(value);
      return {
        entities: {
          ...(team ? { teams: { [team.teamId]: team } } : {}),
          ...(workspace ? { teamWorkspaces: { [workspace.teamId]: workspace } } : {})
        }
      };
    }
    case "teams": {
      const teams = arrayFrom((value as { teams?: unknown }).teams)
        .map((team) => normalizeTeam(team))
        .filter((team): team is NonNullable<ReturnType<typeof normalizeTeam>> => Boolean(team));
      return {
        entities: {
          teams: Object.fromEntries(teams.map((team) => [team.teamId, team]))
        }
      };
    }
    case "ledger":
      return { entities: {} };
    default:
      throw new ProtocolDecodeError(`unknown JSON view kind ${viewKind}`);
  }
}

function normalizeFleetRow(value: unknown) {
  const row = recordFrom(value);
  return create(FleetRowViewSchema, {
    rowId: stringFrom(row.row_id),
    sourceId: stringFrom(row.source_id),
    sessionId: stringFrom(row.session_id),
    teamId: stringFrom(row.team_id),
    provider: stringFrom(row.provider),
    model: stringFrom(row.model),
    status: stringFrom(row.status),
    title: stringFrom(row.title),
    worktreePath: stringFrom(row.worktree_path),
    pendingApprovalCount: numberFrom(row.pending_approval_count),
    latestActivityUnixMs: bigintFrom(row.latest_activity_unix_ms)
  });
}

function normalizeApproval(value: unknown) {
  const approval = recordFrom(value);
  return create(ApprovalViewSchema, {
    sourceId: stringFrom(approval.source_id),
    approvalId: stringFrom(approval.approval_id),
    sessionId: stringFrom(approval.session_id),
    turnId: stringFrom(approval.turn_id),
    risk: stringFrom(approval.risk),
    status: stringFrom(approval.status),
    summary: stringFrom(approval.summary)
  });
}

function normalizeSource(value: unknown) {
  const source = recordFrom(value);
  const state = stringFrom(source.state || source.health || "unknown");
  const sourceId = stringFrom(source.source_id);
  return create(SourceHealthViewSchema, {
    sourceId,
    displayName: stringFrom(source.display_name) || sourceId || "source",
    sourceKind: stringFrom(source.source_kind) || "gooselake-runtime",
    health: state,
    cursor: create(SourceCursorSchema, {
      sourceId,
      sourceEpoch: stringFrom(source.source_epoch),
      sourceSeq: bigintFrom(source.last_source_seq)
    }),
    observedAtUnixMs: bigintFrom(source.observed_at_unix_ms),
    lifecycle: stringFrom(source.lifecycle) || state,
    provisionerKind: stringFrom(source.provisioner_kind) || "static",
    providerKinds: stringArrayFrom(source.provider_kinds),
    models: stringArrayFrom(source.models),
    modelCapabilities: arrayFrom(source.model_capabilities).map((capability) => {
      const record = recordFrom(capability);
      return {
        provider: stringFrom(record.provider),
        model: stringFrom(record.model),
        displayName: stringFrom(record.display_name),
        reasoningLevels: stringArrayFrom(record.reasoning_levels)
      };
    }),
    activeSessionCount: numberFrom(source.active_session_count),
    activeProcessCount: numberFrom(source.active_process_count),
    processCapacity: numberFrom(source.process_capacity),
    supportsWorktrees: Boolean(source.supports_worktrees),
    supportsTeams: Boolean(source.supports_teams),
    replayWindowEvents: bigintFrom(source.replay_window_events),
    replayWindowMs: bigintFrom(source.replay_window_ms),
    region: stringFrom(source.region),
    costHint: stringFrom(source.cost_hint)
  });
}

function normalizeSession(value: unknown) {
  const detail = recordFrom(value);
  const record = recordFrom(detail.session);
  const sessionId = stringFrom(record.id || detail.session_id);
  if (!sessionId) {
    return undefined;
  }
  const metadata = recordFrom(record.metadata);
  const contextWindow = recordFrom(metadata.context_window);
  const hasContextWindow = typeof contextWindow.remaining_percent === "number";
  return create(SessionViewSchema, {
    sourceId: stringFrom(detail.source_id),
    sessionId,
    provider: stringFrom(record.provider),
    model: stringFrom(record.model),
    status: stringFrom(record.status),
    cwd: stringFrom(record.cwd),
    worktreePath: stringFrom(record.worktree_path),
    activeTurnId: stringFrom(record.active_turn_id),
    ...(hasContextWindow
      ? {
          contextRemainingPercent: numberFrom(contextWindow.remaining_percent),
          contextWindowTokens: bigintFrom(contextWindow.window_tokens),
          contextUsedTokens: bigintFrom(contextWindow.used_tokens)
        }
      : {})
  });
}

function normalizeSessionDetail(value: unknown) {
  const detail = recordFrom(value);
  const record = recordFrom(detail.session);
  const sessionId = stringFrom(record.id || detail.session_id);
  if (!sessionId) {
    return undefined;
  }
  const appendedText = stringFrom(detail.appended_text || detail.text);
  const turnId = stringFrom(detail.turn_id);
  const transcript = [
    ...arrayFrom(detail.transcript).map((entry, index) => {
      const row = recordFrom(entry);
      return {
        id: stringFrom(row.id) || `${sessionId}:transcript:${index}`,
        sessionId,
        role: stringFrom(row.role) || "assistant",
        text: stringFrom(row.text),
        turnId: stringFrom(row.turn_id) || undefined,
        createdAtUnixMs: numberFrom(row.created_at)
      };
    }),
    ...(appendedText
      ? [{
          id: `${sessionId}:${turnId || "turn"}:${appendedText}`,
          sessionId,
          role: "assistant",
          text: appendedText,
          turnId: turnId || undefined,
          createdAtUnixMs: numberFrom(detail.created_at)
        }]
      : [])
  ].filter((entry) => entry.text);

  return {
    sessionId,
    sourceId: stringFrom(detail.source_id),
    transcript,
    appendedText,
    latestActivityUnixMs: numberFrom(detail.latest_activity_unix_ms)
  };
}

function normalizeTeam(value: unknown) {
  const workspace = recordFrom(value);
  const teamRecord = recordFrom(workspace.team);
  const teamId = stringFrom(teamRecord.id || workspace.team_id);
  if (!teamId) {
    return undefined;
  }
  const members = collectionFrom(workspace.members).map((memberValue) => {
    const memberView = recordFrom(memberValue);
    const member = recordFrom(memberView.member);
    const session = recordFrom(memberView.session);
    return create(TeamMemberViewSchema, {
      memberId: stringFrom(member.agent_id),
      sessionId: stringFrom(session.id || member.agent_id),
      title: stringFrom(member.title),
      provider: stringFrom(session.provider),
      model: stringFrom(session.model),
      status: stringFrom(session.status)
    });
  });
  return create(TeamViewSchema, {
    sourceId: stringFrom(workspace.source_id),
    teamId,
    name: stringFrom(teamRecord.name),
    leadMemberId: stringFrom(teamRecord.lead_agent_id),
    members
  });
}

function normalizeTeamWorkspace(value: unknown) {
  const workspace = recordFrom(value);
  const teamRecord = recordFrom(workspace.team);
  const deliveryRecord = recordFrom(workspace.delivery);
  const messageRecord = recordFrom(workspace.message);
  const directDelivery = stringFrom(workspace.recipient_agent_id || workspace.message_id)
    ? workspace
    : undefined;
  const directMessage = stringFrom(workspace.sender_agent_id) || workspace.input
    ? workspace
    : undefined;
  const teamId = stringFrom(
    teamRecord.id ||
    workspace.team_id ||
    deliveryRecord.team_id ||
    messageRecord.team_id
  );
  if (!teamId) {
    return undefined;
  }
  return {
    teamId,
    sourceId: stringFrom(workspace.source_id),
    messages: [
      ...collectionFrom(workspace.messages).map(normalizeTeamMessage),
      ...(stringFrom(messageRecord.id) ? [normalizeTeamMessage(messageRecord)] : []),
      ...(directMessage ? [normalizeTeamMessage(directMessage)] : [])
    ].filter((message) => message.id),
    deliveries: [
      ...collectionFrom(workspace.deliveries).map(normalizeTeamDelivery),
      ...(stringFrom(deliveryRecord.id) ? [normalizeTeamDelivery(deliveryRecord)] : []),
      ...(directDelivery ? [normalizeTeamDelivery(directDelivery)] : [])
    ].filter((delivery) => delivery.id)
  };
}

function normalizeTeamMessage(value: unknown) {
  const message = recordFrom(value);
  return {
    id: stringFrom(message.id),
    teamId: stringFrom(message.team_id),
    scope: stringFrom(message.scope),
    senderAgentId: stringFrom(message.sender_agent_id),
    recipientAgentIds: stringArrayFrom(message.recipient_agent_ids),
    text: messageText(message.input),
    createdAtUnixMs: numberFrom(message.created_at)
  };
}

function normalizeTeamDelivery(value: unknown) {
  const delivery = recordFrom(value);
  return {
    id: stringFrom(delivery.id),
    messageId: stringFrom(delivery.message_id),
    teamId: stringFrom(delivery.team_id),
    recipientAgentId: stringFrom(delivery.recipient_agent_id),
    provider: stringFrom(delivery.provider),
    status: stringFrom(delivery.status),
    injectedTurnId: stringFrom(delivery.injected_turn_id) || undefined,
    lastError: stringFrom(delivery.last_error_message || delivery.last_error_code) || undefined,
    updatedAtUnixMs: numberFrom(delivery.updated_at)
  };
}

function messageText(value: unknown): string {
  if (typeof value === "string") {
    return value;
  }
  const items = Array.isArray(value) ? value : [];
  return items
    .map((item) => stringFrom(recordFrom(item).text))
    .filter(Boolean)
    .join("\n");
}

function arrayFrom(value: unknown): unknown[] {
  return Array.isArray(value) ? value : [];
}

function collectionFrom(value: unknown): unknown[] {
  if (Array.isArray(value)) {
    return value;
  }
  if (value && typeof value === "object") {
    return Object.values(value);
  }
  return [];
}

function recordFrom(value: unknown): Record<string, unknown> {
  return value && typeof value === "object" ? value as Record<string, unknown> : {};
}

function stringFrom(value: unknown): string {
  return typeof value === "string" ? value : "";
}

function numberFrom(value: unknown): number {
  return typeof value === "number" ? value : 0;
}

function bigintFrom(value: unknown): bigint {
  return typeof value === "number" ? BigInt(value) : 0n;
}

function stringArrayFrom(value: unknown): string[] {
  return Array.isArray(value)
    ? value.filter((item): item is string => typeof item === "string")
    : [];
}
