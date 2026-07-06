import { create, fromBinary } from "@bufbuild/protobuf";
import { SourceCursorSchema } from "../../../src/gen/goosetower/v1/common_pb";
import {
  ApprovalViewSchema,
  FleetRowViewSchema,
  ProcessViewSchema,
  SessionViewSchema,
  SourceHealthViewSchema,
  TeamViewSchema,
  WorktreeViewSchema,
  type Patch,
  type Snapshot
} from "../../../src/gen/goosetower/v1/view_pb";
import type { NormalizedEntityPatch } from "../types";

export type EntityPatch = {
  readonly entities: NormalizedEntityPatch;
};

export function decodeSnapshot(snapshot: Snapshot): EntityPatch {
  return decodeViewBody(snapshot.viewKind, snapshot.body);
}

export function decodePatch(patch: Patch): EntityPatch {
  return decodeViewBody(patch.viewKind, patch.body);
}

function decodeViewBody(viewKind: string, body: Uint8Array): EntityPatch {
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
    case "session": {
      const session = fromBinary(SessionViewSchema, body);
      return { entities: { sessions: { [session.sessionId]: session } } };
    }
    case "team": {
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
      return { entities: {} };
  }
}

function decodeJsonViewBody(
  viewKind: string,
  body: Uint8Array
): EntityPatch | undefined {
  let value: unknown;
  try {
    value = JSON.parse(new TextDecoder().decode(body));
  } catch {
    return undefined;
  }

  if (!value || typeof value !== "object") {
    return { entities: {} };
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
    default:
      return { entities: {} };
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

function arrayFrom(value: unknown): unknown[] {
  return Array.isArray(value) ? value : [];
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
