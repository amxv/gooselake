import { fromBinary } from "@bufbuild/protobuf";
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
