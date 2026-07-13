import type { RealtimeEnvelope } from "../../../src/gen/goosetower/v1/realtime_pb";
import type { LoadedCoverage, SourceCursorState } from "../types";

export function snapshotCoverageKind(
  viewKind: string,
  entityIds: readonly string[]
): LoadedCoverage["kind"] {
  if (entityIds.length > 0) return "entity";
  if (["board", "fleet_board", "approval_inbox", "teams", "sessions"].includes(viewKind)) {
    return "window";
  }
  return "domain";
}

export function canonicalViewKind(viewKind: string): string {
  if (viewKind === "session") return "session_detail";
  if (viewKind === "team" || viewKind === "team_stream") return "team_workspace";
  if (viewKind === "process") return "process_tail";
  return viewKind;
}

export function isSelectedViewKind(viewKind: string): boolean {
  return viewKind === "session_detail" || viewKind === "team_workspace" ||
    viewKind === "process_tail";
}

export function cursorAuthorityFromEnvelope(
  envelope: RealtimeEnvelope
): {
  gatewayEpoch: string;
  gatewayStartedAtUnixNs: bigint;
  gatewaySeq: bigint;
  sources: SourceCursorState[];
} | undefined {
  if (envelope.payload.case === "snapshot" || envelope.payload.case === "patch") {
    const view = envelope.payload.value;
    if (view.cursor) {
      return {
        gatewayEpoch: view.cursor.gatewayEpoch,
        gatewayStartedAtUnixNs: view.cursor.gatewayStartedAtUnixNs,
        gatewaySeq: view.cursor.gatewaySeq,
        sources: view.cursor.sources.map((source) => ({
          sourceId: source.sourceId,
          sourceEpoch: source.sourceEpoch,
          sourceSeq: source.sourceSeq
        }))
      };
    }
    if (view.schemaVersion > 0) return undefined;
  }
  if (envelope.payload.case === "sourceSnapshotResync") {
    const resync = envelope.payload.value;
    if (!resync.cursor || resync.schemaVersion !== 1) return undefined;
    return {
      gatewayEpoch: resync.cursor.gatewayEpoch,
      gatewayStartedAtUnixNs: resync.cursor.gatewayStartedAtUnixNs,
      gatewaySeq: resync.cursor.gatewaySeq,
      sources: resync.cursor.sources.map((source) => ({
        sourceId: source.sourceId,
        sourceEpoch: source.sourceEpoch,
        sourceSeq: source.sourceSeq
      }))
    };
  }
  return {
    gatewayEpoch: "",
    gatewayStartedAtUnixNs: 0n,
    gatewaySeq: envelope.gatewaySeq,
    sources: envelope.sourceId && envelope.sourceSeq > 0n ? [{
      sourceId: envelope.sourceId,
      sourceEpoch: envelope.sourceEpoch,
      sourceSeq: envelope.sourceSeq
    }] : []
  };
}
