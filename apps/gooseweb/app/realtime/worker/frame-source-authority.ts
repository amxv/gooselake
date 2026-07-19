import type { EntityPatch } from "../protocol/entities";
import type { SourceCursorState } from "../types";

export function validateEntitySourceAgreement(
  patch: EntityPatch,
  cursorSourceIds: readonly string[],
  requireSingleSource: boolean
): void {
  if (requireSingleSource && cursorSourceIds.length !== 1) {
    throw new Error("entity-scoped frame requires exactly one cursor source");
  }
  const allowed = new Set(cursorSourceIds);
  for (const operation of patch.entityOperations) {
    for (const entity of Object.values(operation.payload)) {
      const sourceId = (entity as { sourceId?: string }).sourceId;
      if (!sourceId || !allowed.has(sourceId)) {
        throw new Error("frame body source is missing from canonical cursor authority");
      }
    }
  }
}

export function sourceHealthGapCursors(
  patch: EntityPatch,
  cursors: readonly SourceCursorState[]
): SourceCursorState[] {
  const gapSourceIds = new Set<string>();
  for (const operation of patch.entityOperations) {
    if (operation.domain !== "sources" || operation.operation === "remove") continue;
    for (const entity of Object.values(operation.payload)) {
      const source = entity as { sourceId?: string; health?: string; lifecycle?: string };
      if (source.health === "gap_detected" || source.lifecycle === "gap_detected") {
        if (source.sourceId) gapSourceIds.add(source.sourceId);
      }
    }
  }
  return cursors.filter((cursor) => gapSourceIds.has(cursor.sourceId));
}

export function sourceHealthLiveCursors(
  patch: EntityPatch,
  cursors: readonly SourceCursorState[]
): SourceCursorState[] {
  const liveSourceIds = new Set<string>();
  for (const operation of patch.entityOperations) {
    if (operation.domain !== "sources" || operation.operation === "remove") continue;
    for (const entity of Object.values(operation.payload)) {
      const source = entity as { sourceId?: string; health?: string; lifecycle?: string };
      if (source.health === "live" || source.lifecycle === "live") {
        if (source.sourceId) liveSourceIds.add(source.sourceId);
      }
    }
  }
  return cursors.filter((cursor) => liveSourceIds.has(cursor.sourceId));
}
