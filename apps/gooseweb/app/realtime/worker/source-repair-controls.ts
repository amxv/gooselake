import type { SourceCursorState } from "../types";
import type { CursorState } from "../types";
import type { RealtimeEnvelope } from "../../../src/gen/goosetower/v1/realtime_pb";

type CursorLike = {
  readonly sourceId: string;
  readonly sourceEpoch: string;
  readonly sourceSeq: bigint;
};

export function validateGapDetectedAuthority(
  lastSeen: CursorLike | undefined,
  nextAvailable: CursorLike | undefined,
  known: SourceCursorState | undefined
): SourceCursorState {
  if (!lastSeen || !nextAvailable || !lastSeen.sourceId || !lastSeen.sourceEpoch ||
    !nextAvailable.sourceId || !nextAvailable.sourceEpoch || lastSeen.sourceSeq <= 0n ||
    nextAvailable.sourceSeq <= lastSeen.sourceSeq) {
    throw new Error("gap-detected control lacks a positive monotonic cursor pair");
  }
  if (lastSeen.sourceId !== nextAvailable.sourceId ||
    lastSeen.sourceEpoch !== nextAvailable.sourceEpoch) {
    throw new Error("gap-detected cursor pair disagrees on source authority");
  }
  if (known && (known.sourceEpoch !== lastSeen.sourceEpoch ||
    known.sourceSeq !== lastSeen.sourceSeq)) {
    throw new Error("gap-detected last-seen cursor disagrees with known authority");
  }
  return {
    sourceId: nextAvailable.sourceId,
    sourceEpoch: nextAvailable.sourceEpoch,
    sourceSeq: nextAvailable.sourceSeq
  };
}

export function validateGapFilledAuthority(
  filled: CursorLike | undefined,
  known: SourceCursorState | undefined
): SourceCursorState {
  if (!filled || !filled.sourceId || !filled.sourceEpoch || filled.sourceSeq <= 0n) {
    throw new Error("gap-filled control lacks a positive source cursor");
  }
  if (known && (known.sourceEpoch !== filled.sourceEpoch ||
    filled.sourceSeq < known.sourceSeq)) {
    throw new Error("gap-filled cursor disagrees with known source authority");
  }
  return {
    sourceId: filled.sourceId,
    sourceEpoch: filled.sourceEpoch,
    sourceSeq: filled.sourceSeq
  };
}

export function gapDetectedAuthorityFromEnvelope(
  envelope: RealtimeEnvelope,
  cursor: CursorState
): SourceCursorState {
  if (envelope.payload.case !== "sourceGapDetected") {
    throw new Error("gap-detected envelope is missing its payload");
  }
  const lastSeen = envelope.payload.value.lastSeen;
  const next = envelope.payload.value.nextAvailable;
  const sourceId = lastSeen?.sourceId || next?.sourceId;
  return validateGapDetectedAuthority(
    lastSeen,
    next,
    sourceId ? cursor.sourceCursors[sourceId] : undefined
  );
}

export function gapFilledAuthorityFromEnvelope(
  envelope: RealtimeEnvelope,
  cursor: CursorState
): SourceCursorState {
  if (envelope.payload.case !== "sourceGapFilled") {
    throw new Error("gap-filled envelope is missing its payload");
  }
  const filled = envelope.payload.value.cursor;
  return validateGapFilledAuthority(
    filled,
    filled?.sourceId ? cursor.sourceCursors[filled.sourceId] : undefined
  );
}
