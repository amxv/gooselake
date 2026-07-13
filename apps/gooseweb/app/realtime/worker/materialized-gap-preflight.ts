import { MessageKind } from "../../../src/gen/goosetower/v1/common_pb";
import type { RealtimeEnvelope } from "../../../src/gen/goosetower/v1/realtime_pb";
import {
  hasCursorEpochMismatch,
  isValidCursorVector,
  shouldApplyCursorVector
} from "../cursors";
import type { EntityPatch } from "../protocol/entities";
import type { CursorState, SourceCursorState } from "../types";
import { sourceHealthGapCursors } from "./frame-source-authority";
import { cursorAuthorityFromEnvelope } from "./view-authority";

export type MaterializedGapDisposition =
  | { readonly kind: "none" | "delegate" | "drop" }
  | { readonly kind: "arm"; readonly sources: readonly SourceCursorState[] };

export function preflightMaterializedGap(input: {
  readonly envelope: RealtimeEnvelope;
  readonly patch: EntityPatch;
  readonly cursor: CursorState;
  readonly gatewayEpoch?: string;
  readonly gatewayStartedAtUnixNs?: bigint;
  readonly appliedMessageIds: ReadonlySet<string>;
  readonly frozenSources: ReadonlySet<string>;
}): MaterializedGapDisposition {
  const isSnapshot = input.envelope.messageKind === MessageKind.SNAPSHOT;
  const authority = cursorAuthorityFromEnvelope(input.envelope);
  const gaps = sourceHealthGapCursors(input.patch, authority?.sources ?? []);
  if (gaps.length === 0) return { kind: "none" };
  if (input.envelope.messageId && input.appliedMessageIds.has(input.envelope.messageId)) {
    return { kind: "drop" };
  }
  if (!authority || !isValidCursorVector(authority.sources) ||
    !authority.gatewayEpoch || authority.gatewayStartedAtUnixNs === 0n ||
    (!isSnapshot && authority.gatewaySeq === 0n) || authority.sources.length === 0) {
    return { kind: "delegate" };
  }
  if (authority.gatewayEpoch !== input.gatewayEpoch ||
    authority.gatewayStartedAtUnixNs !== input.gatewayStartedAtUnixNs ||
    (input.cursor.gatewayEpoch && (input.cursor.gatewayEpoch !== authority.gatewayEpoch ||
      input.cursor.gatewayStartedAtUnixNs !== authority.gatewayStartedAtUnixNs)) ||
    (!input.cursor.gatewayEpoch && input.cursor.gatewaySeq > 0n)) {
    return { kind: "delegate" };
  }
  if (hasCursorEpochMismatch(input.cursor, authority.sources)) return { kind: "delegate" };
  if (!isSnapshot && authority.sources.some((source) => {
    const known = input.cursor.sourceCursors[source.sourceId];
    return known?.sourceEpoch === source.sourceEpoch && source.sourceSeq > known.sourceSeq + 1n;
  })) return { kind: "delegate" };
  if (gaps.some((source) => input.frozenSources.has(source.sourceId))) return { kind: "drop" };
  if (!shouldApplyCursorVector(input.cursor, authority.gatewaySeq, authority.sources, {
    allowEqualSourceSeq: true,
    allowEpochChange: false,
    allowGatewayRegression: isSnapshot
  })) return { kind: "drop" };
  return { kind: "arm", sources: gaps };
}
