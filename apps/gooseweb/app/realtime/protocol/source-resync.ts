import type { SourceSnapshotResync } from "../../../src/gen/goosetower/v1/realtime_pb";
import type { EntityDomain } from "../types";
import type { EntityPatch } from "./entities";
import { ProtocolDecodeError } from "./protocol-error";

const SOURCE_REPLACEMENT_COVERAGE = [
  "fleet_rows", "sessions", "session_details", "teams", "team_workspaces",
  "approvals", "processes", "worktrees", "sources"
] as const;

const SOURCE_REPLACEMENT_DOMAINS = [
  "fleetRows", "sessions", "sessionDetails", "teams", "teamWorkspaces",
  "approvals", "processes", "worktrees", "sources"
] as const satisfies readonly EntityDomain[];

/**
 * Decode the bounded ownership-reset commit. The commit deliberately carries
 * no source entities: it invalidates every source-owned domain atomically,
 * after which the connection's bounded active subscriptions repopulate the
 * summaries and selected detail windows they own.
 */
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
  const keys = Object.keys(body).sort();
  if (keys.join(",") !== "source_epoch,source_id,source_seq") {
    throw new ProtocolDecodeError("source resync body must contain only source authority");
  }
  if (requireString(body.source_id, "source_resync.source_id") !== sourceId) {
    throw new ProtocolDecodeError("source resync body disagrees with source identity");
  }
  if (
    requireString(body.source_epoch, "source_resync.source_epoch") !==
    sourceAuthority.sourceEpoch
  ) {
    throw new ProtocolDecodeError("source resync body epoch disagrees with cursor");
  }
  const sourceSeq = requireNonnegativeInteger(body.source_seq, "source_resync.source_seq");
  if (BigInt(sourceSeq) !== sourceAuthority.sourceSeq) {
    throw new ProtocolDecodeError("source resync body sequence disagrees with cursor");
  }

  return {
    entityOperations: SOURCE_REPLACEMENT_DOMAINS.map((domain) => ({
      operation: "replace",
      domain,
      entityIds: [],
      authoritative: true,
      sourceId,
      payload: {}
    }))
  };
}

function requireCoverage(resync: SourceSnapshotResync): void {
  const coverage = resync.coverage;
  if (
    !coverage?.authoritative || coverage.entityIds.length !== 0 ||
    coverage.domains.length !== SOURCE_REPLACEMENT_COVERAGE.length ||
    SOURCE_REPLACEMENT_COVERAGE.some((domain, index) => coverage.domains[index] !== domain)
  ) {
    throw new ProtocolDecodeError("source resync lacks exact authoritative source coverage");
  }
}

function parseBody(bytes: Uint8Array): Record<string, unknown> {
  try {
    const value: unknown = JSON.parse(new TextDecoder().decode(bytes));
    if (!value || typeof value !== "object" || Array.isArray(value)) {
      throw new ProtocolDecodeError("source resync body must be an object");
    }
    return value as Record<string, unknown>;
  } catch (error) {
    if (error instanceof ProtocolDecodeError) throw error;
    throw new ProtocolDecodeError("malformed source resync JSON body");
  }
}

function requireString(value: unknown, field: string): string {
  if (typeof value !== "string" || value.length === 0) {
    throw new ProtocolDecodeError(`${field} must be a nonempty string`);
  }
  return value;
}

function requireNonnegativeInteger(value: unknown, field: string): number {
  if (typeof value !== "number" || !Number.isSafeInteger(value) || value < 0) {
    throw new ProtocolDecodeError(`${field} must be a nonnegative safe integer`);
  }
  return value;
}
