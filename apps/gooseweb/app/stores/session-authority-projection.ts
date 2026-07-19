import { create } from "@bufbuild/protobuf";
import type { SessionView } from "../../src/gen/goosetower/v1/view_pb";
import { SessionViewSchema } from "../../src/gen/goosetower/v1/view_pb";
import { sourceEntityKey } from "../realtime/protocol/entities";
import type {
  EntityOperation,
  NormalizedEntities,
  SessionDetailState
} from "../realtime/types";

type Contributor = "summary" | "detail";

export type SessionAuthorityProjection = {
  readonly summaries: Readonly<Record<string, SessionView>>;
  readonly detailStates: Readonly<Record<string, SessionDetailState>>;
  readonly details: Readonly<Record<string, SessionView>>;
  readonly winners: Readonly<Record<string, Contributor>>;
};

export const emptySessionAuthorityProjection: SessionAuthorityProjection = {
  summaries: {},
  detailStates: {},
  details: {},
  winners: {}
};

export function seedSessionSummaryPatch(
  current: SessionAuthorityProjection,
  sessions: Readonly<Record<string, SessionView>> | undefined
): SessionAuthorityProjection {
  if (!sessions) return current;
  return {
    ...current,
    summaries: { ...current.summaries, ...sessions },
    winners: {
      ...current.winners,
      ...Object.fromEntries(Object.keys(sessions).map((id) => [id, "summary" as const]))
    }
  };
}

export function reduceSessionContributor(
  current: SessionAuthorityProjection,
  previousEntities: NormalizedEntities,
  nextEntities: NormalizedEntities,
  operation: EntityOperation
): SessionAuthorityProjection {
  if (!operation.authoritative) return current;
  if (operation.domain === "sessions") {
    return reduceSummaryOperation(current, nextEntities, operation);
  }
  if (operation.domain === "sessionDetails") {
    return reduceDetailOperation(current, nextEntities, operation);
  }
  if (operation.domain === "worktrees") {
    return refreshWorktreeContributions(current, previousEntities, nextEntities);
  }
  return current;
}

export function renderSessionContributors(
  current: SessionAuthorityProjection
): Readonly<Record<string, SessionView>> {
  const rendered: Record<string, SessionView> = {};
  const ids = new Set([...Object.keys(current.summaries), ...Object.keys(current.details)]);
  for (const id of ids) {
    const winner = current.winners[id];
    const session = winner === "detail"
      ? current.details[id] ?? current.summaries[id]
      : current.summaries[id] ?? current.details[id];
    if (session) rendered[id] = session;
  }
  return rendered;
}

function reduceSummaryOperation(
  current: SessionAuthorityProjection,
  entities: NormalizedEntities,
  operation: EntityOperation
): SessionAuthorityProjection {
  const summaries = reduceSessionRecord(current.summaries, operation);
  const touched = touchedEntityIds(current.summaries, summaries, operation);
  const details = { ...current.details };
  const winners = { ...current.winners };
  for (const id of touched) {
    const detail = current.detailStates[id];
    if (detail) details[id] = projectDetail(detail, summaries[id], entities);
    if (details[id]) winners[id] = "detail";
    else if (summaries[id]) winners[id] = "summary";
    else delete winners[id];
  }
  return { ...current, summaries, details, winners };
}

function reduceDetailOperation(
  current: SessionAuthorityProjection,
  entities: NormalizedEntities,
  operation: EntityOperation
): SessionAuthorityProjection {
  const detailStates = reduceSessionRecord(current.detailStates, operation);
  const details = { ...current.details };
  const winners = { ...current.winners };
  const touched = touchedEntityIds(current.detailStates, detailStates, operation);
  for (const id of touched) {
    const detail = detailStates[id];
    if (detail) {
      details[id] = projectDetail(detail, current.summaries[id], entities);
      winners[id] = "detail";
      continue;
    }
    delete details[id];
    if (current.summaries[id]) winners[id] = "summary";
    else delete winners[id];
  }
  return { ...current, detailStates, details, winners };
}

function refreshWorktreeContributions(
  current: SessionAuthorityProjection,
  previous: NormalizedEntities,
  next: NormalizedEntities
): SessionAuthorityProjection {
  const worktreeIds = new Set([
    ...Object.keys(previous.worktrees),
    ...Object.keys(next.worktrees)
  ].filter((id) => previous.worktrees[id] !== next.worktrees[id]));
  if (worktreeIds.size === 0) return current;
  const details = { ...current.details };
  for (const [id, detail] of Object.entries(current.detailStates)) {
    if (detail.worktreeId &&
      worktreeIds.has(sourceEntityKey(detail.sourceId, detail.worktreeId))) {
      details[id] = projectDetail(detail, current.summaries[id], next);
    }
  }
  return { ...current, details };
}

function projectDetail(
  detail: SessionDetailState,
  summary: SessionView | undefined,
  entities: NormalizedEntities
): SessionView {
  const worktree = detail.worktreeId
    ? entities.worktrees[sourceEntityKey(detail.sourceId, detail.worktreeId)]
    : undefined;
  return create(SessionViewSchema, {
    sourceId: detail.sourceId,
    sessionId: detail.sessionId,
    provider: detail.provider,
    model: detail.model,
    status: detail.status,
    cwd: detail.cwd,
    worktreePath: detail.worktreePath || worktree?.path || "",
    activeTurnId: detail.activeTurnId,
    ...(summary?.contextRemainingPercent === undefined ? {} : {
      contextRemainingPercent: summary.contextRemainingPercent
    }),
    ...(summary?.contextWindowTokens === undefined ? {} : {
      contextWindowTokens: summary.contextWindowTokens
    }),
    ...(summary?.contextUsedTokens === undefined ? {} : {
      contextUsedTokens: summary.contextUsedTokens
    })
  });
}

function reduceSessionRecord<T extends { readonly sourceId: string }>(
  current: Readonly<Record<string, T>>,
  operation: EntityOperation
): Readonly<Record<string, T>> {
  const incoming = operation.payload as Readonly<Record<string, T>>;
  if (operation.operation === "replace" && operation.sourceId) {
    return {
      ...Object.fromEntries(Object.entries(current).filter(([, session]) =>
        session.sourceId !== operation.sourceId
      )),
      ...incoming
    };
  }
  if (operation.operation === "upsert") return { ...current, ...incoming };
  const next = operation.entityIds.length === 0
    ? operation.operation === "replace" ? { ...incoming } : {}
    : { ...current };
  for (const id of operation.entityIds) {
    if (operation.sourceId && next[id]?.sourceId !== operation.sourceId) continue;
    delete next[id];
    if (operation.operation === "replace" && incoming[id]) next[id] = incoming[id];
  }
  return next;
}

function touchedEntityIds(
  previous: Readonly<Record<string, { readonly sourceId: string }>>,
  next: Readonly<Record<string, { readonly sourceId: string }>>,
  operation: EntityOperation
): ReadonlySet<string> {
  const ids = operation.operation === "upsert"
    ? Object.keys(operation.payload)
    : operation.entityIds.length > 0
      ? [...operation.entityIds, ...Object.keys(operation.payload)]
      : [...Object.keys(previous), ...Object.keys(next)];
  return new Set(operation.sourceId
    ? ids.filter((id) => (next[id] ?? previous[id])?.sourceId === operation.sourceId)
    : ids);
}
