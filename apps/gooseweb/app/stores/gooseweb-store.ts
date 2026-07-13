import { useSyncExternalStore } from "react";
import type {
  ConnectionState,
  GoosewebStorePatch,
  GoosewebSnapshot,
  EntityOperation,
  StaleSourceOperation,
  NormalizedEntities,
  SessionDetailState,
  TeamWorkspaceState,
  PendingCommandState,
  SubscriptionState
} from "../realtime/types";
import { emptyCursorState } from "../realtime/cursors";
import {
  emptySessionAuthorityProjection,
  reduceSessionContributor,
  renderSessionContributors,
  seedSessionSummaryPatch,
  type SessionAuthorityProjection
} from "./session-authority-projection";

const emptyEntities = {
  fleetRows: {},
  sessions: {},
  sessionDetails: {},
  teams: {},
  teamWorkspaces: {},
  approvals: {},
  processes: {},
  worktrees: {},
  sources: {}
} as const;

const initialSnapshot: GoosewebSnapshot = {
  connection: "idle",
  heartbeatIntervalMs: 15_000,
  cursor: emptyCursorState,
  entities: emptyEntities,
  subscriptions: {},
  pendingCommands: {},
  staleSources: {},
  invalidatedSourceDomains: {},
  loadedCoverage: {}
};

type Listener = () => void;

let snapshot = initialSnapshot;
let sessionAuthorityProjection = emptySessionAuthorityProjection;
const listeners = new Set<Listener>();

export function getGoosewebSnapshot(): GoosewebSnapshot {
  return snapshot;
}

export function getVisibleGoosewebSnapshot(): GoosewebSnapshot {
  return visibleSnapshot(snapshot);
}

export function resetGoosewebStoreForTests(): void {
  snapshot = initialSnapshot;
  sessionAuthorityProjection = emptySessionAuthorityProjection;
}

export function subscribeGoosewebStore(listener: Listener): () => void {
  listeners.add(listener);
  return () => listeners.delete(listener);
}

export function updateGoosewebStore(patch: GoosewebStorePatch): void {
  const mergedEntities = patch.entities
    ? mergeEntities(snapshot.entities, patch.entities)
    : snapshot.entities;
  const seededProjection = seedSessionSummaryPatch(
    sessionAuthorityProjection,
    patch.entities?.sessions
  );
  const projectedEntities = seededProjection === sessionAuthorityProjection
    ? mergedEntities
    : { ...mergedEntities, sessions: renderSessionContributors(seededProjection) };
  const reduced = applyEntityOperations(
    projectedEntities,
    patch.entityOperations ?? [],
    seededProjection
  );
  sessionAuthorityProjection = reduced.projection;
  snapshot = {
    ...snapshot,
    ...patch,
    entities: reduced.entities,
    subscriptions: patch.subscriptions
      ? { ...snapshot.subscriptions, ...patch.subscriptions }
      : snapshot.subscriptions,
    pendingCommands: patch.pendingCommands
      ? { ...snapshot.pendingCommands, ...patch.pendingCommands }
      : snapshot.pendingCommands,
    staleSources: applyStaleSourceOperations(
      patch.staleSources
        ? { ...snapshot.staleSources, ...patch.staleSources }
        : snapshot.staleSources,
      patch.staleSourceOperations ?? []
    ),
    invalidatedSourceDomains: patch.invalidatedSourceDomains
      ? { ...snapshot.invalidatedSourceDomains, ...patch.invalidatedSourceDomains }
      : snapshot.invalidatedSourceDomains,
    loadedCoverage: patch.loadedCoverage ?? snapshot.loadedCoverage
  };

  for (const listener of listeners) {
    listener();
  }
}

function applyStaleSourceOperations(
  current: Readonly<Record<string, string>>,
  operations: readonly StaleSourceOperation[]
): Readonly<Record<string, string>> {
  let next = current;
  for (const operation of operations) {
    if (operation.operation === "replace") {
      next = { ...operation.reasons };
      continue;
    }
    if (operation.operation === "add") {
      next = { ...next, ...operation.reasons };
      continue;
    }
    const reduced = { ...next };
    for (const sourceId of operation.sourceIds) delete reduced[sourceId];
    next = reduced;
  }
  return next;
}

function applyEntityOperations(
  current: NormalizedEntities,
  operations: readonly EntityOperation[],
  currentProjection: SessionAuthorityProjection
): { readonly entities: NormalizedEntities; readonly projection: SessionAuthorityProjection } {
  let projection = currentProjection;
  let next = current;
  for (const operation of operations) {
    const previous = next;
    if (operation.domain === "sessions") {
      projection = reduceSessionContributor(projection, previous, next, operation);
      next = { ...next, sessions: renderSessionContributors(projection) };
      continue;
    }
    const existing = next[operation.domain] as Readonly<Record<string, unknown>>;
    const incoming = operation.payload;
    if (operation.operation === "replace" && operation.sourceId) {
      const retained = Object.fromEntries(
        Object.entries(existing).filter(([, entity]) =>
          (entity as { sourceId?: string } | undefined)?.sourceId !== operation.sourceId
        )
      );
      next = {
        ...next,
        [operation.domain]: { ...retained, ...incoming }
      } as NormalizedEntities;
    } else if (operation.operation === "upsert") {
      next = {
        ...next,
        [operation.domain]: { ...existing, ...incoming }
      } as NormalizedEntities;
    } else {
      const domain = operation.entityIds.length === 0
        ? operation.operation === "replace" ? { ...incoming } : {}
        : { ...existing };
      for (const entityId of operation.entityIds) {
        if (operation.sourceId &&
          (domain[entityId] as { sourceId?: string } | undefined)?.sourceId !== operation.sourceId) {
          continue;
        }
        delete domain[entityId];
        if (operation.operation === "replace" && entityId in incoming) {
          domain[entityId] = incoming[entityId];
        }
      }
      next = { ...next, [operation.domain]: domain } as NormalizedEntities;
    }
    projection = reduceSessionContributor(projection, previous, next, operation);
    next = { ...next, sessions: renderSessionContributors(projection) };
  }
  return { entities: next, projection };
}

function mergeEntities(
  current: NormalizedEntities,
  patch: GoosewebStorePatch["entities"]
): NormalizedEntities {
  return {
    ...current,
    ...Object.fromEntries(Object.entries(patch ?? {}).map(([domain, records]) => [
      domain,
      { ...(current[domain as keyof NormalizedEntities] as object), ...(records as object) }
    ])),
    sessionDetails: patch?.sessionDetails
      ? mergeSessionDetails(current.sessionDetails, patch.sessionDetails)
      : current.sessionDetails,
    teamWorkspaces: patch?.teamWorkspaces
      ? mergeTeamWorkspaces(current.teamWorkspaces, patch.teamWorkspaces)
      : current.teamWorkspaces
  };
}

function mergeSessionDetails(
  current: Readonly<Record<string, SessionDetailState>>,
  patch: Readonly<Record<string, SessionDetailState>>
) {
  const next = { ...current };
  for (const [sessionId, detail] of Object.entries(patch)) {
    const existing = next[sessionId];
    next[sessionId] = existing
      ? {
          ...existing,
          ...detail,
          transcript: mergeById(existing.transcript, detail.transcript),
          appendedText: detail.appendedText || existing.appendedText
        }
      : detail;
  }
  return next;
}

function mergeTeamWorkspaces(
  current: Readonly<Record<string, TeamWorkspaceState>>,
  patch: Readonly<Record<string, TeamWorkspaceState>>
) {
  const next = { ...current };
  for (const [teamId, workspace] of Object.entries(patch)) {
    const existing = next[teamId];
    next[teamId] = existing
      ? {
          ...existing,
          ...workspace,
          messages: mergeById(existing.messages, workspace.messages),
          deliveries: mergeById(existing.deliveries, workspace.deliveries)
        }
      : workspace;
  }
  return next;
}

function mergeById<T extends { readonly id: string }>(
  current: readonly T[],
  patch: readonly T[]
): readonly T[] {
  const byId = new Map(current.map((item) => [item.id, item]));
  for (const item of patch) {
    byId.set(item.id, { ...byId.get(item.id), ...item });
  }
  return [...byId.values()];
}

export function setConnectionState(connection: ConnectionState): void {
  updateGoosewebStore({ connection });
}

export function setPendingCommand(command: PendingCommandState): void {
  updateGoosewebStore({
    pendingCommands: {
      [command.commandId]: command
    }
  });
}

export function setSubscription(subscription: SubscriptionState): void {
  updateGoosewebStore({
    subscriptions: {
      [subscription.subscriptionId]: subscription
    }
  });
}

export function useGoosewebSelector<T>(selector: (state: GoosewebSnapshot) => T): T {
  return useSyncExternalStore(
    subscribeGoosewebStore,
    () => selector(getGoosewebSnapshot()),
    () => selector(initialSnapshot)
  );
}

export function useConnectionState(): ConnectionState {
  return useGoosewebSelector((state) => state.connection);
}

export function useGoosewebState() {
  return useGoosewebSelector(visibleSnapshot);
}

export function useFleetRows() {
  return useGoosewebSelector((state) => Object.values(visibleSnapshot(state).entities.fleetRows));
}

export function useSessions() {
  return useGoosewebSelector((state) => Object.values(visibleSnapshot(state).entities.sessions));
}

export function useTeams() {
  return useGoosewebSelector((state) => Object.values(visibleSnapshot(state).entities.teams));
}

export function useApprovals() {
  return useGoosewebSelector((state) => Object.values(visibleSnapshot(state).entities.approvals));
}

export function useProcesses() {
  return useGoosewebSelector((state) => Object.values(visibleSnapshot(state).entities.processes));
}

export function useWorktrees() {
  return useGoosewebSelector((state) => Object.values(visibleSnapshot(state).entities.worktrees));
}

export function useSources() {
  return useGoosewebSelector((state) => Object.values(visibleSnapshot(state).entities.sources));
}

let visibleSnapshotInput: GoosewebSnapshot | undefined;
let visibleSnapshotOutput: GoosewebSnapshot | undefined;

function visibleSnapshot(state: GoosewebSnapshot): GoosewebSnapshot {
  if (state === visibleSnapshotInput && visibleSnapshotOutput) return visibleSnapshotOutput;
  const entities = Object.fromEntries(
    Object.entries(state.entities).map(([domain, records]) => [
      domain,
      Object.fromEntries(Object.entries(records).filter(([entityId, entity]) => {
        const sourceId = (entity as { sourceId?: string }).sourceId;
        if (!sourceId || !state.invalidatedSourceDomains[sourceId]?.includes(domain as keyof NormalizedEntities)) {
          return true;
        }
        return Object.values(state.loadedCoverage).some((coverage) =>
          coverage.sourceId === sourceId && coverage.domain === domain &&
          coverage.entityIds.includes(entityId)
        );
      }))
    ])
  ) as NormalizedEntities;
  visibleSnapshotInput = state;
  visibleSnapshotOutput = { ...state, entities };
  return visibleSnapshotOutput;
}

export function usePendingCommands() {
  return useGoosewebSelector((state) => Object.values(state.pendingCommands));
}

export function useVisibleSubscriptions() {
  return useGoosewebSelector((state) => Object.values(state.subscriptions));
}
