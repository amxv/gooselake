import { useSyncExternalStore } from "react";
import type {
  ConnectionState,
  GoosewebStorePatch,
  GoosewebSnapshot,
  EntityMutation,
  NormalizedEntities,
  SessionDetailState,
  TeamWorkspaceState,
  PendingCommandState,
  SubscriptionState
} from "../realtime/types";
import { emptyCursorState } from "../realtime/cursors";

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
  staleSources: {}
};

type Listener = () => void;

let snapshot = initialSnapshot;
const listeners = new Set<Listener>();

export function getGoosewebSnapshot(): GoosewebSnapshot {
  return snapshot;
}

export function resetGoosewebStoreForTests(): void {
  snapshot = initialSnapshot;
}

export function subscribeGoosewebStore(listener: Listener): () => void {
  listeners.add(listener);
  return () => listeners.delete(listener);
}

export function updateGoosewebStore(patch: GoosewebStorePatch): void {
  snapshot = {
    ...snapshot,
    ...patch,
    entities: applyEntityMutations(
      snapshot.entities,
      patch.entities,
      patch.entityMutations ?? []
    ),
    subscriptions: patch.subscriptions
      ? { ...snapshot.subscriptions, ...patch.subscriptions }
      : snapshot.subscriptions,
    pendingCommands: patch.pendingCommands
      ? { ...snapshot.pendingCommands, ...patch.pendingCommands }
      : snapshot.pendingCommands,
    staleSources: patch.staleSources
      ? { ...snapshot.staleSources, ...patch.staleSources }
      : snapshot.staleSources
  };

  for (const listener of listeners) {
    listener();
  }
}

function applyEntityMutations(
  current: NormalizedEntities,
  patch: GoosewebStorePatch["entities"],
  mutations: readonly EntityMutation[]
): NormalizedEntities {
  let next = patch ? mergeEntities(current, patch) : current;
  for (const mutation of mutations) {
    if (mutation.operation === "upsert") {
      continue;
    }
    const existing = next[mutation.domain] as Readonly<Record<string, unknown>>;
    const incoming = (patch?.[mutation.domain] ?? {}) as Readonly<Record<string, unknown>>;
    const domain = mutation.entityIds.length === 0
      ? mutation.operation === "replace" ? { ...incoming } : {}
      : { ...existing };
    for (const entityId of mutation.entityIds) {
      delete domain[entityId];
      if (mutation.operation === "replace" && entityId in incoming) {
        domain[entityId] = incoming[entityId];
      }
    }
    next = { ...next, [mutation.domain]: domain } as NormalizedEntities;
  }
  return next;
}

function mergeEntities(
  current: NormalizedEntities,
  patch: GoosewebStorePatch["entities"]
): NormalizedEntities {
  return {
    ...current,
    ...patch,
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
  return useGoosewebSelector((state) => state);
}

export function useFleetRows() {
  return useGoosewebSelector((state) => Object.values(state.entities.fleetRows));
}

export function useSessions() {
  return useGoosewebSelector((state) => Object.values(state.entities.sessions));
}

export function useTeams() {
  return useGoosewebSelector((state) => Object.values(state.entities.teams));
}

export function useApprovals() {
  return useGoosewebSelector((state) => Object.values(state.entities.approvals));
}

export function useProcesses() {
  return useGoosewebSelector((state) => Object.values(state.entities.processes));
}

export function useWorktrees() {
  return useGoosewebSelector((state) => Object.values(state.entities.worktrees));
}

export function useSources() {
  return useGoosewebSelector((state) => Object.values(state.entities.sources));
}

export function usePendingCommands() {
  return useGoosewebSelector((state) => Object.values(state.pendingCommands));
}

export function useVisibleSubscriptions() {
  return useGoosewebSelector((state) => Object.values(state.subscriptions));
}
