import { useSyncExternalStore } from "react";
import type {
  ConnectionState,
  GoosewebStorePatch,
  GoosewebSnapshot,
  PendingCommandState,
  SubscriptionState
} from "../realtime/types";
import { emptyCursorState } from "../realtime/cursors";

const emptyEntities = {
  fleetRows: {},
  sessions: {},
  teams: {},
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

export function subscribeGoosewebStore(listener: Listener): () => void {
  listeners.add(listener);
  return () => listeners.delete(listener);
}

export function updateGoosewebStore(patch: GoosewebStorePatch): void {
  snapshot = {
    ...snapshot,
    ...patch,
    entities: patch.entities
      ? { ...snapshot.entities, ...patch.entities }
      : snapshot.entities,
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

export function useFleetRows() {
  return useGoosewebSelector((state) => Object.values(state.entities.fleetRows));
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
