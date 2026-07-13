import type { GoosewebStorePatch, NormalizedEntityPatch } from "../types";

export function mergeStorePatch(
  current: GoosewebStorePatch,
  next: GoosewebStorePatch
): GoosewebStorePatch {
  return {
    ...current,
    ...next,
    entities: next.entities
      ? mergeEntityPatches(current.entities, next.entities)
      : current.entities,
    pendingCommands: next.pendingCommands
      ? { ...current.pendingCommands, ...next.pendingCommands }
      : current.pendingCommands,
    subscriptions: next.subscriptions
      ? { ...current.subscriptions, ...next.subscriptions }
      : current.subscriptions,
    staleSources: next.staleSources
      ? { ...current.staleSources, ...next.staleSources }
      : current.staleSources,
    invalidatedSourceDomains: next.invalidatedSourceDomains
      ? { ...current.invalidatedSourceDomains, ...next.invalidatedSourceDomains }
      : current.invalidatedSourceDomains,
    loadedCoverage: next.loadedCoverage ?? current.loadedCoverage,
    entityOperations: append(current.entityOperations, next.entityOperations),
    staleSourceOperations: append(
      current.staleSourceOperations,
      next.staleSourceOperations
    )
  };
}

function mergeEntityPatches(
  current: NormalizedEntityPatch | undefined,
  next: NormalizedEntityPatch
): NormalizedEntityPatch {
  const merged: Record<string, unknown> = { ...current };
  for (const [domain, records] of Object.entries(next)) {
    merged[domain] = {
      ...((current?.[domain as keyof NormalizedEntityPatch] ?? {}) as object),
      ...(records as object)
    };
  }
  return merged as NormalizedEntityPatch;
}

function append<T>(
  current: readonly T[] | undefined,
  next: readonly T[] | undefined
): readonly T[] | undefined {
  return next ? [...(current ?? []), ...next] : current;
}
