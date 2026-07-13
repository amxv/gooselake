import type { GoosewebStorePatch, SourceCursorState } from "../types";

type RepairCompletion = "gap_fill" | "source_resync";

type SourceRepair = {
  readonly completion: RepairCompletion;
  readonly expectedEpoch?: string;
  readonly minimumSourceSeq?: bigint;
  readonly requirements: Readonly<Record<string, string>>;
  readonly gapFilled: boolean;
};

export class SourceRepairTracker {
  private repairs: Record<string, SourceRepair> = {};

  begin(
    sourceId: string,
    completion: RepairCompletion,
    requirements: Readonly<Record<string, string>>,
    expected?: SourceCursorState
  ): void {
    this.repairs = {
      ...this.repairs,
      [sourceId]: {
        completion,
        expectedEpoch: expected?.sourceEpoch,
        minimumSourceSeq: expected?.sourceSeq,
        requirements: { ...requirements },
        gapFilled: false
      }
    };
  }

  retireSnapshot(
    subscriptionId: string,
    requestId: string,
    sourceIds: readonly string[]
  ): void {
    for (const sourceId of sourceIds) {
      const repair = this.repairs[sourceId];
      if (!repair || repair.requirements[subscriptionId] !== requestId) continue;
      const requirements = { ...repair.requirements };
      delete requirements[subscriptionId];
      this.repairs = {
        ...this.repairs,
        [sourceId]: { ...repair, requirements }
      };
    }
  }

  renewSubscription(subscriptionId: string, requestId: string): void {
    for (const [sourceId, repair] of Object.entries(this.repairs)) {
      if (!(subscriptionId in repair.requirements)) continue;
      this.repairs = {
        ...this.repairs,
        [sourceId]: {
          ...repair,
          requirements: { ...repair.requirements, [subscriptionId]: requestId }
        }
      };
    }
  }

  retireSubscription(subscriptionId: string): boolean {
    let retired = false;
    for (const [sourceId, repair] of Object.entries(this.repairs)) {
      if (!(subscriptionId in repair.requirements)) continue;
      retired = true;
      const requirements = { ...repair.requirements };
      delete requirements[subscriptionId];
      this.repairs = {
        ...this.repairs,
        [sourceId]: { ...repair, requirements }
      };
    }
    return retired;
  }

  markGapFilled(cursor: SourceCursorState): boolean {
    const repair = this.repairs[cursor.sourceId];
    if (!repair || repair.completion !== "gap_fill" ||
      (repair.expectedEpoch && cursor.sourceEpoch !== repair.expectedEpoch) ||
      (repair.minimumSourceSeq !== undefined && cursor.sourceSeq < repair.minimumSourceSeq)) {
      return false;
    }
    this.repairs = {
      ...this.repairs,
      [cursor.sourceId]: { ...repair, gapFilled: true }
    };
    return true;
  }

  takeRecovered(authoritativeResetSourceId?: string): string[] {
    const recovered = Object.entries(this.repairs)
      .filter(([sourceId, repair]) => sourceId === authoritativeResetSourceId ||
        (repair.completion === "gap_fill" && repair.gapFilled &&
          Object.keys(repair.requirements).length === 0))
      .map(([sourceId]) => sourceId);
    if (authoritativeResetSourceId && !recovered.includes(authoritativeResetSourceId)) {
      recovered.push(authoritativeResetSourceId);
    }
    if (recovered.length > 0) {
      const recoveredSet = new Set(recovered);
      this.repairs = Object.fromEntries(
        Object.entries(this.repairs).filter(([sourceId]) => !recoveredSet.has(sourceId))
      );
    }
    return recovered;
  }

  get hasPending(): boolean {
    return Object.keys(this.repairs).length > 0;
  }
}

export function sourceRepairRecoveryPatch(
  tracker: SourceRepairTracker,
  frozenSources: Set<string>,
  authoritativeResetSourceId?: string
): GoosewebStorePatch {
  const recoveredSources = tracker.takeRecovered(authoritativeResetSourceId);
  recoveredSources.forEach((sourceId) => frozenSources.delete(sourceId));
  return {
    connection: tracker.hasPending ? "stale" : "connected",
    ...(!tracker.hasPending ? { lastError: undefined } : {}),
    ...(recoveredSources.length > 0 ? {
      staleSourceOperations: [{
        operation: "remove",
        sourceIds: recoveredSources,
        reasons: {}
      }]
    } : {})
  };
}
