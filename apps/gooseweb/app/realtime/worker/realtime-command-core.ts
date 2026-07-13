import { fromBinary, toBinary } from "@bufbuild/protobuf";
import { Lane, MessageKind } from "../../../src/gen/goosetower/v1/common_pb";
import { RealtimeEnvelopeSchema, type RealtimeEnvelope } from "../../../src/gen/goosetower/v1/realtime_pb";
import type { Snapshot } from "../../../src/gen/goosetower/v1/view_pb";
import { realtimeUrlWithTicket } from "../config";
import {
  emptyCursorState,
  hasCursorEpochMismatch,
  isNewGatewayGeneration,
  isValidCursorVector,
  loadCursorState,
  mergeCursorVector,
  persistCursorState,
  shouldApplyCursorVector
} from "../cursors";
import {
  decodePatch,
  decodeNotFoundSnapshot,
  decodeSnapshot,
  decodeSourceSnapshotResync,
  type EntityPatch
} from "../protocol/entities";
import { SOURCE_REPLACEMENT_DOMAINS } from "../protocol/source-resync";
import type {
  CursorState,
  EntityDomain,
  EntityOperation,
  GoosewebStorePatch,
  GoosewebSnapshot,
  LoadedCoverage,
  PendingCommandState,
  SourceCursorState,
  SubscriptionState,
  WorkerInbound,
  WorkerOutbound
} from "../types";
import {
  makeAuthRefresh,
  makeCommand,
  makePing,
  makeResume,
  makeSubscribe,
  makeUnsubscribe
} from "./command-messages";
import { mergeStorePatch } from "./store-patch-batcher";
import { reduceCommandLifecycle } from "./command-rejection";
import { SourceRepairTracker, sourceRepairRecoveryPatch } from "./source-repair-tracker";
import {
  canonicalViewKind,
  cursorAuthorityFromEnvelope,
  isSelectedViewKind,
  snapshotCoverageKind
} from "./view-authority";
const HEARTBEAT_FALLBACK_MS = 15_000;
const PATCH_FLUSH_MS = 16;
export class RealtimeWorkerCore {
  private socket: WebSocket | undefined;
  private cursor: CursorState = emptyCursorState;
  private connectionId: string | undefined;
  private gatewayEpoch: string | undefined;
  private gatewayStartedAtUnixNs: bigint | undefined;
  private heartbeatIntervalMs = HEARTBEAT_FALLBACK_MS;
  private readonly appliedViewMessageIds = new Set<string>();
  private readonly appliedViewMessageOrder: string[] = [];
  private cursorPersistQueue = Promise.resolve();
  private heartbeatTimer: ReturnType<typeof setInterval> | undefined;
  private subscriptions: Record<string, SubscriptionState> = {};
  private pendingCommands: Record<string, PendingCommandState> = {};
  private queuedPatch: GoosewebStorePatch = {};
  private flushTimer: ReturnType<typeof setTimeout> | undefined;
  private startupFramesSent = false;
  private invalidatedSourceDomains: Record<string, readonly EntityDomain[]> = {};
  private loadedCoverage: Record<string, LoadedCoverage> = {};
  private readonly frozenSources = new Set<string>();
  private readonly sourceRepairs = new SourceRepairTracker();
  constructor(
    private readonly post: (message: WorkerOutbound) => void,
    private readonly observeFrame?: (envelope: RealtimeEnvelope) => void
  ) {}
  async handleMessage(message: WorkerInbound): Promise<void> {
    switch (message.type) {
      case "connect":
        await this.connect(message.goosetowerUrl, message.ticket);
        break;
      case "disconnect":
        this.disconnect();
        break;
      case "subscribe":
        this.subscribe(
          message.subscriptionId,
          message.viewKind,
          message.filters ?? {}
        );
        break;
      case "unsubscribe":
        this.unsubscribe(message.subscriptionId);
        break;
      case "command":
        this.sendCommand(message.command, message.idempotencyKey);
        break;
      case "auth-refresh":
        this.sendEnvelope(makeAuthRefresh(message.ticket));
        break;
    }
  }
  private async connect(goosetowerUrl: string, ticket: string): Promise<void> {
    this.disconnect();
    this.startupFramesSent = false;
    this.gatewayEpoch = undefined;
    this.gatewayStartedAtUnixNs = undefined;
    this.cursor = await loadCursorState();
    this.emitState({ connection: "connecting", cursor: this.cursor });

    const socket = new WebSocket(realtimeUrlWithTicket(goosetowerUrl, ticket));
    this.socket = socket;
    socket.binaryType = "arraybuffer";
    socket.onopen = () => {
      if (this.socket !== socket) {
        return;
      }
      this.emitState({ connection: "connecting" });
      this.startHeartbeat();
    };
    socket.onmessage = (event) => {
      if (this.socket !== socket) {
        return;
      }
      try {
        this.receiveFrame(event.data);
      } catch (error) {
        this.emitError(error instanceof Error ? error.message : "Realtime frame handling failed", true);
      }
    };
    socket.onerror = () => {
      if (this.socket !== socket) {
        return;
      }
      this.emitError("Realtime socket error", true);
    };
    socket.onclose = (event) => {
      if (this.socket !== socket) {
        return;
      }
      this.stopHeartbeat();
      this.socket = undefined;
      const code = event?.code;
      const reason = event?.reason;
      this.emitState({
        connection: "offline",
        lastError:
          code === 1000
            ? undefined
            : `Realtime socket closed (${code || "unknown"}${reason ? `: ${reason}` : ""})`
      });
    };
  }
  private disconnect(): void {
    this.stopHeartbeat();
    this.socket?.close();
    this.socket = undefined;
    this.emitState({ connection: "idle" });
  }

  private subscribe(
    subscriptionId: string,
    viewKind: string,
    filters: Readonly<Record<string, string>>
  ): void {
    const subscription: SubscriptionState = {
      subscriptionId,
      requestId: crypto.randomUUID(),
      viewKind,
      filters,
      status: "subscribing"
    };
    this.subscriptions[subscriptionId] = subscription;
    this.post({ type: "subscription-state", subscription });
    this.sendEnvelope(makeSubscribe(subscriptionId, viewKind, filters, subscription.requestId));
  }

  private unsubscribe(subscriptionId: string): void {
    const existing = this.subscriptions[subscriptionId];
    const subscription: SubscriptionState = {
      subscriptionId,
      requestId: existing?.requestId ?? crypto.randomUUID(),
      viewKind: existing?.viewKind ?? "",
      filters: existing?.filters ?? {},
      status: "unsubscribed"
    };
    this.subscriptions[subscriptionId] = subscription;
    this.post({ type: "subscription-state", subscription });
    this.sendEnvelope(makeUnsubscribe(subscriptionId));
    if (this.sourceRepairs.retireSubscription(subscriptionId)) {
      this.emitState(this.recoveryPatch());
    }
  }

  private resendActiveSubscriptions(): void {
    for (const subscription of Object.values(this.subscriptions)) {
      if (subscription.status === "unsubscribed") {
        continue;
      }
      this.sendEnvelope(
        makeSubscribe(
          subscription.subscriptionId,
          subscription.viewKind,
          subscription.filters,
          subscription.requestId
        )
      );
    }
  }

  private sendCommand(command: PendingCommandInput, idempotencyKey?: string): void {
    const commandId = command.commandId || crypto.randomUUID();
    const pending: PendingCommandState = {
      commandId,
      idempotencyKey: idempotencyKey ?? command.idempotencyKey ?? commandId,
      status: "queued",
      createdAtUnixMs: Number(command.createdAtClientUnixMs || BigInt(Date.now())),
      targetScope: command.target.scope,
      targetScopeId: command.target.scopeId,
      targetEntityId: command.target.entityId,
      payloadCase: command.payload?.case
    };
    this.pendingCommands[commandId] = pending;
    this.post({ type: "command-state", command: pending });

    const fullCommand = {
      ...command,
      commandId,
      idempotencyKey: pending.idempotencyKey,
      createdAtClientUnixMs:
        command.createdAtClientUnixMs || BigInt(pending.createdAtUnixMs)
    };

    if (!this.sendEnvelope(makeCommand(fullCommand))) {
      const rejected = {
        ...pending,
        status: "rejected" as const,
        errorCode: "socket_unavailable",
        error: "Realtime socket is not open."
      };
      this.pendingCommands[commandId] = rejected;
      this.post({ type: "command-state", command: rejected });
      return;
    }

    const sent = { ...pending, status: "sent" as const };
    this.pendingCommands[commandId] = sent;
    this.post({ type: "command-state", command: sent });
  }

  private receiveFrame(data: unknown): void {
    if (!(data instanceof ArrayBuffer)) {
      this.emitError("Ignoring non-binary realtime frame", true);
      return;
    }

    let envelope: RealtimeEnvelope;
    try {
      envelope = fromBinary(RealtimeEnvelopeSchema, new Uint8Array(data));
    } catch (error) {
      this.failProtocolFrame(
        error instanceof Error ? error.message : "malformed realtime envelope"
      );
      return;
    }
    this.observeFrame?.(envelope);

    switch (envelope.messageKind) {
      case MessageKind.HELLO:
        this.handleHello(envelope);
        break;
      case MessageKind.SNAPSHOT:
        if (envelope.payload.case !== "snapshot") {
          this.failProtocolFrame("snapshot envelope is missing snapshot payload");
          return;
        }
        try {
          this.validateSnapshotProvenance(envelope.payload.value);
          const patch = envelope.payload.value.notFound
            ? decodeNotFoundSnapshot(envelope.payload.value)
            : decodeSnapshot(
              envelope.payload.value,
              cursorAuthorityFromEnvelope(envelope)?.sources.map((source) => source.sourceId) ?? []
            );
          this.validateSnapshotPatchSource(envelope, envelope.payload.value, patch);
          if (!this.applyViewEnvelopeCursor(envelope)) return;
          this.handleEntityPatch(this.installSnapshotCoverage(envelope, patch));
        } catch (error) {
          this.failProtocolFrame(error instanceof Error ? error.message : "invalid snapshot");
        }
        break;
      case MessageKind.PATCH:
        if (envelope.payload.case !== "patch") {
          this.failProtocolFrame("patch envelope is missing patch payload");
          return;
        }
        try {
          const resolvedSourceIds = cursorAuthorityFromEnvelope(envelope)?.sources
            .map((source) => source.sourceId) ?? [];
          const patch = decodePatch(envelope.payload.value, resolvedSourceIds);
          this.validateEntitySourceAgreement(
            patch,
            resolvedSourceIds,
            true
          );
          if (!this.applyViewEnvelopeCursor(envelope)) return;
          this.handleEntityPatch(this.installPatchCoverage(envelope, patch));
        } catch (error) {
          this.failProtocolFrame(error instanceof Error ? error.message : "invalid patch");
        }
        break;
      case MessageKind.PONG:
        break;
      case MessageKind.COMMAND_ACCEPTED:
      case MessageKind.COMMAND_REJECTED:
      case MessageKind.COMMAND_DUPLICATE:
        this.handleCommandLifecycle(envelope);
        break;
      case MessageKind.CONNECTION_DEGRADED:
        this.emitState({
          connection: "degraded",
          lastError:
            envelope.payload.case === "connectionDegraded"
              ? envelope.payload.value.reason
              : "Connection degraded"
        });
        break;
      case MessageKind.SOURCE_GAP_DETECTED:
        this.handleStaleSignal(envelope);
        break;
      case MessageKind.SOURCE_SNAPSHOT_RESYNC:
        if (envelope.payload.case !== "sourceSnapshotResync") {
          this.failProtocolFrame("source resync envelope is missing its payload");
          return;
        }
        try {
          const patch = decodeSourceSnapshotResync(envelope.payload.value);
          if (!this.applySourceResyncCursor(envelope)) return;
          this.invalidateSourceCoverage(envelope.payload.value.sourceId);
          this.frozenSources.add(envelope.payload.value.sourceId);
          this.sourceRepairs.begin(
            envelope.payload.value.sourceId,
            "source_resync",
            {}
          );
          this.handleEntityPatch(patch);
          this.resendActiveSubscriptions();
          const recovery = this.recoveryPatch(envelope.payload.value.sourceId);
          this.emitState({
            ...recovery,
            invalidatedSourceDomains: this.invalidatedSourceDomains,
            loadedCoverage: this.loadedCoverage
          });
        } catch (error) {
          this.failProtocolFrame(error instanceof Error ? error.message : "invalid source resync");
        }
        break;
      case MessageKind.SOURCE_GAP_FILLED:
        if (envelope.payload.case !== "sourceGapFilled") {
          this.failProtocolFrame("gap-filled envelope is missing its payload");
          return;
        }
        if (!envelope.payload.value.cursor) {
          this.failProtocolFrame("gap-filled payload lacks its source cursor");
          return;
        }
        if (this.sourceRepairs.markGapFilled(envelope.payload.value.cursor)) {
          this.emitState({ connection: "replaying", ...this.recoveryPatch() });
        } else {
          this.emitState({ connection: this.sourceRepairs.hasPending ? "stale" : "replaying" });
        }
        break;
      case MessageKind.ERROR:
        this.emitError(
          envelope.payload.case === "error"
            ? envelope.payload.value.message
            : "Realtime protocol error",
          envelope.payload.case === "error" ? envelope.payload.value.retryable : true
        );
        break;
    }
  }

  private handleHello(envelope: RealtimeEnvelope): void {
    if (envelope.payload.case !== "hello") {
      return;
    }

    const hello = envelope.payload.value;
    if (!hello.gatewayEpoch || hello.gatewayStartedAtUnixNs === 0n) {
      this.failProtocolFrame("hello lacks gateway generation authority");
      return;
    }
    if (this.gatewayEpoch || this.gatewayStartedAtUnixNs) {
      if (
        hello.gatewayEpoch !== this.gatewayEpoch ||
        hello.gatewayStartedAtUnixNs !== this.gatewayStartedAtUnixNs
      ) {
        this.failProtocolFrame("connection gateway generation changed after hello");
      }
      return;
    }
    this.connectionId = hello.connectionId;
    this.gatewayEpoch = hello.gatewayEpoch;
    this.gatewayStartedAtUnixNs = hello.gatewayStartedAtUnixNs;
    this.heartbeatIntervalMs =
      hello.heartbeatIntervalMs || HEARTBEAT_FALLBACK_MS;
    this.emitState({
      connection: "connected",
      connectionId: hello.connectionId,
      heartbeatIntervalMs: this.heartbeatIntervalMs,
      lastError: undefined
    });
    this.startHeartbeat();
    this.sendStartupFrames();
  }

  private sendStartupFrames(): void {
    if (this.startupFramesSent) {
      return;
    }
    this.startupFramesSent = true;
    try {
      this.sendEnvelope(
        makeResume(this.cursor, this.connectionId, this.subscriptions)
      );
      this.resendActiveSubscriptions();
    } catch (error) {
      this.emitError(error instanceof Error ? error.message : "Realtime socket setup failed", true);
    }
  }

  private handleEntityPatch(patch: EntityPatch): void {
    this.emitState({
      entityOperations: patch.entityOperations
    });
  }

  private invalidateSourceCoverage(sourceId: string): void {
    this.invalidatedSourceDomains = {
      ...this.invalidatedSourceDomains,
      [sourceId]: [...SOURCE_REPLACEMENT_DOMAINS]
    };
    this.loadedCoverage = Object.fromEntries(
      Object.entries(this.loadedCoverage).filter(([, coverage]) => coverage.sourceId !== sourceId)
    );
  }

  private installSnapshotCoverage(
    envelope: RealtimeEnvelope,
    patch: EntityPatch
  ): EntityPatch {
    if (envelope.payload.case !== "snapshot") return patch;
    const snapshot = envelope.payload.value;
    const subscriptionId = snapshot.subscriptionId;
    const subscription = this.subscriptions[subscriptionId];
    const filters = subscription?.filters ?? {};
    const sourceIds = snapshot.cursor?.sources.map((source) => source.sourceId) ?? [];
    const transformed: EntityOperation[] = [];
    for (const operation of patch.entityOperations) {
      const kind = snapshotCoverageKind(snapshot.viewKind, operation.entityIds);
      const removedWindowIds = new Set<string>();
      for (const sourceId of sourceIds) {
        const sourcePayload = Object.fromEntries(
          Object.entries(operation.payload).filter(([, entity]) =>
            (entity as { sourceId?: string } | undefined)?.sourceId === sourceId
          )
        );
        const payload = sourceIds.length === 1 ? operation.payload : sourcePayload;
        const payloadIds = Object.keys(payload);
        const key = `${sourceId}:${operation.domain}:${subscriptionId}`;
        const previous = this.loadedCoverage[key];
        const record: LoadedCoverage = {
          sourceId,
          domain: operation.domain,
          subscriptionId,
          kind,
          entityIds: payloadIds,
          filters,
          authoritative: true,
          empty: payloadIds.length === 0
        };
        this.loadedCoverage = { ...this.loadedCoverage, [key]: record };
        if (kind === "domain") {
          const remaining = (this.invalidatedSourceDomains[sourceId] ?? [])
            .filter((domain) => domain !== operation.domain);
          this.invalidatedSourceDomains = {
            ...this.invalidatedSourceDomains,
            [sourceId]: remaining
          };
        }
        if (kind === "window") {
          const removed = (previous?.entityIds ?? []).filter((id) => !payloadIds.includes(id));
          removed.forEach((id) => removedWindowIds.add(id));
          if (sourceIds.length === 1 && removed.length > 0) {
            transformed.push({
              operation: "remove",
              domain: operation.domain,
              entityIds: removed,
              authoritative: true,
              payload: {}
            });
          }
          if (sourceIds.length === 1) {
            transformed.push({ ...operation, operation: "upsert", payload });
          }
        } else if (sourceIds.length === 1) {
          transformed.push({
            ...operation,
            payload,
            sourceId: kind === "domain" ? sourceId : operation.sourceId
          });
        }
      }
      if (sourceIds.length > 1) {
        if (kind === "window" && removedWindowIds.size > 0) {
          transformed.push({
            operation: "remove",
            domain: operation.domain,
            entityIds: [...removedWindowIds],
            authoritative: true,
            payload: {}
          });
        }
        transformed.push(kind === "window" ? { ...operation, operation: "upsert" } : operation);
      }
      if (sourceIds.length === 0) transformed.push(operation);
    }
    this.sourceRepairs.retireSnapshot(
      subscriptionId,
      snapshot.requestId,
      sourceIds
    );
    if (subscription) {
      const active = { ...subscription, status: "active" as const };
      this.subscriptions[subscriptionId] = active;
      this.post({ type: "subscription-state", subscription: active });
    }
    const recovery = this.recoveryPatch();
    this.emitState({
      loadedCoverage: this.loadedCoverage,
      invalidatedSourceDomains: this.invalidatedSourceDomains,
      ...recovery
    });
    return { entityOperations: transformed };
  }

  private validateSnapshotProvenance(snapshot: Snapshot): void {
    if (snapshot.notFound && snapshot.schemaVersion !== 1) {
      throw new Error("not-found snapshot uses an unsupported schema version");
    }
    if (snapshot.schemaVersion !== 1) return;
    if (!snapshot.subscriptionId || !snapshot.requestId) {
      throw new Error("versioned snapshot lacks subscription provenance");
    }
    const subscription = this.subscriptions[snapshot.subscriptionId];
    if (!subscription || subscription.status === "unsubscribed") {
      throw new Error("snapshot references an unknown or unsubscribed subscription");
    }
    if (subscription.requestId !== snapshot.requestId) {
      throw new Error("snapshot request generation does not match current subscription");
    }
    if (canonicalViewKind(subscription.viewKind) !== canonicalViewKind(snapshot.viewKind)) {
      throw new Error("snapshot view kind disagrees with subscription");
    }
    const selectedFilterKey = snapshot.viewKind === "session_detail"
      ? "session_id"
      : snapshot.viewKind === "team_workspace"
        ? "team_id"
        : snapshot.viewKind === "process_tail"
          ? "process_id"
          : undefined;
    const selectedFilter = selectedFilterKey
      ? subscription.filters[selectedFilterKey]
      : undefined;
    if (selectedFilterKey && (
      !selectedFilter || snapshot.coverage?.entityIds.length !== 1 ||
      snapshot.coverage.entityIds[0] !== selectedFilter
    )) {
      throw new Error("snapshot selected entity disagrees with subscription filters");
    }
    if (selectedFilterKey) {
      const sourceId = subscription.filters.source_id;
      if (!sourceId || snapshot.cursor?.sources.length !== 1 ||
        snapshot.cursor.sources[0]?.sourceId !== sourceId) {
        throw new Error("selected snapshot source authority disagrees with subscription");
      }
    }
  }

  private validateSnapshotPatchSource(
    envelope: RealtimeEnvelope,
    snapshot: Snapshot,
    patch: EntityPatch
  ): void {
    const cursorSourceIds = cursorAuthorityFromEnvelope(envelope)?.sources
      .map((source) => source.sourceId) ?? [];
    this.validateEntitySourceAgreement(patch, cursorSourceIds, isSelectedViewKind(snapshot.viewKind));
    const subscription = this.subscriptions[snapshot.subscriptionId];
    const requestedSourceId = subscription?.filters.source_id;
    if (!requestedSourceId || !isSelectedViewKind(snapshot.viewKind)) return;
    for (const operation of patch.entityOperations) {
      for (const entity of Object.values(operation.payload)) {
        if ((entity as { sourceId?: string }).sourceId !== requestedSourceId) {
          throw new Error("selected snapshot body disagrees with requested source");
        }
      }
    }
  }

  private validateEntitySourceAgreement(
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

  private installPatchCoverage(envelope: RealtimeEnvelope, patch: EntityPatch): EntityPatch {
    const sourceIds = (cursorAuthorityFromEnvelope(envelope)?.sources ?? [])
      .map((source) => source.sourceId);
    const transformed = patch.entityOperations.map((operation) => {
      if (operation.operation === "remove") {
        const scopedSourceId = operation.sourceId ?? (sourceIds.length === 1 ? sourceIds[0] : undefined);
        this.loadedCoverage = Object.fromEntries(
          Object.entries(this.loadedCoverage).map(([key, coverage]) => [key, {
            ...coverage,
            entityIds: coverage.domain === operation.domain &&
              (!scopedSourceId || coverage.sourceId === scopedSourceId)
              ? coverage.entityIds.filter((id) => !operation.entityIds.includes(id))
              : coverage.entityIds
          }])
        );
        return { ...operation, sourceId: scopedSourceId };
      }
      for (const [entityId, entity] of Object.entries(operation.payload)) {
        const sourceId = (entity as { sourceId?: string }).sourceId;
        if (!sourceId) continue;
        const key = `${sourceId}:${operation.domain}:__patch__:${entityId}`;
        this.loadedCoverage = {
          ...this.loadedCoverage,
          [key]: {
            sourceId,
            domain: operation.domain,
            subscriptionId: "__patch__",
            kind: "entity",
            entityIds: [entityId],
            filters: {},
            authoritative: true,
            empty: false
          }
        };
      }
      return operation;
    });
    this.emitState({ loadedCoverage: this.loadedCoverage });
    return { entityOperations: transformed };
  }

  private failProtocolFrame(message: string): void {
    this.emitState({ connection: "degraded", lastError: `protocol_error: ${message}` });
    this.emitError(`Realtime protocol error: ${message}`, false);
  }

  private handleCommandLifecycle(envelope: RealtimeEnvelope): void {
    const next = reduceCommandLifecycle(envelope, this.pendingCommands);
    if (!next) return;
    this.pendingCommands[next.commandId] = next;
    this.post({ type: "command-state", command: next });
    if (next.status === "rejected" && next.refreshEntity) {
      const staleSourceId = next.targetEntityId?.startsWith("source:")
        ? next.targetEntityId.replace(/^source:/, "")
        : undefined;
      this.emitState({
        lastError: `${next.errorCode}: ${next.error}`,
        ...(staleSourceId ? {
          staleSourceOperations: [{
            operation: "add" as const,
            sourceIds: [staleSourceId],
            reasons: { [staleSourceId]: next.errorCode ?? "source_stale" }
          }]
        } : {})
      });
    }
  }

  private handleStaleSignal(envelope: RealtimeEnvelope): void {
    if (envelope.payload.case !== "sourceGapDetected") return;
    const next = envelope.payload.value.nextAvailable;
    const lastSeen = envelope.payload.value.lastSeen;
    const sourceId = next?.sourceId || lastSeen?.sourceId || envelope.sourceId;
    if (!sourceId) return;
    this.requestTargetedRepair(sourceId, "gap_detected", next ? {
      sourceId,
      sourceEpoch: next.sourceEpoch,
      sourceSeq: next.sourceSeq
    } : undefined, "gap_fill");
  }

  private requestTargetedRepair(
    sourceId: string,
    reason: string,
    expected?: SourceCursorState,
    completion: "gap_fill" | "source_resync" = "source_resync"
  ): void {
    this.invalidateSourceCoverage(sourceId);
    this.frozenSources.add(sourceId);
    this.emitState({
      connection: "stale",
      invalidatedSourceDomains: this.invalidatedSourceDomains,
      loadedCoverage: this.loadedCoverage,
      staleSourceOperations: [{
        operation: "add",
        sourceIds: [sourceId],
        reasons: { [sourceId]: reason }
      }]
    });
    const requirements: Record<string, string> = {};
    for (const subscription of Object.values(this.subscriptions)) {
      if (subscription.status === "unsubscribed") continue;
      const requestedSource = subscription.filters.source_id;
      if (requestedSource && requestedSource !== sourceId) continue;
      const repairing = {
        ...subscription,
        requestId: crypto.randomUUID(),
        status: "subscribing" as const
      };
      this.subscriptions[subscription.subscriptionId] = repairing;
      requirements[repairing.subscriptionId] = repairing.requestId;
      this.sourceRepairs.renewSubscription(repairing.subscriptionId, repairing.requestId);
      this.post({ type: "subscription-state", subscription: repairing });
      this.sendEnvelope(makeSubscribe(
        repairing.subscriptionId,
        repairing.viewKind,
        repairing.filters,
        repairing.requestId
      ));
    }
    this.sourceRepairs.begin(sourceId, completion, requirements, expected);
  }
  private recoveryPatch(authoritativeResetSourceId?: string): GoosewebStorePatch {
    return sourceRepairRecoveryPatch(
      this.sourceRepairs,
      this.frozenSources,
      authoritativeResetSourceId
    );
  }

  private applyViewEnvelopeCursor(envelope: RealtimeEnvelope): boolean {
    const isSnapshot = envelope.messageKind === MessageKind.SNAPSHOT;
    const isViewFrame = isSnapshot || envelope.messageKind === MessageKind.PATCH;
    if (!isViewFrame) {
      this.failProtocolFrame("non-view frame cannot mutate canonical view authority");
      return false;
    }
    if (
      isViewFrame &&
      envelope.messageId &&
      this.appliedViewMessageIds.has(envelope.messageId)
    ) {
      return false;
    }
    const authority = cursorAuthorityFromEnvelope(envelope);
    if (!authority) {
      this.failProtocolFrame("versioned view frame lacks canonical cursor authority");
      return false;
    }
    const usesLegacyViewAuthority = isViewFrame &&
      (envelope.payload.case === "snapshot" || envelope.payload.case === "patch") &&
      envelope.payload.value.schemaVersion === 0 && !envelope.payload.value.cursor;
    const gatewayEpoch = usesLegacyViewAuthority ? this.gatewayEpoch : authority.gatewayEpoch;
    const gatewayStartedAtUnixNs = usesLegacyViewAuthority
      ? this.gatewayStartedAtUnixNs
      : authority.gatewayStartedAtUnixNs;
    const { gatewaySeq, sources } = authority;
    if (
      !isValidCursorVector(sources) ||
      (isViewFrame && (
        !gatewayEpoch || gatewayStartedAtUnixNs === 0n ||
        (!isSnapshot && gatewaySeq === 0n) || sources.length === 0
      ))
    ) {
      this.failProtocolFrame("cursor vector contains invalid or duplicate source authority");
      return false;
    }
    if (
      gatewayEpoch !== this.gatewayEpoch ||
      gatewayStartedAtUnixNs !== this.gatewayStartedAtUnixNs
    ) {
      this.emitState({
        connection: "stale",
        lastError: "gateway_generation_mismatch: frame does not match current hello"
      });
      return false;
    }
    if (
      (!this.cursor.gatewayEpoch && this.cursor.gatewaySeq > 0n) ||
      (this.cursor.gatewayEpoch && this.cursor.gatewayEpoch !== gatewayEpoch)
    ) {
      this.emitState({
        connection: "stale",
        lastError: "gateway_epoch_mismatch: explicit source resync required"
      });
      return false;
    }
    if (hasCursorEpochMismatch(this.cursor, sources)) {
      for (const source of sources) {
        const existing = this.cursor.sourceCursors[source.sourceId];
        if (existing && existing.sourceEpoch !== source.sourceEpoch) {
          this.requestTargetedRepair(source.sourceId, "source_epoch_mismatch", source);
        }
      }
      return false;
    }
    if (!isSnapshot) {
      const jumped = sources.find((source) => {
        const existing = this.cursor.sourceCursors[source.sourceId];
        return existing?.sourceEpoch === source.sourceEpoch &&
          source.sourceSeq > existing.sourceSeq + 1n;
      });
      if (jumped) {
        this.requestTargetedRepair(jumped.sourceId, "source_cursor_gap", jumped, "gap_fill");
        return false;
      }
      if (sources.some((source) => this.frozenSources.has(source.sourceId))) {
        return false;
      }
    }
    if (!shouldApplyCursorVector(
      this.cursor,
      gatewaySeq,
      sources,
      {
        allowEqualSourceSeq: true,
        allowEpochChange: false,
        allowGatewayRegression: isSnapshot
      }
    )) {
      return false;
    }

    this.cursor = mergeCursorVector(this.cursor, gatewaySeq, sources, {
      gatewayEpoch: gatewayEpoch || this.cursor.gatewayEpoch,
      gatewayStartedAtUnixNs:
        gatewayStartedAtUnixNs || this.cursor.gatewayStartedAtUnixNs
    });
    const cursorToPersist = this.cursor;
    this.cursorPersistQueue = this.cursorPersistQueue
      .then(() => persistCursorState(cursorToPersist))
      .catch((error) => {
        this.emitError(
          error instanceof Error ? error.message : "Failed to persist realtime cursor",
          true
        );
      });
    this.emitState({ cursor: this.cursor });
    if (isViewFrame && envelope.messageId) {
      this.rememberViewMessage(envelope.messageId);
    }
    return true;
  }

  private applySourceResyncCursor(envelope: RealtimeEnvelope): boolean {
    if (envelope.payload.case !== "sourceSnapshotResync") return false;
    if (envelope.messageId && this.appliedViewMessageIds.has(envelope.messageId)) return false;
    const authority = cursorAuthorityFromEnvelope(envelope);
    if (!authority || !authority.gatewayEpoch || authority.gatewayStartedAtUnixNs === 0n ||
        authority.gatewaySeq === 0n || authority.sources.length !== 1) {
      this.failProtocolFrame("source resync lacks canonical cursor authority");
      return false;
    }
    const source = authority.sources[0];
    if (!source || source.sourceId !== envelope.payload.value.sourceId ||
        !isValidCursorVector(authority.sources)) {
      this.failProtocolFrame("source resync cursor disagrees with source identity");
      return false;
    }
    const generationChanged = isNewGatewayGeneration(
      this.cursor,
      authority.gatewayEpoch,
      authority.gatewayStartedAtUnixNs
    );
    if (
      authority.gatewayEpoch !== this.gatewayEpoch ||
      authority.gatewayStartedAtUnixNs !== this.gatewayStartedAtUnixNs
    ) {
      this.emitState({
        connection: "stale",
        lastError: "gateway_generation_mismatch: source resync rejected"
      });
      return false;
    }
    if (!shouldApplyCursorVector(this.cursor, authority.gatewaySeq, authority.sources, {
      allowEqualSourceSeq: true,
      allowEpochChange: true,
      allowGatewayRegression: generationChanged
    })) return false;
    this.cursor = mergeCursorVector(
      this.cursor,
      authority.gatewaySeq,
      authority.sources,
      {
        replaceGateway: generationChanged,
        gatewayEpoch: authority.gatewayEpoch,
        gatewayStartedAtUnixNs: authority.gatewayStartedAtUnixNs
      }
    );
    this.persistAndEmitCursor();
    if (envelope.messageId) this.rememberViewMessage(envelope.messageId);
    return true;
  }

  private persistAndEmitCursor(): void {
    const cursorToPersist = this.cursor;
    this.cursorPersistQueue = this.cursorPersistQueue
      .then(() => persistCursorState(cursorToPersist))
      .catch((error) => {
        this.emitError(
          error instanceof Error ? error.message : "Failed to persist realtime cursor",
          true
        );
      });
    this.emitState({ cursor: this.cursor });
  }

  private rememberViewMessage(messageId: string): void {
    this.appliedViewMessageIds.add(messageId);
    this.appliedViewMessageOrder.push(messageId);
    if (this.appliedViewMessageOrder.length > 2_048) {
      const expired = this.appliedViewMessageOrder.shift();
      if (expired) this.appliedViewMessageIds.delete(expired);
    }
  }

  private startHeartbeat(): void {
    this.stopHeartbeat();
    this.heartbeatTimer = setInterval(() => {
      this.sendEnvelope(makePing());
    }, this.heartbeatIntervalMs);
  }

  private stopHeartbeat(): void {
    if (this.heartbeatTimer) {
      clearInterval(this.heartbeatTimer);
      this.heartbeatTimer = undefined;
    }
  }

  private sendEnvelope(envelope: RealtimeEnvelope): boolean {
    if (!this.socket || this.socket.readyState !== WebSocket.OPEN) {
      return false;
    }

    const lane = envelope.lane;
    if (lane === Lane.BULK && this.socket.bufferedAmount > 1_000_000) {
      this.emitState({
        connection: "degraded",
        lastError: "Realtime bulk lane backpressure"
      });
      return false;
    }

    this.socket.send(toBinary(RealtimeEnvelopeSchema, envelope));
    return true;
  }

  private emitState(patch: GoosewebStorePatch): void {
    this.queuedPatch = mergeStorePatch(this.queuedPatch, patch);
    if (this.flushTimer) {
      return;
    }

    this.flushTimer = setTimeout(() => {
      const next = this.queuedPatch;
      this.queuedPatch = {};
      this.flushTimer = undefined;
      this.post({ type: "state", patch: next });
    }, PATCH_FLUSH_MS);
  }

  private emitError(message: string, retryable: boolean): void {
    this.post({ type: "error", message, retryable });
  }
}

type PendingCommandInput = Parameters<typeof makeCommand>[0];
