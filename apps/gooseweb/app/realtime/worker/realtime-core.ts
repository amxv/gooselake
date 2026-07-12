import { fromBinary, toBinary } from "@bufbuild/protobuf";
import {
  Lane,
  MessageKind
} from "../../../src/gen/goosetower/v1/common_pb";
import {
  RealtimeEnvelopeSchema,
  type RealtimeEnvelope
} from "../../../src/gen/goosetower/v1/realtime_pb";
import { realtimeUrlWithTicket } from "../config";
import {
  emptyCursorState,
  loadCursorState,
  mergeCursor,
  persistCursorState,
  shouldApplyCursor
} from "../cursors";
import { decodePatch, decodeSnapshot, type EntityPatch } from "../protocol/entities";
import type {
  CursorState,
  GoosewebStorePatch,
  GoosewebSnapshot,
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
} from "./messages";

const HEARTBEAT_FALLBACK_MS = 15_000;
const PATCH_FLUSH_MS = 16;

export class RealtimeWorkerCore {
  private socket: WebSocket | undefined;
  private cursor: CursorState = emptyCursorState;
  private connectionId: string | undefined;
  private heartbeatIntervalMs = HEARTBEAT_FALLBACK_MS;
  private heartbeatTimer: ReturnType<typeof setInterval> | undefined;
  private subscriptions: Record<string, SubscriptionState> = {};
  private pendingCommands: Record<string, PendingCommandState> = {};
  private queuedPatch: GoosewebStorePatch = {};
  private flushTimer: ReturnType<typeof setTimeout> | undefined;
  private startupFramesSent = false;

  constructor(private readonly post: (message: WorkerOutbound) => void) {}

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
    this.cursor = await loadCursorState();
    this.emitState({ connection: "connecting", cursor: this.cursor });

    const socket = new WebSocket(realtimeUrlWithTicket(goosetowerUrl, ticket));
    this.socket = socket;
    socket.binaryType = "arraybuffer";
    socket.onopen = () => {
      if (this.socket !== socket) {
        return;
      }
      this.emitState({ connection: "connected" });
      this.startHeartbeat();
      this.sendStartupFrames();
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
      viewKind,
      filters,
      status: "subscribing"
    };
    this.subscriptions[subscriptionId] = subscription;
    this.post({ type: "subscription-state", subscription });
    this.sendEnvelope(makeSubscribe(subscriptionId, viewKind, filters));
  }

  private unsubscribe(subscriptionId: string): void {
    const existing = this.subscriptions[subscriptionId];
    const subscription: SubscriptionState = {
      subscriptionId,
      viewKind: existing?.viewKind ?? "",
      filters: existing?.filters ?? {},
      status: "unsubscribed"
    };
    this.subscriptions[subscriptionId] = subscription;
    this.post({ type: "subscription-state", subscription });
    this.sendEnvelope(makeUnsubscribe(subscriptionId));
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
          subscription.filters
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
      payloadCase: command.payload.case
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

    const envelope = fromBinary(RealtimeEnvelopeSchema, new Uint8Array(data));

    switch (envelope.messageKind) {
      case MessageKind.HELLO:
        this.handleHello(envelope);
        break;
      case MessageKind.SNAPSHOT:
        if (!this.applyEnvelopeCursor(envelope)) {
          return;
        }
        this.handleEntityPatch(
          envelope.payload.case === "snapshot"
            ? decodeSnapshot(envelope.payload.value)
            : { entityOperations: [] }
        );
        break;
      case MessageKind.PATCH:
        if (!this.applyEnvelopeCursor(envelope)) {
          return;
        }
        this.handleEntityPatch(
          envelope.payload.case === "patch"
            ? decodePatch(envelope.payload.value)
            : { entityOperations: [] }
        );
        break;
      case MessageKind.PONG:
        if (!this.applyEnvelopeCursor(envelope)) {
          return;
        }
        this.emitState({ connection: "connected" });
        break;
      case MessageKind.COMMAND_ACCEPTED:
      case MessageKind.COMMAND_REJECTED:
      case MessageKind.COMMAND_DUPLICATE:
        if (!this.applyEnvelopeCursor(envelope)) {
          return;
        }
        this.handleCommandLifecycle(envelope);
        break;
      case MessageKind.CONNECTION_DEGRADED:
        if (!this.applyEnvelopeCursor(envelope)) {
          return;
        }
        this.emitState({
          connection: "degraded",
          lastError:
            envelope.payload.case === "connectionDegraded"
              ? envelope.payload.value.reason
              : "Connection degraded"
        });
        break;
      case MessageKind.SOURCE_GAP_DETECTED:
      case MessageKind.SOURCE_SNAPSHOT_RESYNC:
        if (!this.applyEnvelopeCursor(envelope)) {
          return;
        }
        this.handleStaleSignal(envelope);
        break;
      case MessageKind.SOURCE_GAP_FILLED:
        if (!this.applyEnvelopeCursor(envelope)) {
          return;
        }
        this.emitState({ connection: "replaying" });
        break;
      case MessageKind.ERROR:
        if (!this.applyEnvelopeCursor(envelope)) {
          return;
        }
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
    this.connectionId = hello.connectionId;
    this.heartbeatIntervalMs =
      hello.heartbeatIntervalMs || HEARTBEAT_FALLBACK_MS;
    this.emitState({
      connection: "connected",
      connectionId: hello.connectionId,
      heartbeatIntervalMs: this.heartbeatIntervalMs
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
    this.emitState({ entityOperations: patch.entityOperations });
  }

  private handleCommandLifecycle(envelope: RealtimeEnvelope): void {
    const commandId = envelope.commandId || commandIdFromPayload(envelope);
    if (!commandId) {
      return;
    }

    const current = this.pendingCommands[commandId];
    const base: PendingCommandState = current ?? {
      commandId,
      idempotencyKey: commandId,
      status: "sent",
      createdAtUnixMs: Date.now()
    };

    const rejection =
      envelope.payload.case === "commandRejected"
        ? envelope.payload.value.error
        : undefined;
    const rejectionCode = rejection?.code || "upstream_rejected";
    const next: PendingCommandState =
      envelope.messageKind === MessageKind.COMMAND_ACCEPTED
        ? { ...base, status: "accepted", error: undefined, errorCode: undefined }
        : envelope.messageKind === MessageKind.COMMAND_DUPLICATE
          ? {
              ...base,
              status: "duplicate",
              errorCode: "duplicate",
              error: commandReasonCopy("duplicate")
            }
          : {
              ...base,
              status: "rejected",
              errorCode: rejectionCode,
              error: commandReasonCopy(rejectionCode, rejection?.message),
              refreshEntity: shouldRefreshRejectedCommand(rejectionCode)
            };

    this.pendingCommands[commandId] = next;
    this.post({ type: "command-state", command: next });
    if (next.status === "rejected" && next.refreshEntity) {
      this.emitState({
        lastError: `${next.errorCode}: ${next.error}`,
        staleSources: next.targetEntityId?.startsWith("source:")
          ? { [next.targetEntityId.replace(/^source:/, "")]: next.errorCode ?? "source_stale" }
          : undefined
      });
    }
  }

  private handleStaleSignal(envelope: RealtimeEnvelope): void {
    const staleSources: Record<string, string> = {};
    if (envelope.payload.case === "sourceGapDetected") {
      staleSources[envelope.payload.value.lastSeen?.sourceId ?? envelope.sourceId] =
        "gap_detected";
    } else if (envelope.payload.case === "sourceSnapshotResync") {
      staleSources[envelope.payload.value.sourceId || envelope.sourceId] =
        envelope.payload.value.reason || "snapshot_resync";
    }

    this.emitState({ connection: "stale", staleSources });
  }

  private applyEnvelopeCursor(envelope: RealtimeEnvelope): boolean {
    const source = sourceCursorFromEnvelope(envelope);
    if (!shouldApplyCursor(this.cursor, envelope.gatewaySeq, source)) {
      if (
        !source &&
        (envelope.messageKind === MessageKind.PATCH ||
          envelope.messageKind === MessageKind.SNAPSHOT)
      ) {
        return true;
      }
      return false;
    }

    this.cursor = mergeCursor(this.cursor, envelope.gatewaySeq, source);
    void persistCursorState(this.cursor);
    this.emitState({ cursor: this.cursor });
    return true;
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
    this.queuedPatch = mergeSnapshotPatch(this.queuedPatch, patch);
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

function sourceCursorFromEnvelope(
  envelope: RealtimeEnvelope
): SourceCursorState | undefined {
  if (!envelope.sourceId || envelope.sourceSeq === 0n) {
    return undefined;
  }

  return {
    sourceId: envelope.sourceId,
    sourceEpoch: envelope.sourceEpoch,
    sourceSeq: envelope.sourceSeq
  };
}

function commandIdFromPayload(envelope: RealtimeEnvelope): string {
  switch (envelope.payload.case) {
    case "commandAccepted":
      return envelope.payload.value.commandId;
    case "commandRejected":
      return envelope.payload.value.commandId;
    case "commandDuplicate":
      return envelope.payload.value.commandId;
    default:
      return "";
  }
}

function commandReasonCopy(code: string, fallback?: string): string {
  switch (code) {
    case "unauthorized":
      return "This session is not authorized to run that command.";
    case "invalid_scope":
      return "The command does not match the selected object type.";
    case "invalid_target":
      return "The selected object is no longer available.";
    case "stale_entity_version":
      return "The selected object changed. Refreshing its state before retry.";
    case "source_unavailable":
      return "The runtime source is unavailable.";
    case "source_stale":
      return "The runtime source is stale. Refreshing before retry.";
    case "source_gap":
      return "The runtime event stream has a gap. Refreshing before retry.";
    case "upstream_rejected":
      return fallback || "The runtime rejected the command.";
    case "upstream_timeout":
      return "The runtime did not respond before the command timed out.";
    case "duplicate":
      return "This command was already submitted.";
    default:
      return fallback || "Command rejected.";
  }
}

function shouldRefreshRejectedCommand(code: string): boolean {
  return (
    code === "stale_entity_version" ||
    code === "source_stale" ||
    code === "source_gap" ||
    code === "source_unavailable"
  );
}

function mergeSnapshotPatch(
  current: GoosewebStorePatch,
  next: GoosewebStorePatch
): GoosewebStorePatch {
  return {
    ...current,
    ...next,
    entities: next.entities
      ? { ...current.entities, ...next.entities }
      : current.entities,
    pendingCommands: next.pendingCommands
      ? { ...current.pendingCommands, ...next.pendingCommands }
      : current.pendingCommands,
    subscriptions: next.subscriptions
      ? { ...current.subscriptions, ...next.subscriptions }
      : current.subscriptions,
    staleSources: next.staleSources
      ? { ...current.staleSources, ...next.staleSources }
      : current.staleSources
  };
}
