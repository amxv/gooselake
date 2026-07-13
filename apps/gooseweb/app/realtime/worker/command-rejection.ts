import { MessageKind } from "../../../src/gen/goosetower/v1/common_pb";
import type { RealtimeEnvelope } from "../../../src/gen/goosetower/v1/realtime_pb";
import type { PendingCommandState } from "../types";

export function reduceCommandLifecycle(
  envelope: RealtimeEnvelope,
  pendingCommands: Readonly<Record<string, PendingCommandState>>
): PendingCommandState | undefined {
  const commandId = envelope.commandId || commandIdFromPayload(envelope);
  if (!commandId) return undefined;
  const current = pendingCommands[commandId];
  const base: PendingCommandState = current ?? {
    commandId,
    idempotencyKey: commandId,
    status: "sent",
    createdAtUnixMs: Date.now()
  };
  const rejection = envelope.payload.case === "commandRejected"
    ? envelope.payload.value.error
    : undefined;
  const rejectionCode = rejection?.code || "upstream_rejected";
  if (envelope.messageKind === MessageKind.COMMAND_ACCEPTED) {
    return { ...base, status: "accepted", error: undefined, errorCode: undefined };
  }
  if (envelope.messageKind === MessageKind.COMMAND_DUPLICATE) {
    return {
      ...base,
      status: "duplicate",
      errorCode: "duplicate",
      error: commandReasonCopy("duplicate")
    };
  }
  return {
    ...base,
    status: "rejected",
    errorCode: rejectionCode,
    error: commandReasonCopy(rejectionCode, rejection?.message),
    refreshEntity: shouldRefreshRejectedCommand(rejectionCode)
  };
}

function commandIdFromPayload(envelope: RealtimeEnvelope): string {
  switch (envelope.payload.case) {
    case "commandAccepted": return envelope.payload.value.commandId;
    case "commandRejected": return envelope.payload.value.commandId;
    case "commandDuplicate": return envelope.payload.value.commandId;
    default: return "";
  }
}

export function commandReasonCopy(code: string, fallback?: string): string {
  switch (code) {
    case "unauthorized": return "This session is not authorized to run that command.";
    case "invalid_scope": return "The command does not match the selected object type.";
    case "invalid_target": return "The selected object is no longer available.";
    case "stale_entity_version":
      return "The selected object changed. Refreshing its state before retry.";
    case "source_unavailable": return "The runtime source is unavailable.";
    case "source_stale": return "The runtime source is stale. Refreshing before retry.";
    case "source_gap": return "The runtime event stream has a gap. Refreshing before retry.";
    case "upstream_rejected": return fallback || "The runtime rejected the command.";
    case "upstream_timeout":
      return "The runtime did not respond before the command timed out.";
    case "duplicate": return "This command was already submitted.";
    default: return fallback || "Command rejected.";
  }
}

export function shouldRefreshRejectedCommand(code: string): boolean {
  return code === "stale_entity_version" || code === "source_stale" ||
    code === "source_gap" || code === "source_unavailable";
}
