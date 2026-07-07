import { create } from "@bufbuild/protobuf";
import {
  EntityRefSchema,
  Lane,
  MessageKind,
  Scope
} from "../../../src/gen/goosetower/v1/common_pb";
import {
  AuthRefreshSchema,
  PingSchema,
  RealtimeEnvelopeSchema,
  ResumeSchema,
  SubscribeSchema,
  UnsubscribeSchema,
  type RealtimeEnvelope,
  type Subscribe
} from "../../../src/gen/goosetower/v1/realtime_pb";
import {
  CommandBroadcastTeamMessageSchema,
  CommandCancelDeliverySchema,
  CommandCreateSessionSchema,
  CommandCreateTeamSchema,
  CommandInterruptTurnSchema,
  CommandKillProcessSchema,
  CommandResolveApprovalSchema,
  CommandRetryDeliverySchema,
  CommandSchema,
  CommandSendTeamMessageSchema,
  CommandSendTurnSchema,
  CommandSpawnTeamMemberSchema,
  CommandStartProcessSchema,
  type Command
} from "../../../src/gen/goosetower/v1/commands_pb";
import { cursorStateToProto } from "../cursors";
import type {
  CommandIntent,
  CommandPayloadCase,
  CommandScope,
  CursorState,
  SubscriptionState
} from "../types";

const PROTOCOL_VERSION = 1;

export function makePing(): RealtimeEnvelope {
  return create(RealtimeEnvelopeSchema, {
    protocolVersion: PROTOCOL_VERSION,
    messageId: randomMessageId("ping"),
    messageKind: MessageKind.PING,
    lane: Lane.CRITICAL,
    happenedAtUnixMs: BigInt(Date.now()),
    payload: {
      case: "ping",
      value: create(PingSchema, {
        clientTimeUnixMs: BigInt(Date.now())
      })
    }
  });
}

export function makeResume(
  cursor: CursorState,
  previousConnectionId: string | undefined,
  activeSubscriptions: Readonly<Record<string, SubscriptionState>>
): RealtimeEnvelope {
  return create(RealtimeEnvelopeSchema, {
    protocolVersion: PROTOCOL_VERSION,
    messageId: randomMessageId("resume"),
    messageKind: MessageKind.RESUME,
    lane: Lane.CRITICAL,
    happenedAtUnixMs: BigInt(Date.now()),
    payload: {
      case: "resume",
      value: create(ResumeSchema, {
        cursor: cursorStateToProto(cursor),
        previousConnectionId,
        activeSubscriptions: Object.values(activeSubscriptions)
          .filter((subscription) => subscription.status !== "unsubscribed")
          .map(subscriptionStateToProto)
      })
    }
  });
}

export function makeSubscribe(
  subscriptionId: string,
  viewKind: string,
  filters: Readonly<Record<string, string>>
): RealtimeEnvelope {
  return create(RealtimeEnvelopeSchema, {
    protocolVersion: PROTOCOL_VERSION,
    messageId: randomMessageId("sub"),
    messageKind: MessageKind.SUBSCRIBE,
    lane: Lane.STATE,
    happenedAtUnixMs: BigInt(Date.now()),
    payload: {
      case: "subscribe",
      value: create(SubscribeSchema, {
        subscriptionId,
        viewKind,
        filters: { ...filters }
      })
    }
  });
}

export function makeUnsubscribe(subscriptionId: string): RealtimeEnvelope {
  return create(RealtimeEnvelopeSchema, {
    protocolVersion: PROTOCOL_VERSION,
    messageId: randomMessageId("unsub"),
    messageKind: MessageKind.UNSUBSCRIBE,
    lane: Lane.STATE,
    happenedAtUnixMs: BigInt(Date.now()),
    payload: {
      case: "unsubscribe",
      value: create(UnsubscribeSchema, { subscriptionId })
    }
  });
}

export function makeCommand(command: CommandIntent): RealtimeEnvelope {
  const encodedCommand = commandIntentToProto(command);
  return create(RealtimeEnvelopeSchema, {
    protocolVersion: PROTOCOL_VERSION,
    messageId: randomMessageId("cmd"),
    messageKind: MessageKind.COMMAND,
    lane: Lane.CRITICAL,
    scope: encodedCommand.target?.scope ?? Scope.UNSPECIFIED,
    scopeId: encodedCommand.target?.scopeId ?? "",
    commandId: command.commandId,
    happenedAtUnixMs: BigInt(Date.now()),
    payload: {
      case: "command",
      value: encodedCommand
    }
  });
}

function commandIntentToProto(command: CommandIntent): Command {
  return create(CommandSchema, {
    commandId: command.commandId,
    idempotencyKey: command.idempotencyKey,
    createdAtClientUnixMs: command.createdAtClientUnixMs,
    target: create(EntityRefSchema, {
      scope: scopeToProto(command.target.scope),
      scopeId: command.target.scopeId,
      entityId: command.target.entityId
    }),
    payload: makeCommandPayload(command.payload.case, command.payload.value)
  });
}

function scopeToProto(scope: CommandScope): Scope {
  switch (scope) {
    case "team":
      return Scope.TEAM;
    case "process":
      return Scope.PROCESS;
    case "source":
      return Scope.SOURCE;
    case "session":
      return Scope.SESSION;
  }
}

function makeCommandPayload(
  payloadCase: CommandPayloadCase,
  payloadValue: Readonly<Record<string, unknown>>
): Command["payload"] {
  switch (payloadCase) {
    case "sendTurn":
      return {
        case: payloadCase,
        value: create(CommandSendTurnSchema, payloadValue)
      };
    case "resolveApproval":
      return {
        case: payloadCase,
        value: create(CommandResolveApprovalSchema, payloadValue)
      };
    case "interruptTurn":
      return {
        case: payloadCase,
        value: create(CommandInterruptTurnSchema, payloadValue)
      };
    case "sendTeamMessage":
      return {
        case: payloadCase,
        value: create(CommandSendTeamMessageSchema, payloadValue)
      };
    case "broadcastTeamMessage":
      return {
        case: payloadCase,
        value: create(CommandBroadcastTeamMessageSchema, payloadValue)
      };
    case "spawnTeamMember":
      return {
        case: payloadCase,
        value: create(CommandSpawnTeamMemberSchema, payloadValue)
      };
    case "retryDelivery":
      return {
        case: payloadCase,
        value: create(CommandRetryDeliverySchema, payloadValue)
      };
    case "cancelDelivery":
      return {
        case: payloadCase,
        value: create(CommandCancelDeliverySchema, payloadValue)
      };
    case "killProcess":
      return {
        case: payloadCase,
        value: create(CommandKillProcessSchema, payloadValue)
      };
    case "startProcess":
      return {
        case: payloadCase,
        value: create(CommandStartProcessSchema, payloadValue)
      };
    case "createSession":
      return {
        case: payloadCase,
        value: create(CommandCreateSessionSchema, payloadValue)
      };
    case "createTeam":
      return {
        case: payloadCase,
        value: create(CommandCreateTeamSchema, payloadValue)
      };
  }
}

export function makeAuthRefresh(ticket: string): RealtimeEnvelope {
  return create(RealtimeEnvelopeSchema, {
    protocolVersion: PROTOCOL_VERSION,
    messageId: randomMessageId("auth"),
    messageKind: MessageKind.AUTH_REFRESH,
    lane: Lane.CRITICAL,
    happenedAtUnixMs: BigInt(Date.now()),
    payload: {
      case: "authRefresh",
      value: create(AuthRefreshSchema, { ticket })
    }
  });
}

function subscriptionStateToProto(subscription: SubscriptionState): Subscribe {
  return create(SubscribeSchema, {
    subscriptionId: subscription.subscriptionId,
    viewKind: subscription.viewKind,
    filters: { ...subscription.filters }
  });
}

function randomMessageId(prefix: string): string {
  return `${prefix}_${crypto.randomUUID()}`;
}
