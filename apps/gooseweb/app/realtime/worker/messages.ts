import { create } from "@bufbuild/protobuf";
import {
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
import type { Command } from "../../../src/gen/goosetower/v1/commands_pb";
import { CommandSchema } from "../../../src/gen/goosetower/v1/commands_pb";
import { cursorStateToProto } from "../cursors";
import type { CursorState, SubscriptionState } from "../types";

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

export function makeCommand(command: Command): RealtimeEnvelope {
  return create(RealtimeEnvelopeSchema, {
    protocolVersion: PROTOCOL_VERSION,
    messageId: randomMessageId("cmd"),
    messageKind: MessageKind.COMMAND,
    lane: Lane.CRITICAL,
    scope: command.target?.scope ?? Scope.UNSPECIFIED,
    scopeId: command.target?.scopeId ?? "",
    commandId: command.commandId,
    happenedAtUnixMs: BigInt(Date.now()),
    payload: {
      case: "command",
      value: create(CommandSchema, command)
    }
  });
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
