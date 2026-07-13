import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { resolve } from "node:path";
import { create, fromBinary, toBinary } from "@bufbuild/protobuf";
import { Lane, MessageKind } from "../src/gen/goosetower/v1/common_pb";
import {
  ConnectionDegradedSchema,
  HelloSchema,
  PongSchema,
  RealtimeEnvelopeSchema,
  SourceGapDetectedSchema,
  SourceGapFilledSchema,
  SourceSnapshotResyncSchema
} from "../src/gen/goosetower/v1/realtime_pb";
import {
  CommandAcceptedSchema,
  CommandDuplicateSchema,
  CommandRejectedSchema
} from "../src/gen/goosetower/v1/commands_pb";
import {
  ApprovalViewSchema,
  PatchSchema,
  SnapshotSchema,
  ViewCoverageSchema,
  ViewOperation
} from "../src/gen/goosetower/v1/view_pb";
import {
  EntityRefSchema,
  ErrorDetailSchema,
  SourceCursorSchema
} from "../src/gen/goosetower/v1/common_pb";
import type { WorkerOutbound } from "../app/realtime/types";
import { RealtimeWorkerCore } from "../app/realtime/worker/realtime-command-core";
import { sourceEntityKey } from "../app/realtime/protocol/entities";
import {
  getGoosewebSnapshot,
  getVisibleGoosewebSnapshot,
  resetGoosewebStoreForTests,
  updateGoosewebStore
} from "../app/stores/gooseweb-store";

export const sockets: FakeSocket[] = [];
export const posted: WorkerOutbound[] = [];
resetGoosewebStoreForTests();

export class FakeSocket {
  static readonly OPEN = 1;

  binaryType = "";
  bufferedAmount = 0;
  readyState = FakeSocket.OPEN;
  onopen: (() => void) | null = null;
  onmessage: ((event: { data: unknown }) => void) | null = null;
  onerror: (() => void) | null = null;
  onclose: (() => void) | null = null;
  sent: unknown[] = [];

  constructor(readonly url: string) {
    sockets.push(this);
  }

  send(data: unknown): void {
    this.sent.push(data);
  }

  close(): void {
    this.readyState = 3;
  }

  open(): void {
    this.onopen?.();
  }

  closeFromServer(): void {
    this.readyState = 3;
    this.onclose?.();
  }

  receive(data: Uint8Array): void {
    this.onmessage?.({ data: data.buffer.slice(data.byteOffset, data.byteOffset + data.byteLength) });
  }
}

globalThis.WebSocket = FakeSocket as unknown as typeof WebSocket;

export function sessionBodyFor(sourceId: string, sessionId: string, text: string): Uint8Array {
  return new TextEncoder().encode(JSON.stringify({
    source_id: sourceId,
    session: { id: sessionId, provider: "codex", status: "ready" },
    transcript: [{ role: "assistant", text }],
    appended_text: "",
    latest_activity_unix_ms: 200
  }));
}

export function snapshotEnvelope(input: {
  messageId: string;
  viewKind: string;
  domain: string;
  entityIds?: string[];
  sources: Array<{ sourceId: string; sourceEpoch: string; sourceSeq: bigint }>;
  body: Uint8Array;
  gatewaySeq?: bigint;
  gatewayEpoch?: string;
  gatewayStartedAtUnixNs?: bigint;
  sourceId?: string;
  subscriptionId?: string;
  requestId?: string;
  notFound?: boolean;
  schemaVersion?: number;
  operation?: ViewOperation;
  domains?: string[];
}) {
  const subscriptionId = input.subscriptionId ?? `subscription-${input.viewKind}`;
  return create(RealtimeEnvelopeSchema, {
    protocolVersion: 1,
    messageId: input.messageId,
    messageKind: MessageKind.SNAPSHOT,
    lane: Lane.STATE,
    gatewaySeq: 0n,
    payload: {
      case: "snapshot",
      value: create(SnapshotSchema, {
        viewKind: input.viewKind,
        subscriptionId,
        requestId: input.requestId ?? currentSubscriptionRequestId(subscriptionId),
        notFound: input.notFound ?? false,
        schemaVersion: input.schemaVersion ?? 1,
        operation: input.operation ?? ViewOperation.REPLACE,
        cursor: {
          gatewaySeq: input.gatewaySeq ?? 1n,
          gatewayEpoch: input.gatewayEpoch ?? "gateway-1",
          gatewayStartedAtUnixNs: input.gatewayStartedAtUnixNs ?? 100n,
          sources: input.sources
        },
        coverage: create(ViewCoverageSchema, {
          domains: input.domains ?? [input.domain],
          entityIds: input.entityIds ?? [],
          authoritative: true
        }),
        body: input.body
      })
    }
  });
}

export function currentSubscriptionRequestId(subscriptionId: string): string {
  return [...posted].reverse().find((message) =>
    message.type === "subscription-state" && message.subscription.subscriptionId === subscriptionId
  )?.type === "subscription-state"
    ? ([...posted].reverse().find((message) =>
        message.type === "subscription-state" && message.subscription.subscriptionId === subscriptionId
      ) as Extract<WorkerOutbound, { type: "subscription-state" }>).subscription.requestId ?? ""
    : "";
}

export function patchEnvelope(input: {
  messageId: string;
  gatewaySeq: bigint;
  sourceSeq: bigint;
  sourceEpoch?: string;
  viewKind: string;
  domain: string;
  entityId: string;
  operation: ViewOperation;
  body: Uint8Array;
  gatewayEpoch?: string;
  gatewayStartedAtUnixNs?: bigint;
}) {
  return create(RealtimeEnvelopeSchema, {
    protocolVersion: 1,
    messageId: input.messageId,
    messageKind: MessageKind.PATCH,
    lane: Lane.STATE,
    gatewaySeq: input.gatewaySeq,
    payload: {
      case: "patch",
      value: create(PatchSchema, {
        viewKind: input.viewKind,
        schemaVersion: 1,
        operation: input.operation,
        entity: create(EntityRefSchema, { entityId: input.entityId }),
        cursor: {
          gatewaySeq: input.gatewaySeq,
          gatewayEpoch: input.gatewayEpoch ?? "gateway-1",
          gatewayStartedAtUnixNs: input.gatewayStartedAtUnixNs ?? 100n,
          sources: [{
            sourceId: input.sourceId ?? "source-1",
            sourceEpoch: input.sourceEpoch ?? "epoch-1",
            sourceSeq: input.sourceSeq
          }]
        },
        coverage: create(ViewCoverageSchema, {
          domains: [input.domain],
          entityIds: [input.entityId],
          authoritative: true
        }),
        body: input.body
      })
    }
  });
}

export function sourceResyncEnvelope(input: {
  messageId: string;
  gatewaySeq: bigint;
  sourceEpoch: string;
  sourceSeq: bigint;
  body: Uint8Array;
  gatewayEpoch: string;
  gatewayStartedAtUnixNs: bigint;
  sourceId?: string;
}) {
  const domains = [
    "fleet_rows", "sessions", "session_details", "teams", "team_workspaces",
    "approvals", "processes", "worktrees", "sources"
  ];
  return create(RealtimeEnvelopeSchema, {
    protocolVersion: 1,
    messageId: input.messageId,
    messageKind: MessageKind.SOURCE_SNAPSHOT_RESYNC,
    lane: Lane.CRITICAL,
    gatewaySeq: input.gatewaySeq,
    payload: {
      case: "sourceSnapshotResync",
      value: create(SourceSnapshotResyncSchema, {
        sourceId: input.sourceId ?? "source-1",
        reason: "tower restart",
        schemaVersion: 1,
        cursor: {
          gatewaySeq: input.gatewaySeq,
          gatewayEpoch: input.gatewayEpoch,
          gatewayStartedAtUnixNs: input.gatewayStartedAtUnixNs,
          sources: [{
            sourceId: input.sourceId ?? "source-1",
            sourceEpoch: input.sourceEpoch,
            sourceSeq: input.sourceSeq
          }]
        },
        coverage: create(ViewCoverageSchema, {
          domains,
          authoritative: true
        }),
        body: input.body
      })
    }
  });
}

export function helloEnvelope(
  gatewayEpoch: string,
  gatewayStartedAtUnixNs: bigint,
  heartbeatIntervalMs = 0
) {
  return create(RealtimeEnvelopeSchema, {
    protocolVersion: 1,
    messageId: `hello-${gatewayEpoch}`,
    messageKind: MessageKind.HELLO,
    lane: Lane.CRITICAL,
    payload: {
      case: "hello",
      value: create(HelloSchema, {
        connectionId: `connection-${gatewayEpoch}`,
        protocolVersion: 1,
        resumeSupported: true,
        heartbeatIntervalMs,
        gatewayEpoch,
        gatewayStartedAtUnixNs
      })
    }
  });
}

export function pongEnvelope(messageId: string, gatewaySeq = 0n) {
  return create(RealtimeEnvelopeSchema, {
    protocolVersion: 1,
    messageId,
    messageKind: MessageKind.PONG,
    lane: Lane.CRITICAL,
    gatewaySeq,
    sourceId: gatewaySeq > 0n ? "malformed-control-authority" : "",
    sourceEpoch: gatewaySeq > 0n ? "wrong-control-epoch" : "",
    sourceSeq: gatewaySeq,
    payload: { case: "pong", value: create(PongSchema, {
      clientTimeUnixMs: gatewaySeq,
      serverTimeUnixMs: gatewaySeq
    }) }
  });
}


export function waitForPatchFlush(): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, 25));
}
