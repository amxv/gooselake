import assert from "node:assert/strict";
import { create, fromBinary, toBinary } from "@bufbuild/protobuf";
import { Lane, MessageKind, ErrorDetailSchema, SourceCursorSchema } from "../src/gen/goosetower/v1/common_pb";
import {
  ConnectionDegradedSchema, RealtimeEnvelopeSchema, SourceGapDetectedSchema,
  SourceGapFilledSchema
} from "../src/gen/goosetower/v1/realtime_pb";
import {
  CommandAcceptedSchema, CommandDuplicateSchema, CommandRejectedSchema
} from "../src/gen/goosetower/v1/commands_pb";
import type { WorkerOutbound } from "../app/realtime/types";
import { RealtimeWorkerCore } from "../app/realtime/worker/realtime-command-core";
import {
  getGoosewebSnapshot, resetGoosewebStoreForTests, updateGoosewebStore
} from "../app/stores/gooseweb-store";
import {
  helloEnvelope, pongEnvelope, snapshotEnvelope, sockets, waitForPatchFlush
} from "./realtime-worker-smoke-fixtures";

const heartbeatPosted: WorkerOutbound[] = [];
const heartbeatCore = new RealtimeWorkerCore((message) => {
  heartbeatPosted.push(message);
  if (message.type === "state") updateGoosewebStore(message.patch);
});
await heartbeatCore.handleMessage({
  type: "connect",
  goosetowerUrl: "ws://127.0.0.1:18090/v1/realtime",
  ticket: "heartbeat-control-regression"
});
const heartbeatSocket = sockets[4];
assert.ok(heartbeatSocket);
heartbeatSocket.open();
heartbeatSocket.receive(toBinary(RealtimeEnvelopeSchema, pongEnvelope("pre-hello-pong", 8_999n)));
await waitForPatchFlush();
assert.equal(getGoosewebSnapshot().connection, "connecting",
  "socket-open PONG cannot present an authenticated connected state before Hello");
assert.equal(heartbeatSocket.sent.some((bytes) => {
  if (!(bytes instanceof Uint8Array)) return false;
  const kind = fromBinary(RealtimeEnvelopeSchema, bytes).messageKind;
  return kind === MessageKind.RESUME || kind === MessageKind.SUBSCRIBE;
}), false, "startup resume/subscriptions must wait for authenticated Hello");
heartbeatSocket.receive(toBinary(
  RealtimeEnvelopeSchema,
  helloEnvelope("gateway-heartbeat", 500n, 10)
));
await heartbeatCore.handleMessage({
  type: "subscribe", subscriptionId: "heartbeat-board", viewKind: "board", filters: {}
});
const heartbeatRequestId = [...heartbeatPosted].reverse().find((message) =>
  message.type === "subscription-state" &&
  message.subscription.subscriptionId === "heartbeat-board"
);
assert.equal(heartbeatRequestId?.type, "subscription-state");
heartbeatSocket.receive(toBinary(RealtimeEnvelopeSchema, snapshotEnvelope({
  messageId: "heartbeat-board-snapshot",
  subscriptionId: "heartbeat-board",
  requestId: heartbeatRequestId.type === "subscription-state"
    ? heartbeatRequestId.subscription.requestId
    : "",
  gatewaySeq: 1n,
  gatewayEpoch: "gateway-heartbeat",
  gatewayStartedAtUnixNs: 500n,
  viewKind: "board",
  domain: "fleet_rows",
  sources: [{ sourceId: "heartbeat-source", sourceEpoch: "heartbeat-epoch", sourceSeq: 1n }],
  body: new TextEncoder().encode(JSON.stringify({ rows: [] }))
})));
await waitForPatchFlush();
const canonicalBeforeControls = structuredClone(getGoosewebSnapshot().cursor);
const entitiesBeforeControls = structuredClone(getGoosewebSnapshot().entities);
const coverageBeforeControls = structuredClone(getGoosewebSnapshot().loadedCoverage);

await new Promise((resolve) => setTimeout(resolve, 35));
const heartbeatPings = heartbeatSocket.sent.filter((bytes) =>
  bytes instanceof Uint8Array &&
  fromBinary(RealtimeEnvelopeSchema, bytes).messageKind === MessageKind.PING
);
assert.ok(heartbeatPings.length >= 2,
  "real heartbeat timers must emit across at least two configured intervals");
for (let index = 0; index < 2; index += 1) {
  heartbeatSocket.receive(toBinary(
    RealtimeEnvelopeSchema,
    pongEnvelope(`authorityless-pong-${index}`, 9_000n + BigInt(index))
  ));
}
await waitForPatchFlush();
assert.equal(getGoosewebSnapshot().connection, "connected");
assert.notEqual(getGoosewebSnapshot().lastError?.split(":", 1)[0], "gateway_generation_mismatch");
assert.deepEqual(getGoosewebSnapshot().cursor, canonicalBeforeControls,
  "authority-less PONG must not advance or regress canonical view authority");
assert.deepEqual(getGoosewebSnapshot().entities, entitiesBeforeControls);
assert.deepEqual(getGoosewebSnapshot().loadedCoverage, coverageBeforeControls);
const sentBeforeHeartbeatMutation = heartbeatSocket.sent.length;
await heartbeatCore.handleMessage({
  type: "command",
  command: {
    commandId: "heartbeat-create-session",
    idempotencyKey: "heartbeat-create-session",
    target: { scope: "source", scopeId: "heartbeat-source", entityId: "source:heartbeat-source" },
    createdAtClientUnixMs: BigInt(Date.now()),
    payload: { case: "createSession", value: {
      provider: "codex", model: "gpt-5.4", cwd: "/tmp",
      title: "Heartbeat mutation remains enabled", permissionMode: "", metadata: {}
    } }
  }
});
assert.ok(heartbeatSocket.sent.length > sentBeforeHeartbeatMutation,
  "first-run mutation command must remain dispatchable beyond repeated heartbeat intervals");
const heartbeatMutationBytes = heartbeatSocket.sent.at(-1);
assert.ok(heartbeatMutationBytes instanceof Uint8Array);
const heartbeatMutation = fromBinary(RealtimeEnvelopeSchema, heartbeatMutationBytes);
assert.equal(heartbeatMutation.payload.case, "command");
assert.equal(heartbeatMutation.payload.value.payload.case, "createSession");

const controlFrames = [
  create(RealtimeEnvelopeSchema, {
    messageId: "control-command-accepted", messageKind: MessageKind.COMMAND_ACCEPTED,
    gatewaySeq: 9_100n, sourceId: "wrong-control-source", sourceSeq: 9_100n,
    payload: { case: "commandAccepted", value: create(CommandAcceptedSchema, {
      commandId: "control-accepted", gatewaySeq: 9_100n
    }) }
  }),
  create(RealtimeEnvelopeSchema, {
    messageId: "control-command-rejected", messageKind: MessageKind.COMMAND_REJECTED,
    gatewaySeq: 9_101n,
    payload: { case: "commandRejected", value: create(CommandRejectedSchema, {
      commandId: "control-rejected",
      error: create(ErrorDetailSchema, { code: "upstream_rejected", message: "no", retryable: false })
    }) }
  }),
  create(RealtimeEnvelopeSchema, {
    messageId: "control-command-duplicate", messageKind: MessageKind.COMMAND_DUPLICATE,
    gatewaySeq: 9_102n,
    payload: { case: "commandDuplicate", value: create(CommandDuplicateSchema, {
      commandId: "control-duplicate", originalCommandId: "original"
    }) }
  }),
  create(RealtimeEnvelopeSchema, {
    messageId: "control-degraded", messageKind: MessageKind.CONNECTION_DEGRADED,
    gatewaySeq: 9_103n,
    payload: { case: "connectionDegraded", value: create(ConnectionDegradedSchema, {
      reason: "control degradation"
    }) }
  }),
  create(RealtimeEnvelopeSchema, {
    messageId: "control-gap-detected", messageKind: MessageKind.SOURCE_GAP_DETECTED,
    gatewaySeq: 9_104n,
    payload: { case: "sourceGapDetected", value: create(SourceGapDetectedSchema, {
      lastSeen: create(SourceCursorSchema, {
        sourceId: "heartbeat-source", sourceEpoch: "heartbeat-epoch", sourceSeq: 1n
      }),
      nextAvailable: create(SourceCursorSchema, {
        sourceId: "heartbeat-source", sourceEpoch: "heartbeat-epoch", sourceSeq: 3n
      })
    }) }
  }),
  create(RealtimeEnvelopeSchema, {
    messageId: "control-gap-filled", messageKind: MessageKind.SOURCE_GAP_FILLED,
    gatewaySeq: 9_105n,
    payload: { case: "sourceGapFilled", value: create(SourceGapFilledSchema, {
      cursor: create(SourceCursorSchema, {
        sourceId: "heartbeat-source", sourceEpoch: "heartbeat-epoch", sourceSeq: 3n
      })
    }) }
  }),
  create(RealtimeEnvelopeSchema, {
    messageId: "control-error", messageKind: MessageKind.ERROR,
    gatewaySeq: 9_106n,
    payload: { case: "error", value: create(ErrorDetailSchema, {
      code: "control_error", message: "control error", retryable: true
    }) }
  })
];
const expectedControlConnections = [
  "connected", "connected", "connected", "degraded", "stale", "replaying", "replaying"
] as const;
for (const [index, frame] of controlFrames.entries()) {
  heartbeatSocket.receive(toBinary(RealtimeEnvelopeSchema, frame));
  await waitForPatchFlush();
  assert.deepEqual(getGoosewebSnapshot().cursor, canonicalBeforeControls,
    `${MessageKind[frame.messageKind]} must not mutate canonical view authority`);
  assert.equal(getGoosewebSnapshot().connection, expectedControlConnections[index]);
  heartbeatSocket.receive(toBinary(
    RealtimeEnvelopeSchema,
    pongEnvelope(`pong-after-${MessageKind[frame.messageKind]}-${index}`, 9_200n + BigInt(index))
  ));
  await waitForPatchFlush();
  assert.equal(getGoosewebSnapshot().connection, expectedControlConnections[index],
    `PONG must not overwrite ${expectedControlConnections[index]} safety state`);
  assert.deepEqual(getGoosewebSnapshot().cursor, canonicalBeforeControls);
}
assert.equal(heartbeatPosted.some((message) =>
  message.type === "command-state" && message.command.commandId === "control-accepted" &&
  message.command.status === "accepted"
), true);
assert.equal(heartbeatPosted.some((message) =>
  message.type === "command-state" && message.command.commandId === "control-rejected" &&
  message.command.status === "rejected"
), true);
assert.equal(heartbeatPosted.some((message) =>
  message.type === "command-state" && message.command.commandId === "control-duplicate" &&
  message.command.status === "duplicate"
), true);
assert.equal(heartbeatPosted.some((message) =>
  message.type === "error" && message.message === "control error"
), true);
await heartbeatCore.handleMessage({ type: "disconnect" });
