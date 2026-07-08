import assert from "node:assert/strict";
import { fromBinary } from "@bufbuild/protobuf";
import { RealtimeEnvelopeSchema } from "../src/gen/goosetower/v1/realtime_pb";
import type { WorkerOutbound } from "../app/realtime/types";
import { RealtimeWorkerCore } from "../app/realtime/worker/realtime-command-core";

const sockets: FakeSocket[] = [];
const posted: WorkerOutbound[] = [];

class FakeSocket {
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
}

globalThis.WebSocket = FakeSocket as unknown as typeof WebSocket;

const core = new RealtimeWorkerCore((message) => posted.push(message));
await core.handleMessage({
  type: "connect",
  goosetowerUrl: "ws://127.0.0.1:18090/v1/realtime",
  ticket: "first"
});
assert.equal(sockets.length, 1);
sockets[0]?.open();

await core.handleMessage({
  type: "connect",
  goosetowerUrl: "ws://127.0.0.1:18090/v1/realtime",
  ticket: "second"
});
assert.equal(sockets.length, 2);
sockets[1]?.open();
sockets[0]?.closeFromServer();

await waitForPatchFlush();
assert.equal(
  posted.some(
    (message) => message.type === "state" && message.patch.connection === "offline"
  ),
  false
);

sockets[1]?.closeFromServer();
await waitForPatchFlush();
assert.equal(
  posted.some(
    (message) => message.type === "state" && message.patch.connection === "offline"
  ),
  true
);

await core.handleMessage({
  type: "command",
  command: {
    commandId: "cmd_without_socket",
    idempotencyKey: "cmd_without_socket",
    target: {
      scope: "source",
      scopeId: "local",
      entityId: "source:local"
    },
    createdAtClientUnixMs: BigInt(Date.now()),
    payload: {
      case: "createSession",
      value: {
        provider: "codex",
        model: "gpt-5.4",
        cwd: "/tmp",
        title: "Socket unavailable test",
        permissionMode: "",
        metadata: {}
      }
    }
  }
});

assert.equal(
  posted.some(
    (message) =>
      message.type === "command-state" &&
      message.command.commandId === "cmd_without_socket" &&
      message.command.status === "rejected" &&
      message.command.errorCode === "socket_unavailable"
  ),
  true
);

await core.handleMessage({
  type: "connect",
  goosetowerUrl: "ws://127.0.0.1:18090/v1/realtime",
  ticket: "third"
});
assert.equal(sockets.length, 3);
sockets[2]?.open();
await waitForPatchFlush();
assert.equal(
  posted.some(
    (message) => message.type === "state" && message.patch.connection === "connected"
  ),
  true
);
const sentBeforeCommand = sockets[2]?.sent.length ?? 0;

await core.handleMessage({
  type: "command",
  command: {
    commandId: "cmd_with_socket",
    idempotencyKey: "cmd_with_socket",
    target: {
      scope: "source",
      scopeId: "local",
      entityId: "source:local"
    },
    createdAtClientUnixMs: BigInt(Date.now()),
    payload: {
      case: "createSession",
      value: {
        provider: "codex",
        model: "gpt-5.4",
        cwd: "/tmp",
        title: "Socket write test",
        permissionMode: "",
        metadata: {}
      }
    }
  }
});

assert.equal((sockets[2]?.sent.length ?? 0) > sentBeforeCommand, true);
const sentCommandFrame = sockets[2]?.sent.at(-1);
assert.ok(sentCommandFrame instanceof Uint8Array);
const sentCommandEnvelope = fromBinary(RealtimeEnvelopeSchema, sentCommandFrame);
assert.equal(sentCommandEnvelope.payload.case, "command");
assert.equal(sentCommandEnvelope.payload.value.payload.case, "createSession");
assert.equal(
  posted.some(
    (message) =>
      message.type === "command-state" &&
      message.command.commandId === "cmd_with_socket" &&
      message.command.status === "sent"
  ),
  true
);

const sentBeforeFallbackCommand = sockets[2]?.sent.length ?? 0;
await core.handleMessage({
  type: "command",
  command: {
    commandId: "cmd_with_fallback",
    idempotencyKey: "cmd_with_fallback",
    createdAtClientUnixMs: BigInt(Date.now()),
    fallbackCreateSession: {
      provider: "codex",
      model: "gpt-5.4",
      cwd: "/tmp",
      title: "Fallback payload test",
      permissionMode: "",
      metadata: {}
    },
    target: {
      scope: "source",
      scopeId: "local",
      entityId: "source:local"
    }
  } as never
});
assert.equal((sockets[2]?.sent.length ?? 0) > sentBeforeFallbackCommand, true);
const fallbackCommandFrame = sockets[2]?.sent.at(-1);
assert.ok(fallbackCommandFrame instanceof Uint8Array);
const fallbackCommandEnvelope = fromBinary(
  RealtimeEnvelopeSchema,
  fallbackCommandFrame
);
assert.equal(fallbackCommandEnvelope.payload.case, "command");
assert.equal(fallbackCommandEnvelope.payload.value.payload.case, "createSession");

const sentBeforeImageTurnCommand = sockets[2]?.sent.length ?? 0;
await core.handleMessage({
  type: "command",
  command: {
    commandId: "cmd_image_turn",
    idempotencyKey: "cmd_image_turn",
    target: {
      scope: "session",
      scopeId: "session_1",
      entityId: "session_1"
    },
    createdAtClientUnixMs: BigInt(Date.now()),
    payload: {
      case: "sendTurn",
      value: {
        sessionId: "session_1",
        text: "Inspect this image",
        input: [
          { type: "text", text: "Inspect this image" },
          {
            type: "image",
            mediaType: "image/png",
            data: "iVBORw0KGgo="
          }
        ]
      }
    }
  }
});
assert.equal((sockets[2]?.sent.length ?? 0) > sentBeforeImageTurnCommand, true);
const imageTurnFrame = sockets[2]?.sent.at(-1);
assert.ok(imageTurnFrame instanceof Uint8Array);
const imageTurnEnvelope = fromBinary(RealtimeEnvelopeSchema, imageTurnFrame);
assert.equal(imageTurnEnvelope.payload.case, "command");
assert.equal(imageTurnEnvelope.payload.value.payload.case, "sendTurn");
const imageTurnPayload = imageTurnEnvelope.payload.value.payload.value;
assert.equal(imageTurnPayload.input.length, 2);
assert.equal(imageTurnPayload.input[1]?.type, "image");
assert.equal(imageTurnPayload.input[1]?.mediaType, "image/png");
assert.equal(imageTurnPayload.input[1]?.data, "iVBORw0KGgo=");

const sentBeforeJoinCommand = sockets[2]?.sent.length ?? 0;
await core.handleMessage({
  type: "command",
  command: {
    commandId: "cmd_join_team_member",
    idempotencyKey: "cmd_join_team_member",
    target: {
      scope: "team",
      scopeId: "team_1",
      entityId: "team_1"
    },
    createdAtClientUnixMs: BigInt(Date.now()),
    payload: {
      case: "joinTeamMember",
      value: {
        teamId: "team_1",
        agentId: "session_2",
        title: "Second agent",
        addedBy: "session_1"
      }
    }
  }
});
assert.equal((sockets[2]?.sent.length ?? 0) > sentBeforeJoinCommand, true);
const joinCommandFrame = sockets[2]?.sent.at(-1);
assert.ok(joinCommandFrame instanceof Uint8Array);
const joinCommandEnvelope = fromBinary(RealtimeEnvelopeSchema, joinCommandFrame);
assert.equal(joinCommandEnvelope.payload.case, "command");
assert.equal(joinCommandEnvelope.payload.value.payload.case, "joinTeamMember");

sockets[2]?.closeFromServer();
await waitForPatchFlush();

console.log("realtime worker socket ownership smoke fixture passed");

function waitForPatchFlush(): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, 25));
}
