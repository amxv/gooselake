import assert from "node:assert/strict";
import { fromBinary } from "@bufbuild/protobuf";
import { RealtimeEnvelopeSchema } from "../src/gen/goosetower/v1/realtime_pb";
import type { WorkerOutbound } from "../app/realtime/types";
import { RealtimeWorkerCore } from "../app/realtime/worker/realtime-core";

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

console.log("realtime worker socket ownership smoke fixture passed");

function waitForPatchFlush(): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, 25));
}
