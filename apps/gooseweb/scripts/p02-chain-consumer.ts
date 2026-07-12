import assert from "node:assert/strict";
import { fromBinary } from "@bufbuild/protobuf";
import { RealtimeEnvelopeSchema } from "../src/gen/goosetower/v1/realtime_pb";
import type { WorkerOutbound } from "../app/realtime/types";
import { applyRealtimeWorkerOutput } from "../app/realtime/client";
import { getGoosewebSnapshot } from "../app/stores/gooseweb-store";
import { RealtimeWorkerCore } from "../app/realtime/worker/realtime-command-core";
import { observeFrame, observeStore } from "./support/p02-observers";

const encoded = process.argv[2];
if (!encoded) throw new Error("actual gateway frame argument is required");
const bytes = Uint8Array.from(Buffer.from(encoded, "base64"));
const envelope = fromBinary(RealtimeEnvelopeSchema, bytes);
const posted: WorkerOutbound[] = [];

class ChainSocket {
  static readonly OPEN = 1;
  readyState = ChainSocket.OPEN;
  binaryType = "";
  bufferedAmount = 0;
  onopen: (() => void) | null = null;
  onmessage: ((event: { data: unknown }) => void) | null = null;
  onerror: (() => void) | null = null;
  onclose: (() => void) | null = null;
  send(): void {}
  close(): void { this.readyState = 3; }
  constructor(readonly url: string) { queueMicrotask(() => this.onopen?.()); }
  receive(frame: Uint8Array): void {
    const copy = frame.slice();
    this.onmessage?.({ data: copy.buffer });
  }
}

let socket: ChainSocket | undefined;
globalThis.WebSocket = class extends ChainSocket {
  constructor(url: string) { super(url); socket = this; }
} as unknown as typeof WebSocket;

const core = new RealtimeWorkerCore((message) => {
  posted.push(message);
  applyRealtimeWorkerOutput(message);
});
await core.handleMessage({ type: "connect", goosetowerUrl: "ws://p02.invalid/v1/realtime", ticket: "redacted-test-ticket" });
await new Promise((resolve) => setTimeout(resolve, 0));
socket?.receive(bytes);
await new Promise((resolve) => setTimeout(resolve, 25));

const snapshot = getGoosewebSnapshot();
const detail = snapshot.entities.sessionDetails["p02-session-001"];
assert.ok(detail, "actual Worker/store path must materialize the source session detail");
assert.equal(detail.appendedText, "P02 deterministic terminal");
assert.equal(posted.some((message) => message.type === "state" && Boolean(message.patch.entities?.sessionDetails)), true);
const output = {
  source_id: envelope.sourceId || "p02-source",
  session_id: detail.sessionId,
  visible_text: detail.appendedText,
  frame: observeFrame(envelope),
  store: observeStore(snapshot)
};
await core.handleMessage({ type: "disconnect" });
process.stdout.write(`${JSON.stringify(output)}\n`);
