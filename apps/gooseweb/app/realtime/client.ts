import type { Command } from "../../src/gen/goosetower/v1/commands_pb";
import { goosewebConfig } from "./config";
import type { WorkerInbound, WorkerOutbound } from "./types";
import {
  setPendingCommand,
  setSubscription,
  updateGoosewebStore
} from "../stores/gooseweb-store";

let worker: Worker | undefined;

export function ensureRealtimeWorker(): Worker | undefined {
  if (!goosewebConfig.flags.workerRealtime || typeof window === "undefined") {
    return undefined;
  }

  if (worker) {
    return worker;
  }

  worker = new Worker(new URL("./worker/realtime-worker.ts", import.meta.url), {
    type: "module"
  });
  worker.onmessage = (event: MessageEvent<WorkerOutbound>) => {
    const message = event.data;
    switch (message.type) {
      case "state":
        updateGoosewebStore(message.patch);
        break;
      case "command-state":
        setPendingCommand(message.command);
        break;
      case "subscription-state":
        setSubscription(message.subscription);
        break;
      case "error":
        updateGoosewebStore({
          lastError: message.message,
          connection: message.retryable ? "degraded" : "offline"
        });
        break;
    }
  };

  return worker;
}

export function connectRealtime(ticket: string): void {
  postRealtimeMessage({
    type: "connect",
    ticket,
    goosetowerUrl: goosewebConfig.goosetowerUrl
  });
}

export function disconnectRealtime(): void {
  postRealtimeMessage({ type: "disconnect" });
}

export function subscribeRealtime(
  subscriptionId: string,
  viewKind: string,
  filters: Readonly<Record<string, string>> = {}
): void {
  postRealtimeMessage({
    type: "subscribe",
    subscriptionId,
    viewKind,
    filters
  });
}

export function unsubscribeRealtime(subscriptionId: string): void {
  postRealtimeMessage({ type: "unsubscribe", subscriptionId });
}

export function sendRealtimeCommand(
  command: Command,
  idempotencyKey?: string
): void {
  postRealtimeMessage({ type: "command", command, idempotencyKey });
}

export function refreshRealtimeAuth(ticket: string): void {
  postRealtimeMessage({ type: "auth-refresh", ticket });
}

function postRealtimeMessage(message: WorkerInbound): void {
  ensureRealtimeWorker()?.postMessage(message);
}
