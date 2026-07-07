import { goosewebConfig } from "./config";
import type { CommandIntent, WorkerInbound, WorkerOutbound } from "./types";
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

type DevelopmentTicketRequest = {
  readonly allowed_origins?: readonly string[];
};

export function developmentTicketRequestBody(
  currentOrigin: string | undefined = currentBrowserOrigin(),
  configuredOrigins: readonly string[] = goosewebConfig.devTicketAllowedOrigins
): DevelopmentTicketRequest {
  const allowedOrigins = uniqueOrigins([currentOrigin, ...configuredOrigins]);
  if (allowedOrigins.length === 0) {
    return {};
  }
  return { allowed_origins: allowedOrigins };
}

export async function mintDevelopmentTicket(): Promise<string> {
  const response = await fetch(goosewebConfig.devTicketRoute, {
    method: "POST",
    headers: {
      "content-type": "application/json"
    },
    body: JSON.stringify(developmentTicketRequestBody())
  });
  if (!response.ok) {
    throw new Error(`Dev ticket request failed with ${response.status}`);
  }
  const payload = (await response.json()) as { ticket?: unknown };
  if (typeof payload.ticket !== "string" || !payload.ticket.trim()) {
    throw new Error("Dev ticket response did not include a ticket");
  }
  return payload.ticket;
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
  command: CommandIntent,
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

function currentBrowserOrigin(): string | undefined {
  if (typeof window === "undefined") {
    return undefined;
  }
  return window.location.origin;
}

function uniqueOrigins(origins: readonly (string | undefined)[]): readonly string[] {
  return [...new Set(origins.map((origin) => origin?.trim()).filter(isNonEmptyString))];
}

function isNonEmptyString(value: string | undefined): value is string {
  return typeof value === "string" && value.length > 0;
}
