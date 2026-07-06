export type GoosewebFeatureFlags = {
  readonly workerRealtime: boolean;
  readonly devTicketRoute: boolean;
  readonly debugFrames: boolean;
};

export type GoosewebRuntimeConfig = {
  readonly goosetowerUrl: string;
  readonly devTicketRoute: string;
  readonly pastedDevTicket: string;
  readonly flags: GoosewebFeatureFlags;
};

const env = import.meta.env;

export const goosewebConfig: GoosewebRuntimeConfig = {
  goosetowerUrl:
    env.VITE_GOOSETOWER_URL ?? "ws://localhost:8787/v1/realtime",
  devTicketRoute: env.VITE_GOOSEWEB_DEV_TICKET_ROUTE ?? "/api/dev-ticket",
  pastedDevTicket: env.VITE_GOOSEWEB_DEV_TICKET ?? "",
  flags: {
    workerRealtime: env.VITE_GOOSEWEB_WORKER_REALTIME !== "false",
    devTicketRoute: env.VITE_GOOSEWEB_DEV_TICKET_ROUTE_ENABLED === "true",
    debugFrames: env.VITE_GOOSEWEB_DEBUG_FRAMES === "true"
  }
};

export function realtimeUrlWithTicket(baseUrl: string, ticket: string): string {
  const url = new URL(baseUrl);
  url.searchParams.set("ticket", ticket);
  return url.toString();
}
