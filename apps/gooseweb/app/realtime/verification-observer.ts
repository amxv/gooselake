import type { RealtimeEnvelope } from "../../src/gen/goosetower/v1/realtime_pb";
import type { GoosewebSnapshot, WorkerOutbound } from "./types";

const MAX_ENTRIES = 128;
const MAX_STRING = 2_048;
const SECRET_KEY = /authorization|bearer|token|ticket|password|credential|cookie|csrf|secret|raw.?image|image.?data/i;
const SECRET_VALUE = /bearer\s+\S+|(?:token|ticket|password|cookie|csrf)=[^&\s]+|data:image\/[^;]+;base64,/i;

export type GoosewebVerificationCapture = {
  readonly schema_revision: "gooseweb-served-observer/v1";
  readonly frame_count: number;
  readonly worker_output_count: number;
  readonly frames: readonly unknown[];
  readonly worker_outputs: readonly unknown[];
  readonly store: unknown;
};

export type GoosewebVerificationObserver = {
  recordFrame(envelope: RealtimeEnvelope): void;
  recordWorkerOutput(output: WorkerOutbound, store: GoosewebSnapshot): void;
  capture(store: GoosewebSnapshot): GoosewebVerificationCapture;
};

declare global {
  interface Window {
    __GOOSEWEB_VERIFICATION_OBSERVER__?: {
      capture(): GoosewebVerificationCapture;
    };
  }
}

export function installGoosewebVerificationObserver(
  currentStore: () => GoosewebSnapshot
): GoosewebVerificationObserver | undefined {
  if (!import.meta.env.DEV || typeof window === "undefined") {
    return undefined;
  }
  const observer = createGoosewebVerificationObserver(currentStore);
  window.__GOOSEWEB_VERIFICATION_OBSERVER__ = {
    capture: () => observer.capture(currentStore())
  };
  return observer;
}

export function createGoosewebVerificationObserver(
  currentStore: () => GoosewebSnapshot
): GoosewebVerificationObserver {
  const frames: unknown[] = [];
  const workerOutputs: unknown[] = [];
  let frameCount = 0;
  let workerOutputCount = 0;
  const observer: GoosewebVerificationObserver = {
    recordFrame(envelope) {
      frameCount += 1;
      boundedPush(frames, observeFrame(frameCount, envelope));
    },
    recordWorkerOutput(output, store) {
      workerOutputCount += 1;
      boundedPush(workerOutputs, redact({ capture_index: workerOutputCount, output }));
      void store;
    },
    capture(store) {
      return {
        schema_revision: "gooseweb-served-observer/v1",
        frame_count: frameCount,
        worker_output_count: workerOutputCount,
        frames: [...frames],
        worker_outputs: [...workerOutputs],
        store: observeStore(store)
      };
    }
  };
  return observer;
}

function observeFrame(captureIndex: number, envelope: RealtimeEnvelope): unknown {
  const payload = envelope.payload;
  let viewKind: string | undefined;
  let entityId: string | undefined;
  let body: unknown;
  let cursor: unknown;
  if (payload.case === "snapshot" || payload.case === "patch") {
    viewKind = payload.value.viewKind;
    cursor = payload.value.cursor;
    if (payload.case === "patch") entityId = payload.value.entity?.entityId;
    body = decodeJsonBody(payload.value.body);
  }
  return redact({
    capture_index: captureIndex,
    gateway_seq: envelope.gatewaySeq,
    message_kind: envelope.messageKind,
    payload_kind: payload.case,
    view_kind: viewKind,
    entity_id: entityId,
    cursor,
    body
  });
}

function observeStore(store: GoosewebSnapshot): unknown {
  return redact({
    connection: store.connection,
    cursor: store.cursor,
    entities: {
      sessions: store.entities.sessions,
      sessionDetails: store.entities.sessionDetails,
      teams: store.entities.teams,
      teamWorkspaces: store.entities.teamWorkspaces,
      sources: store.entities.sources
    },
    subscriptions: store.subscriptions,
    pendingCommands: store.pendingCommands,
    staleSources: store.staleSources,
    lastError: store.lastError
  });
}

function decodeJsonBody(body: Uint8Array): unknown {
  if (body.byteLength === 0) return undefined;
  try {
    return JSON.parse(new TextDecoder().decode(body));
  } catch {
    return "[binary omitted]";
  }
}

function redact(value: unknown, seen = new WeakSet<object>()): unknown {
  if (typeof value === "bigint") return value.toString();
  if (typeof value === "string") {
    if (SECRET_VALUE.test(value)) return "[redacted]";
    return value.slice(0, MAX_STRING);
  }
  if (!value || typeof value !== "object") return value;
  if (seen.has(value)) return "[circular omitted]";
  seen.add(value);
  if (value instanceof Uint8Array || value instanceof ArrayBuffer) return "[binary omitted]";
  if (Array.isArray(value)) return value.slice(0, MAX_ENTRIES).map((item) => redact(item, seen));
  return Object.fromEntries(
    Object.entries(value).slice(0, MAX_ENTRIES).map(([key, item]) => [
      key,
      SECRET_KEY.test(key) ? "[redacted]" : redact(item, seen)
    ])
  );
}

function boundedPush(values: unknown[], value: unknown): void {
  if (values.length === MAX_ENTRIES) values.shift();
  values.push(value);
}
