import { createHash } from "node:crypto";
import type { RealtimeEnvelope } from "../../src/gen/goosetower/v1/realtime_pb";
import type { GoosewebSnapshot } from "../../app/realtime/types";

const SECRET_KEY = /authorization|bearer|token|ticket|password|credential|cookie|csrf|secret|raw.?image|image.?data/i;
const SECRET_VALUE = /bearer\s+\S+|(?:token|ticket|password|cookie|csrf)=[^&\s]+|data:image\/[^;]+;base64,/i;

export type ObserverLayer =
  | "fake/runtime"
  | "goosetower/materialized"
  | "goosetower/frame"
  | "gooseweb/worker-store";

export type LayerEvidence = {
  readonly layer: ObserverLayer;
  readonly available: boolean;
  readonly expectedDigest?: string;
  readonly actualDigest?: string;
};

export function redactedObserver<T>(value: T): unknown {
  return redact(value, new WeakSet<object>());
}

export function observeFrame(envelope: RealtimeEnvelope): unknown {
  return redactedObserver({
    protocolVersion: envelope.protocolVersion,
    messageId: envelope.messageId,
    messageKind: envelope.messageKind,
    lane: envelope.lane,
    gatewaySeq: envelope.gatewaySeq.toString(),
    sourceId: envelope.sourceId,
    sourceEpoch: envelope.sourceEpoch,
    sourceSeq: envelope.sourceSeq.toString(),
    payloadCase: envelope.payload.case,
    payload: envelope.payload.value
  });
}

export function observeStore(snapshot: GoosewebSnapshot): unknown {
  return redactedObserver({
    connection: snapshot.connection,
    cursor: {
      gatewaySeq: snapshot.cursor.gatewaySeq.toString(),
      sourceCursors: Object.fromEntries(
        Object.entries(snapshot.cursor.sourceCursors).map(([id, cursor]) => [id, {
          sourceId: cursor.sourceId,
          sourceEpoch: cursor.sourceEpoch,
          sourceSeq: cursor.sourceSeq.toString()
        }])
      )
    },
    entities: snapshot.entities,
    subscriptions: snapshot.subscriptions,
    pendingCommands: snapshot.pendingCommands,
    staleSources: snapshot.staleSources,
    lastError: snapshot.lastError
  });
}

export function stableDigest(value: unknown): string {
  return createHash("sha256").update(stableJson(redactedObserver(value))).digest("hex");
}

export function firstDivergentLayer(evidence: readonly LayerEvidence[]): ObserverLayer | undefined {
  const order: readonly ObserverLayer[] = [
    "fake/runtime",
    "goosetower/materialized",
    "goosetower/frame",
    "gooseweb/worker-store"
  ];
  if (evidence.length !== order.length) {
    throw new Error("observer chain must contain exactly four layers");
  }
  for (let index = 0; index < order.length; index += 1) {
    const item = evidence[index];
    if (!item || item.layer !== order[index]) {
      throw new Error(`observer chain skipped or reordered ${order[index]}`);
    }
    if (!item.available) {
      if (item.expectedDigest || item.actualDigest) {
        throw new Error(`unavailable evidence was inferred for ${item.layer}`);
      }
      throw new Error(`observer evidence unavailable for ${item.layer}`);
    }
    if (!item.expectedDigest || !item.actualDigest) {
      throw new Error(`observer digest missing for ${item.layer}`);
    }
    if (item.expectedDigest !== item.actualDigest) {
      return item.layer;
    }
  }
  return undefined;
}

function redact(value: unknown, seen: WeakSet<object>): unknown {
  if (typeof value === "bigint") return value.toString();
  if (typeof value === "string") return SECRET_VALUE.test(value) ? "[redacted]" : value;
  if (!value || typeof value !== "object") return value;
  if (seen.has(value)) return "[circular omitted]";
  seen.add(value);
  if (value instanceof Uint8Array || value instanceof ArrayBuffer) return "[binary omitted]";
  if (Array.isArray(value)) return value.slice(0, 500).map((item) => redact(item, seen));
  return Object.fromEntries(
    Object.entries(value).slice(0, 500).map(([key, item]) => [
      key,
      SECRET_KEY.test(key) ? "[redacted]" : redact(item, seen)
    ])
  );
}

function stableJson(value: unknown): string {
  if (Array.isArray(value)) return `[${value.map(stableJson).join(",")}]`;
  if (value && typeof value === "object") {
    return `{${Object.entries(value).sort(([a], [b]) => a.localeCompare(b)).map(([key, item]) => `${JSON.stringify(key)}:${stableJson(item)}`).join(",")}}`;
  }
  return JSON.stringify(value);
}
