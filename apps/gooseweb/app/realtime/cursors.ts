import { create } from "@bufbuild/protobuf";
import {
  type CursorVector,
  CursorVectorSchema,
  type SourceCursor,
  SourceCursorSchema
} from "../../src/gen/goosetower/v1/common_pb";
import type { CursorState, SourceCursorState } from "./types";

export const CURSOR_CACHE_NAMESPACE = "gooseweb-realtime-authority-v2";
const CURSOR_DB_NAME = CURSOR_CACHE_NAMESPACE;
const CURSOR_DB_VERSION = 1;
const CURSOR_STORE = "kv";
const CURSOR_STORAGE_KEY = "cursor.v2";
const CURSOR_SCHEMA = "gooseweb-cursor/v2";

type PersistedCursorState = {
  readonly schema: typeof CURSOR_SCHEMA;
  readonly gatewaySeq: string;
  readonly gatewayEpoch?: string;
  readonly gatewayStartedAtUnixNs?: string;
  readonly sourceCursors: Readonly<Record<string, PersistedSourceCursor>>;
};

type PersistedSourceCursor = {
  readonly sourceId: string;
  readonly sourceEpoch: string;
  readonly sourceSeq: string;
};

export const emptyCursorState: CursorState = {
  gatewaySeq: 0n,
  gatewayEpoch: "",
  gatewayStartedAtUnixNs: 0n,
  sourceCursors: {}
};

export async function loadCursorState(): Promise<CursorState> {
  const raw = await readCursorPayload();
  if (!raw) {
    return emptyCursorState;
  }

  try {
    return parsePersistedCursorState(raw);
  } catch {
    return emptyCursorState;
  }
}

export function parsePersistedCursorState(raw: string): CursorState {
  const parsed = JSON.parse(raw) as PersistedCursorState;
  if (parsed.schema !== CURSOR_SCHEMA || !parsed.sourceCursors ||
    typeof parsed.gatewaySeq !== "string") {
    throw new Error("incompatible realtime cursor cache namespace");
  }
  const sourceCursors: Record<string, SourceCursorState> = {};
  for (const [sourceId, cursor] of Object.entries(parsed.sourceCursors ?? {})) {
    const sourceSeq = BigInt(cursor.sourceSeq);
    if (cursor.sourceId !== sourceId || !sourceId || !cursor.sourceEpoch || sourceSeq <= 0n) {
      throw new Error("invalid persisted source cursor authority");
    }
    sourceCursors[sourceId] = {
      sourceId: cursor.sourceId,
      sourceEpoch: cursor.sourceEpoch,
      sourceSeq
    };
  }
  const gatewaySeq = BigInt(parsed.gatewaySeq);
  const gatewayStartedAtUnixNs = BigInt(parsed.gatewayStartedAtUnixNs ?? "0");
  const gatewayEpoch = parsed.gatewayEpoch ?? "";
  if (gatewaySeq < 0n || gatewayStartedAtUnixNs < 0n ||
    (gatewaySeq > 0n && (!gatewayEpoch || gatewayStartedAtUnixNs === 0n))) {
    throw new Error("invalid persisted gateway generation authority");
  }
  return { gatewaySeq, gatewayEpoch, gatewayStartedAtUnixNs, sourceCursors };
}

export async function persistCursorState(cursor: CursorState): Promise<void> {
  const persisted: PersistedCursorState = {
    schema: CURSOR_SCHEMA,
    gatewaySeq: cursor.gatewaySeq.toString(),
    gatewayEpoch: cursor.gatewayEpoch,
    gatewayStartedAtUnixNs: cursor.gatewayStartedAtUnixNs.toString(),
    sourceCursors: Object.fromEntries(
      Object.entries(cursor.sourceCursors).map(([sourceId, sourceCursor]) => [
        sourceId,
        {
          sourceId: sourceCursor.sourceId,
          sourceEpoch: sourceCursor.sourceEpoch,
          sourceSeq: sourceCursor.sourceSeq.toString()
        }
      ])
    )
  };

  await writeCursorPayload(JSON.stringify(persisted));
}

export function cursorStateToProto(cursor: CursorState): CursorVector {
  return create(CursorVectorSchema, {
    gatewaySeq: cursor.gatewaySeq,
    gatewayEpoch: cursor.gatewayEpoch,
    gatewayStartedAtUnixNs: cursor.gatewayStartedAtUnixNs,
    sources: Object.values(cursor.sourceCursors).map((sourceCursor) =>
      create(SourceCursorSchema, {
        sourceId: sourceCursor.sourceId,
        sourceEpoch: sourceCursor.sourceEpoch,
        sourceSeq: sourceCursor.sourceSeq
      })
    )
  });
}

export function cursorProtoToState(cursor: CursorVector | undefined): CursorState {
  if (!cursor) {
    return emptyCursorState;
  }

  const sourceCursors: Record<string, SourceCursorState> = {};
  for (const source of cursor.sources) {
    sourceCursors[source.sourceId] = sourceCursorProtoToState(source);
  }

  return {
    gatewaySeq: cursor.gatewaySeq,
    gatewayEpoch: cursor.gatewayEpoch,
    gatewayStartedAtUnixNs: cursor.gatewayStartedAtUnixNs,
    sourceCursors
  };
}

export function sourceCursorProtoToState(cursor: SourceCursor): SourceCursorState {
  return {
    sourceId: cursor.sourceId,
    sourceEpoch: cursor.sourceEpoch,
    sourceSeq: cursor.sourceSeq
  };
}

export function shouldApplyCursor(
  current: CursorState,
  nextGatewaySeq: bigint,
  nextSource: SourceCursorState | undefined
): boolean {
  if (nextGatewaySeq > 0n && nextGatewaySeq <= current.gatewaySeq) {
    return false;
  }

  if (!nextSource || nextSource.sourceSeq === 0n) {
    return true;
  }

  const currentSource = current.sourceCursors[nextSource.sourceId];
  if (!currentSource) {
    return true;
  }

  if (currentSource.sourceEpoch !== nextSource.sourceEpoch) {
    return true;
  }
  return nextSource.sourceSeq === currentSource.sourceSeq + 1n;
}

export function shouldApplyCursorVector(
  current: CursorState,
  nextGatewaySeq: bigint,
  nextSources: readonly SourceCursorState[],
  options: {
    readonly allowEqualSourceSeq?: boolean;
    readonly allowEpochChange?: boolean;
    readonly allowGatewayRegression?: boolean;
  } = {}
): boolean {
  if (
    !options.allowGatewayRegression &&
    nextGatewaySeq > 0n &&
    nextGatewaySeq <= current.gatewaySeq
  ) {
    return false;
  }
  if (!isValidCursorVector(nextSources)) return false;
  for (const source of nextSources) {
    const existing = current.sourceCursors[source.sourceId];
    if (
      existing &&
      existing.sourceEpoch !== source.sourceEpoch &&
      !options.allowEpochChange
    ) {
      return false;
    }
    if (
      existing?.sourceEpoch === source.sourceEpoch &&
      (source.sourceSeq < existing.sourceSeq ||
        (!options.allowEqualSourceSeq && source.sourceSeq === existing.sourceSeq))
    ) {
      return false;
    }
  }
  return true;
}

export function hasCursorEpochMismatch(
  current: CursorState,
  nextSources: readonly SourceCursorState[]
): boolean {
  return nextSources.some((source) => {
    const existing = current.sourceCursors[source.sourceId];
    return Boolean(existing && existing.sourceEpoch !== source.sourceEpoch);
  });
}

export function isNewGatewayGeneration(
  current: CursorState,
  gatewayEpoch: string,
  gatewayStartedAtUnixNs: bigint
): boolean {
  return Boolean(gatewayEpoch && gatewayStartedAtUnixNs > 0n &&
    (current.gatewayEpoch !== gatewayEpoch ||
      current.gatewayStartedAtUnixNs !== gatewayStartedAtUnixNs));
}

export function isValidCursorVector(nextSources: readonly SourceCursorState[]): boolean {
  const sourceIds = new Set<string>();
  for (const source of nextSources) {
    if (
      !source.sourceId ||
      !source.sourceEpoch ||
      source.sourceSeq === 0n ||
      sourceIds.has(source.sourceId)
    ) {
      return false;
    }
    sourceIds.add(source.sourceId);
  }
  return true;
}

export function mergeCursor(
  current: CursorState,
  nextGatewaySeq: bigint,
  nextSource: SourceCursorState | undefined
): CursorState {
  return {
    gatewaySeq:
      nextGatewaySeq > current.gatewaySeq ? nextGatewaySeq : current.gatewaySeq,
    gatewayEpoch: current.gatewayEpoch,
    gatewayStartedAtUnixNs: current.gatewayStartedAtUnixNs,
    sourceCursors: nextSource
      ? {
          ...current.sourceCursors,
          [nextSource.sourceId]: nextSource
        }
      : current.sourceCursors
  };
}

export function mergeCursorVector(
  current: CursorState,
  nextGatewaySeq: bigint,
  nextSources: readonly SourceCursorState[],
  options: {
    readonly replaceGateway?: boolean;
    readonly gatewayEpoch?: string;
    readonly gatewayStartedAtUnixNs?: bigint;
  } = {}
): CursorState {
  const sourceCursors = { ...current.sourceCursors };
  for (const source of nextSources) {
    sourceCursors[source.sourceId] = source;
  }
  return {
    gatewaySeq: options.replaceGateway
      ? nextGatewaySeq
      : nextGatewaySeq > current.gatewaySeq ? nextGatewaySeq : current.gatewaySeq,
    gatewayEpoch: options.gatewayEpoch ?? current.gatewayEpoch,
    gatewayStartedAtUnixNs:
      options.gatewayStartedAtUnixNs ?? current.gatewayStartedAtUnixNs,
    sourceCursors
  };
}

function openCursorDb(): Promise<IDBDatabase> {
  return new Promise((resolve, reject) => {
    const request = indexedDB.open(CURSOR_DB_NAME, CURSOR_DB_VERSION);
    request.onupgradeneeded = () => {
      request.result.createObjectStore(CURSOR_STORE);
    };
    request.onerror = () => reject(request.error);
    request.onsuccess = () => resolve(request.result);
  });
}

async function readCursorPayload(): Promise<string | undefined> {
  if (!("indexedDB" in globalThis)) {
    return undefined;
  }

  const db = await openCursorDb();
  return new Promise((resolve, reject) => {
    const transaction = db.transaction(CURSOR_STORE, "readonly");
    const request = transaction.objectStore(CURSOR_STORE).get(CURSOR_STORAGE_KEY);
    request.onerror = () => reject(request.error);
    request.onsuccess = () => {
      resolve(typeof request.result === "string" ? request.result : undefined);
    };
    transaction.oncomplete = () => db.close();
  });
}

async function writeCursorPayload(payload: string): Promise<void> {
  if (!("indexedDB" in globalThis)) {
    return;
  }

  const db = await openCursorDb();
  return new Promise((resolve, reject) => {
    const transaction = db.transaction(CURSOR_STORE, "readwrite");
    const request = transaction.objectStore(CURSOR_STORE).put(
      payload,
      CURSOR_STORAGE_KEY
    );
    request.onerror = () => reject(request.error);
    transaction.onerror = () => reject(transaction.error);
    transaction.oncomplete = () => {
      db.close();
      resolve();
    };
  });
}
