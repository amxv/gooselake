import assert from "node:assert/strict";
import { create, toBinary } from "@bufbuild/protobuf";
import { Lane, MessageKind } from "../src/gen/goosetower/v1/common_pb";
import { CommandAcceptedSchema } from "../src/gen/goosetower/v1/commands_pb";
import {
  HelloSchema,
  RealtimeEnvelopeSchema,
  type RealtimeEnvelope
} from "../src/gen/goosetower/v1/realtime_pb";
import {
  PatchSchema,
  ViewCoverageSchema,
  ViewOperation
} from "../src/gen/goosetower/v1/view_pb";
import { decodePatch, sourceEntityKey } from "../app/realtime/protocol/entities";
import type { WorkerOutbound } from "../app/realtime/types";
import { RealtimeWorkerCore } from "../app/realtime/worker/realtime-command-core";
import {
  getGoosewebSnapshot,
  resetGoosewebStoreForTests,
  updateGoosewebStore
} from "../app/stores/gooseweb-store";

const SOURCE_ID = "p02-source";
const SOURCE_EPOCH = "p02-epoch-001";
const SESSION_ID = "p02-session-001";
const GATEWAY_EPOCH = "p09-production-sequence";
const STARTED_AT = 1n;

const malformedFleet = patch("malformed-fleet", 1n, 1n, "fleet_board", "fleet_rows", SESSION_ID, {
  row_id: `${SOURCE_ID}:wrong-session`, source_id: SOURCE_ID, session_id: SESSION_ID
});
assert.equal(malformedFleet.payload.case, "patch");
assert.throws(() => decodePatch(malformedFleet.payload.value, [SOURCE_ID]),
  /fleet row identity disagrees/,
  "canonical fleet identity must not weaken malformed body rejection");

class FakeSocket {
  static readonly OPEN = 1;
  readyState = FakeSocket.OPEN;
  bufferedAmount = 0;
  binaryType = "";
  onopen: (() => void) | null = null;
  onmessage: ((event: { data: ArrayBuffer }) => void) | null = null;
  onerror: (() => void) | null = null;
  onclose: ((event: { code: number; reason: string }) => void) | null = null;
  sent: Uint8Array[] = [];
  constructor(readonly url: string) { socket = this; }
  send(value: Uint8Array) { this.sent.push(value); }
  close() { this.readyState = 3; }
  open() { this.onopen?.(); }
  receive(envelope: RealtimeEnvelope) {
    const bytes = toBinary(RealtimeEnvelopeSchema, envelope);
    this.onmessage?.({ data: bytes.buffer.slice(bytes.byteOffset, bytes.byteOffset + bytes.byteLength) });
  }
}

let socket: FakeSocket | undefined;
Object.assign(globalThis, { WebSocket: FakeSocket });
resetGoosewebStoreForTests();
const posted: WorkerOutbound[] = [];
const core = new RealtimeWorkerCore((message) => {
  posted.push(message);
  if (message.type === "state") updateGoosewebStore(message.patch);
});
await core.handleMessage({
  type: "connect", ticket: "ticket", goosetowerUrl: "ws://p02.invalid/v1/realtime"
});
socket!.open();
socket!.receive(create(RealtimeEnvelopeSchema, {
  protocolVersion: 1,
  messageId: "hello-production-sequence",
  messageKind: MessageKind.HELLO,
  lane: Lane.CRITICAL,
  payload: { case: "hello", value: create(HelloSchema, {
    connectionId: "p09-production-sequence",
    heartbeatIntervalMs: 60_000,
    protocolVersion: 1,
    resumeSupported: true,
    gatewayEpoch: GATEWAY_EPOCH,
    gatewayStartedAtUnixNs: STARTED_AT
  }) }
}));
socket!.receive(patch("initial-session", 46n, 3n, "session_summary", "sessions", SESSION_ID, {
  source_id: SOURCE_ID,
  session: {
    id: SESSION_ID, provider: "codex", model: "gpt-5", status: "ready",
    cwd: "/p02/workspace", metadata: { seed_version: "p02-fake-gooselake/v2" }
  }
}));
await flush();
assert.equal(getGoosewebSnapshot().connection, "connected");

await core.handleMessage({
  type: "command",
  command: {
    commandId: "p09-production-command",
    idempotencyKey: "p09-production-command",
    createdAtClientUnixMs: 1n,
    target: { scope: "session", scopeId: SESSION_ID, entityId: SESSION_ID },
    payload: { case: "sendTurn", value: {
      sessionId: SESSION_ID, text: "P09 lossless authority action 9dc7f36", input: []
    } }
  }
});
socket!.receive(create(RealtimeEnvelopeSchema, {
  protocolVersion: 1,
  messageId: "command-accepted",
  messageKind: MessageKind.COMMAND_ACCEPTED,
  lane: Lane.CRITICAL,
  payload: { case: "commandAccepted", value: create(CommandAcceptedSchema, {
    commandId: "p09-production-command", gatewaySeq: 61n
  }) }
}));

const actionFrames = [
  patch("action-ledger", 62n, 4n, "ledger", "ledger_events", "4", {
    created_at: 1_700_100_000_040, criticality: "critical", kind: "turn.completed",
    lane: "critical", scope: "session", scope_id: SESSION_ID,
    session_id: SESSION_ID, source_epoch: SOURCE_EPOCH, source_id: SOURCE_ID,
    source_seq: 4, team_id: null, turn_id: "p02-turn-001", upstream_row_id: 4,
    upstream_scoped_seq: 2
  }),
  patch("action-fleet", 63n, 4n, "fleet_board", "fleet_rows", SESSION_ID, {
    active_process_count: 0, active_turn_id: null, cwd: "/p02/workspace",
    delivery_status_counts: { injected: 1 }, latest_activity_unix_ms: 1_700_100_000_050,
    model: "gpt-5", pending_approval_count: 0, provider: "codex",
    row_id: `${SOURCE_ID}:${SESSION_ID}`, session_id: SESSION_ID,
    source_health: "live", source_id: SOURCE_ID, status: "ready",
    team_id: "p02-team-001", title: "Lead", version: 1_700_100_000_040,
    worktree_id: null, worktree_path: null
  }),
  patch("action-summary", 64n, 4n, "session_summary", "sessions", SESSION_ID, {
    source_id: SOURCE_ID,
    session: {
      id: SESSION_ID, provider: "codex", model: "gpt-5", status: "ready",
      cwd: "/p02/workspace", active_turn_id: null,
      metadata: { seed_version: "p02-fake-gooselake/v2" }
    }
  }),
  patch("action-detail", 65n, 4n, "session_detail", "session_details", SESSION_ID, {
    active_processes: [], appended_text: "P02 deterministic terminal", discontinuities: [],
    latest_activity_unix_ms: 1_700_100_000_050, pending_approvals: [], recent_processes: [],
    session: { id: SESSION_ID, provider: "codex", model: "gpt-5", status: "ready" },
    source_health: "live", source_id: SOURCE_ID, team_ids: ["p02-team-001"],
    transcript: [], version: 1_700_100_000_040
  }, ViewOperation.REPLACE),
  patch("action-health", 67n, 4n, "source_health", "sources", SOURCE_ID, {
    source_id: SOURCE_ID, source_epoch: SOURCE_EPOCH, display_name: "P02 deterministic",
    source_kind: "gooselake-runtime", provisioner_kind: "static", state: "live",
    last_source_seq: 4, last_error: null, observed_at_unix_ms: 1_700_100_000_050,
    active_session_count: 1, active_process_count: 0, provider_kinds: ["codex"],
    models: ["gpt-5"], model_capabilities: [], process_capacity: null,
    supports_worktrees: true, supports_teams: true, replay_window_events: null,
    replay_window_ms: null, region: null, cost_hint: null
  })
];
for (const frame of actionFrames) socket!.receive(frame);
for (const frame of actionFrames) socket!.receive(frame);
await flush();

const snapshot = getGoosewebSnapshot();
assert.equal(snapshot.connection, "connected",
  `ordinary P02 command frames must not degrade: ${snapshot.lastError ?? "no error"}`);
assert.equal(snapshot.cursor.gatewaySeq, 67n);
assert.equal(snapshot.cursor.sourceCursors[SOURCE_ID]?.sourceSeq, 4n);
assert.equal(snapshot.entities.fleetRows[sourceEntityKey(SOURCE_ID, SESSION_ID)]?.rowId,
  `${SOURCE_ID}:${SESSION_ID}`,
  "fleet authority uses session identity while retaining the materialized display row ID");
assert.equal(snapshot.entities.sessions[sourceEntityKey(SOURCE_ID, SESSION_ID)]?.status, "ready");
assert.equal(snapshot.entities.sessionDetails[sourceEntityKey(SOURCE_ID, SESSION_ID)]?.appendedText,
  "P02 deterministic terminal");
assert.equal(snapshot.entities.sources[sourceEntityKey(SOURCE_ID, SOURCE_ID)]?.health, "live");
assert.equal(posted.filter((message) => message.type === "error").length, 0);
assert.equal(posted.filter((message) => message.type === "state").flatMap((message) =>
  message.patch.entityOperations ?? []).length, 5,
"initial authority plus four normalized action domains apply exactly once despite duplicate frames");
await core.handleMessage({ type: "disconnect" });

console.log("P09 production command sequence remains connected and lossless");

function patch(
  messageId: string,
  gatewaySeq: bigint,
  sourceSeq: bigint,
  viewKind: string,
  domain: string,
  entityId: string,
  body: unknown,
  operation = ViewOperation.UPSERT
): RealtimeEnvelope {
  return create(RealtimeEnvelopeSchema, {
    protocolVersion: 1, messageId, messageKind: MessageKind.PATCH, lane: Lane.STATE,
    payload: { case: "patch", value: create(PatchSchema, {
      viewKind, schemaVersion: 1, operation, entity: { entityId },
      cursor: {
        gatewaySeq, gatewayEpoch: GATEWAY_EPOCH, gatewayStartedAtUnixNs: STARTED_AT,
        sources: [{ sourceId: SOURCE_ID, sourceEpoch: SOURCE_EPOCH, sourceSeq }]
      },
      coverage: create(ViewCoverageSchema, {
        domains: [domain], entityIds: [entityId], authoritative: true
      }),
      body: new TextEncoder().encode(JSON.stringify(body))
    }) }
  });
}

async function flush(): Promise<void> {
  await new Promise((resolve) => setTimeout(resolve, 24));
}
