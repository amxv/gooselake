import assert from "node:assert/strict";
import { create, fromBinary, toBinary } from "@bufbuild/protobuf";
import { Lane, MessageKind } from "../src/gen/goosetower/v1/common_pb";
import { RealtimeEnvelopeSchema } from "../src/gen/goosetower/v1/realtime_pb";
import {
  PatchSchema,
  SnapshotSchema,
  ViewCoverageSchema,
  ViewOperation
} from "../src/gen/goosetower/v1/view_pb";
import { EntityRefSchema } from "../src/gen/goosetower/v1/common_pb";
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

  receive(data: Uint8Array): void {
    this.onmessage?.({ data: data.buffer.slice(data.byteOffset, data.byteOffset + data.byteLength) });
  }
}

globalThis.WebSocket = FakeSocket as unknown as typeof WebSocket;

function snapshotEnvelope(input: {
  messageId: string;
  viewKind: string;
  domain: string;
  entityIds?: string[];
  sources: Array<{ sourceId: string; sourceEpoch: string; sourceSeq: bigint }>;
  body: Uint8Array;
  gatewaySeq?: bigint;
}) {
  return create(RealtimeEnvelopeSchema, {
    protocolVersion: 1,
    messageId: input.messageId,
    messageKind: MessageKind.SNAPSHOT,
    lane: Lane.STATE,
    gatewaySeq: 0n,
    payload: {
      case: "snapshot",
      value: create(SnapshotSchema, {
        viewKind: input.viewKind,
        schemaVersion: 1,
        operation: ViewOperation.REPLACE,
        cursor: { gatewaySeq: input.gatewaySeq ?? 1n, sources: input.sources },
        coverage: create(ViewCoverageSchema, {
          domains: [input.domain],
          entityIds: input.entityIds ?? [],
          authoritative: true
        }),
        body: input.body
      })
    }
  });
}

function patchEnvelope(input: {
  messageId: string;
  gatewaySeq: bigint;
  sourceSeq: bigint;
  sourceEpoch?: string;
  viewKind: string;
  domain: string;
  entityId: string;
  operation: ViewOperation;
  body: Uint8Array;
}) {
  return create(RealtimeEnvelopeSchema, {
    protocolVersion: 1,
    messageId: input.messageId,
    messageKind: MessageKind.PATCH,
    lane: Lane.STATE,
    gatewaySeq: input.gatewaySeq,
    payload: {
      case: "patch",
      value: create(PatchSchema, {
        viewKind: input.viewKind,
        schemaVersion: 1,
        operation: input.operation,
        entity: create(EntityRefSchema, { entityId: input.entityId }),
        cursor: {
          gatewaySeq: input.gatewaySeq,
          sources: [{
            sourceId: "source-1",
            sourceEpoch: input.sourceEpoch ?? "epoch-1",
            sourceSeq: input.sourceSeq
          }]
        },
        coverage: create(ViewCoverageSchema, {
          domains: [input.domain],
          entityIds: [input.entityId],
          authoritative: true
        }),
        body: input.body
      })
    }
  });
}

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

const nestedCursorSnapshot = snapshotEnvelope({
  messageId: "nested-cursor-snapshot",
  gatewaySeq: 41n,
  viewKind: "board",
  domain: "fleet_rows",
  sources: [
    { sourceId: "source-1", sourceEpoch: "epoch-1", sourceSeq: 17n },
    { sourceId: "source-2", sourceEpoch: "epoch-2", sourceSeq: 9n }
  ],
  body: new TextEncoder().encode(JSON.stringify({ rows: [] }))
});
sockets[2]?.receive(toBinary(RealtimeEnvelopeSchema, nestedCursorSnapshot));
await waitForPatchFlush();
assert.equal(posted.some((message) =>
  message.type === "state" &&
  message.patch.cursor?.sourceCursors["source-1"]?.sourceSeq === 17n
), true, "Worker must consume the canonical nested production cursor");
assert.equal(posted.some((message) =>
  message.type === "state" &&
  message.patch.cursor?.sourceCursors["source-2"]?.sourceSeq === 9n
), true, "Worker must atomically retain every source cursor");

const selectedBody = new TextEncoder().encode(JSON.stringify({
  source_id: "source-1",
  session: { id: "session-1", provider: "codex", status: "ready" },
  transcript: [{ role: "assistant", text: "reloaded answer" }],
  appended_text: "",
  latest_activity_unix_ms: 200
}));
const selectedAtEqualAuthority = snapshotEnvelope({
  messageId: "selected-at-equal-authority",
  gatewaySeq: 41n,
  viewKind: "session_detail",
  domain: "session_details",
  entityIds: ["session-1"],
  sources: [{ sourceId: "source-1", sourceEpoch: "epoch-1", sourceSeq: 17n }],
  body: selectedBody
});
const detailOperationsBefore = posted.flatMap((message) =>
  message.type === "state" ? message.patch.entityOperations ?? [] : []
).length;
sockets[2]?.receive(toBinary(RealtimeEnvelopeSchema, selectedAtEqualAuthority));
await waitForPatchFlush();
const detailOperationsAfter = posted.flatMap((message) =>
  message.type === "state" ? message.patch.entityOperations ?? [] : []
).length;
assert.equal(detailOperationsAfter, detailOperationsBefore + 1,
  "equal source authority must not suppress a new scoped snapshot");
sockets[2]?.receive(toBinary(RealtimeEnvelopeSchema, selectedAtEqualAuthority));
await waitForPatchFlush();
assert.equal(posted.flatMap((message) =>
  message.type === "state" ? message.patch.entityOperations ?? [] : []
).length, detailOperationsAfter, "duplicate publication identity must be idempotent");

const staleAndFresh = snapshotEnvelope({
  messageId: "stale-and-fresh-vector",
  viewKind: "board",
  domain: "fleet_rows",
  sources: [
    { sourceId: "source-1", sourceEpoch: "epoch-1", sourceSeq: 16n },
    { sourceId: "source-2", sourceEpoch: "epoch-2", sourceSeq: 10n }
  ],
  body: new TextEncoder().encode(JSON.stringify({ rows: [] }))
});
sockets[2]?.receive(toBinary(RealtimeEnvelopeSchema, staleAndFresh));
await waitForPatchFlush();
assert.equal(posted.some((message) =>
  message.type === "state" &&
  message.patch.cursor?.sourceCursors["source-2"]?.sourceSeq === 10n
), false, "a mixed stale/fresh vector must not advance partially");

const reversedVector = snapshotEnvelope({
  messageId: "reversed-vector",
  gatewaySeq: 41n,
  viewKind: "board",
  domain: "fleet_rows",
  sources: [
    { sourceId: "source-2", sourceEpoch: "epoch-2", sourceSeq: 9n },
    { sourceId: "source-1", sourceEpoch: "epoch-1", sourceSeq: 17n }
  ],
  body: new TextEncoder().encode(JSON.stringify({ rows: [] }))
});
const operationsBeforeReverse = posted.flatMap((message) =>
  message.type === "state" ? message.patch.entityOperations ?? [] : []
).length;
sockets[2]?.receive(toBinary(RealtimeEnvelopeSchema, reversedVector));
await waitForPatchFlush();
assert.equal(posted.flatMap((message) =>
  message.type === "state" ? message.patch.entityOperations ?? [] : []
).length, operationsBeforeReverse + 1, "cursor vector order must not affect applicability");

const malformedAt18 = snapshotEnvelope({
  messageId: "malformed-at-18",
  gatewaySeq: 42n,
  viewKind: "session_detail",
  domain: "session_details",
  entityIds: ["session-1"],
  sources: [{ sourceId: "source-1", sourceEpoch: "epoch-1", sourceSeq: 18n }],
  body: new TextEncoder().encode("{}")
});
sockets[2]?.receive(toBinary(RealtimeEnvelopeSchema, malformedAt18));
await waitForPatchFlush();
const validAt18 = snapshotEnvelope({
  messageId: "valid-at-18",
  gatewaySeq: 42n,
  viewKind: "session_detail",
  domain: "session_details",
  entityIds: ["session-1"],
  sources: [{ sourceId: "source-1", sourceEpoch: "epoch-1", sourceSeq: 18n }],
  body: selectedBody
});
sockets[2]?.receive(toBinary(RealtimeEnvelopeSchema, validAt18));
await waitForPatchFlush();
assert.equal(posted.some((message) =>
  message.type === "state" &&
  message.patch.cursor?.sourceCursors["source-1"]?.sourceSeq === 18n
), true, "malformed detail must not advance the cursor before a valid same-authority frame");

const operationsBeforeMissingAuthority = posted.flatMap((message) =>
  message.type === "state" ? message.patch.entityOperations ?? [] : []
).length;
const invalidAuthoritySnapshots = [
  create(RealtimeEnvelopeSchema, {
    protocolVersion: 1,
    messageId: "missing-cursor",
    messageKind: MessageKind.SNAPSHOT,
    lane: Lane.STATE,
    payload: { case: "snapshot", value: create(SnapshotSchema, {
      viewKind: "session_detail",
      schemaVersion: 1,
      operation: ViewOperation.REPLACE,
      coverage: create(ViewCoverageSchema, {
        domains: ["session_details"], entityIds: ["session-1"], authoritative: true
      }),
      body: selectedBody
    }) }
  }),
  snapshotEnvelope({
    messageId: "empty-sources",
    gatewaySeq: 43n,
    viewKind: "session_detail",
    domain: "session_details",
    entityIds: ["session-1"],
    sources: [],
    body: selectedBody
  }),
  snapshotEnvelope({
    messageId: "zero-source-seq",
    gatewaySeq: 43n,
    viewKind: "session_detail",
    domain: "session_details",
    entityIds: ["session-1"],
    sources: [{ sourceId: "source-1", sourceEpoch: "epoch-1", sourceSeq: 0n }],
    body: selectedBody
  }),
  snapshotEnvelope({
    messageId: "blank-source-epoch",
    gatewaySeq: 43n,
    viewKind: "session_detail",
    domain: "session_details",
    entityIds: ["session-1"],
    sources: [{ sourceId: "source-1", sourceEpoch: "", sourceSeq: 19n }],
    body: selectedBody
  }),
  snapshotEnvelope({
    messageId: "zero-gateway-seq",
    gatewaySeq: 0n,
    viewKind: "session_detail",
    domain: "session_details",
    entityIds: ["session-1"],
    sources: [{ sourceId: "source-1", sourceEpoch: "epoch-1", sourceSeq: 19n }],
    body: selectedBody
  })
];
for (const invalid of invalidAuthoritySnapshots) {
  sockets[2]?.receive(toBinary(RealtimeEnvelopeSchema, invalid));
}
const missingCursorPatch = create(RealtimeEnvelopeSchema, {
  protocolVersion: 1,
  messageId: "patch-missing-cursor",
  messageKind: MessageKind.PATCH,
  lane: Lane.STATE,
  gatewaySeq: 43n,
  payload: { case: "patch", value: create(PatchSchema, {
    viewKind: "session_detail",
    schemaVersion: 1,
    operation: ViewOperation.REPLACE,
    entity: create(EntityRefSchema, { entityId: "session-1" }),
    coverage: create(ViewCoverageSchema, {
      domains: ["session_details"], entityIds: ["session-1"], authoritative: true
    }),
    body: selectedBody
  }) }
});
sockets[2]?.receive(toBinary(RealtimeEnvelopeSchema, missingCursorPatch));
await waitForPatchFlush();
assert.equal(posted.flatMap((message) =>
  message.type === "state" ? message.patch.entityOperations ?? [] : []
).length, operationsBeforeMissingAuthority,
"versioned frames without complete canonical authority must not mutate entities");

const sessionSiblingFrames = [
  patchEnvelope({
    messageId: "session-fleet-19", gatewaySeq: 43n, sourceSeq: 19n,
    viewKind: "fleet_board", domain: "fleet_rows", entityId: "session-1",
    operation: ViewOperation.UPSERT,
    body: new TextEncoder().encode(JSON.stringify({
      source_id: "source-1", row_id: "session-1", session_id: "session-1",
      provider: "codex", status: "ready", latest_activity_unix_ms: 201
    }))
  }),
  patchEnvelope({
    messageId: "session-summary-19", gatewaySeq: 44n, sourceSeq: 19n,
    viewKind: "session_summary", domain: "sessions", entityId: "session-1",
    operation: ViewOperation.UPSERT,
    body: new TextEncoder().encode(JSON.stringify({
      source_id: "source-1",
      session: { id: "session-1", provider: "codex", status: "ready" }
    }))
  }),
  patchEnvelope({
    messageId: "session-detail-19", gatewaySeq: 45n, sourceSeq: 19n,
    viewKind: "session_detail", domain: "session_details", entityId: "session-1",
    operation: ViewOperation.REPLACE, body: selectedBody
  })
];
const operationsBeforeSiblings = posted.flatMap((message) =>
  message.type === "state" ? message.patch.entityOperations ?? [] : []
).length;
for (const frame of sessionSiblingFrames) sockets[2]?.receive(toBinary(RealtimeEnvelopeSchema, frame));
await waitForPatchFlush();
const operationsAfterSiblings = posted.flatMap((message) =>
  message.type === "state" ? message.patch.entityOperations ?? [] : []
).length;
assert.equal(operationsAfterSiblings, operationsBeforeSiblings + 3,
  "distinct gateway publications at one source cursor must all apply");
for (const frame of sessionSiblingFrames) sockets[2]?.receive(toBinary(RealtimeEnvelopeSchema, frame));
await waitForPatchFlush();
assert.equal(posted.flatMap((message) =>
  message.type === "state" ? message.patch.entityOperations ?? [] : []
).length, operationsAfterSiblings, "exact sibling replays must apply zero operations");

const emptyTeamBody = new TextEncoder().encode(JSON.stringify({
  source_id: "source-1",
  team: { id: "team-1", name: "Team", lead_agent_id: "session-1" },
  members: [], messages: [], deliveries: []
}));
const teamSiblingFrames = [
  patchEnvelope({
    messageId: "team-summary-20", gatewaySeq: 46n, sourceSeq: 20n,
    viewKind: "team_summary", domain: "teams", entityId: "team-1",
    operation: ViewOperation.UPSERT, body: emptyTeamBody
  }),
  patchEnvelope({
    messageId: "team-workspace-20", gatewaySeq: 47n, sourceSeq: 20n,
    viewKind: "team_workspace", domain: "team_workspaces", entityId: "team-1",
    operation: ViewOperation.REPLACE, body: emptyTeamBody
  })
];
const operationsBeforeTeamSiblings = posted.flatMap((message) =>
  message.type === "state" ? message.patch.entityOperations ?? [] : []
).length;
for (const frame of teamSiblingFrames) sockets[2]?.receive(toBinary(RealtimeEnvelopeSchema, frame));
await waitForPatchFlush();
assert.equal(posted.flatMap((message) =>
  message.type === "state" ? message.patch.entityOperations ?? [] : []
).length, operationsBeforeTeamSiblings + 2,
"team summary/workspace siblings at equal source authority must both apply");

const epochMismatchPatch = patchEnvelope({
  messageId: "epoch-mismatch-patch", gatewaySeq: 48n, sourceSeq: 1n,
  sourceEpoch: "epoch-new", viewKind: "session_detail", domain: "session_details",
  entityId: "session-1", operation: ViewOperation.REPLACE, body: selectedBody
});
const operationsBeforeEpochMismatch = posted.flatMap((message) =>
  message.type === "state" ? message.patch.entityOperations ?? [] : []
).length;
sockets[2]?.receive(toBinary(RealtimeEnvelopeSchema, epochMismatchPatch));
await waitForPatchFlush();
assert.equal(posted.flatMap((message) =>
  message.type === "state" ? message.patch.entityOperations ?? [] : []
).length, operationsBeforeEpochMismatch, "patch epoch mismatch must not mutate state");
const epochResyncSnapshot = snapshotEnvelope({
  messageId: "epoch-resync-snapshot", gatewaySeq: 48n,
  viewKind: "session_detail", domain: "session_details", entityIds: ["session-1"],
  sources: [{ sourceId: "source-1", sourceEpoch: "epoch-new", sourceSeq: 1n }],
  body: selectedBody
});
sockets[2]?.receive(toBinary(RealtimeEnvelopeSchema, epochResyncSnapshot));
await waitForPatchFlush();
const newEpochPatch = patchEnvelope({
  messageId: "new-epoch-patch", gatewaySeq: 49n, sourceSeq: 2n,
  sourceEpoch: "epoch-new", viewKind: "session_detail", domain: "session_details",
  entityId: "session-1", operation: ViewOperation.REPLACE, body: selectedBody
});
sockets[2]?.receive(toBinary(RealtimeEnvelopeSchema, newEpochPatch));
await waitForPatchFlush();
assert.equal(posted.some((message) =>
  message.type === "state" &&
  message.patch.cursor?.sourceCursors["source-1"]?.sourceEpoch === "epoch-new" &&
  message.patch.cursor?.sourceCursors["source-1"]?.sourceSeq === 2n
), true, "snapshot resync must establish the new epoch for later patches");
const delayedOldEpochPatch = patchEnvelope({
  messageId: "delayed-old-epoch", gatewaySeq: 50n, sourceSeq: 999n,
  sourceEpoch: "epoch-1", viewKind: "session_detail", domain: "session_details",
  entityId: "session-1", operation: ViewOperation.REPLACE, body: selectedBody
});
const operationsBeforeOldEpoch = posted.flatMap((message) =>
  message.type === "state" ? message.patch.entityOperations ?? [] : []
).length;
sockets[2]?.receive(toBinary(RealtimeEnvelopeSchema, delayedOldEpochPatch));
await waitForPatchFlush();
assert.equal(posted.flatMap((message) =>
  message.type === "state" ? message.patch.entityOperations ?? [] : []
).length, operationsBeforeOldEpoch, "delayed old-epoch patch must not flip authority");
sockets[2]?.receive(new Uint8Array([0xff]));
await waitForPatchFlush();
assert.equal(posted.some((message) =>
  message.type === "error" &&
  message.retryable === false &&
  message.message.startsWith("Realtime protocol error:")
), true, "malformed outer frames must fail safely");
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
