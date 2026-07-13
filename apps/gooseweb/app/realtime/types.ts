import type {
  ApprovalView,
  FleetRowView,
  ProcessView,
  SessionView,
  SourceHealthView,
  TeamView,
  WorktreeView
} from "../../src/gen/goosetower/v1/view_pb";

export type ConnectionState =
  | "idle"
  | "connecting"
  | "connected"
  | "degraded"
  | "reconnecting"
  | "replaying"
  | "stale"
  | "offline";

export type NormalizedEntities = {
  readonly fleetRows: Readonly<Record<string, FleetRowView>>;
  readonly sessions: Readonly<Record<string, SessionView>>;
  readonly sessionDetails: Readonly<Record<string, SessionDetailState>>;
  readonly teams: Readonly<Record<string, TeamView>>;
  readonly teamWorkspaces: Readonly<Record<string, TeamWorkspaceState>>;
  readonly approvals: Readonly<Record<string, ApprovalView>>;
  readonly processes: Readonly<Record<string, ProcessView>>;
  readonly worktrees: Readonly<Record<string, WorktreeView>>;
  readonly sources: Readonly<Record<string, SourceHealthView>>;
};

export type NormalizedEntityPatch = {
  readonly fleetRows?: Readonly<Record<string, FleetRowView>>;
  readonly sessions?: Readonly<Record<string, SessionView>>;
  readonly sessionDetails?: Readonly<Record<string, SessionDetailState>>;
  readonly teams?: Readonly<Record<string, TeamView>>;
  readonly teamWorkspaces?: Readonly<Record<string, TeamWorkspaceState>>;
  readonly approvals?: Readonly<Record<string, ApprovalView>>;
  readonly processes?: Readonly<Record<string, ProcessView>>;
  readonly worktrees?: Readonly<Record<string, WorktreeView>>;
  readonly sources?: Readonly<Record<string, SourceHealthView>>;
};

export type EntityDomain = keyof NormalizedEntities;

export type LoadedCoverage = {
  readonly sourceId: string;
  readonly domain: EntityDomain;
  readonly subscriptionId: string;
  readonly kind: "domain" | "window" | "entity";
  readonly entityIds: readonly string[];
  readonly filters: Readonly<Record<string, string>>;
  readonly authoritative: true;
  readonly empty: boolean;
};

export type EntityMutation = {
  readonly operation: "replace" | "upsert" | "remove";
  readonly domain: EntityDomain;
  readonly entityIds: readonly string[];
  readonly authoritative: boolean;
};

export type EntityOperation = EntityMutation & {
  readonly payload: Readonly<Record<string, unknown>>;
  // Present only for an explicit full-source resync transaction. The store
  // replaces entities owned by this source without disturbing sibling sources.
  readonly sourceId?: string;
};

export type SessionTranscriptEntry = {
  readonly id: string;
  readonly sessionId: string;
  readonly role: string;
  readonly text: string;
  readonly turnId?: string;
  readonly createdAtUnixMs?: number;
};

export type SessionDetailState = {
  readonly sessionId: string;
  readonly sourceId: string;
  readonly transcript: readonly SessionTranscriptEntry[];
  readonly appendedText: string;
  readonly latestActivityUnixMs: number;
};

export type TeamMessageState = {
  readonly id: string;
  readonly teamId: string;
  readonly scope: string;
  readonly senderAgentId: string;
  readonly recipientAgentIds: readonly string[];
  readonly text: string;
  readonly createdAtUnixMs: number;
};

export type TeamDeliveryState = {
  readonly id: string;
  readonly messageId: string;
  readonly teamId: string;
  readonly recipientAgentId: string;
  readonly provider: string;
  readonly status: string;
  readonly injectedTurnId?: string;
  readonly lastError?: string;
  readonly updatedAtUnixMs: number;
};

export type TeamWorkspaceState = {
  readonly teamId: string;
  readonly sourceId: string;
  readonly messages: readonly TeamMessageState[];
  readonly deliveries: readonly TeamDeliveryState[];
};

export type PendingCommandState = {
  readonly commandId: string;
  readonly idempotencyKey: string;
  readonly status: "queued" | "sent" | "accepted" | "rejected" | "duplicate";
  readonly createdAtUnixMs: number;
  readonly targetScope?: string;
  readonly targetScopeId?: string;
  readonly targetEntityId?: string;
  readonly payloadCase?: CommandPayloadCase;
  readonly errorCode?: string;
  readonly error?: string;
  readonly refreshEntity?: boolean;
};

export type CommandScope = "session" | "team" | "process" | "source";

export type CommandPayloadCase =
  | "sendTurn"
  | "resolveApproval"
  | "interruptTurn"
  | "sendTeamMessage"
  | "broadcastTeamMessage"
  | "spawnTeamMember"
  | "retryDelivery"
  | "cancelDelivery"
  | "killProcess"
  | "startProcess"
  | "createSession"
  | "createTeam"
  | "joinTeamMember";

export type CommandIntent = {
  readonly commandId: string;
  readonly idempotencyKey: string;
  readonly createdAtClientUnixMs: bigint;
  readonly fallbackCreateSession?: {
    readonly provider: string;
    readonly model: string;
    readonly cwd: string;
    readonly title: string;
    readonly permissionMode: string;
    readonly metadata: Readonly<Record<string, string>>;
  };
  readonly target: {
    readonly scope: CommandScope;
    readonly scopeId: string;
    readonly entityId: string;
  };
  readonly payload: {
    readonly case: CommandPayloadCase;
    readonly value: Readonly<Record<string, unknown>>;
  };
};

export type SubscriptionState = {
  readonly subscriptionId: string;
  readonly viewKind: string;
  readonly filters: Readonly<Record<string, string>>;
  readonly status: "subscribing" | "active" | "stale" | "unsubscribed";
};

export type CursorState = {
  readonly gatewaySeq: bigint;
  readonly gatewayEpoch: string;
  readonly gatewayStartedAtUnixNs: bigint;
  readonly sourceCursors: Readonly<Record<string, SourceCursorState>>;
};

export type SourceCursorState = {
  readonly sourceId: string;
  readonly sourceEpoch: string;
  readonly sourceSeq: bigint;
};

export type GoosewebSnapshot = {
  readonly connection: ConnectionState;
  readonly connectionId?: string;
  readonly heartbeatIntervalMs: number;
  readonly cursor: CursorState;
  readonly entities: NormalizedEntities;
  readonly subscriptions: Readonly<Record<string, SubscriptionState>>;
  readonly pendingCommands: Readonly<Record<string, PendingCommandState>>;
  readonly staleSources: Readonly<Record<string, string>>;
  readonly invalidatedSourceDomains: Readonly<Record<string, readonly EntityDomain[]>>;
  readonly loadedCoverage: Readonly<Record<string, LoadedCoverage>>;
  readonly lastError?: string;
};

export type WorkerInbound =
  | {
      readonly type: "connect";
      readonly ticket: string;
      readonly goosetowerUrl: string;
    }
  | { readonly type: "disconnect" }
  | {
      readonly type: "subscribe";
      readonly subscriptionId: string;
      readonly viewKind: string;
      readonly filters?: Readonly<Record<string, string>>;
    }
  | { readonly type: "unsubscribe"; readonly subscriptionId: string }
  | {
      readonly type: "command";
      readonly command: CommandIntent;
      readonly idempotencyKey?: string;
    }
  | { readonly type: "auth-refresh"; readonly ticket: string };

export type WorkerOutbound =
  | {
      readonly type: "state";
      readonly patch: GoosewebStorePatch;
    }
  | { readonly type: "command-state"; readonly command: PendingCommandState }
  | {
      readonly type: "subscription-state";
      readonly subscription: SubscriptionState;
    }
  | { readonly type: "error"; readonly message: string; readonly retryable: boolean };

export type GoosewebStorePatch = Partial<Omit<GoosewebSnapshot, "entities">> & {
  readonly entities?: NormalizedEntityPatch;
  readonly entityOperations?: readonly EntityOperation[];
};
