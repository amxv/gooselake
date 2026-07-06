import { create, type MessageInitShape } from "@bufbuild/protobuf";
import { createFileRoute } from "@tanstack/react-router";
import { useEffect, useMemo, useState } from "react";
import {
  CommandSchema,
  type Command
} from "../../src/gen/goosetower/v1/commands_pb";
import {
  Scope,
  EntityRefSchema
} from "../../src/gen/goosetower/v1/common_pb";
import {
  connectRealtime,
  disconnectRealtime,
  ensureRealtimeWorker,
  sendRealtimeCommand,
  subscribeRealtime
} from "../../app/realtime/client";
import { goosewebConfig } from "../../app/realtime/config";
import {
  useConnectionState,
  useFleetRows,
  usePendingCommands,
  useSources,
  useVisibleSubscriptions
} from "../../app/stores/gooseweb-store";

export const Route = createFileRoute("/")({
  component: Index
});

function Index() {
  const connection = useConnectionState();
  const fleetRows = useFleetRows();
  const sources = useSources();
  const pendingCommands = usePendingCommands();
  const subscriptions = useVisibleSubscriptions();
  const [ticket, setTicket] = useState(goosewebConfig.pastedDevTicket);
  const [commandText, setCommandText] = useState("");

  useEffect(() => {
    ensureRealtimeWorker();
  }, []);

  const activeSubscriptions = useMemo(
    () =>
      subscriptions.filter(
        (subscription) => subscription.status !== "unsubscribed"
      ).length,
    [subscriptions]
  );

  return (
    <>
      <div className="toolbar">
        <h2>Realtime Core</h2>
        <span className="status-pill">{connection}</span>
      </div>

      <div className="grid">
        <section className="panel">
          <h3>Connection</h3>
          <label className="empty" htmlFor="dev-ticket">
            Development ticket
          </label>
          <textarea
            id="dev-ticket"
            value={ticket}
            onChange={(event) => setTicket(event.target.value)}
            rows={4}
            style={{
              width: "100%",
              resize: "vertical",
              marginTop: 8,
              background: "#101314",
              color: "#e8ecef",
              border: "1px solid #385258",
              borderRadius: 6,
              padding: 10
            }}
          />
          <div style={{ display: "flex", gap: 8, marginTop: 12 }}>
            <button
              type="button"
              onClick={() => connectRealtime(ticket)}
              disabled={!ticket.trim()}
            >
              Connect
            </button>
            <button type="button" onClick={() => disconnectRealtime()}>
              Disconnect
            </button>
          </div>
          <div className="metric">
            <span>Gateway</span>
            <strong>{goosewebConfig.goosetowerUrl}</strong>
          </div>
          <div className="metric">
            <span>Subscriptions</span>
            <strong>{activeSubscriptions}</strong>
          </div>
        </section>

        <section className="panel">
          <h3>Visible Subscriptions</h3>
          <div style={{ display: "flex", gap: 8, flexWrap: "wrap" }}>
            <button
              type="button"
              onClick={() =>
                subscribeRealtime("board:default", "fleet-row", {
                  window: "0:100"
                })
              }
            >
              Board
            </button>
            <button
              type="button"
              onClick={() => subscribeRealtime("inbox:default", "approval")}
            >
              Inbox
            </button>
            <button
              type="button"
              onClick={() => subscribeRealtime("sources:default", "source-health")}
            >
              Sources
            </button>
          </div>
          {subscriptions.length === 0 ? (
            <p className="empty">No active subscriptions.</p>
          ) : (
            subscriptions.map((subscription) => (
              <div className="metric" key={subscription.subscriptionId}>
                <span>{subscription.viewKind}</span>
                <strong>{subscription.status}</strong>
              </div>
            ))
          )}
        </section>

        <section className="panel">
          <h3>Fleet Rows</h3>
          {fleetRows.length === 0 ? (
            <p className="empty">No board rows received.</p>
          ) : (
            fleetRows.slice(0, 8).map((row) => (
              <div className="metric" key={row.rowId}>
                <span>{row.title || row.sessionId}</span>
                <strong>{row.status}</strong>
              </div>
            ))
          )}
        </section>

        <section className="panel">
          <h3>Sources</h3>
          {sources.length === 0 ? (
            <p className="empty">No source health snapshots received.</p>
          ) : (
            sources.map((source) => (
              <div className="metric" key={source.sourceId}>
                <span>{source.displayName || source.sourceId}</span>
                <strong>{source.health}</strong>
              </div>
            ))
          )}
        </section>

        <section className="panel">
          <h3>Command Send</h3>
          <input
            value={commandText}
            onChange={(event) => setCommandText(event.target.value)}
            placeholder="Send a test turn to a selected session later"
            style={{
              width: "100%",
              background: "#101314",
              color: "#e8ecef",
              border: "1px solid #385258",
              borderRadius: 6,
              padding: 10
            }}
          />
          <button
            type="button"
            style={{ marginTop: 12 }}
            disabled={!commandText.trim()}
            onClick={() => {
              sendRealtimeCommand(makePlaceholderCommand(commandText));
              setCommandText("");
            }}
          >
            Queue Command
          </button>
        </section>

        <section className="panel">
          <h3>Pending Commands</h3>
          {pendingCommands.length === 0 ? (
            <p className="empty">No pending command state.</p>
          ) : (
            pendingCommands.map((command) => (
              <div className="metric" key={command.commandId}>
                <span>{command.commandId}</span>
                <strong>{command.status}</strong>
              </div>
            ))
          )}
        </section>
      </div>
    </>
  );
}

function makePlaceholderCommand(text: string): Command {
  const command: MessageInitShape<typeof CommandSchema> = {
    commandId: crypto.randomUUID(),
    idempotencyKey: crypto.randomUUID(),
    createdAtClientUnixMs: BigInt(Date.now()),
    target: create(EntityRefSchema, {
      scope: Scope.SESSION,
      scopeId: "dev-session",
      entityId: "dev-session"
    }),
    payload: {
      case: "sendTurn",
      value: {
        sessionId: "dev-session",
        text
      }
    }
  };

  return create(CommandSchema, command);
}
