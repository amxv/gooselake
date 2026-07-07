import { createFileRoute } from "@tanstack/react-router";
import {
  ActivityIcon,
  BotIcon,
  BoxesIcon,
  ChevronDownIcon,
  ClipboardListIcon,
  CircleIcon,
  FolderIcon,
  InboxIcon,
  LayoutDashboardIcon,
  ListChecksIcon,
  PowerIcon,
  PlusIcon,
  RadioIcon,
  ScrollTextIcon,
  SendIcon,
  SettingsIcon,
  ShieldAlertIcon,
  SquareIcon,
  TerminalIcon,
  UsersIcon,
  WorkflowIcon
} from "lucide-react";
import {
  type FormEvent,
  type ReactNode,
  useEffect,
  useMemo,
  useRef,
  useState
} from "react";
import type {
  ApprovalView,
  FleetRowView,
  ProcessView,
  SessionView,
  SourceHealthView,
  TeamMemberView,
  TeamView,
  WorktreeView
} from "../../src/gen/goosetower/v1/view_pb";
import {
  connectRealtime,
  disconnectRealtime,
  ensureRealtimeWorker,
  mintDevelopmentTicket,
  sendRealtimeCommand,
  subscribeRealtime,
  unsubscribeRealtime
} from "../../app/realtime/client";
import { goosewebConfig } from "../../app/realtime/config";
import type {
  ConnectionState,
  CommandIntent,
  CommandPayloadCase,
  CommandScope,
  GoosewebSnapshot,
  PendingCommandState,
  SessionDetailState,
  TeamWorkspaceState
} from "../../app/realtime/types";
import {
  useGoosewebState
} from "../../app/stores/gooseweb-store";
import { Alert, AlertDescription, AlertTitle } from "~/components/ui/alert";
import { Badge } from "~/components/ui/badge";
import { Button, buttonVariants } from "~/components/ui/button";
import {
  Card,
  CardAction,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle
} from "~/components/ui/card";
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle
} from "~/components/ui/dialog";
import {
  Empty,
  EmptyDescription,
  EmptyHeader,
  EmptyMedia,
  EmptyTitle
} from "~/components/ui/empty";
import {
  Field,
  FieldGroup,
  FieldLabel
} from "~/components/ui/field";
import { Input } from "~/components/ui/input";
import { ScrollArea } from "~/components/ui/scroll-area";
import {
  Select,
  SelectContent,
  SelectGroup,
  SelectItem,
  SelectTrigger,
  SelectValue
} from "~/components/ui/select";
import { Separator } from "~/components/ui/separator";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow
} from "~/components/ui/table";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "~/components/ui/tabs";
import { Textarea } from "~/components/ui/textarea";
import { ToggleGroup, ToggleGroupItem } from "~/components/ui/toggle-group";
import { cn } from "~/lib/utils";

export const Route = createFileRoute("/")({
  component: Index
});

type WorkspaceView =
  | "board"
  | "inbox"
  | "teams"
  | "agents"
  | "ledger"
  | "fleet"
  | "playbooks"
  | "settings";

type BoardFilters = {
  readonly sourceId: string;
  readonly teamId: string;
  readonly status: string;
};

type LedgerEvent = {
  readonly id: string;
  readonly sourceId: string;
  readonly scope: string;
  readonly kind: string;
  readonly cursor: string;
  readonly criticality: string;
  readonly happenedAt: number;
};

const NAV_ITEMS: ReadonlyArray<{
  readonly id: WorkspaceView;
  readonly label: string;
  readonly icon: typeof LayoutDashboardIcon;
}> = [
  { id: "board", label: "Board", icon: LayoutDashboardIcon },
  { id: "inbox", label: "Inbox", icon: InboxIcon },
  { id: "teams", label: "Teams", icon: UsersIcon },
  { id: "agents", label: "Agents", icon: BotIcon },
  { id: "ledger", label: "Ledger", icon: ScrollTextIcon },
  { id: "fleet", label: "Fleet", icon: BoxesIcon },
  { id: "playbooks", label: "Playbooks", icon: WorkflowIcon },
  { id: "settings", label: "Settings", icon: SettingsIcon }
];

function Index() {
  const state = useGoosewebState();
  const fleetRows = useMemo(
    () => Object.values(state.entities.fleetRows),
    [state.entities.fleetRows]
  );
  const sessions = useMemo(
    () => Object.values(state.entities.sessions),
    [state.entities.sessions]
  );
  const teams = useMemo(
    () => Object.values(state.entities.teams),
    [state.entities.teams]
  );
  const approvals = useMemo(
    () => Object.values(state.entities.approvals),
    [state.entities.approvals]
  );
  const processes = useMemo(
    () => Object.values(state.entities.processes),
    [state.entities.processes]
  );
  const worktrees = useMemo(
    () => Object.values(state.entities.worktrees),
    [state.entities.worktrees]
  );
  const sources = useMemo(
    () => Object.values(state.entities.sources),
    [state.entities.sources]
  );
  const pendingCommands = useMemo(
    () =>
      Object.values(state.pendingCommands).filter(
        (command) => command.status === "queued" || command.status === "sent"
      ),
    [state.pendingCommands]
  );
  const subscriptions = useMemo(
    () => Object.values(state.subscriptions),
    [state.subscriptions]
  );
  const sessionDetails = state.entities.sessionDetails;
  const teamWorkspaces = state.entities.teamWorkspaces;
  const [activeView, setActiveView] = useState<WorkspaceView>("board");
  const [selectedRowId, setSelectedRowId] = useState("");
  const [selectedSessionId, setSelectedSessionId] = useState("");
  const [selectedTeamId, setSelectedTeamId] = useState("");
  const [selectedApprovalId, setSelectedApprovalId] = useState("");
  const [selectedProcessId, setSelectedProcessId] = useState("");
  const devAutoConnectAttempted = useRef(false);
  const [filters, setFilters] = useState<BoardFilters>({
    sourceId: "all",
    teamId: "all",
    status: "all"
  });

  useEffect(() => {
    ensureRealtimeWorker();
    subscribeRealtime("board:window", "board", { window: "0:120" });
    subscribeRealtime("inbox:pending", "approval_inbox", { status: "pending" });
    subscribeRealtime("sources:health", "fleet");
    subscribeRealtime("teams:list", "teams");
    subscribeRealtime("ledger:recent", "ledger", { window: "0:120" });

    return () => {
      unsubscribeRealtime("board:window");
      unsubscribeRealtime("inbox:pending");
      unsubscribeRealtime("sources:health");
      unsubscribeRealtime("teams:list");
      unsubscribeRealtime("ledger:recent");
    };
  }, []);

  useEffect(() => {
    if (
      devAutoConnectAttempted.current ||
      !goosewebConfig.flags.devTicketAutoConnect ||
      state.connection !== "idle"
    ) {
      return;
    }

    devAutoConnectAttempted.current = true;
    const pastedTicket = goosewebConfig.pastedDevTicket.trim();
    if (pastedTicket) {
      connectRealtime(pastedTicket);
      return;
    }

    if (!goosewebConfig.flags.devTicketRoute) {
      return;
    }

    void mintDevelopmentTicket()
      .then((ticket) => connectRealtime(ticket))
      .catch((error) => {
        console.error("Unable to auto-connect Gooseweb development ticket", error);
      });
  }, [state.connection]);

  useEffect(() => {
    subscribeRealtime("board:window", "board", {
      window: "0:120",
      source_id: filters.sourceId === "all" ? "" : filters.sourceId,
      team_id: filters.teamId === "all" ? "" : filters.teamId,
      status: filters.status === "all" ? "" : filters.status
    });
  }, [filters]);

  const selectedRow =
    fleetRows.find((row) => row.rowId === selectedRowId) ?? fleetRows[0];
  const selectedSession =
    sessions.find((session) => session.sessionId === selectedSessionId) ??
    sessions.find((session) => session.sessionId === selectedRow?.sessionId) ??
    sessions[0];
  const selectedAgentSession =
    sessions.find((session) => session.sessionId === selectedSessionId) ??
    (selectedSessionId ? selectedSession : undefined);
  const selectedTeam =
    teams.find((team) => team.teamId === selectedTeamId) ??
    teams.find((team) => team.teamId === selectedRow?.teamId) ??
    teams[0];
  const selectedApproval =
    approvals.find((approval) => approval.approvalId === selectedApprovalId) ??
    approvals.find((approval) => approval.sessionId === selectedSession?.sessionId) ??
    approvals[0];
  const selectedProcess =
    processes.find((process) => process.processId === selectedProcessId) ??
    processes[0];
  const selectedWorktree =
    worktrees.find(
      (worktree) =>
        worktree.path === selectedSession?.worktreePath ||
        worktree.path === selectedRow?.worktreePath
    ) ?? worktrees[0];

  useEffect(() => {
    if (!selectedRow) {
      return;
    }
    setSelectedRowId(selectedRow.rowId);
    if (selectedRow.sessionId) {
      subscribeRealtime(`session:${selectedRow.sessionId}`, "session", {
        session_id: selectedRow.sessionId
      });
    }
    if (selectedRow.teamId) {
      subscribeRealtime(`team:${selectedRow.teamId}`, "team", {
        team_id: selectedRow.teamId
      });
    }
  }, [selectedRow]);

  useEffect(() => {
    if (!selectedSession?.sessionId) {
      return;
    }
    subscribeRealtime(`session:${selectedSession.sessionId}`, "session", {
      session_id: selectedSession.sessionId
    });
  }, [selectedSession?.sessionId]);

  useEffect(() => {
    if (!selectedTeam?.teamId) {
      return;
    }
    subscribeRealtime(`team:${selectedTeam.teamId}`, "team", {
      team_id: selectedTeam.teamId
    });
  }, [selectedTeam?.teamId]);

  useEffect(() => {
    if (selectedProcess?.processId) {
      subscribeRealtime(`process:${selectedProcess.processId}`, "process", {
        process_id: selectedProcess.processId,
        tail: "visible"
      });
    }
  }, [selectedProcess?.processId]);

  const ledgerEvents = useMemo(
    () =>
      buildLedgerEvents({
        fleetRows,
        teams,
        approvals,
        processes,
        sources
      }),
    [approvals, fleetRows, processes, sources, teams]
  );
  const activeSubscriptions = subscriptions.filter(
    (subscription) => subscription.status !== "unsubscribed"
  );
  const staleSourceIds = Object.keys(state.staleSources);
  const sourceGapActive =
    state.connection === "stale" ||
    state.connection === "offline" ||
    state.connection === "connecting" ||
    state.connection === "reconnecting" ||
    staleSourceIds.length > 0;

  function addSelectedAgentToTeam() {
    setActiveView("teams");
  }

  return (
    <div className="mission-shell bg-background text-foreground">
      <MissionChrome
        state={state}
        sources={sources}
        subscriptionCount={activeSubscriptions.length}
      />
      <div className="mission-grid min-h-0">
        <MissionRosterRail
          activeView={activeView}
          approvals={approvals}
          rows={fleetRows}
          sessions={sessions}
          teams={teams}
          processes={processes}
          selectedRowId={selectedRow?.rowId ?? ""}
          selectedSessionId={selectedSessionId}
          selectedTeamId={selectedTeam?.teamId ?? ""}
          selectedApprovalId={selectedApproval?.approvalId ?? ""}
          selectedProcessId={selectedProcess?.processId ?? ""}
          sourceGapActive={sourceGapActive}
          addAgentDisabled={!sessions.length || !teams.length || sourceGapActive}
          onViewChange={setActiveView}
          onSelectRow={setSelectedRowId}
          onSelectSession={setSelectedSessionId}
          onSelectTeam={setSelectedTeamId}
          onSelectApproval={setSelectedApprovalId}
          onSelectProcess={setSelectedProcessId}
          onAddAgentToTeam={addSelectedAgentToTeam}
        />

        <main className="mission-center min-w-0 overflow-hidden">
          {sourceGapActive ? (
            <Alert variant="destructive" className="mission-alert">
              <ShieldAlertIcon />
              <AlertTitle>Source state is not command-safe</AlertTitle>
              <AlertDescription>
                Destructive approvals and runtime mutations are disabled until
                replay catches up or the source returns to a trusted state.
              </AlertDescription>
            </Alert>
          ) : null}

          <MissionWorkspace
            state={state}
            activeView={activeView}
            rows={fleetRows}
            sessions={sessions}
            teams={teams}
            approvals={approvals}
            processes={processes}
            sources={sources}
            filters={filters}
            setFilters={setFilters}
            selectedRow={selectedRow}
            selectedSession={
              activeView === "agents" ? selectedAgentSession : selectedSession
            }
            selectedTeam={selectedTeam}
            sessionDetails={sessionDetails}
            teamWorkspaces={teamWorkspaces}
            selectedApproval={selectedApproval}
            selectedRowId={selectedRow?.rowId ?? ""}
            selectedApprovalId={selectedApproval?.approvalId ?? ""}
            setSelectedRowId={setSelectedRowId}
            setSelectedSessionId={setSelectedSessionId}
            setSelectedTeamId={setSelectedTeamId}
            setSelectedApprovalId={setSelectedApprovalId}
            pendingCommands={pendingCommands}
            ledgerEvents={ledgerEvents}
            connection={state.connection}
            subscriptionCount={activeSubscriptions.length}
            sourceGapActive={sourceGapActive}
            staleSourceIds={staleSourceIds}
          />
        </main>

        <MissionProcessRail
          processes={processes}
          selectedProcess={selectedProcess}
          selectedRow={selectedRow}
          selectedSession={selectedSession}
          selectedTeam={selectedTeam}
          selectedApproval={selectedApproval}
          selectedWorktree={selectedWorktree}
          sources={sources}
          staleSourceIds={staleSourceIds}
          pendingCommandCount={pendingCommands.length}
          sourceGapActive={sourceGapActive}
          onSelectProcess={setSelectedProcessId}
        />
      </div>
    </div>
  );
}

function MissionChrome({
  state,
  sources,
  subscriptionCount
}: {
  readonly state: GoosewebSnapshot;
  readonly sources: readonly SourceHealthView[];
  readonly subscriptionCount: number;
}) {
  const source = sources[0];
  return (
    <header className="mission-chrome">
      <div className="mission-window-buttons" aria-hidden="true">
        <span className="mission-dot mission-dot-red" />
        <span className="mission-dot mission-dot-yellow" />
        <span className="mission-dot mission-dot-green" />
      </div>
      <div className="mission-chrome-tools">
        <Button size="icon-sm" type="button" variant="ghost">
          <FolderIcon />
        </Button>
        <Button size="icon-sm" type="button" variant="ghost">
          <SquareIcon />
        </Button>
        <Button size="icon-sm" type="button" variant="ghost">
          <ChevronDownIcon />
        </Button>
      </div>
      <div className="mission-titlebar">
        <span className="mission-titlebar-slot" />
      </div>
      <div className="mission-chrome-status">
        <ConnectionBadge connection={state.connection} />
        <MetricChip label="source" value={source?.displayName || source?.sourceId || "none"} />
        <MetricChip label="seq" value={state.cursor.gatewaySeq.toString()} />
        <MetricChip label="subs" value={String(subscriptionCount)} />
      </div>
    </header>
  );
}

function MissionRosterRail({
  activeView,
  approvals,
  rows,
  sessions,
  teams,
  processes,
  selectedRowId,
  selectedSessionId,
  selectedTeamId,
  selectedApprovalId,
  selectedProcessId,
  sourceGapActive,
  addAgentDisabled,
  onViewChange,
  onSelectRow,
  onSelectSession,
  onSelectTeam,
  onSelectApproval,
  onSelectProcess,
  onAddAgentToTeam
}: {
  readonly activeView: WorkspaceView;
  readonly approvals: readonly ApprovalView[];
  readonly rows: readonly FleetRowView[];
  readonly sessions: readonly SessionView[];
  readonly teams: readonly TeamView[];
  readonly processes: readonly ProcessView[];
  readonly selectedRowId: string;
  readonly selectedSessionId: string;
  readonly selectedTeamId: string;
  readonly selectedApprovalId: string;
  readonly selectedProcessId: string;
  readonly sourceGapActive: boolean;
  readonly addAgentDisabled: boolean;
  readonly onViewChange: (view: WorkspaceView) => void;
  readonly onSelectRow: (id: string) => void;
  readonly onSelectSession: (id: string) => void;
  readonly onSelectTeam: (id: string) => void;
  readonly onSelectApproval: (id: string) => void;
  readonly onSelectProcess: (id: string) => void;
  readonly onAddAgentToTeam: () => void;
}) {
  const items = getEntityItems({
    activeView,
    rows,
    sessions,
    teams,
    approvals,
    processes,
    selectedRowId,
    selectedSessionId,
    selectedTeamId,
    selectedApprovalId,
    selectedProcessId,
    onSelectRow,
    onSelectSession,
    onSelectTeam,
    onSelectApproval,
    onSelectProcess
  });
  const visibleItems = items.slice(0, 9);

  return (
    <aside className="mission-roster">
      <div className="mission-roster-scroll">
        <div className="mission-nav-grid">
          {NAV_ITEMS.map((item) => {
            const Icon = item.icon;
            const count =
              item.id === "inbox"
                ? approvals.filter((approval) => approval.status === "pending").length
                : item.id === "fleet"
                  ? processes.filter((process) => process.status === "running").length
                  : undefined;
            return (
              <Button
                className="mission-nav-button"
                key={item.id}
                type="button"
                variant={activeView === item.id ? "secondary" : "ghost"}
                onClick={() => onViewChange(item.id)}
              >
                <Icon data-icon="inline-start" />
                <span className="truncate">{item.label}</span>
                {count ? <Badge variant="outline">{count}</Badge> : null}
              </Button>
            );
          })}
        </div>

        <Separator className="mission-separator" />

        <div className="mission-rail-section">
          <div className="mission-section-label">
            <span>{sidebarTitle(activeView)}</span>
            <Badge variant={sourceGapActive ? "destructive" : "outline"}>
              {items.length}
            </Badge>
          </div>
          <div className="mission-roster-list">
            {visibleItems.length === 0 ? (
              <EmptyBlock title="No entities" detail="Waiting for realtime snapshots." />
            ) : (
              visibleItems.map((item) => (
                <button
                  className={cn(
                    "mission-roster-card",
                    item.selected && "mission-roster-card-active"
                  )}
                  key={item.id}
                  type="button"
                  onClick={item.onClick}
                >
                  <span className="mission-roster-card-main">
                    <span className="truncate text-[0.95rem] font-medium">
                      {item.title}
                    </span>
                    <span className="truncate text-xs text-muted-foreground">
                      {item.meta}
                    </span>
                  </span>
                  <span className="mission-roster-card-side">
                    <StatusBadge status={item.status} />
                  </span>
                </button>
              ))
            )}
          </div>
        </div>
      </div>

      <div className="mission-roster-actions">
        <Button
          disabled={addAgentDisabled}
          type="button"
          variant="outline"
          onClick={onAddAgentToTeam}
        >
          <PlusIcon data-icon="inline-start" />
          Add Agent to Team
        </Button>
        <Button type="button" variant="outline" onClick={() => onViewChange("teams")}>
          <RadioIcon data-icon="inline-start" />
          Team Comms
        </Button>
        <Button type="button" variant="outline" onClick={() => onViewChange("settings")}>
          <ClipboardListIcon data-icon="inline-start" />
          Docs
        </Button>
      </div>
    </aside>
  );
}

function MissionWorkspace({
  state,
  activeView,
  rows,
  sessions,
  teams,
  approvals,
  processes,
  sources,
  filters,
  setFilters,
  selectedRow,
  selectedSession,
  selectedTeam,
  sessionDetails,
  teamWorkspaces,
  selectedApproval,
  selectedRowId,
  selectedApprovalId,
  setSelectedRowId,
  setSelectedSessionId,
  setSelectedTeamId,
  setSelectedApprovalId,
  pendingCommands,
  ledgerEvents,
  connection,
  subscriptionCount,
  sourceGapActive,
  staleSourceIds
}: {
  readonly state: GoosewebSnapshot;
  readonly activeView: WorkspaceView;
  readonly rows: readonly FleetRowView[];
  readonly sessions: readonly SessionView[];
  readonly teams: readonly TeamView[];
  readonly approvals: readonly ApprovalView[];
  readonly processes: readonly ProcessView[];
  readonly sources: readonly SourceHealthView[];
  readonly filters: BoardFilters;
  readonly setFilters: (filters: BoardFilters) => void;
  readonly selectedRow?: FleetRowView;
  readonly selectedSession?: SessionView;
  readonly selectedTeam?: TeamView;
  readonly sessionDetails: Readonly<Record<string, SessionDetailState>>;
  readonly teamWorkspaces: Readonly<Record<string, TeamWorkspaceState>>;
  readonly selectedApproval?: ApprovalView;
  readonly selectedRowId: string;
  readonly selectedApprovalId: string;
  readonly setSelectedRowId: (id: string) => void;
  readonly setSelectedSessionId: (id: string) => void;
  readonly setSelectedTeamId: (id: string) => void;
  readonly setSelectedApprovalId: (id: string) => void;
  readonly pendingCommands: readonly PendingCommandState[];
  readonly ledgerEvents: readonly LedgerEvent[];
  readonly connection: ConnectionState;
  readonly subscriptionCount: number;
  readonly sourceGapActive: boolean;
  readonly staleSourceIds: readonly string[];
}) {
  const [composerText, setComposerText] = useState("");
  const hasAgentThreadComposer =
    activeView === "agents" && Boolean(selectedSession?.sessionId);
  const isAgentThread = activeView === "agents" && Boolean(selectedSession);

  useEffect(() => {
    if (!hasAgentThreadComposer && composerText) {
      setComposerText("");
    }
  }, [composerText, hasAgentThreadComposer]);

  function submitComposer(event: FormEvent) {
    event.preventDefault();
    if (
      !hasAgentThreadComposer ||
      !selectedSession ||
      !composerText.trim() ||
      sourceGapActive
    ) {
      return;
    }
    sendRealtimeCommand(
      makeCommand("session", selectedSession.sessionId, "sendTurn", {
        sessionId: selectedSession.sessionId,
        text: composerText.trim()
      })
    );
    setComposerText("");
  }

  return (
    <section
      className={cn(
        "mission-workspace",
        isAgentThread
          ? "mission-workspace-thread"
          : "mission-workspace-dashboard"
      )}
    >
      {isAgentThread ? (
        <>
          <div className="mission-workspace-tab" aria-hidden="true" />
          <div className="mission-workspace-header">
            <div>
              <div className="mission-kicker">
                Thinking
                <ChevronDownIcon />
              </div>
              <h1>
                {workspaceTitle(activeView, selectedRow, selectedSession, selectedTeam)}
              </h1>
            </div>
            <div className="mission-header-metrics">
              <ConnectionBadge connection={connection} />
              <MetricChip label="subs" value={String(subscriptionCount)} />
              <MetricChip
                label="stale"
                value={staleSourceIds.length ? staleSourceIds.join(", ") : "none"}
              />
            </div>
          </div>

          <ScrollArea className="mission-workspace-scroll">
            <div className="mission-worklog">
              <WorklogNarrative
                selectedRow={selectedRow}
                selectedSession={selectedSession}
                selectedTeam={selectedTeam}
                selectedApproval={selectedApproval}
                processes={processes}
                sourceGapActive={sourceGapActive}
              />
              <InlineToolStack
                rows={rows}
                approvals={approvals}
                teams={teams}
                processes={processes}
                sources={sources}
              />
              <div className="mission-embedded-pane">
                <MissionViewBody
                  state={state}
                  activeView={activeView}
                  rows={rows}
                  sessions={sessions}
                  teams={teams}
                  approvals={approvals}
                  processes={processes}
                  sources={sources}
                  filters={filters}
                  setFilters={setFilters}
                  selectedRowId={selectedRowId}
                  selectedSession={selectedSession}
                  selectedTeam={selectedTeam}
                  sessionDetails={sessionDetails}
                  teamWorkspaces={teamWorkspaces}
                  selectedApproval={selectedApproval}
                  selectedApprovalId={selectedApprovalId}
                  setSelectedRowId={setSelectedRowId}
                  setSelectedSessionId={setSelectedSessionId}
                  setSelectedTeamId={setSelectedTeamId}
                  setSelectedApprovalId={setSelectedApprovalId}
                  pendingCommands={pendingCommands}
                  ledgerEvents={ledgerEvents}
                  connection={connection}
                  subscriptionCount={subscriptionCount}
                  sourceGapActive={sourceGapActive}
                />
              </div>
            </div>
          </ScrollArea>
        </>
      ) : (
        <DashboardWorkspace
          state={state}
          activeView={activeView}
          rows={rows}
          sessions={sessions}
          teams={teams}
          approvals={approvals}
          processes={processes}
          sources={sources}
          filters={filters}
          setFilters={setFilters}
          selectedRow={selectedRow}
          selectedSession={selectedSession}
          selectedTeam={selectedTeam}
          sessionDetails={sessionDetails}
          teamWorkspaces={teamWorkspaces}
          selectedApproval={selectedApproval}
          selectedRowId={selectedRowId}
          selectedApprovalId={selectedApprovalId}
          setSelectedRowId={setSelectedRowId}
          setSelectedSessionId={setSelectedSessionId}
          setSelectedTeamId={setSelectedTeamId}
          setSelectedApprovalId={setSelectedApprovalId}
          pendingCommands={pendingCommands}
          ledgerEvents={ledgerEvents}
          connection={connection}
          subscriptionCount={subscriptionCount}
          sourceGapActive={sourceGapActive}
          staleSourceIds={staleSourceIds}
        />
      )}

      {hasAgentThreadComposer ? (
        <form className="mission-composer" onSubmit={submitComposer}>
          <Textarea
            aria-label="Agent thread composer"
            value={composerText}
            onChange={(event) => setComposerText(event.target.value)}
            placeholder=""
            rows={4}
          />
          <div className="mission-composer-tray">
            <div className="mission-composer-tools">
              <Button size="icon-sm" type="button" variant="ghost">
                <PlusIcon />
              </Button>
              <SelectFilter
                value={selectedSession?.model || "default"}
                options={unique(["default", "medium", "high", selectedSession?.model || "default"])}
                onChange={() => undefined}
              />
              <SelectFilter
                value={selectedSession?.provider || "runtime"}
                options={unique(["runtime", "codex", "claude", selectedSession?.provider || "runtime"])}
                onChange={() => undefined}
              />
              <MetricChip label="target" value={selectedSession?.sessionId || "none"} />
            </div>
            <div className="mission-composer-actions">
              <span className="mission-context-gauge">
                <CircleIcon />
                36% left
              </span>
              <Button
                disabled={!composerText.trim() || sourceGapActive}
                size="icon-lg"
                type="submit"
                variant="secondary"
              >
                <SquareIcon />
              </Button>
            </div>
          </div>
        </form>
      ) : null}
    </section>
  );
}

function MissionViewBody({
  state,
  activeView,
  rows,
  sessions,
  teams,
  approvals,
  processes,
  sources,
  filters,
  setFilters,
  selectedRowId,
  selectedSession,
  selectedTeam,
  sessionDetails,
  teamWorkspaces,
  selectedApproval,
  selectedApprovalId,
  setSelectedRowId,
  setSelectedSessionId,
  setSelectedTeamId,
  setSelectedApprovalId,
  pendingCommands,
  ledgerEvents,
  connection,
  subscriptionCount,
  sourceGapActive
}: {
  readonly state: GoosewebSnapshot;
  readonly activeView: WorkspaceView;
  readonly rows: readonly FleetRowView[];
  readonly sessions: readonly SessionView[];
  readonly teams: readonly TeamView[];
  readonly approvals: readonly ApprovalView[];
  readonly processes: readonly ProcessView[];
  readonly sources: readonly SourceHealthView[];
  readonly filters: BoardFilters;
  readonly setFilters: (filters: BoardFilters) => void;
  readonly selectedRowId: string;
  readonly selectedSession?: SessionView;
  readonly selectedTeam?: TeamView;
  readonly sessionDetails: Readonly<Record<string, SessionDetailState>>;
  readonly teamWorkspaces: Readonly<Record<string, TeamWorkspaceState>>;
  readonly selectedApproval?: ApprovalView;
  readonly selectedApprovalId: string;
  readonly setSelectedRowId: (id: string) => void;
  readonly setSelectedSessionId: (id: string) => void;
  readonly setSelectedTeamId: (id: string) => void;
  readonly setSelectedApprovalId: (id: string) => void;
  readonly pendingCommands: readonly PendingCommandState[];
  readonly ledgerEvents: readonly LedgerEvent[];
  readonly connection: ConnectionState;
  readonly subscriptionCount: number;
  readonly sourceGapActive: boolean;
}) {
  if (activeView === "agents") {
    return (
      <AgentPane
        sessions={sessions}
        approvals={approvals}
        processes={processes}
        sources={sources}
        selectedSession={selectedSession}
        sessionDetail={
          selectedSession ? sessionDetails[selectedSession.sessionId] : undefined
        }
        selectedApproval={selectedApproval}
        setSelectedSessionId={setSelectedSessionId}
        sourceGapActive={sourceGapActive}
      />
    );
  }
  if (activeView === "teams") {
    return (
      <TeamPane
        teams={teams}
        rows={rows}
        sessions={sessions}
        sources={sources}
        selectedTeam={selectedTeam}
        teamWorkspace={selectedTeam ? teamWorkspaces[selectedTeam.teamId] : undefined}
        setSelectedTeamId={setSelectedTeamId}
        pendingCommands={pendingCommands}
        sourceGapActive={sourceGapActive}
      />
    );
  }
  if (activeView === "inbox") {
    return (
      <InboxPane
        approvals={approvals}
        selectedApprovalId={selectedApprovalId}
        setSelectedApprovalId={setSelectedApprovalId}
        sourceGapActive={sourceGapActive}
      />
    );
  }
  if (activeView === "ledger") {
    return <LedgerPane events={ledgerEvents} sources={sources} />;
  }
  if (activeView === "fleet") {
    return (
      <FleetPane
        sources={sources}
        rows={rows}
        processes={processes}
        connection={connection}
      />
    );
  }
  if (activeView === "playbooks") {
    return (
      <PlaybooksPane
        selectedSession={selectedSession}
        selectedTeam={selectedTeam}
        sourceGapActive={sourceGapActive}
      />
    );
  }
  if (activeView === "settings") {
    return <SettingsPane state={state} subscriptionCount={subscriptionCount} />;
  }
  return (
    <BoardPane
      rows={rows}
      teams={teams}
      sources={sources}
      filters={filters}
      setFilters={setFilters}
      selectedRowId={selectedRowId}
      setSelectedRowId={setSelectedRowId}
    />
  );
}

function DashboardWorkspace({
  state,
  activeView,
  rows,
  sessions,
  teams,
  approvals,
  processes,
  sources,
  filters,
  setFilters,
  selectedRow,
  selectedSession,
  selectedTeam,
  sessionDetails,
  teamWorkspaces,
  selectedApproval,
  selectedRowId,
  selectedApprovalId,
  setSelectedRowId,
  setSelectedSessionId,
  setSelectedTeamId,
  setSelectedApprovalId,
  pendingCommands,
  ledgerEvents,
  connection,
  subscriptionCount,
  sourceGapActive,
  staleSourceIds
}: {
  readonly state: GoosewebSnapshot;
  readonly activeView: WorkspaceView;
  readonly rows: readonly FleetRowView[];
  readonly sessions: readonly SessionView[];
  readonly teams: readonly TeamView[];
  readonly approvals: readonly ApprovalView[];
  readonly processes: readonly ProcessView[];
  readonly sources: readonly SourceHealthView[];
  readonly filters: BoardFilters;
  readonly setFilters: (filters: BoardFilters) => void;
  readonly selectedRow?: FleetRowView;
  readonly selectedSession?: SessionView;
  readonly selectedTeam?: TeamView;
  readonly sessionDetails: Readonly<Record<string, SessionDetailState>>;
  readonly teamWorkspaces: Readonly<Record<string, TeamWorkspaceState>>;
  readonly selectedApproval?: ApprovalView;
  readonly selectedRowId: string;
  readonly selectedApprovalId: string;
  readonly setSelectedRowId: (id: string) => void;
  readonly setSelectedSessionId: (id: string) => void;
  readonly setSelectedTeamId: (id: string) => void;
  readonly setSelectedApprovalId: (id: string) => void;
  readonly pendingCommands: readonly PendingCommandState[];
  readonly ledgerEvents: readonly LedgerEvent[];
  readonly connection: ConnectionState;
  readonly subscriptionCount: number;
  readonly sourceGapActive: boolean;
  readonly staleSourceIds: readonly string[];
}) {
  const runningProcesses = processes.filter((process) => process.status === "running").length;
  const pendingApprovals = approvals.filter((approval) => approval.status === "pending").length;
  const title = dashboardTitle(activeView);
  const description = dashboardDescription(activeView);

  return (
    <>
      <div className="mission-dashboard-header">
        <div className="min-w-0">
          <div className="mission-dashboard-kicker">{title.kicker}</div>
          <h1>{title.heading}</h1>
          <p>{description}</p>
        </div>
        <div className="mission-header-metrics">
          <ConnectionBadge connection={connection} />
          <MetricChip label="subs" value={String(subscriptionCount)} />
          <MetricChip
            label="stale"
            value={staleSourceIds.length ? staleSourceIds.join(", ") : "none"}
          />
        </div>
      </div>

      <div className="mission-dashboard-stats">
        <MetricCard label="board rows" value={String(rows.length)} />
        <MetricCard label="pending approvals" value={String(pendingApprovals)} />
        <MetricCard label="running processes" value={String(runningProcesses)} />
        <MetricCard label="runtime sources" value={String(sources.length)} />
      </div>

      <div className="mission-dashboard-body">
        <MissionViewBody
          state={state}
          activeView={activeView}
          rows={rows}
          sessions={sessions}
          teams={teams}
          approvals={approvals}
          processes={processes}
          sources={sources}
          filters={filters}
          setFilters={setFilters}
          selectedRowId={selectedRowId}
          selectedSession={selectedSession}
          selectedTeam={selectedTeam}
          sessionDetails={sessionDetails}
          teamWorkspaces={teamWorkspaces}
          selectedApproval={selectedApproval}
          selectedApprovalId={selectedApprovalId}
          setSelectedRowId={setSelectedRowId}
          setSelectedSessionId={setSelectedSessionId}
          setSelectedTeamId={setSelectedTeamId}
          setSelectedApprovalId={setSelectedApprovalId}
          pendingCommands={pendingCommands}
          ledgerEvents={ledgerEvents}
          connection={connection}
          subscriptionCount={subscriptionCount}
          sourceGapActive={sourceGapActive}
        />
      </div>

      {activeView === "board" ? (
        <div className="mission-dashboard-inspector">
          <ContextCard
            title="Selected row"
            items={[
              ["row", selectedRow?.rowId],
              ["session", selectedRow?.sessionId],
              ["team", selectedRow?.teamId],
              ["source", selectedRow?.sourceId],
              ["worktree", selectedRow?.worktreePath]
            ]}
          />
        </div>
      ) : null}
    </>
  );
}

function WorklogNarrative({
  selectedRow,
  selectedSession,
  selectedTeam,
  selectedApproval,
  processes,
  sourceGapActive
}: {
  readonly selectedRow?: FleetRowView;
  readonly selectedSession?: SessionView;
  readonly selectedTeam?: TeamView;
  readonly selectedApproval?: ApprovalView;
  readonly processes: readonly ProcessView[];
  readonly sourceGapActive: boolean;
}) {
  const runningCount = processes.filter((process) => process.status === "running").length;
  const codeValue = selectedRow?.sourceId || selectedSession?.sourceId || "source none";
  const selectedLabel =
    selectedRow?.title || selectedSession?.sessionId || selectedTeam?.name || "agent thread";
  const muted = `The selected agent thread is ${selectedLabel}. Gooseweb is keeping the realtime worker, subscriptions, approvals, command queue, and process visibility active while this operator surface is rendered as a dense desktop control room.`;
  const primary = (
    <>
      The active session is <code>{selectedSession?.sessionId || "none"}</code>,
      the current source is <code>{codeValue}</code>, and there are{" "}
      <code>{String(runningCount)}</code> running processes. Command mutation
      controls are available only for this selected agent thread.
    </>
  );
  const approval = selectedApproval
    ? `Approval context is ${selectedApproval.status || "unknown"} with ${selectedApproval.risk || "unknown"} risk.`
    : sourceGapActive
      ? "Source state is stale or reconnecting, so runtime mutation controls are guarded."
      : undefined;

  return (
    <div className="mission-narrative">
      <p className="mission-muted-copy">{muted}</p>
      <p className="mission-primary-copy">{primary}</p>
      {approval ? (
        <p className="mission-primary-copy">
          {approval}
        </p>
      ) : null}
    </div>
  );
}

function InlineToolStack({
  rows,
  approvals,
  teams,
  processes,
  sources
}: {
  readonly rows: readonly FleetRowView[];
  readonly approvals: readonly ApprovalView[];
  readonly teams: readonly TeamView[];
  readonly processes: readonly ProcessView[];
  readonly sources: readonly SourceHealthView[];
}) {
  const items = [
    {
      id: "board",
      label: `Read ${rows.length} board row${rows.length === 1 ? "" : "s"}`,
      detail: `${approvals.filter((approval) => approval.status === "pending").length} pending approvals`
    },
    {
      id: "teams",
      label: `Read ${teams.length} team snapshot${teams.length === 1 ? "" : "s"}`,
      detail: `${teams.reduce((sum, team) => sum + team.members.length, 0)} members materialized`
    },
    {
      id: "processes",
      label: `Read ${processes.length} process record${processes.length === 1 ? "" : "s"}`,
      detail: `${processes.filter((process) => process.status === "running").length} running`
    },
    {
      id: "sources",
      label: `Read ${sources.length} runtime source${sources.length === 1 ? "" : "s"}`,
      detail: sources[0]?.health || "health unknown"
    }
  ];

  return (
    <div className="mission-tool-stack">
      {items.map((item) => (
        <div className="mission-tool-card" key={item.id}>
          <span className="mission-tool-led" />
          <span className="min-w-0 flex-1">
            <span className="block truncate font-medium">{item.label}</span>
            <span className="block truncate text-xs text-muted-foreground">
              {item.detail}
            </span>
          </span>
          <span className="mission-tool-braces" aria-hidden="true">
            {"{}"}
          </span>
        </div>
      ))}
    </div>
  );
}

function MissionProcessRail({
  processes,
  selectedProcess,
  selectedRow,
  selectedSession,
  selectedTeam,
  selectedApproval,
  selectedWorktree,
  sources,
  staleSourceIds,
  pendingCommandCount,
  sourceGapActive,
  onSelectProcess
}: {
  readonly processes: readonly ProcessView[];
  readonly selectedProcess?: ProcessView;
  readonly selectedRow?: FleetRowView;
  readonly selectedSession?: SessionView;
  readonly selectedTeam?: TeamView;
  readonly selectedApproval?: ApprovalView;
  readonly selectedWorktree?: WorktreeView;
  readonly sources: readonly SourceHealthView[];
  readonly staleSourceIds: readonly string[];
  readonly pendingCommandCount: number;
  readonly sourceGapActive: boolean;
  readonly onSelectProcess: (id: string) => void;
}) {
  const [filter, setFilter] = useState<"running" | "completed" | "all">("running");
  const filteredProcesses = processes.filter((process) => {
    if (filter === "all") {
      return true;
    }
    if (filter === "running") {
      return process.status === "running";
    }
    return process.status !== "running";
  });

  return (
    <aside className="mission-processes">
      <div className="mission-process-header">
        <h2>Processes</h2>
        <ToggleGroup
          className="mission-process-toggle"
          onValueChange={(value) => {
            const next = Array.isArray(value) ? value[0] : value;
            if (next === "running" || next === "completed" || next === "all") {
              setFilter(next);
            }
          }}
          value={[filter]}
          variant="outline"
        >
          <ToggleGroupItem value="running">Running</ToggleGroupItem>
          <ToggleGroupItem value="completed">Completed</ToggleGroupItem>
          <ToggleGroupItem value="all">All</ToggleGroupItem>
        </ToggleGroup>
      </div>
      <Separator className="mission-separator" />
      <ScrollArea className="mission-process-scroll">
        <div className="mission-process-list">
          {filteredProcesses.length === 0 ? (
            <EmptyBlock title="No processes" detail="Process materialization is empty." />
          ) : (
            filteredProcesses.map((process) => (
              <div
                className={cn(
                  "mission-process-card",
                  process.processId === selectedProcess?.processId &&
                    "mission-process-card-active"
                )}
                key={process.processId}
                role="button"
                tabIndex={0}
                onClick={() => onSelectProcess(process.processId)}
                onKeyDown={(event) => {
                  if (event.key === "Enter" || event.key === " ") {
                    onSelectProcess(process.processId);
                  }
                }}
              >
                <span className="mission-process-card-top">
                  <StatusBadge status={process.status || "unknown"} />
                  <span className="text-xs text-muted-foreground">
                    {process.sourceId || "source"}
                  </span>
                  <Button
                    disabled={sourceGapActive || process.status !== "running"}
                    size="xs"
                    type="button"
                    variant="destructive"
                    onClick={(event) => {
                      event.stopPropagation();
                      sendRealtimeCommand(
                        makeCommand("process", process.processId, "killProcess", {
                          processId: process.processId
                        })
                      );
                    }}
                  >
                    Kill
                  </Button>
                </span>
                <span className="mission-process-command">
                  {process.command || process.processId}
                </span>
                <span className="mission-process-meta">
                  <span>source_id {process.sourceId || "unknown"}</span>
                  <span>process_id {process.processId}</span>
                  <span>exit_code {String(process.exitCode)}</span>
                  <span>status {process.status || "unknown"}</span>
                </span>
              </div>
            ))
          )}
        </div>
        <div className="mission-context-stack">
          <ContextCard
            title="Selection"
            items={[
              ["row", selectedRow?.rowId],
              ["session", selectedSession?.sessionId],
              ["team", selectedTeam?.teamId],
              ["approval", selectedApproval?.approvalId],
              ["process", selectedProcess?.processId],
              ["worktree", selectedWorktree?.path]
            ]}
          />
          <ContextCard
            title="Source health"
            items={sources.map((source) => [
              source.displayName || source.sourceId,
              `${source.health} / ${ageFrom(toNumber(source.observedAtUnixMs))}`
            ])}
          />
          <ContextCard
            title="Safety"
            items={[
              ["stale sources", staleSourceIds.length ? staleSourceIds.join(", ") : "none"],
              ["pending commands", String(pendingCommandCount)]
            ]}
          />
        </div>
      </ScrollArea>
    </aside>
  );
}

function workspaceTitle(
  activeView: WorkspaceView,
  selectedRow?: FleetRowView,
  selectedSession?: SessionView,
  selectedTeam?: TeamView
): string {
  if (activeView === "teams") {
    return selectedTeam?.name || "Coordinating team workspace";
  }
  if (activeView === "agents") {
    return selectedSession?.sessionId || "Investigating agent session";
  }
  if (activeView === "inbox") {
    return "Resolving approval queue";
  }
  if (activeView === "fleet") {
    return "Inspecting runtime source health";
  }
  if (activeView === "ledger") {
    return "Auditing gateway event flow";
  }
  return selectedRow?.title || selectedRow?.sessionId || "Investigating source health issues";
}

function dashboardTitle(view: WorkspaceView): { readonly kicker: string; readonly heading: string } {
  switch (view) {
    case "inbox":
      return { kicker: "Approval operations", heading: "Inbox" };
    case "teams":
      return { kicker: "Coordination operations", heading: "Teams" };
    case "agents":
      return { kicker: "Agent workspace", heading: "Select an agent session" };
    case "ledger":
      return { kicker: "Audit operations", heading: "Ledger" };
    case "fleet":
      return { kicker: "Runtime operations", heading: "Fleet" };
    case "playbooks":
      return { kicker: "Template operations", heading: "Playbooks" };
    case "settings":
      return { kicker: "Admin operations", heading: "Settings" };
    case "board":
    default:
      return { kicker: "Mission board", heading: "Board" };
  }
}

function dashboardDescription(view: WorkspaceView): string {
  switch (view) {
    case "inbox":
      return "Review pending approvals, stale-source guards, and command-safe resolution controls.";
    case "teams":
      return "Inspect team membership, delivery state, and coordination commands without chat-thread chrome.";
    case "agents":
      return "Choose a session from the roster to open the agent thread and reveal the anchored composer.";
    case "ledger":
      return "Filter runtime and gateway events by source, scope, cursor, and criticality.";
    case "fleet":
      return "Track runtime source health, replay lag, process capacity, and future provisioning controls.";
    case "playbooks":
      return "Send prepared command or team-message templates to explicit selected targets.";
    case "settings":
      return "Manage gateway connection, protocol state, feature flags, and debug exports.";
    case "board":
    default:
      return "Scan active sessions, source ownership, approvals, processes, worktrees, and latest activity.";
  }
}

function BoardPane({
  rows,
  teams,
  sources,
  filters,
  setFilters,
  selectedRowId,
  setSelectedRowId
}: {
  readonly rows: readonly FleetRowView[];
  readonly teams: readonly TeamView[];
  readonly sources: readonly SourceHealthView[];
  readonly filters: BoardFilters;
  readonly setFilters: (filters: BoardFilters) => void;
  readonly selectedRowId: string;
  readonly setSelectedRowId: (id: string) => void;
}) {
  const filteredRows = rows.filter((row) => {
    return (
      (filters.sourceId === "all" || row.sourceId === filters.sourceId) &&
      (filters.teamId === "all" || row.teamId === filters.teamId) &&
      (filters.status === "all" || row.status === filters.status)
    );
  });
  const virtual = useVirtualRows(filteredRows, 44, 10);

  return (
    <Card className="h-full min-h-0">
      <CardHeader className="border-b">
        <CardTitle>Board</CardTitle>
        <CardDescription>
          Virtualized runtime rows; selection drives detail subscriptions.
        </CardDescription>
        <CardAction className="flex items-center gap-2">
          <SelectFilter
            value={filters.sourceId}
            options={["all", ...sources.map((source) => source.sourceId)]}
            onChange={(sourceId) => setFilters({ ...filters, sourceId })}
          />
          <SelectFilter
            value={filters.teamId}
            options={["all", ...teams.map((team) => team.teamId)]}
            onChange={(teamId) => setFilters({ ...filters, teamId })}
          />
          <SelectFilter
            value={filters.status}
            options={["all", ...unique(rows.map((row) => row.status).filter(Boolean))]}
            onChange={(status) => setFilters({ ...filters, status })}
          />
        </CardAction>
      </CardHeader>
      <CardContent className="min-h-0 flex-1 p-0">
        <div className="h-full overflow-auto" ref={virtual.containerRef}>
          <Table>
            <TableHeader className="sticky top-0 z-10 bg-card">
              <TableRow>
                <TableHead>Agent</TableHead>
                <TableHead>Status</TableHead>
                <TableHead>Active turn</TableHead>
                <TableHead>Provider/model</TableHead>
                <TableHead>Approvals</TableHead>
                <TableHead>Process</TableHead>
                <TableHead>Worktree</TableHead>
                <TableHead>Latest</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {virtual.topPadding > 0 ? <SpacerRow height={virtual.topPadding} colSpan={8} /> : null}
              {virtual.visibleItems.map((row) => (
                <TableRow
                  className={cn(
                    "cursor-pointer",
                    row.rowId === selectedRowId && "bg-muted/60"
                  )}
                  key={row.rowId}
                  onClick={() => setSelectedRowId(row.rowId)}
                >
                  <TableCell>
                    <div className="grid gap-0.5">
                      <span className="truncate font-medium">
                        {row.title || row.sessionId || row.rowId}
                      </span>
                      <span className="truncate text-xs text-muted-foreground">
                        {row.sourceId}
                      </span>
                    </div>
                  </TableCell>
                  <TableCell><StatusBadge status={row.status} /></TableCell>
                  <TableCell>{row.sessionId || "none"}</TableCell>
                  <TableCell>{row.provider || "unknown"} / {row.model || "default"}</TableCell>
                  <TableCell>{row.pendingApprovalCount}</TableCell>
                  <TableCell>{row.status === "running" ? "active" : "idle"}</TableCell>
                  <TableCell className="max-w-48 truncate">{row.worktreePath || "unassigned"}</TableCell>
                  <TableCell>{formatTime(toNumber(row.latestActivityUnixMs))}</TableCell>
                </TableRow>
              ))}
              {virtual.bottomPadding > 0 ? <SpacerRow height={virtual.bottomPadding} colSpan={8} /> : null}
            </TableBody>
          </Table>
        </div>
      </CardContent>
    </Card>
  );
}

function AgentPane({
  sessions,
  approvals,
  processes,
  sources,
  selectedSession,
  sessionDetail,
  selectedApproval,
  setSelectedSessionId,
  sourceGapActive
}: {
  readonly sessions: readonly SessionView[];
  readonly approvals: readonly ApprovalView[];
  readonly processes: readonly ProcessView[];
  readonly sources: readonly SourceHealthView[];
  readonly selectedSession?: SessionView;
  readonly sessionDetail?: SessionDetailState;
  readonly selectedApproval?: ApprovalView;
  readonly setSelectedSessionId: (id: string) => void;
  readonly sourceGapActive: boolean;
}) {
  const defaultSourceId = selectedSession?.sourceId || sources[0]?.sourceId || "";
  const [createSourceId, setCreateSourceId] = useState(defaultSourceId);
  const [createTitle, setCreateTitle] = useState("Lead agent");
  const [createProvider, setCreateProvider] = useState("codex");
  const [createModel, setCreateModel] = useState("gpt-5.4");
  const [createCwd, setCreateCwd] = useState("");

  useEffect(() => {
    if (!createSourceId && defaultSourceId) {
      setCreateSourceId(defaultSourceId);
    }
  }, [createSourceId, defaultSourceId]);

  const sessionApprovals = approvals.filter(
    (approval) => approval.sessionId === selectedSession?.sessionId
  );
  const transcriptItems = sessionDetail?.transcript ?? [];
  const latestTranscript = transcriptItems.at(-1);
  const timeline = [
    ...transcriptItems.map((entry) => ({
      id: entry.id,
      title: entry.role === "user" ? "User message" : "Assistant response",
      meta: entry.text
    })),
    ...sessionApprovals.map((approval) => ({
      id: approval.approvalId,
      title: approval.summary || "Approval requested",
      meta: `${approval.status} / ${approval.risk || "unknown risk"}`
    })),
    ...processes.map((process) => ({
      id: process.processId,
      title: process.command || process.processId,
      meta: process.status
    }))
  ];

  function createSession() {
    const sourceId = createSourceId || defaultSourceId;
    if (!sourceId || !createProvider.trim() || sourceGapActive) {
      return;
    }
    sendRealtimeCommand(
      makeCommand("source", sourceId, "createSession", {
        provider: createProvider.trim(),
        model: createModel.trim(),
        cwd: createCwd.trim(),
        title: createTitle.trim(),
        permissionMode: "",
        metadata: {}
      })
    );
  }

  return (
    <div className="grid h-full min-h-0 grid-cols-[minmax(0,1fr)_19rem] gap-3">
      <Card className="min-h-0">
        <CardHeader className="border-b">
          <CardTitle>Agent workspace</CardTitle>
          <CardDescription>{selectedSession?.sessionId || "No session selected"}</CardDescription>
          <CardAction className="flex gap-2">
            <SelectFilter
              value={selectedSession?.sessionId ?? ""}
              options={sessions.map((session) => session.sessionId)}
              onChange={setSelectedSessionId}
            />
            <Button
              disabled={!selectedSession?.activeTurnId || sourceGapActive}
              type="button"
              variant="destructive"
              onClick={() =>
                selectedSession &&
                sendRealtimeCommand(
                  makeCommand("session", selectedSession.sessionId, "interruptTurn", {
                    sessionId: selectedSession.sessionId,
                    turnId: selectedSession.activeTurnId
                  })
                )
              }
            >
              <SquareIcon data-icon="inline-start" />
              Interrupt
            </Button>
          </CardAction>
        </CardHeader>
        <CardContent className="flex min-h-0 flex-1 flex-col gap-3">
          <form
            className="grid gap-3 rounded-md border bg-muted/20 p-3"
            onSubmit={(event) => {
              event.preventDefault();
              createSession();
            }}
          >
            <div className="grid grid-cols-2 gap-2">
              <Field>
                <FieldLabel htmlFor="create-agent-source">Source</FieldLabel>
                <SelectFilter
                  value={createSourceId || defaultSourceId}
                  options={sources.map((source) => source.sourceId)}
                  onChange={setCreateSourceId}
                />
              </Field>
              <Field>
                <FieldLabel htmlFor="create-agent-title">Title</FieldLabel>
                <Input
                  id="create-agent-title"
                  value={createTitle}
                  onChange={(event) => setCreateTitle(event.target.value)}
                />
              </Field>
              <Field>
                <FieldLabel htmlFor="create-agent-provider">Provider</FieldLabel>
                <Input
                  id="create-agent-provider"
                  value={createProvider}
                  onChange={(event) => setCreateProvider(event.target.value)}
                />
              </Field>
              <Field>
                <FieldLabel htmlFor="create-agent-model">Model</FieldLabel>
                <Input
                  id="create-agent-model"
                  value={createModel}
                  onChange={(event) => setCreateModel(event.target.value)}
                />
              </Field>
            </div>
            <Field>
              <FieldLabel htmlFor="create-agent-cwd">Working directory</FieldLabel>
              <Input
                id="create-agent-cwd"
                value={createCwd}
                onChange={(event) => setCreateCwd(event.target.value)}
                placeholder="/path/to/project"
              />
            </Field>
            <Button
              disabled={!sources.length || !createProvider.trim() || sourceGapActive}
              onClick={createSession}
              type="button"
            >
              <PlusIcon data-icon="inline-start" />
              Create agent
            </Button>
          </form>
          {selectedSession ? (
            <>
              <div className="grid grid-cols-3 gap-2">
                <MetricCard label="provider" value={selectedSession.provider || "unknown"} />
                <MetricCard label="model" value={selectedSession.model || "default"} />
                <MetricCard label="status" value={selectedSession.status || "unknown"} />
                <MetricCard label="active turn" value={selectedSession.activeTurnId || "none"} />
                <MetricCard label="cwd" value={selectedSession.cwd || "unset"} />
                <MetricCard label="worktree" value={selectedSession.worktreePath || "unassigned"} />
              </div>
              <Card className="min-h-36 flex-1 bg-muted/20" size="sm">
                <CardHeader>
                  <CardTitle>Conversation stream</CardTitle>
                  <CardDescription>
                    Token updates are frame-batched by the realtime worker.
                  </CardDescription>
                </CardHeader>
                <CardContent className="space-y-2">
                  {latestTranscript ? (
                    transcriptItems.slice(-4).map((entry) => (
                      <div className="rounded-md border bg-background p-2" key={entry.id}>
                        <div className="mb-1 text-xs font-medium text-muted-foreground">
                          {entry.role}{entry.turnId ? ` / ${entry.turnId}` : ""}
                        </div>
                        <div className="whitespace-pre-wrap text-sm">{entry.text}</div>
                      </div>
                    ))
                  ) : selectedSession.activeTurnId ? (
                    `Streaming turn ${selectedSession.activeTurnId}.`
                  ) : (
                    "No active turn stream for this session."
                  )}
                </CardContent>
              </Card>
            </>
          ) : (
            <EmptyBlock title="No session" detail="Create an agent on a live source or select an existing session." />
          )}
        </CardContent>
      </Card>
      <div className="flex min-h-0 flex-col gap-3">
        <TimelineCard title="Timeline" items={timeline} />
        <ApprovalCard approval={selectedApproval} sourceGapActive={sourceGapActive} />
      </div>
    </div>
  );
}

function TeamPane({
  teams,
  rows,
  sessions,
  sources,
  selectedTeam,
  teamWorkspace,
  setSelectedTeamId,
  pendingCommands,
  sourceGapActive
}: {
  readonly teams: readonly TeamView[];
  readonly rows: readonly FleetRowView[];
  readonly sessions: readonly SessionView[];
  readonly sources: readonly SourceHealthView[];
  readonly selectedTeam?: TeamView;
  readonly teamWorkspace?: TeamWorkspaceState;
  readonly setSelectedTeamId: (id: string) => void;
  readonly pendingCommands: readonly PendingCommandState[];
  readonly sourceGapActive: boolean;
}) {
  const leadOptions = unique([
    ...sessions.map((session) => session.sessionId),
    ...rows.map((row) => row.sessionId).filter(Boolean)
  ]);
  const defaultLeadId =
    selectedTeam?.members.find((member) => member.memberId === selectedTeam.leadMemberId)
      ?.sessionId ||
    selectedTeam?.leadMemberId ||
    leadOptions[0] ||
    "";
  const defaultSourceId =
    selectedTeam?.sourceId ||
    sessions.find((session) => session.sessionId === defaultLeadId)?.sourceId ||
    rows.find((row) => row.sessionId === defaultLeadId)?.sourceId ||
    sources[0]?.sourceId ||
    "";
  const [mode, setMode] = useState<"direct" | "broadcast">("broadcast");
  const [recipient, setRecipient] = useState("");
  const [message, setMessage] = useState("");
  const [spawnOpen, setSpawnOpen] = useState(false);
  const [spawnTitle, setSpawnTitle] = useState("");
  const [spawnPrompt, setSpawnPrompt] = useState("");
  const [teamSourceId, setTeamSourceId] = useState(defaultSourceId);
  const [teamName, setTeamName] = useState("Live Team");
  const [leadAgentId, setLeadAgentId] = useState(defaultLeadId);
  const [joinAgentId, setJoinAgentId] = useState("");
  const joinPointerHandledRef = useRef(false);
  const members = selectedTeam?.members ?? [];
  const deliveries = teamWorkspace?.deliveries ?? [];
  const messages = teamWorkspace?.messages ?? [];
  const memberAgentIds = new Set(
    members.flatMap((member) => [member.memberId, member.sessionId].filter(Boolean))
  );
  const joinOptions = unique([
    ...sessions.map((session) => session.sessionId),
    ...rows.map((row) => row.sessionId).filter(Boolean)
  ]).filter((sessionId) => sessionId && !memberAgentIds.has(sessionId));
  const lead = members.find((member) => member.memberId === selectedTeam?.leadMemberId);
  const hasLeadForNewTeam = Boolean(leadAgentId || defaultLeadId);

  useEffect(() => {
    if (!teamSourceId && defaultSourceId) {
      setTeamSourceId(defaultSourceId);
    }
    if (!leadAgentId && defaultLeadId) {
      setLeadAgentId(defaultLeadId);
    }
    if (joinOptions[0] && !joinOptions.includes(joinAgentId)) {
      setJoinAgentId(joinOptions[0]);
    }
    if (!joinOptions.length && joinAgentId) {
      setJoinAgentId("");
    }
  }, [defaultLeadId, defaultSourceId, joinAgentId, joinOptions, leadAgentId, teamSourceId]);

  function createTeam() {
    const sourceId = teamSourceId || defaultSourceId;
    const leadId = leadAgentId || defaultLeadId;
    if (!sourceId || !leadId || !teamName.trim() || sourceGapActive) {
      return;
    }
    sendRealtimeCommand(
      makeCommand("source", sourceId, "createTeam", {
        name: teamName.trim(),
        leadAgentId: leadId,
        memberAgentIds: [],
        createdBy: leadId
      })
    );
  }

  function sendMessage(event: FormEvent, sendMode = mode) {
    event.preventDefault();
    if (!selectedTeam || !message.trim() || sourceGapActive) {
      return;
    }
    sendRealtimeCommand(
      sendMode === "direct"
        ? makeCommand("team", selectedTeam.teamId, "sendTeamMessage", {
            teamId: selectedTeam.teamId,
            recipientMemberId: recipient || members[0]?.memberId || "",
            text: message.trim()
          })
        : makeCommand("team", selectedTeam.teamId, "broadcastTeamMessage", {
            teamId: selectedTeam.teamId,
            text: message.trim()
          })
    );
    setMessage("");
  }

  function joinAgentToTeam() {
    if (!selectedTeam || !joinAgentId || sourceGapActive) {
      return;
    }
    sendRealtimeCommand(
      makeCommand("team", selectedTeam.teamId, "joinTeamMember", {
        teamId: selectedTeam.teamId,
        agentId: joinAgentId,
        title: joinAgentId,
        addedBy: selectedTeam.leadMemberId || ""
      })
    );
  }

  function joinAgentToTeamFromPointer() {
    joinPointerHandledRef.current = true;
    joinAgentToTeam();
  }

  function joinAgentToTeamFromClick() {
    if (joinPointerHandledRef.current) {
      joinPointerHandledRef.current = false;
      return;
    }
    joinAgentToTeam();
  }

  function spawnMember(event: FormEvent) {
    event.preventDefault();
    if (!selectedTeam || !spawnTitle.trim() || sourceGapActive) {
      return;
    }
    sendRealtimeCommand(
      makeCommand("team", selectedTeam.teamId, "spawnTeamMember", {
        teamId: selectedTeam.teamId,
        title: spawnTitle.trim(),
        prompt: spawnPrompt.trim(),
        modelPreset: ""
      })
    );
    setSpawnOpen(false);
    setSpawnTitle("");
    setSpawnPrompt("");
  }

  return (
    <>
      <div className="grid h-full min-h-0 grid-cols-[minmax(0,1fr)_19rem] gap-3">
        <Card className="min-h-0">
          <CardHeader className="border-b">
            <CardTitle>Team workspace</CardTitle>
            <CardDescription>{selectedTeam?.name || "No team selected"}</CardDescription>
            <CardAction className="flex gap-2">
              <SelectFilter
                value={selectedTeam?.teamId ?? ""}
                options={teams.map((team) => team.teamId)}
                onChange={setSelectedTeamId}
              />
              <Button
                disabled={!selectedTeam || sourceGapActive}
                type="button"
                onClick={() => setSpawnOpen(true)}
              >
                Spawn
              </Button>
            </CardAction>
          </CardHeader>
          <CardContent className="flex min-h-0 flex-1 flex-col gap-3">
            <form
              className="grid gap-3 rounded-md border bg-muted/20 p-3"
              onSubmit={(event) => {
                event.preventDefault();
                createTeam();
              }}
            >
              <div className="grid grid-cols-3 gap-2">
                <Field>
                  <FieldLabel>Source</FieldLabel>
                  <SelectFilter
                    value={teamSourceId || defaultSourceId}
                    options={sources.map((source) => source.sourceId)}
                    onChange={setTeamSourceId}
                  />
                </Field>
                <Field>
                  <FieldLabel htmlFor="create-team-name">Team name</FieldLabel>
                  <Input
                    id="create-team-name"
                    value={teamName}
                    onChange={(event) => setTeamName(event.target.value)}
                  />
                </Field>
                <Field>
                  <FieldLabel>Lead agent</FieldLabel>
                  <SelectFilter
                    value={leadAgentId || defaultLeadId}
                    options={leadOptions}
                    onChange={setLeadAgentId}
                  />
                </Field>
              </div>
              <Button
                disabled={
                  !sources.length ||
                  !leadOptions.length ||
                  !teamName.trim() ||
                  !hasLeadForNewTeam ||
                  sourceGapActive
                }
                onClick={createTeam}
                type="button"
              >
                <PlusIcon data-icon="inline-start" />
                Create team
              </Button>
            </form>
            <div className="grid grid-cols-3 gap-2">
              <MetricCard label="lead" value={lead?.title || lead?.memberId || "unset"} />
              <MetricCard label="members" value={String(members.length)} />
              <MetricCard label="team id" value={selectedTeam?.teamId || "none"} />
            </div>
            <div className="grid grid-cols-2 gap-2">
              {members.length === 0 ? (
                <EmptyBlock title="No members" detail="Waiting for team snapshot." />
              ) : (
                members.map((member) => (
                  <MemberCard key={member.memberId} leadId={selectedTeam?.leadMemberId ?? ""} member={member} />
                ))
              )}
            </div>
            {selectedTeam ? (
              <form
                className="grid gap-3 rounded-md border bg-muted/20 p-3"
                onSubmit={(event) => {
                  event.preventDefault();
                  joinAgentToTeam();
                }}
              >
                <Field>
                  <FieldLabel>Existing agent</FieldLabel>
                  <SelectFilter
                    value={joinAgentId || joinOptions[0] || ""}
                    options={joinOptions}
                    onChange={setJoinAgentId}
                  />
                </Field>
                <button
                  className={buttonVariants()}
                  disabled={!joinOptions.length || !joinAgentId || sourceGapActive}
                  type="button"
                  onClick={joinAgentToTeamFromClick}
                  onPointerUp={joinAgentToTeamFromPointer}
                >
                  <PlusIcon data-icon="inline-start" />
                  Join selected agent
                </button>
              </form>
            ) : null}
            {selectedTeam ? (
              <form onSubmit={(event) => sendMessage(event)}>
                <FieldGroup>
                  <Field>
                    <FieldLabel>Delivery mode</FieldLabel>
                  <div className="flex gap-2">
                    <Button
                      type="button"
                      variant={mode === "broadcast" ? "secondary" : "outline"}
                      onClick={() => setMode("broadcast")}
                    >
                      Broadcast
                    </Button>
                    <Button
                      type="button"
                      variant={mode === "direct" ? "secondary" : "outline"}
                      onClick={() => setMode("direct")}
                    >
                      Direct
                    </Button>
                  </div>
                </Field>
                  {mode === "direct" ? (
                    <Field>
                      <FieldLabel>Recipient</FieldLabel>
                      <SelectFilter
                        value={recipient || members[0]?.memberId || ""}
                        options={members.map((member) => member.memberId)}
                        onChange={setRecipient}
                      />
                    </Field>
                  ) : null}
                  <Field>
                    <FieldLabel htmlFor="team-message">Team message</FieldLabel>
                    <Textarea
                      id="team-message"
                      value={message}
                      onChange={(event) => setMessage(event.target.value)}
                      placeholder="Message selected team"
                      rows={3}
                    />
                  </Field>
                  <div className="flex gap-2">
                    <Button
                      disabled={!message.trim() || sourceGapActive}
                      type="button"
                      onClick={(event) => sendMessage(event, "direct")}
                    >
                      <SendIcon data-icon="inline-start" />
                      Send direct
                    </Button>
                    <Button
                      disabled={!message.trim() || sourceGapActive}
                      type="button"
                      variant="outline"
                      onClick={(event) => sendMessage(event, "broadcast")}
                    >
                      <SendIcon data-icon="inline-start" />
                      Broadcast
                    </Button>
                  </div>
                </FieldGroup>
              </form>
            ) : (
              <EmptyBlock title="No team selected" detail="Select a team to enable team message actions." />
            )}
          </CardContent>
        </Card>
        <div className="flex min-h-0 flex-col gap-3">
          <TimelineCard
            title="Delivery state"
            items={[
              ...deliveries.map((delivery) => ({
                id: delivery.id,
                title: delivery.recipientAgentId || delivery.id,
                meta: `${delivery.status}${delivery.injectedTurnId ? ` / ${delivery.injectedTurnId}` : ""}${delivery.lastError ? ` / ${delivery.lastError}` : ""}`
              })),
              ...pendingCommands.map((command) => ({
                id: command.commandId,
                title: command.commandId,
                meta:
                  command.status === "rejected"
                    ? `${command.errorCode ?? "rejected"}: ${command.error ?? "Command rejected"}`
                    : command.status === "duplicate"
                      ? `${command.errorCode ?? "duplicate"}: ${command.error ?? "Already submitted"}`
                      : command.status
              }))
            ]}
            renderAction={(id) => (
              <div className="flex gap-1">
                <Button
                  disabled={!selectedTeam || sourceGapActive}
                  size="xs"
                  type="button"
                  variant="outline"
                  onClick={() =>
                    selectedTeam &&
                    sendRealtimeCommand(
                      makeCommand("team", selectedTeam.teamId, "retryDelivery", {
                        deliveryId: id
                      })
                    )
                  }
                >
                  Retry
                </Button>
                <Button
                  disabled={!selectedTeam || sourceGapActive}
                  size="xs"
                  type="button"
                  variant="outline"
                  onClick={() =>
                    selectedTeam &&
                    sendRealtimeCommand(
                      makeCommand("team", selectedTeam.teamId, "cancelDelivery", {
                        messageId: id
                      })
                    )
                  }
                >
                  Cancel
                </Button>
              </div>
            )}
          />
          <TimelineCard
            title="Team events"
            items={[
              ...messages.map((message) => ({
                id: message.id,
                title: message.scope || "team message",
                meta: message.text || message.senderAgentId
              })),
              ...members.map((member) => ({
                id: member.memberId,
                title: member.title || member.memberId,
                meta: `${member.status || "unknown"} / ${member.provider || "provider"}`
              }))
            ]}
          />
        </div>
      </div>
      <Dialog open={spawnOpen} onOpenChange={setSpawnOpen}>
        <DialogContent>
          <DialogHeader>
            <DialogTitle>Spawn teammate</DialogTitle>
            <DialogDescription>
              Creates a team member through the runtime spawn command.
            </DialogDescription>
          </DialogHeader>
          <form className="flex flex-col gap-4" onSubmit={spawnMember}>
            <FieldGroup>
              <Field>
                <FieldLabel htmlFor="spawn-title">Title</FieldLabel>
                <Input
                  id="spawn-title"
                  value={spawnTitle}
                  onChange={(event) => setSpawnTitle(event.target.value)}
                />
              </Field>
              <Field>
                <FieldLabel htmlFor="spawn-prompt">Onboarding prompt</FieldLabel>
                <Textarea
                  id="spawn-prompt"
                  value={spawnPrompt}
                  onChange={(event) => setSpawnPrompt(event.target.value)}
                  rows={5}
                />
              </Field>
            </FieldGroup>
            <DialogFooter>
              <Button disabled={!spawnTitle.trim() || sourceGapActive} type="submit">
                Spawn teammate
              </Button>
            </DialogFooter>
          </form>
        </DialogContent>
      </Dialog>
    </>
  );
}

function InboxPane({
  approvals,
  selectedApprovalId,
  setSelectedApprovalId,
  sourceGapActive
}: {
  readonly approvals: readonly ApprovalView[];
  readonly selectedApprovalId: string;
  readonly setSelectedApprovalId: (id: string) => void;
  readonly sourceGapActive: boolean;
}) {
  const [rejectReasonById, setRejectReasonById] = useState<Record<string, string>>({});
  const virtual = useVirtualRows(approvals, 116, 8);

  return (
    <Card className="h-full min-h-0">
      <CardHeader className="border-b">
        <CardTitle>Approval inbox</CardTitle>
        <CardDescription>
          Global pending approvals with inline rejection feedback.
        </CardDescription>
        <CardAction>
          <Badge variant={sourceGapActive ? "destructive" : "outline"}>
            {sourceGapActive ? "stale guarded" : `${approvals.length} requests`}
          </Badge>
        </CardAction>
      </CardHeader>
      <CardContent className="min-h-0 flex-1 p-0">
        <div className="h-full overflow-auto p-3" ref={virtual.containerRef}>
          <div style={{ height: virtual.topPadding }} />
          <div className="flex flex-col gap-2">
            {virtual.visibleItems.map((approval) => {
              const reason = rejectReasonById[approval.approvalId] ?? "";
              return (
                <Card
                  className={cn(
                    "cursor-pointer",
                    approval.approvalId === selectedApprovalId && "ring-primary"
                  )}
                  key={approval.approvalId}
                  size="sm"
                  onClick={() => setSelectedApprovalId(approval.approvalId)}
                >
                  <CardHeader>
                    <CardTitle>{approval.summary || approval.approvalId}</CardTitle>
                    <CardDescription>{approval.sessionId} / {approval.turnId}</CardDescription>
                    <CardAction><StatusBadge status={approval.risk || approval.status} /></CardAction>
                  </CardHeader>
                  <CardContent className="flex items-center gap-2">
                    <Input
                      value={reason}
                      onChange={(event) =>
                        setRejectReasonById({
                          ...rejectReasonById,
                          [approval.approvalId]: event.target.value
                        })
                      }
                      placeholder="Rejection feedback"
                    />
                    <Button
                      disabled={sourceGapActive || approval.status !== "pending"}
                      type="button"
                      onClick={() =>
                        sendRealtimeCommand(
                          makeCommand("session", approval.sessionId, "resolveApproval", {
                            approvalId: approval.approvalId,
                            approved: true,
                            reason: ""
                          })
                        )
                      }
                    >
                      Approve
                    </Button>
                    <Button
                      disabled={sourceGapActive || approval.status !== "pending"}
                      type="button"
                      variant="destructive"
                      onClick={() =>
                        sendRealtimeCommand(
                          makeCommand("session", approval.sessionId, "resolveApproval", {
                            approvalId: approval.approvalId,
                            approved: false,
                            reason
                          })
                        )
                      }
                    >
                      Reject
                    </Button>
                  </CardContent>
                </Card>
              );
            })}
          </div>
          <div style={{ height: virtual.bottomPadding }} />
        </div>
      </CardContent>
    </Card>
  );
}

function LedgerPane({
  events,
  sources
}: {
  readonly events: readonly LedgerEvent[];
  readonly sources: readonly SourceHealthView[];
}) {
  const [sourceFilter, setSourceFilter] = useState("all");
  const [scopeFilter, setScopeFilter] = useState("all");
  const filtered = events.filter(
    (event) =>
      (sourceFilter === "all" || event.sourceId === sourceFilter) &&
      (scopeFilter === "all" || event.scope === scopeFilter)
  );
  const virtual = useVirtualRows(filtered, 48, 10);

  return (
    <Card className="h-full min-h-0">
      <CardHeader className="border-b">
        <CardTitle>Ledger</CardTitle>
        <CardDescription>Virtualized runtime and gateway event feed.</CardDescription>
        <CardAction className="flex gap-2">
          <SelectFilter
            value={sourceFilter}
            options={["all", ...sources.map((source) => source.sourceId)]}
            onChange={setSourceFilter}
          />
          <SelectFilter
            value={scopeFilter}
            options={["all", ...unique(events.map((event) => event.scope))]}
            onChange={setScopeFilter}
          />
        </CardAction>
      </CardHeader>
      <CardContent className="min-h-0 flex-1 p-0">
        <div className="flex items-center gap-3 border-b px-3 py-2 text-xs text-muted-foreground">
          <span>cursor/replay marker</span>
          <code>{filtered[0]?.cursor || "none"}</code>
          <span>gateway audit events appear when emitted by the source</span>
        </div>
        <div className="h-[calc(100%-2.25rem)] overflow-auto" ref={virtual.containerRef}>
          <Table>
            <TableHeader className="sticky top-0 z-10 bg-card">
              <TableRow>
                <TableHead>Criticality</TableHead>
                <TableHead>Scope</TableHead>
                <TableHead>Kind</TableHead>
                <TableHead>Source</TableHead>
                <TableHead>Cursor</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {virtual.topPadding > 0 ? <SpacerRow height={virtual.topPadding} colSpan={5} /> : null}
              {virtual.visibleItems.map((event) => (
                <TableRow key={event.id}>
                  <TableCell><StatusBadge status={event.criticality} /></TableCell>
                  <TableCell>{event.scope}</TableCell>
                  <TableCell>{event.kind}</TableCell>
                  <TableCell>{event.sourceId}</TableCell>
                  <TableCell><code>{event.cursor}</code></TableCell>
                </TableRow>
              ))}
              {virtual.bottomPadding > 0 ? <SpacerRow height={virtual.bottomPadding} colSpan={5} /> : null}
            </TableBody>
          </Table>
        </div>
      </CardContent>
    </Card>
  );
}

function FleetPane({
  sources,
  rows,
  processes,
  connection
}: {
  readonly sources: readonly SourceHealthView[];
  readonly rows: readonly FleetRowView[];
  readonly processes: readonly ProcessView[];
  readonly connection: ConnectionState;
}) {
  const source = sources[0];
  const activeProcesses = processes.filter((process) => process.status === "running").length;
  const controlsEnabled = goosewebConfig.flags.fleetProvisioningControls;

  return (
    <div className="grid h-full min-h-0 grid-cols-[minmax(0,1fr)_19rem] gap-3">
      <Card>
        <CardHeader>
          <CardTitle>Fleet</CardTitle>
          <CardDescription>Runtime source health and capacity.</CardDescription>
          <CardAction className="flex gap-2">
            <Button disabled={!controlsEnabled} variant="outline">
              <PowerIcon />
              Provision
            </Button>
            <Button disabled={!controlsEnabled || !source} variant="outline">
              <ListChecksIcon />
              Drain
            </Button>
          </CardAction>
        </CardHeader>
        <CardContent className="grid grid-cols-4 gap-2">
          <MetricCard label="sources" value={String(sources.length || 0)} />
          <MetricCard label="health" value={source?.lifecycle || source?.health || connection} />
          <MetricCard label="stale age" value={source ? ageFrom(toNumber(source.observedAtUnixMs)) : "unknown"} />
          <MetricCard label="replay lag" value={source?.cursor ? source.cursor.sourceSeq.toString() : "0"} />
          <MetricCard label="active sessions" value={String(source?.activeSessionCount || rows.length)} />
          <MetricCard
            label="process capacity"
            value={`${source?.activeProcessCount || activeProcesses}/${source?.processCapacity || Math.max(activeProcesses, 1)}`}
          />
          {(source?.providerKinds.length ? source.providerKinds : ["codex", "claude", "acp"]).map((provider) => (
            <MetricCard
              key={provider}
              label={`${provider} auth`}
              value={source?.providerKinds.includes(provider) || rows.some((row) => row.provider === provider) ? "available" : "unknown"}
            />
          ))}
        </CardContent>
      </Card>
      <Card>
        <CardHeader>
          <CardTitle>Source operations</CardTitle>
          <CardDescription>Explicit admin controls. No live session migration.</CardDescription>
        </CardHeader>
        <CardContent className="flex min-h-0 flex-col gap-3">
          <div className="grid gap-2">
            {sources.length ? sources.map((item) => (
              <div className="rounded-md border border-border/70 p-2" key={item.sourceId}>
                <div className="flex items-center justify-between gap-2">
                  <div className="min-w-0">
                    <div className="truncate text-sm font-medium">{item.displayName || item.sourceId}</div>
                    <div className="truncate text-xs text-muted-foreground">
                      {item.sourceKind} · {item.provisionerKind || "static"} · {item.region || "region unknown"}
                    </div>
                  </div>
                  <Badge variant={item.lifecycle === "live" || item.health === "live" ? "default" : "secondary"}>
                    {item.lifecycle || item.health}
                  </Badge>
                </div>
                <div className="mt-2 grid grid-cols-2 gap-1 text-xs text-muted-foreground">
                  <span>{item.supportsWorktrees ? "worktrees" : "no worktrees"}</span>
                  <span>{item.supportsTeams ? "teams" : "no teams"}</span>
                  <span>{item.models.length ? item.models.join(", ") : "models unknown"}</span>
                  <span>{item.costHint || "cost unknown"}</span>
                </div>
              </div>
            )) : (
              <div className="rounded-md border border-dashed p-3 text-sm text-muted-foreground">
                No runtime sources are materialized.
              </div>
            )}
          </div>
          <Separator />
          <div className="grid gap-2">
            <Button disabled={!controlsEnabled} variant="outline">
              <PowerIcon />
              Provision source
            </Button>
            <Button disabled={!controlsEnabled || !source} variant="outline">
              <ListChecksIcon />
              Drain source
            </Button>
            <Button disabled={!controlsEnabled || !source} variant="outline">
              <SquareIcon />
              Terminate source
            </Button>
            <Button disabled={!controlsEnabled || !source} variant="outline">
              <TerminalIcon />
              Inspect logs/health
            </Button>
          </div>
        </CardContent>
      </Card>
    </div>
  );
}

function PlaybooksPane({
  selectedSession,
  selectedTeam,
  sourceGapActive
}: {
  readonly selectedSession?: SessionView;
  readonly selectedTeam?: TeamView;
  readonly sourceGapActive: boolean;
}) {
  const templates = [
    ["Status", "Give a concise status update with blockers and next command."],
    ["Handoff", "Summarize completed work, changed files, checks run, and follow-ups."],
    ["Approval context", "Explain why this approval is needed and what risk it carries."]
  ] as const;

  return (
    <Card className="h-full">
      <CardHeader>
        <CardTitle>Playbooks</CardTitle>
        <CardDescription>Minimal command and message templates.</CardDescription>
      </CardHeader>
      <CardContent className="grid grid-cols-3 gap-3">
        {templates.map(([title, body]) => (
          <Card key={title} size="sm">
            <CardHeader>
              <CardTitle>{title}</CardTitle>
              <CardDescription>{body}</CardDescription>
            </CardHeader>
            <CardContent className="flex gap-2">
              <Button
                disabled={!selectedSession || sourceGapActive}
                size="sm"
                type="button"
                onClick={() =>
                  selectedSession &&
                  sendRealtimeCommand(
                    makeCommand("session", selectedSession.sessionId, "sendTurn", {
                      sessionId: selectedSession.sessionId,
                      text: body
                    })
                  )
                }
              >
                Agent
              </Button>
              <Button
                disabled={!selectedTeam || sourceGapActive}
                size="sm"
                type="button"
                variant="outline"
                onClick={() =>
                  selectedTeam &&
                  sendRealtimeCommand(
                    makeCommand("team", selectedTeam.teamId, "broadcastTeamMessage", {
                      teamId: selectedTeam.teamId,
                      text: body
                    })
                  )
                }
              >
                Team
              </Button>
            </CardContent>
          </Card>
        ))}
      </CardContent>
    </Card>
  );
}

function SettingsPane({
  state,
  subscriptionCount
}: {
  readonly state: GoosewebSnapshot;
  readonly subscriptionCount: number;
}) {
  const [ticket, setTicket] = useState(goosewebConfig.pastedDevTicket);
  const [ticketStatus, setTicketStatus] = useState("");
  const [ticketLoading, setTicketLoading] = useState(false);
  const [connectedTicket, setConnectedTicket] = useState("");
  const debugExport = JSON.stringify(
    {
      connection: state.connection,
      connectionId: state.connectionId,
      gatewaySeq: state.cursor.gatewaySeq.toString(),
      subscriptions: subscriptionCount,
      staleSources: state.staleSources,
      flags: goosewebConfig.flags
    },
    null,
    2
  );

  async function mintAndConnectDevelopmentTicket() {
    setTicketLoading(true);
    setTicketStatus("");
    try {
      const nextTicket = await mintDevelopmentTicket();
      setTicket(nextTicket);
      setConnectedTicket(nextTicket);
      connectRealtime(nextTicket);
      setTicketStatus("Development ticket connected.");
    } catch (error) {
      setTicketStatus(error instanceof Error ? error.message : "Unable to mint development ticket.");
    } finally {
      setTicketLoading(false);
    }
  }

  function connectDevelopmentTicket() {
    const nextTicket = ticket.trim();
    if (!nextTicket) {
      return;
    }
    if (nextTicket === connectedTicket) {
      setTicketStatus(
        state.connection === "offline" || state.connection === "idle"
          ? "Development ticket was already used. Mint a new ticket to reconnect."
          : "Development ticket already connected."
      );
      return;
    }

    setConnectedTicket(nextTicket);
    connectRealtime(nextTicket);
    setTicketStatus("Development ticket connected.");
  }

  return (
    <Tabs className="h-full" defaultValue="connection">
      <TabsList>
        <TabsTrigger value="connection">Connection</TabsTrigger>
        <TabsTrigger value="flags">Flags</TabsTrigger>
        <TabsTrigger value="debug">Debug export</TabsTrigger>
      </TabsList>
      <TabsContent className="min-h-0" value="connection">
        <Card>
          <CardHeader>
            <CardTitle>Settings</CardTitle>
            <CardDescription>Gateway, protocol, user, and workspace state.</CardDescription>
          </CardHeader>
          <CardContent>
            <FieldGroup>
              <Field>
                <FieldLabel>Gateway URL</FieldLabel>
                <Input readOnly value={goosewebConfig.goosetowerUrl} />
              </Field>
              <Field>
                <FieldLabel>Protocol version</FieldLabel>
                <Input readOnly value="1" />
              </Field>
              <Field>
                <FieldLabel>Current user/workspace</FieldLabel>
                <Input readOnly value={state.connectionId || "development user"} />
              </Field>
              <Field>
                <FieldLabel>Development ticket</FieldLabel>
                <Textarea
                  value={ticket}
                  onChange={(event) => {
                    setTicket(event.target.value);
                    if (event.target.value.trim() !== connectedTicket) {
                      setTicketStatus("");
                    }
                  }}
                  rows={5}
                />
              </Field>
              <div className="flex gap-2">
                {goosewebConfig.flags.devTicketRoute ? (
                  <button
                    className={buttonVariants()}
                    disabled={ticketLoading}
                    onClick={mintAndConnectDevelopmentTicket}
                    type="button"
                  >
                    {ticketLoading ? "Minting" : "Mint dev ticket"}
                  </button>
                ) : null}
                <Button
                  disabled={
                    !ticket.trim() ||
                    (ticket.trim() === connectedTicket &&
                      state.connection !== "offline" &&
                      state.connection !== "idle")
                  }
                  onClick={connectDevelopmentTicket}
                  type="button"
                >
                  Connect
                </Button>
                <Button onClick={disconnectRealtime} type="button" variant="outline">
                  Disconnect
                </Button>
              </div>
              {ticketStatus ? (
                <p className="text-muted-foreground text-sm">{ticketStatus}</p>
              ) : null}
            </FieldGroup>
          </CardContent>
        </Card>
      </TabsContent>
      <TabsContent value="flags">
        <Card>
          <CardHeader>
            <CardTitle>Feature flags</CardTitle>
          </CardHeader>
          <CardContent className="grid grid-cols-3 gap-2">
            {Object.entries(goosewebConfig.flags).map(([key, value]) => (
              <MetricCard key={key} label={key} value={String(value)} />
            ))}
          </CardContent>
        </Card>
      </TabsContent>
      <TabsContent className="min-h-0" value="debug">
        <Card className="h-full">
          <CardHeader>
            <CardTitle>Debug export</CardTitle>
            <CardDescription>Browser state with durable secrets excluded.</CardDescription>
          </CardHeader>
          <CardContent>
            <pre className="max-h-[60vh] overflow-auto rounded-lg bg-muted p-3 text-xs">
              {debugExport}
            </pre>
          </CardContent>
        </Card>
      </TabsContent>
    </Tabs>
  );
}

function ApprovalCard({
  approval,
  sourceGapActive
}: {
  readonly approval?: ApprovalView;
  readonly sourceGapActive: boolean;
}) {
  return (
    <Card>
      <CardHeader>
        <CardTitle>Approval context</CardTitle>
        <CardDescription>{approval?.status || "none"}</CardDescription>
      </CardHeader>
      <CardContent className="flex flex-col gap-2">
        {approval ? (
          <>
            <div className="text-sm font-medium">{approval.summary || approval.approvalId}</div>
            <MetricCard label="risk" value={approval.risk || "unknown"} />
            <MetricCard label="turn" value={approval.turnId || "none"} />
            <div className="flex gap-2">
              <Button
                disabled={sourceGapActive || approval.status !== "pending"}
                size="sm"
                type="button"
                onClick={() =>
                  sendRealtimeCommand(
                    makeCommand("session", approval.sessionId, "resolveApproval", {
                      approvalId: approval.approvalId,
                      approved: true,
                      reason: ""
                    })
                  )
                }
              >
                Approve
              </Button>
              <Button
                disabled={sourceGapActive || approval.status !== "pending"}
                size="sm"
                type="button"
                variant="destructive"
                onClick={() =>
                  sendRealtimeCommand(
                    makeCommand("session", approval.sessionId, "resolveApproval", {
                      approvalId: approval.approvalId,
                      approved: false,
                      reason: "Rejected from Gooseweb context panel"
                    })
                  )
                }
              >
                Reject
              </Button>
            </div>
          </>
        ) : (
          <EmptyBlock title="No approval" detail="Controls stay outside streaming panes." />
        )}
      </CardContent>
    </Card>
  );
}

function MemberCard({
  member,
  leadId
}: {
  readonly member: TeamMemberView;
  readonly leadId: string;
}) {
  return (
    <Card size="sm">
      <CardHeader>
        <CardTitle>{member.title || member.memberId}</CardTitle>
        <CardDescription>{member.sessionId}</CardDescription>
        <CardAction>
          <StatusBadge status={member.status || "unknown"} />
        </CardAction>
      </CardHeader>
      <CardContent className="grid grid-cols-2 gap-2">
        <MetricCard label="provider" value={member.provider || "unknown"} />
        <MetricCard label="model" value={member.model || "default"} />
        {member.memberId === leadId ? <Badge>lead</Badge> : null}
      </CardContent>
    </Card>
  );
}

function TimelineCard({
  title,
  items,
  renderAction
}: {
  readonly title: string;
  readonly items: readonly { readonly id: string; readonly title: string; readonly meta: string }[];
  readonly renderAction?: (id: string) => ReactNode;
}) {
  const virtual = useVirtualRows(items, 56, 6);
  return (
    <Card className="min-h-0 flex-1">
      <CardHeader className="border-b">
        <CardTitle>{title}</CardTitle>
        <CardDescription>{items.length} items</CardDescription>
      </CardHeader>
      <CardContent className="min-h-0 flex-1 p-0">
        <div className="h-full overflow-auto p-2" ref={virtual.containerRef}>
          <div style={{ height: virtual.topPadding }} />
          <div className="flex flex-col gap-1">
            {virtual.visibleItems.length === 0 ? (
              <EmptyBlock title="No entries" detail="Waiting for timeline events." />
            ) : (
              virtual.visibleItems.map((item) => (
                <div className="flex items-center gap-2 rounded-lg px-2 py-1.5 hover:bg-muted" key={item.id}>
                  <ActivityIcon />
                  <span className="grid min-w-0 flex-1">
                    <span className="truncate text-sm font-medium">{item.title}</span>
                    <span className="truncate text-xs text-muted-foreground">{item.meta}</span>
                  </span>
                  {renderAction ? renderAction(item.id) : null}
                </div>
              ))
            )}
          </div>
          <div style={{ height: virtual.bottomPadding }} />
        </div>
      </CardContent>
    </Card>
  );
}

function ContextCard({
  title,
  items
}: {
  readonly title: string;
  readonly items: readonly (readonly [string, string | undefined])[];
}) {
  return (
    <Card size="sm">
      <CardHeader>
        <CardTitle>{title}</CardTitle>
      </CardHeader>
      <CardContent className="flex flex-col gap-2">
        {items.length === 0 ? (
          <EmptyBlock title="No data" detail="Waiting for source state." />
        ) : (
          items.map(([label, value]) => (
            <MetricChip key={label} label={label} value={value || "none"} />
          ))
        )}
      </CardContent>
    </Card>
  );
}

function MetricCard({ label, value }: { readonly label: string; readonly value: string }) {
  return (
    <div className="min-w-0 rounded-lg border bg-muted/20 p-2">
      <div className="truncate text-xs text-muted-foreground">{label}</div>
      <div className="truncate text-sm font-medium">{value}</div>
    </div>
  );
}

function MetricChip({ label, value }: { readonly label: string; readonly value: string }) {
  return (
    <Badge className="max-w-48 gap-1" variant="outline">
      <span className="text-muted-foreground">{label}</span>
      <span className="truncate">{value}</span>
    </Badge>
  );
}

function ConnectionBadge({ connection }: { readonly connection: ConnectionState }) {
  return (
    <Badge variant={connection === "connected" ? "secondary" : connection === "offline" ? "destructive" : "outline"}>
      <RadioIcon data-icon="inline-start" />
      {connection}
    </Badge>
  );
}

function StatusBadge({ status }: { readonly status: string }) {
  const normalized = status.toLowerCase();
  const variant =
    normalized.includes("fail") ||
    normalized.includes("offline") ||
    normalized.includes("stale") ||
    normalized.includes("critical")
      ? "destructive"
      : normalized.includes("pending") ||
          normalized.includes("running") ||
          normalized.includes("replay")
        ? "secondary"
        : "outline";
  return <Badge variant={variant}>{status || "unknown"}</Badge>;
}

function SelectFilter({
  value,
  options,
  onChange
}: {
  readonly value: string;
  readonly options: readonly string[];
  readonly onChange: (value: string) => void;
}) {
  return (
    <Select value={value} onValueChange={(next) => next !== null && onChange(next)}>
      <SelectTrigger size="sm">
        <SelectValue />
      </SelectTrigger>
      <SelectContent>
        <SelectGroup>
          {options.map((option) => (
            <SelectItem key={option} value={option}>
              {option || "none"}
            </SelectItem>
          ))}
        </SelectGroup>
      </SelectContent>
    </Select>
  );
}

function EmptyBlock({ title, detail }: { readonly title: string; readonly detail: string }) {
  return (
    <Empty className="min-h-32 border">
      <EmptyHeader>
        <EmptyMedia variant="icon">
          <ClipboardListIcon />
        </EmptyMedia>
        <EmptyTitle>{title}</EmptyTitle>
        <EmptyDescription>{detail}</EmptyDescription>
      </EmptyHeader>
    </Empty>
  );
}

function SpacerRow({ height, colSpan }: { readonly height: number; readonly colSpan: number }) {
  return (
    <TableRow aria-hidden="true">
      <TableCell colSpan={colSpan} style={{ height, padding: 0 }} />
    </TableRow>
  );
}

function useVirtualRows<T>(items: readonly T[], rowHeight: number, overscan: number) {
  const containerRef = useRef<HTMLDivElement | null>(null);
  const [scrollTop, setScrollTop] = useState(0);
  const [height, setHeight] = useState(420);

  useEffect(() => {
    const element = containerRef.current;
    if (!element) {
      return;
    }

    const updateHeight = () => setHeight(element.clientHeight || 420);
    updateHeight();
    const resizeObserver = new ResizeObserver(updateHeight);
    resizeObserver.observe(element);
    const onScroll = () => setScrollTop(element.scrollTop);
    element.addEventListener("scroll", onScroll, { passive: true });

    return () => {
      resizeObserver.disconnect();
      element.removeEventListener("scroll", onScroll);
    };
  }, []);

  const startIndex = Math.max(0, Math.floor(scrollTop / rowHeight) - overscan);
  const visibleCount = Math.ceil(height / rowHeight) + overscan * 2;
  const visibleItems = items.slice(startIndex, startIndex + visibleCount);
  const topPadding = startIndex * rowHeight;
  const bottomPadding = Math.max(
    0,
    (items.length - startIndex - visibleItems.length) * rowHeight
  );

  return {
    containerRef,
    visibleItems,
    topPadding,
    bottomPadding
  };
}

function makeCommand(
  scope: CommandScope,
  scopeId: string,
  payloadCase: CommandPayloadCase,
  payloadValue: Record<string, unknown>
): CommandIntent {
  return {
    commandId: crypto.randomUUID(),
    idempotencyKey: crypto.randomUUID(),
    createdAtClientUnixMs: BigInt(Date.now()),
    ...(payloadCase === "createSession"
      ? {
          fallbackCreateSession: {
            provider: stringCommandValue(payloadValue, "provider"),
            model: stringCommandValue(payloadValue, "model"),
            cwd: stringCommandValue(payloadValue, "cwd"),
            title: stringCommandValue(payloadValue, "title"),
            permissionMode: stringCommandValue(payloadValue, "permissionMode"),
            metadata: {}
          }
        }
      : {}),
    target: {
      scope,
      scopeId,
      entityId: scope === "source" ? `source:${scopeId}` : scopeId
    },
    payload: {
      case: payloadCase,
      value: payloadValue
    }
  };
}

function stringCommandValue(value: Record<string, unknown>, key: string): string {
  const next = value[key];
  return typeof next === "string" ? next : "";
}

function getEntityItems(input: {
  readonly activeView: WorkspaceView;
  readonly rows: readonly FleetRowView[];
  readonly sessions: readonly SessionView[];
  readonly teams: readonly TeamView[];
  readonly approvals: readonly ApprovalView[];
  readonly processes: readonly ProcessView[];
  readonly selectedRowId: string;
  readonly selectedSessionId: string;
  readonly selectedTeamId: string;
  readonly selectedApprovalId: string;
  readonly selectedProcessId: string;
  readonly onSelectRow: (id: string) => void;
  readonly onSelectSession: (id: string) => void;
  readonly onSelectTeam: (id: string) => void;
  readonly onSelectApproval: (id: string) => void;
  readonly onSelectProcess: (id: string) => void;
}) {
  if (input.activeView === "teams") {
    return input.teams.map((team) => ({
      id: team.teamId,
      title: team.name || team.teamId,
      meta: `${team.members.length} members`,
      status: team.leadMemberId ? "lead set" : "no lead",
      selected: team.teamId === input.selectedTeamId,
      onClick: () => input.onSelectTeam(team.teamId)
    }));
  }
  if (input.activeView === "inbox") {
    return input.approvals.map((approval) => ({
      id: approval.approvalId,
      title: approval.summary || approval.approvalId,
      meta: approval.sessionId,
      status: approval.status,
      selected: approval.approvalId === input.selectedApprovalId,
      onClick: () => input.onSelectApproval(approval.approvalId)
    }));
  }
  if (input.activeView === "agents") {
    return input.sessions.map((session) => ({
      id: session.sessionId,
      title: session.sessionId,
      meta: `${session.provider}/${session.model || "default"}`,
      status: session.status,
      selected: session.sessionId === input.selectedSessionId,
      onClick: () => input.onSelectSession(session.sessionId)
    }));
  }
  if (input.activeView === "fleet") {
    return input.processes.map((process) => ({
      id: process.processId,
      title: process.command || process.processId,
      meta: process.processId,
      status: process.status,
      selected: process.processId === input.selectedProcessId,
      onClick: () => input.onSelectProcess(process.processId)
    }));
  }
  return input.rows.map((row) => ({
    id: row.rowId,
    title: row.title || row.sessionId || row.rowId,
    meta: `${row.provider || "provider"}/${row.model || "default"}`,
    status: row.status,
    selected: row.rowId === input.selectedRowId,
    onClick: () => input.onSelectRow(row.rowId)
  }));
}

function sidebarTitle(view: WorkspaceView): string {
  switch (view) {
    case "inbox":
      return "Pending approvals";
    case "teams":
      return "Teams";
    case "agents":
      return "Sessions";
    case "fleet":
      return "Processes";
    case "ledger":
      return "Ledger scope";
    case "playbooks":
      return "Targets";
    case "settings":
      return "Runtime";
    case "board":
    default:
      return "Board rows";
  }
}

function buildLedgerEvents(input: {
  readonly fleetRows: readonly FleetRowView[];
  readonly teams: readonly TeamView[];
  readonly approvals: readonly ApprovalView[];
  readonly processes: readonly ProcessView[];
  readonly sources: readonly SourceHealthView[];
}): readonly LedgerEvent[] {
  const events: LedgerEvent[] = [];
  for (const source of input.sources) {
    events.push({
      id: `source:${source.sourceId}:${source.cursor?.sourceSeq ?? 0n}`,
      sourceId: source.sourceId,
      scope: "source",
      kind: `source.${source.health || "unknown"}`,
      cursor: `${source.cursor?.sourceEpoch || "epoch"}:${source.cursor?.sourceSeq ?? 0n}`,
      criticality: source.health || "state",
      happenedAt: toNumber(source.observedAtUnixMs)
    });
  }
  for (const row of input.fleetRows) {
    events.push({
      id: `row:${row.rowId}`,
      sourceId: row.sourceId,
      scope: "session",
      kind: `board.${row.status || "unknown"}`,
      cursor: String(row.latestActivityUnixMs || 0),
      criticality: row.pendingApprovalCount > 0 ? "critical" : "state",
      happenedAt: toNumber(row.latestActivityUnixMs)
    });
  }
  for (const approval of input.approvals) {
    events.push({
      id: `approval:${approval.approvalId}`,
      sourceId: approval.sourceId,
      scope: "approval",
      kind: `approval.${approval.status || "unknown"}`,
      cursor: approval.turnId,
      criticality: approval.status === "pending" ? "critical" : "state",
      happenedAt: Date.now()
    });
  }
  for (const team of input.teams) {
    events.push({
      id: `team:${team.teamId}`,
      sourceId: team.sourceId,
      scope: "team",
      kind: "team.snapshot",
      cursor: team.teamId,
      criticality: "state",
      happenedAt: Date.now()
    });
  }
  for (const process of input.processes) {
    events.push({
      id: `process:${process.processId}`,
      sourceId: process.sourceId,
      scope: "process",
      kind: `process.${process.status || "unknown"}`,
      cursor: process.processId,
      criticality: process.status === "failed" ? "critical" : "bulk",
      happenedAt: Date.now()
    });
  }
  return events.sort((a, b) => b.happenedAt - a.happenedAt);
}

function unique(values: readonly string[]): readonly string[] {
  return [...new Set(values)];
}

function formatTime(unixMs: number): string {
  if (!unixMs) {
    return "none";
  }
  return new Intl.DateTimeFormat(undefined, {
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit"
  }).format(new Date(unixMs));
}

function ageFrom(unixMs: number): string {
  if (!unixMs) {
    return "unknown";
  }
  const seconds = Math.max(0, Math.round((Date.now() - unixMs) / 1000));
  return seconds < 60 ? `${seconds}s` : `${Math.round(seconds / 60)}m`;
}

function toNumber(value: number | bigint): number {
  return typeof value === "bigint" ? Number(value) : value;
}
