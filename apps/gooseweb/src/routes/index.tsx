import { createFileRoute } from "@tanstack/react-router";
import { create } from "@bufbuild/protobuf";
import {
  ActivityIcon,
  ArrowUpIcon,
  BotIcon,
  BoxesIcon,
  ChevronDownIcon,
  ClipboardListIcon,
  FolderIcon,
  InfoIcon,
  Maximize2Icon,
  Minimize2Icon,
  MessageSquareIcon,
  SearchIcon,
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
  WorkflowIcon,
  XIcon
} from "lucide-react";
import {
  type FormEvent,
  type KeyboardEvent,
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
import { SessionViewSchema } from "../../src/gen/goosetower/v1/view_pb";
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
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuRadioGroup,
  DropdownMenuRadioItem,
  DropdownMenuTrigger
} from "~/components/ui/dropdown-menu";
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
  Sheet,
  SheetContent,
  SheetTitle
} from "~/components/ui/sheet";
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

let devAutoConnectStarted = false;

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

type TeamFeedItem = {
  readonly id: string;
  readonly kind: "message" | "delivery" | "command" | "member";
  readonly title: string;
  readonly body: string;
  readonly meta: readonly (readonly [string, string])[];
  readonly timestampUnixMs?: number;
  readonly status?: string;
  readonly deliveryId?: string;
  readonly cancelMessageId?: string;
};

type AgentThreadItem = {
  readonly id: string;
  readonly kind: "human" | "assistant" | "thinking" | "tool" | "team" | "approval";
  readonly title: string;
  readonly body: string;
  readonly meta?: string;
  readonly timestampUnixMs?: number;
  readonly status?: string;
  readonly output?: string;
  readonly toolDiff?: AgentToolDiff;
  readonly processCard?: AgentProcessCard;
};

type AgentProcessCard = {
  readonly state: "running" | "completed";
  readonly title: string;
  readonly command: string;
  readonly pid?: number;
  readonly processId?: string;
  readonly cwd?: string;
  readonly duration?: string;
  readonly injectedAt?: string;
};

type AgentToolDiffLine = {
  readonly oldLine?: number;
  readonly newLine?: number;
  readonly kind: "context" | "add" | "remove";
  readonly text: string;
};

type AgentToolDiff = {
  readonly path: string;
  readonly added: number;
  readonly removed: number;
  readonly summary: string;
  readonly rows: readonly AgentToolDiffLine[];
};

type SessionTranscriptEntry = SessionDetailState["transcript"][number];

type RecentCommitItem = {
  readonly hash: string;
  readonly title: string;
  readonly repo: string;
  readonly age: string;
  readonly head?: boolean;
};

type RecentChangeItem = {
  readonly label: string;
  readonly count?: number;
};

type OpenAIUsageWindow = {
  readonly label: string;
  readonly remainingPercent: number;
  readonly resetText: string;
};

type OpenAIAccountUsage = {
  readonly email: string;
  readonly plan: string;
  readonly authMode: string;
  readonly authFilePath: string;
  readonly windows: readonly OpenAIUsageWindow[];
};

type ProcessMonitorFilter = "running" | "completed" | "all";
type ProcessMonitorStream = "stdout" | "stderr";

type ProcessMonitorItem = {
  readonly processId: string;
  readonly status: string;
  readonly command: string;
  readonly age: string;
  readonly agentId: string;
  readonly title: string;
  readonly cwd: string;
  readonly exitCode?: number;
  readonly stdout: string;
  readonly stderr: string;
  readonly fixture?: boolean;
};

type ComposerEffort = "medium" | "high" | "extra-high";

const COMPOSER_EFFORT_OPTIONS: ReadonlyArray<{
  readonly value: ComposerEffort;
  readonly label: string;
}> = [
  { value: "medium", label: "Medium" },
  { value: "high", label: "High" },
  { value: "extra-high", label: "Extra High" }
];

const PROCESS_MONITOR_FILTERS: ReadonlyArray<{
  readonly id: ProcessMonitorFilter;
  readonly label: string;
}> = [
  { id: "running", label: "Running" },
  { id: "completed", label: "Completed" },
  { id: "all", label: "All" }
];

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
  const sessionOptions = useMemo(
    () => mergeSessionOptions(sessions, fleetRows),
    [fleetRows, sessions]
  );
  const [activeView, setActiveView] = useState<WorkspaceView>("board");
  const [selectedRowId, setSelectedRowId] = useState("");
  const [selectedSessionId, setSelectedSessionId] = useState("");
  const [selectedTeamId, setSelectedTeamId] = useState("");
  const [selectedApprovalId, setSelectedApprovalId] = useState("");
  const [selectedProcessId, setSelectedProcessId] = useState("");
  const [addAgentDialogOpen, setAddAgentDialogOpen] = useState(false);
  const [processMonitorOpen, setProcessMonitorOpen] = useState(false);
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
      devAutoConnectStarted ||
      !goosewebConfig.flags.devTicketAutoConnect ||
      (state.connection !== "idle" && state.connection !== "offline")
    ) {
      return;
    }

    devAutoConnectStarted = true;
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
    sessionOptions.find((session) => session.sessionId === selectedSessionId) ??
    sessionOptions.find((session) => session.sessionId === selectedRow?.sessionId) ??
    sessionOptions[0];
  const selectedAgentSession =
    sessionOptions.find((session) => session.sessionId === selectedSessionId) ??
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

  const staleSourceIds = Object.keys(state.staleSources);
  const ledgerEvents = useMemo(
    () =>
      buildLedgerEvents({
        fleetRows,
        teams,
        approvals,
        processes,
        sources,
        connection: state.connection,
        staleSourceIds
      }),
    [approvals, fleetRows, processes, sources, staleSourceIds, state.connection, teams]
  );
  const activeSubscriptions = subscriptions.filter(
    (subscription) => subscription.status !== "unsubscribed"
  );
  const sourceGapActive =
    state.connection === "stale" ||
    state.connection === "offline" ||
    state.connection === "connecting" ||
    state.connection === "reconnecting" ||
    staleSourceIds.length > 0;

  function addSelectedAgentToTeam() {
    setActiveView("teams");
    setAddAgentDialogOpen(true);
  }

  return (
    <div className="mission-shell bg-background text-foreground">
      <MissionChrome
        activeView={activeView}
        approvals={approvals}
        processes={processes}
        state={state}
        sources={sources}
        subscriptionCount={activeSubscriptions.length}
        onProcessMonitorOpen={() => setProcessMonitorOpen(true)}
        onViewChange={setActiveView}
      />
      <div
        className={cn(
          "mission-grid min-h-0",
          activeView === "agents" && "mission-grid-agents"
        )}
      >
        <MissionRosterRail
          rows={fleetRows}
          sessions={sessionOptions}
          teams={teams}
          approvals={approvals}
          processes={processes}
          sources={sources}
          selectedRowId={selectedRowId}
          selectedSessionId={selectedSessionId}
          selectedTeamId={selectedTeamId}
          selectedApprovalId={selectedApprovalId}
          selectedProcessId={selectedProcessId}
          sourceGapActive={sourceGapActive}
          addAgentDisabled={sourceGapActive || (!sessionOptions.length && !sources.length)}
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
            sessions={sessionOptions}
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
            addAgentDialogOpen={addAgentDialogOpen}
            onAddAgentDialogOpenChange={setAddAgentDialogOpen}
          />
        </main>

        {activeView === "agents" ? null : (
          <MissionProcessRail
            processes={processes}
            selectedProcess={selectedProcess}
            selectedRow={selectedRow}
            selectedSession={selectedSession}
            selectedTeam={selectedTeam}
            selectedApproval={selectedApproval}
            selectedWorktree={selectedWorktree}
            sources={sources}
            connection={state.connection}
            staleSourceIds={staleSourceIds}
            pendingCommandCount={pendingCommands.length}
            sourceGapActive={sourceGapActive}
            onSelectProcess={setSelectedProcessId}
          />
        )}
      </div>
      <AgentsProcessMonitorSheet
        open={processMonitorOpen}
        processes={processes}
        sourceGapActive={sourceGapActive}
        onOpenChange={setProcessMonitorOpen}
      />
    </div>
  );
}

function MissionChrome({
  activeView,
  approvals,
  processes,
  state,
  sources,
  subscriptionCount,
  onProcessMonitorOpen,
  onViewChange
}: {
  readonly activeView: WorkspaceView;
  readonly approvals: readonly ApprovalView[];
  readonly processes: readonly ProcessView[];
  readonly state: GoosewebSnapshot;
  readonly sources: readonly SourceHealthView[];
  readonly subscriptionCount: number;
  readonly onProcessMonitorOpen: () => void;
  readonly onViewChange: (view: WorkspaceView) => void;
}) {
  const source = sources[0];
  return (
    <header className="mission-chrome">
      <div className="mission-chrome-tools">
        <Button size="icon-sm" type="button" variant="ghost">
          <FolderIcon />
        </Button>
        {activeView === "agents" ? (
          <Button
            aria-label="Open processes panel"
            aria-pressed={false}
            className="mission-chrome-tool-active"
            size="icon-sm"
            title="Processes"
            type="button"
            variant="ghost"
            onClick={onProcessMonitorOpen}
          >
            <ActivityIcon />
          </Button>
        ) : (
          <Button size="icon-sm" type="button" variant="ghost">
            <SquareIcon />
          </Button>
        )}
        <Button size="icon-sm" type="button" variant="ghost">
          <ChevronDownIcon />
        </Button>
      </div>
      <nav className="mission-top-nav" aria-label="Workspace views">
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
              aria-current={activeView === item.id ? "page" : undefined}
              className="mission-top-nav-button"
              key={item.id}
              size="sm"
              type="button"
              variant={activeView === item.id ? "secondary" : "ghost"}
              onClick={() => onViewChange(item.id)}
            >
              <Icon data-icon="inline-start" />
              <span>{item.label}</span>
              {count ? <Badge variant="outline">{count}</Badge> : null}
            </Button>
          );
        })}
      </nav>
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
  approvals,
  rows,
  sessions,
  teams,
  processes,
  sources,
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
  readonly approvals: readonly ApprovalView[];
  readonly rows: readonly FleetRowView[];
  readonly sessions: readonly SessionView[];
  readonly teams: readonly TeamView[];
  readonly processes: readonly ProcessView[];
  readonly sources: readonly SourceHealthView[];
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
  const groups = getAgentRosterGroups({
    rows,
    sessions,
    teams,
    approvals,
    processes,
    sources,
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
  const fixtureGroups =
    isRosterVisualFixtureEnabled() && groups.every((group) => group.items.length === 0)
      ? getDevAgentRosterGroups({
          selectedSessionId,
          onSelectSession,
          onSelectRow,
          onSelectTeam,
          onSelectApproval,
          onSelectProcess
        })
      : [];
  const visibleGroups = fixtureGroups.length ? fixtureGroups : groups;
  return (
    <aside className="mission-roster">
      <div className="mission-roster-scroll">
        <div className="mission-roster-summary">
          <h2>Agents</h2>
          <StatusBadge status={sourceGapActive ? "stale" : "live"} />
        </div>

        <div className="mission-roster-groups">
          {visibleGroups.map((group) => (
            <section className="mission-roster-group" key={group.id}>
              <div className="mission-roster-group-heading">
                <span className="mission-roster-group-name">{group.label}</span>
                <span className="mission-roster-group-count">{group.count}</span>
              </div>
              {group.items.length ? (
                <div className="mission-roster-list">
                  {group.items.map((item) => (
                    <button
                      className={cn(
                        "mission-roster-card",
                        item.selected && "mission-roster-card-active"
                      )}
                      key={item.id}
                      title={item.title}
                      type="button"
                      onClick={() => {
                        item.onClick();
                        onViewChange("agents");
                      }}
                    >
                      <span
                        className="mission-roster-status"
                        data-activity={item.activity}
                        data-status={item.status}
                      >
                        {item.activity === "process" ? (
                          <ActivityIcon aria-hidden="true" />
                        ) : (
                          <span aria-hidden="true" />
                        )}
                      </span>
                      <span className="mission-roster-card-main">
                        <span className="mission-roster-card-title">
                          {item.title}
                        </span>
                        <span className="mission-roster-card-meta">
                          {item.meta}
                        </span>
                      </span>
                      <span className="mission-roster-card-side">
                        <span>{item.aside}</span>
                      </span>
                    </button>
                  ))}
                </div>
              ) : null}
            </section>
          ))}
          {visibleGroups.length === 0 ? (
            <div className="mission-roster-quiet" aria-hidden="true" />
          ) : null}
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

type AgentRosterItem = {
  readonly id: string;
  readonly title: string;
  readonly meta: string;
  readonly aside: string;
  readonly status: string;
  readonly activity: "turn" | "process" | "unread" | "idle";
  readonly selected: boolean;
  readonly onClick: () => void;
};

type AgentRosterGroup = {
  readonly id: string;
  readonly label: string;
  readonly count: number;
  readonly items: readonly AgentRosterItem[];
};

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
  staleSourceIds,
  addAgentDialogOpen,
  onAddAgentDialogOpenChange
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
  readonly addAgentDialogOpen: boolean;
  readonly onAddAgentDialogOpenChange: (open: boolean) => void;
}) {
  const [composerText, setComposerText] = useState("");
  const [composerExpanded, setComposerExpanded] = useState(false);
  const [composerEffort, setComposerEffort] = useState<ComposerEffort>("high");
  const [openAIAccountOpen, setOpenAIAccountOpen] = useState(false);
  const openAIAccountUsage = getOpenAIAccountUsageFixture();
  const hasAgentThreadComposer =
    activeView === "agents" && Boolean(selectedSession?.sessionId);
  const showAgentThreadComposer = activeView === "agents";
  const isAgentThread = activeView === "agents";
  const sessionOptions = useMemo(
    () => mergeSessionOptions(sessions, rows),
    [rows, sessions]
  );
  const canInterruptSelectedTurn =
    hasAgentThreadComposer && Boolean(selectedSession?.activeTurnId) && !sourceGapActive;

  useEffect(() => {
    if (!hasAgentThreadComposer && composerText) {
      setComposerText("");
    }
  }, [composerText, hasAgentThreadComposer]);

  function dispatchComposerMessage() {
    if (
      !hasAgentThreadComposer ||
      !selectedSession ||
      !composerText.trim() ||
      sourceGapActive
    ) {
      return false;
    }
    sendRealtimeCommand(
      makeCommand("session", selectedSession.sessionId, "sendTurn", {
        sessionId: selectedSession.sessionId,
        text: composerText.trim()
      })
    );
    setComposerText("");
    return true;
  }

  function submitComposer(event: FormEvent) {
    event.preventDefault();
    dispatchComposerMessage();
  }

  function handleComposerKeyDown(event: KeyboardEvent<HTMLTextAreaElement>) {
    if (event.key !== "Enter" || event.shiftKey || event.metaKey || event.ctrlKey || event.altKey) {
      return;
    }
    event.preventDefault();
    dispatchComposerMessage();
  }

  function interruptSelectedTurn() {
    if (!selectedSession?.sessionId || !selectedSession.activeTurnId || sourceGapActive) {
      return;
    }
    sendRealtimeCommand(
      makeCommand("session", selectedSession.sessionId, "interruptTurn", {
        sessionId: selectedSession.sessionId,
        turnId: selectedSession.activeTurnId
      })
    );
  }

  return (
    <section
      className={cn(
        "mission-workspace",
        isAgentThread
          ? "mission-workspace-thread"
          : "mission-workspace-dashboard",
        isAgentThread && !selectedSession && "mission-workspace-thread-empty"
      )}
    >
      {isAgentThread ? (
        <>
          <div className="mission-workspace-tab" aria-hidden="true" />
          {selectedSession ? (
            <div className="mission-workspace-header">
              <div>
                <div className="mission-kicker">
                  {agentThreadKicker(selectedSession)}
                </div>
                <h1>{workspaceTitle(activeView, selectedRow, selectedSession, selectedTeam)}</h1>
              </div>
            </div>
          ) : null}

          <ScrollArea className="mission-workspace-scroll">
            <div className="mission-worklog">
              <div className="mission-embedded-pane">
                <MissionViewBody
                  state={state}
                  activeView={activeView}
                  rows={rows}
                  sessions={sessionOptions}
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
                  staleSourceIds={staleSourceIds}
                  addAgentDialogOpen={addAgentDialogOpen}
                  onAddAgentDialogOpenChange={onAddAgentDialogOpenChange}
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
          sessions={sessionOptions}
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
          addAgentDialogOpen={addAgentDialogOpen}
          onAddAgentDialogOpenChange={onAddAgentDialogOpenChange}
        />
      )}

      {showAgentThreadComposer ? (
        <form className="mission-composer" onSubmit={submitComposer}>
          <div className="mission-composer-input-frame">
            <Button
              aria-label={composerExpanded ? "Minimize composer" : "Maximize composer"}
              className="mission-composer-expand"
              size="icon-sm"
              type="button"
              variant="ghost"
              onClick={() => setComposerExpanded((expanded) => !expanded)}
            >
              {composerExpanded ? <Minimize2Icon /> : <Maximize2Icon />}
            </Button>
            <Textarea
              aria-label="Agent thread composer"
              className={cn(composerExpanded && "mission-composer-input-expanded")}
              disabled={!hasAgentThreadComposer}
              value={composerText}
              onChange={(event) => setComposerText(event.target.value)}
              onKeyDown={handleComposerKeyDown}
              placeholder=""
              rows={4}
            />
          </div>
          <div className="mission-composer-tray">
            <div className="mission-composer-tools">
              <Button
                aria-label="Add attachment"
                disabled
                size="icon-sm"
                title="Attachments are not available in Gooseweb yet"
                type="button"
                variant="ghost"
              >
                <PlusIcon />
              </Button>
              <span className="mission-composer-control">
                <span>{formatComposerModelLabel(selectedSession)}</span>
                <ChevronDownIcon aria-hidden="true" />
              </span>
              <ComposerEffortDropdown
                value={composerEffort}
                onValueChange={setComposerEffort}
              />
              <span className="mission-composer-control mission-composer-control-static">
                <span>{formatComposerModeLabel(selectedSession)}</span>
                <ChevronDownIcon aria-hidden="true" />
              </span>
              <button
                aria-label="Open OpenAI account and usage"
                className="mission-composer-info"
                type="button"
                title={formatComposerContextLabel(selectedSession)}
                onClick={() => setOpenAIAccountOpen(true)}
              >
                <InfoIcon aria-hidden="true" />
              </button>
            </div>
            <div className="mission-composer-actions">
              {canInterruptSelectedTurn ? (
                <Button
                  aria-label="Stop active turn"
                  className="mission-composer-submit"
                  size="icon"
                  type="button"
                  variant="secondary"
                  onClick={interruptSelectedTurn}
                >
                  <SquareIcon />
                </Button>
              ) : (
                <Button
                  aria-label="Send agent thread message"
                  className="mission-composer-submit"
                  disabled={!hasAgentThreadComposer || !composerText.trim() || sourceGapActive}
                  size="icon"
                  type="submit"
                  variant="secondary"
                >
                  <ArrowUpIcon />
                </Button>
              )}
            </div>
          </div>
          <OpenAIAccountUsageSheet
            account={openAIAccountUsage}
            open={openAIAccountOpen}
            onOpenChange={setOpenAIAccountOpen}
          />
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
  sourceGapActive,
  staleSourceIds,
  addAgentDialogOpen,
  onAddAgentDialogOpenChange
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
  readonly staleSourceIds: readonly string[];
  readonly addAgentDialogOpen: boolean;
  readonly onAddAgentDialogOpenChange: (open: boolean) => void;
}) {
  if (activeView === "agents") {
    return (
      <AgentPane
        approvals={approvals}
        processes={processes}
        selectedSession={selectedSession}
        sessionDetail={
          selectedSession ? sessionDetails[selectedSession.sessionId] : undefined
        }
        selectedApproval={selectedApproval}
        teamWorkspaces={teamWorkspaces}
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
        addAgentDialogOpen={addAgentDialogOpen}
        onAddAgentDialogOpenChange={onAddAgentDialogOpenChange}
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
        staleSourceIds={staleSourceIds}
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

function ComposerEffortDropdown({
  value,
  onValueChange
}: {
  readonly value: ComposerEffort;
  readonly onValueChange: (value: ComposerEffort) => void;
}) {
  return (
    <DropdownMenu>
      <DropdownMenuTrigger
        aria-label="Reasoning effort selector"
        className="mission-composer-control mission-composer-control-trigger"
        data-composer-effort-trigger
      >
        <span>{formatComposerEffortLabel(value)}</span>
        <ChevronDownIcon aria-hidden="true" />
      </DropdownMenuTrigger>
      <DropdownMenuContent
        align="start"
        className="mission-composer-menu"
        side="top"
        sideOffset={8}
      >
        <DropdownMenuRadioGroup
          value={value}
          onValueChange={(nextValue) => onValueChange(nextValue as ComposerEffort)}
        >
          {COMPOSER_EFFORT_OPTIONS.map((option) => (
            <DropdownMenuRadioItem
              className="mission-composer-menu-item"
              data-composer-effort-option={option.value}
              key={option.value}
              value={option.value}
            >
              {option.label}
            </DropdownMenuRadioItem>
          ))}
        </DropdownMenuRadioGroup>
      </DropdownMenuContent>
    </DropdownMenu>
  );
}

function OpenAIAccountUsageSheet({
  account,
  open,
  onOpenChange
}: {
  readonly account: OpenAIAccountUsage | null;
  readonly open: boolean;
  readonly onOpenChange: (open: boolean) => void;
}) {
  return (
    <Sheet open={open} onOpenChange={onOpenChange}>
      <SheetContent
        className="mission-openai-sheet"
        side="right"
        showCloseButton={false}
      >
        <div className="mission-openai-sheet-header">
          <SheetTitle className="mission-openai-title">OpenAI</SheetTitle>
          <button
            aria-label="Close OpenAI account and usage"
            className="mission-openai-close"
            type="button"
            onClick={() => onOpenChange(false)}
          >
            <XIcon aria-hidden="true" />
          </button>
        </div>

        <div className="mission-openai-sheet-body">
          <section className="mission-openai-section" aria-label="OpenAI account">
            <h2>Account</h2>
            <div className="mission-openai-account-card" data-openai-account-card>
              {account ? (
                <>
                  <div className="mission-openai-account-main">
                    <div className="mission-openai-account-copy">
                      <strong>{account.email}</strong>
                      <div className="mission-openai-chips">
                        <span>{account.plan}</span>
                        <span>{account.authMode}</span>
                      </div>
                    </div>
                    <div className="mission-openai-actions">
                      <button type="button">Refresh</button>
                      <button type="button">Sign out</button>
                    </div>
                  </div>
                  <div className="mission-openai-auth-path">{account.authFilePath}</div>
                </>
              ) : (
                <div className="mission-openai-unavailable">
                  <strong>Not connected</strong>
                  <div className="mission-openai-chips">
                    <span>local</span>
                    <span>no usage data</span>
                  </div>
                  <div className="mission-openai-auth-path">OpenAI account data unavailable</div>
                </div>
              )}
            </div>
          </section>

          <section className="mission-openai-section" aria-label="OpenAI usage">
            <h2>Usage</h2>
            <div className="mission-openai-usage-card" data-openai-usage-card>
              {account ? (
                <>
                  {account.windows.map((window) => (
                    <OpenAIUsageRow key={window.label} window={window} />
                  ))}
                  <div className="mission-openai-refresh-row">
                    <button type="button">Refresh usage</button>
                  </div>
                </>
              ) : (
                <div className="mission-openai-empty-usage" aria-hidden="true" />
              )}
            </div>
          </section>
        </div>
      </SheetContent>
    </Sheet>
  );
}

function OpenAIUsageRow({ window }: { readonly window: OpenAIUsageWindow }) {
  const progress = Math.min(100, Math.max(0, window.remainingPercent));

  return (
    <div className="mission-openai-usage-row" data-openai-usage-row={window.label}>
      <div className="mission-openai-usage-heading">
        <span>{window.label}</span>
        <span>{progress}%</span>
      </div>
      <div className="mission-openai-progress" aria-label={`${window.label} ${progress}%`}>
        <span style={{ width: `${progress}%` }} />
      </div>
      <p>{window.resetText}</p>
    </div>
  );
}

function AgentsProcessMonitorSheet({
  open,
  processes,
  sourceGapActive,
  onOpenChange
}: {
  readonly open: boolean;
  readonly processes: readonly ProcessView[];
  readonly sourceGapActive: boolean;
  readonly onOpenChange: (open: boolean) => void;
}) {
  const [filter, setFilter] = useState<ProcessMonitorFilter>("running");
  const [activeStream, setActiveStream] = useState<ProcessMonitorStream>("stdout");
  const [followTail, setFollowTail] = useState(true);
  const processItems = getProcessMonitorItems(processes);
  const filteredItems = processItems.filter((process) => processMatchesMonitorFilter(process, filter));
  const selectedItem = filteredItems[0] ?? processItems[0];
  const activeLog = selectedItem ? selectedItem[activeStream] : "";

  useEffect(() => {
    setActiveStream("stdout");
    setFollowTail(true);
  }, [open, selectedItem?.processId]);

  function killProcess(process: ProcessMonitorItem) {
    if (sourceGapActive || process.status !== "running" || process.fixture) {
      return;
    }
    sendRealtimeCommand(
      makeCommand("process", process.processId, "killProcess", {
        processId: process.processId
      })
    );
  }

  return (
    <Sheet open={open} onOpenChange={onOpenChange}>
      <SheetContent
        className="mission-process-monitor-sheet"
        side="right"
        showCloseButton={false}
      >
        <div className="mission-process-monitor-toolbar" aria-label="Process monitor tools">
          <button type="button" aria-label="Process logs" aria-pressed="true">
            <ScrollTextIcon aria-hidden="true" />
          </button>
          <button type="button" aria-label="Process options">
            <ChevronDownIcon aria-hidden="true" />
          </button>
          <button type="button" aria-label="Process settings">
            <SettingsIcon aria-hidden="true" />
          </button>
          <button
            aria-label="Close processes panel"
            className="mission-process-monitor-close"
            type="button"
            onClick={() => onOpenChange(false)}
          >
            <XIcon aria-hidden="true" />
          </button>
        </div>

        <div className="mission-process-monitor-header">
          <SheetTitle className="mission-process-monitor-title">Processes</SheetTitle>
          <div className="mission-process-monitor-tabs" role="tablist" aria-label="Process filters">
            {PROCESS_MONITOR_FILTERS.map((option) => (
              <button
                aria-selected={filter === option.id}
                key={option.id}
                role="tab"
                type="button"
                onClick={() => setFilter(option.id)}
              >
                {option.label}
              </button>
            ))}
          </div>
        </div>

        <div className="mission-process-monitor-body">
          {filteredItems.length ? (
            filteredItems.map((process) => (
              <article className="mission-process-monitor-card" data-process-monitor-card key={process.processId}>
                <div className="mission-process-monitor-card-head">
                  <span
                    className="mission-process-monitor-status"
                    data-status={process.status}
                  >
                    {process.status === "running" ? <span aria-hidden="true" /> : null}
                    {process.status}
                  </span>
                  <span className="mission-process-monitor-age">{process.age}</span>
                  {process.status === "running" ? (
                    <button
                      className="mission-process-monitor-kill"
                      disabled={sourceGapActive && !process.fixture}
                      type="button"
                      onClick={() => killProcess(process)}
                    >
                      Kill
                    </button>
                  ) : null}
                </div>

                <div className="mission-process-monitor-command">{process.command}</div>
                <div className="mission-process-monitor-meta">
                  <span>
                    <b>agent_id</b> {process.agentId}
                  </span>
                  <span>
                    <b>title</b> {process.title}
                  </span>
                  <span>
                    <b>cwd</b> {process.cwd}
                  </span>
                </div>

                <div className="mission-process-monitor-log-shell">
                  <div className="mission-process-monitor-log-tabs">
                    <button
                      aria-selected={activeStream === "stdout"}
                      role="tab"
                      type="button"
                      onClick={() => setActiveStream("stdout")}
                    >
                      stdout
                    </button>
                    <button
                      aria-selected={activeStream === "stderr"}
                      role="tab"
                      type="button"
                      onClick={() => setActiveStream("stderr")}
                    >
                      stderr
                    </button>
                    <button
                      aria-label={`Copy ${activeStream} output`}
                      className="mission-process-monitor-copy"
                      type="button"
                    >
                      <ClipboardListIcon aria-hidden="true" />
                    </button>
                  </div>
                  <label className="mission-process-monitor-follow">
                    <span>Follow tail</span>
                    <input
                      checked={followTail}
                      type="checkbox"
                      onChange={(event) => setFollowTail(event.target.checked)}
                    />
                    <span aria-hidden="true" />
                  </label>
                  <pre className="mission-process-monitor-log" data-process-log-output>
                    {activeLog || "Log stream unavailable."}
                  </pre>
                </div>
              </article>
            ))
          ) : (
            <div className="mission-process-monitor-empty" aria-hidden="true" />
          )}
        </div>
      </SheetContent>
    </Sheet>
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
  staleSourceIds,
  addAgentDialogOpen,
  onAddAgentDialogOpenChange
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
  readonly addAgentDialogOpen: boolean;
  readonly onAddAgentDialogOpenChange: (open: boolean) => void;
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
          staleSourceIds={staleSourceIds}
          addAgentDialogOpen={addAgentDialogOpen}
          onAddAgentDialogOpenChange={onAddAgentDialogOpenChange}
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

function MissionProcessRail({
  processes,
  selectedProcess,
  selectedRow,
  selectedSession,
  selectedTeam,
  selectedApproval,
  selectedWorktree,
  sources,
  connection,
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
  readonly connection: ConnectionState;
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
            items={sources.map((source) => {
              const displayState = sourceDisplayState(source, connection, staleSourceIds);
              return [
                source.displayName || source.sourceId,
                `${displayState.status} / ${displayState.age}`
              ];
            })}
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

function agentThreadKicker(selectedSession?: SessionView): string {
  const status = selectedSession?.status || "selected";
  return `Agent thread / ${status}`;
}

function formatComposerModelLabel(selectedSession?: SessionView): string {
  return selectedSession?.model || "Default";
}

function formatComposerModeLabel(selectedSession?: SessionView): string {
  return selectedSession?.provider || "Runtime";
}

function formatComposerEffortLabel(value: ComposerEffort): string {
  return COMPOSER_EFFORT_OPTIONS.find((option) => option.value === value)?.label || "High";
}

function formatComposerContextLabel(selectedSession?: SessionView): string {
  if (!selectedSession?.sessionId) {
    return "No active context";
  }
  return `Session ${selectedSession.sessionId}`;
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

function mergeSessionOptions(
  sessions: readonly SessionView[],
  rows: readonly FleetRowView[]
): readonly SessionView[] {
  const byId = new Map<string, SessionView>();
  for (const session of sessions) {
    if (session.sessionId) {
      byId.set(session.sessionId, session);
    }
  }
  for (const row of rows) {
    if (!row.sessionId || byId.has(row.sessionId)) {
      continue;
    }
    byId.set(
      row.sessionId,
      create(SessionViewSchema, {
        sourceId: row.sourceId,
        sessionId: row.sessionId,
        provider: row.provider,
        model: row.model,
        status: row.status,
        cwd: "",
        worktreePath: row.worktreePath,
        activeTurnId: ""
      })
    );
  }
  return Array.from(byId.values());
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
  approvals,
  processes,
  selectedSession,
  sessionDetail,
  selectedApproval,
  teamWorkspaces,
  sourceGapActive
}: {
  readonly approvals: readonly ApprovalView[];
  readonly processes: readonly ProcessView[];
  readonly selectedSession?: SessionView;
  readonly sessionDetail?: SessionDetailState;
  readonly selectedApproval?: ApprovalView;
  readonly teamWorkspaces: Readonly<Record<string, TeamWorkspaceState>>;
  readonly sourceGapActive: boolean;
}) {
  const showDevFixture = isThreadVisualFixtureEnabled();
  const showCommitsFixture = isRecentCommitsVisualFixtureEnabled();
  const sessionApprovals = approvals.filter(
    (approval) => approval.sessionId === selectedSession?.sessionId
  );
  const relatedProcesses = processes.filter(
    (process) =>
      !selectedSession?.sourceId || process.sourceId === selectedSession.sourceId
  );
  const focusedApproval =
    selectedApproval?.sessionId === selectedSession?.sessionId
      ? selectedApproval
      : sessionApprovals[0];
  const transcriptItems = sessionDetail?.transcript ?? [];
  const relatedTeamMessages = Object.values(teamWorkspaces)
    .flatMap((workspace) => workspace.messages)
    .filter((message) => {
      if (!selectedSession?.sessionId) {
        return false;
      }
      return (
        message.senderAgentId === selectedSession.sessionId ||
        message.recipientAgentIds.includes(selectedSession.sessionId)
      );
    });
  const threadItems: readonly AgentThreadItem[] = showDevFixture && !selectedSession
    ? DEV_AGENT_THREAD_ITEMS
    : [
    ...transcriptItems.map((entry) => transcriptThreadItem(entry, selectedSession)),
    ...sessionApprovals.map(approvalThreadItem),
    ...relatedTeamMessages.map(teamMessageThreadItem),
    ...relatedProcesses.map(processThreadItem)
  ];

  return (
    <div
      className={cn(
        "mission-agent-thread",
        showCommitsFixture && "mission-agent-thread-with-commits"
      )}
    >
      <div className="mission-agent-thread-main">
        {selectedSession ? (
          <div className="mission-thread-meta" aria-label="Selected session details">
            <span>{selectedSession.provider || "provider unknown"}</span>
            <span>{selectedSession.model || "default model"}</span>
            <span>{selectedSession.status || "status unknown"}</span>
            {selectedSession.activeTurnId ? <span>turn {selectedSession.activeTurnId}</span> : null}
            {selectedSession.cwd ? <span>{selectedSession.cwd}</span> : null}
          </div>
        ) : showDevFixture ? (
          <div className="mission-thread-meta" aria-label="Development thread visual fixture">
            <span>dev visual fixture</span>
            <span>query gated</span>
          </div>
        ) : null}

        <div className="mission-thread-feed">
          {!selectedSession && !showDevFixture ? (
            <div className="mission-thread-empty mission-thread-empty-quiet" aria-hidden="true" />
          ) : threadItems.length === 0 ? (
            <div className="mission-thread-empty">
              {selectedSession?.activeTurnId
                ? `Streaming turn ${selectedSession.activeTurnId}.`
                : "No messages yet."}
            </div>
          ) : (
            threadItems.map((item) => <AgentThreadRow item={item} key={item.id} />)
          )}
        </div>

        {focusedApproval ? (
          <div className="mission-thread-approval">
            <ApprovalCard approval={focusedApproval} sourceGapActive={sourceGapActive} />
          </div>
        ) : null}
      </div>

      {showCommitsFixture ? <RecentCommitsPanel /> : null}
    </div>
  );
}

function AgentThreadRow({ item }: { readonly item: AgentThreadItem }) {
  if (item.kind === "thinking") {
    return (
      <article className="mission-thread-row mission-thread-thinking">
        <button
          aria-expanded="true"
          className="mission-thread-thinking-toggle"
          type="button"
        >
          <span>Thinking</span>
          <ChevronDownIcon aria-hidden="true" />
        </button>
        <div className="mission-thread-thinking-body">{item.body}</div>
      </article>
    );
  }

  if (item.kind === "tool") {
    if (item.toolDiff) {
      return <AgentToolDiffCard item={item} diff={item.toolDiff} />;
    }
    if (item.processCard) {
      return <AgentProcessCardRow item={item} process={item.processCard} />;
    }

    return (
      <article className="mission-thread-row mission-thread-tool" data-thread-row="tool">
        <div className="mission-thread-tool-header">
          <TerminalIcon aria-hidden="true" />
          <div className="min-w-0">
            <div className="mission-thread-tool-title">{item.title}</div>
            <div className="mission-thread-tool-meta">
              {item.meta || item.status || "tool"}
            </div>
          </div>
          {item.status ? <span className="mission-thread-tool-status">{item.status}</span> : null}
        </div>
        {item.output ? (
          <pre className="mission-thread-tool-code">{item.output}</pre>
        ) : null}
        <div className="mission-thread-tool-output">{item.body}</div>
      </article>
    );
  }

  if (item.kind === "team") {
    return (
      <article className="mission-thread-row mission-thread-team" data-thread-row="team">
        <div className="mission-thread-team-header">
          <MessageSquareIcon aria-hidden="true" />
          <span>{item.title}</span>
          <span>{item.timestampUnixMs ? formatTime(item.timestampUnixMs) : item.status}</span>
        </div>
        <div className="mission-thread-team-body">
          <p>{item.body || "(empty message)"}</p>
          {item.meta ? <span>{item.meta}</span> : null}
        </div>
      </article>
    );
  }

  if (item.kind === "approval") {
    return (
      <article className="mission-thread-row mission-thread-tool" data-thread-row="approval">
        <div className="mission-thread-tool-header">
          <ShieldAlertIcon aria-hidden="true" />
          <div className="min-w-0">
            <div className="mission-thread-tool-title">{item.title}</div>
            <div className="mission-thread-tool-meta">{item.meta || "approval"}</div>
          </div>
          {item.status ? <span className="mission-thread-tool-status">{item.status}</span> : null}
        </div>
        <div className="mission-thread-tool-output">{item.body}</div>
      </article>
    );
  }

  return (
    <article
      className={cn(
        "mission-thread-row",
        item.kind === "human" ? "mission-thread-row-human" : "mission-thread-row-assistant"
      )}
      data-thread-row={item.kind}
    >
      <div className="mission-thread-row-label">
        <span>{item.title}</span>
        <span>{item.timestampUnixMs ? formatTime(item.timestampUnixMs) : item.meta}</span>
      </div>
      <div className="mission-thread-row-body">{item.body}</div>
    </article>
  );
}

function AgentProcessCardRow({
  item,
  process
}: {
  readonly item: AgentThreadItem;
  readonly process: AgentProcessCard;
}) {
  const metadata = [
    process.injectedAt ? `injected ${process.injectedAt}` : null,
    typeof process.pid === "number" ? `PID ${process.pid}` : null,
    process.processId,
    process.duration ? `duration ${process.duration}` : null,
    process.cwd
  ].filter((value): value is string => Boolean(value));

  return (
    <article
      className={cn(
        "mission-thread-row mission-thread-tool mission-thread-process-card",
        process.state === "running" && "mission-thread-process-card-running"
      )}
      data-process-card={process.state}
      data-thread-row="tool"
    >
      <div className="mission-thread-process-header">
        <span
          className="mission-thread-process-indicator"
          aria-label={
            process.state === "running"
              ? "Background process is running"
              : "Background process completed"
          }
        />
        <TerminalIcon aria-hidden="true" />
        <span>{process.title}</span>
        <div className="mission-thread-process-actions">
          {process.state === "running" ? (
            <button type="button" aria-label="View process logs" title="View logs">
              View logs
            </button>
          ) : null}
          <button type="button" aria-label="Show process JSON" title="Show process JSON">
            <span aria-hidden="true">{"{}"}</span>
          </button>
        </div>
      </div>

      <div className="mission-thread-process-command">
        <pre>
          <span>$ </span>
          {process.command}
        </pre>
        <button type="button" aria-label="Copy process command" title="Copy command">
          <ClipboardListIcon aria-hidden="true" />
        </button>
      </div>

      {metadata.length ? (
        <div className="mission-thread-process-meta">
          {metadata.map((value) => (
            <span key={`${item.id}:${value}`}>{value}</span>
          ))}
        </div>
      ) : null}

      {item.body ? <div className="mission-thread-tool-output">{item.body}</div> : null}
    </article>
  );
}

function AgentToolDiffCard({
  item,
  diff
}: {
  readonly item: AgentThreadItem;
  readonly diff: AgentToolDiff;
}) {
  return (
    <article
      className="mission-thread-row mission-thread-tool mission-thread-tool-rich"
      data-thread-row="tool"
      data-tool-diff-card
    >
      <div className="mission-thread-diff-header">
        <button
          aria-expanded="true"
          aria-label={`Collapse diff for ${diff.path}`}
          className="mission-thread-diff-toggle"
          type="button"
        >
          <ChevronDownIcon aria-hidden="true" />
        </button>
        <div className="mission-thread-diff-title">
          <span>{item.title}</span>
          <strong>{diff.path}</strong>
        </div>
        <div className="mission-thread-diff-actions">
          <span className="mission-thread-diff-count mission-thread-diff-count-add">
            +{diff.added}
          </span>
          <span className="mission-thread-diff-count mission-thread-diff-count-remove">
            -{diff.removed}
          </span>
          <button type="button" aria-label="Copy diff preview" title="Copy diff preview">
            <ClipboardListIcon aria-hidden="true" />
          </button>
          <button type="button" aria-label="Show tool JSON" title="Show tool JSON">
            <span aria-hidden="true">{"{}"}</span>
          </button>
        </div>
      </div>

      <div className="mission-thread-diff-subheader">
        <span>{item.meta || "tool result"}</span>
        {item.status ? <span>{item.status}</span> : null}
        <span>{diff.summary}</span>
      </div>

      <div className="mission-thread-diff-body" role="table" aria-label={`Diff preview for ${diff.path}`}>
        {diff.rows.map((row, index) => (
          <div
            className={cn("mission-thread-diff-line", `mission-thread-diff-line-${row.kind}`)}
            key={`${item.id}:diff:${index}`}
            role="row"
          >
            <span role="cell">{typeof row.oldLine === "number" ? row.oldLine : ""}</span>
            <span role="cell">{typeof row.newLine === "number" ? row.newLine : ""}</span>
            <code role="cell">{row.text}</code>
          </div>
        ))}
      </div>

      <div className="mission-thread-tool-output">{item.body}</div>
    </article>
  );
}

function RecentCommitsPanel() {
  return (
    <aside className="mission-commit-inspector" aria-label="Recent commits">
      <div className="mission-commit-header">
        <h2>Recent Commits</h2>
        <button type="button" aria-label="Search commits" title="Search commits">
          <SearchIcon aria-hidden="true" />
        </button>
      </div>

      <div className="mission-commit-timeline">
        {DEV_RECENT_COMMITS.map((commit) => (
          <article className="mission-commit-row" key={commit.hash} data-commit-row>
            <span className={cn("mission-commit-dot", commit.head && "mission-commit-dot-head")} />
            <div className="mission-commit-content">
              <div className="mission-commit-topline">
                <span className="mission-commit-hash">{commit.hash}</span>
                <button
                  type="button"
                  aria-label={`Copy commit ${commit.hash}`}
                  title={`Copy ${commit.hash}`}
                >
                  <ClipboardListIcon aria-hidden="true" />
                </button>
                {commit.head ? <span className="mission-commit-head">HEAD</span> : null}
              </div>
              <h3>{commit.title}</h3>
              <p>{commit.repo} · {commit.age}</p>
            </div>
          </article>
        ))}
      </div>

      <div className="mission-change-strip">
        <button type="button" aria-label="Local changes summary">
          <span>{DEV_RECENT_CHANGES.length ? "Changes" : "No changes"}</span>
          <ChevronDownIcon aria-hidden="true" />
        </button>
        {DEV_RECENT_CHANGES.length ? (
          <div className="mission-change-list">
            {DEV_RECENT_CHANGES.map((change) => (
              <span key={change.label}>
                {change.label}
                {typeof change.count === "number" ? ` ${change.count}` : ""}
              </span>
            ))}
          </div>
        ) : null}
      </div>

      <div className="mission-workspace-pill">
        <span>gooselake</span>
        <ClipboardListIcon aria-hidden="true" />
      </div>
    </aside>
  );
}

function transcriptThreadItem(
  entry: SessionTranscriptEntry,
  selectedSession?: SessionView
): AgentThreadItem {
  const isHuman = entry.role === "user";
  return {
    id: entry.id,
    kind: isHuman ? "human" : "assistant",
    title: isHuman ? "Human" : "Agent",
    body: entry.text,
    timestampUnixMs: entry.createdAtUnixMs,
    meta: entry.turnId ? `turn ${entry.turnId}` : selectedSession?.model || ""
  };
}

function approvalThreadItem(approval: ApprovalView): AgentThreadItem {
  return {
    id: approval.approvalId,
    kind: "approval",
    title: approval.summary || "Approval requested",
    body: `${approval.status} / ${approval.risk || "unknown risk"}`,
    meta: approval.turnId || approval.approvalId,
    status: approval.status
  };
}

function teamMessageThreadItem(
  message: TeamWorkspaceState["messages"][number]
): AgentThreadItem {
  const scope = teamMessageScope(message);
  return {
    id: message.id,
    kind: "team",
    title: scope === "broadcast" ? "Team broadcast" : "Direct message",
    body: message.text,
    timestampUnixMs: message.createdAtUnixMs,
    meta: message.senderAgentId ? `from ${message.senderAgentId}` : message.teamId,
    status: scope
  };
}

function processThreadItem(process: ProcessView): AgentThreadItem {
  return {
    id: process.processId,
    kind: "tool",
    title: process.command ? "Process" : "Tool activity",
    body:
      process.exitCode !== 0
        ? `${process.status} / exit ${process.exitCode}`
        : process.status,
    meta: process.processId,
    status: process.status,
    output: process.command || ""
  };
}

function isThreadVisualFixtureEnabled(): boolean {
  if (!import.meta.env.DEV || typeof window === "undefined") {
    return false;
  }
  return new URLSearchParams(window.location.search).has("goosewebThreadFixture");
}

function isRosterVisualFixtureEnabled(): boolean {
  if (!import.meta.env.DEV || typeof window === "undefined") {
    return false;
  }
  const params = new URLSearchParams(window.location.search);
  return params.has("goosewebRosterFixture") || params.has("goosewebThreadFixture");
}

function isRecentCommitsVisualFixtureEnabled(): boolean {
  if (!import.meta.env.DEV || typeof window === "undefined") {
    return false;
  }
  const params = new URLSearchParams(window.location.search);
  return params.has("goosewebCommitsFixture") || params.has("goosewebThreadFixture");
}

function isOpenAIAccountVisualFixtureEnabled(): boolean {
  if (!import.meta.env.DEV || typeof window === "undefined") {
    return false;
  }
  return new URLSearchParams(window.location.search).has("goosewebOpenAIFixture");
}

function getOpenAIAccountUsageFixture(): OpenAIAccountUsage | null {
  return isOpenAIAccountVisualFixtureEnabled() ? DEV_OPENAI_ACCOUNT_USAGE : null;
}

function isProcessMonitorVisualFixtureEnabled(): boolean {
  if (!import.meta.env.DEV || typeof window === "undefined") {
    return false;
  }
  return new URLSearchParams(window.location.search).has("goosewebProcessFixture");
}

function getProcessMonitorItems(processes: readonly ProcessView[]): readonly ProcessMonitorItem[] {
  if (isProcessMonitorVisualFixtureEnabled()) {
    return DEV_PROCESS_MONITOR_ITEMS;
  }

  return processes.map((process) => ({
    processId: process.processId,
    status: process.status || "unknown",
    command: process.command || process.processId,
    age: "--",
    agentId: process.sourceId || "unknown",
    title: process.processId,
    cwd: "--",
    exitCode: process.exitCode,
    stdout: "",
    stderr: ""
  }));
}

function processMatchesMonitorFilter(
  process: ProcessMonitorItem,
  filter: ProcessMonitorFilter
): boolean {
  if (filter === "all") {
    return true;
  }
  if (filter === "running") {
    return process.status === "running";
  }
  return process.status !== "running";
}

const DEV_AGENT_THREAD_ITEMS: readonly AgentThreadItem[] = [
  {
    id: "dev-thread:user",
    kind: "human",
    title: "Human",
    body: "Please inspect the local Gooseweb Agents surface and report the tightest UI issue.",
    meta: "dev exercise"
  },
  {
    id: "dev-thread:thinking",
    kind: "thinking",
    title: "Thinking",
    body:
      "Checking the empty-session baseline first, then comparing the composer and thread rows against the desktop reference."
  },
  {
    id: "dev-thread:assistant",
    kind: "assistant",
    title: "Agent",
    body:
      "The composer now stays anchored at narrow widths. I am moving on to the central thread so message, tool, and team events are readable without adding a dashboard panel.",
    meta: "gpt-5"
  },
  {
    id: "dev-thread:tool",
    kind: "tool",
    title: "Edited",
    body: "Updated the dev stack readiness loop and kept the process-exit path explicit.",
    meta: "tool result",
    status: "completed",
    output: "apply_patch scripts/dev-gooseweb-stack.sh",
    toolDiff: {
      path: "scripts/dev-gooseweb-stack.sh",
      added: 9,
      removed: 4,
      summary: "readiness loop",
      rows: [
        { oldLine: 188, newLine: 188, kind: "context", text: "local label=\"$2\"" },
        { oldLine: 189, kind: "remove", text: "local attempts=\"${3:-120}\"" },
        { newLine: 189, kind: "add", text: "local pid=\"${3:-}\"" },
        { newLine: 190, kind: "add", text: "local attempts=\"${4:-1200}\"" },
        { oldLine: 190, newLine: 191, kind: "context", text: "local delay=\"${4:-0.5}\"" },
        { oldLine: 196, newLine: 197, kind: "context", text: "fi" },
        {
          newLine: 198,
          kind: "add",
          text: "if [[ -n \"${pid}\" ]] && ! kill -0 \"${pid}\" 2>/dev/null; then"
        },
        {
          newLine: 199,
          kind: "add",
          text: "  echo \"${label} process exited before ${url} became ready\" >&2"
        },
        { newLine: 200, kind: "add", text: "  exit 1" },
        { newLine: 201, kind: "add", text: "fi" },
        { oldLine: 197, newLine: 202, kind: "context", text: "sleep \"${delay}\"" }
      ]
    }
  },
  {
    id: "dev-thread:process-running",
    kind: "tool",
    title: "Running background process",
    body: "",
    meta: "proc_68",
    status: "running",
    processCard: {
      state: "running",
      title: "Running background process",
      command: "bun run --cwd apps/gooseweb build",
      pid: 43221,
      processId: "proc_68",
      cwd: "/Users/ashray/code/amxv/gooselake"
    }
  },
  {
    id: "dev-thread:process-result",
    kind: "tool",
    title: "Background process result",
    body: "",
    meta: "proc_68",
    status: "completed",
    processCard: {
      state: "completed",
      title: "Background process result",
      command: "bun run --cwd apps/gooseweb build",
      pid: 43221,
      processId: "proc_68",
      duration: "43s",
      injectedAt: "Jul 8 at 11:33:12 PM"
    }
  },
  {
    id: "dev-thread:team",
    kind: "team",
    title: "Direct message",
    body:
      "Accepted the responsive composer pass. Continue with the thread renderer and keep the commit focused.",
    meta: "from platinum_pearl",
    status: "direct"
  },
  {
    id: "dev-thread:approval",
    kind: "approval",
    title: "Approval requested",
    body: "pending / medium risk",
    meta: "turn dev-turn",
    status: "pending"
  }
];

const DEV_RECENT_COMMITS: readonly RecentCommitItem[] = [
  {
    hash: "cfcf893",
    title: "Polish agents roster rows",
    repo: "amxv",
    age: "now",
    head: true
  },
  {
    hash: "72d012b",
    title: "Polish agents thread rendering",
    repo: "amxv",
    age: "18m"
  },
  {
    hash: "e93ab4f",
    title: "Polish agents composer controls",
    repo: "amxv",
    age: "36m"
  },
  {
    hash: "9c28b1d",
    title: "Polish agents roster sidebar",
    repo: "amxv",
    age: "47m"
  },
  {
    hash: "a0f9c01",
    title: "Remove gooseweb traffic light chrome",
    repo: "amxv",
    age: "60m"
  }
];

const DEV_RECENT_CHANGES: readonly RecentChangeItem[] = [];

const DEV_OPENAI_ACCOUNT_USAGE: OpenAIAccountUsage = {
  email: "gooseweb.fixture@example.com",
  plan: "pro",
  authMode: "ChatGPT OAuth",
  authFilePath: "/Users/ashray/.gg/codex/auth.json",
  windows: [
    {
      label: "5-hour window",
      remainingPercent: 83,
      resetText: "Resets Jul 9 at 1:54 AM"
    },
    {
      label: "Weekly window",
      remainingPercent: 89,
      resetText: "Resets Jul 14 at 12:16 PM"
    }
  ]
};

const DEV_PROCESS_MONITOR_ITEMS: readonly ProcessMonitorItem[] = [
  {
    processId: "proc_fixture_running",
    status: "running",
    command: "make dev",
    age: "2h 39m",
    agentId: "platinum_pearl",
    title: "Gooseweb Browser QA Lead",
    cwd: "/Users/ashray/code/amxv/gooselake",
    stdout:
      "2026-07-08T18:07:05.893880Z  INFO gateway audit subscribe.changed\n" +
      "connection_id=conn_29\n" +
      "subscription_id=sources:health\n" +
      "view_kind=fleet\n" +
      "2026-07-08T18:07:05.894033Z  INFO gateway audit subscribe.changed\n" +
      "connection_id=conn_29\n" +
      "subscription_id=teams:list\n" +
      "view_kind=teams\n" +
      "2026-07-08T18:07:05.894233Z  INFO gateway audit subscribe.changed\n" +
      "connection_id=conn_29\n" +
      "subscription_id=ledger:recent\n" +
      "view_kind=ledger",
    stderr:
      "gooseweb dev stack watching local source changes\n" +
      "vite dev server ready on http://127.0.0.1:13001\n" +
      "runtime gateway ready on http://127.0.0.1:18090",
    fixture: true
  },
  {
    processId: "proc_fixture_completed",
    status: "exited",
    command: "bun run --cwd apps/gooseweb build",
    age: "43s",
    agentId: "agricultural_venture",
    title: "Gooseweb Agents Fixer",
    cwd: "/Users/ashray/code/amxv/gooselake",
    exitCode: 0,
    stdout:
      "vite v7.3.6 building client environment for production...\n" +
      "transforming...\n" +
      "2332 modules transformed.\n" +
      "rendering chunks...\n" +
      "built in 10.73s",
    stderr:
      "WARNING Cannot find base config file \"astro/tsconfigs/strict\"\n" +
      "external imports from TanStack SSR server were not used",
    fixture: true
  }
];

function TeamPane({
  teams,
  rows,
  sessions,
  sources,
  selectedTeam,
  teamWorkspace,
  setSelectedTeamId,
  pendingCommands,
  sourceGapActive,
  addAgentDialogOpen,
  onAddAgentDialogOpenChange
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
  readonly addAgentDialogOpen: boolean;
  readonly onAddAgentDialogOpenChange: (open: boolean) => void;
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
  const [spawnTitle, setSpawnTitle] = useState("");
  const [spawnPrompt, setSpawnPrompt] = useState("");
  const [teamSourceId, setTeamSourceId] = useState(defaultSourceId);
  const [teamName, setTeamName] = useState("Live Team");
  const [leadAgentId, setLeadAgentId] = useState(defaultLeadId);
  const [joinAgentId, setJoinAgentId] = useState("");
  const joinActivationHandledRef = useRef(false);
  const members = selectedTeam?.members ?? [];
  const deliveries = teamWorkspace?.deliveries ?? [];
  const teamPendingCommands = pendingCommands.filter(
    (command) => command.targetScope === "team" && command.targetScopeId === selectedTeam?.teamId
  );
  const teamFeed = useMemo(
    () => buildTeamFeed(selectedTeam, teamWorkspace, pendingCommands),
    [pendingCommands, selectedTeam, teamWorkspace]
  );
  const memberAgentIds = new Set(
    members.flatMap((member) => [member.memberId, member.sessionId].filter(Boolean))
  );
  const recipientOptions = unique(
    members.map((member) => teamMemberIdentity(member)).filter(Boolean)
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
            recipientMemberId: recipient || recipientOptions[0] || "",
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

  function joinAgentToTeamFromActivation() {
    if (joinActivationHandledRef.current) {
      return;
    }
    joinActivationHandledRef.current = true;
    joinAgentToTeam();
  }

  function joinAgentToTeamFromClick() {
    if (joinActivationHandledRef.current) {
      joinActivationHandledRef.current = false;
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
    onAddAgentDialogOpenChange(false);
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
                disabled={sourceGapActive}
                type="button"
                onClick={() => onAddAgentDialogOpenChange(true)}
              >
                <PlusIcon data-icon="inline-start" />
                Add agent
              </Button>
            </CardAction>
          </CardHeader>
          <CardContent className="flex min-h-0 flex-1 flex-col gap-3">
            <div className="grid grid-cols-3 gap-2">
              <MetricCard label="lead" value={lead?.title || lead?.memberId || "unset"} />
              <MetricCard label="members" value={String(members.length)} />
              <MetricCard label="team id" value={selectedTeam?.teamId || "none"} />
            </div>
            <TeamStream
              items={teamFeed}
              sourceGapActive={sourceGapActive}
              teamId={selectedTeam?.teamId ?? ""}
            />
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
                        value={recipient || recipientOptions[0] || ""}
                        options={recipientOptions}
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
          <Card className="min-h-0 flex-1">
            <CardHeader className="border-b">
              <CardTitle>Team roster</CardTitle>
              <CardDescription>{members.length} members</CardDescription>
            </CardHeader>
            <CardContent className="min-h-0 flex-1 overflow-auto p-2">
              <div className="grid gap-2">
                {members.length === 0 ? (
                  <EmptyBlock title="No members" detail="Waiting for team snapshot." />
                ) : (
                  members.map((member) => (
                    <MemberCard key={member.memberId} leadId={selectedTeam?.leadMemberId ?? ""} member={member} />
                  ))
                )}
              </div>
            </CardContent>
          </Card>
          <ContextCard
            title="Delivery queue"
            items={[
              ["records", String(deliveries.length)],
              ["pending commands", String(teamPendingCommands.length)],
              ["latest status", deliveries.at(-1)?.status ?? teamPendingCommands.at(-1)?.status]
            ]}
          />
        </div>
      </div>
      <Dialog open={addAgentDialogOpen} onOpenChange={onAddAgentDialogOpenChange}>
        <DialogContent className="max-w-2xl">
          <DialogHeader>
            <DialogTitle>Add agent to team</DialogTitle>
            <DialogDescription>
              {selectedTeam
                ? `Target team: ${selectedTeam.name || selectedTeam.teamId}.`
                : "Select an existing team to join or spawn members, or create a team from a live source."}
            </DialogDescription>
          </DialogHeader>
          <div className="grid gap-4">
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
                type="submit"
              >
                <PlusIcon data-icon="inline-start" />
                Create team
              </Button>
            </form>
            {selectedTeam ? (
              <div className="grid grid-cols-2 gap-3">
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
                    onMouseUp={joinAgentToTeamFromActivation}
                    onPointerUp={joinAgentToTeamFromActivation}
                  >
                    <PlusIcon data-icon="inline-start" />
                    Join selected agent
                  </button>
                </form>
                <form className="grid gap-3 rounded-md border bg-muted/20 p-3" onSubmit={spawnMember}>
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
                      rows={4}
                    />
                  </Field>
                  <Button disabled={!spawnTitle.trim() || sourceGapActive} type="submit">
                    Spawn teammate
                  </Button>
                </form>
              </div>
            ) : (
              <EmptyBlock title="No team selected" detail="Create or select a team before joining or spawning members." />
            )}
          </div>
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
  connection,
  staleSourceIds
}: {
  readonly sources: readonly SourceHealthView[];
  readonly rows: readonly FleetRowView[];
  readonly processes: readonly ProcessView[];
  readonly connection: ConnectionState;
  readonly staleSourceIds: readonly string[];
}) {
  const source = sources[0];
  const sourceState = source ? sourceDisplayState(source, connection, staleSourceIds) : undefined;
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
          <MetricCard label="health" value={sourceState?.status || connection} />
          <MetricCard label="stale age" value={sourceState?.age || "not observed"} />
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
            {sources.length ? sources.map((item) => {
              const itemState = sourceDisplayState(item, connection, staleSourceIds);
              return (
                <div className="rounded-md border border-border/70 p-2" key={item.sourceId}>
                  <div className="flex items-center justify-between gap-2">
                    <div className="min-w-0">
                      <div className="truncate text-sm font-medium">{item.displayName || item.sourceId}</div>
                      <div className="truncate text-xs text-muted-foreground">
                        {item.sourceKind} · {item.provisionerKind || "static"} · {item.region || "region unknown"}
                      </div>
                    </div>
                    <Badge variant={itemState.healthy ? "default" : "secondary"}>
                      {itemState.status}
                    </Badge>
                  </div>
                  <div className="mt-2 grid grid-cols-2 gap-1 text-xs text-muted-foreground">
                    <span>{item.supportsWorktrees ? "worktrees" : "no worktrees"}</span>
                    <span>{item.supportsTeams ? "teams" : "no teams"}</span>
                    <span>{item.models.length ? item.models.join(", ") : "models unknown"}</span>
                    <span>{item.costHint || "cost unknown"}</span>
                  </div>
                </div>
              );
            }) : (
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
        <CardDescription>{approval?.status || "No approval pending."}</CardDescription>
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
          <EmptyBlock title="No approval pending." />
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
        <CardTitle>{member.title || teamMemberIdentity(member)}</CardTitle>
        <CardDescription>{member.sessionId || member.memberId}</CardDescription>
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

function TeamStream({
  items,
  sourceGapActive,
  teamId
}: {
  readonly items: readonly TeamFeedItem[];
  readonly sourceGapActive: boolean;
  readonly teamId: string;
}) {
  return (
    <Card className="min-h-64 flex-1 bg-muted/20" size="sm">
      <CardHeader className="border-b">
        <CardTitle>Team stream</CardTitle>
        <CardDescription>{items.length} events</CardDescription>
      </CardHeader>
      <CardContent className="min-h-0 flex-1 overflow-auto p-2">
        <div className="flex flex-col gap-2">
          {items.length === 0 ? (
            <EmptyBlock title="No team activity" detail="Send a team message to start the stream." />
          ) : (
            items.map((item) => (
              <div
                className={cn(
                  "grid gap-2 rounded-md border bg-background/80 p-2",
                  item.kind === "message" && "border-primary/30 bg-primary/5",
                  item.kind === "member" && "border-dashed bg-muted/30"
                )}
                key={item.id}
              >
                <div className="flex min-w-0 items-start gap-2">
                  <TeamFeedIcon kind={item.kind} />
                  <div className="min-w-0 flex-1">
                    <div className="flex min-w-0 flex-wrap items-center gap-2">
                      <Badge variant={item.kind === "message" ? "secondary" : "outline"}>
                        {teamFeedKindLabel(item.kind)}
                      </Badge>
                      <span className="truncate text-sm font-medium">{item.title}</span>
                      {item.status ? <StatusBadge status={item.status} /> : null}
                    </div>
                    <div className="mt-1 whitespace-pre-wrap break-words text-sm">{item.body}</div>
                  </div>
                  <span className="shrink-0 text-xs text-muted-foreground">
                    {item.timestampUnixMs ? formatTime(item.timestampUnixMs) : "state"}
                  </span>
                </div>
                <div className="flex flex-wrap items-center gap-1 pl-7">
                  {item.meta.map(([label, value]) => (
                    <MetricChip key={`${item.id}:${label}`} label={label} value={value} />
                  ))}
                  {item.deliveryId ? (
                    <div className="ml-auto flex gap-1">
                      <Button
                        disabled={!teamId || sourceGapActive}
                        size="xs"
                        type="button"
                        variant="outline"
                        onClick={() =>
                          sendRealtimeCommand(
                            makeCommand("team", teamId, "retryDelivery", {
                              deliveryId: item.deliveryId
                            })
                          )
                        }
                      >
                        Retry
                      </Button>
                      <Button
                        disabled={!teamId || !item.cancelMessageId || sourceGapActive}
                        size="xs"
                        type="button"
                        variant="outline"
                        onClick={() =>
                          sendRealtimeCommand(
                            makeCommand("team", teamId, "cancelDelivery", {
                              messageId: item.cancelMessageId
                            })
                          )
                        }
                      >
                        Cancel
                      </Button>
                    </div>
                  ) : null}
                </div>
              </div>
            ))
          )}
        </div>
      </CardContent>
    </Card>
  );
}

function TeamFeedIcon({ kind }: { readonly kind: TeamFeedItem["kind"] }) {
  const className = "mt-0.5 size-4 shrink-0 text-muted-foreground";
  if (kind === "message") {
    return <SendIcon className={className} />;
  }
  if (kind === "member") {
    return <UsersIcon className={className} />;
  }
  if (kind === "command") {
    return <TerminalIcon className={className} />;
  }
  return <ActivityIcon className={className} />;
}

function buildTeamFeed(
  selectedTeam: TeamView | undefined,
  teamWorkspace: TeamWorkspaceState | undefined,
  pendingCommands: readonly PendingCommandState[]
): readonly TeamFeedItem[] {
  if (!selectedTeam) {
    return [];
  }
  const teamId = selectedTeam.teamId;
  const messages = teamWorkspace?.messages ?? [];
  const deliveries = teamWorkspace?.deliveries ?? [];
  const messageById = new Map(messages.map((message) => [message.id, message]));
  const items: TeamFeedItem[] = [
    ...messages.map((message) => teamMessageFeedItem(message)),
    ...deliveries.map((delivery) => teamDeliveryFeedItem(delivery, messageById.get(delivery.messageId))),
    ...pendingCommands
      .filter((command) => command.targetScope === "team" && command.targetScopeId === teamId)
      .map((command) => teamCommandFeedItem(command))
  ];

  if (messages.length + deliveries.length < 4) {
    items.push(
      ...selectedTeam.members.map((member) =>
        teamMemberFeedItem(member, member.memberId === selectedTeam.leadMemberId)
      )
    );
  }

  return items.sort((a, b) => {
    const left = a.timestampUnixMs ?? Number.MAX_SAFE_INTEGER;
    const right = b.timestampUnixMs ?? Number.MAX_SAFE_INTEGER;
    return left === right ? a.id.localeCompare(b.id) : left - right;
  });
}

function teamMessageFeedItem(message: TeamWorkspaceState["messages"][number]): TeamFeedItem {
  const scope = teamMessageScope(message);
  return {
    id: `message:${message.id}`,
    kind: "message",
    title: scope === "direct" ? "Direct message" : "Broadcast message",
    body: message.text || "(empty message)",
    timestampUnixMs: message.createdAtUnixMs,
    meta: [
      ["sender", message.senderAgentId || "unknown"],
      ["recipients", message.recipientAgentIds.length ? message.recipientAgentIds.join(", ") : "team"],
      ["message", message.id]
    ]
  };
}

function teamDeliveryFeedItem(
  delivery: TeamWorkspaceState["deliveries"][number],
  message?: TeamWorkspaceState["messages"][number]
): TeamFeedItem {
  return {
    id: `delivery:${delivery.id}`,
    kind: "delivery",
    title: `Delivery to ${delivery.recipientAgentId || "unknown recipient"}`,
    body: delivery.lastError || message?.text || delivery.injectedTurnId || "Delivery state updated.",
    timestampUnixMs: delivery.updatedAtUnixMs,
    status: delivery.status || "unknown",
    deliveryId: delivery.id,
    cancelMessageId: delivery.messageId,
    meta: [
      ["scope", message ? teamMessageScope(message) : "delivery"],
      ["provider", delivery.provider || "unknown"],
      ["message", delivery.messageId || "unknown"],
      ["turn", delivery.injectedTurnId || "none"]
    ]
  };
}

function teamCommandFeedItem(command: PendingCommandState): TeamFeedItem {
  return {
    id: `command:${command.commandId}`,
    kind: "command",
    title: command.payloadCase || "team command",
    body:
      command.status === "rejected"
        ? `${command.errorCode ?? "rejected"}: ${command.error ?? "Command rejected"}`
        : command.status === "duplicate"
          ? `${command.errorCode ?? "duplicate"}: ${command.error ?? "Already submitted"}`
          : `Command ${command.status}.`,
    timestampUnixMs: command.createdAtUnixMs,
    status: command.status,
    meta: [
      ["command", command.commandId],
      ["target", command.targetEntityId || command.targetScopeId || "team"]
    ]
  };
}

function teamMemberFeedItem(member: TeamMemberView, isLead: boolean): TeamFeedItem {
  return {
    id: `member:${member.memberId}`,
    kind: "member",
    title: isLead ? "Lead member online" : "Member status",
    body: member.title || teamMemberIdentity(member) || "Team member",
    status: member.status || "unknown",
    meta: [
      ["member", member.memberId || "unknown"],
      ["session", member.sessionId || "none"],
      ["provider", member.provider || "unknown"],
      ["model", member.model || "default"]
    ]
  };
}

function teamMessageScope(message: TeamWorkspaceState["messages"][number]): string {
  const scope = message.scope.toLowerCase();
  if (scope.includes("direct") || message.recipientAgentIds.length === 1) {
    return "direct";
  }
  if (scope.includes("broadcast")) {
    return "broadcast";
  }
  return message.scope || "team";
}

function teamFeedKindLabel(kind: TeamFeedItem["kind"]): string {
  if (kind === "message") {
    return "chat";
  }
  if (kind === "delivery") {
    return "delivery";
  }
  if (kind === "command") {
    return "command";
  }
  return "member";
}

function teamMemberIdentity(member: TeamMemberView): string {
  return member.memberId || member.sessionId || member.title || "";
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

function EmptyBlock({ title, detail }: { readonly title: string; readonly detail?: string }) {
  return (
    <Empty className="min-h-32 border">
      <EmptyHeader>
        <EmptyMedia variant="icon">
          <ClipboardListIcon />
        </EmptyMedia>
        <EmptyTitle>{title}</EmptyTitle>
        {detail ? <EmptyDescription>{detail}</EmptyDescription> : null}
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

function getAgentRosterGroups(input: {
  readonly rows: readonly FleetRowView[];
  readonly sessions: readonly SessionView[];
  readonly teams: readonly TeamView[];
  readonly approvals: readonly ApprovalView[];
  readonly processes: readonly ProcessView[];
  readonly sources: readonly SourceHealthView[];
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
}): readonly AgentRosterGroup[] {
  const rowsBySessionId = new Map<string, FleetRowView>();
  for (const row of input.rows) {
    if (row.sessionId && !rowsBySessionId.has(row.sessionId)) {
      rowsBySessionId.set(row.sessionId, row);
    }
  }

  const teamBySessionId = new Map<string, TeamView>();
  const teamMemberBySessionId = new Map<string, TeamMemberView>();
  for (const team of input.teams) {
    for (const member of team.members) {
      if (member.sessionId) {
        teamBySessionId.set(member.sessionId, team);
        teamMemberBySessionId.set(member.sessionId, member);
      }
    }
  }

  const sessionItems = input.sessions.map((session) =>
    makeSessionRosterItem({
      session,
      row: rowsBySessionId.get(session.sessionId),
      team: teamBySessionId.get(session.sessionId),
      member: teamMemberBySessionId.get(session.sessionId),
      approvals: input.approvals,
      processes: input.processes,
      selectedRowId: input.selectedRowId,
      selectedSessionId: input.selectedSessionId,
      selectedTeamId: input.selectedTeamId,
      onSelectRow: input.onSelectRow,
      onSelectSession: input.onSelectSession,
      onSelectTeam: input.onSelectTeam,
      onSelectApproval: input.onSelectApproval,
      onSelectProcess: input.onSelectProcess
    })
  );

  const rowItems = input.sessions.length
    ? []
    : input.rows.map((row) =>
        makeRowRosterItem({
          row,
          approvals: input.approvals,
          processes: input.processes,
          selectedRowId: input.selectedRowId,
          onSelectRow: input.onSelectRow,
          onSelectSession: input.onSelectSession,
          onSelectTeam: input.onSelectTeam,
          onSelectApproval: input.onSelectApproval,
          onSelectProcess: input.onSelectProcess
        })
      );

  const items = [...sessionItems, ...rowItems];
  const itemsBySourceId = new Map<string, AgentRosterItem[]>();
  for (const item of items) {
    const sourceId = item.id.split(":", 1)[0] || "source";
    const bucket = itemsBySourceId.get(sourceId) ?? [];
    bucket.push(item);
    itemsBySourceId.set(sourceId, bucket);
  }

  const teamGroups = input.teams
    .map((team) => {
      const teamItems = items.filter((item) => item.id.includes(`:team:${team.teamId}:`));
      return {
        id: `team:${team.teamId}`,
        label: team.name || team.teamId,
        count: team.members.length,
        items: teamItems
      };
    })
    .filter((group) => group.count > 0 || group.items.length > 0);

  const groupedSessionIds = new Set(
    teamGroups.flatMap((group) => group.items.map((item) => item.id))
  );

  const sourceGroups = input.sources.map((source) => {
    const sourceItems = (itemsBySourceId.get(source.sourceId) ?? []).filter(
      (item) => !groupedSessionIds.has(item.id)
    );
    const sourceName = source.displayName || source.sourceId || "local";
    return {
      id: `source:${source.sourceId}`,
      label: sourceName,
      count: source.activeSessionCount || sourceItems.length,
      items: sourceItems.slice(0, 10)
    };
  });

  const orphanItems = items.filter(
    (item) =>
      !groupedSessionIds.has(item.id) &&
      !input.sources.some((source) => item.id.startsWith(`${source.sourceId}:`))
  );
  const orphanGroups =
    orphanItems.length > 0
      ? [
          {
            id: "source:runtime",
            label: "runtime",
            count: orphanItems.length,
            items: orphanItems.slice(0, 10)
          }
        ]
      : [];

  return [...teamGroups, ...sourceGroups, ...orphanGroups];
}

function getDevAgentRosterGroups(input: {
  readonly selectedSessionId: string;
  readonly onSelectRow: (id: string) => void;
  readonly onSelectSession: (id: string) => void;
  readonly onSelectTeam: (id: string) => void;
  readonly onSelectApproval: (id: string) => void;
  readonly onSelectProcess: (id: string) => void;
}): readonly AgentRosterGroup[] {
  const selectFixture = (sessionId: string, rowId: string, processId = "") => {
    input.onSelectSession(sessionId);
    input.onSelectRow(rowId);
    input.onSelectTeam("dev-roster-team");
    input.onSelectApproval(sessionId === "dev-roster-lead" ? "dev-roster-approval" : "");
    input.onSelectProcess(processId);
  };
  const items: readonly AgentRosterItem[] = [
    {
      id: "dev-roster:team:dev-roster-team:session:dev-roster-lead",
      title: "Lead",
      meta: "Finished Cove",
      aside: "4m",
      status: "running",
      activity: "turn",
      selected: input.selectedSessionId === "dev-roster-lead",
      onClick: () => selectFixture("dev-roster-lead", "dev-roster-row-lead")
    },
    {
      id: "dev-roster:team:dev-roster-team:session:dev-roster-browser",
      title: "Gooseweb Browser QA",
      meta: "Platinum Pearl",
      aside: "now",
      status: "running",
      activity: "process",
      selected: input.selectedSessionId === "dev-roster-browser",
      onClick: () =>
        selectFixture("dev-roster-browser", "dev-roster-row-browser", "dev-roster-process")
    },
    {
      id: "dev-roster:team:dev-roster-team:session:dev-roster-composer",
      title: "Gooseweb Agents Composer",
      meta: "Social Spring",
      aside: "now",
      status: "completed",
      activity: "unread",
      selected: input.selectedSessionId === "dev-roster-composer",
      onClick: () => selectFixture("dev-roster-composer", "dev-roster-row-composer")
    }
  ];
  return [
    {
      id: "dev-roster-group:gooselake",
      label: "gooselake",
      count: items.length,
      items
    }
  ];
}

function makeSessionRosterItem(input: {
  readonly session: SessionView;
  readonly row?: FleetRowView;
  readonly team?: TeamView;
  readonly member?: TeamMemberView;
  readonly approvals: readonly ApprovalView[];
  readonly processes: readonly ProcessView[];
  readonly selectedRowId: string;
  readonly selectedSessionId: string;
  readonly selectedTeamId: string;
  readonly onSelectRow: (id: string) => void;
  readonly onSelectSession: (id: string) => void;
  readonly onSelectTeam: (id: string) => void;
  readonly onSelectApproval: (id: string) => void;
  readonly onSelectProcess: (id: string) => void;
}): AgentRosterItem {
  const { session, row, team, member } = input;
  const pendingForSession = input.approvals.filter(
    (approval) =>
      approval.sessionId === session.sessionId && approval.status === "pending"
  ).length;
  const activeProcess = input.processes.find(
    (process) => process.sourceId === session.sourceId && process.status === "running"
  );
  const activity = toNumber(row?.latestActivityUnixMs ?? 0n);
  const title = member?.title || row?.title || compactSessionId(session.sessionId);
  const meta = [
    team ? "Lead" : undefined,
    session.provider || row?.provider || "runtime",
    session.model || row?.model || "default",
    session.cwd || session.worktreePath ? basename(session.cwd || session.worktreePath) : undefined,
    pendingForSession ? `${pendingForSession} approval` : undefined
  ]
    .filter(Boolean)
    .join(" / ");

  return {
    id: `${session.sourceId}:team:${team?.teamId ?? ""}:session:${session.sessionId}`,
    title,
    meta,
    aside: activity ? ageFrom(activity) : session.activeTurnId ? "now" : session.status || "",
    status: session.status || row?.status || "unknown",
    activity: session.activeTurnId ? "turn" : activeProcess ? "process" : "idle",
    selected:
      session.sessionId === input.selectedSessionId ||
      team?.teamId === input.selectedTeamId ||
      Boolean(row?.rowId && row.rowId === input.selectedRowId),
    onClick: () => {
      input.onSelectSession(session.sessionId);
      if (row?.rowId) {
        input.onSelectRow(row.rowId);
      }
      if (team?.teamId) {
        input.onSelectTeam(team.teamId);
      }
      const pendingApproval = input.approvals.find(
        (approval) =>
          approval.sessionId === session.sessionId && approval.status === "pending"
      );
      if (pendingApproval) {
        input.onSelectApproval(pendingApproval.approvalId);
      }
      if (activeProcess) {
        input.onSelectProcess(activeProcess.processId);
      }
    }
  };
}

function makeRowRosterItem(input: {
  readonly row: FleetRowView;
  readonly approvals: readonly ApprovalView[];
  readonly processes: readonly ProcessView[];
  readonly selectedRowId: string;
  readonly onSelectRow: (id: string) => void;
  readonly onSelectSession: (id: string) => void;
  readonly onSelectTeam: (id: string) => void;
  readonly onSelectApproval: (id: string) => void;
  readonly onSelectProcess: (id: string) => void;
}): AgentRosterItem {
  const { row } = input;
  const activeProcess = input.processes.find(
    (process) => process.sourceId === row.sourceId && process.status === "running"
  );
  return {
    id: `${row.sourceId}:team:${row.teamId}:row:${row.rowId}`,
    title: row.title || compactSessionId(row.sessionId) || row.rowId,
    meta: [
      row.provider || "runtime",
      row.model || "default",
      row.worktreePath ? basename(row.worktreePath) : undefined,
      row.pendingApprovalCount ? `${row.pendingApprovalCount} approval` : undefined
    ]
      .filter(Boolean)
      .join(" / "),
    aside: ageFrom(toNumber(row.latestActivityUnixMs)),
    status: row.status || "unknown",
    activity: row.status === "running" ? "turn" : activeProcess ? "process" : "idle",
    selected: row.rowId === input.selectedRowId,
    onClick: () => {
      input.onSelectRow(row.rowId);
      if (row.sessionId) {
        input.onSelectSession(row.sessionId);
      }
      if (row.teamId) {
        input.onSelectTeam(row.teamId);
      }
      const pendingApproval = input.approvals.find(
        (approval) =>
          approval.sessionId === row.sessionId && approval.status === "pending"
      );
      if (pendingApproval) {
        input.onSelectApproval(pendingApproval.approvalId);
      }
      if (activeProcess) {
        input.onSelectProcess(activeProcess.processId);
      }
    }
  };
}

function compactSessionId(sessionId: string): string {
  if (!sessionId) {
    return "";
  }
  const parts = sessionId.split(/[_:.-]/).filter(Boolean);
  return parts.slice(-2).join(" ") || sessionId;
}

function basename(path: string): string {
  const normalized = path.trim().replace(/\/+$/, "");
  if (!normalized) {
    return "";
  }
  return normalized.split("/").filter(Boolean).pop() ?? normalized;
}

function buildLedgerEvents(input: {
  readonly fleetRows: readonly FleetRowView[];
  readonly teams: readonly TeamView[];
  readonly approvals: readonly ApprovalView[];
  readonly processes: readonly ProcessView[];
  readonly sources: readonly SourceHealthView[];
  readonly connection: ConnectionState;
  readonly staleSourceIds: readonly string[];
}): readonly LedgerEvent[] {
  const events: LedgerEvent[] = [];
  for (const source of input.sources) {
    const displayState = sourceDisplayState(source, input.connection, input.staleSourceIds);
    events.push({
      id: `source:${source.sourceId}:${source.cursor?.sourceSeq ?? 0n}`,
      sourceId: source.sourceId,
      scope: "source",
      kind: `source.${displayState.status}`,
      cursor: `${source.cursor?.sourceEpoch || "epoch"}:${source.cursor?.sourceSeq ?? 0n}`,
      criticality: displayState.status,
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

function sourceDisplayState(
  source: SourceHealthView,
  connection: ConnectionState,
  staleSourceIds: readonly string[]
): { readonly status: string; readonly age: string; readonly healthy: boolean } {
  const rawStatus = firstKnown(source.lifecycle, source.health);
  const isStale = staleSourceIds.includes(source.sourceId);
  const observedAge = ageFrom(toNumber(source.observedAtUnixMs));
  const connected =
    connection === "connected" ||
    connection === "degraded" ||
    connection === "replaying";

  if (isStale) {
    return {
      status: rawStatus || "stale",
      age: observedAge === "unknown" ? "stale" : observedAge,
      healthy: false
    };
  }

  const status = rawStatus || (connected ? "connected" : connection);
  return {
    status,
    age: observedAge === "unknown" && connected ? "not stale" : observedAge,
    healthy: status === "live" || status === "connected"
  };
}

function firstKnown(...values: readonly string[]): string {
  return values.find((value) => value && value !== "unknown") || "";
}

function toNumber(value: number | bigint): number {
  return typeof value === "bigint" ? Number(value) : value;
}
