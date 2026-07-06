import { create, type MessageInitShape } from "@bufbuild/protobuf";
import { createFileRoute } from "@tanstack/react-router";
import {
  ActivityIcon,
  BotIcon,
  BoxesIcon,
  ClipboardListIcon,
  InboxIcon,
  LayoutDashboardIcon,
  RadioIcon,
  ScrollTextIcon,
  SendIcon,
  SettingsIcon,
  ShieldAlertIcon,
  SquareIcon,
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
import {
  CommandSchema,
  type Command
} from "../../src/gen/goosetower/v1/commands_pb";
import {
  EntityRefSchema,
  Scope
} from "../../src/gen/goosetower/v1/common_pb";
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
  sendRealtimeCommand,
  subscribeRealtime,
  unsubscribeRealtime
} from "../../app/realtime/client";
import { goosewebConfig } from "../../app/realtime/config";
import type {
  ConnectionState,
  GoosewebSnapshot,
  PendingCommandState
} from "../../app/realtime/types";
import {
  useGoosewebState
} from "../../app/stores/gooseweb-store";
import { Alert, AlertDescription, AlertTitle } from "~/components/ui/alert";
import { Badge } from "~/components/ui/badge";
import { Button } from "~/components/ui/button";
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
  FieldDescription,
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
  Sidebar,
  SidebarContent,
  SidebarFooter,
  SidebarGroup,
  SidebarGroupContent,
  SidebarGroupLabel,
  SidebarHeader,
  SidebarInset,
  SidebarMenu,
  SidebarMenuBadge,
  SidebarMenuButton,
  SidebarMenuItem,
  SidebarProvider,
  SidebarRail
} from "~/components/ui/sidebar";
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
    () => Object.values(state.pendingCommands),
    [state.pendingCommands]
  );
  const subscriptions = useMemo(
    () => Object.values(state.subscriptions),
    [state.subscriptions]
  );
  const [activeView, setActiveView] = useState<WorkspaceView>("board");
  const [selectedRowId, setSelectedRowId] = useState("");
  const [selectedSessionId, setSelectedSessionId] = useState("");
  const [selectedTeamId, setSelectedTeamId] = useState("");
  const [selectedApprovalId, setSelectedApprovalId] = useState("");
  const [selectedProcessId, setSelectedProcessId] = useState("");
  const [filters, setFilters] = useState<BoardFilters>({
    sourceId: "all",
    teamId: "all",
    status: "all"
  });

  useEffect(() => {
    ensureRealtimeWorker();
    subscribeRealtime("board:window", "fleet-row", { window: "0:120" });
    subscribeRealtime("inbox:pending", "approval", { status: "pending" });
    subscribeRealtime("sources:health", "source-health");
    subscribeRealtime("ledger:recent", "ledger", { window: "0:120" });

    return () => {
      unsubscribeRealtime("board:window");
      unsubscribeRealtime("inbox:pending");
      unsubscribeRealtime("sources:health");
      unsubscribeRealtime("ledger:recent");
    };
  }, []);

  useEffect(() => {
    subscribeRealtime("board:window", "fleet-row", {
      window: "0:120",
      source: filters.sourceId,
      team: filters.teamId,
      status: filters.status
    });
  }, [filters]);

  const selectedRow =
    fleetRows.find((row) => row.rowId === selectedRowId) ?? fleetRows[0];
  const selectedSession =
    sessions.find((session) => session.sessionId === selectedSessionId) ??
    sessions.find((session) => session.sessionId === selectedRow?.sessionId) ??
    sessions[0];
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
    state.connection === "reconnecting" ||
    staleSourceIds.length > 0;

  return (
    <SidebarProvider
      className="h-svh overflow-hidden"
      style={
        {
          "--sidebar-width": "12.5rem",
          "--sidebar-width-icon": "3rem"
        } as React.CSSProperties
      }
    >
      <Sidebar collapsible="icon" variant="sidebar">
        <SidebarHeader>
          <div className="flex items-center gap-2 px-2 py-1">
            <div className="flex size-8 items-center justify-center rounded-lg bg-sidebar-primary text-sidebar-primary-foreground">
              GW
            </div>
            <div className="min-w-0 group-data-[collapsible=icon]:hidden">
              <div className="truncate text-sm font-medium">Gooseweb</div>
              <div className="truncate text-xs text-muted-foreground">
                Goosetower V0
              </div>
            </div>
          </div>
        </SidebarHeader>
        <SidebarContent>
          <SidebarGroup>
            <SidebarGroupLabel>Operate</SidebarGroupLabel>
            <SidebarGroupContent>
              <SidebarMenu>
                {NAV_ITEMS.map((item) => {
                  const Icon = item.icon;
                  const count =
                    item.id === "inbox"
                      ? approvals.filter((approval) => approval.status === "pending").length
                      : undefined;
                  return (
                    <SidebarMenuItem key={item.id}>
                      <SidebarMenuButton
                        isActive={activeView === item.id}
                        tooltip={item.label}
                        onClick={() => setActiveView(item.id)}
                      >
                        <Icon />
                        <span>{item.label}</span>
                      </SidebarMenuButton>
                      {count ? <SidebarMenuBadge>{count}</SidebarMenuBadge> : null}
                    </SidebarMenuItem>
                  );
                })}
              </SidebarMenu>
            </SidebarGroupContent>
          </SidebarGroup>
        </SidebarContent>
        <SidebarFooter>
          <ConnectionBadge connection={state.connection} />
        </SidebarFooter>
        <SidebarRail />
      </Sidebar>

      <SidebarInset className="h-svh min-w-0 overflow-hidden">
        <div className="grid h-full min-h-0 grid-cols-[16rem_minmax(0,1fr)_18rem] bg-background">
          <EntityList
            activeView={activeView}
            rows={fleetRows}
            sessions={sessions}
            teams={teams}
            approvals={approvals}
            processes={processes}
            selectedRowId={selectedRow?.rowId ?? ""}
            selectedSessionId={selectedSession?.sessionId ?? ""}
            selectedTeamId={selectedTeam?.teamId ?? ""}
            selectedApprovalId={selectedApproval?.approvalId ?? ""}
            selectedProcessId={selectedProcess?.processId ?? ""}
            onSelectRow={setSelectedRowId}
            onSelectSession={setSelectedSessionId}
            onSelectTeam={setSelectedTeamId}
            onSelectApproval={setSelectedApprovalId}
            onSelectProcess={setSelectedProcessId}
          />

          <main className="flex min-w-0 flex-col overflow-hidden border-x">
            <TopStatus
              state={state}
              sources={sources}
              subscriptionCount={activeSubscriptions.length}
            />
            <Separator />
            <div className="min-h-0 flex-1 overflow-hidden p-3">
              {sourceGapActive ? (
                <Alert variant="destructive" className="mb-3">
                  <ShieldAlertIcon />
                  <AlertTitle>Source state is not command-safe</AlertTitle>
                  <AlertDescription>
                    Destructive approvals and runtime mutations are disabled until
                    replay catches up or the source returns to a trusted state.
                  </AlertDescription>
                </Alert>
              ) : null}

              {activeView === "board" ? (
                <BoardPane
                  rows={fleetRows}
                  teams={teams}
                  sources={sources}
                  filters={filters}
                  setFilters={setFilters}
                  selectedRowId={selectedRow?.rowId ?? ""}
                  setSelectedRowId={setSelectedRowId}
                />
              ) : null}
              {activeView === "agents" ? (
                <AgentPane
                  sessions={sessions}
                  approvals={approvals}
                  processes={processes}
                  selectedSession={selectedSession}
                  selectedApproval={selectedApproval}
                  setSelectedSessionId={setSelectedSessionId}
                  sourceGapActive={sourceGapActive}
                />
              ) : null}
              {activeView === "teams" ? (
                <TeamPane
                  teams={teams}
                  selectedTeam={selectedTeam}
                  setSelectedTeamId={setSelectedTeamId}
                  pendingCommands={pendingCommands}
                  sourceGapActive={sourceGapActive}
                />
              ) : null}
              {activeView === "inbox" ? (
                <InboxPane
                  approvals={approvals}
                  selectedApprovalId={selectedApproval?.approvalId ?? ""}
                  setSelectedApprovalId={setSelectedApprovalId}
                  sourceGapActive={sourceGapActive}
                />
              ) : null}
              {activeView === "ledger" ? (
                <LedgerPane events={ledgerEvents} sources={sources} />
              ) : null}
              {activeView === "fleet" ? (
                <FleetPane
                  sources={sources}
                  rows={fleetRows}
                  processes={processes}
                  connection={state.connection}
                />
              ) : null}
              {activeView === "playbooks" ? (
                <PlaybooksPane
                  selectedSession={selectedSession}
                  selectedTeam={selectedTeam}
                  sourceGapActive={sourceGapActive}
                />
              ) : null}
              {activeView === "settings" ? (
                <SettingsPane
                  state={state}
                  subscriptionCount={activeSubscriptions.length}
                />
              ) : null}
            </div>
          </main>

          <Inspector
            selectedRow={selectedRow}
            selectedSession={selectedSession}
            selectedTeam={selectedTeam}
            selectedApproval={selectedApproval}
            selectedProcess={selectedProcess}
            selectedWorktree={selectedWorktree}
            sources={sources}
            staleSourceIds={staleSourceIds}
            pendingCommandCount={pendingCommands.length}
          />
        </div>
      </SidebarInset>
    </SidebarProvider>
  );
}

function TopStatus({
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
    <header className="flex h-14 shrink-0 items-center justify-between gap-3 px-4">
      <div className="min-w-0">
        <div className="text-xs uppercase tracking-wide text-muted-foreground">
          Operating workspace
        </div>
        <h1 className="truncate text-base font-medium">
          Runtime control board
        </h1>
      </div>
      <div className="flex min-w-0 items-center gap-2">
        <ConnectionBadge connection={state.connection} />
        <MetricChip label="source" value={source?.displayName || source?.sourceId || "none"} />
        <MetricChip label="seq" value={state.cursor.gatewaySeq.toString()} />
        <MetricChip label="subs" value={String(subscriptionCount)} />
      </div>
    </header>
  );
}

function EntityList({
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
}: {
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

  return (
    <aside className="flex min-w-0 flex-col overflow-hidden bg-muted/20">
      <div className="flex h-14 shrink-0 items-center justify-between px-3">
        <div>
          <div className="text-xs uppercase tracking-wide text-muted-foreground">
            Entity list
          </div>
          <div className="text-sm font-medium">{sidebarTitle(activeView)}</div>
        </div>
        <Badge variant="outline">{items.length}</Badge>
      </div>
      <Separator />
      <ScrollArea className="min-h-0 flex-1">
        <div className="flex flex-col gap-1 p-2">
          {items.length === 0 ? (
            <EmptyBlock title="No entities" detail="Waiting for realtime snapshots." />
          ) : (
            items.map((item) => (
              <Button
                className="h-auto justify-start px-2 py-2"
                key={item.id}
                type="button"
                variant={item.selected ? "secondary" : "ghost"}
                onClick={item.onClick}
              >
                <span className="grid min-w-0 flex-1 gap-0.5 text-left">
                  <span className="truncate text-sm">{item.title}</span>
                  <span className="truncate text-xs text-muted-foreground">
                    {item.meta}
                  </span>
                </span>
                <StatusBadge status={item.status} />
              </Button>
            ))
          )}
        </div>
      </ScrollArea>
    </aside>
  );
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
  selectedSession,
  selectedApproval,
  setSelectedSessionId,
  sourceGapActive
}: {
  readonly sessions: readonly SessionView[];
  readonly approvals: readonly ApprovalView[];
  readonly processes: readonly ProcessView[];
  readonly selectedSession?: SessionView;
  readonly selectedApproval?: ApprovalView;
  readonly setSelectedSessionId: (id: string) => void;
  readonly sourceGapActive: boolean;
}) {
  const [turnText, setTurnText] = useState("");
  const sessionApprovals = approvals.filter(
    (approval) => approval.sessionId === selectedSession?.sessionId
  );
  const timeline = [
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

  function submitTurn(event: FormEvent) {
    event.preventDefault();
    if (!selectedSession || !turnText.trim() || sourceGapActive) {
      return;
    }
    sendRealtimeCommand(
      makeCommand("session", selectedSession.sessionId, "sendTurn", {
        sessionId: selectedSession.sessionId,
        text: turnText.trim()
      })
    );
    setTurnText("");
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
                  <CardTitle>Streaming current response</CardTitle>
                  <CardDescription>
                    Token updates are frame-batched by the realtime worker.
                  </CardDescription>
                </CardHeader>
                <CardContent>
                  {selectedSession.activeTurnId
                    ? `Streaming turn ${selectedSession.activeTurnId}.`
                    : "No active turn stream for this session."}
                </CardContent>
              </Card>
              <form onSubmit={submitTurn}>
                <FieldGroup>
                  <Field>
                    <FieldLabel htmlFor="turn-text">Turn composer</FieldLabel>
                    <Textarea
                      id="turn-text"
                      value={turnText}
                      onChange={(event) => setTurnText(event.target.value)}
                      placeholder="Message selected agent"
                      rows={4}
                    />
                    <FieldDescription>
                      Sends a command through Goosetower with an idempotency key.
                    </FieldDescription>
                  </Field>
                  <Button disabled={!turnText.trim() || sourceGapActive} type="submit">
                    <SendIcon data-icon="inline-start" />
                    Send turn
                  </Button>
                </FieldGroup>
              </form>
            </>
          ) : (
            <EmptyBlock title="No session" detail="Select a board row or session." />
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
  selectedTeam,
  setSelectedTeamId,
  pendingCommands,
  sourceGapActive
}: {
  readonly teams: readonly TeamView[];
  readonly selectedTeam?: TeamView;
  readonly setSelectedTeamId: (id: string) => void;
  readonly pendingCommands: readonly PendingCommandState[];
  readonly sourceGapActive: boolean;
}) {
  const [mode, setMode] = useState<"direct" | "broadcast">("broadcast");
  const [recipient, setRecipient] = useState("");
  const [message, setMessage] = useState("");
  const [spawnOpen, setSpawnOpen] = useState(false);
  const [spawnTitle, setSpawnTitle] = useState("");
  const [spawnPrompt, setSpawnPrompt] = useState("");
  const members = selectedTeam?.members ?? [];
  const lead = members.find((member) => member.memberId === selectedTeam?.leadMemberId);

  function sendMessage(event: FormEvent) {
    event.preventDefault();
    if (!selectedTeam || !message.trim() || sourceGapActive) {
      return;
    }
    sendRealtimeCommand(
      mode === "direct"
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
            <form onSubmit={sendMessage}>
              <FieldGroup>
                <Field>
                  <FieldLabel>Delivery mode</FieldLabel>
                  <ToggleGroup
                    onValueChange={(value) => {
                      const next = Array.isArray(value) ? value[0] : value;
                      if (next === "direct" || next === "broadcast") {
                        setMode(next);
                      }
                    }}
                    value={[mode]}
                    variant="outline"
                  >
                    <ToggleGroupItem value="broadcast">Broadcast</ToggleGroupItem>
                    <ToggleGroupItem value="direct">Direct</ToggleGroupItem>
                  </ToggleGroup>
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
                  <FieldLabel htmlFor="team-message">Team composer</FieldLabel>
                  <Textarea
                    id="team-message"
                    value={message}
                    onChange={(event) => setMessage(event.target.value)}
                    placeholder="Message team"
                    rows={4}
                  />
                </Field>
                <Button disabled={!message.trim() || !selectedTeam || sourceGapActive} type="submit">
                  <SendIcon data-icon="inline-start" />
                  Send
                </Button>
              </FieldGroup>
            </form>
          </CardContent>
        </Card>
        <div className="flex min-h-0 flex-col gap-3">
          <TimelineCard
            title="Delivery state"
            items={pendingCommands.map((command) => ({
              id: command.commandId,
              title: command.commandId,
              meta: command.status
            }))}
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
            items={members.map((member) => ({
              id: member.memberId,
              title: member.title || member.memberId,
              meta: `${member.status || "unknown"} / ${member.provider || "provider"}`
            }))}
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

  return (
    <div className="grid h-full min-h-0 grid-cols-[minmax(0,1fr)_19rem] gap-3">
      <Card>
        <CardHeader>
          <CardTitle>Fleet</CardTitle>
          <CardDescription>V0 one-runtime source view.</CardDescription>
          <CardAction className="flex gap-2">
            <Button disabled variant="outline">Add runtime</Button>
            <Button disabled variant="outline">Provision</Button>
          </CardAction>
        </CardHeader>
        <CardContent className="grid grid-cols-4 gap-2">
          <MetricCard label="health" value={source?.health || connection} />
          <MetricCard label="stale age" value={source ? ageFrom(toNumber(source.observedAtUnixMs)) : "unknown"} />
          <MetricCard label="replay lag" value={source?.cursor ? source.cursor.sourceSeq.toString() : "0"} />
          <MetricCard label="active sessions" value={String(rows.length)} />
          <MetricCard label="process capacity" value={`${activeProcesses}/${Math.max(activeProcesses, 1)}`} />
          {["codex", "claude", "acp"].map((provider) => (
            <MetricCard
              key={provider}
              label={`${provider} auth`}
              value={rows.some((row) => row.provider === provider) ? "available" : "unknown"}
            />
          ))}
        </CardContent>
      </Card>
      <Card>
        <CardHeader>
          <CardTitle>Future source actions</CardTitle>
          <CardDescription>Placeholders only in V0.</CardDescription>
        </CardHeader>
        <CardContent className="flex flex-col gap-2">
          {["add runtime", "provision source", "drain source", "inspect source logs"].map((item) => (
            <Button disabled key={item} variant="outline">{item}</Button>
          ))}
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
                <Textarea value={ticket} onChange={(event) => setTicket(event.target.value)} rows={5} />
              </Field>
              <div className="flex gap-2">
                <Button disabled={!ticket.trim()} onClick={() => connectRealtime(ticket)} type="button">
                  Connect
                </Button>
                <Button onClick={disconnectRealtime} type="button" variant="outline">
                  Disconnect
                </Button>
              </div>
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

function Inspector({
  selectedRow,
  selectedSession,
  selectedTeam,
  selectedApproval,
  selectedProcess,
  selectedWorktree,
  sources,
  staleSourceIds,
  pendingCommandCount
}: {
  readonly selectedRow?: FleetRowView;
  readonly selectedSession?: SessionView;
  readonly selectedTeam?: TeamView;
  readonly selectedApproval?: ApprovalView;
  readonly selectedProcess?: ProcessView;
  readonly selectedWorktree?: WorktreeView;
  readonly sources: readonly SourceHealthView[];
  readonly staleSourceIds: readonly string[];
  readonly pendingCommandCount: number;
}) {
  return (
    <aside className="min-w-0 overflow-hidden bg-muted/20">
      <div className="flex h-14 items-center px-3">
        <div>
          <div className="text-xs uppercase tracking-wide text-muted-foreground">
            Inspector
          </div>
          <div className="text-sm font-medium">Context</div>
        </div>
      </div>
      <Separator />
      <ScrollArea className="h-[calc(100%-3.5rem)]">
        <div className="flex flex-col gap-3 p-3">
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
  scope: "session" | "team" | "process",
  scopeId: string,
  payloadCase: NonNullable<Command["payload"]["case"]>,
  payloadValue: Record<string, unknown>
): Command {
  const command: MessageInitShape<typeof CommandSchema> = {
    commandId: crypto.randomUUID(),
    idempotencyKey: crypto.randomUUID(),
    createdAtClientUnixMs: BigInt(Date.now()),
    target: create(EntityRefSchema, {
      scope:
        scope === "team"
          ? Scope.TEAM
          : scope === "process"
            ? Scope.PROCESS
            : Scope.SESSION,
      scopeId,
      entityId: scopeId
    }),
    payload: {
      case: payloadCase,
      value: payloadValue
    } as Command["payload"]
  };

  return create(CommandSchema, command);
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
