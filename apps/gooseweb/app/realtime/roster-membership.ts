import type {
  FleetRowView,
  SessionView,
  TeamView
} from "../../src/gen/goosetower/v1/view_pb";
import { sourceEntityKey } from "./protocol/entities";

export type StopAgentMembership = {
  readonly sourceId: string;
  readonly sessionId: string;
  readonly turnId: string;
  readonly teamKey: string;
};

export function rosterSessionKey(sourceId: string, sessionId: string): string {
  return sourceEntityKey(sourceId, sessionId);
}

export function rosterTeamKey(sourceId: string, teamId: string): string {
  return sourceEntityKey(sourceId, teamId);
}

export function rosterTeamGroupId(sourceId: string, teamId: string): string {
  return `team:${rosterTeamKey(sourceId, teamId)}`;
}

export function stopAgentSourceRoute(sourceId: string): string {
  return `source:${sourceId}`;
}

export function fleetRowForSession(
  rows: readonly FleetRowView[],
  sourceId: string,
  sessionId: string
): FleetRowView | undefined {
  return rows.find((row) => row.sourceId === sourceId && row.sessionId === sessionId);
}

export function teamKeyForSession(
  sourceId: string,
  sessionId: string,
  row: FleetRowView | undefined,
  teams: readonly TeamView[]
): string {
  if (row?.sourceId === sourceId && row.teamId) {
    return rosterTeamKey(sourceId, row.teamId);
  }
  const enrichedTeam = teams.find((team) =>
    team.sourceId === sourceId &&
    team.members.some((member) => member.sessionId === sessionId)
  );
  return enrichedTeam?.teamId ? rosterTeamKey(sourceId, enrichedTeam.teamId) : "";
}

export function buildStopAgentMemberships(
  sessions: readonly SessionView[],
  rows: readonly FleetRowView[],
  teams: readonly TeamView[]
): readonly StopAgentMembership[] {
  return sessions.flatMap((session) => {
    if (!session.sourceId || !session.sessionId || !session.activeTurnId) return [];
    const row = fleetRowForSession(rows, session.sourceId, session.sessionId);
    return [{
      sourceId: session.sourceId,
      sessionId: session.sessionId,
      turnId: session.activeTurnId,
      teamKey: teamKeyForSession(session.sourceId, session.sessionId, row, teams)
    }];
  });
}
