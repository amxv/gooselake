import assert from "node:assert/strict";
import { create } from "@bufbuild/protobuf";
import {
  FleetRowViewSchema,
  SessionViewSchema,
  TeamMemberViewSchema,
  TeamViewSchema
} from "../src/gen/goosetower/v1/view_pb";
import {
  buildStopAgentMemberships,
  rosterTeamGroupId,
  rosterTeamKey,
  stopAgentSourceRoute,
  teamKeyForSession
} from "../app/realtime/roster-membership";

const sessions = ["A", "B"].map((sourceId) => create(SessionViewSchema, {
  sourceId,
  sessionId: "session-1",
  activeTurnId: `turn-${sourceId}`,
  status: "running"
}));
const rows = ["A", "B"].map((sourceId) => create(FleetRowViewSchema, {
  sourceId,
  rowId: "row-1",
  sessionId: "session-1",
  teamId: "team-1",
  status: "running"
}));
const summaryOnlyTeams = ["A", "B"].map((sourceId) => create(TeamViewSchema, {
  sourceId,
  teamId: "team-1",
  name: `${sourceId} team`,
  leadMemberId: `lead-${sourceId}`,
  members: []
}));

const targets = buildStopAgentMemberships(sessions, rows, summaryOnlyTeams);
assert.equal(targets.length, 2);
assert.deepEqual(targets.map((target) => target.teamKey), [
  rosterTeamKey("A", "team-1"),
  rosterTeamKey("B", "team-1")
]);
const selectedB = targets.filter((target) => target.teamKey === rosterTeamKey("B", "team-1"));
assert.deepEqual(selectedB, [{
  sourceId: "B",
  sessionId: "session-1",
  turnId: "turn-B",
  teamKey: rosterTeamKey("B", "team-1")
}]);
assert.equal(stopAgentSourceRoute(selectedB[0]!.sourceId), "source:B");
assert.equal(selectedB[0]!.sessionId, "session-1");
assert.equal(selectedB[0]!.turnId, "turn-B");
assert.notEqual(rosterTeamGroupId("A", "team-1"), rosterTeamGroupId("B", "team-1"));

const enrichedB = create(TeamViewSchema, {
  sourceId: "B",
  teamId: "team-2",
  name: "B enriched",
  members: [create(TeamMemberViewSchema, { memberId: "member-B", sessionId: "session-1" })]
});
assert.equal(
  teamKeyForSession("B", "session-1", undefined, [summaryOnlyTeams[0]!, enrichedB]),
  rosterTeamKey("B", "team-2"),
  "selected workspace/team membership enriches only the matching source"
);
assert.equal(
  teamKeyForSession("A", "session-1", undefined, [enrichedB]),
  "",
  "same-ID sibling sessions must not inherit another source's enriched membership"
);

console.log("source-qualified roster membership smoke fixture passed");
