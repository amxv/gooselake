# Gooseweb UX Browser Audit - 2026-07-07 - open_leader

Browser session:

- `agent-browser --session gooseweb-ux-open-leader-headless`
- Headless, per user correction.
- App URL: `http://127.0.0.1:13001/`
- Goosetower: `http://127.0.0.1:18090`
- Runtime: `http://127.0.0.1:18080`

Evidence directory:

- `tmp/gooseweb-ux-browser-lead-headless/`

Reference context read:

- `README.md`
- `gg/goosetower-gooseweb-implementation-plan-2026-07-06.md`
- `tmp/gooseweb-browser-testing-handoff-2026-07-07.md`
- `src/content/docs/architecture.md`
- `src/content/docs/teams.md`
- `src/content/docs/client-design.md`

## Environment Notes

- Goosetower health is OK.
- Goosetower materializer reports source `local` as `live`, with 2 sessions, 2 teams, 0 approvals, 0 processes, 0 worktrees, and 0 discontinuities.
- The first clean headless browser restart landed on a stale SSR page because the Vite dev server had died. Restarted Gooseweb as managed process `proc_14`.
- After restart, Gooseweb connected and rendered 2 board rows. The shell briefly reported `degraded`, then switched to `connected` after navigating.

## Issue 1 - Add-Agent Flow Is Not Modal-First And Creation Controls Remain Embedded

Severity: High

Evidence:

- `tmp/gooseweb-ux-browser-lead-headless/01-teams-initial.png`
- `tmp/gooseweb-ux-browser-lead-headless/02-add-agent-modal.png`
- `tmp/gooseweb-ux-browser-lead-headless/03-agents-initial.png`

Steps:

1. Open Gooseweb.
2. Navigate to Teams.
3. Observe main Teams pane.
4. Click left-sidebar `Add Agent to Team`.
5. Navigate to Agents.
6. Observe main Agents pane.

Actual:

- Teams main pane includes embedded creation/control chrome: `Spawn`, `Source`, `Team name`, `Lead agent`, `Create team`, `Existing agent`, and `Join selected agent`.
- Clicking `Add Agent to Team` did not open a modal in this run; screenshot `02-add-agent-modal.png` shows the same Teams embedded controls.
- Agents main pane includes embedded create-agent form controls: `Source`, `Title`, `Provider`, `Model`, `Working directory`, `Create agent`.
- These controls occupy prime operator space even when the operator is trying to inspect or control existing teams/agents.

Expected:

- The left-sidebar `Add Agent to Team` button should launch a focused modal.
- The modal should contain the add/spawn/join controls and should be contextual to the selected team when possible.
- Primary Teams and Agents panels should focus on live operation surfaces: team/member state, message/delivery stream, selected agent thread, status, approvals, and composer.
- Creation controls should not be embedded in the middle of the primary Teams or Agents panel.

Architecture/product rationale:

- The Goosetower plan calls for desktop-class multi-agent operating workflows and shadcn overlays/forms.
- The user explicitly requested the add-agent flow as a left-sidebar modal, not an embedded middle-panel creation form.

## Issue 2 - Teams Does Not Yet Feel Like A Chronological Chat/Control Stream

Severity: High

Evidence:

- `tmp/gooseweb-ux-browser-lead-headless/01-teams-initial.png`

Steps:

1. Navigate to Teams.
2. Select `Headless Mission Team`.
3. Inspect Team events, delivery state, and message controls.

Actual:

- `Delivery state` shows `0 items` and `No entries`.
- `Team events` shows 2 items that are member summaries (`sess_codex_... ready / codex`) rather than a rich chronological stream.
- There is no dense chat-like transcript showing user messages, assistant messages, tool calls, tool results, delivery transitions, and in-between control events.
- Direct/broadcast controls exist, but the surrounding surface still reads more like a sparse admin panel than a team control stream.

Expected:

- Teams should show a chronological, chat-like control stream similar in spirit to gg-desktop.
- Messages and delivery state should both be visible because runtime docs explicitly separate message records from delivery records.
- The stream should support operator scanning: sender/recipient, message text, delivery state, injected/deferred/failed transitions, tool-call/control events, and timestamps.

## Issue 3 - Selected Agent Thread Still Starts With Placeholder/Explanatory Copy

Severity: Medium

Evidence:

- `tmp/gooseweb-ux-browser-lead-headless/04-agent-selected.png`

Steps:

1. Navigate to Agents.
2. Click left-rail session `sess_codex_1783414749860_2`.
3. Inspect main agent thread.

Actual:

- The selected agent thread begins with large explanatory text:
  - "The selected agent thread is..."
  - "Gooseweb is keeping the realtime worker..."
  - "The active session is..."
- It includes readout-like blocks such as "Read 2 board rows", "Read 1 team snapshot", "Read 0 process records", and "Read 1 runtime source".
- These statements are implementation narration rather than operator content.

Expected:

- Once an agent is selected, the thread should prioritize the actual conversation/control stream and compact status metadata.
- Explanatory copy should disappear from primary operator surfaces unless it is an actionable empty state.

## Issue 4 - Source Health Reads Unknown Despite Live Materializer

Severity: Medium

Evidence:

- `tmp/gooseweb-ux-browser-lead-headless/00-board-restarted-live.png`
- `tmp/gooseweb-ux-browser-lead-headless/06-fleet.png`

Steps:

1. Confirm Goosetower materializer reports source `local` as `live`.
2. Open Board and Fleet.
3. Inspect Source health panels.

Actual:

- Board/Fleet show source `local` as `unknown / unknown` or `health unknown`.
- Goosetower debug materializer reports `state:"live"`.

Expected:

- Gooseweb should surface the live Goosetower source state consistently.
- If gateway source health is incomplete, the UI should distinguish "not reported" from actual unknown/offline state.

## Initial Fix Recommendation

Start with Issue 1 as the first focused fixer loop:

- It directly matches the user's explicit instruction.
- It is concrete and acceptance-testable in the browser.
- It clears primary Teams/Agents workspace area for subsequent stream/thread improvements.

Acceptance for Issue 1:

- Clicking the left-sidebar `Add Agent to Team` opens a modal.
- The modal contains add/spawn/join controls for the selected team/source.
- Teams main pane no longer embeds team creation/add-agent controls in the center of the operator workspace.
- Agents main pane no longer embeds create-agent controls before the selected thread/empty state.
- Existing command wiring remains intact or degrades clearly if a required selection is missing.
- Narrow relevant checks pass.

## Accepted Fix 1

Fixer: `incomplete_sapphire`

Commit:

- `b3fc9c3 Move Gooseweb add-agent controls into dialogs`

Browser acceptance evidence:

- `tmp/gooseweb-ux-browser-lead-headless/08-b3fc9c3-add-agent-dialog.png`
- `tmp/gooseweb-ux-browser-lead-headless/09-b3fc9c3-teams-clean.png`
- `tmp/gooseweb-ux-browser-lead-headless/10-b3fc9c3-agents-clean.png`
- `tmp/gooseweb-ux-browser-lead-headless/11-b3fc9c3-new-agent-dialog.png`

Accepted:

- Sidebar `Add Agent to Team` opens the add-agent dialog and switches context to Teams.
- Dialog contains create-team, join existing agent, and spawn teammate controls.
- Teams main pane no longer embeds create-team/join/spawn controls in the center panel.
- Agents main pane no longer embeds create-agent controls before the thread/empty state.
- New agent controls remain available through the `New agent` dialog.

Follow-up issues still open:

- Teams still needs a chronological chat/control stream.
- Selected agent thread still starts with explanatory placeholder copy.
- Source health still reads unknown despite live materializer state.

## Accepted Fix 2

Fixer: `extensive_hamster`

Commit:

- `0178107 Make Gooseweb teams a chronological stream`

Browser acceptance evidence:

- `tmp/gooseweb-ux-browser-lead-headless/12-0178107-teams-stream.png`
- `tmp/gooseweb-ux-browser-lead-headless/14-0178107-teams-dom-broadcast.png`

Accepted:

- Teams now has a primary `Team stream` body.
- The stream renders chat rows separately from delivery rows.
- Chat rows show direct/broadcast type, sender, recipients, message ID, timestamps, and text when present.
- Delivery rows show recipient, status, provider, message ID, turn ID, timestamps, and retry/cancel controls.
- Member summaries are separated into `Team roster` and are no longer presented as the chat stream.

Follow-up issues still open:

- Selected agent thread still starts with explanatory placeholder copy.
- Source health still sometimes reads unknown despite live materializer state.

## Accepted Fix 3

Fixer: `puzzle_mammal`

Commits:

- `f14c0e9 Remove selected agent thread narration`
- `ceb83dd Tighten selected agent thread copy`

Browser acceptance evidence:

- `tmp/gooseweb-ux-browser-lead-headless/16-f14c0e9-agent-thread-clean.png`
- `tmp/gooseweb-ux-browser-lead-headless/17-ceb83dd-agent-thread-copy.png`

Accepted:

- The selected agent thread no longer renders the large WorklogNarrative/read-count implementation block.
- Header now uses `Agent thread / <status>` instead of a misleading hardcoded `Thinking` label.
- Header title, selected session status, and composer target now match.
- Implementation narration such as realtime worker framing and controls-placement copy was removed.
- The thread body prioritizes session facts, conversation stream, approval context, and the anchored composer.

Follow-up issue still open:

- Source health should be retested; earlier evidence showed `unknown / unknown` despite a live materializer.

## Accepted Fix 4

Fixer: `seen_sunset`

Commit:

- `91b48c4 Fix Gooseweb source health fallback`

Browser acceptance evidence:

- `tmp/gooseweb-ux-browser-lead-headless/20-91b48c4-board-source-health.png`
- `tmp/gooseweb-ux-browser-lead-headless/21-91b48c4-fleet-source-health.png`
- `tmp/gooseweb-ux-browser-lead-headless/22-91b48c4-ledger-source-health.png`

Accepted:

- Board/process rail source health now renders the connected local source as `live / <age>` instead of `unknown / unknown`.
- Fleet summary shows `health live` and a concrete stale age.
- Fleet source operation card shows the local runtime display name and `live` status.
- Ledger source event row now renders `source.live` instead of `source.unknown`.

## Final State

Accepted code commits on `main`:

- `b3fc9c3 Move Gooseweb add-agent controls into dialogs`
- `0178107 Make Gooseweb teams a chronological stream`
- `f14c0e9 Remove selected agent thread narration`
- `ceb83dd Tighten selected agent thread copy`
- `91b48c4 Fix Gooseweb source health fallback`

Final browser evidence directory:

- `tmp/gooseweb-ux-browser-lead-headless/`

The main operator surfaces are materially closer to the requested product direction:

- Add-agent and create-agent controls live behind dialogs instead of occupying the primary workspaces.
- Teams has a chronological chat/control stream with delivery metadata and a separate roster.
- Selected agent threads open directly into session facts, conversation state, approval context, and composer.
- Source health is consistently rendered as live for the connected local runtime.
