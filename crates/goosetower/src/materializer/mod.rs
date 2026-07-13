pub mod bootstrap;
pub mod reducers;
pub mod snapshots;
pub mod state;

pub use bootstrap::{BootstrapError, BootstrapOptions, SourceBootstrap};
pub use reducers::{CoalescingPatchBuffer, PatchEffect};
pub use snapshots::{
    snapshot_cross_source_approval_inbox, snapshot_cross_source_board,
    snapshot_cross_source_health, snapshot_cross_source_ledger, snapshot_cross_source_teams,
    snapshot_cross_source_worktrees, ApprovalInboxSubscription, BoardSubscription,
    LedgerSubscription, ProcessTailSubscription, SelectedSessionSubscription,
    SelectedTeamSubscription, SourceReplacementView, TeamSummarySubscription,
    MAX_TEAM_DELIVERY_LIMIT, MAX_TEAM_MESSAGE_LIMIT, MAX_TEAM_SUMMARY_LIMIT,
};
pub use state::{
    AgentRowView, ApprovalInboxView, EntityKey, EntityVersion, FleetBoardView, LedgerEventView,
    LedgerView, MaterializedPatch, MaterializedPatchKind, MaterializedState, MaterializerStatus,
    ProcessTailView, SessionDetailView, SourceHealthView, SourceOwnershipIndexes,
    TeamSummaryListView, TeamSummaryView, TeamWorkspaceView, WorktreeView,
};

#[cfg(test)]
mod tests;
