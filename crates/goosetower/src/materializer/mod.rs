pub mod bootstrap;
pub mod reducers;
pub mod snapshots;
pub mod state;

pub use bootstrap::{BootstrapError, BootstrapOptions, SourceBootstrap};
pub use reducers::{CoalescingPatchBuffer, PatchEffect};
pub use snapshots::{
    ApprovalInboxSubscription, BoardSubscription, LedgerSubscription, ProcessTailSubscription,
    SelectedSessionSubscription, SelectedTeamSubscription,
};
pub use state::{
    AgentRowView, ApprovalInboxView, EntityKey, EntityVersion, FleetBoardView, LedgerEventView,
    LedgerView, MaterializedPatch, MaterializedPatchKind, MaterializedState, MaterializerStatus,
    ProcessTailView, SessionDetailView, SourceHealthView, TeamWorkspaceView, WorktreeView,
};

#[cfg(test)]
mod tests;
