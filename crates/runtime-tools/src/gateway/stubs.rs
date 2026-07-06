use async_trait::async_trait;
use runtime_core::{
    ManagedWorktreeRecord, RuntimeError, TeamBroadcastRequest, TeamCancelMessageRequest,
    TeamCommsService, TeamCreateRequest, TeamDeliveryRecord, TeamGetDeliveriesRequest,
    TeamInterruptAllRequest, TeamInterruptAllResponse, TeamJoinRequest, TeamListMessagesRequest,
    TeamListMessagesResponse, TeamMemberSpawnRequest, TeamMemberSpawnResponse, TeamMessageAck,
    TeamRemoveMemberRequest, TeamRetryDeliveryRequest, TeamSendDirectRequest, TeamSetLeadRequest,
    TeamViewSnapshotRequest, TeamViewSnapshotResponse, TeamWithMembers, WorktreeClaimRequest,
    WorktreeClaimResponse, WorktreeCleanupRequest, WorktreeCleanupResponse, WorktreeCreateRequest,
    WorktreeCreateResponse, WorktreeMemberRemovedRequest, WorktreeMemberRemovedResponse,
    WorktreeReleaseRequest, WorktreeReleaseResponse, WorktreeService,
};

use crate::{TeamCommsConfig, WorktreeServiceConfig};

#[derive(Debug)]
pub struct StubTeamCommsService {
    config: TeamCommsConfig,
}

#[derive(Debug)]
pub struct StubWorktreeService {
    config: WorktreeServiceConfig,
}

impl StubTeamCommsService {
    pub fn new(config: TeamCommsConfig) -> Self {
        Self { config }
    }
}

impl StubWorktreeService {
    pub fn new(config: WorktreeServiceConfig) -> Self {
        Self { config }
    }
}

#[async_trait]
impl TeamCommsService for StubTeamCommsService {
    async fn healthcheck(&self) -> Result<(), RuntimeError> {
        if self.config.enabled {
            return Ok(());
        }
        Err(RuntimeError::Bootstrap(
            "team comms service is disabled".to_string(),
        ))
    }

    async fn create_team(
        &self,
        _request: TeamCreateRequest,
    ) -> Result<TeamWithMembers, RuntimeError> {
        Err(RuntimeError::Unsupported(
            "team comms service is not implemented".to_string(),
        ))
    }

    async fn list_teams(&self) -> Result<Vec<TeamWithMembers>, RuntimeError> {
        Err(RuntimeError::Unsupported(
            "team comms service is not implemented".to_string(),
        ))
    }

    async fn get_team(&self, _team_id: &str) -> Result<TeamWithMembers, RuntimeError> {
        Err(RuntimeError::Unsupported(
            "team comms service is not implemented".to_string(),
        ))
    }

    async fn join_team(&self, _request: TeamJoinRequest) -> Result<TeamWithMembers, RuntimeError> {
        Err(RuntimeError::Unsupported(
            "team comms service is not implemented".to_string(),
        ))
    }

    async fn remove_team_member(
        &self,
        _request: TeamRemoveMemberRequest,
    ) -> Result<TeamWithMembers, RuntimeError> {
        Err(RuntimeError::Unsupported(
            "team comms service is not implemented".to_string(),
        ))
    }

    async fn set_team_lead(
        &self,
        _request: TeamSetLeadRequest,
    ) -> Result<TeamWithMembers, RuntimeError> {
        Err(RuntimeError::Unsupported(
            "team comms service is not implemented".to_string(),
        ))
    }

    async fn delete_team(&self, _team_id: &str) -> Result<(), RuntimeError> {
        Err(RuntimeError::Unsupported(
            "team comms service is not implemented".to_string(),
        ))
    }

    async fn interrupt_all_team_turns(
        &self,
        _request: TeamInterruptAllRequest,
    ) -> Result<TeamInterruptAllResponse, RuntimeError> {
        Err(RuntimeError::Unsupported(
            "team comms service is not implemented".to_string(),
        ))
    }

    async fn send_direct(
        &self,
        _request: TeamSendDirectRequest,
    ) -> Result<TeamMessageAck, RuntimeError> {
        Err(RuntimeError::Unsupported(
            "team comms service is not implemented".to_string(),
        ))
    }

    async fn broadcast(
        &self,
        _request: TeamBroadcastRequest,
    ) -> Result<TeamMessageAck, RuntimeError> {
        Err(RuntimeError::Unsupported(
            "team comms service is not implemented".to_string(),
        ))
    }

    async fn list_messages(
        &self,
        _request: TeamListMessagesRequest,
    ) -> Result<TeamListMessagesResponse, RuntimeError> {
        Err(RuntimeError::Unsupported(
            "team comms service is not implemented".to_string(),
        ))
    }

    async fn get_deliveries(
        &self,
        _request: TeamGetDeliveriesRequest,
    ) -> Result<Vec<TeamDeliveryRecord>, RuntimeError> {
        Err(RuntimeError::Unsupported(
            "team comms service is not implemented".to_string(),
        ))
    }

    async fn retry_delivery(
        &self,
        _request: TeamRetryDeliveryRequest,
    ) -> Result<TeamDeliveryRecord, RuntimeError> {
        Err(RuntimeError::Unsupported(
            "team comms service is not implemented".to_string(),
        ))
    }

    async fn cancel_message(
        &self,
        _request: TeamCancelMessageRequest,
    ) -> Result<Vec<TeamDeliveryRecord>, RuntimeError> {
        Err(RuntimeError::Unsupported(
            "team comms service is not implemented".to_string(),
        ))
    }

    async fn get_view_snapshot(
        &self,
        _request: TeamViewSnapshotRequest,
    ) -> Result<TeamViewSnapshotResponse, RuntimeError> {
        Err(RuntimeError::Unsupported(
            "team comms service is not implemented".to_string(),
        ))
    }

    fn replay_team_events(
        &self,
        _team_id: &str,
        _after_seq: Option<i64>,
        _limit: usize,
    ) -> Result<Vec<runtime_core::RuntimeEventRecord>, RuntimeError> {
        Err(RuntimeError::Unsupported(
            "team comms service is not implemented".to_string(),
        ))
    }
}

#[async_trait]
impl WorktreeService for StubWorktreeService {
    async fn healthcheck(&self) -> Result<(), RuntimeError> {
        let _enabled = self.config.enabled;
        Ok(())
    }

    async fn list_worktrees(&self) -> Result<Vec<ManagedWorktreeRecord>, RuntimeError> {
        Err(RuntimeError::Unsupported(
            "worktree service is not implemented".to_string(),
        ))
    }

    async fn get_worktree(
        &self,
        _worktree_id: &str,
    ) -> Result<ManagedWorktreeRecord, RuntimeError> {
        Err(RuntimeError::Unsupported(
            "worktree service is not implemented".to_string(),
        ))
    }

    async fn create_worktree(
        &self,
        _request: WorktreeCreateRequest,
    ) -> Result<WorktreeCreateResponse, RuntimeError> {
        Err(RuntimeError::Unsupported(
            "worktree service is not implemented".to_string(),
        ))
    }

    async fn claim_worktree(
        &self,
        _request: WorktreeClaimRequest,
    ) -> Result<WorktreeClaimResponse, RuntimeError> {
        Err(RuntimeError::Unsupported(
            "worktree service is not implemented".to_string(),
        ))
    }

    async fn release_worktree(
        &self,
        _request: WorktreeReleaseRequest,
    ) -> Result<WorktreeReleaseResponse, RuntimeError> {
        Err(RuntimeError::Unsupported(
            "worktree service is not implemented".to_string(),
        ))
    }

    async fn cleanup_worktree(
        &self,
        _request: WorktreeCleanupRequest,
    ) -> Result<WorktreeCleanupResponse, RuntimeError> {
        Err(RuntimeError::Unsupported(
            "worktree service is not implemented".to_string(),
        ))
    }

    async fn spawn_team_member(
        &self,
        _request: TeamMemberSpawnRequest,
    ) -> Result<TeamMemberSpawnResponse, RuntimeError> {
        Err(RuntimeError::Unsupported(
            "worktree service is not implemented".to_string(),
        ))
    }

    async fn on_member_removed(
        &self,
        _request: WorktreeMemberRemovedRequest,
    ) -> Result<WorktreeMemberRemovedResponse, RuntimeError> {
        Err(RuntimeError::Unsupported(
            "worktree service is not implemented".to_string(),
        ))
    }
}
