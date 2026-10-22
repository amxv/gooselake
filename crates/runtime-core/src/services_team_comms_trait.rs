use async_trait::async_trait;

use crate::{
    AgentBroadcastMessageRequest, AgentCancelMessageRequest, AgentDeliveryListRequest,
    AgentDeliveryRecord, AgentDirectMessageRequest, AgentMessageAck, AgentMessageListRequest,
    AgentMessageListResponse, AgentRetryDeliveryRequest, RuntimeError, RuntimeEventRecord,
    TeamBroadcastRequest, TeamCancelMessageRequest, TeamCreateRequest, TeamDeliveryRecord,
    TeamGetDeliveriesRequest, TeamInterruptAllRequest, TeamInterruptAllResponse, TeamJoinRequest,
    TeamListMessagesRequest, TeamListMessagesResponse, TeamMessageAck, TeamRemoveMemberRequest,
    TeamRetryDeliveryRequest, TeamSendDirectRequest, TeamSetLeadRequest, TeamViewSnapshotRequest,
    TeamViewSnapshotResponse, TeamWithMembers,
};

#[async_trait]
pub trait TeamCommsService: Send + Sync {
    async fn healthcheck(&self) -> Result<(), RuntimeError>;

    async fn create_team(
        &self,
        request: TeamCreateRequest,
    ) -> Result<TeamWithMembers, RuntimeError>;

    async fn list_teams(&self) -> Result<Vec<TeamWithMembers>, RuntimeError>;

    async fn get_team(&self, team_id: &str) -> Result<TeamWithMembers, RuntimeError>;

    async fn join_team(&self, request: TeamJoinRequest) -> Result<TeamWithMembers, RuntimeError>;

    async fn remove_team_member(
        &self,
        request: TeamRemoveMemberRequest,
    ) -> Result<TeamWithMembers, RuntimeError>;

    async fn set_team_lead(
        &self,
        request: TeamSetLeadRequest,
    ) -> Result<TeamWithMembers, RuntimeError>;

    async fn delete_team(&self, team_id: &str) -> Result<(), RuntimeError>;

    async fn interrupt_all_team_turns(
        &self,
        request: TeamInterruptAllRequest,
    ) -> Result<TeamInterruptAllResponse, RuntimeError>;

    async fn send_direct(
        &self,
        request: TeamSendDirectRequest,
    ) -> Result<TeamMessageAck, RuntimeError>;

    async fn broadcast(
        &self,
        request: TeamBroadcastRequest,
    ) -> Result<TeamMessageAck, RuntimeError>;

    async fn send_agent_direct(
        &self,
        _request: AgentDirectMessageRequest,
    ) -> Result<AgentMessageAck, RuntimeError> {
        Err(RuntimeError::Unsupported(
            "agent-first direct messaging is not implemented by this comms service".to_string(),
        ))
    }

    async fn broadcast_workspace(
        &self,
        _request: AgentBroadcastMessageRequest,
    ) -> Result<AgentMessageAck, RuntimeError> {
        Err(RuntimeError::Unsupported(
            "agent-first broadcast messaging is not implemented by this comms service".to_string(),
        ))
    }

    async fn list_agent_messages(
        &self,
        _request: AgentMessageListRequest,
    ) -> Result<AgentMessageListResponse, RuntimeError> {
        Err(RuntimeError::Unsupported(
            "agent-first message listing is not implemented by this comms service".to_string(),
        ))
    }

    async fn get_agent_deliveries(
        &self,
        _request: AgentDeliveryListRequest,
    ) -> Result<Vec<AgentDeliveryRecord>, RuntimeError> {
        Err(RuntimeError::Unsupported(
            "agent-first delivery listing is not implemented by this comms service".to_string(),
        ))
    }

    async fn retry_agent_delivery(
        &self,
        _request: AgentRetryDeliveryRequest,
    ) -> Result<AgentDeliveryRecord, RuntimeError> {
        Err(RuntimeError::Unsupported(
            "agent-first delivery retry is not implemented by this comms service".to_string(),
        ))
    }

    async fn cancel_agent_message(
        &self,
        _request: AgentCancelMessageRequest,
    ) -> Result<Vec<AgentDeliveryRecord>, RuntimeError> {
        Err(RuntimeError::Unsupported(
            "agent-first message cancellation is not implemented by this comms service".to_string(),
        ))
    }

    async fn list_messages(
        &self,
        request: TeamListMessagesRequest,
    ) -> Result<TeamListMessagesResponse, RuntimeError>;

    async fn get_deliveries(
        &self,
        request: TeamGetDeliveriesRequest,
    ) -> Result<Vec<TeamDeliveryRecord>, RuntimeError>;

    async fn retry_delivery(
        &self,
        request: TeamRetryDeliveryRequest,
    ) -> Result<TeamDeliveryRecord, RuntimeError>;

    async fn cancel_message(
        &self,
        request: TeamCancelMessageRequest,
    ) -> Result<Vec<TeamDeliveryRecord>, RuntimeError>;

    async fn get_view_snapshot(
        &self,
        request: TeamViewSnapshotRequest,
    ) -> Result<TeamViewSnapshotResponse, RuntimeError>;

    fn replay_team_events(
        &self,
        team_id: &str,
        after_seq: Option<i64>,
        limit: usize,
    ) -> Result<Vec<RuntimeEventRecord>, RuntimeError>;
}
