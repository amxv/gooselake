use async_trait::async_trait;

macro_rules! legacy_team_event_methods {
    () => {
        /// /v1 team roster authority and its critical event must be committed
        /// in the same durable transaction. There is no fallback to separate
        /// upserts or silently discarded event journal errors.
        fn commit_legacy_team_mutation(
            &self,
            _mutation: &crate::LegacyTeamMutation,
        ) -> Result<crate::RuntimeEventRecord, crate::RuntimeError> {
            Err(crate::RuntimeError::Unsupported(
                "atomic legacy team roster event journal unavailable".into(),
            ))
        }

        /// Legacy `/v1` message and all initial deliveries must be admitted
        /// together with one critical message event and a critical pending
        /// event per delivery, before any recipient injection/native effects.
        /// Idempotent replay is a durable receipt, never an implicit resend.
        fn commit_legacy_team_message_admission(
            &self,
            _message: &crate::TeamMessageRecord,
            _deliveries: &[crate::TeamDeliveryRecord],
            _events: &[crate::NewRuntimeEvent],
        ) -> Result<crate::TeamMessageAck, crate::RuntimeError> {
            Err(crate::RuntimeError::Unsupported(
                "atomic legacy team message admission journal unavailable".into(),
            ))
        }

        /// Pre-native cancellation of *all* pending/deferred legacy message
        /// deliveries and its critical events must commit as one durable
        /// operation. Never publish a cancelled cache state before this
        /// receipt, or cancel a delivery that was already native-injecting.
        fn commit_legacy_team_message_cancellation(
            &self,
            _message: &crate::TeamMessageRecord,
            _before: &[crate::TeamDeliveryRecord],
            _after: &[crate::TeamDeliveryRecord],
            _events: &[crate::NewRuntimeEvent],
        ) -> Result<Vec<crate::TeamDeliveryRecord>, crate::RuntimeError> {
            Err(crate::RuntimeError::Unsupported(
                "atomic legacy team cancellation event journal unavailable".into(),
            ))
        }

        /// Read and validate the *previously committed* terminal receipt,
        /// including all cancelled delivery events. A transport retry after
        /// restart is not a new native operation; missing/forged receipt
        /// must fail closed rather than certify an unjournaled cancellation.
        fn get_legacy_team_message_cancellation_receipt(
            &self,
            _message: &crate::TeamMessageRecord,
        ) -> Result<Option<Vec<crate::TeamDeliveryRecord>>, crate::RuntimeError> {
            Err(crate::RuntimeError::Unsupported(
                "legacy team cancellation receipt verification unavailable".into(),
            ))
        }
    };
}
pub(super) use legacy_team_event_methods;

use crate::{
    AgentBroadcastMessageRequest, AgentCancelMessageMutationCommand,
    AgentCancelMessageMutationResponse, AgentCancelMessageRequest, AgentDeliveryListRequest,
    AgentDeliveryRecord, AgentDirectMessageRequest, AgentMessageAck, AgentMessageListRequest,
    AgentMessageListResponse, AgentRetryDeliveryMutationCommand,
    AgentRetryDeliveryMutationResponse, AgentRetryDeliveryRequest, RuntimeError,
    RuntimeEventRecord, TeamBroadcastRequest, TeamCancelMessageRequest, TeamCreateRequest,
    TeamDeliveryRecord, TeamGetDeliveriesRequest, TeamInterruptAllRequest,
    TeamInterruptAllResponse, TeamJoinRequest, TeamListMessagesRequest, TeamListMessagesResponse,
    TeamMessageAck, TeamRemoveMemberRequest, TeamRetryDeliveryRequest, TeamSendDirectRequest,
    TeamSetLeadRequest, TeamViewSnapshotRequest, TeamViewSnapshotResponse, TeamWithMembers,
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

    /// Bounded delivery inspection for operator HTTP. Legacy implementations
    /// can use the default sorted-list fallback, while persistent stores may
    /// override it with an indexed keyset query.
    async fn get_agent_deliveries_page(
        &self,
        request: AgentDeliveryListRequest,
        cursor: Option<(i64, String)>,
        limit: usize,
    ) -> Result<Vec<AgentDeliveryRecord>, RuntimeError> {
        Ok(self
            .get_agent_deliveries(request)
            .await?
            .into_iter()
            .filter(|row| {
                cursor
                    .as_ref()
                    .is_none_or(|(at, id)| (row.created_at, row.id.as_str()) > (*at, id.as_str()))
            })
            .take(limit)
            .collect())
    }

    async fn retry_agent_delivery(
        &self,
        _request: AgentRetryDeliveryRequest,
    ) -> Result<AgentDeliveryRecord, RuntimeError> {
        Err(RuntimeError::Unsupported(
            "agent-first delivery retry is not implemented by this comms service".to_string(),
        ))
    }

    async fn retry_agent_delivery_operator(
        &self,
        _command: AgentRetryDeliveryMutationCommand,
    ) -> Result<AgentRetryDeliveryMutationResponse, RuntimeError> {
        Err(RuntimeError::Unsupported(
            "durable operator delivery retry admission unavailable".into(),
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

    async fn cancel_agent_message_operator(
        &self,
        _command: AgentCancelMessageMutationCommand,
    ) -> Result<AgentCancelMessageMutationResponse, RuntimeError> {
        Err(RuntimeError::Unsupported(
            "durable operator message cancellation unavailable".into(),
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
