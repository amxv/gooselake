use runtime_core::{
    RuntimeError, TeamMemberSpawnAdmission, TeamMemberSpawnRequest, TeamMemberSpawnResponse,
};

use super::RuntimeWorktreeService;

impl RuntimeWorktreeService {
    pub(super) fn inspect_legacy_spawn_receipt(
        &self,
        request: &TeamMemberSpawnRequest,
    ) -> Result<TeamMemberSpawnResponse, RuntimeError> {
        let team_id = request.team_id.trim();
        if team_id.is_empty() {
            return Err(RuntimeError::InvalidState("team_id is required".into()));
        }
        let unsupported = || {
            RuntimeError::Unsupported(
            "fresh legacy team spawn is unavailable; inspect an original keyed receipt or use workspace-authoritative agent creation".into(),
        )
        };
        let Some(key) = request.idempotency_key.as_deref() else {
            return Err(unsupported());
        };
        if key.is_empty()
            || key.len() > 512
            || key.trim() != key
            || key.chars().any(char::is_control)
        {
            return Err(RuntimeError::InvalidState(
                "spawn idempotency key must be 1-512 printable characters without surrounding whitespace".into(),
            ));
        }
        let hash = runtime_core::normalized_json_hash(&serde_json::to_value(request).map_err(
            |error| RuntimeError::InvalidState(format!("invalid spawn request: {error}")),
        )?)?;
        // Historical authority precedes all mutable feature/session/roster,
        // filesystem and provider checks. A missing record cannot execute.
        match self.store.replay_team_member_spawn(
            team_id, &request.source_session_id, key, &hash,
        )? {
            Some(TeamMemberSpawnAdmission::Replay { response }) => Ok(response),
            Some(TeamMemberSpawnAdmission::Unresolved { operation_id }) => {
                Err(RuntimeError::operation_unresolved(
                    operation_id.clone(),
                    format!("original legacy spawn is not certified complete; inspect /v1/diagnostics/team-operations/{operation_id}; native retry is not authorized"),
                ))
            }
            Some(TeamMemberSpawnAdmission::Execute { .. }) => Err(RuntimeError::Conflict(
                "historical spawn inspection cannot grant native execution".into(),
            )),
            None => Err(unsupported()),
        }
    }

    pub(super) async fn spawn_team_member_guarded(
        &self,
        request: TeamMemberSpawnRequest,
    ) -> Result<TeamMemberSpawnResponse, RuntimeError> {
        let original_key = request.idempotency_key.clone();
        let team_id = request.team_id.trim().to_string();
        let source_session_id = request.source_session_id.clone();
        let normalized_hash = if original_key.is_some() {
            Some(runtime_core::normalized_json_hash(
                &serde_json::to_value(&request).map_err(|error| {
                    RuntimeError::InvalidState(format!("invalid team spawn request: {error}"))
                })?,
            )?)
        } else {
            None
        };
        match self.spawn_team_member_impl(request).await {
            Ok(response) => Ok(response),
            Err(error @ RuntimeError::OperationUnresolved { .. }) => Err(error),
            Err(error) => {
                // Even a fully confirmed rollback consumed the keyed native
                // admission. Return its *original* inspection coordinate on
                // the first error; never imply that a fresh replay is safe.
                // Pure preflight failures have no admitted row, so preserve
                // their original error types.
                if let (Some(key), Some(hash)) = (original_key, normalized_hash) {
                    if let Some(TeamMemberSpawnAdmission::Unresolved { operation_id }) = self
                        .store
                        .replay_team_member_spawn(&team_id, &source_session_id, &key, &hash)?
                    {
                        return Err(RuntimeError::operation_unresolved(
                            operation_id,
                            format!("keyed spawn has a durable native admission; original error: {error}"),
                        ));
                    }
                }
                Err(error)
            }
        }
    }
}
