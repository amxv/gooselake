use runtime_core::{
    RuntimeError, WorktreeCleanupRequest, WorktreeMemberRemovedRequest,
    WorktreeMemberRemovedResponse, WorktreeService,
};

use super::RuntimeWorktreeService;
use crate::now_ms;

fn removal_diagnostic_failure(original: RuntimeError, diagnostic: RuntimeError) -> RuntimeError {
    if original.legacy_native_recovery().is_some() {
        eprintln!("member removal retained its original legacy refusal; diagnostic persistence also failed: {diagnostic}");
        original
    } else {
        diagnostic
    }
}

impl RuntimeWorktreeService {
    pub(super) async fn on_member_removed_impl(
        &self,
        request: WorktreeMemberRemovedRequest,
    ) -> Result<WorktreeMemberRemovedResponse, RuntimeError> {
        self.ensure_enabled()?;
        let mut released_claims = Vec::new();
        let mut cleanup_results = Vec::new();
        let mut diagnostics = Vec::new();
        let active_claims = self
            .store
            .list_active_managed_worktree_claims_for_session(&request.agent_id)?;
        for claim in active_claims {
            let released = runtime_core::ManagedWorktreeClaimRecord {
                released_at: Some(now_ms()),
                ..claim.clone()
            };
            let released = match self.store.transition_managed_worktree_claim(
                &released,
                runtime_core::ManagedWorktreeClaimTransition::Release,
            ) {
                Ok(released) => released,
                Err(error) => {
                    let legacy_recovery = error.legacy_native_recovery();
                    let diag = self.store.append_team_operation_diagnostic(
                        None,
                        Some(request.team_id.as_str()),
                        "worktree_claim_release_failed",
                        error.to_string().as_str(),
                        &serde_json::json!({
                            "worktree_id": claim.worktree_id,
                            "session_id": claim.session_id,
                            "removal_team_id": request.team_id,
                            "legacy_recovery": legacy_recovery.as_ref().map(|value| value.error_details())
                        }),
                        now_ms(),
                    ).map_err(|diagnostic| removal_diagnostic_failure(error, diagnostic))?;
                    diagnostics.push(diag);
                    continue;
                }
            };
            released_claims.push(released.clone());
            match self
                .cleanup_worktree(WorktreeCleanupRequest {
                    worktree_id: released.worktree_id.clone(),
                    reason: Some("team_member_removed".to_string()),
                })
                .await
            {
                Ok(result) => cleanup_results.push(result),
                Err(error) => {
                    let legacy_recovery = error.legacy_native_recovery();
                    let diag = self.store.append_team_operation_diagnostic(
                        None,
                        Some(request.team_id.as_str()),
                        "worktree_cleanup_failed_on_member_remove",
                        error.to_string().as_str(),
                        &serde_json::json!({
                            "worktree_id": released.worktree_id,
                            "session_id": released.session_id,
                            "removal_team_id": request.team_id,
                            "legacy_recovery": legacy_recovery.as_ref().map(|value| value.error_details())
                        }),
                        now_ms(),
                    ).map_err(|diagnostic| removal_diagnostic_failure(error, diagnostic))?;
                    diagnostics.push(diag);
                }
            }
        }
        Ok(WorktreeMemberRemovedResponse {
            released_claims,
            cleanup_results,
            diagnostics,
        })
    }
}
