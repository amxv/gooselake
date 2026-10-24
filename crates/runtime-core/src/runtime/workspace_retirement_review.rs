use std::collections::HashSet;

use super::RuntimeSessionManager;
use crate::{RuntimeError, WorkspaceRetirementReviewCommand, WorkspaceRetirementReviewOutcome};

impl RuntimeSessionManager {
    pub async fn reconcile_workspace_retirement(
        &self,
        command: &WorkspaceRetirementReviewCommand,
    ) -> Result<WorkspaceRetirementReviewOutcome, RuntimeError> {
        let _mutation = self.session_policy_mutation_lock.lock().await;
        // Native completion callbacks take these locks in this order. Keep
        // their projection update outside the receipt-to-hydration interval.
        let mut turns = self.turns.write().await;
        let mut sessions = self.sessions.write().await;
        let outcome = self.store.reconcile_workspace_retirement(command)?;
        // The worker skips finalized effects. Refresh only this operation's
        // projections while sharing the same mutation guard as retirement so
        // an operator-verified archive is immediately visible to every caller.
        let operation = self
            .store
            .get_operation(&command.operation_id)?
            .ok_or_else(|| {
                RuntimeError::NotFound(format!("retirement operation {}", command.operation_id))
            })?;
        let agents: HashSet<_> = operation
            .effects
            .iter()
            .filter(|effect| effect.effect_kind == "workspace_provider_close")
            .map(|effect| effect.target_id.as_str())
            .collect();
        let state = self.store.hydrate_runtime_state()?;
        for session in state
            .sessions
            .into_iter()
            .filter(|session| agents.contains(session.id.as_str()))
        {
            sessions.insert(session.id.clone(), session);
        }
        for turn in state
            .turns
            .into_iter()
            .filter(|turn| agents.contains(turn.session_id.as_str()))
        {
            turns.insert(turn.id.clone(), turn);
        }
        drop(sessions);
        drop(turns);
        let mut approvals = self.approvals.write().await;
        for approval in state
            .approvals
            .into_iter()
            .filter(|approval| agents.contains(approval.session_id.as_str()))
        {
            approvals.insert(approval.id.clone(), approval);
        }
        drop(approvals);
        let mut admissions = self.turn_admissions.write().await;
        for admission in self
            .store
            .list_turn_admissions()?
            .into_iter()
            .filter(|admission| agents.contains(admission.session_id.as_str()))
        {
            admissions.insert(admission.turn_id.clone(), admission);
        }
        Ok(outcome)
    }
}
