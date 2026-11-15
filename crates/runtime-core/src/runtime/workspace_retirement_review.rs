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
        // Read every affected snapshot before changing any in-memory authority:
        // one corrupt *affected* target must not partially refresh its peers.
        let refreshed = agents
            .into_iter()
            .map(|agent_id| self.store.get_persisted_retirement_review_session(agent_id))
            .collect::<Result<Vec<_>, _>>()?;
        for snapshot in refreshed.iter().flatten() {
            sessions.insert(snapshot.session.id.clone(), snapshot.session.clone());
            for turn in &snapshot.turns {
                turns.insert(turn.id.clone(), turn.clone());
            }
        }
        drop(sessions);
        drop(turns);
        let mut approvals = self.approvals.write().await;
        for snapshot in refreshed.iter().flatten() {
            for approval in &snapshot.approvals {
                approvals.insert(approval.id.clone(), approval.clone());
            }
        }
        drop(approvals);
        let mut admissions = self.turn_admissions.write().await;
        for snapshot in refreshed.into_iter().flatten() {
            for admission in snapshot.admissions {
                admissions.insert(admission.turn_id.clone(), admission);
            }
        }
        Ok(outcome)
    }
}
