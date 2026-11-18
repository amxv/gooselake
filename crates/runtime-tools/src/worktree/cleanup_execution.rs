//! One invocation per durable grant. There is no replay execution path.
use super::*;
use runtime_core::{
    BranchDeletionAck, CheckoutRemovalAck, CheckoutRemovalGrant, ManagedCleanupAttempt,
    ManagedCleanupBranchAdmission, ManagedCleanupObservation, WorktreeCleanupResponse,
};

impl RuntimeWorktreeService {
    fn hold_cleanup(
        &self,
        attempt: ManagedCleanupAttempt,
        observation: ManagedCleanupObservation,
        diagnostic: String,
    ) -> RuntimeError {
        let id = match &attempt {
            ManagedCleanupAttempt::Checkout(g) => &g.binding.operation_id,
            ManagedCleanupAttempt::Branch(g) => &g.binding.operation_id,
        }
        .clone();
        // A failed hold cannot erase the already committed pending fence.
        let _ =
            self.store
                .hold_managed_worktree_cleanup(&attempt, &observation, &diagnostic, now_ms());
        RuntimeError::operation_unresolved(id.clone(),format!("{diagnostic}; inspect /v2/operations/{id}; the native action will not be dispatched again"))
    }

    /// Private injected runner keeps disposable failure fixtures independent
    /// of process-global PATH and real provider/native environments.
    pub(super) fn execute_cleanup<F>(
        &self,
        grant: CheckoutRemovalGrant,
        mut run: F,
    ) -> Result<WorktreeCleanupResponse, RuntimeError>
    where
        F: FnMut(&str, &[&str]) -> Result<i32, RuntimeError>,
    {
        let target = &grant.binding.target;
        let checkout_attempt = ManagedCleanupAttempt::Checkout(grant.clone());
        match run(
            &target.repo_root,
            &["worktree", "remove", &target.checkout_path],
        ) {
            Ok(0) => {}
            Ok(code) => {
                return Err(self.hold_cleanup(
                    checkout_attempt,
                    ManagedCleanupObservation::Exited { code },
                    "original checkout removal did not return zero; branch was not granted".into(),
                ))
            }
            Err(error) => {
                return Err(self.hold_cleanup(
                    checkout_attempt,
                    ManagedCleanupObservation::Unknown {
                        diagnostic: error.to_string(),
                    },
                    "original checkout removal outcome is unresolved; branch was not granted"
                        .into(),
                ))
            }
        }
        let checkout_ack = CheckoutRemovalAck {
            grant: grant.clone(),
            exit_code: 0,
            observed_at: now_ms(),
        };
        let branch=match self.store.acknowledge_managed_checkout_removal(&checkout_ack) {
            Ok(ManagedCleanupBranchAdmission::Execute{grant})=>grant,
            Ok(ManagedCleanupBranchAdmission::Unresolved{operation_id})=>return Err(RuntimeError::operation_unresolved(operation_id.clone(),
                format!("checkout acknowledgement did not return a fresh branch grant; inspect /v2/operations/{operation_id}"))),
            Err(error)=>return Err(self.hold_cleanup(checkout_attempt,ManagedCleanupObservation::Exited{code:0},
                format!("checkout returned zero but its durable receipt or branch grant is unresolved: {error}"))),
        };
        let branch_attempt = ManagedCleanupAttempt::Branch(branch.clone());
        if let Err(error) =
            self.ensure_repository_native_mutation_allowed(&branch.binding.target.repo_root)
        {
            return Err(self.hold_cleanup(
                branch_attempt,
                ManagedCleanupObservation::Unknown {
                    diagnostic: error.to_string(),
                },
                "branch grant retained without dispatch because repository eligibility changed"
                    .into(),
            ));
        }
        let target = &branch.binding.target;
        match run(&target.repo_root, &["branch", "-d", &target.branch_name]) {
            Ok(0) => {}
            Ok(code) => {
                return Err(self.hold_cleanup(
                    branch_attempt,
                    ManagedCleanupObservation::Exited { code },
                    "original branch deletion did not return zero".into(),
                ))
            }
            Err(error) => {
                return Err(self.hold_cleanup(
                    branch_attempt,
                    ManagedCleanupObservation::Unknown {
                        diagnostic: error.to_string(),
                    },
                    "original branch deletion outcome is unresolved".into(),
                ))
            }
        }
        self.store.acknowledge_managed_branch_deletion(&BranchDeletionAck {grant:branch,exit_code:0,observed_at:now_ms()})
            .map_err(|error|self.hold_cleanup(branch_attempt,ManagedCleanupObservation::Exited{code:0},
                format!("branch returned zero but its durable terminal receipt is unresolved: {error}")))
    }
}
