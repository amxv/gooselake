/// Keep the RuntimeStore forwarding surface compact; admission, conflict
/// classification and exact stage/response settlement live in the repository
/// transaction implementation rather than in an in-process cache.
macro_rules! delegate_team_spawn_admission {
    () => {
        fn replay_team_member_spawn(
            &self,
            team_id: &str,
            source_session_id: &str,
            idempotency_key: &str,
            request_hash: &str,
        ) -> Result<Option<runtime_core::TeamMemberSpawnAdmission>, runtime_core::RuntimeError> {
            self.repository().replay_team_member_spawn(
                team_id,
                source_session_id,
                idempotency_key,
                request_hash,
            )
        }

        fn admit_team_member_spawn(
            &self,
            operation_id: &str,
            request: &runtime_core::TeamMemberSpawnRequest,
            request_hash: &str,
            requested_at: i64,
        ) -> Result<runtime_core::TeamMemberSpawnAdmission, runtime_core::RuntimeError> {
            self.repository().admit_team_member_spawn(
                operation_id,
                request,
                request_hash,
                requested_at,
            )
        }

        fn complete_team_member_spawn(
            &self,
            operation_id: &str,
            response: &runtime_core::TeamMemberSpawnResponse,
            completed_at: i64,
        ) -> Result<(), runtime_core::RuntimeError> {
            self.repository()
                .complete_team_member_spawn(operation_id, response, completed_at)
        }
    };
}

pub(crate) use delegate_team_spawn_admission;
