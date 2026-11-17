macro_rules! delegate_agent_tool_methods {
    () => {
        fn read_operation_effect_legacy_recovery(
            &self,
            operation_id: &str,
            effect_id: &str,
            effect_kind: &str,
            target_id: &str,
        ) -> Result<
            Option<runtime_core::legacy_spawn_obligations::LegacyNativeRecovery>,
            RuntimeError,
        > {
            self.repository.read_operation_effect_legacy_recovery(
                operation_id,
                effect_id,
                effect_kind,
                target_id,
            )
        }

        fn record_operation_effect_legacy_recovery(
            &self,
            operation_id: &str,
            effect_id: &str,
            effect_kind: &str,
            target_id: &str,
            recovery: &runtime_core::legacy_spawn_obligations::LegacyNativeRecovery,
            at_ms: i64,
        ) -> Result<(), RuntimeError> {
            self.repository.record_operation_effect_legacy_recovery(
                operation_id,
                effect_id,
                effect_kind,
                target_id,
                recovery,
                at_ms,
            )
        }

        fn plan_member_agent_create(
            &self,
            command: &runtime_core::MemberAgentCreateCommand,
        ) -> Result<runtime_core::MemberAgentCreateCommand, RuntimeError> {
            self.repository.plan_member_agent_create(command)
        }
        fn replay_member_agent_create(
            &self,
            command: &runtime_core::MemberAgentCreateCommand,
        ) -> Result<Option<runtime_core::OperatorAgentCreateAdmission>, RuntimeError> {
            self.repository.replay_member_agent_create(command)
        }
        fn admit_member_agent_create(
            &self,
            command: &runtime_core::MemberAgentCreateCommand,
        ) -> Result<runtime_core::OperatorAgentCreateAdmission, RuntimeError> {
            self.repository.admit_member_agent_create(command)
        }
        fn member_agent_create_result(
            &self,
            command: &runtime_core::MemberAgentCreateCommand,
        ) -> Result<Value, RuntimeError> {
            self.repository.member_agent_create_result(command)
        }
        fn admit_member_worktree_creation(
            &self,
            command: &runtime_core::MemberAgentCreateCommand,
            git: &runtime_core::WorkspaceWorktreeCreationCommand,
        ) -> Result<runtime_core::WorkspaceWorktreeCreationAdmission, RuntimeError> {
            self.repository.admit_member_worktree_creation(command, git)
        }
        fn plan_member_agent_rebind(
            &self,
            parent: &str,
            caller: &str,
            policy: runtime_core::WorkspaceMembershipPolicy,
            destination: &str,
            repository: &runtime_core::RepositoryIdentity,
            at_ms: i64,
        ) -> Result<runtime_core::MemberAgentRebindCommand, RuntimeError> {
            self.repository.plan_member_agent_rebind(
                parent,
                caller,
                policy,
                destination,
                repository,
                at_ms,
            )
        }
        fn replay_member_agent_rebind(
            &self,
            command: &runtime_core::MemberAgentRebindCommand,
        ) -> Result<Option<runtime_core::MemberAgentRebindAdmission>, RuntimeError> {
            self.repository.replay_member_agent_rebind(command)
        }
        fn admit_member_agent_rebind(
            &self,
            command: &runtime_core::MemberAgentRebindCommand,
        ) -> Result<runtime_core::MemberAgentRebindAdmission, RuntimeError> {
            self.repository.admit_member_agent_rebind(command)
        }
        fn reject_member_agent_rebind(
            &self,
            command: &runtime_core::MemberAgentRebindCommand,
            reason: &str,
            at_ms: i64,
        ) -> Result<(), RuntimeError> {
            self.repository
                .reject_member_agent_rebind(command, reason, at_ms)
        }
        fn quarantine_started_workspace_agent_rebinds(
            &self,
            at_ms: i64,
        ) -> Result<Vec<String>, RuntimeError> {
            self.repository
                .quarantine_started_workspace_agent_rebinds(at_ms)
        }
        fn unresolved_workspace_agent_rebind_operation(
            &self,
            agent_id: &str,
        ) -> Result<Option<String>, RuntimeError> {
            self.repository
                .unresolved_workspace_agent_rebind_operation(agent_id)
        }
        fn plan_member_agent_archives(
            &self,
            parent_operation_id: &str,
            caller_agent_id: &str,
            policy: runtime_core::WorkspaceMembershipPolicy,
            planned_at: i64,
        ) -> Result<Vec<runtime_core::OperatorAgentArchiveCommand>, RuntimeError> {
            self.repository.plan_member_agent_archives(
                parent_operation_id,
                caller_agent_id,
                policy,
                planned_at,
            )
        }
        fn reject_member_agent_archive(
            &self,
            command: &runtime_core::OperatorAgentArchiveCommand,
            reason: &str,
            at_ms: i64,
        ) -> Result<(), RuntimeError> {
            self.repository
                .reject_member_agent_archive(command, reason, at_ms)
        }
        fn replay_agent_tool_mutation(
            &self,
            command: &AgentToolMutationCommand,
        ) -> Result<Option<AgentToolMutationAdmission>, RuntimeError> {
            self.repository.replay_agent_tool_mutation(command)
        }
        fn begin_agent_tool_mutation(
            &self,
            command: &AgentToolMutationCommand,
        ) -> Result<AgentToolMutationAdmission, RuntimeError> {
            self.repository.begin_agent_tool_mutation(command)
        }
        fn finish_agent_tool_mutation(
            &self,
            operation_id: &str,
            exact_result: &Value,
            finished_at: i64,
        ) -> Result<(), RuntimeError> {
            self.repository
                .finish_agent_tool_mutation(operation_id, exact_result, finished_at)
        }
    };
}

pub(crate) use delegate_agent_tool_methods;
