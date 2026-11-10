macro_rules! delegate_workspace_status_roster {
    () => {
        fn list_workspace_status_roster(
            &self,
            workspace_id: &str,
            lead_agent_id: Option<&str>,
            limit: usize,
        ) -> Result<runtime_core::WorkspaceStatusRosterSnapshot, runtime_core::RuntimeError> {
            self.repository
                .list_workspace_status_roster(workspace_id, lead_agent_id, limit)
        }
    };
}

pub(crate) use delegate_workspace_status_roster;
