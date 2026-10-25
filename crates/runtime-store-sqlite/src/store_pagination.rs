// Keep the SQLite-backed keyset and existing workspace lookup delegation
// inside the single RuntimeStore implementation without bloating store.rs.
macro_rules! delegate_workspace_listing {
    () => {
        fn list_workspaces(
            &self,
        ) -> Result<Vec<runtime_core::WorkspaceRecord>, runtime_core::RuntimeError> {
            self.repository.list_workspaces()
        }

        fn list_workspaces_page(
            &self,
            after: Option<(i64, String)>,
            limit: usize,
        ) -> Result<Vec<runtime_core::WorkspaceRecord>, runtime_core::RuntimeError> {
            self.repository.list_workspaces_page(after, limit)
        }

        fn get_workspace(
            &self,
            workspace_id: &str,
        ) -> Result<Option<runtime_core::WorkspaceRecord>, runtime_core::RuntimeError> {
            self.repository.get_workspace(workspace_id)
        }

        fn list_workspace_agents(
            &self,
            workspace_id: &str,
            lifecycle: Option<runtime_core::WorkspaceAgentLifecycleState>,
        ) -> Result<Vec<runtime_core::WorkspaceAgentRecord>, runtime_core::RuntimeError> {
            self.repository
                .list_workspace_agents(workspace_id, lifecycle)
        }

        fn list_workspace_agents_page(
            &self,
            workspace_id: &str,
            lifecycle: Option<runtime_core::WorkspaceAgentLifecycleState>,
            after: Option<(i64, String)>,
            limit: usize,
        ) -> Result<Vec<runtime_core::WorkspaceAgentRecord>, runtime_core::RuntimeError> {
            self.repository
                .list_workspace_agents_page(workspace_id, lifecycle, after, limit)
        }
    };
}

pub(crate) use delegate_workspace_listing;
