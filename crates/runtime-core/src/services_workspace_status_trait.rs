use std::collections::HashMap;

/// The only two fields worktree inventory needs from an active workspace
/// agent. It is a derived read projection, never independent route authority.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkspaceActiveAgentRoute {
    pub agent_id: String,
    pub authoritative_cwd: String,
}

/// Bounded active roster plus the complete active-member count and only the
/// aliases needed for displayed creator and lead provenance. This read model
/// never owns workspace membership or alias authority.
#[derive(Debug, Clone, PartialEq)]
pub struct WorkspaceStatusRosterSnapshot {
    pub members: Vec<crate::WorkspaceAgentRecord>,
    pub total_active_members: usize,
    pub alias_by_agent_id: HashMap<String, String>,
}

// Other RuntimeStore implementations keep the previous whole-roster behavior.
// SQLite overrides this with an indexed, bounded, single-snapshot query.
macro_rules! default_workspace_status_roster {
    () => {
        fn list_workspace_status_roster(
            &self,
            workspace_id: &str,
            _lead_agent_id: Option<&str>,
            limit: usize,
        ) -> Result<crate::WorkspaceStatusRosterSnapshot, crate::RuntimeError> {
            let mut members = self.list_workspace_agents(
                workspace_id,
                Some(crate::WorkspaceAgentLifecycleState::Active),
            )?;
            members.sort_by(|left, right| {
                left.alias
                    .cmp(&right.alias)
                    .then(left.agent_id.cmp(&right.agent_id))
            });
            let total_active_members = members.len();
            let alias_by_agent_id = members
                .iter()
                .map(|member| (member.agent_id.clone(), member.alias.clone()))
                .collect();
            members.truncate(limit);
            Ok(crate::WorkspaceStatusRosterSnapshot {
                members,
                total_active_members,
                alias_by_agent_id,
            })
        }
    };
}

pub(crate) use default_workspace_status_roster;

// Other backends may only support the existing full roster query. SQLite
// overrides this to avoid decoding provider sessions and agent profiles.
macro_rules! default_workspace_active_agent_routes {
    () => {
        fn list_workspace_active_agent_routes(
            &self,
            workspace_id: &str,
        ) -> Result<Vec<crate::WorkspaceActiveAgentRoute>, crate::RuntimeError> {
            Ok(self
                .list_workspace_agents(
                    workspace_id,
                    Some(crate::WorkspaceAgentLifecycleState::Active),
                )?
                .into_iter()
                .map(|agent| crate::WorkspaceActiveAgentRoute {
                    agent_id: agent.agent_id,
                    authoritative_cwd: agent.recreation_policy.authoritative_cwd,
                })
                .collect())
        }
    };
}

pub(crate) use default_workspace_active_agent_routes;
