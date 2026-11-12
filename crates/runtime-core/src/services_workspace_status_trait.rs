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
        crate::services_workspace_status_trait::default_workspace_active_agent_routes_for_cwds!();
    };
}

pub(crate) use default_workspace_active_agent_routes;

// Other runtime stores may still expose only the complete active-agent route
// projection. SQLite overrides this to look up exactly the worktree paths
// needed by an HTTP inventory page, preserving full-claim conflict checks.
macro_rules! default_workspace_active_agent_routes_for_cwds {
    () => {
        fn list_workspace_active_agent_routes_for_cwds(
            &self,
            workspace_id: &str,
            cwds: &[String],
        ) -> Result<Vec<crate::WorkspaceActiveAgentRoute>, crate::RuntimeError> {
            if cwds.is_empty() {
                return Ok(Vec::new());
            }
            let selected = cwds
                .iter()
                .map(String::as_str)
                .collect::<std::collections::HashSet<_>>();
            Ok(self
                .list_workspace_active_agent_routes(workspace_id)?
                .into_iter()
                .filter(|route| selected.contains(route.authoritative_cwd.as_str()))
                .collect())
        }
    };
}

pub(crate) use default_workspace_active_agent_routes_for_cwds;
