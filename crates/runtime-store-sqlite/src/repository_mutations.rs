use runtime_core::{RuntimeError, RuntimeRecordMutation};
use rusqlite::Connection;

use crate::SqliteRuntimeRepository;

impl SqliteRuntimeRepository {
    pub(crate) fn apply_runtime_mutations_on(
        connection: &Connection,
        mutations: &[RuntimeRecordMutation],
    ) -> Result<(), RuntimeError> {
        for mutation in mutations {
            match mutation {
                RuntimeRecordMutation::Session(record) => {
                    Self::upsert_session_on(connection, record)?
                }
                RuntimeRecordMutation::Turn(record) => Self::upsert_turn_on(connection, record)?,
                RuntimeRecordMutation::Approval(record) => {
                    Self::upsert_approval_on(connection, record)?
                }
                RuntimeRecordMutation::Team(record) => Self::upsert_team_on(connection, record)?,
                RuntimeRecordMutation::TeamMember(record) => {
                    Self::upsert_team_member_on(connection, record)?
                }
                RuntimeRecordMutation::TeamMemberDelete { team_id, agent_id } => {
                    Self::delete_team_member_on(connection, team_id, agent_id)?
                }
                RuntimeRecordMutation::TeamMessage(record) => {
                    Self::upsert_team_message_on(connection, record)?
                }
                RuntimeRecordMutation::TeamDelivery(record) => {
                    Self::upsert_team_delivery_on(connection, record)?
                }
                RuntimeRecordMutation::ManagedWorktree(record) => {
                    Self::upsert_managed_worktree_on(connection, record)?
                }
                RuntimeRecordMutation::ManagedWorktreeClaim(record) => {
                    Self::upsert_managed_worktree_claim_on(connection, record)?
                }
                RuntimeRecordMutation::Process(record) => {
                    Self::upsert_process_on(connection, record)?
                }
            }
        }
        Ok(())
    }
}
