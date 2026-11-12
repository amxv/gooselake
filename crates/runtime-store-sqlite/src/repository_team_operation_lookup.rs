use runtime_core::{RuntimeError, TeamOperationJournalRecord};
use rusqlite::{params, OptionalExtension};

use crate::db::{db_error, open_connection, string_to_json};
use crate::SqliteRuntimeRepository;

impl SqliteRuntimeRepository {
    /// Exact primary-key read; inspection never materializes the whole team
    /// or global journal, even after an incomplete native spawn rollback.
    pub fn get_team_operation_journal(
        &self,
        operation_id: &str,
    ) -> Result<Option<TeamOperationJournalRecord>, RuntimeError> {
        let connection = open_connection(&self.database_path)?;
        connection
            .query_row(
                "SELECT operation_id, team_id, kind, stage, payload_json, created_at, updated_at
                 FROM team_operation_journal WHERE operation_id = ?1",
                params![operation_id],
                |row| {
                    Ok(TeamOperationJournalRecord {
                        operation_id: row.get(0)?,
                        team_id: row.get(1)?,
                        kind: row.get(2)?,
                        stage: row.get(3)?,
                        payload: string_to_json(row.get(4)?)?,
                        created_at: row.get(5)?,
                        updated_at: row.get(6)?,
                    })
                },
            )
            .optional()
            .map_err(|error| db_error("failed loading team operation journal by ID", error))
    }
}
