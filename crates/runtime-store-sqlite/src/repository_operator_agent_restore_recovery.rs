use runtime_core::RuntimeError;
use rusqlite::params;

use crate::db::{db_error, open_connection};
use crate::repository_operator_agent_restore::{CLAIM_KIND, EFFECT_KIND, OPERATION_KIND};
use crate::SqliteRuntimeRepository;

impl SqliteRuntimeRepository {
    /// A crash might happen at any instruction after the durable native intent.
    /// Never infer that an intended-but-unreceipted provider resume was absent.
    pub fn quarantine_started_operator_agent_restores(
        &self,
        at_ms: i64,
    ) -> Result<Vec<String>, RuntimeError> {
        let operation_ids = {
            let connection = open_connection(&self.database_path)?;
            let mut statement = connection
                .prepare(
                    "SELECT o.operation_id FROM runtime_operations o
                 JOIN runtime_operation_effects e ON e.operation_id=o.operation_id
                 JOIN runtime_operation_resource_claims c ON c.owner_operation_id=o.operation_id
                 WHERE o.kind=?1 AND o.phase='requested'
                   AND e.effect_kind=?2 AND e.phase='intended'
                   AND c.resource_kind=?3 AND c.claim_mode='exclusive'
                 GROUP BY o.operation_id ORDER BY o.created_at,o.operation_id",
                )
                .map_err(|error| db_error("failed preparing provider restore quarantine", error))?;
            let ids = statement
                .query_map(params![OPERATION_KIND, EFFECT_KIND, CLAIM_KIND], |row| {
                    row.get::<_, String>(0)
                })
                .map_err(|error| db_error("failed reading started restore intents", error))?
                .collect::<Result<Vec<_>, _>>()
                .map_err(|error| db_error("failed decoding started restore intents", error))?;
            ids
        };
        for operation_id in &operation_ids {
            self.mark_operator_agent_restore_uncertain(
                operation_id,
                "startup found intended native restore without an atomic agent receipt; exact provider outcome unknown",
                at_ms,
            )?;
        }
        Ok(operation_ids)
    }
}
