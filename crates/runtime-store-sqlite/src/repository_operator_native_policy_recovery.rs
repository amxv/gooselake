use runtime_core::RuntimeError;
use rusqlite::params;

use crate::db::{db_error, open_connection};
use crate::repository_operator_native_policy::{CLAIM_KIND, EFFECT_KIND, OPERATION_KIND};
use crate::SqliteRuntimeRepository;

impl SqliteRuntimeRepository {
    pub fn quarantine_started_operator_native_policies(
        &self,
        at_ms: i64,
    ) -> Result<Vec<String>, RuntimeError> {
        // Snapshot in a read transaction, close the connection and quarantine
        // each still-started effect with its existing IMMEDIATE CAS transition.
        // No provider API is called and no resource claim is released.
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
                .map_err(|error| {
                    db_error("failed preparing native policy startup quarantine", error)
                })?;
            let operation_ids = statement
                .query_map(params![OPERATION_KIND, EFFECT_KIND, CLAIM_KIND], |row| {
                    row.get::<_, String>(0)
                })
                .map_err(|error| db_error("failed reading native policy startup intents", error))?
                .collect::<Result<Vec<_>, _>>()
                .map_err(|error| {
                    db_error("failed decoding native policy startup intents", error)
                })?;
            operation_ids
        };
        let mut quarantined = Vec::new();
        for operation_id in operation_ids {
            self.mark_operator_native_policy_uncertain(
                &operation_id,
                "startup found native policy intent without durable acknowledgement; dispatch outcome unknown",
                at_ms,
            )?;
            quarantined.push(operation_id);
        }
        Ok(quarantined)
    }
}
