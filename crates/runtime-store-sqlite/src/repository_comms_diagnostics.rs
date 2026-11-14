use std::collections::BTreeMap;

use runtime_core::{LegacyCommsDiagnosticCounts, RuntimeError};
use rusqlite::TransactionBehavior;

use crate::db::{db_error, open_connection};
use crate::SqliteRuntimeRepository;

pub(crate) const LEGACY_ROW_COUNTS_SQL: &str = "
    SELECT (SELECT COUNT(*) FROM teams),
           (SELECT COUNT(*) FROM team_members),
           (SELECT COUNT(*) FROM team_messages)";

pub(crate) const LEGACY_DELIVERY_STATUS_COUNTS_SQL: &str =
    "SELECT status, COUNT(*) FROM team_deliveries GROUP BY status";

fn checked_count(value: i64) -> Result<usize, RuntimeError> {
    usize::try_from(value).map_err(|_| {
        RuntimeError::ProtocolViolation("negative or oversized comms diagnostic count".into())
    })
}

impl SqliteRuntimeRepository {
    /// Historical legacy team row counts, not canonical /v2 agent messaging.
    /// Both queries use one deferred SQLite read snapshot; neither reads
    /// message inputs, delivery errors, or unrelated provider/runtime state.
    pub fn legacy_comms_diagnostic_counts(
        &self,
    ) -> Result<LegacyCommsDiagnosticCounts, RuntimeError> {
        let mut connection = open_connection(&self.database_path)?;
        let snapshot = connection
            .transaction_with_behavior(TransactionBehavior::Deferred)
            .map_err(|error| db_error("failed starting comms diagnostic snapshot", error))?;

        let (teams, members, messages): (i64, i64, i64) = snapshot
            .query_row(LEGACY_ROW_COUNTS_SQL, [], |row| {
                Ok((row.get(0)?, row.get(1)?, row.get(2)?))
            })
            .map_err(|error| db_error("failed aggregating legacy team row counts", error))?;
        let mut counts = LegacyCommsDiagnosticCounts {
            team_count: checked_count(teams)?,
            member_count: checked_count(members)?,
            message_count: checked_count(messages)?,
            delivery_status_counts: BTreeMap::new(),
            ..Default::default()
        };
        {
            let mut statement = snapshot
                .prepare(LEGACY_DELIVERY_STATUS_COUNTS_SQL)
                .map_err(|error| {
                    db_error("failed preparing legacy delivery status counts", error)
                })?;
            let mut rows = statement
                .query([])
                .map_err(|error| db_error("failed querying legacy delivery statuses", error))?;
            while let Some(row) = rows
                .next()
                .map_err(|error| db_error("failed reading legacy delivery status row", error))?
            {
                let status: String = row
                    .get(0)
                    .map_err(|error| db_error("failed decoding legacy delivery status", error))?;
                let count =
                    checked_count(row.get(1).map_err(|error| {
                        db_error("failed decoding legacy delivery count", error)
                    })?)?;
                counts.delivery_total =
                    counts.delivery_total.checked_add(count).ok_or_else(|| {
                        RuntimeError::ProtocolViolation("legacy delivery count overflow".into())
                    })?;
                counts.delivery_status_counts.insert(status, count);
            }
        }

        snapshot
            .commit()
            .map_err(|error| db_error("failed closing comms diagnostic snapshot", error))?;
        Ok(counts)
    }
}
