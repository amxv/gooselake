use runtime_core::legacy_operation_inspection::*;
use runtime_core::RuntimeError;
use rusqlite::{params, Connection, OptionalExtension, TransactionBehavior};

use crate::db::{db_error, open_connection};
use crate::repository_team_spawn_admission::certified_spawn_response;
use crate::SqliteRuntimeRepository;

const JOURNAL_JSON_SQL: &str = "SELECT typeof(payload_json), length(CAST(payload_json AS BLOB)),
    CASE WHEN typeof(payload_json)='text' AND length(CAST(payload_json AS BLOB))<=?2
    THEN CAST(payload_json AS BLOB) END FROM team_operation_journal WHERE operation_id=?1";
const ADMISSION_JSON_SQL: &str = "SELECT typeof(completed_response_json),
    length(CAST(completed_response_json AS BLOB)),
    CASE WHEN typeof(completed_response_json)='text'
    AND length(CAST(completed_response_json AS BLOB))<=?2 THEN CAST(completed_response_json AS BLOB) END
    FROM team_spawn_admissions WHERE operation_id=?1";
const DIAGNOSTIC_JSON_SQL: &str = "SELECT typeof(payload_json), length(CAST(payload_json AS BLOB)),
    CASE WHEN typeof(payload_json)='text' AND length(CAST(payload_json AS BLOB))<=?2
    THEN CAST(payload_json AS BLOB) END FROM team_operation_diagnostics WHERE id=?1";

pub(crate) const DIAGNOSTIC_PAGE_SQL: &str = "SELECT id,operation_id,team_id,code,
    substr(CAST(message AS BLOB),1,?3),length(CAST(message AS BLOB)),created_at
    FROM team_operation_diagnostics WHERE operation_id=?1 AND id>?2
    ORDER BY id ASC LIMIT ?4";

struct PayloadBudget {
    loaded: usize,
}
struct RawJson {
    kind: String,
    bytes: u64,
    text: Option<String>,
    error: Option<String>,
}

impl PayloadBudget {
    fn read(
        &mut self,
        connection: &Connection,
        sql: &str,
        key: impl rusqlite::ToSql,
    ) -> Result<RawJson, RuntimeError> {
        let remaining = LEGACY_INSPECTION_TOTAL_PAYLOAD_BYTES.saturating_sub(self.loaded);
        let allowed = remaining.min(LEGACY_INSPECTION_MAX_PAYLOAD_BYTES);
        // SQLite TEXT may contain invalid UTF-8. Read the guarded bytes before
        // decoding so damaged text cannot hide the original evidence envelope.
        let (kind, bytes, raw): (String, Option<u64>, Option<Vec<u8>>) = connection
            .query_row(sql, params![key, allowed], |row| {
                Ok((row.get(0)?, row.get(1)?, row.get(2)?))
            })
            .map_err(|error| db_error("failed reading bounded historical JSON evidence", error))?;
        let bytes = bytes.unwrap_or(0);
        let (text, error) = if kind == "null" {
            (None, None)
        } else if kind != "text" {
            (
                None,
                Some(format!("historical JSON stored as {kind}, expected text")),
            )
        } else if let Some(raw) = raw {
            self.loaded += raw.len();
            match String::from_utf8(raw) {
                Ok(text) => (Some(text), None),
                Err(error) => (
                    None,
                    Some(format!(
                        "historical JSON contains invalid UTF-8: {}",
                        error.utf8_error()
                    )),
                ),
            }
        } else {
            (
                None,
                Some(if bytes > LEGACY_INSPECTION_MAX_PAYLOAD_BYTES as u64 {
                    format!("historical JSON omitted: {bytes} bytes exceeds per-payload limit {LEGACY_INSPECTION_MAX_PAYLOAD_BYTES}")
                } else {
                    format!("historical JSON omitted: {bytes} bytes exceeds remaining payload budget {remaining}")
                }),
            )
        };
        Ok(RawJson {
            kind,
            bytes,
            text,
            error,
        })
    }
}

impl RawJson {
    fn decode(self) -> InspectedLegacyJson {
        let (payload, error) = match (self.text, self.error) {
            (_, Some(error)) => (None, Some(error)),
            (Some(text), None) => match serde_json::from_str(&text) {
                Ok(value) => (Some(value), None),
                Err(error) => (
                    None,
                    Some(format!("historical JSON decode failed: {error}")),
                ),
            },
            (None, None) => (None, Some("historical JSON is SQL null".into())),
        };
        InspectedLegacyJson {
            payload,
            payload_bytes: self.bytes,
            payload_decode_error: error,
        }
    }
}

fn empty_evidence() -> InspectedLegacyJson {
    InspectedLegacyJson {
        payload: None,
        payload_bytes: 0,
        payload_decode_error: None,
    }
}

fn read_operation(
    connection: &Connection,
    operation_id: &str,
    budget: &mut PayloadBudget,
) -> Result<Option<InspectedTeamOperation>, RuntimeError> {
    let mut operation = connection
        .query_row(
            "SELECT operation_id,team_id,kind,stage,created_at,updated_at
        FROM team_operation_journal WHERE operation_id=?1",
            [operation_id],
            |row| {
                Ok(InspectedTeamOperation {
                    operation_id: row.get(0)?,
                    team_id: row.get(1)?,
                    kind: row.get(2)?,
                    stage: row.get(3)?,
                    created_at: row.get(4)?,
                    updated_at: row.get(5)?,
                    evidence: empty_evidence(),
                })
            },
        )
        .optional()
        .map_err(|error| db_error("failed inspecting original team operation", error))?;
    if let Some(operation) = &mut operation {
        operation.evidence = budget
            .read(connection, JOURNAL_JSON_SQL, operation_id)?
            .decode();
    }
    Ok(operation)
}

fn read_admission(
    connection: &Connection,
    operation_id: &str,
    operation: Option<&InspectedTeamOperation>,
    budget: &mut PayloadBudget,
) -> Result<Option<InspectedSpawnAdmission>, RuntimeError> {
    let mut admission = connection
        .query_row(
            "SELECT operation_id,team_id,source_session_id,created_at,updated_at
        FROM team_spawn_admissions WHERE operation_id=?1",
            [operation_id],
            |row| {
                Ok(InspectedSpawnAdmission {
                    operation_id: row.get(0)?,
                    team_id: row.get(1)?,
                    source_session_id: row.get(2)?,
                    created_at: row.get(3)?,
                    updated_at: row.get(4)?,
                    completion_response_present: false,
                    completion_response_bytes: 0,
                    completion_certified: false,
                    completion_decode_error: None,
                })
            },
        )
        .optional()
        .map_err(|error| db_error("failed inspecting original spawn admission", error))?;
    if let Some(admission) = &mut admission {
        let raw = budget.read(connection, ADMISSION_JSON_SQL, operation_id)?;
        admission.completion_response_present = raw.kind != "null";
        admission.completion_response_bytes = raw.bytes;
        admission.completion_certified = certified_spawn_response(
            operation_id,
            &admission.team_id,
            raw.text.as_deref(),
            operation.map(|o| o.stage.as_str()),
            operation.map(|o| o.team_id.as_str()),
            operation.map(|o| o.kind.as_str()),
        )
        .is_some();
        admission.completion_decode_error = raw.error.or_else(|| {
            (admission.completion_response_present && !admission.completion_certified).then(|| {
                "saved completion does not certify this original operation and journal".into()
            })
        });
    }
    Ok(admission)
}

fn message_preview(raw: &[u8], original_bytes: u64) -> (String, bool, bool) {
    let utf8_replaced = std::str::from_utf8(raw).is_err();
    let mut preview = String::from_utf8_lossy(raw).into_owned();
    let shortened = preview.len() > LEGACY_INSPECTION_MESSAGE_BYTES;
    if shortened {
        let mut end = LEGACY_INSPECTION_MESSAGE_BYTES;
        while !preview.is_char_boundary(end) {
            end -= 1;
        }
        preview.truncate(end);
    }
    (
        preview,
        original_bytes > raw.len() as u64 || shortened,
        utf8_replaced,
    )
}

fn read_diagnostics(
    connection: &Connection,
    operation_id: &str,
    after: i64,
    limit: usize,
    budget: &mut PayloadBudget,
) -> Result<(Vec<InspectedTeamOperationDiagnostic>, Option<i64>), RuntimeError> {
    // Fetch only metadata and a bounded message preview. JSON reads below use
    // the remaining aggregate budget, in this same SQLite read transaction.
    let mut diagnostics = {
        let mut statement = connection
            .prepare(DIAGNOSTIC_PAGE_SQL)
            .map_err(|error| db_error("failed preparing historical diagnostic page", error))?;
        let rows = statement
            .query_map(
                params![
                    operation_id,
                    after,
                    LEGACY_INSPECTION_MESSAGE_BYTES,
                    limit + 1
                ],
                |row| {
                    let message: Vec<u8> = row.get(4)?;
                    let message_bytes: u64 = row.get(5)?;
                    let (message, message_truncated, message_utf8_replaced) =
                        message_preview(&message, message_bytes);
                    Ok(InspectedTeamOperationDiagnostic {
                        id: row.get(0)?,
                        operation_id: row.get(1)?,
                        team_id: row.get(2)?,
                        code: row.get(3)?,
                        message,
                        message_bytes,
                        message_truncated,
                        message_utf8_replaced,
                        created_at: row.get(6)?,
                        evidence: empty_evidence(),
                    })
                },
            )
            .map_err(|error| db_error("failed querying historical diagnostic page", error))?;
        rows.collect::<Result<Vec<_>, _>>()
            .map_err(|error| db_error("failed reading historical diagnostic page", error))?
    };
    let more = diagnostics.len() > limit;
    diagnostics.truncate(limit);
    for diagnostic in &mut diagnostics {
        diagnostic.evidence = budget
            .read(connection, DIAGNOSTIC_JSON_SQL, diagnostic.id)?
            .decode();
    }
    let next = if more {
        diagnostics.last().map(|d| d.id)
    } else {
        None
    };
    Ok((diagnostics, next))
}

impl SqliteRuntimeRepository {
    /// Tolerate historical JSON damage without changing any evidence. The
    /// exact journal, independent admission and bounded page share a snapshot.
    pub fn inspect_legacy_team_operation(
        &self,
        operation_id: &str,
        after_diagnostic_id: Option<i64>,
        limit: usize,
    ) -> Result<Option<LegacyTeamOperationInspection>, RuntimeError> {
        let after = after_diagnostic_id.unwrap_or(0);
        if after < 0 || !(1..=LEGACY_INSPECTION_MAX_DIAGNOSTICS).contains(&limit) {
            return Err(RuntimeError::Configuration(format!(
                "historical inspection requires after_diagnostic_id >= 0 and limit 1..={LEGACY_INSPECTION_MAX_DIAGNOSTICS}")));
        }
        let mut connection = open_connection(&self.database_path)?;
        let snapshot = connection
            .transaction_with_behavior(TransactionBehavior::Deferred)
            .map_err(|error| db_error("failed starting historical inspection snapshot", error))?;
        let mut budget = PayloadBudget { loaded: 0 };
        let operation = read_operation(&snapshot, operation_id, &mut budget)?;
        let admission = read_admission(&snapshot, operation_id, operation.as_ref(), &mut budget)?;
        if operation.is_none() && admission.is_none() {
            return Ok(None);
        }
        let (diagnostics, next_diagnostic_id) =
            read_diagnostics(&snapshot, operation_id, after, limit, &mut budget)?;
        let requires_manual_review = !admission.as_ref().is_some_and(|a| a.completion_certified);
        snapshot
            .commit()
            .map_err(|error| db_error("failed closing historical inspection snapshot", error))?;
        Ok(Some(LegacyTeamOperationInspection {
            operation,
            admission,
            diagnostics,
            next_diagnostic_id,
            diagnostics_truncated: next_diagnostic_id.is_some(),
            max_payload_bytes: LEGACY_INSPECTION_MAX_PAYLOAD_BYTES,
            total_payload_budget_bytes: LEGACY_INSPECTION_TOTAL_PAYLOAD_BYTES,
            payload_bytes_loaded: budget.loaded,
            native_retry_authorized: false,
            requires_manual_review,
        }))
    }
}
