//! Transaction-owned bounded classification of unresolved historical spawns.
//! A caller may retain an IMMEDIATE writer lock through a guarded mutation;
//! the read-only wrapper below cannot replace that admission authority.
use crate::db::{db_error, open_connection};
use crate::repository_team_spawn_admission::certified_spawn_response;
use crate::SqliteRuntimeRepository;
use runtime_core::legacy_spawn_obligations::*;
use runtime_core::{ManagedWorktreeRecord, RuntimeError};
use rusqlite::{params, Connection, OptionalExtension, Row, TransactionBehavior};
use serde::Deserialize;
use std::{collections::BTreeSet, path::Path, sync::Arc};

const KIND: &str = "spawn_member_with_worktree";
const PAGE_ROWS: usize = 64;
const METADATA_CELL_BYTES: usize = 4096;
const JSON_CELL_BYTES: usize = 65536;
const TARGET_COUNT_LIMIT: usize = 1024;
const TARGET_BYTES_LIMIT: usize = 4 * 1024 * 1024;

#[derive(Debug)]
enum Stop {
    Database(RuntimeError),
    Limit(LegacyScanLimit),
}
type ScanResult<T> = Result<T, Stop>;
struct ScanState {
    budget: LegacySpawnScanBudget,
    stats: LegacySpawnScanStats,
}
impl ScanState {
    fn charge(&mut self, bytes: usize) -> ScanResult<()> {
        let next = self
            .stats
            .materialized_bytes
            .checked_add(bytes)
            .ok_or(Stop::Limit(LegacyScanLimit::ByteBudget))?;
        if next > self.budget.max_bytes {
            return Err(Stop::Limit(LegacyScanLimit::ByteBudget));
        }
        self.stats.materialized_bytes = next;
        Ok(())
    }
    fn remaining_bytes(&self) -> usize {
        self.budget
            .max_bytes
            .saturating_sub(self.stats.materialized_bytes)
    }
}
struct Metadata {
    operation_id: String,
    team_id: String,
    journal_team_id: Option<String>,
    kind: Option<String>,
    stage: Option<String>,
    journal_present: bool,
    already_keyed: bool,
}
enum RawJson {
    Absent,
    NonText,
    InvalidUtf8,
    Text(String),
}
#[derive(Default)]
struct Scope {
    /// Some is a complete session axis; None means unknown, not an empty set.
    complete_child: Option<String>,
    positive_children: BTreeSet<String>,
    /// Complete spelling snapshot; not certified historical physical identity.
    checkout_snapshot: Option<ManagedWorktreeRecord>,
    positive_worktrees: BTreeSet<String>,
    evidence_problem: Option<&'static str>,
}

/// Existing receipts/no-ops must be handled before this new-admission fence.
/// A protected mutation retains its caller-owned IMMEDIATE lock afterwards.
/// A read-only runtime preflight supplies a Deferred transaction, never an
/// autocommit connection or permanent process-local clear cache.
pub(crate) fn classify_legacy_spawn_obligations(
    connection: &Connection,
    targets: &[LegacyNativeTarget],
    budget: LegacySpawnScanBudget,
) -> Result<LegacySpawnScan, RuntimeError> {
    if connection.is_autocommit() {
        return Err(RuntimeError::InvalidState(
            "legacy native obligation scan requires a caller-owned transaction".into(),
        ));
    }
    let mut state = ScanState {
        budget,
        stats: LegacySpawnScanStats::default(),
    };
    if !targets_within_budget(targets) {
        // Do not allocate an unbounded per-target result for invalid input.
        return Ok(incomplete(
            Vec::new(),
            None,
            None,
            LegacyScanLimit::TargetBudget,
            state.stats,
        ));
    }
    // Clear is provisional internally; every partial exit replaces it.
    let mut decisions = vec![LegacyTargetDecision::Clear; targets.len()];
    if targets.is_empty() {
        return Ok(LegacySpawnScan {
            decisions,
            termination: LegacyScanTermination::ExhaustedBothStreams,
            stats: state.stats,
        });
    }
    for stream in [
        LegacySpawnStream::KeyedAdmissions,
        LegacySpawnStream::SpawnJournal,
    ] {
        let mut after = None;
        loop {
            let remaining = budget.max_rows.saturating_sub(state.stats.source_rows_read);
            if remaining == 0 {
                if stream_has_more(connection, stream, after.as_deref())? {
                    return Ok(incomplete(
                        decisions,
                        Some(stream),
                        after,
                        LegacyScanLimit::RowBudget,
                        state.stats,
                    ));
                }
                break;
            }
            let limit = PAGE_ROWS.min(remaining);
            let page = match metadata_page(connection, stream, after.as_deref(), limit, &mut state)
            {
                Ok(page) => page,
                Err(Stop::Database(error)) => return Err(error),
                Err(Stop::Limit(reason)) => {
                    return Ok(incomplete(
                        decisions,
                        Some(stream),
                        after,
                        reason,
                        state.stats,
                    ))
                }
            };
            let exhausted = page.len() < limit;
            for meta in page {
                // Count all spawn-journal keys before excluding keyed rows.
                // SQL NOT EXISTS before LIMIT would hide filtered scan work.
                if meta.already_keyed {
                    after = Some(meta.operation_id);
                    continue;
                }
                let scope = match inspect_row(connection, stream, &meta, &mut state) {
                    Ok(scope) => scope,
                    Err(Stop::Database(error)) => return Err(error),
                    Err(Stop::Limit(reason)) => {
                        return Ok(incomplete(
                            decisions,
                            Some(stream),
                            after,
                            reason,
                            state.stats,
                        ))
                    }
                };
                if let Some(scope) = scope {
                    let coordinate = Arc::new(LegacySpawnCoordinate {
                        operation_id: meta.operation_id.clone(),
                        team_id: meta.team_id,
                        stage: meta.stage,
                        evidence_problem: scope.evidence_problem.map(str::to_owned),
                    });
                    for (target, decision) in targets.iter().zip(decisions.iter_mut()) {
                        if !matches!(decision, LegacyTargetDecision::Clear) {
                            continue;
                        }
                        if let Some(reason) = matches_target(&scope, target) {
                            *decision = LegacyTargetDecision::Blocked {
                                coordinate: coordinate.clone(),
                                reason,
                            };
                        }
                    }
                }
                after = Some(meta.operation_id);
                if decisions
                    .iter()
                    .all(|d| matches!(d, LegacyTargetDecision::Blocked { .. }))
                {
                    return Ok(LegacySpawnScan {
                        decisions,
                        termination: LegacyScanTermination::AllTargetsBlocked,
                        stats: state.stats,
                    });
                }
            }
            if exhausted {
                break;
            }
        }
    }
    Ok(LegacySpawnScan {
        decisions,
        termination: LegacyScanTermination::ExhaustedBothStreams,
        stats: state.stats,
    })
}

fn incomplete(
    mut decisions: Vec<LegacyTargetDecision>,
    stream: Option<LegacySpawnStream>,
    after_operation_id: Option<String>,
    reason: LegacyScanLimit,
    stats: LegacySpawnScanStats,
) -> LegacySpawnScan {
    let problem = Arc::new(LegacyScanIncomplete {
        stream,
        after_operation_id,
        reason,
        stats,
    });
    for decision in &mut decisions {
        if matches!(decision, LegacyTargetDecision::Clear) {
            *decision = LegacyTargetDecision::ScanIncomplete(problem.clone());
        }
    }
    LegacySpawnScan {
        decisions,
        termination: LegacyScanTermination::Incomplete(problem),
        stats,
    }
}
fn valid_id(value: &str) -> bool {
    !value.is_empty()
        && value.trim() == value
        && value.len() <= METADATA_CELL_BYTES
        && !value.chars().any(char::is_control)
}
fn targets_within_budget(targets: &[LegacyNativeTarget]) -> bool {
    if targets.len() > TARGET_COUNT_LIMIT {
        return false;
    }
    let mut total = 0usize;
    for target in targets {
        let parts: Vec<&str> = match target {
            LegacyNativeTarget::Session(id) if valid_id(id) => vec![id.as_str()],
            LegacyNativeTarget::Session(_) => return false,
            LegacyNativeTarget::Checkout {
                worktree_id,
                checkout_path,
                repository_root,
                branch_name,
            } => {
                let parts = [worktree_id, checkout_path, repository_root, branch_name]
                    .into_iter()
                    .filter_map(|value| value.as_deref())
                    .collect::<Vec<_>>();
                if parts.is_empty()
                    || parts
                        .iter()
                        .any(|value| value.is_empty() || value.contains('\0'))
                {
                    return false;
                }
                parts
            }
        };
        for part in parts {
            let Some(next) = total.checked_add(part.len()) else {
                return false;
            };
            total = next;
            if total > TARGET_BYTES_LIMIT {
                return false;
            }
        }
    }
    true
}

fn cursor_clause(stream: LegacySpawnStream, after: bool) -> String {
    match (stream, after) {
        (LegacySpawnStream::KeyedAdmissions, false) => String::new(),
        (LegacySpawnStream::KeyedAdmissions, true) => "WHERE a.operation_id > ?1".into(),
        (LegacySpawnStream::SpawnJournal, false) => format!("WHERE j.kind='{KIND}'"),
        (LegacySpawnStream::SpawnJournal, true) => {
            format!("WHERE j.kind='{KIND}' AND j.operation_id > ?1")
        }
    }
}
pub(crate) fn stream_has_more_sql(stream: LegacySpawnStream, has_cursor: bool) -> String {
    let table = match stream {
        LegacySpawnStream::KeyedAdmissions => "team_spawn_admissions a",
        LegacySpawnStream::SpawnJournal => "team_operation_journal j",
    };
    format!(
        "SELECT EXISTS(SELECT 1 FROM {table} {} LIMIT 1)",
        cursor_clause(stream, has_cursor)
    )
}
fn stream_has_more(
    connection: &Connection,
    stream: LegacySpawnStream,
    after: Option<&str>,
) -> Result<bool, RuntimeError> {
    let sql = stream_has_more_sql(stream, after.is_some());
    let result = if let Some(after) = after {
        connection.query_row(&sql, [after], |row| row.get(0))
    } else {
        connection.query_row(&sql, [], |row| row.get(0))
    };
    result.map_err(|error| db_error("failed checking bounded legacy spawn stream", error))
}
fn bounded_text_sql(column: &str) -> String {
    // Constant column identifiers only. CASE guards bytes before Rust copies.
    format!(
        "typeof({column}), length(CAST({column} AS BLOB)),
        CASE WHEN typeof({column})='text'
             AND length(CAST({column} AS BLOB))<={METADATA_CELL_BYTES}
        THEN CAST({column} AS BLOB) END"
    )
}
pub(crate) fn metadata_page_sql(stream: LegacySpawnStream, has_cursor: bool) -> String {
    let (columns, from, order, flags) = match stream {
        LegacySpawnStream::KeyedAdmissions => (
            ["a.operation_id", "a.team_id", "j.team_id", "j.kind", "j.stage"],
            "team_spawn_admissions a LEFT JOIN team_operation_journal j ON j.operation_id=a.operation_id",
            "a.operation_id", "j.rowid IS NOT NULL, 0",
        ),
        LegacySpawnStream::SpawnJournal => (
            ["j.operation_id", "j.team_id", "j.team_id", "j.kind", "j.stage"],
            "team_operation_journal j LEFT JOIN team_spawn_admissions a ON a.operation_id=j.operation_id",
            "j.operation_id", "1, a.operation_id IS NOT NULL",
        ),
    };
    let projection = columns
        .into_iter()
        .map(bounded_text_sql)
        .collect::<Vec<_>>()
        .join(",");
    let placeholder = if has_cursor { "?2" } else { "?1" };
    format!(
        "SELECT {projection}, {flags} FROM {from} {} ORDER BY {order} LIMIT {placeholder}",
        cursor_clause(stream, has_cursor)
    )
}
fn metadata_page(
    connection: &Connection,
    stream: LegacySpawnStream,
    after: Option<&str>,
    limit: usize,
    state: &mut ScanState,
) -> ScanResult<Vec<Metadata>> {
    let sql = metadata_page_sql(stream, after.is_some());
    let mut statement = connection
        .prepare(&sql)
        .map_err(|error| Stop::Database(db_error("failed preparing legacy spawn page", error)))?;
    let limit = i64::try_from(limit).map_err(|_| Stop::Limit(LegacyScanLimit::RowBudget))?;
    let mut rows = if let Some(after) = after {
        statement.query(params![after, limit])
    } else {
        statement.query([limit])
    }
    .map_err(|error| Stop::Database(db_error("failed reading legacy spawn page", error)))?;
    state.stats.pages_read += 1;
    let mut output = Vec::new();
    while let Some(row) = rows
        .next()
        .map_err(|error| Stop::Database(db_error("failed advancing legacy spawn page", error)))?
    {
        state.stats.source_rows_read += 1;
        let operation_id = read_metadata_cell(row, 0, state)?
            .filter(|value| valid_id(value))
            .ok_or(Stop::Limit(LegacyScanLimit::InvalidMetadata))?;
        let team_id = read_metadata_cell(row, 3, state)?
            .filter(|value| valid_id(value))
            .ok_or(Stop::Limit(LegacyScanLimit::InvalidMetadata))?;
        output.push(Metadata {
            operation_id,
            team_id,
            journal_team_id: read_metadata_cell(row, 6, state)?,
            kind: read_metadata_cell(row, 9, state)?,
            stage: read_metadata_cell(row, 12, state)?,
            journal_present: row.get(15).map_err(|error| {
                Stop::Database(db_error("failed reading journal presence", error))
            })?,
            already_keyed: row.get(16).map_err(|error| {
                Stop::Database(db_error("failed reading admission presence", error))
            })?,
        });
    }
    Ok(output)
}
fn read_metadata_cell(
    row: &Row<'_>,
    index: usize,
    state: &mut ScanState,
) -> ScanResult<Option<String>> {
    let kind: String = row
        .get(index)
        .map_err(|error| Stop::Database(db_error("failed reading metadata type", error)))?;
    if kind == "null" {
        return Ok(None);
    }
    if kind != "text" {
        return Err(Stop::Limit(LegacyScanLimit::InvalidMetadata));
    }
    let length: i64 = row
        .get(index + 1)
        .map_err(|error| Stop::Database(db_error("failed reading metadata size", error)))?;
    let length =
        usize::try_from(length).map_err(|_| Stop::Limit(LegacyScanLimit::InvalidMetadata))?;
    if length > METADATA_CELL_BYTES {
        return Err(Stop::Limit(LegacyScanLimit::MetadataCellLimit));
    }
    if length > state.remaining_bytes() {
        return Err(Stop::Limit(LegacyScanLimit::ByteBudget));
    }
    let raw: Vec<u8> = row
        .get(index + 2)
        .map_err(|error| Stop::Database(db_error("failed reading bounded metadata", error)))?;
    state.charge(raw.len())?;
    String::from_utf8(raw)
        .map(Some)
        .map_err(|_| Stop::Limit(LegacyScanLimit::InvalidMetadata))
}
fn load_json(
    connection: &Connection,
    operation_id: &str,
    receipt: bool,
    state: &mut ScanState,
) -> ScanResult<RawJson> {
    let (table, column) = if receipt {
        ("team_spawn_admissions", "completed_response_json")
    } else {
        ("team_operation_journal", "payload_json")
    };
    let sql = format!(
        "SELECT typeof({column}), length(CAST({column} AS BLOB)),
         CASE WHEN typeof({column})='text'
              AND length(CAST({column} AS BLOB))<=?2
              AND length(CAST({column} AS BLOB))<=?3
         THEN CAST({column} AS BLOB) END
         FROM {table} WHERE operation_id=?1"
    );
    let remaining = i64::try_from(state.remaining_bytes()).unwrap_or(i64::MAX);
    let raw: Option<(String, Option<i64>, Option<Vec<u8>>)> = connection
        .query_row(
            &sql,
            params![operation_id, JSON_CELL_BYTES as i64, remaining],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .optional()
        .map_err(|error| {
            Stop::Database(db_error("failed loading bounded legacy evidence", error))
        })?;
    let Some((kind, length, bytes)) = raw else {
        return Ok(RawJson::Absent);
    };
    if kind == "null" {
        return Ok(RawJson::Absent);
    }
    if kind != "text" {
        return Ok(RawJson::NonText);
    }
    let length = length
        .and_then(|value| usize::try_from(value).ok())
        .ok_or(Stop::Limit(LegacyScanLimit::InvalidMetadata))?;
    if length > JSON_CELL_BYTES {
        return Err(Stop::Limit(LegacyScanLimit::PayloadCellLimit));
    }
    if length > state.remaining_bytes() {
        return Err(Stop::Limit(LegacyScanLimit::ByteBudget));
    }
    let bytes = bytes.ok_or(Stop::Limit(LegacyScanLimit::InvalidMetadata))?;
    state.charge(bytes.len())?;
    Ok(match String::from_utf8(bytes) {
        Ok(text) => RawJson::Text(text),
        Err(_) => RawJson::InvalidUtf8,
    })
}
fn inspect_row(
    connection: &Connection,
    stream: LegacySpawnStream,
    meta: &Metadata,
    state: &mut ScanState,
) -> ScanResult<Option<Scope>> {
    if !meta.journal_present {
        return Ok(Some(Scope {
            evidence_problem: Some("journal_missing"),
            ..Scope::default()
        }));
    }
    if meta.journal_team_id.as_deref() != Some(meta.team_id.as_str())
        || meta.kind.as_deref() != Some(KIND)
    {
        return Ok(Some(Scope {
            evidence_problem: Some("journal_identity_mismatch"),
            ..Scope::default()
        }));
    }
    // Typed receipt first: corrupt or oversized irrelevant journal payload
    // must not defeat a saved completion certified by the existing helper.
    if stream == LegacySpawnStream::KeyedAdmissions && meta.stage.as_deref() == Some("completed") {
        if let RawJson::Text(response) = load_json(connection, &meta.operation_id, true, state)? {
            if certified_spawn_response(
                &meta.operation_id,
                &meta.team_id,
                Some(&response),
                meta.stage.as_deref(),
                meta.journal_team_id.as_deref(),
                meta.kind.as_deref(),
            )
            .is_some()
            {
                state.stats.certified_receipts += 1;
                return Ok(None);
            }
        }
    }
    let raw = load_json(connection, &meta.operation_id, false, state)?;
    Ok(Some(decode_scope(meta.stage.as_deref(), raw)))
}

#[derive(Deserialize)]
struct NestedChildProjection {
    #[serde(default)]
    spawned_session_id: Option<String>,
}
#[derive(Deserialize)]
struct ChildProjection {
    spawned_session_id: String,
    #[serde(default)]
    payload: Option<NestedChildProjection>,
}
#[derive(Deserialize)]
struct WorktreeProjection {
    worktree: ManagedWorktreeRecord,
}

fn decode_scope(stage: Option<&str>, raw: RawJson) -> Scope {
    let text = match raw {
        RawJson::Text(text) => text,
        RawJson::Absent => {
            return Scope {
                evidence_problem: Some("payload_missing"),
                ..Scope::default()
            }
        }
        RawJson::NonText => {
            return Scope {
                evidence_problem: Some("payload_not_text"),
                ..Scope::default()
            }
        }
        RawJson::InvalidUtf8 => {
            return Scope {
                evidence_problem: Some("payload_invalid_utf8"),
                ..Scope::default()
            }
        }
    };
    let value: serde_json::Value = match serde_json::from_str(&text) {
        Ok(serde_json::Value::Object(object)) => serde_json::Value::Object(object),
        _ => {
            return Scope {
                evidence_problem: Some("payload_malformed_or_not_object"),
                ..Scope::default()
            }
        }
    };
    let mut scope = Scope::default();
    // source_session_id and created_by_session_id are NOT child identities.
    for object in [Some(&value), value.get("payload")].into_iter().flatten() {
        if let Some(id) = object
            .get("spawned_session_id")
            .and_then(|value| value.as_str())
            .filter(|id| valid_id(id))
        {
            scope.positive_children.insert(id.to_string());
        }
        for field in ["worktree_id", "assigned_worktree_id", "created_worktree_id"] {
            if let Some(id) = object
                .get(field)
                .and_then(|value| value.as_str())
                .filter(|id| valid_id(id))
            {
                scope.positive_worktrees.insert(id.to_string());
            }
        }
    }
    if matches!(
        stage,
        Some(
            "session_created"
                | "team_joined"
                | "onboarding_sent"
                | "completed"
                | "rollback_pending"
                | "rollback_review_required"
                | "rolled_back"
        )
    ) {
        // Derived projections reject duplicate relevant keys at both levels;
        // Value alone would use last-key-wins and invent disjointness.
        if let Ok(child) = serde_json::from_str::<ChildProjection>(&text) {
            let nested_agrees = child
                .payload
                .as_ref()
                .and_then(|payload| payload.spawned_session_id.as_deref())
                .is_none_or(|id| id == child.spawned_session_id);
            if valid_id(&child.spawned_session_id)
                && nested_agrees
                && scope
                    .positive_children
                    .iter()
                    .all(|id| id == &child.spawned_session_id)
            {
                scope.complete_child = Some(child.spawned_session_id);
            }
        }
    }
    if stage == Some("worktree_created") {
        if let Ok(saved) = serde_json::from_str::<WorktreeProjection>(&text) {
            let record = saved.worktree;
            if valid_id(&record.id)
                && [&record.repo_root, &record.worktree_cwd, &record.branch_name]
                    .into_iter()
                    .all(|value| !value.is_empty() && !value.contains('\0'))
            {
                scope.positive_worktrees.insert(record.id.clone());
                scope.checkout_snapshot = Some(record);
            }
        }
    }
    if scope.complete_child.is_none() && scope.checkout_snapshot.is_none() {
        scope.evidence_problem = Some("stage_has_no_complete_target_axis");
    }
    scope
}
fn matches_target(scope: &Scope, target: &LegacyNativeTarget) -> Option<LegacyTargetBlockReason> {
    use LegacyTargetBlockReason as Reason;
    match target {
        LegacyNativeTarget::Session(id) => {
            if scope.positive_children.contains(id) {
                return Some(Reason::ExactSession);
            }
            if scope
                .complete_child
                .as_ref()
                .is_some_and(|child| child != id)
            {
                return None;
            }
            Some(Reason::UnknownSessionScope)
        }
        LegacyNativeTarget::Checkout {
            worktree_id,
            checkout_path,
            repository_root,
            branch_name,
        } => {
            if worktree_id
                .as_ref()
                .is_some_and(|id| scope.positive_worktrees.contains(id))
            {
                return Some(Reason::ExactWorktreeId);
            }
            let Some(saved) = scope.checkout_snapshot.as_ref() else {
                return Some(Reason::UnknownCheckoutScope);
            };
            if checkout_path.as_ref().is_some_and(|path| {
                Path::new(path).starts_with(&saved.worktree_cwd)
                    || Path::new(&saved.worktree_cwd).starts_with(path)
            }) {
                return Some(Reason::OverlappingCheckoutSpelling);
            }
            if repository_root.as_deref() == Some(saved.repo_root.as_str())
                && branch_name.as_deref() == Some(saved.branch_name.as_str())
            {
                return Some(Reason::ExactRepositoryBranch);
            }
            // Different raw paths/IDs or present-day canonical paths cannot
            // prove what a historical symlink referenced at native dispatch.
            Some(Reason::HistoricalPathAliasUncertain)
        }
    }
}

impl SqliteRuntimeRepository {
    /// One read snapshot for runtime preflight, with no durable clear cache.
    pub fn classify_legacy_spawn_obligations(
        &self,
        targets: &[LegacyNativeTarget],
        budget: LegacySpawnScanBudget,
    ) -> Result<LegacySpawnScan, RuntimeError> {
        let mut connection = open_connection(&self.database_path)?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Deferred)
            .map_err(|error| {
                db_error("failed beginning legacy native preflight snapshot", error)
            })?;
        let scan = classify_legacy_spawn_obligations(&transaction, targets, budget)?;
        transaction.commit().map_err(|error| {
            db_error("failed completing legacy native preflight snapshot", error)
        })?;
        Ok(scan)
    }
}

pub(crate) fn guard_legacy_native_targets(
    connection: &Connection,
    targets: &[LegacyNativeTarget],
) -> Result<(), RuntimeError> {
    classify_legacy_spawn_obligations(connection, targets, LegacySpawnScanBudget::default())?
        .require_all_clear()
}

pub(crate) fn guard_legacy_native_session(
    connection: &Connection,
    session_id: &str,
) -> Result<(), RuntimeError> {
    guard_legacy_native_targets(
        connection,
        &[LegacyNativeTarget::Session(session_id.to_owned())],
    )
}
