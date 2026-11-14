use runtime_core::{
    LegacyWorkspaceMigrationStatus, LegacyWorkspaceMigrationSubjectKind, RuntimeError,
};
use rusqlite::{params, OptionalExtension, TransactionBehavior};

use crate::db::{collect_rows, db_error, open_connection};
use crate::repository_workspace_migration::migration_subject_from_row;
use crate::SqliteRuntimeRepository;

const SUBJECT_KINDS: [LegacyWorkspaceMigrationSubjectKind; 5] = [
    LegacyWorkspaceMigrationSubjectKind::Session,
    LegacyWorkspaceMigrationSubjectKind::Team,
    LegacyWorkspaceMigrationSubjectKind::TeamMember,
    LegacyWorkspaceMigrationSubjectKind::ManagedWorktree,
    LegacyWorkspaceMigrationSubjectKind::WorktreeClaim,
];

impl SqliteRuntimeRepository {
    /// Persisted migration diagnostics: one SQLite read snapshot, indexed
    /// subject-kind keysets and index-only global totals. Never deserialize
    /// the full historical evidence collection just to return one HTTP page.
    /// None means the exact cursor row no longer belongs to this collection.
    pub fn workspace_migration_status_page(
        &self,
        after: Option<(LegacyWorkspaceMigrationSubjectKind, String)>,
        limit: usize,
    ) -> Result<Option<LegacyWorkspaceMigrationStatus>, RuntimeError> {
        let mut connection = open_connection(&self.database_path)?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Deferred)
            .map_err(|error| db_error("failed starting migration status read snapshot", error))?;

        if let Some((kind, id)) = &after {
            let present = transaction
                .query_row(
                    "SELECT 1 FROM legacy_workspace_migration_subjects
                     WHERE subject_kind = ?1 AND subject_id = ?2",
                    params![kind.as_str(), id],
                    |_| Ok(()),
                )
                .optional()
                .map_err(|error| db_error("failed validating migration page cursor", error))?
                .is_some();
            if !present {
                return Ok(None);
            }
        }

        // Classification/kind are both in the existing covering index. A
        // dozen-or-so groups replace full evidence JSON hydration while
        // preserving totals for the entire collection after the page cursor.
        let mut mapped_subjects = 0usize;
        let mut archived_subjects = 0usize;
        let mut unresolved_subjects = 0usize;
        let mut unresolved_active_sessions = 0usize;
        {
            let mut statement = transaction
                .prepare(
                    "SELECT classification, subject_kind, COUNT(*)
                     FROM legacy_workspace_migration_subjects
                     GROUP BY classification, subject_kind",
                )
                .map_err(|error| db_error("failed preparing migration status totals", error))?;
            let rows = statement
                .query_map([], |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, i64>(2)?,
                    ))
                })
                .map_err(|error| db_error("failed querying migration status totals", error))?;
            for row in rows {
                let (classification, kind, count) =
                    row.map_err(|error| db_error("failed reading migration status totals", error))?;
                if LegacyWorkspaceMigrationSubjectKind::from_str(&kind).is_none() {
                    return Err(RuntimeError::Bootstrap(format!(
                        "invalid migration subject kind {kind:?}"
                    )));
                }
                let count = usize::try_from(count).map_err(|_| {
                    RuntimeError::Bootstrap("invalid migration subject count".into())
                })?;
                match classification.as_str() {
                    "mapped" => mapped_subjects += count,
                    "archived_history" => archived_subjects += count,
                    "unresolved" => {
                        unresolved_subjects += count;
                        if kind == "session" {
                            unresolved_active_sessions += count;
                        }
                    }
                    _ => {
                        return Err(RuntimeError::Bootstrap(format!(
                            "invalid migration classification {classification:?}"
                        )));
                    }
                }
            }
        }

        // Kind ordering is the Rust enum order, NOT SQLite text collation
        // (which puts managed_worktree before session). Seek each kind in
        // the primary key's subject_id order, reading at most limit rows in
        // total with no global sort, offset or historical evidence scan.
        let mut subjects = Vec::with_capacity(limit.min(101));
        for kind in SUBJECT_KINDS {
            if subjects.len() >= limit {
                break;
            }
            let seek_id = match &after {
                Some((cursor_kind, _)) if kind < *cursor_kind => continue,
                Some((cursor_kind, id)) if kind == *cursor_kind => id.as_str(),
                _ => "",
            };
            let mut statement = transaction
                .prepare(
                    "SELECT subject_kind, subject_id, classification, workspace_id,
                            canonical_root, git_common_dir, repository_fingerprint,
                            reason_code, evidence_json, resolution_source, applied_at, updated_at
                     FROM legacy_workspace_migration_subjects
                     WHERE subject_kind = ?1 AND subject_id > ?2
                     ORDER BY subject_id LIMIT ?3",
                )
                .map_err(|error| db_error("failed preparing migration subject page", error))?;
            let rows = statement
                .query_map(
                    params![kind.as_str(), seek_id, (limit - subjects.len()) as i64],
                    migration_subject_from_row,
                )
                .map_err(|error| db_error("failed querying migration subject page", error))?;
            subjects.extend(collect_rows(rows)?);
        }

        transaction
            .commit()
            .map_err(|error| db_error("failed closing migration status read snapshot", error))?;
        Ok(Some(LegacyWorkspaceMigrationStatus {
            subjects,
            mapped_subjects,
            archived_subjects,
            unresolved_subjects,
            unresolved_active_sessions,
            cutover_blocked: unresolved_subjects > 0,
        }))
    }
}
