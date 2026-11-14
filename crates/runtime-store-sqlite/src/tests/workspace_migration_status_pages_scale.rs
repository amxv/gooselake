use super::*;
use runtime_core::LegacyWorkspaceMigrationSubjectKind as Kind;

#[test]
fn persisted_migration_status_reads_indexed_pages_with_complete_totals_and_exact_cursors() {
    let temp = tempfile::tempdir().expect("private database fixture");
    let repository = repo(&temp);
    repository.initialize_schema().expect("schema 40");
    let path = temp.path().join("runtime.sqlite3");
    let mut connection = open_connection(&path).expect("disposable SQLite connection");
    let transaction = connection.transaction().expect("seed transaction");
    let kinds = [
        "session",
        "team",
        "team_member",
        "managed_worktree",
        "worktree_claim",
    ];
    let evidence = serde_json::json!({ "private": "non-page evidence ".repeat(60) });
    // Multiple categories, different classifications and reverse insertion
    // make a globally sorted in-memory answer a poor substitute for the PK.
    for number in (0..2_700).rev() {
        let kind = kinds[number % kinds.len()];
        let classification = if number % 7 == 0 {
            "unresolved"
        } else if number % 3 == 0 {
            "archived_history"
        } else {
            "mapped"
        };
        transaction
            .execute(
                "INSERT INTO legacy_workspace_migration_subjects
                    (subject_kind, subject_id, classification, reason_code,
                     evidence_json, resolution_source, updated_at)
                 VALUES (?1, ?2, ?3, 'disposable_test', ?4, 'deterministic', 123)",
                params![
                    kind,
                    format!("subject_{number:05}"),
                    classification,
                    evidence.to_string()
                ],
            )
            .expect("seed disposable migration subject");
    }
    transaction.commit().expect("commit fixture");

    for (sql, parameters, expected_index) in [
        (
            "EXPLAIN QUERY PLAN SELECT subject_id
             FROM legacy_workspace_migration_subjects
             WHERE subject_kind=?1 AND subject_id>?2
             ORDER BY subject_id LIMIT ?3",
            vec!["session", "subject_01000", "39"],
            "sqlite_autoindex_legacy_workspace_migration_subjects_1",
        ),
        (
            "EXPLAIN QUERY PLAN SELECT classification,subject_kind,COUNT(*)
             FROM legacy_workspace_migration_subjects
             GROUP BY classification,subject_kind",
            vec![],
            "idx_legacy_workspace_migration_classification",
        ),
    ] {
        let detail = connection
            .prepare(sql)
            .expect("prepare query plan")
            .query_map(rusqlite::params_from_iter(parameters), |row| {
                row.get::<_, String>(3)
            })
            .expect("explain query")
            .collect::<Result<Vec<_>, _>>()
            .expect("collect plan")
            .join(" ");
        assert!(
            detail.contains(expected_index),
            "missing indexed seek: {detail}"
        );
        assert!(
            !detail.contains("USE TEMP B-TREE"),
            "unexpected sort: {detail}"
        );
    }
    drop(connection);

    let full = repository
        .workspace_migration_status()
        .expect("reference full status");
    assert_eq!(full.subjects.len(), 2_700);
    assert!(full.unresolved_active_sessions > 0);
    assert!(full.cutover_blocked);

    // This page contract is identical to the full in-memory status, while
    // each persisted page decodes at most 39 evidence JSON values.
    let mut all = Vec::new();
    let mut cursor = None;
    loop {
        let page = repository
            .workspace_migration_status_page(cursor.clone(), 39)
            .expect("persisted page")
            .expect("cursor belongs to collection");
        assert_eq!(page.mapped_subjects, full.mapped_subjects);
        assert_eq!(page.archived_subjects, full.archived_subjects);
        assert_eq!(page.unresolved_subjects, full.unresolved_subjects);
        assert_eq!(
            page.unresolved_active_sessions,
            full.unresolved_active_sessions
        );
        assert_eq!(page.cutover_blocked, full.cutover_blocked);
        let size = page.subjects.len();
        cursor = page
            .subjects
            .last()
            .map(|subject| (subject.subject_kind, subject.subject_id.clone()));
        all.extend(page.subjects);
        if size < 39 {
            break;
        }
    }
    assert_eq!(
        all, full.subjects,
        "no gaps, duplicates, or reordered kind boundaries"
    );

    let last_session = full
        .subjects
        .iter()
        .rfind(|subject| subject.subject_kind == Kind::Session)
        .expect("session boundary");
    let boundary = repository
        .workspace_migration_status_page(
            Some((last_session.subject_kind, last_session.subject_id.clone())),
            2,
        )
        .unwrap()
        .unwrap();
    assert_eq!(boundary.subjects[0].subject_kind, Kind::Team);
    assert_eq!(boundary.subjects.len(), 2);
    assert!(repository
        .workspace_migration_status_page(Some((Kind::Team, "not_a_subject".into())), 12)
        .unwrap()
        .is_none());
    assert!(repository
        .workspace_migration_status_page(
            Some((Kind::ManagedWorktree, last_session.subject_id.clone())),
            12,
        )
        .unwrap()
        .is_none());

    // Reopened short-lived connections see the same original opaque evidence.
    let reopened = repo(&temp);
    let first = reopened
        .workspace_migration_status_page(None, 50)
        .unwrap()
        .unwrap();
    assert_eq!(first.subjects, full.subjects[..50]);
    assert_eq!(first.unresolved_subjects, full.unresolved_subjects);
}
