use super::*;

const REMOVED: &str = "session_1";
const UNRELATED: &str = "session_2";

#[test]
fn removed_member_active_claims_seek_schema48_in_old_order_across_corrupt_history_and_restart() {
    let temp = tempfile::tempdir().unwrap();
    let repository = repo(&temp);
    repository.initialize_schema().unwrap();
    let target_session = sample_session();
    assert_eq!(target_session.id, REMOVED);
    repository.upsert_session(&target_session).unwrap();
    let mut foreign = sample_session();
    foreign.id = UNRELATED.into();
    foreign.provider_session_ref = Some("native-other-session".into());
    repository.upsert_session(&foreign).unwrap();

    let path = temp.path().join("runtime.sqlite3");
    let mut db = open_connection(&path).unwrap();
    let transaction = db.transaction().unwrap();
    for index in 0..2_400 {
        let id = format!("wt_history_{index:04}");
        transaction
            .execute(
                "INSERT INTO managed_worktrees (
               id,repo_root,worktree_root,worktree_cwd,branch_name,worktree_name,
               unified_workspace_path,deletion_policy,created_at,updated_at
             ) VALUES(?1,'/tmp/unrelated-removal-root','/tmp',?2,?3,?4,
                      '/tmp','retain_on_last_claim',?5,?5)",
                params![
                    id,
                    format!("/tmp/removal-{index:04}"),
                    format!("gg/removal-{index:04}"),
                    format!("removal-{index:04}"),
                    index as i64,
                ],
            )
            .unwrap();
        let removed_session_claim = matches!(index, 111 | 112 | 114 | 115);
        let created_at = if matches!(index, 111 | 112 | 114) {
            12_i64
        } else if index == 115 {
            5_i64
        } else {
            index as i64
        };
        transaction
            .execute(
                "INSERT INTO managed_worktree_claims (
               worktree_id,session_id,claim_role,created_at,released_at
             ) VALUES(?1,?2,'consumer',?3,?4)",
                params![
                    id,
                    if removed_session_claim {
                        REMOVED
                    } else {
                        UNRELATED
                    },
                    created_at,
                    if index == 114 { Some(99_i64) } else { None },
                ],
            )
            .unwrap();
    }
    transaction.commit().unwrap();
    let plan = db
        .prepare(
            "EXPLAIN QUERY PLAN
         SELECT worktree_id,session_id,claim_role,created_at,released_at
         FROM managed_worktree_claims
         WHERE session_id='session_1' AND released_at IS NULL
         ORDER BY created_at ASC,worktree_id ASC,session_id ASC",
        )
        .unwrap()
        .query_map([], |row| row.get::<_, String>(3))
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap()
        .join(" | ");
    assert!(
        plan.contains(
            "SEARCH managed_worktree_claims USING INDEX idx_worktree_claims_active_session_order"
        ),
        "{plan}"
    );
    assert!(!plan.contains("USE TEMP B-TREE"), "{plan}");

    let claims = repository
        .list_active_managed_worktree_claims_for_session(REMOVED)
        .unwrap();
    let names = claims
        .iter()
        .map(|claim| claim.worktree_id.as_str())
        .collect::<Vec<_>>();
    assert_eq!(
        names,
        ["wt_history_0115", "wt_history_0111", "wt_history_0112"]
    );
    assert!(claims
        .iter()
        .all(|claim| claim.session_id == REMOVED && claim.released_at.is_none()));
    assert!(repository
        .list_active_managed_worktree_claims_for_session("never-existed")
        .unwrap()
        .is_empty());
    let rows: i64 = db
        .query_row("SELECT COUNT(*) FROM managed_worktree_claims", [], |row| {
            row.get(0)
        })
        .unwrap();
    assert_eq!(rows, 2400);

    let pk_plan = db
        .prepare(
            "EXPLAIN QUERY PLAN
             SELECT worktree_id,session_id,claim_role,created_at,released_at
             FROM managed_worktree_claims
             WHERE worktree_id='wt_history_0111' AND session_id='session_1'",
        )
        .unwrap()
        .query_map([], |row| row.get::<_, String>(3))
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap()
        .join(" | ");
    assert!(
        pk_plan.contains(
            "SEARCH managed_worktree_claims USING INDEX sqlite_autoindex_managed_worktree_claims_1"
        ),
        "{pk_plan}"
    );
    let expected_claim = claims
        .iter()
        .find(|row| row.worktree_id == "wt_history_0111")
        .unwrap();
    assert_eq!(
        repository
            .get_managed_worktree_claim("wt_history_0111", REMOVED)
            .unwrap()
            .as_ref(),
        Some(expected_claim)
    );
    assert!(repository
        .get_managed_worktree_claim("wt_history_0111", UNRELATED)
        .unwrap()
        .is_none());

    db.execute(
        "UPDATE sessions SET metadata_json='{foreign-corrupted-session' WHERE id=?1",
        [UNRELATED],
    )
    .unwrap();
    assert!(repository.hydrate_runtime_state().is_err());
    let reopened = SqliteRuntimeRepository::new(path.clone());
    assert_eq!(
        reopened
            .list_active_managed_worktree_claims_for_session(REMOVED)
            .unwrap(),
        claims
    );
    let store = SqliteRuntimeStore::new(SqliteStoreConfig {
        database_path: path,
    });
    assert_eq!(
        store
            .list_active_managed_worktree_claims_for_session(REMOVED)
            .unwrap(),
        claims,
        "RuntimeStore method must delegate to indexed SQLite projection"
    );
    assert_eq!(
        store
            .get_managed_worktree_claim("wt_history_0111", REMOVED)
            .unwrap()
            .as_ref(),
        Some(expected_claim),
        "exact claim point read must survive unrelated corrupt session history"
    );
    assert!(reopened
        .get_managed_worktree_claim("wt_history_0111", UNRELATED)
        .unwrap()
        .is_none());
    assert!(store.hydrate_runtime_state().is_err());
    db.execute(
        "UPDATE managed_worktree_claims SET released_at=555
         WHERE worktree_id='wt_history_0111' AND session_id=?1",
        [REMOVED],
    )
    .unwrap();
    assert_eq!(
        reopened
            .list_active_managed_worktree_claims_for_session(REMOVED)
            .unwrap()
            .iter()
            .map(|claim| claim.worktree_id.as_str())
            .collect::<Vec<_>>(),
        vec!["wt_history_0115", "wt_history_0112"],
        "released claims must not be included in post-removal cleanup reads",
    );
    assert_eq!(
        db.query_row("SELECT COUNT(*) FROM managed_worktree_claims", [], |row| {
            row.get::<_, i64>(0)
        })
        .unwrap(),
        rows
    );
    assert!(reopened.hydrate_runtime_state().is_err());
    assert_eq!(
        reopened
            .get_managed_worktree_claim("wt_history_0111", REMOVED)
            .unwrap()
            .unwrap()
            .released_at,
        Some(555),
        "a released historical claim must remain visible to a pure existence preflight"
    );
}
