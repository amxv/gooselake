use super::legacy_spawn_history::HistoricalWorktreeAssignment;
use super::worktree_spawn_recovery::{fixture, worktree_input};
use super::*;
use runtime_core::WorktreeService;

#[tokio::test]
async fn prepared_git_history_replays_from_reopened_sqlite_without_provider_or_git_effects() {
    for created in [true, false] {
        let fixture = fixture().await;
        fixture.init_git();
        let mut request = fixture.keyed_request(if created {
            "created-history-once"
        } else {
            "reused-history-once"
        });
        request.worktree = Some(worktree_input(
            if created { "create" } else { "reuse" },
            "original-checkout",
        ));
        let record = fixture.prepared_checkout(&request, created);
        let assignment = if created {
            HistoricalWorktreeAssignment::Created(record.clone())
        } else {
            HistoricalWorktreeAssignment::Reused(record.clone())
        };
        let seed = fixture.onboarding_history(&request, Some(assignment));
        let first = &seed.response;
        let provider_before = fixture
            .provider
            .effect_calls
            .load(std::sync::atomic::Ordering::SeqCst);
        let git_before = fixture.git(&["worktree", "list", "--porcelain"]);
        assert!(String::from_utf8_lossy(&git_before).contains(&record.worktree_cwd));
        let reopened = Arc::new(SqliteRuntimeStore::new(SqliteStoreConfig {
            database_path: fixture.database_path.clone(),
        }));
        let connection = rusqlite::Connection::open(&fixture.database_path).unwrap();
        // Corrupt one item before the FIRST completion. The real validator
        // must refuse every mismatched claim, route and creation provenance.
        connection.execute("UPDATE managed_worktree_claims SET released_at=1234 WHERE worktree_id=?1 AND session_id=?2",
            rusqlite::params![record.id,first.spawned_session.id]).unwrap();
        assert!(matches!(
            reopened.complete_team_member_spawn(&first.operation_id, first, 2000),
            Err(RuntimeError::Conflict(_))
        ));
        connection.execute("UPDATE managed_worktree_claims SET released_at=NULL WHERE worktree_id=?1 AND session_id=?2",
            rusqlite::params![record.id,first.spawned_session.id]).unwrap();
        let original_role = if created { "owner" } else { "consumer" };
        connection.execute("UPDATE managed_worktree_claims SET claim_role=?3 WHERE worktree_id=?1 AND session_id=?2",
            rusqlite::params![record.id,first.spawned_session.id,if created {"consumer"} else {"owner"}]).unwrap();
        assert!(matches!(
            reopened.complete_team_member_spawn(&first.operation_id, first, 2000),
            Err(RuntimeError::Conflict(_))
        ));
        connection.execute("UPDATE managed_worktree_claims SET claim_role=?3 WHERE worktree_id=?1 AND session_id=?2",
            rusqlite::params![record.id,first.spawned_session.id,original_role]).unwrap();
        for (column, original) in [
            ("repo_root", &record.repo_root),
            ("worktree_cwd", &record.worktree_cwd),
            ("branch_name", &record.branch_name),
        ] {
            connection
                .execute(
                    &format!("UPDATE managed_worktrees SET {column}='foreign-route' WHERE id=?1"),
                    [&record.id],
                )
                .unwrap();
            assert!(
                matches!(
                    reopened.complete_team_member_spawn(&first.operation_id, first, 2000),
                    Err(RuntimeError::Conflict(_))
                ),
                "{column}"
            );
            connection
                .execute(
                    &format!("UPDATE managed_worktrees SET {column}=?2 WHERE id=?1"),
                    rusqlite::params![record.id, original],
                )
                .unwrap();
        }
        if created {
            connection.execute("UPDATE managed_worktrees SET created_by_operation_id='foreign-operation' WHERE id=?1",[&record.id]).unwrap();
            assert!(matches!(
                reopened.complete_team_member_spawn(&first.operation_id, first, 2000),
                Err(RuntimeError::Conflict(_))
            ));
            connection
                .execute(
                    "UPDATE managed_worktrees SET created_by_operation_id=?2 WHERE id=?1",
                    rusqlite::params![record.id, first.operation_id],
                )
                .unwrap();
        } else {
            assert_ne!(
                record.created_by_operation_id.as_deref(),
                Some(first.operation_id.as_str())
            );
        }
        let mut forged = first.clone();
        forged.worktree_created_by_operation = !created;
        assert!(matches!(
            reopened.complete_team_member_spawn(&first.operation_id, &forged, 2000),
            Err(RuntimeError::Conflict(_))
        ));
        assert_eq!(fixture.fresh_journal().stage, "onboarding_sent");
        seed.complete_seed(reopened.as_ref(), 2000).unwrap();
        assert_eq!(fixture.fresh_journal().stage, "completed");
        let mut registry = ProviderRegistry::new();
        registry.register(fixture.provider.clone()).unwrap();
        let runtime = Arc::new(
            RuntimeSessionManager::new(reopened.clone(), Arc::new(registry), 512).unwrap(),
        );
        let comms = RuntimeTeamCommsService::new(
            reopened.clone(),
            runtime.clone(),
            RuntimeTeamCommsConfig {
                enabled: true,
                max_pending_deliveries: 1000,
            },
        )
        .unwrap();
        let worker = RuntimeWorktreeService::new(
            reopened.clone(),
            runtime,
            comms,
            fixture.service.config.clone(),
        )
        .unwrap();
        let before = reopened.hydrate_runtime_state().unwrap();
        let replay = worker.spawn_team_member(request).await.unwrap();
        assert_eq!(
            serde_json::to_value(&replay).unwrap(),
            serde_json::to_value(first).unwrap()
        );
        assert_eq!(reopened.hydrate_runtime_state().unwrap(), before);
        assert_eq!(
            reopened.get_managed_worktree(&record.id).unwrap(),
            Some(record)
        );
        assert_eq!(
            fixture.git(&["worktree", "list", "--porcelain"]),
            git_before
        );
        assert_eq!(
            fixture
                .provider
                .effect_calls
                .load(std::sync::atomic::Ordering::SeqCst),
            provider_before
        );
        assert_eq!(fixture.provider.state.lock().await.sessions.len(), 1);
    }
}
