use super::support::*;
use crate::db::open_connection;
use crate::tests::workspace_agent::workspace_agent_fixture;
use runtime_core::RuntimeError;
use rusqlite::params;

#[test]
fn exact_startup_replay_cannot_follow_an_effect_grant_to_a_different_operation() {
    for other_completed in [false, true] {
        let fixture = Fixture::new(true);
        let original_command = fixture.command("startup_replay_original", "original_instance");
        let original = execute(&fixture.repository, &original_command, 300);
        let (other_session, other_agent) = workspace_agent_fixture(
            &fixture.workspace,
            "startup_replay_other_session",
            "startup-replay-other-heron",
        );
        fixture
            .repository
            .create_workspace_agent(&other_session, &other_agent)
            .unwrap();
        let other_command = command(
            &fixture.repository,
            "startup_replay_other_operation",
            "other_instance",
            &other_session.id,
        );
        let other = execute(&fixture.repository, &other_command, 301);
        if other_completed {
            fixture
                .repository
                .complete_startup_reattach(&other, &ack(&other, false), 400)
                .unwrap();
        }
        assert_attempt(&fixture.repository, &original, "requested", "started", true);
        let db = open_connection(&fixture.repository.database_path).unwrap();
        // Keep A's command, normalized intent, operation row, effect identity and
        // claim intact. Only the embedded grant is replaced by B's genuine grant.
        db.execute(
            "UPDATE runtime_operation_effects
             SET evidence_json=json_set(evidence_json,'$.grant',json(?2))
             WHERE operation_id=?1",
            params![
                original_command.operation_id,
                serde_json::to_string(&other).unwrap()
            ],
        )
        .unwrap();
        let before = snapshot(&fixture.repository);
        let result = fixture
            .repository
            .admit_startup_reattach(&original_command, 500);
        assert!(
            matches!(result, Err(RuntimeError::Conflict(_))),
            "A's exact attempt replay followed B's embedded grant: {result:?}"
        );
        assert_eq!(snapshot(&fixture.repository), before);
        let original_owner: (String, u64, i64) = db
            .query_row(
                "SELECT owner_operation_id,fence_generation,acquired_at
             FROM runtime_operation_resource_claims WHERE resource_kind=?1 AND resource_id=?2",
                params![RESUME_CLAIM, original_command.session_id],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .unwrap();
        assert_eq!(
            original_owner,
            (
                original.claim.owner_operation_id.clone(),
                original.claim.fence_generation,
                original.claim.acquired_at,
            )
        );
        assert_attempt(
            &fixture.repository,
            &other,
            if other_completed {
                "completed"
            } else {
                "requested"
            },
            if other_completed {
                "finalized"
            } else {
                "started"
            },
            !other_completed,
        );
    }
}
