use super::*;

#[test]
fn signed_hook_attempt_ceiling_rolls_back_claim_before_any_shell_dispatch() {
    let (_temp, repo) = fixture();
    approve(&repo, 0, vec![hook()]);
    repo.admit_team_hook_effect(&admission()).unwrap();
    let db = open_connection(&repo.database_path).unwrap();
    db.execute(
        "UPDATE workspace_hook_effects SET attempt_count=?1
         WHERE workspace_id='ws_one' AND parent_event_id='evt_one'
           AND event_name='post_teammate_add' AND hook_name='bootstrap'",
        [i64::MAX],
    )
    .unwrap();
    assert!(repo
        .claim_team_hook_effect(
            "ws_one",
            "evt_one",
            TeamHookEventName::PostTeammateAdd,
            "bootstrap",
            100,
        )
        .is_err());
    let (attempt, kind, phase): (i64, String, String) = db
        .query_row(
            "SELECT attempt_count,typeof(attempt_count),phase FROM workspace_hook_effects
         WHERE workspace_id='ws_one' AND parent_event_id='evt_one'
           AND event_name='post_teammate_add' AND hook_name='bootstrap'",
            [],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .unwrap();
    assert_eq!(
        (attempt, kind, phase),
        (i64::MAX, "integer".into(), "intended".into())
    );
    // This fabricated signed-MAX value exceeds the public u32 domain;
    // reading it is deliberately rejected rather than coerced.

    // The public hook attempt counter is u32, tighter than SQLite's i64.
    // An in-range signed INTEGER at u32::MAX must also be rejected before
    // shell dispatch if it cannot round-trip the +1 into the public model.
    db.execute(
        "UPDATE workspace_hook_effects SET attempt_count=?1
         WHERE workspace_id='ws_one' AND parent_event_id='evt_one'
           AND event_name='post_teammate_add' AND hook_name='bootstrap'",
        [i64::from(u32::MAX)],
    )
    .unwrap();
    assert!(repo
        .claim_team_hook_effect(
            "ws_one",
            "evt_one",
            TeamHookEventName::PostTeammateAdd,
            "bootstrap",
            100,
        )
        .is_err());
    let (attempt, kind, phase): (i64, String, String) = db
        .query_row(
            "SELECT attempt_count,typeof(attempt_count),phase FROM workspace_hook_effects
         WHERE workspace_id='ws_one' AND parent_event_id='evt_one'
           AND event_name='post_teammate_add' AND hook_name='bootstrap'",
            [],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .unwrap();
    assert_eq!(
        (attempt, kind, phase),
        (i64::from(u32::MAX), "integer".into(), "intended".into())
    );

    db.execute(
        "UPDATE workspace_hook_effects SET attempt_count=?1
         WHERE workspace_id='ws_one' AND parent_event_id='evt_one'
           AND event_name='post_teammate_add' AND hook_name='bootstrap'",
        [i64::from(u32::MAX - 1)],
    )
    .unwrap();
    let claimed = repo
        .claim_team_hook_effect(
            "ws_one",
            "evt_one",
            TeamHookEventName::PostTeammateAdd,
            "bootstrap",
            101,
        )
        .unwrap();
    assert_eq!(claimed.attempt_count, u32::MAX);
    assert_eq!(claimed.phase, TeamHookEffectPhase::Started);
    let (attempt, kind): (i64, String) = db
        .query_row(
            "SELECT attempt_count,typeof(attempt_count) FROM workspace_hook_effects
         WHERE workspace_id='ws_one' AND parent_event_id='evt_one'
           AND event_name='post_teammate_add' AND hook_name='bootstrap'",
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap();
    assert_eq!((attempt, kind), (i64::from(u32::MAX), "integer".into()));
}
