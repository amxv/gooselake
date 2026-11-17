//! Adversarial classification tests. No provisional error adapter is used.
use super::legacy_spawn_obligations_support::*;
use crate::repository_legacy_spawn_obligations::classify_legacy_spawn_obligations;
use runtime_core::legacy_spawn_obligations::{
    LegacyScanLimit as Limit, LegacyScanTermination, LegacySpawnScanBudget, LegacySpawnStream,
    LegacyTargetBlockReason as Reason, LegacyTargetDecision,
};

#[test]
fn scan_requires_caller_snapshot_and_empty_store_exhaustion_is_exact() {
    let mut f = Fixture::new();
    assert!(classify_legacy_spawn_obligations(
        &f.db,
        &[session("target")],
        LegacySpawnScanBudget::default()
    )
    .is_err());
    let empty = f.scan(&[session("target")], budget(0, 0));
    assert_clear(&empty);
    assert_eq!(empty.stats.source_rows_read, 0);
    assert_eq!(empty.stats.materialized_bytes, 0);
    journal(&f.db, "admission-only-target", TEAM, KIND, "planned", "{}");
    let capped = f.scan(&[session("target")], budget(0, 0));
    let problem = assert_incomplete(&capped, 0, Limit::RowBudget);
    assert_eq!(problem.stream, Some(LegacySpawnStream::SpawnJournal));
    assert!(problem.after_operation_id.is_none());
}

#[test]
fn every_keyed_admission_is_seen_across_pages_and_across_team_kind_boundaries() {
    let mut f = Fixture::new();
    for i in 0..66 {
        certified(&f.db, &format!("a_{i:03}"), "{irrelevant-malformed-payload");
    }
    journal(
        &f.db,
        "z_foreign",
        "different-team",
        "different-kind",
        "rolled_back",
        "{}",
    );
    admission(&f.db, "z_foreign", "original-other-team", Some("{}"));
    let result = f.scan(
        &[session("arbitrary-target")],
        LegacySpawnScanBudget::default(),
    );
    let original = assert_blocked(&result, 0, "z_foreign", Reason::UnknownSessionScope);
    assert_eq!(original.team_id, "original-other-team");
    assert_eq!(
        original.evidence_problem.as_deref(),
        Some("journal_identity_mismatch")
    );
    assert_eq!(result.stats.certified_receipts, 66);
    assert_eq!(result.stats.source_rows_read, 67);
    assert!(result.stats.pages_read >= 2);
}

#[test]
fn unkeyed_completed_and_rolled_back_rows_remain_candidates_after_page_boundary() {
    for terminal in ["completed", "rolled_back"] {
        let mut f = Fixture::new();
        for i in 0..66 {
            journal(
                &f.db,
                &format!("u_{i:03}"),
                TEAM,
                KIND,
                "session_created",
                &child_payload("other"),
            );
        }
        journal(
            &f.db,
            "u_999",
            "another-team",
            KIND,
            terminal,
            &child_payload("victim"),
        );
        let result = f.scan(&[session("victim")], LegacySpawnScanBudget::default());
        let original = assert_blocked(&result, 0, "u_999", Reason::ExactSession);
        assert_eq!(original.team_id, "another-team");
        assert_eq!(original.stage.as_deref(), Some(terminal));
        assert_eq!(result.stats.source_rows_read, 67);
        assert_eq!(result.stats.certified_receipts, 0);
    }
}

#[test]
fn every_saved_stage_has_independent_session_and_checkout_scope() {
    let saved = worktree();
    let cases = [
        (
            "planned",
            serde_json::json!({"source_session_id":"source","worktree":null}),
            false,
        ),
        (
            "worktree_created",
            serde_json::json!({"worktree":saved}),
            false,
        ),
        (
            "session_create_review_required",
            serde_json::json!({
                "reason":"session_create_failed","error":"lost ack",
                "created_worktree_id":saved.id,"assigned_worktree_id":saved.id
            }),
            false,
        ),
        (
            "session_created",
            serde_json::json!({"spawned_session_id":"victim"}),
            true,
        ),
        (
            "team_joined",
            serde_json::json!({"spawned_session_id":"victim"}),
            true,
        ),
        (
            "onboarding_sent",
            serde_json::json!({
                "source_session_id":"source","spawned_session_id":"victim","provider":"codex",
                "provider_session_ref":"historical-native","canonical_provider_session_ref":null,
                "worktree_id":null,"message_id":"message","delivery_ids":["delivery"]
            }),
            true,
        ),
        (
            "completed",
            serde_json::json!({"spawned_session_id":"victim","worktree_id":null}),
            true,
        ),
        (
            "rollback_pending",
            serde_json::json!({
                "reason":"lost-stage","spawned_session_id":"victim",
                "assigned_worktree_id":null,"created_worktree_id":null,
                "remove_member":true,"release_claim":false,"payload":{}
            }),
            true,
        ),
        (
            "rollback_review_required",
            serde_json::json!({
                "spawned_session_id":"victim","assigned_worktree_id":saved.id,
                "compensation_outcomes":{"session_close":"unknown"},"uncertainties":["lost"],"payload":{}
            }),
            true,
        ),
        (
            "rolled_back",
            serde_json::json!({
                "spawned_session_id":"victim","assigned_worktree_id":null,"created_worktree_id":null,
                "compensation_outcomes":{"session_close":"confirmed"},"uncertainties":[],"payload":{}
            }),
            true,
        ),
        (
            "future_unknown_stage",
            serde_json::json!({"spawned_session_id":"victim"}),
            false,
        ),
    ];
    for keyed in [false, true] {
        for (stage, payload, complete_child) in &cases {
            let mut f = Fixture::new();
            journal(&f.db, "original", TEAM, KIND, stage, &payload.to_string());
            if keyed {
                admission(&f.db, "original", TEAM, None);
            }
            let result = f.scan(
                &[
                    session("victim"),
                    session("unrelated"),
                    checkout(Some(&saved.id), Some("/unrelated/checkout")),
                ],
                LegacySpawnScanBudget::default(),
            );
            assert!(
                matches!(result.decisions[0], LegacyTargetDecision::Blocked { .. }),
                "{stage} {result:?}"
            );
            assert!(
                matches!(result.decisions[2], LegacyTargetDecision::Blocked { .. }),
                "{stage} {result:?}"
            );
            if *complete_child {
                assert!(
                    matches!(result.decisions[1], LegacyTargetDecision::Clear),
                    "{stage} {result:?}"
                );
                assert_blocked(&result, 0, "original", Reason::ExactSession);
            } else {
                assert_blocked(&result, 1, "original", Reason::UnknownSessionScope);
            }
        }
    }
}

#[test]
fn admission_only_and_wrong_journal_identity_never_borrow_a_completed_receipt() {
    for variant in ["missing", "wrong-team", "wrong-kind"] {
        let mut f = Fixture::new();
        let receipt = receipt("original", TEAM, "other-child");
        admission(&f.db, "original", TEAM, Some(&receipt));
        if variant != "missing" {
            journal(
                &f.db,
                "original",
                if variant == "wrong-team" {
                    "foreign-team"
                } else {
                    TEAM
                },
                if variant == "wrong-kind" {
                    "foreign-kind"
                } else {
                    KIND
                },
                "completed",
                &child_payload("other-child"),
            );
        }
        let result = f.scan(&[session("unrelated")], LegacySpawnScanBudget::default());
        let coordinate = assert_blocked(&result, 0, "original", Reason::UnknownSessionScope);
        assert_eq!(coordinate.team_id, TEAM);
        assert_eq!(
            coordinate.evidence_problem.as_deref(),
            Some(if variant == "missing" {
                "journal_missing"
            } else {
                "journal_identity_mismatch"
            })
        );
        assert_eq!(result.stats.certified_receipts, 0);
    }
}

#[test]
fn typed_saved_completion_precedes_malformed_oversized_or_nontext_journal_payload() {
    for variant in ["malformed", "oversized", "blob", "invalid-utf8"] {
        let mut f = Fixture::new();
        let receipt = certified(&f.db, "original", "{malformed");
        match variant {
            "oversized" => {
                f.db.execute(
                    "UPDATE team_operation_journal SET payload_json=?1",
                    [" ".repeat(JSON_LIMIT + 1)],
                )
                .unwrap();
            }
            "blob" => {
                f.db.execute(
                    "UPDATE team_operation_journal SET payload_json=?1",
                    [vec![0_u8; JSON_LIMIT + 1]],
                )
                .unwrap();
            }
            "invalid-utf8" => {
                f.db.execute_batch(
                    "UPDATE team_operation_journal SET payload_json=CAST(x'80' AS TEXT);",
                )
                .unwrap();
            }
            _ => {}
        }
        let before = raw_rows(&f.db, "team_operation_journal");
        let result = f.scan(
            &[session("any"), checkout(None, Some("/any"))],
            LegacySpawnScanBudget::default(),
        );
        assert_clear(&result);
        assert_eq!(result.stats.certified_receipts, 1);
        assert_eq!(result.stats.source_rows_read, 2);
        assert_eq!(
            result.stats.materialized_bytes,
            metadata_bytes("original", TEAM, "completed") * 2 + receipt.len()
        );
        assert_eq!(raw_rows(&f.db, "team_operation_journal"), before);
    }
}

#[test]
fn nonnull_invalid_receipts_do_not_clear_original_child_or_checkout() {
    for variant in [
        "not-json",
        "not-object",
        "wrong-operation",
        "wrong-team",
        "wrong-stage",
        "blob",
    ] {
        let mut f = Fixture::new();
        let mut response = saved_response("original", TEAM, "victim");
        match variant {
            "wrong-operation" => response.operation_id = "another-operation".into(),
            "wrong-team" => response.team.team.id = "another-team".into(),
            "wrong-stage" => response.journal_stage = "rolled_back".into(),
            _ => {}
        }
        let value = match variant {
            "not-json" => "{malformed".into(),
            "not-object" => "[]".into(),
            _ => serde_json::to_string(&response).unwrap(),
        };
        journal(
            &f.db,
            "original",
            TEAM,
            KIND,
            "completed",
            &child_payload("victim"),
        );
        admission(&f.db, "original", TEAM, Some(&value));
        if variant == "blob" {
            f.db.execute(
                "UPDATE team_spawn_admissions SET completed_response_json=?1",
                [value.as_bytes()],
            )
            .unwrap();
        }
        let result = f.scan(
            &[
                session("victim"),
                session("unrelated"),
                checkout(None, Some("/any")),
            ],
            LegacySpawnScanBudget::default(),
        );
        assert_blocked(&result, 0, "original", Reason::ExactSession);
        assert!(matches!(result.decisions[1], LegacyTargetDecision::Clear));
        assert_blocked(&result, 2, "original", Reason::UnknownCheckoutScope);
        assert_eq!(result.stats.certified_receipts, 0);
    }
}

#[test]
fn duplicate_root_nested_and_contradictory_child_fields_cannot_prove_disjointness() {
    let cases = [
        r#"{"spawned_session_id":"a","spawned_session_id":"b"}"#,
        r#"{"spawned_session_id":"a","spawned_session_id":"a"}"#,
        r#"{"spawned_session_id":"a","payload":{"spawned_session_id":"a","spawned_session_id":"a"}}"#,
        r#"{"spawned_session_id":"a","payload":{"spawned_session_id":"b"}}"#,
        r#"{"spawned_session_id":"a","payload":{},"payload":{}}"#,
        r#"{"payload":{"spawned_session_id":"a"}}"#,
        r#"{"spawned_session_id":"a","payload":["ambiguous"]}"#,
        r#"{"spawned_session_id":null}"#,
        r#"{"spawned_session_id":7}"#,
        r#"{"spawned_session_id":""}"#,
        r#"{"spawned_session_id":" a "}"#,
        r#"{"spawned_session_id":"bad\nchild"}"#,
    ];
    for payload in cases {
        let mut f = Fixture::new();
        journal(&f.db, "original", TEAM, KIND, "rollback_pending", payload);
        let result = f.scan(&[session("unrelated")], LegacySpawnScanBudget::default());
        assert_blocked(&result, 0, "original", Reason::UnknownSessionScope);
    }
    let mut f = Fixture::new();
    journal(
        &f.db,
        "original",
        TEAM,
        KIND,
        "rollback_pending",
        r#"{"spawned_session_id":"a","payload":{"spawned_session_id":"a"},"ignored":1,"ignored":2}"#,
    );
    assert_clear(&f.scan(&[session("unrelated")], LegacySpawnScanBudget::default()));
}

#[test]
fn duplicate_worktree_keys_destroy_complete_snapshot_without_guessing_absence() {
    let record = serde_json::to_string(&worktree()).unwrap();
    let duplicate_id = record.replacen(
        r#""id":"historical-worktree""#,
        r#""id":"old","id":"historical-worktree""#,
        1,
    );
    assert_ne!(duplicate_id, record);
    for payload in [
        format!(r#"{{"worktree":{duplicate_id}}}"#),
        format!(r#"{{"worktree":{record},"worktree":{record}}}"#),
        r#"{"worktree":{"id":"historical-worktree"}}"#.into(),
    ] {
        let mut f = Fixture::new();
        journal(&f.db, "original", TEAM, KIND, "worktree_created", &payload);
        let result = f.scan(
            &[checkout(None, Some("/another/checkout"))],
            LegacySpawnScanBudget::default(),
        );
        assert_blocked(&result, 0, "original", Reason::UnknownCheckoutScope);
    }
}

#[test]
fn full_saved_checkout_spelling_retains_alias_uncertainty_and_original_provenance() {
    let mut f = Fixture::new();
    let record = worktree();
    journal(
        &f.db,
        "original",
        TEAM,
        KIND,
        "worktree_created",
        &serde_json::json!({"worktree":record}).to_string(),
    );
    let result = f.scan(
        &[
            checkout(Some(&record.id), Some("/different/current/path")),
            checkout(None, Some("/saved/old-link/./checkout/nested")),
            checkout(Some("different-id"), Some("/different/new-link/checkout")),
            session("source-not-child"),
        ],
        LegacySpawnScanBudget::default(),
    );
    assert_blocked(&result, 0, "original", Reason::ExactWorktreeId);
    assert_blocked(&result, 1, "original", Reason::OverlappingCheckoutSpelling);
    assert_blocked(&result, 2, "original", Reason::HistoricalPathAliasUncertain);
    assert_blocked(&result, 3, "original", Reason::UnknownSessionScope);
}

#[test]
fn id_only_routes_and_null_route_fields_never_prove_checkout_disjointness() {
    for route in [serde_json::Value::Null, serde_json::json!("old-id")] {
        let mut f = Fixture::new();
        journal(
            &f.db,
            "original",
            TEAM,
            KIND,
            "onboarding_sent",
            &serde_json::json!({"spawned_session_id":"child","worktree_id":route}).to_string(),
        );
        // Current mutable ID mapping is deliberately not historical negative proof.
        f.db.execute_batch(
            "CREATE TABLE managed_worktrees(id TEXT PRIMARY KEY, worktree_cwd TEXT);
             INSERT INTO managed_worktrees VALUES('old-id','/new/current/location');",
        )
        .unwrap();
        let result = f.scan(
            &[
                session("other"),
                checkout(Some("another-id"), Some("/unrelated")),
            ],
            LegacySpawnScanBudget::default(),
        );
        assert!(matches!(result.decisions[0], LegacyTargetDecision::Clear));
        assert_blocked(&result, 1, "original", Reason::UnknownCheckoutScope);
    }
}

#[test]
fn malformed_or_invalid_utf8_payload_preserves_original_coordinate() {
    for variant in ["malformed", "array", "blob", "invalid-utf8"] {
        let mut f = Fixture::new();
        journal(
            &f.db,
            "historical-id-without-prefix",
            TEAM,
            KIND,
            "session_created",
            if variant == "array" {
                "[]"
            } else {
                "{malformed"
            },
        );
        if variant == "blob" {
            f.db.execute(
                "UPDATE team_operation_journal SET payload_json=?1",
                [child_payload("other").into_bytes()],
            )
            .unwrap();
        } else if variant == "invalid-utf8" {
            f.db.execute_batch(
                "UPDATE team_operation_journal SET payload_json=CAST(x'80' AS TEXT);",
            )
            .unwrap();
        }
        let result = f.scan(&[session("unrelated")], LegacySpawnScanBudget::default());
        let coordinate = assert_blocked(
            &result,
            0,
            "historical-id-without-prefix",
            Reason::UnknownSessionScope,
        );
        assert_eq!(coordinate.team_id, TEAM);
        assert_eq!(coordinate.stage.as_deref(), Some("session_created"));
        assert_eq!(
            coordinate.evidence_problem.as_deref(),
            Some(match variant {
                "blob" => "payload_not_text",
                "invalid-utf8" => "payload_invalid_utf8",
                _ => "payload_malformed_or_not_object",
            })
        );
    }
}

#[test]
fn exact_row_boundary_requires_both_streams_and_never_blames_a_certified_operation() {
    let mut f = Fixture::new();
    certified(&f.db, "certified", "{not-read");
    let short = f.scan(&[session("target")], budget(1, 1024 * 1024));
    let problem = assert_incomplete(&short, 0, Limit::RowBudget);
    assert_eq!(problem.stream, Some(LegacySpawnStream::SpawnJournal));
    assert!(problem.after_operation_id.is_none());
    assert_eq!(short.stats.certified_receipts, 1);
    let exact = f.scan(&[session("target")], budget(2, 1024 * 1024));
    assert_clear(&exact);
    assert_eq!(exact.stats.source_rows_read, 2);
    journal(
        &f.db,
        "z_extra",
        TEAM,
        KIND,
        "session_created",
        &child_payload("other"),
    );
    let additional = f.scan(&[session("target")], budget(2, 1024 * 1024));
    let problem = assert_incomplete(&additional, 0, Limit::RowBudget);
    assert_eq!(problem.after_operation_id.as_deref(), Some("certified"));
    assert_clear(&f.scan(&[session("target")], budget(3, 1024 * 1024)));
}

#[test]
fn partial_batch_retains_known_blocker_and_marks_other_targets_incomplete() {
    let mut f = Fixture::new();
    journal(
        &f.db,
        "a_known",
        TEAM,
        KIND,
        "session_created",
        &child_payload("victim"),
    );
    journal(&f.db, "z_unexamined", TEAM, KIND, "planned", "{}");
    let result = f.scan(
        &[session("unrelated"), session("victim")],
        budget(1, 1024 * 1024),
    );
    let problem = assert_incomplete(&result, 0, Limit::RowBudget);
    assert_eq!(problem.after_operation_id.as_deref(), Some("a_known"));
    assert_blocked(&result, 1, "a_known", Reason::ExactSession);
}

#[test]
fn aggregate_budget_counts_utf8_bytes_and_does_not_advance_past_unexamined_payload() {
    let mut f = Fixture::new();
    let op = "op_é";
    let team = "团队";
    let payload =
        serde_json::json!({"spawned_session_id":"不同-child","padding":"é😀"}).to_string();
    journal(&f.db, op, team, KIND, "session_created", &payload);
    let metadata = metadata_bytes(op, team, "session_created");
    let exact_bytes = metadata + payload.len();
    let exact = f.scan(&[session("unrelated")], budget(1, exact_bytes));
    assert_clear(&exact);
    assert_eq!(exact.stats.materialized_bytes, exact_bytes);
    let short = f.scan(&[session("unrelated")], budget(1, exact_bytes - 1));
    let problem = assert_incomplete(&short, 0, Limit::ByteBudget);
    assert!(problem.after_operation_id.is_none());
    assert_eq!(
        short.stats.materialized_bytes, metadata,
        "SQL must not materialize the over-budget JSON"
    );
}

#[test]
fn payload_cell_limit_accepts_exact_bytes_and_refuses_one_more_without_partial_parse() {
    for extra in [0, 1] {
        let mut f = Fixture::new();
        let prefix = r#"{"spawned_session_id":"other","padding":""#;
        let suffix = r#""}"#;
        let payload = format!(
            "{prefix}{}{suffix}",
            "x".repeat(JSON_LIMIT + extra - prefix.len() - suffix.len())
        );
        assert_eq!(payload.len(), JSON_LIMIT + extra);
        journal(&f.db, "original", TEAM, KIND, "session_created", &payload);
        let result = f.scan(&[session("target")], LegacySpawnScanBudget::default());
        if extra == 0 {
            assert_clear(&result);
        } else {
            assert_incomplete(&result, 0, Limit::PayloadCellLimit);
            assert_eq!(
                result.stats.materialized_bytes,
                metadata_bytes("original", TEAM, "session_created")
            );
        }
    }
}

#[test]
fn oversized_valid_saved_receipt_is_scan_incomplete_not_a_claim_of_unresolved_effect() {
    for extra in [0, 1] {
        let mut f = Fixture::new();
        let mut response = saved_response("original", TEAM, "child");
        response.spawned_session.metadata = serde_json::json!({"padding":""});
        let base = serde_json::to_string(&response).unwrap().len();
        response.spawned_session.metadata =
            serde_json::json!({"padding":"x".repeat(JSON_LIMIT + extra - base)});
        let receipt = serde_json::to_string(&response).unwrap();
        assert_eq!(receipt.len(), JSON_LIMIT + extra);
        journal(&f.db, "original", TEAM, KIND, "completed", "{malformed");
        admission(&f.db, "original", TEAM, Some(&receipt));
        let result = f.scan(&[session("target")], LegacySpawnScanBudget::default());
        if extra == 0 {
            assert_clear(&result);
            assert_eq!(result.stats.certified_receipts, 1);
        } else {
            let problem = assert_incomplete(&result, 0, Limit::PayloadCellLimit);
            assert_eq!(problem.stream, Some(LegacySpawnStream::KeyedAdmissions));
            assert!(problem.after_operation_id.is_none());
            assert_eq!(result.stats.certified_receipts, 0);
        }
    }
}

#[test]
fn metadata_limit_and_invalid_coordinate_bytes_do_not_manufacture_an_operation_id() {
    for extra in [0, 1] {
        let mut f = Fixture::new();
        let op = "o".repeat(METADATA_LIMIT + extra);
        journal(
            &f.db,
            &op,
            TEAM,
            KIND,
            "session_created",
            &child_payload("other"),
        );
        let result = f.scan(&[session("target")], LegacySpawnScanBudget::default());
        if extra == 0 {
            assert_clear(&result);
        } else {
            let problem = assert_incomplete(&result, 0, Limit::MetadataCellLimit);
            assert!(problem.after_operation_id.is_none());
            assert_eq!(result.stats.materialized_bytes, 0);
        }
    }
    for expression in ["NULL", "CAST(x'80' AS TEXT)"] {
        let mut f = Fixture::new();
        journal(
            &f.db,
            "original",
            TEAM,
            KIND,
            "session_created",
            &child_payload("other"),
        );
        f.db.execute_batch(&format!(
            "UPDATE team_operation_journal SET operation_id={expression};"
        ))
        .unwrap();
        let result = f.scan(&[session("target")], LegacySpawnScanBudget::default());
        assert_incomplete(&result, 0, Limit::InvalidMetadata);
    }
}

#[test]
fn prefetch_failure_keeps_cursor_before_the_unexamined_page() {
    let mut f = Fixture::new();
    for i in 0..64 {
        journal(
            &f.db,
            &format!("a_{i:03}"),
            TEAM,
            KIND,
            "session_created",
            &child_payload("other"),
        );
    }
    journal(
        &f.db,
        "b_valid_but_not_examined",
        TEAM,
        KIND,
        "session_created",
        &child_payload("other"),
    );
    journal(
        &f.db,
        "c_oversized_metadata",
        &"t".repeat(METADATA_LIMIT + 1),
        KIND,
        "planned",
        "{}",
    );
    let result = f.scan(&[session("target")], LegacySpawnScanBudget::default());
    let problem = assert_incomplete(&result, 0, Limit::MetadataCellLimit);
    assert_eq!(problem.after_operation_id.as_deref(), Some("a_063"));
    assert_ne!(
        problem.after_operation_id.as_deref(),
        Some("b_valid_but_not_examined")
    );
}

#[test]
fn invalid_target_budget_is_not_an_empty_clear_decision_set() {
    let mut f = Fixture::new();
    for targets in [
        vec![session("target"); 1025],
        vec![checkout(None, Some(&"x".repeat(4 * 1024 * 1024 + 1)))],
        vec![session(" ")],
        vec![checkout(None, None)],
    ] {
        let result = f.scan(&targets, LegacySpawnScanBudget::default());
        assert!(result.decisions.is_empty());
        let LegacyScanTermination::Incomplete(problem) = result.termination else {
            panic!("must reject target set")
        };
        assert_eq!(problem.reason, Limit::TargetBudget);
        assert_eq!(problem.stats.source_rows_read, 0);
        assert_eq!(problem.stats.materialized_bytes, 0);
    }
}

#[test]
fn database_failures_propagate_and_successful_scans_never_write_history() {
    let mut f = Fixture::new();
    certified(&f.db, "certified", "{}");
    journal(
        &f.db,
        "unkeyed",
        TEAM,
        KIND,
        "session_created",
        &child_payload("other"),
    );
    let before = (
        raw_rows(&f.db, "team_operation_journal"),
        raw_rows(&f.db, "team_spawn_admissions"),
    );
    let changes: i64 =
        f.db.query_row("SELECT total_changes()", [], |row| row.get(0))
            .unwrap();
    assert_clear(&f.scan(&[session("target")], LegacySpawnScanBudget::default()));
    assert_eq!(
        (
            raw_rows(&f.db, "team_operation_journal"),
            raw_rows(&f.db, "team_spawn_admissions")
        ),
        before
    );
    let after: i64 =
        f.db.query_row("SELECT total_changes()", [], |row| row.get(0))
            .unwrap();
    assert_eq!(changes, after);
    f.db.execute_batch("DROP TABLE team_operation_journal;")
        .unwrap();
    assert!(scan(
        &mut f.db,
        &[session("target")],
        LegacySpawnScanBudget::default()
    )
    .is_err());
}

#[test]
fn typed_receipt_requires_completed_journal_stage_and_nested_only_child_is_not_complete_scope() {
    for stage in ["planned", "rollback_pending", "rolled_back"] {
        let mut f = Fixture::new();
        journal(
            &f.db,
            "original",
            TEAM,
            KIND,
            stage,
            &child_payload("victim"),
        );
        admission(
            &f.db,
            "original",
            TEAM,
            Some(&receipt("original", TEAM, "victim")),
        );
        let result = f.scan(
            &[session("victim"), checkout(None, Some("/any"))],
            LegacySpawnScanBudget::default(),
        );
        assert_blocked(&result, 0, "original", Reason::ExactSession);
        assert_blocked(&result, 1, "original", Reason::UnknownCheckoutScope);
        assert_eq!(result.stats.certified_receipts, 0);
    }
    let mut f = Fixture::new();
    journal(
        &f.db,
        "nested",
        TEAM,
        KIND,
        "rollback_pending",
        r#"{"payload":{"spawned_session_id":"victim"}}"#,
    );
    let result = f.scan(
        &[session("victim"), session("unrelated")],
        LegacySpawnScanBudget::default(),
    );
    assert_blocked(&result, 0, "nested", Reason::ExactSession);
    assert_blocked(&result, 1, "nested", Reason::UnknownSessionScope);
}
