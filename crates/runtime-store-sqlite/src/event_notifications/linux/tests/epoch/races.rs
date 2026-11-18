use super::*;

#[test]
fn weakened_epoch_carrier_with_original_value_fails_active_replay_without_repair() {
    for view in [false, true] {
        let fixture = Fixture::new();
        fixture.seed("weak_structure");
        let db = writer(&fixture);
        let original = read_journal_epoch(&db).unwrap();
        let (observer, subscription, control) = fixture.idle_observer();
        let pause = control.arm(Phase::AfterReplay);
        let reader = subscription.replay.clone();
        let (rx, thread) = on_thread(move || reader.list_runtime_events_bound(&global(), None, 8));
        pause.wait_reached();
        db.execute_batch("BEGIN IMMEDIATE; DROP TABLE runtime_event_journal_identity;")
            .unwrap();
        if view {
            db.execute_batch(&format!(
                "CREATE VIEW runtime_event_journal_identity AS SELECT 1 AS singleton,'{}' AS epoch",
                original.as_str()
            ))
            .unwrap();
        } else {
            db.execute_batch("CREATE TABLE runtime_event_journal_identity(singleton,epoch)")
                .unwrap();
            db.execute(
                "INSERT INTO runtime_event_journal_identity VALUES(1,?1)",
                [original.as_str()],
            )
            .unwrap();
        }
        db.execute_batch("COMMIT").unwrap();
        let sql: String = db
            .query_row(
                "SELECT sql FROM sqlite_master WHERE name='runtime_event_journal_identity'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        pause.release();
        assert_runtime(finish(rx, thread), "event_observer_unavailable");
        assert_sticky(
            &observer,
            &subscription,
            &control,
            "event_observer_unavailable",
        );
        assert_eq!(
            db.query_row(
                "SELECT sql FROM sqlite_master WHERE name='runtime_event_journal_identity'",
                [],
                |r| r.get::<_, String>(0)
            )
            .unwrap(),
            sql
        );
        assert_eq!(
            db.query_row(
                "SELECT epoch FROM runtime_event_journal_identity",
                [],
                |r| r.get::<_, String>(0)
            )
            .unwrap(),
            original.as_str()
        );
        drop((subscription, observer));
        control.wait_exited();
    }
}

#[test]
fn epoch_rotation_or_metadata_loss_after_replay_never_returns_a_page() {
    for bound in [false, true] {
        for remove_metadata in [false, true] {
            let fixture = Fixture::new();
            let anchor = fixture.seed("race_anchor");
            fixture.seed("race_tail");
            let db = writer(&fixture);
            let original_epoch = read_journal_epoch(&db).unwrap();
            let (observer, subscription, control) = fixture.idle_observer();
            let collection = global();
            let after = cursor(&original_epoch, &collection, &anchor, &anchor.event_id);
            let reads_before = observer.observation_counters().high_water_reads;
            let pause = control.arm(Phase::AfterReplay);
            let reader = subscription.replay.clone();
            let (rx, thread) = on_thread(move || {
                if bound {
                    reader
                        .list_runtime_events_bound(&collection, Some(&after), 8)
                        .map(|p| p.rows)
                } else {
                    reader
                        .list_runtime_events(None, Some(anchor.row_id), 8)
                        .map_err(EventReadError::from)
                }
            });
            pause.wait_reached();
            let code = if remove_metadata {
                db.execute_batch("DELETE FROM runtime_event_journal_identity")
                    .unwrap();
                "event_observer_unavailable"
            } else {
                set_epoch(&db, &different_epoch(&original_epoch));
                "journal_generation_changed"
            };
            assert_eq!(*subscription.health.borrow(), EventObserverHealth::Ready);
            assert_eq!(
                observer.observation_counters().high_water_reads,
                reads_before,
                "replay state lock excludes rescuing worker probe"
            );
            pause.release();
            assert_runtime(finish(rx, thread), code);
            assert_sticky(&observer, &subscription, &control, code);
            drop((subscription, observer));
            control.wait_exited();
        }
    }
}

#[test]
fn epoch_anchor_and_page_share_snapshot_then_current_epoch_is_checked() {
    for rotate_epoch in [false, true] {
        let fixture = Fixture::new();
        let anchor = fixture.seed("snapshot_anchor");
        let tail = fixture.seed("snapshot_tail");
        let db = writer(&fixture);
        let original_epoch = read_journal_epoch(&db).unwrap();
        let live_epoch = if rotate_epoch {
            different_epoch(&original_epoch)
        } else {
            original_epoch.clone()
        };
        let (observer, mut subscription, control) = fixture.idle_observer();
        let collection = global();
        let after = cursor(&original_epoch, &collection, &anchor, &anchor.event_id);
        let pause = control.arm(Phase::AfterReplayEpoch);
        let reader = subscription.replay.clone();
        let read_collection = collection.clone();
        let read_after = after.clone();
        let (rx, thread) = on_thread(move || {
            reader.list_runtime_events_bound(&read_collection, Some(&read_after), 8)
        });
        pause.wait_reached();
        // Independent WAL writer commits after the reader fixed its snapshot,
        // before the anchor and tail are read through that snapshot.
        db.execute_batch("BEGIN IMMEDIATE").unwrap();
        set_epoch(&db, &live_epoch);
        assert_eq!(
            db.execute(
                "UPDATE runtime_events SET event_id=?1 WHERE id=?2",
                params!["replacement_anchor", anchor.row_id]
            )
            .unwrap(),
            1
        );
        assert_eq!(
            db.execute(
                "UPDATE runtime_events SET event_id=?1 WHERE id=?2",
                params!["replacement_tail", tail.row_id]
            )
            .unwrap(),
            1
        );
        db.execute_batch("COMMIT").unwrap();
        assert_eq!(read_journal_epoch(&db).unwrap(), live_epoch);
        assert_eq!(
            db.query_row(
                "SELECT event_id FROM runtime_events WHERE id=?1",
                [anchor.row_id],
                |r| r.get::<_, String>(0)
            )
            .unwrap(),
            "replacement_anchor"
        );
        pause.release();
        let result = finish(rx, thread);
        if rotate_epoch {
            assert_runtime(result, "journal_generation_changed");
            assert_sticky(
                &observer,
                &subscription,
                &control,
                "journal_generation_changed",
            );
        } else {
            let page = result.expect("all three reads must use the old snapshot");
            assert_eq!(page.epoch, original_epoch);
            assert_eq!(page.rows, vec![tail.clone()]);
            assert_eq!(*subscription.health.borrow(), EventObserverHealth::Ready);
            assert_cursor(
                subscription
                    .replay
                    .list_runtime_events_bound(&collection, Some(&after), 8),
                EventCursorRejection::AnchorMismatch,
            );
            let replacement = cursor(&original_epoch, &collection, &anchor, "replacement_anchor");
            let current: BoundEventPage<RuntimeEventRecord> = subscription
                .replay
                .list_runtime_events_bound(&collection, Some(&replacement), 8)
                .unwrap();
            let mut expected_tail = tail;
            expected_tail.event_id = "replacement_tail".into();
            assert_eq!(current.epoch, original_epoch);
            assert_eq!(current.rows, vec![expected_tail]);
            assert_no_commit(&mut subscription, &control);
            assert_eq!(*subscription.health.borrow(), EventObserverHealth::Ready);
        }
        drop((subscription, observer));
        control.wait_exited();
    }
}

#[test]
fn bound_replay_rejects_physical_replacement_and_aba_at_each_head_relation() {
    for replacement_rows in [1, 2, 3] {
        for restore_original in [false, true] {
            let a = Fixture::new();
            let anchor = a.seed("physical_anchor");
            a.seed("physical_tail");
            let b = Fixture::new();
            for index in 0..replacement_rows {
                b.seed(&format!("replacement_{index}"));
            }
            let (observer, subscription, control) = a.idle_observer();
            let collection = global();
            let initial = subscription
                .replay
                .list_runtime_events_bound(&collection, None, 1)
                .unwrap();
            let after = cursor(&initial.epoch, &collection, &anchor, &anchor.event_id);
            let pause = control.arm(Phase::AfterReplay);
            let reader = subscription.replay.clone();
            let (rx, thread) =
                on_thread(move || reader.list_runtime_events_bound(&collection, Some(&after), 8));
            pause.wait_reached();
            let saved = a.root.path().join("saved-main");
            std::fs::rename(&a.path, &saved).unwrap();
            std::fs::rename(&b.path, &a.path).unwrap();
            if restore_original {
                std::fs::rename(&a.path, &b.path).unwrap();
                std::fs::rename(&saved, &a.path).unwrap();
            }
            pause.release();
            assert_runtime(finish(rx, thread), "journal_generation_changed");
            assert_sticky(
                &observer,
                &subscription,
                &control,
                "journal_generation_changed",
            );
            drop((subscription, observer));
            control.wait_exited();
        }
    }
}
