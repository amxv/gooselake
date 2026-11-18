use super::*;
use crate::repository_event_cursor::{read_journal_epoch, validate_cursor};
use runtime_core::{EventCollection, EventCursor, EventCursorRejection, EventReadError};

fn assert_rejection<T: std::fmt::Debug>(
    result: Result<T, EventReadError>,
    rejection: EventCursorRejection,
) {
    assert!(
        matches!(result, Err(EventReadError::Cursor(found)) if found==rejection),
        "expected {rejection:?}"
    );
}

#[test]
fn bound_event_history_requires_exact_collection_axis_anchor_and_keeps_limit_outside_identity() {
    let temp = tempfile::tempdir().unwrap();
    let repository = repo(&temp);
    repository.initialize_schema().unwrap();
    for index in 0..7 {
        super::event_epoch::seed_event(
            &repository,
            &format!("bound_{index}"),
            if index == 0 || index == 6 {
                "workspace_exact"
            } else {
                "workspace_other"
            },
        );
    }
    let exact = EventCollection::history(Some((
        RuntimeEventScope::Workspace,
        "workspace_exact".into(),
    )))
    .unwrap();
    let page = repository
        .list_operator_events_page_bound(&exact, None, 1)
        .unwrap();
    let first = &page.rows[0];
    let cursor = EventCursor::for_event(
        &page.epoch,
        &exact,
        first.row_id,
        first.seq,
        &first.event_id,
    )
    .unwrap();
    for limit in [1, 17, 101] {
        let page = repository
            .list_operator_events_page_bound(&exact, Some(&cursor), limit)
            .unwrap();
        assert_eq!(page.rows.len(), 1);
        assert_eq!((page.rows[0].row_id, page.rows[0].seq), (7, 2));
        let public = serde_json::to_value(&page.rows[0]).unwrap();
        assert_eq!(public.as_object().unwrap().len(), 8);
        assert!(public.get("payload").is_none());
    }
    for other in [
        EventCollection::history(None).unwrap(),
        EventCollection::history(Some((
            RuntimeEventScope::Workspace,
            "workspace_other".into(),
        )))
        .unwrap(),
        EventCollection::history(Some((
            RuntimeEventScope::Workspace,
            "Workspace_exact".into(),
        )))
        .unwrap(),
    ] {
        assert_rejection(
            repository.list_operator_events_page_bound(&other, Some(&cursor), 1),
            EventCursorRejection::CollectionMismatch,
        );
    }
    let stream = EventCollection::stream(Some((
        RuntimeEventScope::Workspace,
        "workspace_exact".into(),
    )))
    .unwrap();
    let streamed = EventCursor::for_event(&page.epoch, &stream, 7, 2, "bound_6").unwrap();
    assert_eq!(streamed.position(), 2);
    assert_rejection(
        repository.list_operator_events_page_bound(&exact, Some(&streamed), 1),
        EventCursorRejection::CollectionMismatch,
    );
    assert_rejection(
        repository.list_operator_events_page_bound(&stream, None, 1),
        EventCursorRejection::CollectionMismatch,
    );
    let db = Connection::open(&repository.database_path).unwrap();
    let epoch = read_journal_epoch(&db).unwrap();
    validate_cursor(&db, &epoch, &stream, Some(&streamed)).unwrap();
    let bad = EventCursor::for_event(&epoch, &stream, 2, 7, "bound_6").unwrap();
    assert_rejection(
        validate_cursor(&db, &epoch, &stream, Some(&bad)),
        EventCursorRejection::AnchorMismatch,
    );
    let global = EventCollection::history(None).unwrap();
    let same_number_wrong_event = EventCursor::for_event(&epoch, &global, 7, 2, "bound_1").unwrap();
    assert_rejection(
        repository.list_operator_events_page_bound(&global, Some(&same_number_wrong_event), 1),
        EventCursorRejection::AnchorMismatch,
    );
    // Same epoch and numeric position cannot authorize a different event ID.
    db.execute(
        "UPDATE runtime_events SET event_id='replacement_event' WHERE id=1",
        [],
    )
    .unwrap();
    assert_rejection(
        repository.list_operator_events_page_bound(&exact, Some(&cursor), 1),
        EventCursorRejection::AnchorMismatch,
    );
    for limit in [0, 102] {
        assert!(repository
            .list_operator_events_page_bound(&exact, None, limit)
            .is_err());
    }
}

#[test]
fn bound_event_subject_anchor_requires_membership_and_one_read_snapshot() {
    let temp = tempfile::tempdir().unwrap();
    let repository = repo(&temp);
    repository.initialize_schema().unwrap();
    let event = super::event_epoch::seed_event(&repository, "subject_bound", "workspace_exact");
    let subject =
        EventCollection::history(Some((RuntimeEventScope::Message, "message_epoch".into())))
            .unwrap();
    let page = repository
        .list_operator_events_page_bound(&subject, None, 1)
        .unwrap();
    let cursor = EventCursor::for_event(
        &page.epoch,
        &subject,
        event.row_id,
        event.seq,
        &event.event_id,
    )
    .unwrap();
    assert!(repository
        .list_operator_events_page_bound(&subject, Some(&cursor), 1)
        .unwrap()
        .rows
        .is_empty());
    let missing =
        EventCollection::history(Some((RuntimeEventScope::Message, "other_message".into())))
            .unwrap();
    let forged = EventCursor::for_event(
        &page.epoch,
        &missing,
        event.row_id,
        event.seq,
        &event.event_id,
    )
    .unwrap();
    assert_rejection(
        repository.list_operator_events_page_bound(&missing, Some(&forged), 1),
        EventCursorRejection::AnchorMismatch,
    );
    let mut reader = Connection::open(&repository.database_path).unwrap();
    let writer = Connection::open(&repository.database_path).unwrap();
    let snapshot_page =
        crate::repository_event_history::read_bound_history(&mut reader, &subject, None, 2, || {
            writer
                .execute(
                    "UPDATE runtime_event_journal_identity SET epoch=lower(hex(randomblob(16)))",
                    [],
                )
                .unwrap();
            writer
                .execute(
                    "UPDATE runtime_events SET event_id='after_snapshot' WHERE id=?1",
                    [event.row_id],
                )
                .unwrap();
        })
        .unwrap();
    assert_eq!(snapshot_page.epoch, page.epoch);
    assert_eq!(snapshot_page.rows.len(), 1);
    assert_eq!(snapshot_page.rows[0].event_id, event.event_id);
    // Repeat with a real anchor; the callback mutates both identity and row
    // after the snapshot's epoch read, before anchor validation and page SQL.
    let fresh = repository
        .list_operator_events_page_bound(&subject, None, 1)
        .unwrap();
    let fresh_cursor = EventCursor::for_event(
        &fresh.epoch,
        &subject,
        event.row_id,
        event.seq,
        "after_snapshot",
    )
    .unwrap();
    let anchored = crate::repository_event_history::read_bound_history(
        &mut reader,
        &subject,
        Some(&fresh_cursor),
        2,
        || {
            writer
                .execute(
                    "UPDATE runtime_event_journal_identity SET epoch=lower(hex(randomblob(16)))",
                    [],
                )
                .unwrap();
            writer
                .execute(
                    "UPDATE runtime_events SET event_id='after_anchor_snapshot' WHERE id=?1",
                    [event.row_id],
                )
                .unwrap();
        },
    )
    .unwrap();
    assert_eq!(anchored.epoch, fresh.epoch);
    assert!(anchored.rows.is_empty());
    assert_rejection(
        repository.list_operator_events_page_bound(&subject, Some(&cursor), 1),
        EventCursorRejection::EpochMismatch,
    );
}
