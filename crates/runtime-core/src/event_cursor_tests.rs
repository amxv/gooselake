//! Golden vector independently computed with Python hashlib.
use crate::{
    EventCollection, EventCursor, EventCursorRejection, EventEpoch, EventPositionKind,
    RuntimeEventScope,
};

const EPOCH: &str = "00112233445566778899aabbccddeeff";
const GOLDEN: &str = "ev1.00112233445566778899aabbccddeeff.r.workspace.77732f2041cea9.41.5f1b1391d098e48174ca983469c58d7822c422877d62a3ef9f6ffa4edb771d3e";

fn fixture() -> (EventEpoch, EventCollection, EventCursor) {
    let epoch = EventEpoch::from_persisted(EPOCH).unwrap();
    let collection =
        EventCollection::history(Some((RuntimeEventScope::Workspace, "ws/ AΩ".into()))).unwrap();
    let cursor = EventCursor::for_event(&epoch, &collection, 41, 7, "evt_雪/1").unwrap();
    (epoch, collection, cursor)
}

#[test]
fn event_cursor_golden_binds_exact_context_and_domain_separated_event_identity() {
    let (epoch, collection, cursor) = fixture();
    assert_eq!(cursor.encode(), GOLDEN);
    assert_eq!(EventCursor::decode(GOLDEN).unwrap(), cursor);
    cursor.validate_context(&epoch, &collection).unwrap();
    cursor.validate_anchor(Some("evt_雪/1")).unwrap();
    assert_eq!(
        cursor.validate_anchor(Some("evt_雪/2")),
        Err(EventCursorRejection::AnchorMismatch)
    );
    assert_eq!(
        cursor.validate_anchor(None),
        Err(EventCursorRejection::AnchorMismatch)
    );
    let other_epoch = EventEpoch::from_persisted("ffeeddccbbaa99887766554433221100").unwrap();
    assert_eq!(
        cursor.validate_context(&other_epoch, &collection),
        Err(EventCursorRejection::EpochMismatch)
    );
    let other_scope =
        EventCollection::history(Some((RuntimeEventScope::Workspace, "ws/ aΩ".into()))).unwrap();
    assert_eq!(
        cursor.validate_context(&epoch, &other_scope),
        Err(EventCursorRejection::CollectionMismatch)
    );
    let stream =
        EventCollection::stream(Some((RuntimeEventScope::Workspace, "ws/ AΩ".into()))).unwrap();
    assert_eq!(stream.position_kind(), EventPositionKind::ScopeSeq);
    assert_eq!(
        cursor.validate_context(&epoch, &stream),
        Err(EventCursorRejection::CollectionMismatch)
    );
    assert!(collection.require_stream().is_err());
    assert!(stream.require_history().is_err());
    let streamed = EventCursor::for_event(&epoch, &stream, 41, 7, "evt_雪/1").unwrap();
    assert_eq!(streamed.position(), 7);
    assert_ne!(streamed.encode(), cursor.encode());
}

#[test]
fn event_cursor_uses_exact_utf8_bytes_and_only_global_or_subject_streams_share_history_axis() {
    let epoch = EventEpoch::from_persisted(EPOCH).unwrap();
    for raw_id in [" A.a/\n\0雪 ".to_owned(), "é".repeat(128)] {
        let collection =
            EventCollection::stream(Some((RuntimeEventScope::Message, raw_id.clone()))).unwrap();
        assert_eq!(collection.position_kind(), EventPositionKind::RowId);
        collection.require_history().unwrap();
        collection.require_stream().unwrap();
        let cursor = EventCursor::for_event(&epoch, &collection, i64::MAX, 2, "event").unwrap();
        let encoded = cursor.encode();
        assert!(encoded.is_ascii() && encoded.len() <= 1024);
        let decoded = EventCursor::decode(&encoded).unwrap();
        assert_eq!(
            decoded.collection().scope(),
            Some((RuntimeEventScope::Message, raw_id.as_str()))
        );
        assert_eq!(decoded.position(), i64::MAX);
        assert_eq!(decoded, cursor);
    }
    for raw_id in [String::new(), "é".repeat(128) + "x", "é".repeat(129)] {
        assert!(EventCollection::history(Some((RuntimeEventScope::Workspace, raw_id))).is_err());
    }
    let global = EventCollection::stream(None).unwrap();
    assert_eq!(global, EventCollection::history(None).unwrap());
    let cursor = EventCursor::for_event(&epoch, &global, 3, 1, "event").unwrap();
    assert_eq!(EventCursor::decode(&cursor.encode()).unwrap(), cursor);
    assert!(EventCursor::decode(&cursor.encode().replacen(".r.global.", ".s.global.", 1)).is_err());
    assert!(EventCursor::for_event(&epoch, &global, 0, 1, "event").is_err());
}

#[test]
fn event_cursor_rejects_noncanonical_versions_fields_numbers_hex_and_lengths() {
    for raw in [
        "".to_owned(),
        "0".to_owned(),
        "41".to_owned(),
        "x".repeat(1025),
        format!("{GOLDEN}.extra"),
    ] {
        assert_eq!(
            EventCursor::decode(&raw),
            Err(EventCursorRejection::Malformed),
            "{raw:?}"
        );
    }
    for (field, bad) in [
        (0, "ev2"),
        (1, "00112233445566778899AABBCCDDEEFF"),
        (1, "0011"),
        (2, "row_id"),
        (3, "WORKSPACE"),
        (3, " workspace "),
        (4, "7"),
        (4, "GG"),
        (4, "ff"),
        (4, ""),
        (5, "0"),
        (5, "-1"),
        (5, "+41"),
        (5, "041"),
        (5, "1.0"),
        (5, "9223372036854775808"),
        (5, " 41"),
        (6, "00"),
        (
            6,
            "5F1B1391D098E48174CA983469C58D7822C422877D62A3EF9F6FFA4EDB771D3E",
        ),
    ] {
        let mut parts: Vec<_> = GOLDEN.split('.').map(str::to_owned).collect();
        parts[field] = bad.into();
        let raw = parts.join(".");
        assert_eq!(
            EventCursor::decode(&raw),
            Err(EventCursorRejection::Malformed),
            "{field}: {bad}"
        );
    }
    let mut parts: Vec<_> = GOLDEN.split('.').map(str::to_owned).collect();
    parts[4] = "61".repeat(257);
    assert!(EventCursor::decode(&parts.join(".")).is_err());
    parts[4] = "61".into();
    parts[2] = "s".into();
    parts[3] = "message".into();
    assert!(EventCursor::decode(&parts.join(".")).is_err());
    for bad in [
        "",
        "00112233445566778899AABBCCDDEEFF",
        "00112233445566778899aabbccddeeff\0",
        "00112233445566778899aabbccddeefg",
    ] {
        assert!(EventEpoch::from_persisted(bad).is_err());
    }
}
