//! Canonical continuation coordinates bound to an exact journal incarnation.
//! A continuation coordinate is not authorization or a completeness proof.
use crate::{RuntimeError, RuntimeEventScope};
use ring::digest::{Context, SHA256};
use thiserror::Error;

pub const MAX_EVENT_CURSOR_BYTES: usize = 1024;
pub const MAX_EVENT_SCOPE_ID_BYTES: usize = 256;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EventEpoch(String);

impl EventEpoch {
    pub fn from_persisted(value: &str) -> Result<Self, RuntimeError> {
        if !lower_hex(value, 32) {
            return Err(RuntimeError::Bootstrap(
                "journal epoch must be exactly 128 bits of canonical lowercase hex".into(),
            ));
        }
        Ok(Self(value.to_owned()))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum EventPositionKind {
    RowId,
    ScopeSeq,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EventCollection {
    scope: Option<(RuntimeEventScope, String)>,
    position_kind: EventPositionKind,
}

impl EventCollection {
    pub fn history(
        scope: Option<(RuntimeEventScope, String)>,
    ) -> Result<Self, EventCursorRejection> {
        Self::new(scope, EventPositionKind::RowId)
    }

    pub fn stream(
        scope: Option<(RuntimeEventScope, String)>,
    ) -> Result<Self, EventCursorRejection> {
        let kind = if scope
            .as_ref()
            .is_none_or(|(kind, _)| kind.is_subject_projection())
        {
            EventPositionKind::RowId
        } else {
            EventPositionKind::ScopeSeq
        };
        Self::new(scope, kind)
    }

    fn new(
        scope: Option<(RuntimeEventScope, String)>,
        position_kind: EventPositionKind,
    ) -> Result<Self, EventCursorRejection> {
        if scope
            .as_ref()
            .is_some_and(|(_, id)| id.is_empty() || id.len() > MAX_EVENT_SCOPE_ID_BYTES)
            || (position_kind == EventPositionKind::ScopeSeq
                && scope
                    .as_ref()
                    .is_none_or(|(kind, _)| kind.is_subject_projection()))
        {
            return Err(EventCursorRejection::CollectionMismatch);
        }
        Ok(Self {
            scope,
            position_kind,
        })
    }

    pub fn scope(&self) -> Option<(RuntimeEventScope, &str)> {
        self.scope.as_ref().map(|(kind, id)| (*kind, id.as_str()))
    }

    pub fn position_kind(&self) -> EventPositionKind {
        self.position_kind
    }

    pub fn position(&self, row_id: i64, seq: i64) -> i64 {
        match self.position_kind {
            EventPositionKind::RowId => row_id,
            EventPositionKind::ScopeSeq => seq,
        }
    }

    pub fn require_history(&self) -> Result<(), EventCursorRejection> {
        if self.position_kind == EventPositionKind::RowId {
            Ok(())
        } else {
            Err(EventCursorRejection::CollectionMismatch)
        }
    }

    pub fn require_stream(&self) -> Result<(), EventCursorRejection> {
        if self == &Self::stream(self.scope.clone())? {
            Ok(())
        } else {
            Err(EventCursorRejection::CollectionMismatch)
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EventCursor {
    epoch: EventEpoch,
    collection: EventCollection,
    position: i64,
    event_id_digest: [u8; 32],
}

impl EventCursor {
    /// Construct only from the epoch returned with this exact row snapshot.
    pub fn for_event(
        epoch: &EventEpoch,
        collection: &EventCollection,
        row_id: i64,
        seq: i64,
        event_id: &str,
    ) -> Result<Self, RuntimeError> {
        let position = collection.position(row_id, seq);
        if position <= 0 {
            return Err(RuntimeError::Bootstrap(
                "cannot bind a nonpositive durable event coordinate".into(),
            ));
        }
        Ok(Self {
            epoch: epoch.clone(),
            collection: collection.clone(),
            position,
            event_id_digest: event_digest(event_id),
        })
    }

    pub fn position(&self) -> i64 {
        self.position
    }
    pub fn epoch(&self) -> &EventEpoch {
        &self.epoch
    }
    pub fn collection(&self) -> &EventCollection {
        &self.collection
    }

    pub fn validate_context(
        &self,
        epoch: &EventEpoch,
        collection: &EventCollection,
    ) -> Result<(), EventCursorRejection> {
        if &self.collection != collection {
            return Err(EventCursorRejection::CollectionMismatch);
        }
        if &self.epoch != epoch {
            return Err(EventCursorRejection::EpochMismatch);
        }
        Ok(())
    }

    pub fn validate_anchor(&self, event_id: Option<&str>) -> Result<(), EventCursorRejection> {
        if event_id.is_some_and(|id| event_digest(id) == self.event_id_digest) {
            Ok(())
        } else {
            Err(EventCursorRejection::AnchorMismatch)
        }
    }

    pub fn encode(&self) -> String {
        let (kind, id) = match self.collection.scope() {
            Some((kind, id)) => (kind.as_str(), hex(id.as_bytes())),
            None => ("global", "-".to_owned()),
        };
        let axis = match self.collection.position_kind {
            EventPositionKind::RowId => "r",
            EventPositionKind::ScopeSeq => "s",
        };
        format!(
            "ev1.{}.{axis}.{kind}.{id}.{}.{}",
            self.epoch.as_str(),
            self.position,
            hex(&self.event_id_digest)
        )
    }

    pub fn decode(raw: &str) -> Result<Self, EventCursorRejection> {
        if raw.is_empty() || raw.len() > MAX_EVENT_CURSOR_BYTES || !raw.is_ascii() {
            return Err(EventCursorRejection::Malformed);
        }
        let parts: Vec<_> = raw.split('.').collect();
        if parts.len() != 7 || parts[0] != "ev1" {
            return Err(EventCursorRejection::Malformed);
        }
        let epoch =
            EventEpoch::from_persisted(parts[1]).map_err(|_| EventCursorRejection::Malformed)?;
        let axis = match parts[2] {
            "r" => EventPositionKind::RowId,
            "s" => EventPositionKind::ScopeSeq,
            _ => return Err(EventCursorRejection::Malformed),
        };
        let scope = if parts[3] == "global" {
            if parts[4] != "-" {
                return Err(EventCursorRejection::Malformed);
            }
            None
        } else {
            let kind = RuntimeEventScope::from_str(parts[3])
                .filter(|kind| kind.as_str() == parts[3])
                .ok_or(EventCursorRejection::Malformed)?;
            if parts[4].len() > 2 * MAX_EVENT_SCOPE_ID_BYTES {
                return Err(EventCursorRejection::Malformed);
            }
            let id = String::from_utf8(unhex(parts[4]).ok_or(EventCursorRejection::Malformed)?)
                .map_err(|_| EventCursorRejection::Malformed)?;
            Some((kind, id))
        };
        let collection =
            EventCollection::new(scope, axis).map_err(|_| EventCursorRejection::Malformed)?;
        let position = parts[5]
            .parse::<i64>()
            .ok()
            .filter(|p| *p > 0 && p.to_string() == parts[5])
            .ok_or(EventCursorRejection::Malformed)?;
        if !lower_hex(parts[6], 64) {
            return Err(EventCursorRejection::Malformed);
        }
        let event_id_digest = unhex(parts[6])
            .and_then(|v| v.try_into().ok())
            .ok_or(EventCursorRejection::Malformed)?;
        let cursor = Self {
            epoch,
            collection,
            position,
            event_id_digest,
        };
        if cursor.encode() != raw {
            return Err(EventCursorRejection::Malformed);
        }
        Ok(cursor)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Error)]
pub enum EventCursorRejection {
    #[error("invalid or unsupported canonical event cursor")]
    Malformed,
    #[error("event cursor does not identify this exact collection and position kind")]
    CollectionMismatch,
    #[error("event cursor belongs to another journal incarnation; inspect snapshot lineage before starting a new continuation")]
    EpochMismatch,
    #[error("event cursor anchor is absent or identifies a different event")]
    AnchorMismatch,
}

impl EventCursorRejection {
    pub const fn code(self) -> &'static str {
        match self {
            Self::Malformed => "event_cursor_invalid",
            Self::CollectionMismatch => "event_cursor_collection_mismatch",
            Self::EpochMismatch => "event_cursor_epoch_mismatch",
            Self::AnchorMismatch => "event_cursor_anchor_mismatch",
        }
    }
}

#[derive(Debug, Error)]
pub enum EventReadError {
    #[error(transparent)]
    Cursor(#[from] EventCursorRejection),
    #[error(transparent)]
    Runtime(#[from] RuntimeError),
}

#[derive(Clone, Debug)]
pub struct BoundEventPage<T> {
    pub epoch: EventEpoch,
    pub rows: Vec<T>,
}

fn event_digest(event_id: &str) -> [u8; 32] {
    let mut digest = Context::new(&SHA256);
    digest.update(b"gooselake:event-cursor:event-id:v1\0");
    digest.update(event_id.as_bytes());
    digest
        .finish()
        .as_ref()
        .try_into()
        .expect("SHA256 is 32 bytes")
}

fn lower_hex(value: &str, length: usize) -> bool {
    value.len() == length
        && value
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}

fn hex(bytes: &[u8]) -> String {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    let mut encoded = String::with_capacity(bytes.len() * 2);
    for &byte in bytes {
        encoded.push(DIGITS[(byte >> 4) as usize] as char);
        encoded.push(DIGITS[(byte & 15) as usize] as char);
    }
    encoded
}

fn unhex(value: &str) -> Option<Vec<u8>> {
    if value.len() % 2 != 0 || !lower_hex(value, value.len()) {
        return None;
    }
    fn digit(byte: u8) -> u8 {
        if byte.is_ascii_digit() {
            byte - b'0'
        } else {
            byte - b'a' + 10
        }
    }
    Some(
        value
            .as_bytes()
            .chunks_exact(2)
            .map(|pair| (digit(pair[0]) << 4) | digit(pair[1]))
            .collect(),
    )
}
