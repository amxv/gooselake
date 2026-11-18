use runtime_core::{
    EventCollection, EventCursor, EventEpoch, EventReadError, RuntimeError, RuntimeEventRecord,
    RuntimeEventReplayReader, RuntimeEventScope,
};

#[derive(Clone)]
pub(super) enum StreamCursor {
    Legacy {
        scope: Option<(RuntimeEventScope, String)>,
        after: Option<i64>,
    },
    Bound {
        collection: EventCollection,
        after: Option<EventCursor>,
    },
}
pub(super) struct StreamPage {
    pub(super) epoch: Option<EventEpoch>,
    pub(super) rows: Vec<RuntimeEventRecord>,
}
impl StreamCursor {
    pub(super) fn read(
        &self,
        replay: &dyn RuntimeEventReplayReader,
        limit: usize,
    ) -> Result<StreamPage, EventReadError> {
        match self {
            Self::Legacy { scope, after } => Ok(StreamPage {
                epoch: None,
                rows: replay.list_runtime_events(
                    scope.as_ref().map(|(kind, id)| (*kind, id.as_str())),
                    *after,
                    limit,
                )?,
            }),
            Self::Bound { collection, after } => {
                let page = replay.list_runtime_events_bound(collection, after.as_ref(), limit)?;
                Ok(StreamPage {
                    epoch: Some(page.epoch),
                    rows: page.rows,
                })
            }
        }
    }
    pub(super) fn position(&self) -> i64 {
        match self {
            Self::Legacy { after, .. } => after.unwrap_or(0),
            Self::Bound { after, .. } => after.as_ref().map_or(0, EventCursor::position),
        }
    }
    pub(super) fn record_position(&self, record: &RuntimeEventRecord) -> i64 {
        match self {
            Self::Legacy { scope, .. } => {
                if scope
                    .as_ref()
                    .is_none_or(|(kind, _)| kind.is_subject_projection())
                {
                    record.row_id
                } else {
                    record.seq
                }
            }
            Self::Bound { collection, .. } => collection.position(record.row_id, record.seq),
        }
    }
    /// Prepare a candidate; install only after the output permit is sent.
    pub(super) fn frame_cursor(
        &self,
        page_epoch: Option<&EventEpoch>,
        record: &RuntimeEventRecord,
    ) -> Result<(String, Self), RuntimeError> {
        match self {
            Self::Legacy { scope, .. } => {
                let next = self.record_position(record);
                Ok((
                    next.to_string(),
                    Self::Legacy {
                        scope: scope.clone(),
                        after: Some(next),
                    },
                ))
            }
            Self::Bound { collection, .. } => {
                let epoch = page_epoch.ok_or_else(|| {
                    RuntimeError::Bootstrap("bound replay page omitted its journal epoch".into())
                })?;
                let next = EventCursor::for_event(
                    epoch,
                    collection,
                    record.row_id,
                    record.seq,
                    &record.event_id,
                )?;
                Ok((
                    next.encode(),
                    Self::Bound {
                        collection: collection.clone(),
                        after: Some(next),
                    },
                ))
            }
        }
    }
}
