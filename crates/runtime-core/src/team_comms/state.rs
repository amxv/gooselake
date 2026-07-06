use std::collections::HashMap;

use crate::{TeamDeliveryRecord, TeamMemberRecord, TeamMessageRecord, TeamRecord};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum DeliveryAttemptTrigger {
    Queue,
    Retry,
    TurnCompletedBoundary,
    StartupRecovery,
}

#[derive(Default)]
pub(super) struct TeamCommsState {
    pub(super) teams: HashMap<String, TeamRecord>,
    pub(super) members_by_team: HashMap<String, HashMap<String, TeamMemberRecord>>,
    pub(super) messages: HashMap<String, TeamMessageRecord>,
    pub(super) deliveries: HashMap<String, TeamDeliveryRecord>,
    pub(super) team_message_ids: HashMap<String, Vec<String>>,
    pub(super) team_delivery_ids: HashMap<String, Vec<String>>,
    pub(super) message_delivery_ids: HashMap<String, Vec<String>>,
    pub(super) recipient_delivery_ids: HashMap<String, Vec<String>>,
    pub(super) idempotency_index: HashMap<String, String>,
}
