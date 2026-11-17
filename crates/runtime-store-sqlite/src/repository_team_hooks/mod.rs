//! Durable operator-granted hook capabilities and parent-event effects.

mod application;
mod approval;
mod effects;
mod journal;
mod recovery;
mod trigger;

pub(crate) use effects::hook_is_approved as effect_grant_is_current;
pub(crate) use effects::read_effect as read_team_hook_effect;
pub(crate) use journal::{
    insert_critical_receipt_tx, journal_hook_change_tx, next_critical_sequence_tx,
    verify_critical_proof_tx, verify_hook_proofs_tx, CriticalEventProof, HookJournalProof,
};
pub(crate) use trigger::{active_workspace_hook_targets_tx, queue_team_hook_trigger_tx};

#[cfg(test)]
mod tests;
