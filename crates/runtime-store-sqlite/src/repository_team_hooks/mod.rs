//! Durable operator-granted hook capabilities and parent-event effects.

mod application;
mod approval;
mod effects;
mod trigger;

pub(crate) use trigger::{active_workspace_hook_targets_tx, queue_team_hook_trigger_tx};

#[cfg(test)]
mod tests;
