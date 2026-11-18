//! Rebind admission freezes exact claim lifetimes before provider dispatch.
//! A reservation starts a lifetime; promotion or definitive rejection finishes
//! that reservation without allocating another token or reviving an old token.
use super::*;
use crate::repository_worktree_claim_generation as lifetime;
use runtime_core::ManagedWorktreeClaimRecord;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(super) struct BoundClaim {
    worktree_id: String,
    claim: Option<ManagedWorktreeClaimRecord>,
}

#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(super) struct ClaimFences {
    version: u8,
    previous: Option<BoundClaim>,
    destination: Option<ManagedWorktreeClaimRecord>,
}

fn conflict(message: &str) -> RuntimeError {
    RuntimeError::Conflict(format!("worktree rebind claim fence: {message}"))
}

pub(super) fn reserve(
    tx: &Transaction<'_>,
    operation: &WorkspaceAgentRebindOperation,
) -> Result<ClaimFences, RuntimeError> {
    let previous = operation
        .previous_worktree_id
        .as_deref()
        .map(|id| -> Result<BoundClaim, RuntimeError> {
            Ok(BoundClaim {
                worktree_id: id.into(),
                claim: lifetime::read(tx, id, &operation.agent_id)?,
            })
        })
        .transpose()?;
    let conflicting: i64 = tx
        .query_row(
            "SELECT COUNT(*) FROM managed_worktree_claims WHERE session_id=?1
         AND released_at IS NULL AND worktree_id IS NOT ?2",
            params![operation.agent_id, operation.previous_worktree_id],
            |row| row.get(0),
        )
        .map_err(|e| db_error("failed checking existing worktree associations", e))?;
    if conflicting != 0 {
        return Err(conflict(
            "agent has an unrecognized managed-worktree association",
        ));
    }
    let destination = if let Some(target) = operation.destination_worktree_id.as_deref() {
        let prior = lifetime::read(tx, target, &operation.agent_id)?;
        let reservation = ManagedWorktreeClaimRecord {
            worktree_id: target.into(),
            session_id: operation.agent_id.clone(),
            claim_role: "rebind_reservation".into(),
            created_at: operation.created_at,
            released_at: None,
            claim_generation: Some(lifetime::next(prior.as_ref())?),
        };
        lifetime::write(tx, &reservation)?;
        Some(reservation)
    } else {
        None
    };
    Ok(ClaimFences {
        version: 1,
        previous,
        destination,
    })
}

/// Missing historical evidence is unresolved, never rebuilt from current rows.
pub(super) fn validate(
    tx: &Transaction<'_>,
    operation: &WorkspaceAgentRebindOperation,
) -> Result<ClaimFences, RuntimeError> {
    let raw: Option<String> = tx
        .query_row(
            "SELECT evidence_json FROM runtime_operation_transitions
         WHERE operation_id=?1 AND sequence=1 AND from_phase IS NULL AND to_phase='requested'",
            [&operation.operation_id],
            |row| row.get(0),
        )
        .optional()
        .map_err(|e| db_error("failed reading admitted rebind claim fences", e))?
        .flatten();
    let value: serde_json::Value = serde_json::from_str(
        raw.as_deref()
            .ok_or_else(|| conflict("missing admission evidence"))?,
    )
    .map_err(|_| conflict("invalid admission evidence"))?;
    let value = value
        .get("claim_fences")
        .ok_or_else(|| conflict("historical intent lacks lifetime authority"))?;
    if value.get("previous").is_none()
        || value.get("destination").is_none()
        || value
            .get("previous")
            .is_some_and(|p| !p.is_null() && p.get("claim").is_none())
    {
        return Err(conflict("incomplete admission claim snapshot"));
    }
    let fences: ClaimFences = serde_json::from_value(value.clone())
        .map_err(|_| conflict("invalid admission claim snapshot"))?;
    if fences.version != 1
        || fences.previous.as_ref().map(|p| p.worktree_id.as_str())
            != operation.previous_worktree_id.as_deref()
        || fences.destination.as_ref().map(|p| p.worktree_id.as_str())
            != operation.destination_worktree_id.as_deref()
    {
        return Err(conflict("admission snapshot does not bind the exact route"));
    }
    if let Some(previous) = &fences.previous {
        if let Some(claim) = &previous.claim {
            lifetime::generation(claim)?;
            if claim.worktree_id != previous.worktree_id || claim.session_id != operation.agent_id {
                return Err(conflict("previous claim belongs to another key"));
            }
        }
    }
    if let Some(destination) = &fences.destination {
        lifetime::generation(destination)?;
        if destination.session_id != operation.agent_id
            || destination.claim_role != "rebind_reservation"
            || destination.released_at.is_some()
            || destination.created_at != operation.created_at
        {
            return Err(conflict("invalid admitted reservation"));
        }
    }
    if let (Some(previous), Some(destination)) = (&fences.previous, &fences.destination) {
        if previous.worktree_id == destination.worktree_id
            && destination.claim_generation != Some(lifetime::next(previous.claim.as_ref())?)
        {
            return Err(conflict(
                "saved reservation is not the successor of the observed lifetime",
            ));
        }
    }
    verify(tx, operation, &fences.current())?;
    Ok(fences)
}

impl ClaimFences {
    fn current(&self) -> Vec<BoundClaim> {
        let mut result = Vec::new();
        if let Some(previous) = &self.previous {
            if self
                .destination
                .as_ref()
                .is_none_or(|d| d.worktree_id != previous.worktree_id)
            {
                result.push(previous.clone());
            }
        }
        if let Some(destination) = &self.destination {
            result.push(BoundClaim {
                worktree_id: destination.worktree_id.clone(),
                claim: Some(destination.clone()),
            });
        }
        result
    }
}

pub(super) fn verify(
    tx: &Transaction<'_>,
    operation: &WorkspaceAgentRebindOperation,
    expected: &[BoundClaim],
) -> Result<(), RuntimeError> {
    for bound in expected {
        if lifetime::read(tx, &bound.worktree_id, &operation.agent_id)? != bound.claim {
            return Err(conflict("observed claim changed during rebind"));
        }
    }
    Ok(())
}

pub(super) fn promote(
    tx: &Transaction<'_>,
    fences: &ClaimFences,
    at: i64,
) -> Result<Vec<BoundClaim>, RuntimeError> {
    let mut expected = fences.current();
    for bound in &mut expected {
        if let Some(claim) = &mut bound.claim {
            if fences
                .destination
                .as_ref()
                .is_some_and(|d| d.worktree_id == bound.worktree_id)
            {
                claim.claim_role = "owner".into();
                claim.created_at = at;
            } else if claim.released_at.is_none() {
                claim.released_at = Some(at);
            } else {
                continue; // Terminal previous history needs verification, not a write.
            }
            lifetime::write(tx, claim)?;
        }
    }
    Ok(expected)
}

pub(super) fn require_previous_route(
    tx: &Transaction<'_>,
    operation: &WorkspaceAgentRebindOperation,
) -> Result<(), RuntimeError> {
    let route: Option<(i64, Option<String>, Option<String>, Option<String>, String)> = tx
        .query_row(
            "SELECT a.revision,s.cwd,s.worktree_id,s.active_turn_id,a.recreation_policy_json
         FROM workspace_agents a JOIN sessions s ON s.id=a.session_id
         WHERE a.session_id=?1 AND a.workspace_id=?2 AND a.lifecycle_state='active'",
            params![operation.agent_id, operation.workspace_id],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?, r.get(4)?)),
        )
        .optional()
        .map_err(|e| db_error("failed checking rejected rebind route", e))?;
    let Some((revision, cwd, worktree_id, turn, policy)) = route else {
        return Err(conflict("previous route is unavailable"));
    };
    let policy: WorkspaceAgentRecreationPolicy = serde_json::from_str(&policy)
        .map_err(|_| conflict("previous recreation policy is invalid"))?;
    if Some(revision) != i64::try_from(operation.expected_revision).ok()
        || cwd.as_deref() != Some(operation.previous_cwd.as_str())
        || worktree_id != operation.previous_worktree_id
        || turn.is_some()
        || policy.authoritative_cwd != operation.previous_cwd
    {
        return Err(conflict("previous route was superseded"));
    }
    Ok(())
}

/// Called only for supported definitive rejection, never an unknown outcome.
pub(super) fn reject(
    tx: &Transaction<'_>,
    operation: &WorkspaceAgentRebindOperation,
    at: i64,
) -> Result<Vec<BoundClaim>, RuntimeError> {
    let fences = validate(tx, operation)?;
    let mut expected = fences.current();
    if let Some(destination) = &fences.destination {
        let prior = fences
            .previous
            .as_ref()
            .filter(|p| p.worktree_id == destination.worktree_id)
            .and_then(|p| p.claim.as_ref())
            .filter(|c| c.released_at.is_none());
        let replacement = if let Some(prior) = prior {
            require_previous_route(tx, operation)?;
            ManagedWorktreeClaimRecord {
                claim_generation: destination.claim_generation,
                ..prior.clone()
            }
        } else {
            ManagedWorktreeClaimRecord {
                released_at: Some(at),
                ..destination.clone()
            }
        };
        lifetime::write(tx, &replacement)?;
        let bound = expected
            .iter_mut()
            .find(|p| p.worktree_id == destination.worktree_id)
            .ok_or_else(|| conflict("destination snapshot is absent"))?;
        bound.claim = Some(replacement);
    }
    Ok(expected)
}

/// Preservation-only snapshot for ambiguous or historical intent quarantine.
/// It grants no mutation or settlement authority and needs no old seq1 proof.
pub(super) fn current_snapshot(
    tx: &Transaction<'_>,
    operation: &WorkspaceAgentRebindOperation,
) -> Result<Vec<BoundClaim>, RuntimeError> {
    let mut keys = std::collections::BTreeSet::new();
    keys.extend(operation.previous_worktree_id.iter());
    keys.extend(operation.destination_worktree_id.iter());
    keys.into_iter()
        .map(|id| {
            Ok(BoundClaim {
                worktree_id: id.clone(),
                claim: lifetime::read(tx, id, &operation.agent_id)?,
            })
        })
        .collect()
}
