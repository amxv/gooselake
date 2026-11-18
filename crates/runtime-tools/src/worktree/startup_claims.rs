//! Historical claim planning keeps raw source and destination lifetimes separate.
use super::*;

type Key = (String, String);
fn key(record: &ManagedWorktreeClaimRecord) -> Key {
    (record.worktree_id.clone(), record.session_id.clone())
}
fn insert_plan(
    plan: &mut BTreeMap<Key, ManagedWorktreeClaimRecord>,
    record: ManagedWorktreeClaimRecord,
) -> Result<(), RuntimeError> {
    if let Some(prior) = plan.insert(key(&record), record.clone()) {
        if prior != record {
            return Err(RuntimeError::Conflict(
                "startup planned conflicting writes to one raw claim key".into(),
            ));
        }
    }
    Ok(())
}

pub(super) fn plan(
    hydrated: &runtime_core::RuntimeHydratedState,
    winners: &BTreeMap<String, ManagedWorktreeRecord>,
    loser_to_winner: &BTreeMap<String, String>,
    now: i64,
) -> Result<Vec<ManagedWorktreeClaimRecord>, RuntimeError> {
    let mut originals = BTreeMap::new();
    for row in &hydrated.managed_worktree_claims {
        if originals.insert(key(row), row).is_some() {
            return Err(RuntimeError::Conflict(
                "startup hydration contains duplicate raw claim keys".into(),
            ));
        }
    }
    // Entity IDs are opaque foreign keys. Whitespace does not prove that two
    // different agents or worktree records have the same identity.
    let sessions = hydrated
        .sessions
        .iter()
        .map(|s| s.id.as_str())
        .collect::<BTreeSet<_>>();
    let mut groups = BTreeMap::<Key, ManagedWorktreeClaimRecord>::new();
    let mut plan = BTreeMap::new();
    for original in &hydrated.managed_worktree_claims {
        // Terminal aliases are history, not authority to release a canonical
        // active destination again on the next restart.
        if original.released_at.is_some() {
            continue;
        }
        if original
            .claim_generation
            .and_then(|g| i64::try_from(g).ok())
            .is_none_or(|g| g <= 0)
        {
            return Err(RuntimeError::Conflict(
                "active historical claim lacks an observed lifetime token".into(),
            ));
        }
        if original.worktree_id.is_empty() || original.session_id.is_empty() {
            let mut terminal = original.clone();
            terminal.released_at = Some(now);
            insert_plan(&mut plan, terminal)?;
            continue;
        }
        let mut destination = original.clone();
        if let Some(winner) = loser_to_winner.get(&original.worktree_id) {
            destination.worktree_id = winner.clone();
            let mut terminal = original.clone();
            terminal.released_at = Some(now);
            insert_plan(&mut plan, terminal)?;
        }
        destination.claim_role = original.claim_role.trim().to_string();
        if destination.claim_role.is_empty() {
            destination.claim_role = "consumer".into();
        }
        let destination_key = key(&destination);
        let prior = originals.get(&destination_key);
        if prior.is_some_and(|row| row.released_at.is_some()) {
            return Err(RuntimeError::Conflict(
                "historical claim remap cannot reactivate a terminal destination".into(),
            ));
        }
        // Every contributor gets the destination's own observed token. None
        // explicitly asks SQLite to allocate 1 only if this target stays absent.
        destination.claim_generation = prior.and_then(|row| row.claim_generation);
        if !winners.contains_key(&destination.worktree_id)
            || !sessions.contains(destination.session_id.as_str())
        {
            destination.released_at = Some(now);
        }
        if let Some(existing) = groups.get_mut(&destination_key) {
            existing.created_at = existing.created_at.min(destination.created_at);
            existing.claim_role = RuntimeWorktreeService::merge_claim_role(
                &existing.claim_role,
                &destination.claim_role,
            );
            if existing.released_at != destination.released_at
                || existing.claim_generation != destination.claim_generation
            {
                return Err(RuntimeError::Conflict(
                    "historical claim contributors disagree on destination authority".into(),
                ));
            }
        } else {
            groups.insert(destination_key, destination);
        }
    }
    let mut active = BTreeMap::<String, Vec<Key>>::new();
    for (key, row) in &groups {
        if row.released_at.is_none() {
            active
                .entry(row.session_id.clone())
                .or_default()
                .push(key.clone());
        }
    }
    for keys in active.values_mut() {
        keys.sort_by(|left, right| {
            let time = |key: &Key| {
                winners
                    .get(&key.0)
                    .map(|w| w.created_at)
                    .unwrap_or(i64::MAX)
            };
            time(left)
                .cmp(&time(right))
                .then_with(|| left.0.cmp(&right.0))
        });
        let mut primary = false;
        for key in keys {
            let row = groups.get_mut(key).expect("key selected from claim groups");
            if row.claim_role == "rebind_reservation" {
                continue;
            }
            if !primary {
                primary = true;
            } else {
                row.released_at = Some(now);
            }
        }
    }
    for row in groups.into_values() {
        insert_plan(&mut plan, row)?;
    }
    Ok(plan
        .into_values()
        .filter(|row| !originals.get(&key(row)).is_some_and(|old| *old == row))
        .collect())
}
