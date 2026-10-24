//! Transactional provider compaction observations. The provider supplies
//! evidence for an exact native attachment, never workspace or recipient
//! authority. Terminal side effects share the state transition transaction.

use runtime_core::{
    CompactionObservationReceipt, ProviderCompactSessionOutcome, ProviderCompactionObservation,
    RuntimeError, SessionCompactionRun, TeamHookEventName, WorkspaceSystemNoticeRecord,
};
use rusqlite::{params, OptionalExtension, Transaction, TransactionBehavior};
use serde_json::json;

use crate::db::{db_error, open_connection};
use crate::repository_team_hooks::{active_workspace_hook_targets_tx, queue_team_hook_trigger_tx};
use crate::repository_workspace_notice::{workspace_active_lead_tx, workspace_notice_policy_tx};
use crate::SqliteRuntimeRepository;

fn read_run(raw: String) -> Result<SessionCompactionRun, RuntimeError> {
    serde_json::from_str(&raw)
        .map_err(|error| RuntimeError::Bootstrap(format!("invalid compaction row: {error}")))
}

/// The exactly-once notice and hook claims are committed with completion,
/// never after a potentially failing separate model/provider request.
pub(crate) fn admit_compaction_completion_tx(
    tx: &Transaction<'_>,
    run: &SessionCompactionRun,
    at_ms: i64,
) -> Result<(), RuntimeError> {
    let (workspace_id, alias, creator_id, subscription): (String, String, Option<String>, String) =
        tx.query_row(
            "SELECT a.workspace_id,a.identity_alias,p.creator_session_id,
                    p.creator_compaction_subscription
             FROM workspace_agents a
             JOIN workspace_agent_profiles p ON p.session_id=a.session_id
             JOIN workspaces w ON w.workspace_id=a.workspace_id
             WHERE a.session_id=?1 AND a.lifecycle_state='active'
               AND w.lifecycle_state='active'",
            [&run.agent_id],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
        )
        .map_err(|error| db_error("failed identifying compaction workspace", error))?;
    let policy = workspace_notice_policy_tx(tx, &workspace_id)?.policy;
    let mut recipients = std::collections::BTreeSet::new();
    if subscription == "auto" {
        if let Some(creator_id) = creator_id {
            let creator_active: bool = tx
                .query_row(
                    "SELECT EXISTS(SELECT 1 FROM workspace_agents
                 WHERE session_id=?1 AND workspace_id=?2 AND lifecycle_state='active')",
                    params![creator_id, workspace_id],
                    |row| row.get(0),
                )
                .map_err(|error| db_error("failed verifying compaction creator", error))?;
            if creator_active && creator_id != run.agent_id {
                recipients.insert(creator_id);
            }
        }
    }
    if policy.compaction_to_lead {
        if let Some((lead_id, _)) = workspace_active_lead_tx(tx, &workspace_id)? {
            if lead_id != run.agent_id {
                recipients.insert(lead_id);
            }
        }
    }
    if policy.compaction_to_team {
        let mut stmt = tx
            .prepare(
                "SELECT session_id FROM workspace_agents WHERE workspace_id=?1
             AND lifecycle_state='active' ORDER BY session_id",
            )
            .map_err(|error| db_error("failed listing compaction team recipients", error))?;
        let all = stmt
            .query_map([&workspace_id], |row| row.get::<_, String>(0))
            .map_err(|error| db_error("failed querying compaction team", error))?
            .collect::<Result<Vec<_>, _>>()
            .map_err(|error| db_error("failed collecting compaction team", error))?;
        for id in all {
            if id != run.agent_id {
                recipients.insert(id);
            }
        }
    }
    let event_id = format!("compaction:{}", run.run_id);
    for recipient in recipients {
        let notice = WorkspaceSystemNoticeRecord::lifecycle(
            &workspace_id,
            &recipient,
            &event_id,
            "compaction",
            format!(
                "{alias} completed context compaction in the workspace team. Run: {}. Trigger: {}. This is a system notice, not a user request.",
                run.run_id,
                run.trigger.as_deref().unwrap_or("provider"),
            ),
            at_ms,
        );
        SqliteRuntimeRepository::insert_workspace_system_notice_tx(tx, &notice)?;
    }
    let targets = active_workspace_hook_targets_tx(tx, &workspace_id)?;
    queue_team_hook_trigger_tx(
        tx,
        &workspace_id,
        TeamHookEventName::PostCompaction,
        &format!("workspace-hook:compaction:{}", run.run_id),
        Some(&run.agent_id),
        json!({
            "kind": "compaction",
            "run_id": run.run_id,
            "agent_id": run.agent_id,
            "source": run.source,
            "trigger": run.trigger,
            "pre_tokens": run.pre_tokens,
            "post_tokens": run.post_tokens,
            "context_window_size": run.context_window_size,
        }),
        &targets,
        at_ms,
    )?;
    Ok(())
}

impl SqliteRuntimeRepository {
    pub fn observe_provider_compaction(
        &self,
        observation: &ProviderCompactionObservation,
    ) -> Result<CompactionObservationReceipt, RuntimeError> {
        if observation.proposed_run_id.len() > 128
            || !observation.proposed_run_id.starts_with("cmp_")
            || observation.agent_id.len() > 128
            || observation.provider_session_ref.trim().is_empty()
            || observation.provider_session_ref.len() > 1024
            || observation
                .canonical_provider_session_ref
                .as_deref()
                .is_some_and(|value| value.len() > 1024)
            || observation
                .turn_id
                .as_deref()
                .is_some_and(|value| value.len() > 128)
            || observation
                .trigger
                .as_deref()
                .is_some_and(|value| value.len() > 128)
            || observation.observed_at_ms < 0
            || !matches!(
                observation.phase.as_str(),
                "started" | "compacting" | "completed" | "failed" | "cancelled"
            )
        {
            return Err(RuntimeError::ProtocolViolation(
                "invalid provider compaction observation".into(),
            ));
        }
        let mut connection = open_connection(&self.database_path)?;
        let tx = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|error| db_error("failed beginning provider compaction observation", error))?;
        let identity: Option<(u64, i64)> = tx
            .query_row(
                "SELECT a.revision,s.updated_at FROM workspace_agents a
             JOIN sessions s ON s.id=a.session_id
             JOIN workspaces w ON w.workspace_id=a.workspace_id
             WHERE a.session_id=?1 AND s.provider=?2 AND s.provider_session_ref=?3
               AND s.canonical_provider_session_ref IS ?4
               AND a.lifecycle_state='active' AND w.lifecycle_state='active'",
                params![
                    observation.agent_id,
                    observation.provider.as_str(),
                    observation.provider_session_ref,
                    observation.canonical_provider_session_ref
                ],
                |row| {
                    let revision: i64 = row.get(0)?;
                    let updated_at: i64 = row.get(1)?;
                    Ok((u64::try_from(revision).unwrap_or(0), updated_at))
                },
            )
            .optional()
            .map_err(|error| db_error("failed verifying compaction native attachment", error))?;
        let Some((revision, session_updated_at)) = identity else {
            return Err(RuntimeError::Conflict(
                "stale provider compaction observation for another attachment".into(),
            ));
        };
        if let Some(turn_id) = observation.turn_id.as_deref() {
            let owned: bool = tx
                .query_row(
                    "SELECT EXISTS(SELECT 1 FROM turns WHERE id=?1 AND session_id=?2)",
                    params![turn_id, observation.agent_id],
                    |row| row.get(0),
                )
                .map_err(|error| db_error("failed verifying compaction turn owner", error))?;
            if !owned {
                return Err(RuntimeError::Conflict(
                    "compaction observation has no matching runtime turn".into(),
                ));
            }
        }
        let pending: Option<String> = tx
            .query_row(
                "SELECT record_json FROM session_compaction_runs
             WHERE session_id=?1
               AND phase IN ('intended','accepted','started','compacting','manual_review')
             LIMIT 1",
                [&observation.agent_id],
                |row| row.get(0),
            )
            .optional()
            .map_err(|error| db_error("failed locating current compaction run", error))?;
        let was_pending = pending.is_some();
        let terminal = matches!(
            observation.phase.as_str(),
            "completed" | "failed" | "cancelled"
        );
        let mut run = if let Some(raw) = pending {
            let pending_run = read_run(raw)?;
            if pending_run.provider_session_ref != observation.provider_session_ref
                || pending_run.canonical_provider_session_ref
                    != observation.canonical_provider_session_ref
                || pending_run.agent_revision != revision
            {
                return Err(RuntimeError::Conflict(
                    "compaction signal cannot settle a different durable attachment".into(),
                ));
            }
            if pending_run.phase == "manual_review" {
                return Err(RuntimeError::Conflict(
                    "uncorrelated native compaction signal cannot settle manual review".into(),
                ));
            }
            if pending_run.source == "manual"
                && observation.turn_id.is_some()
                && observation.trigger.as_deref() != Some("manual")
            {
                return Err(RuntimeError::Conflict(
                    "late automatic turn compaction cannot settle a manual request".into(),
                ));
            }
            if let (Some(existing), Some(observed)) = (
                pending_run.observed_turn_id.as_deref(),
                observation.turn_id.as_deref(),
            ) {
                if existing != observed {
                    return Err(RuntimeError::Conflict(
                        "compaction turn correlation changed".into(),
                    ));
                }
            }
            pending_run
        } else {
            // Native protocols do not consistently expose a stable compaction
            // item ID. Do not manufacture strong correlation: suppress only
            // a short weak-alias replay window (and repeated turn evidence).
            let recent: Option<String> = tx
                .query_row(
                    "SELECT record_json FROM session_compaction_runs
                 WHERE session_id=?1 AND phase IN ('completed','failed','cancelled')
                 ORDER BY updated_at_ms DESC LIMIT 1",
                    [&observation.agent_id],
                    |row| row.get(0),
                )
                .optional()
                .map_err(|error| db_error("failed resolving compaction replay", error))?;
            if let Some(raw) = recent {
                let prior = read_run(raw)?;
                let same_turn =
                    observation.turn_id.is_some() && prior.observed_turn_id == observation.turn_id;
                let weak_duplicate = observation.turn_id.is_none()
                    && prior.observed_turn_id.is_none()
                    && observation.observed_at_ms >= prior.updated_at_ms
                    && observation.observed_at_ms - prior.updated_at_ms <= 250;
                if prior.provider == observation.provider
                    && prior.provider_session_ref == observation.provider_session_ref
                    && prior.canonical_provider_session_ref
                        == observation.canonical_provider_session_ref
                    && (same_turn || weak_duplicate)
                {
                    if terminal && prior.phase != observation.phase {
                        return Err(RuntimeError::Conflict(
                            "contradictory terminal compaction signal within one correlation window".into(),
                        ));
                    }
                    return Ok(CompactionObservationReceipt {
                        run: prior,
                        changed: false,
                    });
                }
            }
            let run = SessionCompactionRun {
                run_id: observation.proposed_run_id.clone(),
                agent_id: observation.agent_id.clone(),
                provider: observation.provider,
                provider_session_ref: observation.provider_session_ref.clone(),
                canonical_provider_session_ref: observation.canonical_provider_session_ref.clone(),
                agent_revision: revision,
                expected_session_updated_at: session_updated_at,
                idempotency_key: None,
                phase: observation.phase.clone(),
                outcome: None,
                error_code: None,
                review_evidence: None,
                source: "provider".into(),
                confidence: Some(if observation.turn_id.is_some() {
                    "native_turn".into()
                } else {
                    "native_session".into()
                }),
                observed_turn_id: observation.turn_id.clone(),
                trigger: observation.trigger.clone(),
                pre_tokens: observation.pre_tokens,
                post_tokens: observation.post_tokens,
                context_window_size: observation.context_window_size,
                created_at_ms: observation.observed_at_ms,
                updated_at_ms: observation.observed_at_ms,
            };
            run
        };
        let previous = was_pending.then_some(run.clone());
        let before = run.phase.clone();
        if terminal {
            run.phase = observation.phase.clone();
            // An unsolicited automatic compaction has native completion
            // evidence, not a separately accepted manual provider RPC.
            run.outcome = (observation.phase == "completed" && run.source == "manual")
                .then_some(ProviderCompactSessionOutcome::Accepted);
            run.error_code = match observation.phase.as_str() {
                "failed" => Some("provider_compaction_failed".into()),
                "cancelled" => Some("provider_compaction_cancelled".into()),
                _ => None,
            };
        } else if run.phase == "intended" || run.phase == "accepted" || run.phase == "started" {
            run.phase = observation.phase.clone();
        }
        if run.trigger.is_none() {
            run.trigger = observation.trigger.clone();
        }
        if run.observed_turn_id.is_none() {
            run.observed_turn_id = observation.turn_id.clone();
        }
        if run.confidence.is_none() {
            run.confidence = Some(if observation.turn_id.is_some() {
                "native_turn".into()
            } else {
                "native_session".into()
            });
        }
        if run.pre_tokens.is_none() {
            run.pre_tokens = observation.pre_tokens;
        }
        if run.post_tokens.is_none() {
            run.post_tokens = observation.post_tokens;
        }
        if run.context_window_size.is_none() {
            run.context_window_size = observation.context_window_size;
        }
        if let Some(old) = previous.as_ref() {
            let mut candidate = run.clone();
            candidate.updated_at_ms = old.updated_at_ms;
            if candidate == *old {
                return Ok(CompactionObservationReceipt {
                    run: old.clone(),
                    changed: false,
                });
            }
        }
        run.updated_at_ms = run.updated_at_ms.max(observation.observed_at_ms);
        run.validate()?;
        let encoded = serde_json::to_string(&run).map_err(|error| {
            RuntimeError::Bootstrap(format!("invalid compaction snapshot: {error}"))
        })?;
        if was_pending {
            tx.execute(
                "UPDATE session_compaction_runs SET phase=?2,record_json=?3,updated_at_ms=?4
                 WHERE run_id=?1",
                params![run.run_id, run.phase, encoded, run.updated_at_ms],
            )
            .map_err(|error| db_error("failed advancing provider compaction", error))?;
        } else {
            tx.execute(
                "INSERT INTO session_compaction_runs
                 (run_id,session_id,idempotency_key,phase,record_json,created_at_ms,updated_at_ms)
                 VALUES (?1,?2,NULL,?3,?4,?5,?6)",
                params![
                    run.run_id,
                    run.agent_id,
                    run.phase,
                    encoded,
                    run.created_at_ms,
                    run.updated_at_ms
                ],
            )
            .map_err(|error| db_error("failed recording native compaction", error))?;
        }
        if run.phase == "completed" && (!was_pending || before != "completed") {
            admit_compaction_completion_tx(&tx, &run, run.updated_at_ms)?;
        }
        tx.commit()
            .map_err(|error| db_error("failed committing native compaction", error))?;
        Ok(CompactionObservationReceipt { run, changed: true })
    }
}
