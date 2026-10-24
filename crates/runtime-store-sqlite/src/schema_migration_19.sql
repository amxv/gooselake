-- Retirement is an admitted, durable multi-resource operation. The workspace
-- remains formally active until terminal reconciliation, but this row freezes
-- new work in the same SQLite authority domain, including legacy ingress.
-- Do not delete a fence to recover an ambiguous provider/native effect.
CREATE TABLE workspace_retirement_fences (
  workspace_id TEXT PRIMARY KEY REFERENCES workspaces(workspace_id) ON DELETE RESTRICT,
  operation_id TEXT NOT NULL UNIQUE REFERENCES runtime_operations(operation_id) ON DELETE RESTRICT,
  phase TEXT NOT NULL CHECK(phase IN ('frozen', 'manual_review', 'retired')),
  snapshot_json TEXT NOT NULL CHECK(json_valid(snapshot_json)),
  created_at INTEGER NOT NULL,
  updated_at INTEGER NOT NULL
);

-- Managed checkouts normally live outside the canonical repository root.
-- Every native execution route remains fenced while retirement is unresolved
-- or terminal, including legacy sessions/processes without workspace ownership.
CREATE VIEW workspace_retirement_routes AS
SELECT f.workspace_id,f.operation_id,w.canonical_root AS route
FROM workspace_retirement_fences f JOIN workspaces w ON w.workspace_id=f.workspace_id
UNION
SELECT f.workspace_id,f.operation_id,t.worktree_cwd AS route
FROM workspace_retirement_fences f JOIN workspaces w ON w.workspace_id=f.workspace_id
JOIN managed_worktrees t ON t.repo_root=w.canonical_root
WHERE t.worktree_cwd<>'' AND substr(t.worktree_cwd,1,1)='/';

-- Freeze writes, not just API calls. The old /v1 paths and background workers
-- must be unable to admit new work after the frozen operation commits.
CREATE TRIGGER retirement_block_agent_create
BEFORE INSERT ON workspace_agents
WHEN NOT EXISTS(SELECT 1 FROM workspace_agents WHERE session_id=NEW.session_id)
 AND EXISTS(SELECT 1 FROM workspace_retirement_fences WHERE workspace_id=NEW.workspace_id)
BEGIN SELECT RAISE(ABORT,'workspace retirement freezes agent creation'); END;

CREATE TRIGGER retirement_block_agent_restore
BEFORE UPDATE OF lifecycle_state ON workspace_agents
WHEN NEW.lifecycle_state='active' AND OLD.lifecycle_state<>'active'
 AND EXISTS(SELECT 1 FROM workspace_retirement_fences WHERE workspace_id=NEW.workspace_id)
BEGIN SELECT RAISE(ABORT,'workspace retirement freezes agent restoration'); END;

CREATE TRIGGER retirement_block_ownership_create
BEFORE INSERT ON workspace_session_ownership
WHEN NOT EXISTS(SELECT 1 FROM workspace_session_ownership WHERE session_id=NEW.session_id)
 AND EXISTS(SELECT 1 FROM workspace_retirement_fences WHERE workspace_id=NEW.workspace_id)
BEGIN SELECT RAISE(ABORT,'workspace retirement freezes agent ownership'); END;

CREATE TRIGGER retirement_block_workspace_lead
BEFORE UPDATE OF lead_agent_id ON workspaces
WHEN NEW.lead_agent_id IS NOT NULL AND EXISTS(
 SELECT 1 FROM workspace_retirement_fences WHERE workspace_id=NEW.workspace_id)
BEGIN SELECT RAISE(ABORT,'workspace retirement freezes lead promotion'); END;

CREATE TRIGGER retirement_block_turn_admission
BEFORE INSERT ON turn_admissions
WHEN NOT EXISTS(SELECT 1 FROM turn_admissions WHERE turn_id=NEW.turn_id)
 AND (EXISTS(SELECT 1 FROM workspace_retirement_fences f
 JOIN workspace_session_ownership o ON o.workspace_id=f.workspace_id
 WHERE o.session_id=NEW.session_id)
 OR EXISTS(SELECT 1 FROM workspace_retirement_routes r JOIN sessions s ON s.id=NEW.session_id
 WHERE s.cwd=r.route OR substr(s.cwd,1,length(r.route)+1)=r.route||'/'))
BEGIN SELECT RAISE(ABORT,'workspace retirement freezes new turns'); END;

-- A turn may have been admitted before the workspace was frozen but not yet
-- dispatched. Deny its first provider-dispatch claim after the freeze, while
-- still allowing already-started native turns to record truthful outcomes.
CREATE TRIGGER retirement_block_queued_turn_dispatch
BEFORE UPDATE OF dispatch_state ON turn_admissions
WHEN OLD.dispatch_state='pending' AND NEW.dispatch_state='dispatching'
 AND (EXISTS(SELECT 1 FROM workspace_retirement_fences f
       JOIN workspace_session_ownership o ON o.workspace_id=f.workspace_id
       WHERE o.session_id=NEW.session_id)
 OR EXISTS(SELECT 1 FROM workspace_retirement_routes r JOIN sessions s ON s.id=NEW.session_id
       WHERE s.cwd=r.route OR substr(s.cwd,1,length(r.route)+1)=r.route||'/'))
BEGIN SELECT RAISE(ABORT,'workspace retirement freezes pending turn dispatch'); END;

CREATE TRIGGER retirement_block_new_compaction
BEFORE INSERT ON session_compaction_runs
WHEN NOT EXISTS(SELECT 1 FROM session_compaction_runs WHERE run_id=NEW.run_id)
 AND EXISTS(SELECT 1 FROM workspace_retirement_fences f
 JOIN workspace_session_ownership o ON o.workspace_id=f.workspace_id
 WHERE o.session_id=NEW.session_id)
BEGIN SELECT RAISE(ABORT,'workspace retirement freezes native compaction'); END;

CREATE TRIGGER retirement_block_new_rebind
BEFORE INSERT ON workspace_agent_rebinds
WHEN NOT EXISTS(SELECT 1 FROM workspace_agent_rebinds WHERE operation_id=NEW.operation_id)
 AND EXISTS(SELECT 1 FROM workspace_retirement_fences WHERE workspace_id=NEW.workspace_id)
BEGIN SELECT RAISE(ABORT,'workspace retirement freezes native workspace rebind'); END;

CREATE TRIGGER retirement_block_new_edit
BEFORE INSERT ON session_edit_reruns
WHEN NOT EXISTS(SELECT 1 FROM session_edit_reruns WHERE operation_id=NEW.operation_id)
 AND EXISTS(SELECT 1 FROM workspace_retirement_fences f
 JOIN workspace_session_ownership o ON o.workspace_id=f.workspace_id
 WHERE o.session_id=NEW.session_id)
BEGIN SELECT RAISE(ABORT,'workspace retirement freezes native historical edit'); END;

CREATE TRIGGER retirement_block_legacy_turn
BEFORE INSERT ON turns
WHEN NOT EXISTS(SELECT 1 FROM turns WHERE id=NEW.id)
 AND (EXISTS(SELECT 1 FROM workspace_retirement_fences f
 JOIN workspace_session_ownership o ON o.workspace_id=f.workspace_id
 WHERE o.session_id=NEW.session_id)
 OR EXISTS(SELECT 1 FROM workspace_retirement_routes r JOIN sessions s ON s.id=NEW.session_id
 WHERE s.cwd=r.route OR substr(s.cwd,1,length(r.route)+1)=r.route||'/'))
BEGIN SELECT RAISE(ABORT,'workspace retirement freezes legacy turns'); END;

CREATE TRIGGER retirement_block_session_create
BEFORE INSERT ON sessions
WHEN NOT EXISTS(SELECT 1 FROM sessions WHERE id=NEW.id)
 AND NEW.cwd IS NOT NULL AND EXISTS(SELECT 1 FROM workspace_retirement_routes r
 WHERE NEW.cwd=r.route OR substr(NEW.cwd,1,length(r.route)+1)=r.route||'/')
BEGIN SELECT RAISE(ABORT,'workspace retirement freezes legacy session creation'); END;

CREATE TRIGGER retirement_block_session_rebind
BEFORE UPDATE OF cwd ON sessions
WHEN NEW.cwd IS NOT NULL AND NEW.cwd IS NOT OLD.cwd
 AND EXISTS(SELECT 1 FROM workspace_retirement_routes r
 WHERE NEW.cwd=r.route OR substr(NEW.cwd,1,length(r.route)+1)=r.route||'/')
BEGIN SELECT RAISE(ABORT,'workspace retirement freezes legacy session rebind'); END;

CREATE TRIGGER retirement_block_session_reopen
BEFORE UPDATE OF status,active_turn_id ON sessions
WHEN ((OLD.status='closed' AND NEW.status<>'closed')
 OR (NEW.active_turn_id IS NOT NULL AND NEW.active_turn_id IS NOT OLD.active_turn_id))
 AND (EXISTS(SELECT 1 FROM workspace_retirement_fences f JOIN workspace_session_ownership o
 ON o.workspace_id=f.workspace_id WHERE o.session_id=NEW.id)
 OR EXISTS(SELECT 1 FROM workspace_retirement_routes r
 WHERE NEW.cwd=r.route OR substr(NEW.cwd,1,length(r.route)+1)=r.route||'/'))
BEGIN SELECT RAISE(ABORT,'workspace retirement freezes session reopening'); END;

CREATE TRIGGER retirement_block_terminal_turn_reopen
BEFORE UPDATE OF status ON turns
WHEN OLD.status IN ('completed','failed','interrupted','cancelled')
 AND NEW.status NOT IN ('completed','failed','interrupted','cancelled')
 AND (EXISTS(SELECT 1 FROM workspace_retirement_fences f JOIN workspace_session_ownership o
 ON o.workspace_id=f.workspace_id WHERE o.session_id=NEW.session_id)
 OR EXISTS(SELECT 1 FROM workspace_retirement_routes r JOIN sessions s ON s.id=NEW.session_id
 WHERE s.cwd=r.route OR substr(s.cwd,1,length(r.route)+1)=r.route||'/'))
BEGIN SELECT RAISE(ABORT,'workspace retirement freezes turn reopening'); END;

CREATE TRIGGER retirement_block_managed_process
BEFORE INSERT ON managed_processes
WHEN NOT EXISTS(SELECT 1 FROM managed_processes WHERE process_id=NEW.process_id)
 AND (EXISTS(SELECT 1 FROM workspace_retirement_fences WHERE workspace_id=NEW.workspace_id)
 OR EXISTS(SELECT 1 FROM workspace_retirement_fences f JOIN workspace_session_ownership o
 ON o.workspace_id=f.workspace_id WHERE o.session_id=NEW.owner_session_id)
 OR EXISTS(SELECT 1 FROM workspace_retirement_routes r
 WHERE NEW.cwd=r.route OR substr(NEW.cwd,1,length(r.route)+1)=r.route||'/'))
BEGIN SELECT RAISE(ABORT,'workspace retirement freezes process admission'); END;

CREATE TRIGGER retirement_block_process_launch
BEFORE UPDATE OF status ON managed_processes
WHEN NEW.status IN ('launch_reserved','running') AND NEW.status<>OLD.status
 AND (EXISTS(SELECT 1 FROM workspace_retirement_fences WHERE workspace_id=NEW.workspace_id)
 OR EXISTS(SELECT 1 FROM workspace_retirement_fences f JOIN workspace_session_ownership o
 ON o.workspace_id=f.workspace_id WHERE o.session_id=NEW.owner_session_id)
 OR EXISTS(SELECT 1 FROM workspace_retirement_routes r
 WHERE NEW.cwd=r.route OR substr(NEW.cwd,1,length(r.route)+1)=r.route||'/'))
BEGIN SELECT RAISE(ABORT,'workspace retirement freezes process launch'); END;

CREATE TRIGGER retirement_block_legacy_process
BEFORE INSERT ON processes
WHEN NOT EXISTS(SELECT 1 FROM processes WHERE id=NEW.id)
 AND EXISTS(SELECT 1 FROM workspace_retirement_routes r
 WHERE NEW.cwd=r.route OR substr(NEW.cwd,1,length(r.route)+1)=r.route||'/')
BEGIN SELECT RAISE(ABORT,'workspace retirement freezes legacy process admission'); END;

CREATE TRIGGER retirement_block_legacy_process_reopen
BEFORE UPDATE OF status ON processes
WHEN OLD.status IN ('completed','failed','killed','canceled','timed_out','interrupted')
 AND NEW.status NOT IN ('completed','failed','killed','canceled','timed_out','interrupted')
 AND EXISTS(SELECT 1 FROM workspace_retirement_routes r
 WHERE NEW.cwd=r.route OR substr(NEW.cwd,1,length(r.route)+1)=r.route||'/')
BEGIN SELECT RAISE(ABORT,'workspace retirement freezes legacy process reopening'); END;

CREATE TRIGGER retirement_block_workspace_message
BEFORE INSERT ON agent_messages
WHEN NOT EXISTS(SELECT 1 FROM agent_messages WHERE id=NEW.id)
 AND (EXISTS(SELECT 1 FROM workspace_retirement_fences WHERE workspace_id=NEW.workspace_id)
 OR EXISTS(SELECT 1 FROM workspace_retirement_fences f JOIN workspace_session_ownership o
 ON o.workspace_id=f.workspace_id WHERE o.session_id=NEW.sender_agent_id))
BEGIN SELECT RAISE(ABORT,'workspace retirement freezes message send'); END;

CREATE TRIGGER retirement_block_recipient_delivery
BEFORE INSERT ON agent_deliveries
WHEN NOT EXISTS(SELECT 1 FROM agent_deliveries WHERE id=NEW.id)
 AND EXISTS(SELECT 1 FROM workspace_retirement_fences f JOIN workspace_session_ownership o
 ON o.workspace_id=f.workspace_id WHERE o.session_id=NEW.recipient_agent_id)
BEGIN SELECT RAISE(ABORT,'workspace retirement freezes new deliveries'); END;

CREATE TRIGGER retirement_block_delivery_injection
BEFORE UPDATE OF status ON agent_deliveries
WHEN NEW.status='injecting' AND OLD.status<>'injecting'
 AND EXISTS(SELECT 1 FROM workspace_retirement_fences f
 JOIN workspace_session_ownership o ON o.workspace_id=f.workspace_id
 WHERE o.session_id=NEW.recipient_agent_id
   OR EXISTS(SELECT 1 FROM agent_messages m
     WHERE m.id=NEW.message_id AND m.sender_agent_id=o.session_id))
BEGIN SELECT RAISE(ABORT,'workspace retirement freezes message injection'); END;

CREATE TRIGGER retirement_block_legacy_message
BEFORE INSERT ON team_messages
WHEN NOT EXISTS(SELECT 1 FROM team_messages WHERE id=NEW.id)
 AND EXISTS(SELECT 1 FROM workspace_retirement_fences f JOIN workspace_session_ownership o
 ON o.workspace_id=f.workspace_id WHERE o.session_id=NEW.sender_agent_id)
BEGIN SELECT RAISE(ABORT,'workspace retirement freezes legacy messages'); END;

CREATE TRIGGER retirement_block_legacy_delivery
BEFORE INSERT ON team_deliveries
WHEN NOT EXISTS(SELECT 1 FROM team_deliveries WHERE id=NEW.id)
 AND EXISTS(SELECT 1 FROM workspace_retirement_fences f JOIN workspace_session_ownership o
 ON o.workspace_id=f.workspace_id WHERE o.session_id=NEW.recipient_agent_id)
BEGIN SELECT RAISE(ABORT,'workspace retirement freezes legacy deliveries'); END;

CREATE TRIGGER retirement_block_legacy_delivery_injection
BEFORE UPDATE OF status ON team_deliveries
WHEN NEW.status='injecting' AND OLD.status<>'injecting'
 AND EXISTS(SELECT 1 FROM workspace_retirement_fences f
 JOIN workspace_session_ownership o ON o.workspace_id=f.workspace_id
 WHERE o.session_id=NEW.recipient_agent_id
   OR EXISTS(SELECT 1 FROM team_messages m
     WHERE m.id=NEW.message_id AND m.sender_agent_id=o.session_id))
BEGIN SELECT RAISE(ABORT,'workspace retirement freezes legacy message injection'); END;

CREATE TRIGGER retirement_block_worktree_create
BEFORE INSERT ON managed_worktrees
WHEN NOT EXISTS(SELECT 1 FROM managed_worktrees WHERE id=NEW.id)
 AND EXISTS(SELECT 1 FROM workspace_retirement_fences f JOIN workspaces w ON w.workspace_id=f.workspace_id
 WHERE NEW.repo_root=w.canonical_root)
BEGIN SELECT RAISE(ABORT,'workspace retirement freezes managed worktree creation'); END;

CREATE TRIGGER retirement_block_worktree_claim
BEFORE INSERT ON managed_worktree_claims
WHEN NEW.released_at IS NULL
 AND NOT EXISTS(SELECT 1 FROM managed_worktree_claims
 WHERE worktree_id=NEW.worktree_id AND session_id=NEW.session_id)
 AND (EXISTS(SELECT 1 FROM workspace_retirement_fences f JOIN workspace_session_ownership o
 ON o.workspace_id=f.workspace_id WHERE o.session_id=NEW.session_id)
 OR EXISTS(SELECT 1 FROM workspace_retirement_fences f JOIN workspaces w ON w.workspace_id=f.workspace_id
 JOIN managed_worktrees t ON t.repo_root=w.canonical_root WHERE t.id=NEW.worktree_id))
BEGIN SELECT RAISE(ABORT,'workspace retirement freezes worktree claims'); END;

CREATE TRIGGER retirement_block_worktree_reclaim
BEFORE UPDATE OF released_at ON managed_worktree_claims
WHEN OLD.released_at IS NOT NULL AND NEW.released_at IS NULL
 AND (EXISTS(SELECT 1 FROM workspace_retirement_fences f JOIN workspace_session_ownership o
 ON o.workspace_id=f.workspace_id WHERE o.session_id=NEW.session_id)
 OR EXISTS(SELECT 1 FROM workspace_retirement_fences f JOIN workspaces w ON w.workspace_id=f.workspace_id
 JOIN managed_worktrees t ON t.repo_root=w.canonical_root WHERE t.id=NEW.worktree_id))
BEGIN SELECT RAISE(ABORT,'workspace retirement freezes worktree reclaim'); END;

CREATE TRIGGER retirement_block_notice_delivery
BEFORE UPDATE OF state ON workspace_system_notices
WHEN NEW.state='injecting' AND OLD.state<>'injecting'
 AND EXISTS(SELECT 1 FROM workspace_retirement_fences WHERE workspace_id=NEW.workspace_id)
BEGIN SELECT RAISE(ABORT,'workspace retirement freezes notice delivery'); END;

CREATE TRIGGER retirement_block_hook_dispatch
BEFORE UPDATE OF phase ON workspace_hook_effects
WHEN NEW.phase='started' AND OLD.phase<>'started'
 AND EXISTS(SELECT 1 FROM workspace_retirement_fences WHERE workspace_id=NEW.workspace_id)
BEGIN SELECT RAISE(ABORT,'workspace retirement freezes hook dispatch'); END;

-- A worker may have selected a terminal completion before admission froze the
-- workspace. It cannot re-open settled authority from that stale snapshot.
-- Native process terminalization remains legal; undelivered output is retained
-- as pending with an explicit deferral receipt for a later owner restore.
CREATE TRIGGER retirement_block_stale_process_completion
BEFORE UPDATE OF completion_state,completion_attempt_count,completion_last_error ON managed_processes
WHEN OLD.status NOT IN ('queued','launch_reserved','running')
 AND NEW.completion_state IN ('pending','injecting')
 AND NOT (NEW.completion_state='pending'
   AND COALESCE(NEW.completion_last_error,'')='workspace_retirement_deferred_completion')
 AND (EXISTS(SELECT 1 FROM workspace_retirement_fences f WHERE f.workspace_id=NEW.workspace_id)
   OR EXISTS(SELECT 1 FROM workspace_retirement_fences f JOIN workspace_session_ownership o
     ON o.workspace_id=f.workspace_id WHERE o.session_id=NEW.owner_session_id)
   OR EXISTS(SELECT 1 FROM workspace_retirement_routes r WHERE NEW.cwd=r.route
     OR substr(NEW.cwd,1,length(r.route)+1)=r.route||'/'))
BEGIN SELECT RAISE(ABORT,'workspace retirement freezes stale process completion'); END;

-- Explicit operator reconciliation is independently idempotent and audited.
-- The original retirement and its ingress fence remain the authority owner.
CREATE TABLE workspace_retirement_reviews (
  review_id TEXT PRIMARY KEY,
  operation_id TEXT NOT NULL REFERENCES runtime_operations(operation_id) ON DELETE RESTRICT,
  effect_id TEXT NOT NULL REFERENCES runtime_operation_effects(effect_id) ON DELETE RESTRICT,
  actor_id TEXT NOT NULL CHECK(length(trim(actor_id))>0),
  idempotency_key TEXT NOT NULL CHECK(length(trim(idempotency_key))>0),
  normalized_request_hash TEXT NOT NULL,
  normalized_request_json TEXT NOT NULL CHECK(json_valid(normalized_request_json)),
  receipt_json TEXT NOT NULL CHECK(json_valid(receipt_json)),
  reviewed_at INTEGER NOT NULL,
  UNIQUE(operation_id,actor_id,idempotency_key)
);
CREATE TRIGGER retirement_review_receipt_immutable_update
BEFORE UPDATE ON workspace_retirement_reviews
BEGIN SELECT RAISE(ABORT,'retirement review receipts are immutable'); END;
CREATE TRIGGER retirement_review_receipt_immutable_delete
BEFORE DELETE ON workspace_retirement_reviews
BEGIN SELECT RAISE(ABORT,'retirement review receipts are immutable'); END;

-- Operator-verified native closure can settle logical liveness while retaining
-- an honestly unknown historical dispatch. Its exact historical native binding
-- is fixed by matching turn/close receipts, even after an explicit later restore.
CREATE VIEW workspace_retirement_closed_turns AS
SELECT o.workspace_id,t.id AS turn_id,s.id AS session_id,c.operation_id
FROM workspace_session_ownership o
JOIN sessions s ON s.id=o.session_id
JOIN turns t ON t.session_id=s.id
JOIN runtime_operation_effects e ON e.target_id=t.id
  AND e.effect_kind='workspace_turn_interrupt' AND e.phase='finalized'
JOIN runtime_operation_effects c ON c.operation_id=e.operation_id AND c.target_id=s.id
  AND c.effect_kind='workspace_provider_close' AND c.phase='finalized'
JOIN runtime_operations op ON op.operation_id=c.operation_id AND op.workspace_id=o.workspace_id
WHERE op.kind='workspace_retirement' AND op.phase IN ('requested','manual_review','completed')
  AND s.active_turn_id IS NOT t.id
  AND t.status IN ('completed','failed','interrupted','cancelled') AND t.completed_at IS NOT NULL
  AND json_extract(e.evidence_json,'$.outcome')='operator_verified_native_attachment_closed'
  AND json_extract(e.evidence_json,'$.provider_close_effect_id')=c.effect_id
  AND json_extract(c.evidence_json,'$.verified')=1
  AND json_extract(c.evidence_json,'$.proof_kind')='operator_verified_closed'
  AND json_extract(e.evidence_json,'$.native_identity')=json_extract(c.evidence_json,'$.verified_identity')
  AND json_extract(e.evidence_json,'$.operator_review.review_id')=json_extract(c.evidence_json,'$.operator_review.review_id')
  AND EXISTS(SELECT 1 FROM json_each(c.evidence_json,'$.operator_settled_turn_ids') WHERE value=t.id);
