-- Retirement operator-review refresh reads all approvals of an owning
-- session ordered across turns, unlike schema45's settled single-turn read.
-- The turn-leading index cannot satisfy this session-wide timestamp order.
CREATE INDEX idx_approvals_retirement_session_order
ON approvals(session_id, created_at, id);

-- Likewise turn_admissions(session_id,dispatch_state,admitted_at,turn_id)
-- seeks one session but sorts across different dispatch states. Preserve
-- the exact admitted_at/turn_id read order with an additive seek index.
CREATE INDEX idx_turn_admissions_retirement_session_order
ON turn_admissions(session_id, admitted_at, turn_id);
