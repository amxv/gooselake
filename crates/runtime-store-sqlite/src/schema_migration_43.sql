-- Process admission obtains the next global queue order via MAX(queue_order).
-- Existing queue/diagnostic indexes lead with status or workspace_id and
-- therefore require a full index scan for that global maximum. An ordered
-- single-column index allows SQLite's MAX optimization to read the tail,
-- independent of status, workspace and historical queue reorderings.
CREATE INDEX idx_managed_processes_queue_allocation
ON managed_processes(queue_order);
