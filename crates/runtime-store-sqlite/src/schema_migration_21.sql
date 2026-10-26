CREATE INDEX idx_managed_processes_admitted_page
ON managed_processes(admitted_at DESC, process_id DESC);

CREATE INDEX idx_managed_processes_workspace_admitted_page
ON managed_processes(workspace_id, admitted_at DESC, process_id DESC);
