// Shared, declarative registration table: the HTTP router and the OpenAPI
// operation catalog both expand this one set of typed handler expressions.
// Never scrape http/mod.rs source text to infer routes or HTTP methods.
macro_rules! route_table {
    ($emit:ident) => {
        $emit! {
            mcp ("/v1/mcp", true) {
                "/capabilities" => get(mcp_capabilities);
                "/invoke" => post(mcp_invoke);
            }
            scoped_mcp ("/v2/mcp", true) {
                "/capabilities" => get(scoped_mcp_capabilities);
                "/invoke" => post(scoped_mcp_invoke);
            }
            protected ("/v1", true) {
                "/health" => get(protected_health);
                "/providers" => get(list_providers);
                "/providers/{provider}/models" => get(list_provider_models);
                "/providers/codex/auth/status" => get(codex_auth_status);
                "/providers/acp/auth/status" => get(acp_auth_status);
                "/providers/claude/auth/status" => get(claude_auth_status);
                "/providers/claude/auth/api-key" => post(claude_auth_api_key);
                "/providers/claude/auth/import-json" => post(claude_auth_import_json);
                "/providers/claude/auth/import-file" => post(claude_auth_import_file);
                "/providers/claude/auth/logout" => post(claude_auth_logout);
                "/openapi.yaml" => get(openapi_yaml);
                "/version" => get(version);
                "/events" => get(replay_global_events);
                "/events/stream" => get(stream_global_events);
                "/sessions" => post(create_session).get(list_sessions);
                "/sessions/{session_id}" => get(get_session);
                "/sessions/{session_id}/resume" => post(resume_session);
                "/sessions/{session_id}/close" => post(close_session);
                "/sessions/{session_id}/turns" => post(send_turn);
                "/sessions/{session_id}/turns/{turn_id}/interrupt" => post(interrupt_turn);
                "/sessions/{session_id}/approvals/{approval_id}" => post(respond_approval);
                "/sessions/{session_id}/events" => get(replay_session_events);
                "/sessions/{session_id}/events/stream" => get(stream_session_events);
                "/processes" => post(start_process).get(list_processes);
                "/processes/scheduler" => get(get_process_scheduler).post(update_process_scheduler);
                "/processes/queue/reorder" => post(reorder_process_queue);
                "/processes/{process_id}" => get(get_process);
                "/processes/{process_id}/logs" => get(get_process_logs);
                "/processes/{process_id}/events" => get(replay_process_events);
                "/processes/{process_id}/events/stream" => get(stream_process_events);
                "/processes/{process_id}/kill" => post(kill_process);
                "/worktrees" => post(create_worktree).get(list_worktrees);
                "/worktrees/{worktree_id}" => get(get_worktree);
                "/worktrees/{worktree_id}/claims" => post(claim_worktree);
                "/worktrees/{worktree_id}/release" => post(release_worktree);
                "/worktrees/{worktree_id}/cleanup" => post(cleanup_worktree);
                "/diagnostics" => get(runtime_diagnostics);
                "/diagnostics/providers" => get(provider_diagnostics);
                "/diagnostics/comms" => get(comms_diagnostics);
                "/diagnostics/processes" => get(process_diagnostics);
                "/diagnostics/worktrees" => get(worktree_diagnostics);
                "/diagnostics/recovery" => get(recovery_diagnostics);
                "/diagnostics/team-operations" => get(list_team_operation_diagnostics);
                "/teams" => post(create_team).get(list_teams);
                "/teams/{team_id}" => get(get_team).delete(delete_team);
                "/teams/{team_id}/members" => post(join_team_member);
                "/teams/{team_id}/members/spawn" => post(spawn_team_member);
                "/teams/{team_id}/members/{agent_id}" => delete(remove_team_member);
                "/teams/{team_id}/lead" => post(set_team_lead);
                "/teams/{team_id}/messages" => post(send_team_direct).get(list_team_messages);
                "/teams/{team_id}/broadcasts" => post(send_team_broadcast);
                "/teams/{team_id}/deliveries" => get(list_team_deliveries);
                "/teams/{team_id}/deliveries/{delivery_id}/retry" => post(retry_team_delivery);
                "/teams/{team_id}/messages/{message_id}/cancel" => post(cancel_team_message);
                "/teams/{team_id}/view" => get(get_team_view_snapshot);
                "/teams/{team_id}/events" => get(replay_team_events);
                "/teams/{team_id}/events/stream" => get(stream_team_events);
                "/teams/{team_id}/interrupt-all" => post(interrupt_all_team_turns);
            }
            protected_v2 ("/v2", true) {
                "/capabilities" => get(runtime_capabilities_v2);
                "/agents/{agent_id}" => get(get_agent_by_id);
                "/agents/{agent_id}/events/stream" => get(stream_operator_agent_events);
                "/agents/{agent_id}/turns/{turn_id}/edit-rerun" => post(edit_agent_turn_and_rerun);
                "/agents/{agent_id}/edits/{operation_id}" => get(get_agent_edit_operation);
                "/agents/{agent_id}/edits/{operation_id}/resolve" => post(resolve_agent_edit_operation);
                "/agents/{agent_id}/compact" => post(compact_agent_session);
                "/agents/{agent_id}/compactions/{run_id}" => get(get_agent_compaction_run);
                "/agents/{agent_id}/compactions/{run_id}/resolve" => post(resolve_agent_compaction_review);
                "/agents/{agent_id}/turns" => post(send_agent_turn_v2);
                "/agents/{agent_id}/context-limit" => get(get_agent_context_limit);
                "/agents/{agent_id}/context-limit/refresh" => post(refresh_agent_context_limit);
                "/agents/{agent_id}/preferences" => post(update_agent_preferences);
                "/agents/{agent_id}/permission" => post(update_agent_permission);
                "/agents/{agent_id}/native-policy/{operation_id}/resolve" => post(resolve_agent_native_policy_review) @ 16 * 1024;
                "/providers/{provider}/capabilities" => get(provider_capabilities_v2);
                "/providers/{provider}/models/discover" => post(discover_provider_models_v2);
                "/providers/{provider}/skills/discover" => post(discover_provider_skills_v2);
                "/workspaces" => post(register_workspace).get(list_workspaces);
                "/workspaces/{workspace_id}" => get(get_workspace);
                "/workspaces/{workspace_id}/events/stream" => get(stream_operator_workspace_events);
                "/workspaces/{workspace_id}/retire" => post(retire_workspace);
                "/workspaces/{workspace_id}/retirement/advance" => post(advance_workspace_retirement);
                "/workspaces/{workspace_id}/retirement/{operation_id}/reconcile" => post(reconcile_workspace_retirement) @ 16 * 1024;
                "/workspaces/{workspace_id}/retirement-preflight" => get(workspace_retirement_preflight);
                "/workspaces/{workspace_id}/hooks" => get(get_workspace_hooks);
                "/workspaces/{workspace_id}/hooks/approval" => get(get_workspace_hook_approval).put(update_workspace_hook_approval);
                "/workspaces/{workspace_id}/hooks/effects" => get(list_workspace_hook_effects);
                "/workspaces/{workspace_id}/hooks/effects/{parent_event_id}/{event_name}/{hook_name}" => get(get_workspace_hook_effect);
                "/workspaces/{workspace_id}/worktrees" => get(list_workspace_worktree_inventory).post(create_workspace_worktree);
                "/workspaces/{workspace_id}/lead" => post(set_workspace_lead);
                "/workspaces/{workspace_id}/notice-policy" => get(get_workspace_notice_policy).put(update_workspace_notice_policy);
                "/workspaces/{workspace_id}/interrupt" => post(interrupt_workspace_turns);
                "/workspaces/{workspace_id}/agents" => post(create_workspace_agent).get(list_workspace_agents);
                "/workspaces/{workspace_id}/agents/creations/{operation_id}/resolve" => post(resolve_workspace_agent_create_review);
                "/workspaces/{workspace_id}/agents/{agent_id}" => get(get_workspace_agent);
                "/workspaces/{workspace_id}/agents/{agent_id}/onboarding" => get(get_workspace_agent_onboarding);
                "/workspaces/{workspace_id}/agents/{agent_id}/worktree" => post(rebind_workspace_agent);
                "/workspaces/{workspace_id}/agents/{agent_id}/rebinds/{operation_id}" => get(get_workspace_agent_rebind);
                "/workspaces/{workspace_id}/agents/{agent_id}/rebinds/{operation_id}/cleanup" => post(retry_workspace_agent_rebind_cleanup);
                "/workspaces/{workspace_id}/agents/{agent_id}/archive" => post(archive_workspace_agent);
                "/workspaces/{workspace_id}/agents/{agent_id}/archives/{operation_id}/resolve" => post(resolve_workspace_agent_archive_review);
                "/workspaces/{workspace_id}/agents/{agent_id}/restore" => post(restore_workspace_agent);
                "/workspaces/{workspace_id}/agents/{agent_id}/restores/{operation_id}/resolve" => post(resolve_workspace_agent_restore_review);
                "/messages" => post(create_agent_message).get(list_agent_messages);
                "/messages/{message_id}/deliveries" => get(list_agent_deliveries);
                "/messages/{message_id}/cancel" => post(cancel_agent_message);
                "/deliveries/{delivery_id}/retry" => post(retry_agent_delivery);
                "/migrations/workspaces" => get(get_workspace_migration_status);
                "/migrations/workspaces/preview" => post(preview_workspace_migration);
                "/migrations/workspaces/apply" => post(apply_workspace_migration);
                "/migrations/workspaces/resolutions/{subject_kind}/{subject_id}" => post(resolve_workspace_migration_subject);
                "/operations" => get(list_operations);
                "/operations/{operation_id}" => get(get_operation);
                "/events" => get(list_operator_events);
                "/events/stream" => get(stream_operator_global_events);
                "/events/{event_id}" => get(get_operator_event);
                "/processes" => get(list_operator_processes);
                "/processes/{process_id}/events/stream" => get(stream_operator_process_events);
                "/processes/{process_id}" => get(get_operator_process);
            }
            root ("", false) {
                "/health" => get(health);
                "/openapi.yaml" => get(openapi_yaml);
            }
        }
    };
}
pub(crate) use route_table;
