use super::*;
use serde_json::{json, Value};

fn write_config(root: &Path, config: &Value) {
    let path = root.join(TEAM_HOOK_CONFIG_RELATIVE_PATH);
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    std::fs::write(path, serde_json::to_vec(config).unwrap()).unwrap();
}

fn fixture(hooks: Value) -> Value {
    json!({"version":1,"hooks":hooks})
}

fn named(name: &str, command: &str) -> Value {
    json!({"name":name,"command":command})
}

#[test]
fn no_configuration_means_no_effective_hooks() {
    let temp = tempfile::tempdir().unwrap();
    assert!(resolve_effective_team_hooks(temp.path(), None, None)
        .unwrap()
        .is_empty());
}

#[test]
fn user_and_project_names_merge_in_stable_order_with_project_override() {
    let temp = tempfile::tempdir().unwrap();
    let user = temp.path().join("user");
    let project = temp.path().join("project");
    write_config(
        &user,
        &fixture(json!({
            "pre_teammate_add":[named("Worktree Setup","echo user"),named("Second","echo second")],
            "post_direct_message_send":[named("Notify","echo direct")]
        })),
    );
    write_config(
        &project,
        &fixture(json!({
            "pre_teammate_add":[named("worktree_setup","echo project"),named("Third","echo third")]
        })),
    );
    let hooks = resolve_effective_team_hooks(&project, Some(&user), None).unwrap();
    assert_eq!(hooks.len(), 4);
    assert_eq!(hooks[0].name, "worktree_setup");
    assert_eq!(hooks[0].command, "echo project");
    assert_eq!(hooks[0].scope, TeamHookScope::Project);
    assert_eq!(hooks[0].timeout_ms, DEFAULT_TIMEOUT_MS);
    assert_eq!(hooks[1].name, "second");
    assert_eq!(
        hooks[2].event_name,
        TeamHookEventName::PostDirectMessageSend
    );
    assert_eq!(hooks[3].name, "third");
    let filtered = resolve_effective_team_hooks(
        &project,
        Some(&user),
        Some(TeamHookEventName::PreTeammateAdd),
    )
    .unwrap();
    assert_eq!(filtered.len(), 3);
    assert_eq!(filtered[0].command, "echo project");
}

#[test]
fn rejects_duplicate_names_after_normalization() {
    let temp = tempfile::tempdir().unwrap();
    write_config(
        temp.path(),
        &fixture(json!({"post_compaction":[
            named("Swap--Name", "echo 1"), named("swap name", "echo 2")
        ]})),
    );
    assert!(resolve_effective_team_hooks(temp.path(), None, None)
        .unwrap_err()
        .to_string()
        .contains("duplicate normalized"));
}

#[test]
fn rejects_unknown_event_and_unknown_entry_key_and_unsupported_version() {
    let temp = tempfile::tempdir().unwrap();
    let invalid = [
        fixture(json!({"post_unknown":[named("test", "echo test")]})),
        fixture(json!({"pre_teammate_add":[{"name":"test","command":"echo", "cwd":"/"}]})),
        json!({"version":2,"hooks":{"post_compaction":[]}}),
        json!({"version":1,"unexpected_field":true}),
    ];
    for config in invalid {
        write_config(temp.path(), &config);
        assert!(resolve_effective_team_hooks(temp.path(), None, None).is_err());
    }
}

#[test]
fn missing_v1_version_defaults_to_legacy_compatible_v1() {
    let temp = tempfile::tempdir().unwrap();
    write_config(
        temp.path(),
        &json!({"hooks":{"post_teammate_add":[named("hello","echo ok")]}}),
    );
    let hooks = resolve_effective_team_hooks(temp.path(), None, None).unwrap();
    assert_eq!(hooks.len(), 1);
    assert_eq!(hooks[0].event_name, TeamHookEventName::PostTeammateAdd);
}

#[test]
fn validates_name_command_timeout_even_when_other_event_requested() {
    let temp = tempfile::tempdir().unwrap();
    let entries = [
        json!({"name":"untrusted;echo danger","command":"echo safe"}),
        json!({"name":"plain","command":"   "}),
        json!({"name":"plain","command":"echo safe","timeout_ms":99}),
        json!({"name":"plain","command":"echo safe","timeout_ms":120001}),
    ];
    for entry in entries {
        write_config(temp.path(), &fixture(json!({"post_compaction":[entry]})));
        assert!(resolve_effective_team_hooks(
            temp.path(),
            None,
            Some(TeamHookEventName::PostDirectMessageSend)
        )
        .is_err());
    }
}

#[test]
fn rejects_oversized_config_and_too_many_entries() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join(TEAM_HOOK_CONFIG_RELATIVE_PATH);
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    std::fs::write(&path, vec![b' '; (MAX_CONFIG_BYTES + 1) as usize]).unwrap();
    assert!(resolve_effective_team_hooks(temp.path(), None, None).is_err());
    let hooks = (0..MAX_HOOKS_PER_EVENT + 1)
        .map(|index| named(&format!("hook_{index}"), "echo a"))
        .collect::<Vec<_>>();
    write_config(temp.path(), &fixture(json!({"post_teammate_remove":hooks})));
    assert!(resolve_effective_team_hooks(temp.path(), None, None).is_err());
}

#[test]
fn preserves_distinct_events_and_does_not_guess_defaults() {
    let temp = tempfile::tempdir().unwrap();
    write_config(
        temp.path(),
        &fixture(json!({
            "post_teammate_remove":[{"name":"a","command":"echo a","timeout_ms":100}],
            "post_broadcast_message_send":[{"name":"a","command":"echo b","timeout_ms":120000}]
        })),
    );
    let hooks = resolve_effective_team_hooks(temp.path(), None, None).unwrap();
    assert_eq!(hooks.len(), 2);
    assert_eq!(hooks[0].timeout_ms, 100);
    assert_eq!(hooks[1].timeout_ms, 120000);
}
