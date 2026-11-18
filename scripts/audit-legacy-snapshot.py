#!/usr/bin/env python3
"""Counts-only, immutable read-only preflight for an isolated SQLite snapshot.

This is NOT workspace migration preview/apply, schema migration, a cutover
decision, or permission to delete data. Never point it at a running database.
It refuses active/stale SQLite journal sidecars and only reads fixed schemas.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import sqlite3
import sys
sys.dont_write_bytecode = True

from snapshot_inspection import schema_observation, sealed_database, seven_family_observations, source_schema_version


# (name, required table/column identities, fixed count query).
# Missing older-schema tables mean "unknown", never a safe zero.
METRICS = (
    ("legacy_sessions", {"sessions": ("id",)}, "SELECT COUNT(*) FROM sessions"),
    ("legacy_teams", {"teams": ("id",)}, "SELECT COUNT(*) FROM teams"),
    ("legacy_team_members", {"team_members": ("team_id",)}, "SELECT COUNT(*) FROM team_members"),
    ("legacy_team_messages", {"team_messages": ("id",)}, "SELECT COUNT(*) FROM team_messages"),
    ("legacy_team_deliveries", {"team_deliveries": ("id",)}, "SELECT COUNT(*) FROM team_deliveries"),
    ("managed_worktrees", {"managed_worktrees": ("id",)}, "SELECT COUNT(*) FROM managed_worktrees"),
    ("historical_events", {"runtime_events": ("id",)}, "SELECT COUNT(*) FROM runtime_events"),
    ("agent_messages", {"agent_messages": ("id",)}, "SELECT COUNT(*) FROM agent_messages"),
    ("agent_deliveries", {"agent_deliveries": ("id",)}, "SELECT COUNT(*) FROM agent_deliveries"),
    ("unresolved_legacy_subjects", {"legacy_workspace_migration_subjects": ("classification",)},
     "SELECT COUNT(*) FROM legacy_workspace_migration_subjects WHERE classification='unresolved'"),
    ("unapplied_classified_legacy_subjects",
     {"legacy_workspace_migration_subjects": ("classification", "applied_at")},
     "SELECT COUNT(*) FROM legacy_workspace_migration_subjects "
     "WHERE classification IN ('mapped','archived_history') AND applied_at IS NULL"),
    ("active_sessions_without_workspace_ownership",
     {"sessions": ("id", "status"), "workspace_session_ownership": ("session_id",)},
     "SELECT COUNT(*) FROM sessions s WHERE (typeof(s.status)<>'text' OR s.status!='closed') AND NOT EXISTS "
     "(SELECT 1 FROM workspace_session_ownership o WHERE o.session_id=s.id)"),
    ("nonterminal_turns", {"turns": ("status",)},
     "SELECT COUNT(*) FROM turns WHERE typeof(status)<>'text' OR status NOT IN "
     "('completed','failed','interrupted','cancelled')"),
    ("unresolved_approvals", {"approvals": ("status",)},
     "SELECT COUNT(*) FROM approvals WHERE typeof(status)<>'text' OR status NOT IN "
     "('accept','decline','approved','accepted','denied','rejected','cancelled','expired','failed','resolved')"),
    ("outstanding_dispatches", {"turn_admissions": ("dispatch_state",)},
     "SELECT COUNT(*) FROM turn_admissions WHERE typeof(dispatch_state)<>'text' OR dispatch_state "
     "NOT IN ('dispatched','not_dispatched')"),
    ("nonterminal_operations", {"runtime_operations": ("phase",)},
     "SELECT COUNT(*) FROM runtime_operations WHERE typeof(phase)<>'text' OR phase NOT IN ('completed','failed')"),
    ("unfinalized_native_effects", {"runtime_operation_effects": ("phase",)},
     "SELECT COUNT(*) FROM runtime_operation_effects WHERE typeof(phase)<>'text' OR phase!='finalized'"),
    ("undelivered_outbox", {"runtime_operation_outbox": ("state",)},
     "SELECT COUNT(*) FROM runtime_operation_outbox WHERE typeof(state)<>'text' OR state!='delivered'"),
    ("unsettled_agent_deliveries", {"agent_deliveries": ("status",)},
     "SELECT COUNT(*) FROM agent_deliveries WHERE typeof(status)<>'text' OR status NOT IN "
     "('injected','cancelled')"),
    ("unsettled_legacy_deliveries", {"team_deliveries": ("status",)},
     "SELECT COUNT(*) FROM team_deliveries WHERE typeof(status)<>'text' OR status NOT IN "
     "('injected','cancelled')"),
    ("nonterminal_managed_processes", {"managed_processes": ("status",)},
     "SELECT COUNT(*) FROM managed_processes WHERE typeof(status)<>'text' OR status NOT IN "
     "('completed','failed','timed_out','killed','canceled','interrupted')"),
    ("undelivered_process_completions", {"managed_processes": ("completion_state",)},
     "SELECT COUNT(*) FROM managed_processes "
     "WHERE typeof(completion_state)<>'text' OR completion_state NOT IN ('not_required','delivered')"),
    ("nonterminal_legacy_processes", {"processes": ("status",)},
     "SELECT COUNT(*) FROM processes WHERE typeof(status)<>'text' OR status NOT IN "
     "('completed','failed','timed_out','killed','canceled','interrupted')"),
    ("unfinished_system_notices", {"workspace_system_notices": ("state",)},
     "SELECT COUNT(*) FROM workspace_system_notices "
     "WHERE typeof(state)<>'text' OR state NOT IN ('delivered','cancelled')"),
    ("unsettled_hook_effects", {"workspace_hook_effects": ("phase",)},
     "SELECT COUNT(*) FROM workspace_hook_effects "
     "WHERE typeof(phase)<>'text' OR phase NOT IN ('succeeded','failed','timed_out')"),
    ("unapplied_hook_results", {"workspace_hook_effects": ("application_state",)},
     "SELECT COUNT(*) FROM workspace_hook_effects WHERE typeof(application_state)<>'text' OR application_state NOT IN ('not_ready','applied','rejected')"),
    ("unsettled_retirement_fences", {"workspace_retirement_fences": ("phase",)},
     "SELECT COUNT(*) FROM workspace_retirement_fences WHERE typeof(phase)<>'text' OR phase!='retired'"),
    ("active_worktree_claims", {"managed_worktree_claims": ("released_at",)},
     "SELECT COUNT(*) FROM managed_worktree_claims WHERE released_at IS NULL"),
    ("unsettled_worktree_cleanups", {"managed_worktree_cleanup_receipts": ("phase",)},
     "SELECT COUNT(*) FROM managed_worktree_cleanup_receipts WHERE typeof(phase)<>'text' OR phase!='completed'"),
    ("unsettled_team_spawn_journals", {"team_operation_journal": ("kind", "stage")},
     "SELECT COUNT(*) FROM team_operation_journal "
     "WHERE kind='spawn_member_with_worktree' AND (typeof(stage)<>'text' OR stage NOT IN ('completed','rolled_back'))"),
    ("unsettled_keyed_team_spawns", {"team_spawn_admissions": ("completed_response_json",)},
     "SELECT COUNT(*) FROM team_spawn_admissions WHERE completed_response_json IS NULL"),
    ("unsettled_workspace_rebinds", {"workspace_agent_rebinds": ("phase",)},
     "SELECT COUNT(*) FROM workspace_agent_rebinds "
     "WHERE typeof(phase)<>'text' OR phase NOT IN ('completed','rejected')"),
    ("unsettled_compactions", {"session_compaction_runs": ("phase",)},
     "SELECT COUNT(*) FROM session_compaction_runs "
     "WHERE typeof(phase)<>'text' OR phase NOT IN ('completed','failed','not_performed')"),
    ("unsettled_edit_reruns", {"session_edit_reruns": ("phase",)},
     "SELECT COUNT(*) FROM session_edit_reruns WHERE typeof(phase)<>'text' OR phase NOT IN ('completed','failed')"),
)

# An inventory count is not itself an unresolved obligation, e.g. preserving
# an old worktree or archived message may be mandatory but is not proof of
# pending native work. All the other metrics need independent reconciliation.
INVENTORY_METRICS = frozenset(
    {
        "legacy_sessions", "legacy_teams", "legacy_team_members",
        "legacy_team_messages", "legacy_team_deliveries", "managed_worktrees",
        "historical_events", "agent_messages", "agent_deliveries",
    }
)


def relation_columns(connection: sqlite3.Connection) -> dict[str, frozenset[str]]:
    tables = {
        row[0]
        for row in connection.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        )
    }
    known_names = {
        table
        for _metric, requirements, _sql in METRICS
        for table in requirements
    } | {"schema_migrations", "legacy_workspace_migration_state"}
    result = {}
    for name in sorted(known_names & tables):
        # Names originate exclusively from this source file, not the database.
        result[name] = frozenset(
            row[1] for row in connection.execute(f'PRAGMA table_info("{name}")')
        )
    return result


def observe(db: sqlite3.Connection, schema: dict) -> dict[str, object]:
    if not db.in_transaction:
        raise ValueError("legacy observations require the owner's read transaction")
    columns = relation_columns(db)
    preview_recorded = None
    if {"migration_key"} <= columns.get("legacy_workspace_migration_state", frozenset()):
        preview_recorded = bool(
            db.execute(
                "SELECT 1 FROM legacy_workspace_migration_state "
                "WHERE migration_key='workspace_authority'"
            ).fetchone()
        )

    counts = {}
    for metric, required, query in METRICS:
        missing = [
            table
            for table, names in required.items()
            if not set(names) <= columns.get(table, frozenset())
        ]
        counts[metric] = (
            None if missing else int(db.execute(query).fetchone()[0])
        )

    obligations = [
        metric for metric, count in counts.items()
        if metric not in INVENTORY_METRICS and count is not None and count > 0
    ]
    unknown = [
        metric for metric, count in counts.items()
        if metric not in INVENTORY_METRICS and count is None
    ]
    return {
        "kind": "gooselake_legacy_sqlite_snapshot_audit",
        "inspection_only": True,
        "cutover_authorized": False,
        "source_mode": "immutable_read_only",
        "schema": {**schema, "workspace_migration_preview_recorded": preview_recorded},
        "counts": counts,
        "observed_outstanding_obligations": obligations,
        "unknown_obligation_checks": unknown,
        "inspection_requirements": [
            "retain the independently recorded manifest SHA-256",
            "review observed and unknown obligations without inferring native outcomes",
            "obtain separate authority for any future backup, restore, or activation",
        ],
        "note": (
            "Counts are read-only observations, not migration proof. "
            "A zero count or empty database never authorizes cutover."
        ),
    }


def audit_snapshot(path: Path) -> dict[str, object]:
    with sealed_database(path) as (db, source):
        report = observe(db, schema_observation(db))
        report["snapshot"] = {"sha256": source.sha256, "bytes": source.size}
        report["seven_families"] = seven_family_observations(db)
    return report


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--snapshot", required=True, type=Path,
        help="explicit path to a consistently checkpointed *isolated copy* of SQLite",
    )
    args = parser.parse_args()
    try:
        report = audit_snapshot(args.snapshot)
    except (OSError, ValueError, sqlite3.Error) as error:
        print(f"legacy snapshot audit refused: {error}", file=sys.stderr)
        return 2
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    sys.exit(main())
