#!/usr/bin/env python3
"""Offline safety regression for counts-only legacy snapshot inspection."""

from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
import sqlite3
import subprocess
import sys
import tempfile
import unittest


SCRIPT = Path(__file__).with_name("audit-legacy-snapshot.py")


def run_audit(path: Path) -> subprocess.CompletedProcess[str]:
    env = dict(os.environ, PYTHONDONTWRITEBYTECODE="1")
    return subprocess.run(
        [sys.executable, str(SCRIPT), "--snapshot", str(path)],
        capture_output=True, text=True, check=False, env=env, timeout=10,
    )


def digest(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


class LegacySnapshotAuditTests(unittest.TestCase):
    def test_pending_native_worktree_cleanup_is_a_nonlive_cutover_blocker(self) -> None:
        with tempfile.TemporaryDirectory(prefix="gg-snapshot-cleanup-") as root:
            snapshot = Path(root) / "consistent-schema28.sqlite3"
            with sqlite3.connect(snapshot) as db:
                db.executescript(
                    "CREATE TABLE schema_migrations(version INTEGER PRIMARY KEY);"
                    "CREATE TABLE managed_worktree_cleanup_receipts(phase TEXT);"
                    "INSERT INTO managed_worktree_cleanup_receipts VALUES"
                    "('pending'),('completed');"
                )
                db.executemany(
                    "INSERT INTO schema_migrations VALUES (?)", [(i,) for i in range(1, 29)]
                )
            original = digest(snapshot)
            report_result = run_audit(snapshot)
            self.assertEqual(report_result.returncode, 0, report_result.stderr)
            report = json.loads(report_result.stdout)
            self.assertEqual(report["schema"]["snapshot_version"], 28)
            self.assertEqual(report["schema"]["source_version"], 29)
            self.assertEqual(report["schema"]["missing_applied_versions"], [29])
            self.assertEqual(report["counts"]["unsettled_worktree_cleanups"], 1)
            self.assertIn("unsettled_worktree_cleanups", report["observed_outstanding_obligations"])
            self.assertFalse(report["cutover_authorized"])
            self.assertEqual(digest(snapshot), original)

    def test_populated_snapshot_reports_counts_only_and_cannot_authorize_cutover(self) -> None:
        with tempfile.TemporaryDirectory(prefix="gg-snapshot-audit-") as root:
            snapshot = Path(root) / "consistent-copy.sqlite3"
            with sqlite3.connect(snapshot) as db:
                db.executescript(
                    """
                    CREATE TABLE schema_migrations(version INTEGER PRIMARY KEY);
                    CREATE TABLE sessions(id TEXT PRIMARY KEY, status TEXT);
                    CREATE TABLE workspace_session_ownership(session_id TEXT PRIMARY KEY);
                    CREATE TABLE turns(status TEXT);
                    CREATE TABLE approvals(status TEXT);
                    CREATE TABLE teams(id TEXT PRIMARY KEY);
                    CREATE TABLE legacy_workspace_migration_subjects(
                      classification TEXT, applied_at INTEGER);
                    CREATE TABLE legacy_workspace_migration_state(migration_key TEXT);
                    CREATE TABLE runtime_operations(phase TEXT);
                    CREATE TABLE runtime_operation_effects(phase TEXT);
                    CREATE TABLE runtime_operation_outbox(state TEXT);
                    CREATE TABLE agent_deliveries(id TEXT PRIMARY KEY, status TEXT);
                    CREATE TABLE managed_processes(status TEXT, completion_state TEXT);
                    CREATE TABLE managed_worktree_claims(released_at INTEGER);
                    CREATE TABLE runtime_events(id INTEGER PRIMARY KEY);
                    INSERT INTO sessions VALUES ('active','running'),('closed','closed');
                    INSERT INTO workspace_session_ownership VALUES ('closed');
                    INSERT INTO turns VALUES ('running'),('completed');
                    INSERT INTO approvals VALUES ('pending'),('approved');
                    INSERT INTO teams VALUES ('historical-team');
                    INSERT INTO legacy_workspace_migration_subjects VALUES
                      ('unresolved',NULL),('mapped',NULL),('mapped',42);
                    INSERT INTO legacy_workspace_migration_state
                      VALUES ('workspace_authority');
                    INSERT INTO runtime_operations VALUES ('requested'),('completed');
                    INSERT INTO runtime_operation_effects VALUES ('uncertain');
                    INSERT INTO runtime_operation_outbox VALUES ('pending');
                    INSERT INTO agent_deliveries VALUES ('a','deferred'),('b','injected');
                    INSERT INTO managed_processes VALUES
                      ('running','pending'),('completed','delivered');
                    INSERT INTO managed_worktree_claims VALUES (NULL),(13);
                    INSERT INTO runtime_events VALUES (1),(2);
                    """
                )
                db.executemany(
                    "INSERT INTO schema_migrations VALUES (?)", [(i,) for i in range(1, 28)]
                )
            snapshot.chmod(0o400)
            before_digest = digest(snapshot)
            before_names = sorted(p.name for p in snapshot.parent.iterdir())
            result = run_audit(snapshot)
            self.assertEqual(result.returncode, 0, result.stderr)
            report = json.loads(result.stdout)
            self.assertTrue(report["inspection_only"])
            self.assertFalse(report["cutover_authorized"])
            self.assertEqual(report["source_mode"], "immutable_read_only")
            self.assertEqual(report["schema"]["snapshot_version"], 27)
            self.assertEqual(report["schema"]["source_version"], 29)
            self.assertEqual(report["schema"]["missing_applied_versions"], [28, 29])
            self.assertTrue(report["schema"]["workspace_migration_preview_recorded"])
            for name in [
                "unresolved_legacy_subjects", "unapplied_classified_legacy_subjects",
                "active_sessions_without_workspace_ownership", "nonterminal_turns",
                "unresolved_approvals", "nonterminal_operations",
                "unfinalized_native_effects", "undelivered_outbox",
                "unsettled_agent_deliveries", "nonterminal_managed_processes",
                "undelivered_process_completions", "active_worktree_claims",
            ]:
                self.assertEqual(report["counts"][name], 1, name)
                self.assertIn(name, report["observed_outstanding_obligations"])
            self.assertEqual(report["counts"]["legacy_teams"], 1)
            self.assertEqual(report["counts"]["historical_events"], 2)
            self.assertNotIn("legacy_teams", report["observed_outstanding_obligations"])
            self.assertIsNone(report["counts"]["unsettled_legacy_deliveries"])
            self.assertIn("unsettled_legacy_deliveries", report["unknown_obligation_checks"])
            self.assertIsNone(report["counts"]["unsettled_worktree_cleanups"])
            self.assertIn("unsettled_worktree_cleanups", report["unknown_obligation_checks"])
            self.assertEqual(digest(snapshot), before_digest)
            self.assertEqual(
                sorted(p.name for p in snapshot.parent.iterdir()), before_names,
                "even SQLite sidecars must not be created by inspection",
            )

    def test_older_schema_must_report_unknown_not_zero_for_absent_tables(self) -> None:
        with tempfile.TemporaryDirectory(prefix="gg-snapshot-old-") as root:
            snapshot = Path(root) / "generation-one.sqlite3"
            with sqlite3.connect(snapshot) as db:
                db.executescript(
                    "CREATE TABLE sessions (id TEXT PRIMARY KEY,status TEXT);"
                    "CREATE TABLE teams (id TEXT PRIMARY KEY);"
                    "INSERT INTO sessions VALUES ('old','active');"
                    "INSERT INTO teams VALUES ('legacy');"
                )
            result = run_audit(snapshot)
            self.assertEqual(result.returncode, 0, result.stderr)
            report = json.loads(result.stdout)
            self.assertIsNone(report["schema"]["snapshot_version"])
            self.assertIsNone(report["schema"]["missing_applied_versions"])
            self.assertIsNone(report["schema"]["workspace_migration_preview_recorded"])
            self.assertIsNone(report["counts"]["unresolved_legacy_subjects"])
            self.assertIsNone(report["counts"]["active_sessions_without_workspace_ownership"])
            self.assertEqual(report["counts"]["legacy_sessions"], 1)
            self.assertEqual(report["counts"]["legacy_teams"], 1)
            self.assertIn("unresolved_legacy_subjects", report["unknown_obligation_checks"])
            self.assertFalse(report["cutover_authorized"])

    def test_gapped_migration_chain_is_reported_without_changing_snapshot(self) -> None:
        with tempfile.TemporaryDirectory(prefix="gg-snapshot-gap-") as root:
            snapshot = Path(root) / "gapped.sqlite3"
            with sqlite3.connect(snapshot) as db:
                db.execute("CREATE TABLE schema_migrations(version INTEGER PRIMARY KEY)")
                db.executemany(
                    "INSERT INTO schema_migrations VALUES (?)", [(1,), (3,), (27,)]
                )
            before = digest(snapshot)
            result = run_audit(snapshot)
            self.assertEqual(result.returncode, 0, result.stderr)
            report = json.loads(result.stdout)
            self.assertEqual(report["schema"]["snapshot_version"], 27)
            self.assertIn(2, report["schema"]["missing_applied_versions"])
            self.assertIn(26, report["schema"]["missing_applied_versions"])
            self.assertFalse(report["cutover_authorized"])
            self.assertEqual(digest(snapshot), before)

    def test_wal_and_journal_sidecars_fail_closed(self) -> None:
        with tempfile.TemporaryDirectory(prefix="gg-snapshot-sidecar-") as root:
            snapshot = Path(root) / "snapshot.sqlite3"
            with sqlite3.connect(snapshot) as db:
                db.execute("CREATE TABLE sessions(id TEXT)")
            before = digest(snapshot)
            for extension in ("-wal", "-shm", "-journal"):
                sidecar = snapshot.with_name(snapshot.name + extension)
                sidecar.write_bytes(b"not-checkpointed")
                result = run_audit(snapshot)
                self.assertEqual(result.returncode, 2, extension)
                self.assertIn("checkpointed", result.stderr)
                self.assertEqual(result.stdout, "")
                self.assertEqual(digest(snapshot), before)
                sidecar.unlink()

    def test_missing_non_sqlite_and_symlink_inputs_fail_closed(self) -> None:
        with tempfile.TemporaryDirectory(prefix="gg-snapshot-invalid-") as root:
            snapshot = Path(root) / "unavailable.sqlite3"
            result = run_audit(snapshot)
            self.assertEqual(result.returncode, 2)
            snapshot.write_text("not a SQLite database", encoding="utf-8")
            result = run_audit(snapshot)
            self.assertEqual(result.returncode, 2)
            self.assertEqual(result.stdout, "")
            shortcut = Path(root) / "link.sqlite3"
            shortcut.symlink_to(snapshot)
            result = run_audit(shortcut)
            self.assertEqual(result.returncode, 2)
            self.assertIn("non-symlink", result.stderr)


if __name__ == "__main__":
    unittest.main()
