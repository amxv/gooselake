#!/usr/bin/env python3
"""Offline-only adversarial tests for the retained event subject audit CLI."""

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


ROOT = Path(__file__).resolve().parents[1]
AUDIT = ROOT / "scripts/audit-event-subjects.py"
SUBJECT_MIGRATION = ROOT / "crates/runtime-store-sqlite/src/schema_migration_27.sql"


def digest(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def run_audit(path: Path) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, str(AUDIT), "--snapshot", str(path)],
        capture_output=True, text=True, timeout=35, check=False,
        env=dict(os.environ, PYTHONDONTWRITEBYTECODE="1"),
    )


def create_snapshot(path: Path, count: int = 1) -> None:
    with sqlite3.connect(path) as db:
        db.executescript(
            "CREATE TABLE schema_migrations(version INTEGER PRIMARY KEY);"
            "CREATE TABLE runtime_events("
            "id INTEGER PRIMARY KEY AUTOINCREMENT,"
            "event_id TEXT NOT NULL UNIQUE,scope TEXT NOT NULL,"
            "scope_id TEXT NOT NULL,turn_id TEXT,seq INTEGER NOT NULL,"
            "kind TEXT NOT NULL,critical INTEGER NOT NULL,"
            "payload_json TEXT NOT NULL,created_at INTEGER NOT NULL);"
            "CREATE UNIQUE INDEX idx_runtime_events_scope_seq "
            "ON runtime_events(scope,scope_id,seq);"
        )
        scopes = ("workspace", "session", "process", "turn", "message", "operation")
        db.executemany(
            "INSERT INTO runtime_events(event_id,scope,scope_id,turn_id,seq,kind,critical,payload_json,created_at)"
            " VALUES (?1,?2,?3,?4,?5,'test.retained',?6,?7,?8)",
            (
                (f"event_{index}", scopes[index % len(scopes)],
                 f"{scopes[index % len(scopes)]}_fixture",
                 "turn_fixture" if index % 2 == 0 else None,
                 index // len(scopes) + 1,
                 int(index % 3 != 0),
                 json.dumps({
                     "message_id": "msg_fixture" if index % 4 == 0 else None,
                     "subject": {"message_id": "msg_nested"} if index % 4 == 1 else {},
                     "operation_id": "op_fixture" if index % 5 == 0 else None,
                 }),
                 index + 10)
                for index in range(count)
            ),
        )
        db.executescript(SUBJECT_MIGRATION.read_text(encoding="utf-8"))
        db.executemany(
            "INSERT INTO schema_migrations(version) VALUES (?1)",
            ((number,) for number in range(1, 36)),
        )


class EventSubjectAuditTests(unittest.TestCase):
    def test_large_mixed_source_backfill_and_indexed_snapshot_are_consistent(self) -> None:
        with tempfile.TemporaryDirectory(prefix="gg-event-audit-scale-") as root:
            path = Path(root) / "checkpoint.sqlite3"
            create_snapshot(path, 2_600)
            with sqlite3.connect(path) as db:
                plan = " ".join(row[3] for row in db.execute(
                    "EXPLAIN QUERY PLAN SELECT 1 FROM runtime_event_subjects "
                    "WHERE subject_scope='message' AND subject_id='msg_fixture' AND event_row_id=100"
                ))
                self.assertIn("PRIMARY KEY", plan, plan)
                self.assertNotIn("USE TEMP B-TREE", plan)
                continuity_plan = " ".join(row[3] for row in db.execute(
                    "EXPLAIN QUERY PLAN SELECT id,scope,scope_id,seq "
                    "FROM runtime_events ORDER BY scope,scope_id,seq"
                ))
                self.assertIn("idx_runtime_events_scope_seq", continuity_plan)
                self.assertNotIn("USE TEMP B-TREE", continuity_plan)
            original = digest(path)
            result = run_audit(path)
            self.assertEqual(result.returncode, 0, result.stderr)
            report = json.loads(result.stdout)
            self.assertEqual(report["schema_version"], 35)
            self.assertEqual(report["counts"]["canonical_events"], 2_600)
            self.assertGreater(report["counts"]["stored_subject_links"], 2_600)
            self.assertEqual(report["counts"]["missing_subject_links"], 0)
            self.assertEqual(report["counts"]["unexpected_subject_links"], 0)
            self.assertEqual(report["counts"]["primary_scope_rows_checked"], 2_600)
            self.assertEqual(report["counts"]["missing_primary_scope_sequence_slots"], 0)
            self.assertEqual(report["counts"]["invalid_primary_scope_sequences"], 0)
            self.assertEqual(report["observed_high_water"]["last_retained_global_row_id"], 2_600)
            self.assertEqual(report["observed_high_water"]["first_retained_global_row_id"], 1)
            self.assertEqual(report["observed_high_water"]["primary_scope_count"], 6)
            self.assertIsNone(report["observed_high_water"]["persisted_recovery_safe_row_id"])
            self.assertFalse(report["observed_high_water"]["retention_authorized"])
            self.assertFalse(report["observed_high_water"]["live_fanout_certified"])
            self.assertTrue(report["consistent"])
            self.assertTrue(report["inspection_only"])
            self.assertFalse(report["repair_authorized"])
            self.assertEqual(digest(path), original)
            self.assertEqual(sorted(p.name for p in Path(root).iterdir()), [path.name])

    def test_cascaded_source_deletion_leaves_subject_parity_but_flags_scope_gap(self) -> None:
        # Deletes with FK cascades remove every derived subject link. A
        # subject-only audit previously returned green for this lost event.
        for deleted_id in (1, 7):
            with self.subTest(deleted_id=deleted_id), tempfile.TemporaryDirectory(
                prefix="gg-event-audit-gap-"
            ) as root:
                path = Path(root) / "checkpoint.sqlite3"
                create_snapshot(path, 30)
                with sqlite3.connect(path) as db:
                    db.execute("PRAGMA foreign_keys=ON")
                    db.execute("DELETE FROM runtime_events WHERE id=?", (deleted_id,))
                    self.assertEqual(db.execute(
                        "SELECT COUNT(*) FROM runtime_event_subjects s "
                        "LEFT JOIN runtime_events e ON e.id=s.event_row_id "
                        "WHERE e.id IS NULL"
                    ).fetchone()[0], 0)
                original = digest(path)
                result = run_audit(path)
                self.assertEqual(result.returncode, 1, result.stderr)
                report = json.loads(result.stdout)
                self.assertEqual(report["counts"]["missing_subject_links"], 0)
                self.assertEqual(report["counts"]["unexpected_subject_links"], 0)
                self.assertEqual(report["counts"]["canonical_events"], 29)
                self.assertEqual(report["counts"]["missing_primary_scope_sequence_slots"], 1)
                self.assertEqual(report["examples"]["primary_scope_sequences"][0]["reason"],
                                 "sequence_gap")
                self.assertEqual(report["observed_high_water"]["last_retained_global_row_id"], 30)
                self.assertFalse(report["consistent"])
                self.assertEqual(digest(path), original)

    def test_invalid_primary_scope_seq_is_not_certified_as_consistent(self) -> None:
        with tempfile.TemporaryDirectory(prefix="gg-event-audit-seq-") as root:
            path = Path(root) / "checkpoint.sqlite3"
            create_snapshot(path, 6)
            with sqlite3.connect(path) as db:
                db.execute(
                    "INSERT INTO runtime_events(event_id,scope,scope_id,seq,kind,critical,payload_json,created_at) "
                    "VALUES ('invalid_seq','workspace','workspace_fixture',0,'test.invalid',1,'{}',99)"
                )
            before = digest(path)
            result = run_audit(path)
            self.assertEqual(result.returncode, 1, result.stderr)
            report = json.loads(result.stdout)
            self.assertEqual(report["counts"]["invalid_primary_scope_sequences"], 1)
            self.assertEqual(report["counts"]["missing_subject_links"], 0)
            self.assertEqual(report["counts"]["unexpected_subject_links"], 0)
            self.assertEqual(report["observed_high_water"]["last_retained_global_row_id"], 7)
            self.assertEqual(digest(path), before)

    def test_missing_entire_tail_cannot_authorize_retention_from_observed_high_water(self) -> None:
        with tempfile.TemporaryDirectory(prefix="gg-event-audit-tail-limit-") as root:
            path = Path(root) / "checkpoint.sqlite3"
            create_snapshot(path, 6)
            with sqlite3.connect(path) as db:
                db.execute("PRAGMA foreign_keys=ON")
                db.execute("DELETE FROM runtime_events WHERE id=6")
            before = digest(path)
            result = run_audit(path)
            self.assertEqual(result.returncode, 0, result.stderr)
            report = json.loads(result.stdout)
            self.assertTrue(report["consistent"])
            self.assertEqual(report["observed_high_water"]["last_retained_global_row_id"], 5)
            self.assertIsNone(report["observed_high_water"]["persisted_recovery_safe_row_id"])
            self.assertFalse(report["observed_high_water"]["retention_authorized"])
            self.assertFalse(report["observed_high_water"]["live_fanout_certified"])
            self.assertEqual(digest(path), before)

    def test_missing_and_forged_subject_links_are_both_reported_without_repair(self) -> None:
        with tempfile.TemporaryDirectory(prefix="gg-event-audit-corrupt-") as root:
            path = Path(root) / "checkpoint.sqlite3"
            create_snapshot(path, 3)
            with sqlite3.connect(path) as db:
                db.execute(
                    "DELETE FROM runtime_event_subjects WHERE subject_scope='message' "
                    "AND subject_id='msg_fixture' AND event_row_id=1"
                )
                db.execute(
                    "INSERT INTO runtime_event_subjects(subject_scope,subject_id,event_row_id) "
                    "VALUES ('operation','op_forged',1)"
                )
            original = digest(path)
            path.chmod(0o400)
            result = run_audit(path)
            self.assertEqual(result.returncode, 1, result.stderr)
            report = json.loads(result.stdout)
            self.assertEqual(report["counts"]["missing_subject_links"], 1)
            self.assertEqual(report["counts"]["unexpected_subject_links"], 1)
            self.assertEqual(report["examples"]["missing"][0]["event_row_id"], 1)
            self.assertEqual(report["examples"]["unexpected"][0]["id"], "op_forged")
            self.assertFalse(report["consistent"])
            self.assertEqual(digest(path), original)
            self.assertEqual(sorted(p.name for p in Path(root).iterdir()), [path.name])

    def test_structured_source_ids_are_not_silently_certified_as_strings(self) -> None:
        with tempfile.TemporaryDirectory(prefix="gg-event-audit-structured-") as root:
            path = Path(root) / "checkpoint.sqlite3"
            create_snapshot(path, 1)
            with sqlite3.connect(path) as db:
                db.execute(
                    "INSERT INTO runtime_events(event_id,scope,scope_id,turn_id,seq,kind,critical,payload_json,created_at) "
                    "VALUES ('structured','workspace','workspace_fixture',NULL,2,'test.structured',1,?1,42)",
                    (json.dumps({
                        "message_id": {"not": "a-string"},
                        "subject": {"message_id": "must-not-override-top-level"},
                        "operation_id": ["not-an-operation-id"],
                    }),),
                )
            original = digest(path)
            result = run_audit(path)
            self.assertEqual(result.returncode, 1, result.stderr)
            report = json.loads(result.stdout)
            self.assertEqual(report["counts"]["nonstring_or_ambiguous_subject_sources"], 2)
            self.assertEqual(report["counts"]["unexpected_subject_links"], 2)
            self.assertEqual(report["counts"]["missing_subject_links"], 0)
            self.assertFalse(report["consistent"])
            self.assertEqual(digest(path), original)

    def test_critical_trigger_fault_rollback_leaves_clean_original_snapshot(self) -> None:
        with tempfile.TemporaryDirectory(prefix="gg-event-audit-fault-") as root:
            path = Path(root) / "checkpoint.sqlite3"
            create_snapshot(path, 1)
            with sqlite3.connect(path) as db:
                db.executescript(
                    "CREATE TRIGGER fixture_fault BEFORE INSERT ON runtime_event_subjects "
                    "WHEN NEW.subject_scope='message' AND NEW.subject_id='msg_fault' "
                    "BEGIN SELECT RAISE(ABORT,'simulated last-link fault'); END;"
                )
                with self.assertRaises(sqlite3.Error):
                    db.execute(
                        "INSERT INTO runtime_events(event_id,scope,scope_id,seq,kind,critical,payload_json,created_at) "
                        "VALUES ('rolled_back','workspace','workspace_fixture',2,'test.critical',1,"
                        "'{\"message_id\":\"msg_fault\"}',42)"
                    )
                db.execute("DROP TRIGGER fixture_fault")
                self.assertEqual(
                    db.execute("SELECT COUNT(*) FROM runtime_events").fetchone()[0], 1
                )
            result = run_audit(path)
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertTrue(json.loads(result.stdout)["consistent"])

    def test_missing_schema_or_applied_migration_is_not_a_safe_empty_result(self) -> None:
        for damage in (
            "DROP TRIGGER runtime_event_subjects_after_insert",
            "DROP TABLE runtime_event_subjects",
            "DELETE FROM schema_migrations WHERE version=27",
        ):
            with self.subTest(damage=damage), tempfile.TemporaryDirectory(
                prefix="gg-event-audit-schema-"
            ) as root:
                path = Path(root) / "checkpoint.sqlite3"
                create_snapshot(path, 1)
                with sqlite3.connect(path) as db:
                    db.execute(damage)
                original = digest(path)
                result = run_audit(path)
                self.assertEqual(result.returncode, 2, result.stderr)
                self.assertIn("refused", result.stderr)
                self.assertEqual(digest(path), original)

    def test_same_named_weakened_schema_is_refused_despite_matching_retained_links(self) -> None:
        for altered_trigger in (True, False):
            with self.subTest(altered_trigger=altered_trigger), tempfile.TemporaryDirectory(
                prefix="gg-event-audit-schema-spoof-"
            ) as root:
                path = Path(root) / "checkpoint.sqlite3"
                create_snapshot(path, 12)
                with sqlite3.connect(path) as db:
                    links = db.execute(
                        "SELECT subject_scope,subject_id,event_row_id "
                        "FROM runtime_event_subjects ORDER BY 1,2,3"
                    ).fetchall()
                    if altered_trigger:
                        db.executescript(
                            "DROP TRIGGER runtime_event_subjects_after_insert;"
                            "CREATE TRIGGER runtime_event_subjects_after_insert "
                            "AFTER INSERT ON runtime_events BEGIN SELECT 1; END;"
                        )
                    else:
                        db.executescript(
                            "DROP TABLE runtime_event_subjects;"
                            "CREATE TABLE runtime_event_subjects("
                            "subject_scope TEXT,subject_id TEXT,event_row_id INTEGER);"
                        )
                        db.executemany(
                            "INSERT INTO runtime_event_subjects VALUES (?,?,?)", links
                        )
                    self.assertEqual(db.execute(
                        "SELECT subject_scope,subject_id,event_row_id "
                        "FROM runtime_event_subjects ORDER BY 1,2,3"
                    ).fetchall(), links)
                before = digest(path)
                result = run_audit(path)
                self.assertEqual(result.returncode, 2, result.stderr)
                self.assertIn("canonical migration27", result.stderr)
                self.assertEqual(digest(path), before)

    def test_live_sidecars_and_symlink_snapshots_are_refused(self) -> None:
        with tempfile.TemporaryDirectory(prefix="gg-event-audit-sidecars-") as root:
            path = Path(root) / "checkpoint.sqlite3"
            create_snapshot(path, 1)
            before = digest(path)
            for suffix in ("-wal", "-shm", "-journal"):
                sidecar = path.with_name(path.name + suffix)
                sidecar.write_bytes(b"unsafe-journal")
                self.assertEqual(run_audit(path).returncode, 2)
                sidecar.unlink()
            link = Path(root) / "alias.sqlite3"
            link.symlink_to(path)
            self.assertEqual(run_audit(link).returncode, 2)
            self.assertEqual(digest(path), before)


if __name__ == "__main__":
    unittest.main()
