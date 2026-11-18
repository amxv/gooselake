#!/usr/bin/env python3
"""Disposable observer fixtures, separate from the real-schema Rust proof."""
from __future__ import annotations
import sys
sys.dont_write_bytecode = True
from contextlib import closing
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import shutil
import sqlite3
import subprocess
import tempfile
import unittest
from unittest import mock

HERE = Path(__file__).resolve().parent
ROOT = HERE.parent
sys.path.insert(0, str(HERE))
import snapshot_inspection as inspection

EXPECTED_SOURCE_VERSION = 53
EPOCH_SQL = ROOT / "crates/runtime-store-sqlite/src/schema_migration_53.sql"
INITIAL_EPOCH = "00112233445566778899aabbccddeeff"
PREPARED_EPOCH = "ffeeddccbbaa99887766554433221100"
MANIFEST_CLI = HERE / "audit-snapshot-manifest.py"
LEGACY_CLI = HERE / "audit-legacy-snapshot.py"
EVENT_CLI = HERE / "audit-event-subjects.py"

def load(filename, name):
    spec = importlib.util.spec_from_file_location(name, HERE / filename)
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module

event_fixtures = load("test-event-subject-audit.py", "_sealed_event_fixtures")
events = load("audit-event-subjects.py", "_sealed_event_observer")
legacy = load("audit-legacy-snapshot.py", "_sealed_legacy_observer")
manifest = load("audit-snapshot-manifest.py", "_sealed_manifest")

def digest(data):
    return hashlib.sha256(data).hexdigest()

def run_cli(script, *args):
    return subprocess.run([sys.executable, str(script), *map(str, args)],
                          check=False, capture_output=True, text=True, timeout=20,
                          env=dict(os.environ, PYTHONDONTWRITEBYTECODE="1"))

def execute(path, sql, parameters=()):
    with closing(sqlite3.connect(path)) as db:
        db.execute("PRAGMA foreign_keys=ON")
        with db:
            db.execute(sql, parameters)

def execute_script(path, sql):
    with closing(sqlite3.connect(path)) as db:
        db.execute("PRAGMA foreign_keys=ON")
        db.executescript(sql)

def assert_no_sidecars(test, path):
    for suffix in ("-wal", "-shm", "-journal"):
        with test.assertRaises(FileNotFoundError):
            os.lstat(path.with_name(path.name + suffix))

def create_current_observer_fixture(path, count=18):
    # Actual schema27 projection/trigger, indexed event rows and prefix1..35.
    # The later carriers below are explicitly synthetic observation fixtures.
    event_fixtures.create_snapshot(path, count)
    with closing(sqlite3.connect(path)) as db:
        with db:
            db.executemany("INSERT INTO schema_migrations(version) VALUES (?)",
                           [(n,) for n in range(36, EXPECTED_SOURCE_VERSION + 1)])
        db.executescript(EPOCH_SQL.read_text(encoding="utf-8"))
        with db:
            db.execute("UPDATE runtime_event_journal_identity SET epoch=? WHERE singleton=1", (INITIAL_EPOCH,))
        db.executescript("""
            CREATE TABLE turns(status);
            CREATE TABLE turn_admissions(dispatch_state);
            CREATE TABLE managed_processes(status,completion_state);
            CREATE TABLE approvals(status);
            CREATE TABLE workspace_hook_effects(phase,application_state);
            CREATE TABLE session_edit_reruns(phase);
            CREATE TABLE workspace_agent_rebinds(phase);
            CREATE TABLE workspace_retirement_fences(phase);
            INSERT INTO turns VALUES ('running'),('completed');
            INSERT INTO turn_admissions VALUES ('dispatching'),('dispatched');
            INSERT INTO managed_processes VALUES ('completed','injecting'),('completed','delivered');
            INSERT INTO approvals VALUES ('decision_pending'),('accept'),('decline');
            INSERT INTO workspace_hook_effects VALUES ('started','not_ready'),('succeeded','pending'),('succeeded','applied');
            INSERT INTO session_edit_reruns VALUES ('native_started'),('completed');
            INSERT INTO workspace_agent_rebinds VALUES ('intended'),('completed');
            INSERT INTO workspace_retirement_fences VALUES ('manual_review'),('retired');
        """)

class SealedFixture(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="gg-sealed-manifest-")
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.snapshot = self.root / "sealed.sqlite3"
        self.assertEqual(inspection.source_schema_version(), EXPECTED_SOURCE_VERSION)
        create_current_observer_fixture(self.snapshot)
        assert_no_sidecars(self, self.snapshot)

    def capture(self):
        document = manifest.capture_document(self.snapshot)
        return document, manifest.canonical(document)

    def retained(self, raw=None, name="manifest.json"):
        if raw is None:
            _, raw = self.capture()
        path = self.root / name
        with path.open("xb") as output:
            output.write(raw)
        return path, digest(raw)

    def unchanged(self, action):
        before = self.snapshot.read_bytes()
        names = sorted(p.name for p in self.root.iterdir())
        result = action()
        self.assertEqual(self.snapshot.read_bytes(), before)
        self.assertEqual(sorted(p.name for p in self.root.iterdir()), names)
        assert_no_sidecars(self, self.snapshot)
        return result

    def test_capture_and_external_pin_are_read_only(self):
        document, raw = self.unchanged(self.capture)
        self.assertEqual(document["snapshot"], {"bytes": self.snapshot.stat().st_size, "sha256": digest(self.snapshot.read_bytes())})
        self.assertEqual(document["observations"]["schema"]["journal_epoch"], INITIAL_EPOCH)
        self.assertTrue(document["observations"]["events"]["retained_subject_audit"]["consistent"])
        for key in ["cutover_authorized", "restore_authorized", "native_retry_authorized"]:
            self.assertFalse(document[key])
        retained, pin = self.retained(raw)
        report = self.unchanged(lambda: manifest.verify_document(self.snapshot, retained, pin))
        self.assertTrue(report["manifest_match"])
        result = self.unchanged(lambda: run_cli(MANIFEST_CLI, "capture", "--snapshot", self.snapshot))
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(result.stdout.encode("utf-8"), raw)
        self.assertIn(digest(raw), result.stderr)

    def test_cli_requires_external_hash(self):
        retained, _ = self.retained()
        result = run_cli(MANIFEST_CLI, "verify", "--snapshot", self.snapshot, "--manifest", retained)
        self.assertEqual(result.returncode, 2)
        self.assertEqual(result.stdout, "")
        self.assertIn("--expected-manifest-sha256", result.stderr)

    def test_composition_uses_one_connection_and_transaction(self):
        real_connect = sqlite3.connect
        connections, statements = [], []
        def tracked_connect(*args, **kwargs):
            db = real_connect(*args, **kwargs)
            connections.append(db)
            db.set_trace_callback(statements.append)
            return db
        with mock.patch.object(inspection.sqlite3, "connect", side_effect=tracked_connect):
            self.unchanged(lambda: manifest.capture_document(self.snapshot))
        normalized = [s.strip().upper() for s in statements]
        self.assertEqual(len(connections), 1)
        self.assertEqual(sum(s.startswith("BEGIN") for s in normalized), 1)
        self.assertEqual(normalized.count("ROLLBACK"), 1)
        self.assertFalse(any(s.startswith("COMMIT") for s in normalized))

    def test_observers_cannot_write_or_end_transaction(self):
        before = self.snapshot.read_bytes()
        with inspection.sealed_database(self.snapshot) as (db, source):
            self.assertEqual(db.execute("PRAGMA query_only").fetchone(), (1,))
            for statement in ["COMMIT", "ROLLBACK", "PRAGMA query_only=OFF", "PRAGMA user_version=7",
                              "UPDATE approvals SET status='accept'", "DROP TABLE approvals",
                              "ATTACH DATABASE ':memory:' AS forbidden"]:
                with self.subTest(statement=statement), self.assertRaises(sqlite3.DatabaseError):
                    db.execute(statement)
                self.assertTrue(db.in_transaction)
            self.assertEqual(source.sha256, digest(before))
        self.assertEqual(self.snapshot.read_bytes(), before)

    def test_symlink_ancestors_dotdot_and_hardlinks_refuse(self):
        leaf = self.root / "leaf.sqlite3"
        leaf.symlink_to(self.snapshot)
        ancestor = self.root / "ancestor"
        ancestor.symlink_to(self.root, target_is_directory=True)
        nested = self.root / "nested"
        nested.mkdir()
        (nested / "jump").symlink_to(self.root, target_is_directory=True)
        dangling = self.root / "dangling.sqlite3"
        dangling.symlink_to(self.root / "not-present")
        for path in [leaf, ancestor / self.snapshot.name, nested / "jump" / ".." / self.snapshot.name, dangling]:
            with self.subTest(path=path), self.assertRaises((OSError, ValueError)):
                manifest.capture_document(path)
        alias = self.root / "hard-link.sqlite3"
        os.link(self.snapshot, alias)
        for path in [alias, self.snapshot]:
            with self.assertRaises(ValueError):
                manifest.capture_document(path)

    def test_all_sidecar_entries_refuse_by_all_clis(self):
        for suffix in ("-wal", "-shm", "-journal"):
            for kind in ("file", "directory", "fifo", "symlink", "dangling"):
                with self.subTest(suffix=suffix, kind=kind):
                    sidecar = self.snapshot.with_name(self.snapshot.name + suffix)
                    if kind == "file": sidecar.write_bytes(b"sidecar")
                    elif kind == "directory": sidecar.mkdir()
                    elif kind == "fifo": os.mkfifo(sidecar)
                    elif kind == "symlink": sidecar.symlink_to(self.snapshot)
                    else: sidecar.symlink_to(self.root / "absent-target")
                    try:
                        for script, prefix in [(LEGACY_CLI, []), (EVENT_CLI, []), (MANIFEST_CLI, ["capture"])]:
                            result = run_cli(script, *prefix, "--snapshot", self.snapshot)
                            self.assertEqual(result.returncode, 2, result.stderr)
                            self.assertEqual(result.stdout, "")
                    finally:
                        if kind == "directory": sidecar.rmdir()
                        else: sidecar.unlink()

    def test_guard_detects_observed_changes_after_entry(self):
        original = self.snapshot.read_bytes()
        for damage in ("append", "truncate", "replace", "modify-restore-mtime"):
            path = self.root / f"{damage}.sqlite3"
            path.write_bytes(original)
            with self.subTest(damage=damage), self.assertRaises(ValueError):
                with inspection.SealedInput(path, sqlite_sidecars=True):
                    if damage == "append":
                        with path.open("ab") as output: output.write(b"added")
                    elif damage == "truncate":
                        with path.open("r+b") as output: output.truncate(len(original) - 1)
                    elif damage == "replace":
                        replacement = self.root / "replacement.sqlite3"
                        replacement.write_bytes(original)
                        os.replace(replacement, path)
                    else:
                        before = path.stat()
                        with path.open("r+b") as output:
                            output.seek(len(original) - 1)
                            output.write(bytes([original[-1] ^ 1]))
                        os.utime(path, ns=(before.st_atime_ns, before.st_mtime_ns))
        sidecar = self.snapshot.with_name(self.snapshot.name + "-wal")
        with self.assertRaises(ValueError):
            with inspection.SealedInput(self.snapshot, sqlite_sidecars=True):
                sidecar.symlink_to(self.root / "not-created")
        sidecar.unlink()
        directory = self.root / "owned"
        directory.mkdir()
        path = directory / "copy.sqlite3"
        path.write_bytes(original)
        parked = self.root / "parked"
        with self.assertRaises(ValueError):
            with inspection.SealedInput(path, sqlite_sidecars=True):
                directory.rename(parked)
                directory.symlink_to(parked, target_is_directory=True)

    def test_typed_bounded_migration_markers(self):
        cases = {"text": list(range(1, 53)) + ["53"], "real": list(range(1, 53)) + [53.0],
                 "null": list(range(1, 53)) + [None], "blob": list(range(1, 53)) + [sqlite3.Binary(b"53")],
                 "zero": [0] + list(range(1, 54)), "negative": [-1] + list(range(1, 54)),
                 "duplicate": list(range(1, 54)) + [53], "future": list(range(1, 55)),
                 "huge_future": list(range(1, 54)) + [2**63 - 1]}
        for name, markers in cases.items():
            path = self.root / f"markers-{name}.sqlite3"
            create_current_observer_fixture(path)
            with closing(sqlite3.connect(path)) as db:
                db.executescript("DROP TABLE schema_migrations; CREATE TABLE schema_migrations(version);")
                with db: db.executemany("INSERT INTO schema_migrations VALUES (?)", [(v,) for v in markers])
            before = path.read_bytes()
            with self.subTest(name=name), self.assertRaises(ValueError):
                with inspection.sealed_database(path) as (db, _): inspection.schema_observation(db)
            self.assertEqual(path.read_bytes(), before)

    def test_older_and_gapped_chains_are_observations(self):
        old = self.root / "old.sqlite3"
        event_fixtures.create_snapshot(old, 6)
        before = old.read_bytes()
        report = legacy.audit_snapshot(old)
        self.assertEqual(report["schema"]["snapshot_version"], 35)
        self.assertIsNone(report["schema"]["journal_epoch"])
        self.assertIsNone(report["counts"]["unresolved_approvals"])
        self.assertFalse(report["cutover_authorized"])
        with self.assertRaises(ValueError): manifest.capture_document(old)
        self.assertEqual(old.read_bytes(), before)
        execute(self.snapshot, "DELETE FROM schema_migrations WHERE version=27")
        with inspection.sealed_database(self.snapshot) as (db, _):
            observed = inspection.schema_observation(db)
            self.assertFalse(observed["complete_prefix"])
            self.assertIn(27, observed["missing_applied_versions"])
            with self.assertRaises(ValueError): inspection.schema_observation(db, require_current=True)
            with self.assertRaises(ValueError): events.verify_source_schema(db)

    def test_current_epoch_damage_refuses_without_repair(self):
        damage = {
            "missing-row": "DELETE FROM runtime_event_journal_identity",
            "missing-table": "DROP TABLE runtime_event_journal_identity",
            "bad": "PRAGMA ignore_check_constraints=ON; UPDATE runtime_event_journal_identity SET epoch='bad';",
            "uppercase": "PRAGMA ignore_check_constraints=ON; UPDATE runtime_event_journal_identity SET epoch='AABBCCDDEEFF00112233445566778899';",
            "blob": "PRAGMA ignore_check_constraints=ON; UPDATE runtime_event_journal_identity SET epoch=zeroblob(16);",
            "singleton": "PRAGMA ignore_check_constraints=ON; UPDATE runtime_event_journal_identity SET singleton=2;",
            "extra": "PRAGMA ignore_check_constraints=ON; INSERT INTO runtime_event_journal_identity VALUES(2,'ffeeddccbbaa99887766554433221100');",
            "weak-null": "DROP TABLE runtime_event_journal_identity; CREATE TABLE runtime_event_journal_identity(singleton,epoch); INSERT INTO runtime_event_journal_identity VALUES(1,NULL);",
            "weak-valid": f"DROP TABLE runtime_event_journal_identity; CREATE TABLE runtime_event_journal_identity(singleton,epoch); INSERT INTO runtime_event_journal_identity VALUES(1,'{INITIAL_EPOCH}');",
            "view-valid": f"DROP TABLE runtime_event_journal_identity; CREATE VIEW runtime_event_journal_identity AS SELECT 1 AS singleton,'{INITIAL_EPOCH}' AS epoch;",
        }
        for name, sql in damage.items():
            path = self.root / f"epoch-{name}.sqlite3"
            create_current_observer_fixture(path)
            execute_script(path, sql)
            before = path.read_bytes()
            for _ in range(2):
                with self.subTest(name=name), self.assertRaises(ValueError): manifest.capture_document(path)
                self.assertEqual(path.read_bytes(), before)
                assert_no_sidecars(self, path)

    def test_approval_terminals_and_malformed_carriers(self):
        terminal = ("accept", "decline", "approved", "accepted", "denied", "rejected", "cancelled", "expired", "failed", "resolved")
        unresolved = (None, "decision_pending", "unknown_future", 9, sqlite3.Binary(b"not-text"))
        with closing(sqlite3.connect(self.snapshot)) as db:
            with db:
                db.execute("DELETE FROM approvals")
                db.executemany("INSERT INTO approvals VALUES (?)", [(v,) for v in (*terminal, *unresolved)])
        report = self.unchanged(lambda: legacy.audit_snapshot(self.snapshot))
        self.assertEqual(report["counts"]["unresolved_approvals"], len(unresolved))
        with inspection.sealed_database(self.snapshot) as (db, _): families = inspection.seven_family_observations(db)
        self.assertEqual(families["approval"]["unresolved_rows_by_carrier"]["approvals.status"], len(unresolved))
        self.assertTrue(families["approval"]["has_observed_unresolved_rows"])

    def test_all_seven_families_and_missing_is_unknown(self):
        document, _ = self.capture()
        families = document["observations"]["seven_families"]
        self.assertEqual(set(families), {"turn", "process_completion", "approval", "hook", "edit", "rebind", "retirement"})
        for observation in families.values():
            self.assertTrue(observation["all_carriers_observed"])
            self.assertTrue(observation["has_observed_unresolved_rows"])
        execute(self.snapshot, "DROP TABLE session_edit_reruns")
        document, _ = self.capture()
        edit = document["observations"]["seven_families"]["edit"]
        self.assertEqual(edit["unresolved_rows_by_carrier"], {"session_edit_reruns.phase": None})
        self.assertFalse(edit["all_carriers_observed"])
        self.assertFalse(document["cutover_authorized"])

    def test_pin_strict_json_and_semantic_bytes(self):
        _, original = self.capture()
        retained, pin = self.retained(original)
        wrong = ("0" if pin[0] != "0" else "1") + pin[1:]
        with self.assertRaisesRegex(ValueError, "external SHA-256"): manifest.verify_document(self.snapshot, retained, wrong)
        retained.write_bytes(original + b" ")
        with self.assertRaisesRegex(ValueError, "external SHA-256"): manifest.verify_document(self.snapshot, retained, pin)
        duplicate = b'{"format":' + json.dumps(manifest.FORMAT).encode() + b"," + original[1:]
        for name, raw in {"duplicate": duplicate, "noncanonical": original + b" ", "constant": b'{"format":NaN}\n'}.items():
            path, new_pin = self.retained(raw, name + ".json")
            with self.assertRaises(ValueError): manifest.verify_document(self.snapshot, path, new_pin)
        for name, value in [("true", True), ("integer", 0)]:
            altered = json.loads(original)
            altered["cutover_authorized"] = value
            path, new_pin = self.retained(manifest.canonical(altered), name + ".json")
            with self.assertRaisesRegex(ValueError, "differ"): manifest.verify_document(self.snapshot, path, new_pin)

    def test_payload_change_with_identical_heads_fails_witness(self):
        before, raw = self.capture()
        retained, pin = self.retained(raw)
        with closing(sqlite3.connect(self.snapshot)) as db:
            payload = json.loads(db.execute("SELECT payload_json FROM runtime_events WHERE id=1").fetchone()[0])
            payload["nonrouting_fixture_note"] = "changed"
            with db: db.execute("UPDATE runtime_events SET payload_json=? WHERE id=1", (json.dumps(payload),))
        after, _ = self.capture()
        self.assertEqual(before["observations"]["events"], after["observations"]["events"])
        self.assertNotEqual(before["snapshot"]["sha256"], after["snapshot"]["sha256"])
        with self.assertRaisesRegex(ValueError, "differ"): manifest.verify_document(self.snapshot, retained, pin)

    def test_retained_witness_detects_tail_scope_loss_but_not_precapture_loss(self):
        for name, sql in [("tail", "DELETE FROM runtime_events WHERE id=(SELECT MAX(id) FROM runtime_events)"),
                          ("scope", "DELETE FROM runtime_events WHERE scope='workspace'")]:
            path = self.root / f"lost-{name}.sqlite3"
            create_current_observer_fixture(path, 18)
            raw = manifest.canonical(manifest.capture_document(path))
            retained, pin = self.retained(raw, f"lost-{name}.json")
            execute(path, sql)
            damaged = manifest.capture_document(path)
            self.assertTrue(damaged["observations"]["events"]["retained_subject_audit"]["consistent"])
            with self.assertRaisesRegex(ValueError, "differ"): manifest.verify_document(path, retained, pin)
            new_manifest, new_pin = self.retained(manifest.canonical(damaged), f"post-loss-{name}.json")
            report = manifest.verify_document(path, new_manifest, new_pin)
            self.assertTrue(report["manifest_match"])
            self.assertFalse(report["cutover_authorized"])
            self.assertTrue(any("already absent" in text for text in report["limits"]))

    def test_unprepared_copy_matches_and_prepared_epoch_needs_new_witness(self):
        source_bytes = self.snapshot.read_bytes()
        original, raw = self.capture()
        retained, pin = self.retained(raw)
        target = self.root / "owned-target.sqlite3"
        shutil.copyfile(self.snapshot, target)  # Closed DELETE-journal fixture.
        self.assertTrue(manifest.verify_document(target, retained, pin)["manifest_match"])
        execute(target, "UPDATE runtime_event_journal_identity SET epoch=? WHERE singleton=1", (PREPARED_EPOCH,))
        with self.assertRaisesRegex(ValueError, "differ"): manifest.verify_document(target, retained, pin)
        self.assertTrue(manifest.verify_document(self.snapshot, retained, pin)["manifest_match"])
        prepared = manifest.capture_document(target)
        self.assertEqual(prepared["observations"]["schema"]["journal_epoch"], PREPARED_EPOCH)
        self.assertEqual(original["observations"]["events"], prepared["observations"]["events"])
        self.assertNotEqual(original["snapshot"]["sha256"], prepared["snapshot"]["sha256"])
        self.assertEqual(self.snapshot.read_bytes(), source_bytes)

    def test_binary_scope_framing_has_independent_literal_oracle(self):
        # Deliberately inherit NOCASE and insert reverse BINARY order. The
        # witness must still distinguish A/a and preserve exact UTF-8 bytes.
        with closing(sqlite3.connect(":memory:")) as db:
            db.execute("CREATE TABLE runtime_events(id INTEGER,event_id TEXT,scope TEXT COLLATE NOCASE,scope_id TEXT COLLATE NOCASE,seq INTEGER)")
            db.executemany("INSERT INTO runtime_events VALUES(?,?,'session',?,1)", [(1, "evt-é", "é"), (2, "evt-a", "a"), (3, "evt-A", "A")])
            rows = list(manifest.checked_scope_rows(db))
        expected = [("session", "A", 1, 3, 3, 1, 1, 3, "evt-A"), ("session", "a", 1, 2, 2, 1, 1, 2, "evt-a"), ("session", "é", 1, 1, 1, 1, 1, 1, "evt-é")]
        self.assertEqual(rows, expected)
        literal = (b"gooselake.snapshot.scope-heads.v1\n"
                   b'["scope","scope_id","row_count","first_row_id","last_row_id","first_seq","last_seq","head_row_id","head_event_id"]\n'
                   b'["session","A",1,3,3,1,1,3,"evt-A"]\n'
                   b'["session","a",1,2,2,1,1,2,"evt-a"]\n'
                   b'["session","\xc3\xa9",1,1,1,1,1,1,"evt-\xc3\xa9"]\n')
        actual = manifest.framed_digest(b"gooselake.snapshot.scope-heads.v1\n", manifest.SCOPE_FIELDS, iter(rows))
        self.assertEqual(actual["rows"], 3)
        self.assertEqual(actual["sha256"], digest(literal))

    def test_empty_journal_has_explicit_empty_heads(self):
        empty = self.root / "empty.sqlite3"
        create_current_observer_fixture(empty, 0)
        document = manifest.capture_document(empty)
        observed = document["observations"]["events"]
        self.assertEqual(observed["global"]["row_count"], 0)
        self.assertIsNone(observed["global"]["first_retained"])
        self.assertIsNone(observed["global"]["last_retained"])
        self.assertIsNone(observed["global"]["allocated_through_row_id"])
        self.assertEqual(observed["primary_scope_heads"]["rows"], 0)
        self.assertFalse(observed["global"]["original_history_completeness_proven"])

if __name__ == "__main__":
    unittest.main()
