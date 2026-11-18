#!/usr/bin/env python3
"""Capture/verify an externally pinned witness for a sealed SQLite copy.

Capture emits canonical manifest bytes on stdout and their SHA-256 on stderr.
Verification requires that independently retained hash. Neither operation
creates a snapshot or writes a database or manifest file.
"""
from __future__ import annotations
import sys
sys.dont_write_bytecode = True
import argparse
import hashlib
import hmac
import importlib.util
import json
from pathlib import Path
import re
import sqlite3
from snapshot_inspection import SealedInput, schema_observation, sealed_database, seven_family_observations

HERE = Path(__file__).resolve().parent
FORMAT = "gooselake.sealed-snapshot-manifest.v1"
MAX_MANIFEST_BYTES = 1024 * 1024
SCOPE_FIELDS = ["scope", "scope_id", "row_count", "first_row_id", "last_row_id", "first_seq", "last_seq", "head_row_id", "head_event_id"]
SCHEMA_FIELDS = ["type", "name", "table", "sql"]
LIMITS = [
    "The manifest witnesses the bytes presented at capture time.",
    "It does not prove consistent acquisition from an original database.",
    "It cannot detect history already absent before the retained witness.",
    "An unprepared byte-identical copy retains the original journal epoch.",
    "Unknown native effects remain unknown and receive no retry authority.",
    "No restore, activation, repair, retention, pruning, or cutover is authorized.",
]

def canonical(value) -> bytes:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False).encode("utf-8") + b"\n"

def framed_digest(domain: bytes, fields: list[str], rows) -> dict:
    if not domain.endswith(b"\n"):
        raise ValueError("digest domain must have a newline delimiter")
    digest = hashlib.sha256(domain)
    digest.update(canonical(fields))
    count = 0
    for row in rows:
        digest.update(canonical(list(row)))
        count += 1
    return {"domain": domain.decode("ascii").rstrip("\n"), "fields": fields, "rows": count, "sha256": digest.hexdigest()}

def load_observer(filename: str, module_name: str):
    spec = importlib.util.spec_from_file_location(module_name, HERE / filename)
    if spec is None or spec.loader is None:
        raise ValueError(f"observer is unavailable: {filename}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module

def checked_scope_rows(connection: sqlite3.Connection):
    invalid = connection.execute(
        "SELECT COUNT(*) FROM runtime_events WHERE typeof(id)<>'integer' OR id<=0 "
        "OR typeof(seq)<>'integer' OR seq<=0 OR typeof(scope)<>'text' "
        "OR typeof(scope_id)<>'text' OR typeof(event_id)<>'text'"
    ).fetchone()[0]
    if invalid:
        raise ValueError("event head witness has invalid storage-typed coordinates")
    query = """
        SELECT e.scope,e.scope_id,COUNT(*),MIN(e.id),MAX(e.id),MIN(e.seq),MAX(e.seq),
          (SELECT h.id FROM runtime_events h
           WHERE h.scope=e.scope COLLATE BINARY AND h.scope_id=e.scope_id COLLATE BINARY
           ORDER BY h.seq DESC,h.id DESC LIMIT 1),
          (SELECT h.event_id FROM runtime_events h
           WHERE h.scope=e.scope COLLATE BINARY AND h.scope_id=e.scope_id COLLATE BINARY
           ORDER BY h.seq DESC,h.id DESC LIMIT 1)
        FROM runtime_events e
        GROUP BY e.scope COLLATE BINARY,e.scope_id COLLATE BINARY
        ORDER BY e.scope COLLATE BINARY,e.scope_id COLLATE BINARY
    """
    for row in connection.execute(query):
        if (len(row) != 9 or not isinstance(row[0], str) or not isinstance(row[1], str)
                or not isinstance(row[8], str) or any(type(v) is not int or v <= 0 for v in row[2:8])):
            raise ValueError("invalid per-scope head witness")
        yield row

def global_head(connection: sqlite3.Connection) -> dict:
    count = connection.execute("SELECT COUNT(*) FROM runtime_events").fetchone()[0]
    first = connection.execute("SELECT id,event_id,scope,scope_id,seq FROM runtime_events ORDER BY id ASC LIMIT 1").fetchone()
    last = connection.execute("SELECT id,event_id,scope,scope_id,seq FROM runtime_events ORDER BY id DESC LIMIT 1").fetchone()
    allocation = connection.execute("SELECT seq,typeof(seq) FROM sqlite_sequence WHERE name='runtime_events' LIMIT 2").fetchall()
    if len(allocation) > 1:
        raise ValueError("duplicate event allocation high-water rows")
    allocated_through = None
    if allocation:
        value, storage_type = allocation[0]
        if storage_type != "integer" or type(value) is not int or value < 0:
            raise ValueError("invalid event allocation high-water value")
        allocated_through = value
    if count and (first is None or last is None or allocated_through is None or allocated_through < last[0]):
        raise ValueError("event allocation carrier disagrees with retained rows")
    return {"row_count": count, "row_fields": ["id", "event_id", "scope", "scope_id", "seq"],
            "first_retained": list(first) if first is not None else None,
            "last_retained": list(last) if last is not None else None,
            "allocated_through_row_id": allocated_through, "original_history_completeness_proven": False}

def observations(connection: sqlite3.Connection) -> dict:
    schema = schema_observation(connection, require_current=True)
    if schema["journal_epoch"] is None:
        raise ValueError("current manifest requires its persisted event epoch")
    legacy = load_observer("audit-legacy-snapshot.py", "_manifest_legacy_observer")
    events = load_observer("audit-event-subjects.py", "_manifest_event_observer")
    version = events.verify_source_schema(connection)
    if version != schema["snapshot_version"]:
        raise ValueError("event and manifest schema observations disagree")
    retained = events.inspect(connection, version)
    inventory = legacy.observe(connection, schema)
    schema_rows = connection.execute("SELECT type,name,tbl_name,sql FROM sqlite_master ORDER BY type COLLATE BINARY,name COLLATE BINARY,tbl_name COLLATE BINARY,sql COLLATE BINARY")
    schema_sql = framed_digest(b"gooselake.snapshot.schema-sql.v1\n", SCHEMA_FIELDS, schema_rows)
    scopes = framed_digest(b"gooselake.snapshot.scope-heads.v1\n", SCOPE_FIELDS, checked_scope_rows(connection))
    return {"schema": schema, "schema_sql": schema_sql,
            "events": {"global": global_head(connection), "primary_scope_heads": scopes,
                       "retained_subject_audit": {"consistent": retained["consistent"], "counts": retained["counts"]}},
            "seven_families": seven_family_observations(connection), "inventory_counts": inventory["counts"],
            "unknown_obligation_checks": inventory["unknown_obligation_checks"],
            "observed_outstanding_obligations": inventory["observed_outstanding_obligations"]}

def capture_document(snapshot: Path) -> dict:
    with sealed_database(snapshot) as (connection, source):
        document = {"format": FORMAT, "snapshot": {"bytes": source.size, "sha256": source.sha256},
                    "observations": observations(connection), "inspection_only": True,
                    "cutover_authorized": False, "restore_authorized": False,
                    "native_retry_authorized": False, "limits": LIMITS}
    return document

def unique_object(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("manifest has duplicate JSON object keys")
        result[key] = value
    return result

def reject_constant(value):
    raise ValueError(f"manifest has nonstandard JSON constant: {value}")

def verify_document(snapshot: Path, manifest: Path, expected_manifest_sha256: str) -> dict:
    if re.fullmatch(r"[0-9a-f]{64}", expected_manifest_sha256) is None:
        raise ValueError("expected manifest SHA-256 must be 64 lowercase hex digits")
    with SealedInput(manifest, sqlite_sidecars=False, max_bytes=MAX_MANIFEST_BYTES) as retained:
        if not hmac.compare_digest(retained.sha256, expected_manifest_sha256):
            raise ValueError("retained manifest differs from its external SHA-256")
        raw = retained.read_small_file()
        document = json.loads(raw.decode("utf-8"), object_pairs_hook=unique_object, parse_constant=reject_constant)
        if not isinstance(document, dict) or document.get("format") != FORMAT:
            raise ValueError("unsupported manifest format")
        if raw != canonical(document):
            raise ValueError("manifest is not in the canonical v1 byte format")
        observed = capture_document(snapshot)
        # Canonical bytes avoid Python's loose True == 1 equality and reject
        # missing, extra, stale, altered, or mistyped fields.
        if raw != canonical(observed):
            raise ValueError("snapshot bytes or observations differ from retained manifest")
    return {"kind": "gooselake_sealed_snapshot_manifest_verification", "manifest_sha256": expected_manifest_sha256,
            "snapshot_sha256": observed["snapshot"]["sha256"], "manifest_match": True,
            "retained_history_consistent": observed["observations"]["events"]["retained_subject_audit"]["consistent"],
            "seven_families": observed["observations"]["seven_families"], "inspection_only": True,
            "cutover_authorized": False, "restore_authorized": False, "native_retry_authorized": False, "limits": LIMITS}

def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    capture = commands.add_parser("capture")
    capture.add_argument("--snapshot", required=True, type=Path)
    verify = commands.add_parser("verify")
    verify.add_argument("--snapshot", required=True, type=Path)
    verify.add_argument("--manifest", required=True, type=Path)
    verify.add_argument("--expected-manifest-sha256", required=True)
    args = parser.parse_args()
    try:
        if args.command == "capture":
            raw = canonical(capture_document(args.snapshot))
            if len(raw) > MAX_MANIFEST_BYTES:
                raise ValueError("generated manifest exceeds the v1 byte limit")
            digest = hashlib.sha256(raw).hexdigest()
            sys.stdout.buffer.write(raw)
            sys.stdout.buffer.flush()
            print(f"retain independently: manifest_sha256={digest}", file=sys.stderr)
        else:
            result = verify_document(args.snapshot, args.manifest, args.expected_manifest_sha256)
            sys.stdout.buffer.write(canonical(result))
        return 0
    except (OSError, ValueError, UnicodeError, sqlite3.Error, RecursionError, TypeError, KeyError) as error:
        print(f"snapshot manifest refused: {error}", file=sys.stderr)
        return 2

if __name__ == "__main__":
    sys.exit(main())
