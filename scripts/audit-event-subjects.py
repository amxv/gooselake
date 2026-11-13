#!/usr/bin/env python3
"""Read-only comparison of retained events against their derived subject index.

Pass ONLY a consistently checkpointed, isolated offline SQLite copy. This
command is not a repair, migration, retention decision or native replay tool.
It never opens a database for writing and never manufactures missing events.
"""

from __future__ import annotations

import argparse
from contextlib import closing
import json
from pathlib import Path
import sqlite3
import sys
from typing import Any


SUBJECT_SCOPES = frozenset({"turn", "message", "operation"})
SAMPLE_LIMIT = 12


def isolated_snapshot(path: Path) -> None:
    if not path.is_file() or any(part.is_symlink() for part in (path, *path.parents)):
        raise ValueError("snapshot must be a regular file without symlink components")
    for suffix in ("-wal", "-shm", "-journal"):
        if path.with_name(path.name + suffix).exists():
            raise ValueError(
                "snapshot has SQLite journal sidecars; provide a consistently "
                "checkpointed isolated copy, not a live database"
            )


def verify_source_schema(connection: sqlite3.Connection) -> int:
    def columns(table: str) -> set[str]:
        # Only trusted, fixed table identifiers are passed to this function.
        return {row[1] for row in connection.execute(f'PRAGMA table_info("{table}")')}

    required = {
        "runtime_events": {"id", "scope", "scope_id", "turn_id", "payload_json"},
        "runtime_event_subjects": {"subject_scope", "subject_id", "event_row_id"},
        "schema_migrations": {"version"},
    }
    for table, names in required.items():
        if not names <= columns(table):
            raise ValueError(f"snapshot lacks required indexed event schema: {table}")
    # A trigger with the expected name but an empty/weakened body can leave
    # every retained link consistent while silently corrupting FUTURE writes.
    # Attest both full definitions against migration 27, including constraints,
    # instead of accepting merely matching object names or column presence.
    migration = (Path(__file__).resolve().parents[1] /
                 "crates/runtime-store-sqlite/src/schema_migration_27.sql").read_text(
                     encoding="utf-8"
                 )
    table_start = migration.index("CREATE TABLE runtime_event_subjects (")
    table_end = migration.index(") WITHOUT ROWID;", table_start) + len(") WITHOUT ROWID;")
    trigger_start = migration.index("CREATE TRIGGER runtime_event_subjects_after_insert")
    for kind, name, parent, expected in (
        ("table", "runtime_event_subjects", "runtime_event_subjects",
         migration[table_start:table_end]),
        ("trigger", "runtime_event_subjects_after_insert", "runtime_events",
         migration[trigger_start:]),
    ):
        found = connection.execute(
            "SELECT sql FROM sqlite_master WHERE type=? AND name=? AND tbl_name=?",
            (kind, name, parent),
        ).fetchone()
        if found is None or found[0].strip().rstrip(";") != expected.strip().rstrip(";"):
            raise ValueError(f"snapshot lacks canonical migration27 {name} SQL definition")
    migrations = [row[0] for row in connection.execute(
        "SELECT version FROM schema_migrations ORDER BY version"
    )]
    if not migrations or 27 not in migrations or migrations != list(range(1, max(migrations) + 1)):
        raise ValueError("snapshot lacks complete applied migration chain through schema27")
    return max(migrations)


def source_links(
    row_id: int, scope: str, scope_id: str, turn_id: str | None,
    payload_text: str,
) -> tuple[set[tuple[str, str, int]], list[str]]:
    """Independent semantic oracle: only actual string JSON IDs are subjects.

    SQLite json_extract may stringify an object/array while returning TEXT;
    such payloads are intentionally flagged instead of accepted as IDs.
    """
    links = set()
    if scope in SUBJECT_SCOPES and isinstance(scope_id, str) and scope_id:
        links.add((scope, scope_id, row_id))
    if isinstance(turn_id, str) and turn_id:
        links.add(("turn", turn_id, row_id))
    anomalies = []
    try:
        payload = json.loads(payload_text, parse_constant=lambda value: (_ for _ in ()).throw(
            ValueError(f"nonstandard JSON constant {value}")
        ))
    except (TypeError, ValueError, UnicodeError):
        return links, ["malformed_payload_json"]
    if not isinstance(payload, dict):
        return links, anomalies

    def subject_text(value: Any, field: str) -> str | None:
        if isinstance(value, (list, dict)):
            anomalies.append(f"structured_{field}")
        elif isinstance(value, str) and "\x00" in value:
            # SQLite length() on extracted text and Python len() disagree
            # around NUL. Do not certify either interpretation silently.
            anomalies.append(f"nul_in_{field}")
        if isinstance(value, str) and "\x00" not in value and 1 <= len(value) <= 256:
            return value
        return None

    # Match schema27's COALESCE precedence: only a JSON null permits fallback.
    message = payload.get("message_id")
    if message is None and isinstance(payload.get("subject"), dict):
        message = payload["subject"].get("message_id")
    if (id_text := subject_text(message, "message_id")) is not None:
        links.add(("message", id_text, row_id))
    if (id_text := subject_text(payload.get("operation_id"), "operation_id")) is not None:
        links.add(("operation", id_text, row_id))
    return links, anomalies


def inspect(connection: sqlite3.Connection, schema_version: int) -> dict[str, Any]:
    # One SQLite read transaction covers *both* independent scans; the input
    # file is immutable and offline. No loaded set grows with the database.
    connection.execute("BEGIN")
    event_count = projected_count = expected_count = 0
    missing_count = unexpected_count = malformed_count = structured_count = 0
    missing_samples: list[dict[str, Any]] = []
    unexpected_samples: list[dict[str, Any]] = []
    source_samples: list[dict[str, Any]] = []
    membership_sql = (
        "SELECT 1 FROM runtime_event_subjects WHERE "
        "subject_scope=?1 AND subject_id=?2 AND event_row_id=?3"
    )

    events = connection.execute(
        "SELECT id,scope,scope_id,turn_id,payload_json FROM runtime_events ORDER BY id"
    )
    for row_id, scope, scope_id, turn_id, payload_text in events:
        event_count += 1
        expected, anomalies = source_links(row_id, scope, scope_id, turn_id, payload_text)
        expected_count += len(expected)
        if anomalies:
            malformed_count += int("malformed_payload_json" in anomalies)
            structured_count += sum(item != "malformed_payload_json" for item in anomalies)
            if len(source_samples) < SAMPLE_LIMIT:
                source_samples.append({"event_row_id": row_id, "reasons": anomalies})
        for key in sorted(expected):
            if connection.execute(membership_sql, key).fetchone() is None:
                missing_count += 1
                if len(missing_samples) < SAMPLE_LIMIT:
                    missing_samples.append(dict(zip(("scope", "id", "event_row_id"), key)))

    # One streaming indexed join avoids a second event-by-event lookup while
    # detecting forged or orphaned projection rows, not just missing links.
    projection = connection.execute(
        "SELECT s.subject_scope,s.subject_id,s.event_row_id, "
        "e.scope,e.scope_id,e.turn_id,e.payload_json "
        "FROM runtime_event_subjects s "
        "LEFT JOIN runtime_events e ON e.id=s.event_row_id "
        "ORDER BY s.subject_scope,s.subject_id,s.event_row_id"
    )
    for kind, subject_id, row_id, scope, scope_id, turn_id, payload_text in projection:
        projected_count += 1
        link = (kind, subject_id, row_id)
        if payload_text is None or link not in source_links(
            row_id, scope, scope_id, turn_id, payload_text
        )[0]:
            unexpected_count += 1
            if len(unexpected_samples) < SAMPLE_LIMIT:
                unexpected_samples.append({"scope": kind, "id": subject_id, "event_row_id": row_id})

    connection.execute("COMMIT")
    consistent = not (missing_count or unexpected_count or malformed_count or structured_count)
    return {
        "kind": "gooselake_retained_event_subject_audit",
        "inspection_only": True,
        "repair_authorized": False,
        "source_mode": "immutable_read_only",
        "schema_version": schema_version,
        "counts": {
            "canonical_events": event_count,
            "expected_subject_links": expected_count,
            "stored_subject_links": projected_count,
            "missing_subject_links": missing_count,
            "unexpected_subject_links": unexpected_count,
            "malformed_json_sources": malformed_count,
            "nonstring_or_ambiguous_subject_sources": structured_count,
        },
        "examples": {
            "missing": missing_samples,
            "unexpected": unexpected_samples,
            "source_anomalies": source_samples,
        },
        "consistent": consistent,
        "note": (
            "An empty discrepancy report is source/index parity for this offline "
            "snapshot only, not proof of event-family completeness, native effect "
            "safety, future trigger SQL integrity, or permission to repair/prune."
        ),
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--snapshot", required=True, type=Path,
        help="consistently checkpointed isolated offline SQLite file, never a live database",
    )
    args = parser.parse_args()
    try:
        isolated_snapshot(args.snapshot)
        with closing(sqlite3.connect(
            args.snapshot.absolute().as_uri() + "?mode=ro&immutable=1", uri=True
        )) as connection:
            connection.execute("PRAGMA query_only=ON")
            if connection.execute("PRAGMA query_only").fetchone()[0] != 1:
                raise ValueError("SQLite did not enter query-only mode")
            version = verify_source_schema(connection)
            report = inspect(connection, version)
    except (OSError, ValueError, sqlite3.Error) as error:
        print(f"event subject audit refused: {error}", file=sys.stderr)
        return 2
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0 if report["consistent"] else 1


if __name__ == "__main__":
    sys.exit(main())
