"""Read-only inspection of explicitly sealed SQLite copies.

No backup, checkpoint, initialization, repair, epoch rotation, restore,
activation or file publication is implemented here.
"""
from __future__ import annotations
from contextlib import contextmanager
import hashlib
import os
from pathlib import Path
import re
import sqlite3
import stat

ROOT = Path(__file__).resolve().parents[1]
STORE = ROOT / "crates/runtime-store-sqlite/src"
SIDECARS = ("-wal", "-shm", "-journal")
CHUNK_BYTES = 1024 * 1024
EPOCH_VERSION = 53
APPROVAL_TERMINAL = (
    "accept", "decline", "approved", "accepted", "denied", "rejected",
    "cancelled", "expired", "failed", "resolved",
)

def absolute_lexical_path(value: Path) -> Path:
    text = os.fspath(value)
    if "\x00" in text or text.startswith("//"):
        raise ValueError("input has an ambiguous or invalid path")
    path = Path(text)
    if ".." in path.parts:
        raise ValueError("input path must not contain '..' components")
    return path if path.is_absolute() else Path(os.getcwd()) / path

def ancestor_identity(path: Path) -> tuple:
    result = []
    for parent in reversed(path.parents):
        found = os.lstat(parent)
        if stat.S_ISLNK(found.st_mode) or not stat.S_ISDIR(found.st_mode):
            raise ValueError("input has a symlink or non-directory ancestor")
        result.append((os.fspath(parent), found.st_dev, found.st_ino, found.st_mode))
    return tuple(result)

def physical_stamp(found: os.stat_result) -> tuple[int, ...]:
    # Reading may change atime, which is intentionally excluded.
    return (found.st_dev, found.st_ino, found.st_mode, found.st_nlink,
            found.st_size, found.st_mtime_ns, found.st_ctime_ns)

class SealedInput:
    """Detect observed path/content changes around a complete inspection.

    This is not a lock and cannot make a concurrently modified database safe
    for immutable mode. A separately acquired offline copy is a prerequisite.
    """
    def __init__(self, path: Path, *, sqlite_sidecars: bool, max_bytes: int | None = None):
        self.path = absolute_lexical_path(path)
        self.sqlite_sidecars = sqlite_sidecars
        self.max_bytes = max_bytes
        self.fd = None
        self.size = 0
        self.sha256 = ""
        self._stamp = None
        self._ancestors = None

    def _reject_sidecars(self) -> None:
        if self.sqlite_sidecars:
            for suffix in SIDECARS:
                try:
                    os.lstat(self.path.with_name(self.path.name + suffix))
                except FileNotFoundError:
                    continue
                raise ValueError("snapshot has a SQLite sidecar entry; provide a consistently checkpointed, sealed offline copy")

    def _chunks(self):
        assert self.fd is not None
        offset = 0
        while offset < self.size:
            chunk = os.pread(self.fd, min(CHUNK_BYTES, self.size - offset), offset)
            if not chunk:
                raise ValueError("input shortened during inspection")
            offset += len(chunk)
            yield chunk
        if os.pread(self.fd, 1, self.size):
            raise ValueError("input grew during inspection")

    def _digest(self) -> str:
        assert self.fd is not None
        before = physical_stamp(os.fstat(self.fd))
        digest = hashlib.sha256()
        for chunk in self._chunks():
            digest.update(chunk)
        if before != self._stamp or physical_stamp(os.fstat(self.fd)) != self._stamp:
            raise ValueError("input metadata changed while hashing")
        return digest.hexdigest()

    def check(self, *, rehash: bool) -> None:
        assert self.fd is not None
        if ancestor_identity(self.path) != self._ancestors:
            raise ValueError("input ancestor identity changed")
        self._reject_sidecars()
        current = os.lstat(self.path)
        if not stat.S_ISREG(current.st_mode):
            raise ValueError("input is no longer a regular non-symlink file")
        if physical_stamp(current) != self._stamp:
            raise ValueError("input path identity or metadata changed")
        if physical_stamp(os.fstat(self.fd)) != self._stamp:
            raise ValueError("opened input identity or metadata changed")
        if rehash and self._digest() != self.sha256:
            raise ValueError("input bytes changed during inspection")

    def __enter__(self):
        try:
            self._ancestors = ancestor_identity(self.path)
            self._reject_sidecars()
            found = os.lstat(self.path)
            if not stat.S_ISREG(found.st_mode):
                raise ValueError("input must be a regular non-symlink file")
            if found.st_nlink != 1:
                raise ValueError("sealed input must not have hard-link aliases")
            if self.max_bytes is not None and found.st_size > self.max_bytes:
                raise ValueError("input exceeds this format's byte limit")
            self.size = found.st_size
            self._stamp = physical_stamp(found)
            self.fd = os.open(self.path, os.O_RDONLY | os.O_CLOEXEC | os.O_NOFOLLOW)
            if physical_stamp(os.fstat(self.fd)) != self._stamp:
                raise ValueError("input changed while opening")
            self.sha256 = self._digest()
            self.check(rehash=False)
            return self
        except BaseException:
            if self.fd is not None:
                os.close(self.fd)
                self.fd = None
            raise

    def read_small_file(self) -> bytes:
        if self.max_bytes is None:
            raise ValueError("whole-file materialization needs an explicit limit")
        return b"".join(self._chunks())

    def __exit__(self, exc_type, exc_value, traceback):
        try:
            self.check(rehash=True)
        finally:
            if self.fd is not None:
                os.close(self.fd)
                self.fd = None
        return False

def _read_authorizer(action, first, second, database, trigger):
    if action in (sqlite3.SQLITE_SELECT, sqlite3.SQLITE_READ):
        return sqlite3.SQLITE_OK
    if action == sqlite3.SQLITE_FUNCTION:
        allowed = {"count", "min", "max", "sum", "typeof", "length", "json_extract", "json_valid", "coalesce"}
        return sqlite3.SQLITE_OK if (second or first or "").lower() in allowed else sqlite3.SQLITE_DENY
    if action == sqlite3.SQLITE_PRAGMA:
        name = (first or "").lower()
        if name in {"table_info", "table_xinfo", "index_list", "index_info"}:
            return sqlite3.SQLITE_OK
        if name in {"query_only", "user_version", "application_id", "schema_version"} and second is None:
            return sqlite3.SQLITE_OK
    # Observers cannot close the owner's transaction, attach, write or run DDL.
    return sqlite3.SQLITE_DENY

@contextmanager
def sealed_database(path: Path):
    with SealedInput(path, sqlite_sidecars=True) as source:
        connection = None
        try:
            connection = sqlite3.connect(source.path.as_uri() + "?mode=ro&immutable=1", uri=True, isolation_level=None)
            connection.execute("PRAGMA query_only=ON")
            connection.execute("PRAGMA trusted_schema=OFF")
            if connection.execute("PRAGMA query_only").fetchone() != (1,):
                raise ValueError("SQLite did not enter query-only mode")
            source.check(rehash=False)
            connection.execute("BEGIN DEFERRED")
            connection.set_authorizer(_read_authorizer)
            yield connection, source
            if not connection.in_transaction:
                raise ValueError("observer escaped the owner's read transaction")
        finally:
            if connection is not None:
                connection.set_authorizer(None)
                try:
                    if connection.in_transaction:
                        connection.execute("ROLLBACK")
                finally:
                    connection.close()
        # SealedInput rechecks every byte and physical identity before report.

def columns(connection: sqlite3.Connection, table: str) -> frozenset[str]:
    quoted = '"' + table.replace('"', '""') + '"'
    found = connection.execute("SELECT type FROM sqlite_master WHERE name=?", (table,)).fetchone()
    if found is None:
        return frozenset()
    if found != ("table",):
        raise ValueError(f"expected a real table: {table}")
    return frozenset(row[1] for row in connection.execute(f"PRAGMA table_info({quoted})"))

def source_schema_version() -> int:
    source = (STORE / "schema.rs").read_text(encoding="utf-8")
    match = re.search(r"pub\(crate\) const SCHEMA_VERSION:\s*i64\s*=\s*(\d+);", source)
    if not match:
        raise ValueError("current source schema version is unavailable")
    version = int(match.group(1))
    chain = (STORE / "schema_chain.rs").read_text(encoding="utf-8")
    declared = [int(value) for value in re.findall(r"\bversion:\s*(\d+)\s*,", chain)]
    if declared != list(range(1, version + 1)):
        raise ValueError("source schema version and migration chain disagree")
    return version

def journal_epoch(connection: sqlite3.Connection) -> str:
    if not {"singleton", "epoch"} <= columns(connection, "runtime_event_journal_identity"):
        raise ValueError("required persisted journal identity is missing")
    migration = (STORE / "schema_migration_53.sql").read_text(encoding="utf-8")
    start = migration.index("CREATE TABLE runtime_event_journal_identity (")
    end = migration.index("\n);", start) + len("\n);")
    expected = migration[start:end].strip().rstrip(";")
    found = connection.execute("SELECT sql FROM sqlite_master WHERE type='table' AND name='runtime_event_journal_identity' AND tbl_name='runtime_event_journal_identity'").fetchone()
    if found is None or found[0].strip().rstrip(";") != expected:
        raise ValueError("journal identity lacks canonical schema53 constraints")
    rows = connection.execute("SELECT singleton,typeof(singleton),epoch,typeof(epoch) FROM runtime_event_journal_identity ORDER BY singleton LIMIT 2").fetchall()
    if (len(rows) != 1 or rows[0][0] != 1 or rows[0][1] != "integer" or rows[0][3] != "text"
            or not isinstance(rows[0][2], str) or re.fullmatch(r"[0-9a-f]{32}", rows[0][2]) is None):
        raise ValueError("journal identity requires exactly its valid singleton")
    return rows[0][2]

def schema_observation(connection: sqlite3.Connection, *, require_current: bool = False) -> dict:
    if not connection.in_transaction:
        raise ValueError("schema observation requires the owner's read transaction")
    expected = source_schema_version()
    known = columns(connection, "schema_migrations")
    versions = None
    if known:
        if "version" not in known:
            raise ValueError("migration table lacks its version column")
        typed = connection.execute("SELECT version,typeof(version) FROM schema_migrations ORDER BY version LIMIT ?", (expected + 1,)).fetchall()
        if len(typed) > expected:
            raise ValueError("excess applied migration markers")
        versions = []
        for value, storage_type in typed:
            if storage_type != "integer" or type(value) is not int or value <= 0 or value > expected:
                raise ValueError("malformed or unsupported future migration marker")
            versions.append(value)
        if len(set(versions)) != len(versions):
            raise ValueError("duplicate applied migration markers")
    version = max(versions) if versions else None
    complete = bool(versions) and versions == list(range(1, version + 1))
    if require_current and (not complete or version != expected):
        raise ValueError("manifest requires the complete current migration chain")
    epoch = journal_epoch(connection) if version is not None and version >= EPOCH_VERSION else None
    return {"source_version": expected, "snapshot_version": version, "applied_versions": versions,
            "complete_prefix": complete, "missing_applied_versions": sorted(set(range(1, expected + 1)) - set(versions)) if versions is not None else None,
            "journal_epoch": epoch}

# Each count belongs to its original carrier; overlapping carriers are not
# summed into an invented count of distinct operations.
FAMILY_CARRIERS = {
    "turn": (("turns", "status", ("completed", "failed", "interrupted", "cancelled")),
             ("turn_admissions", "dispatch_state", ("dispatched", "not_dispatched"))),
    "process_completion": (("managed_processes", "completion_state", ("not_required", "delivered")),),
    "approval": (("approvals", "status", APPROVAL_TERMINAL),),
    "hook": (("workspace_hook_effects", "phase", ("succeeded", "failed", "timed_out")),
             ("workspace_hook_effects", "application_state", ("not_ready", "applied", "rejected"))),
    "edit": (("session_edit_reruns", "phase", ("completed", "failed")),),
    "rebind": (("workspace_agent_rebinds", "phase", ("completed", "rejected")),),
    "retirement": (("workspace_retirement_fences", "phase", ("retired",)),),
}

def seven_family_observations(connection: sqlite3.Connection) -> dict:
    if not connection.in_transaction:
        raise ValueError("family observation requires the owner's read transaction")
    result = {}
    for family, carriers in FAMILY_CARRIERS.items():
        counts = {}
        for table, column, terminal in carriers:
            key = f"{table}.{column}"
            if column not in columns(connection, table):
                counts[key] = None
                continue
            placeholders = ",".join("?" for _ in terminal)
            counts[key] = connection.execute(
                f'SELECT COUNT(*) FROM "{table}" WHERE typeof("{column}")<>\'text\' OR "{column}" NOT IN ({placeholders})', terminal).fetchone()[0]
        result[family] = {"unresolved_rows_by_carrier": counts,
                          "all_carriers_observed": all(v is not None for v in counts.values()),
                          "has_observed_unresolved_rows": any(v is not None and v > 0 for v in counts.values())}
    return result
