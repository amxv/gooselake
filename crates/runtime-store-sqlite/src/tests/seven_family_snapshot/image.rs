//! Exact logical images for the owned SQLite backup fixture, not SQL exports.
use rusqlite::{
    backup::{Backup, StepResult},
    types::ValueRef,
    Connection, OpenFlags,
};
use serde::Serialize;
use std::{
    collections::BTreeMap,
    fmt::Write as _,
    fs,
    path::{Path, PathBuf},
};

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize)]
pub(super) enum Cell {
    Null,
    Integer(i64),
    RealBits(u64),
    Text(Vec<u8>),
    Blob(Vec<u8>),
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub(super) struct Rows {
    pub columns: Vec<String>,
    pub rows: Vec<Vec<Cell>>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub(super) struct Image {
    pub schema: Rows,
    pub tables: BTreeMap<String, Rows>,
    pub event_heads: Rows,
    pub user_version: i64,
    pub application_id: i64,
}

fn query_rows(db: &Connection, sql: &str) -> Rows {
    let mut statement = db.prepare(sql).unwrap();
    let columns = statement
        .column_names()
        .into_iter()
        .map(str::to_owned)
        .collect::<Vec<_>>();
    let mut cursor = statement.query([]).unwrap();
    let mut rows = Vec::new();
    while let Some(row) = cursor.next().unwrap() {
        let values = (0..columns.len())
            .map(|i| match row.get_ref(i).unwrap() {
                ValueRef::Null => Cell::Null,
                ValueRef::Integer(v) => Cell::Integer(v),
                ValueRef::Real(v) => Cell::RealBits(v.to_bits()),
                ValueRef::Text(v) => Cell::Text(v.to_vec()),
                ValueRef::Blob(v) => Cell::Blob(v.to_vec()),
            })
            .collect();
        rows.push(values);
    }
    rows.sort();
    Rows { columns, rows }
}

pub(super) fn image_in_read_transaction(db: &Connection) -> Image {
    let schema = query_rows(
        db,
        "SELECT rowid,type,name,tbl_name,rootpage,sql FROM sqlite_master",
    );
    let names = db
        .prepare("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name COLLATE BINARY")
        .unwrap()
        .query_map([], |r| r.get::<_, String>(0))
        .unwrap()
        .collect::<rusqlite::Result<Vec<_>>>()
        .unwrap();
    let mut tables = BTreeMap::new();
    for name in names {
        let without_rowid: bool = db
            .query_row(
                "SELECT wr FROM pragma_table_list WHERE schema='main' AND name=?1",
                [&name],
                |r| r.get(0),
            )
            .unwrap();
        let quoted = format!("\"{}\"", name.replace('"', "\"\""));
        let projection = if without_rowid {
            format!("SELECT * FROM {quoted}")
        } else {
            let declared = query_rows(db, &format!("SELECT * FROM {quoted} LIMIT 0"));
            assert!(
                declared.columns.iter().all(|n| !matches!(
                    n.to_ascii_lowercase().as_str(),
                    "rowid" | "_rowid_" | "oid"
                )),
                "explicit rowid strategy required for {name}"
            );
            format!("SELECT _rowid_ AS __fixture_rowid__,* FROM {quoted}")
        };
        tables.insert(name, query_rows(db, &projection));
    }
    Image {schema,tables,event_heads:query_rows(db,"SELECT scope,scope_id,COUNT(*),MIN(seq),MAX(seq) FROM runtime_events GROUP BY scope COLLATE BINARY,scope_id COLLATE BINARY"),
        user_version:db.query_row("PRAGMA user_version",[],|r|r.get(0)).unwrap(),
        application_id:db.query_row("PRAGMA application_id",[],|r|r.get(0)).unwrap()}
}

pub(super) fn readonly(path: &Path) -> Connection {
    Connection::open_with_flags(
        path,
        OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
    )
    .unwrap()
}
pub(super) fn image(path: &Path) -> Image {
    let db = readonly(path);
    db.execute_batch("BEGIN DEFERRED").unwrap();
    let result = image_in_read_transaction(&db);
    db.execute_batch("ROLLBACK").unwrap();
    result
}
pub(super) fn sha256(bytes: &[u8]) -> String {
    let digest = ring::digest::digest(&ring::digest::SHA256, bytes);
    let mut result = String::with_capacity(64);
    for byte in digest.as_ref() {
        write!(&mut result, "{byte:02x}").unwrap();
    }
    result
}
pub(super) fn image_hash(value: &Image) -> String {
    sha256(&serde_json::to_vec(value).unwrap())
}

pub(super) fn assert_no_sidecars(path: &Path) {
    for suffix in ["-wal", "-shm", "-journal"] {
        let mut sidecar = path.as_os_str().to_os_string();
        sidecar.push(suffix);
        match fs::symlink_metadata(PathBuf::from(sidecar)) {
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            other => panic!("sealed fixture sidecar: {other:?}"),
        }
    }
}
pub(super) fn seal_owned_database(path: &Path) {
    let db = Connection::open(path).unwrap();
    let checkpoint: (i64, i64, i64) = db
        .query_row("PRAGMA wal_checkpoint(TRUNCATE)", [], |r| {
            Ok((r.get(0)?, r.get(1)?, r.get(2)?))
        })
        .unwrap();
    assert_eq!(checkpoint.0, 0, "owned target checkpoint busy");
    let mode: String = db
        .query_row("PRAGMA journal_mode=DELETE", [], |r| r.get(0))
        .unwrap();
    assert_eq!(mode.to_ascii_lowercase(), "delete");
    drop(db);
    assert_no_sidecars(path);
}
pub(super) fn backup_once(source: &Connection, destination: &Path) {
    drop(
        fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(destination)
            .unwrap(),
    );
    let mut target = Connection::open(destination).unwrap();
    {
        let backup = Backup::new(source, &mut target).unwrap();
        assert!(matches!(backup.step(-1).unwrap(), StepResult::Done));
        assert_eq!(backup.progress().remaining, 0);
    }
    drop(target);
    seal_owned_database(destination);
}

pub(super) fn no_write<T>(
    repository: &crate::SqliteRuntimeRepository,
    action: impl FnOnce() -> T,
) -> T {
    let before = image(&repository.database_path);
    let result = action();
    assert_eq!(image(&repository.database_path), before);
    result
}
