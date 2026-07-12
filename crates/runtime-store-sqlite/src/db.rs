use std::io::{ErrorKind, Write};
use std::path::{Path, PathBuf};
use std::time::Duration;

use rand::{rngs::OsRng, RngCore};
use runtime_core::{RuntimeError, RuntimeEventCriticality, RuntimeEventRecord, RuntimeEventScope};
use rusqlite::{params, Connection, OptionalExtension};
use serde_json::Value;

use crate::schema::{SCHEMA_SQL, SCHEMA_VERSION};

pub(crate) fn open_connection(path: &Path) -> Result<Connection, RuntimeError> {
    let connection = Connection::open(path).map_err(|error| {
        db_error(
            format!("failed to open sqlite database {}", path.display()),
            error,
        )
    })?;
    connection
        .busy_timeout(Duration::from_secs(5))
        .map_err(|error| db_error("failed to set sqlite busy timeout", error))?;
    connection
        .execute_batch(
            "PRAGMA foreign_keys = ON;
             PRAGMA journal_mode = WAL;
             PRAGMA synchronous = NORMAL;",
        )
        .map_err(|error| db_error("failed to configure sqlite pragmas", error))?;
    Ok(connection)
}

pub(crate) fn apply_schema(connection: &mut Connection) -> Result<(), RuntimeError> {
    let transaction = connection
        .transaction()
        .map_err(|error| db_error("failed to start sqlite schema transaction", error))?;

    transaction
        .execute_batch(SCHEMA_SQL)
        .map_err(|error| db_error("failed applying sqlite schema", error))?;

    let existing: Option<i64> = transaction
        .query_row(
            "SELECT version FROM schema_migrations WHERE version = ?1",
            params![SCHEMA_VERSION],
            |row| row.get(0),
        )
        .optional()
        .map_err(|error| db_error("failed reading schema_migrations", error))?;

    if existing.is_none() {
        transaction
            .execute(
                "INSERT INTO schema_migrations (version, applied_at)
                 VALUES (?1, strftime('%s','now'))",
                params![SCHEMA_VERSION],
            )
            .map_err(|error| db_error("failed writing schema migration row", error))?;
    }

    transaction
        .commit()
        .map_err(|error| db_error("failed committing schema transaction", error))?;
    Ok(())
}

pub(crate) fn initialize_source_identity(
    connection: &mut Connection,
    database_path: &Path,
    authority_root: Option<&Path>,
) -> Result<(), RuntimeError> {
    ensure_identity_platform()?;
    let generation_path = database_path.with_extension("source-generation");
    let generation_marker = match read_identity_file(&generation_path)? {
        Some(value) if valid_identifier(&value, "gen") => value,
        Some(_) => {
            return Err(RuntimeError::Bootstrap(format!(
                "invalid source generation marker {}",
                generation_path.display()
            )))
        }
        None => {
            let marker = random_identifier("gen");
            write_identity_file(&generation_path, &marker)?;
            marker
        }
    };
    let database_fingerprint = database_fingerprint(database_path)?;
    let authority_path = source_authority_path(database_path, authority_root)?;
    let authority = read_identity_file(&authority_path)?
        .map(|value| SourceAuthority::parse(&value, &authority_path))
        .transpose()?;

    let transaction = connection
        .transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)
        .map_err(|error| db_error("failed to start source identity transaction", error))?;
    let stored: Option<(String, String, String)> = transaction
        .query_row(
            "SELECT source_epoch, generation_marker, database_fingerprint FROM source_metadata WHERE singleton = 1",
            [],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .optional()
        .map_err(|error| db_error("failed reading source identity", error))?;

    let database_watermark = transaction
        .query_row(
            "SELECT COALESCE(MAX(id), 0) FROM runtime_events",
            [],
            |row| row.get::<_, i64>(0),
        )
        .map_err(|error| db_error("failed reading identity watermark", error))?;
    let retain = matches!((&stored, &authority),
        (Some((epoch, stored_marker, stored_fingerprint)), Some(authority))
            if epoch == &authority.source_epoch
                && stored_marker == &generation_marker
                && stored_marker == &authority.generation_marker
                && stored_fingerprint == &database_fingerprint
                && database_watermark >= authority.high_watermark);
    let source_epoch = if retain {
        stored.as_ref().expect("retained identity").0.clone()
    } else {
        random_identifier("src")
    };

    match stored {
        Some(_) if !retain => {
            transaction
                .execute(
                    "UPDATE source_metadata SET source_epoch = ?1, generation_marker = ?2, database_fingerprint = ?3 WHERE singleton = 1",
                    params![source_epoch, generation_marker, database_fingerprint],
                )
                .map_err(|error| db_error("failed rotating source identity", error))?;
        }
        Some(_) => {}
        None => {
            transaction
                .execute(
                    "INSERT INTO source_metadata (singleton, source_epoch, generation_marker, database_fingerprint) VALUES (1, ?1, ?2, ?3)",
                    params![source_epoch, generation_marker, database_fingerprint],
                )
                .map_err(|error| db_error("failed initializing source identity", error))?;
        }
    }
    transaction
        .commit()
        .map_err(|error| db_error("failed committing source identity", error))?;
    write_source_authority(
        database_path,
        authority_root,
        &SourceAuthority {
            source_epoch,
            generation_marker,
            database_fingerprint,
            high_watermark: database_watermark,
        },
    )
}

#[derive(Debug)]
struct SourceAuthority {
    source_epoch: String,
    generation_marker: String,
    database_fingerprint: String,
    high_watermark: i64,
}

impl SourceAuthority {
    fn parse(value: &str, path: &Path) -> Result<Self, RuntimeError> {
        let fields = value.split('|').collect::<Vec<_>>();
        if fields.len() != 5
            || fields[0] != "v1"
            || !valid_identifier(fields[1], "src")
            || !valid_identifier(fields[2], "gen")
        {
            return Err(RuntimeError::Bootstrap(format!(
                "invalid source authority checkpoint {}",
                path.display()
            )));
        }
        let high_watermark = fields[4].parse::<i64>().map_err(|_| {
            RuntimeError::Bootstrap(format!(
                "invalid source authority watermark {}",
                path.display()
            ))
        })?;
        Ok(Self {
            source_epoch: fields[1].to_string(),
            generation_marker: fields[2].to_string(),
            database_fingerprint: fields[3].to_string(),
            high_watermark,
        })
    }

    fn encode(&self) -> String {
        format!(
            "v1|{}|{}|{}|{}",
            self.source_epoch,
            self.generation_marker,
            self.database_fingerprint,
            self.high_watermark
        )
    }
}

pub(crate) fn checkpoint_source_identity_at(
    connection: &Connection,
    database_path: &Path,
    authority_root: Option<&Path>,
    high_watermark: i64,
) -> Result<(), RuntimeError> {
    static CHECKPOINT_LOCK: std::sync::OnceLock<std::sync::Mutex<()>> = std::sync::OnceLock::new();
    let _guard = CHECKPOINT_LOCK
        .get_or_init(|| std::sync::Mutex::new(()))
        .lock()
        .map_err(|_| RuntimeError::Bootstrap("source authority checkpoint lock poisoned".into()))?;
    let mut authority = connection
        .query_row(
            "SELECT source_epoch, generation_marker, database_fingerprint
             FROM source_metadata WHERE singleton = 1",
            [],
            |row| {
                Ok(SourceAuthority {
                    source_epoch: row.get(0)?,
                    generation_marker: row.get(1)?,
                    database_fingerprint: row.get(2)?,
                    high_watermark,
                })
            },
        )
        .map_err(|error| db_error("failed reading source authority checkpoint", error))?;
    let authority_path = source_authority_path(database_path, authority_root)?;
    if let Some(existing) = read_identity_file(&authority_path)?
        .map(|value| SourceAuthority::parse(&value, &authority_path))
        .transpose()?
    {
        if existing.source_epoch == authority.source_epoch
            && existing.generation_marker == authority.generation_marker
            && existing.database_fingerprint == authority.database_fingerprint
            && existing.high_watermark > authority.high_watermark
        {
            authority.high_watermark = existing.high_watermark;
        }
    }
    #[cfg(test)]
    run_before_authority_write_hook(database_path)?;
    write_source_authority(database_path, authority_root, &authority)
}

#[cfg(test)]
type AuthorityWriteHook = Box<dyn FnOnce() -> Result<(), RuntimeError> + Send + 'static>;

#[cfg(test)]
static BEFORE_AUTHORITY_WRITE_HOOK: std::sync::Mutex<Option<(PathBuf, AuthorityWriteHook)>> =
    std::sync::Mutex::new(None);

#[cfg(test)]
pub(crate) fn install_before_authority_write_hook(path: PathBuf, hook: AuthorityWriteHook) {
    *BEFORE_AUTHORITY_WRITE_HOOK
        .lock()
        .expect("authority hook mutex poisoned") = Some((path, hook));
}

#[cfg(test)]
fn run_before_authority_write_hook(database_path: &Path) -> Result<(), RuntimeError> {
    let hook = {
        let mut slot = BEFORE_AUTHORITY_WRITE_HOOK
            .lock()
            .expect("authority hook mutex poisoned");
        if slot.as_ref().is_some_and(|(path, _)| path == database_path) {
            slot.take().map(|(_, hook)| hook)
        } else {
            None
        }
    };
    if let Some(hook) = hook {
        hook()?;
    }
    Ok(())
}

fn write_source_authority(
    database_path: &Path,
    authority_root: Option<&Path>,
    authority: &SourceAuthority,
) -> Result<(), RuntimeError> {
    write_identity_file(
        &source_authority_path(database_path, authority_root)?,
        &authority.encode(),
    )
}

fn source_authority_path(
    database_path: &Path,
    authority_root: Option<&Path>,
) -> Result<PathBuf, RuntimeError> {
    let canonical = database_path.canonicalize().map_err(|error| {
        RuntimeError::Bootstrap(format!(
            "failed canonicalizing source database {}: {error}",
            database_path.display()
        ))
    })?;
    let mut hash = 0xcbf29ce484222325_u64;
    for byte in canonical.to_string_lossy().as_bytes() {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x100000001b3);
    }
    let root = authority_root
        .map(Path::to_path_buf)
        .or_else(|| {
            std::env::var_os("GG_RUNTIME_SOURCE_AUTHORITY_DIR")
                .map(PathBuf::from)
                .or_else(|| {
                    std::env::var_os("HOME")
                        .map(|home| PathBuf::from(home).join(".gg/runtime-source-authority"))
                })
        })
        .ok_or_else(|| {
            RuntimeError::Bootstrap(
                "source authority directory is unavailable; set GG_RUNTIME_SOURCE_AUTHORITY_DIR"
                    .into(),
            )
        })?;
    std::fs::create_dir_all(&root).map_err(|error| {
        RuntimeError::Bootstrap(format!(
            "failed creating source authority directory {}: {error}",
            root.display()
        ))
    })?;
    Ok(root.join(format!("{hash:016x}.checkpoint")))
}

#[cfg(not(windows))]
fn ensure_identity_platform() -> Result<(), RuntimeError> {
    Ok(())
}

#[cfg(windows)]
fn ensure_identity_platform() -> Result<(), RuntimeError> {
    Err(RuntimeError::Unsupported(
        "durable source identity is unavailable on Windows until atomic replace-existing semantics are implemented".into(),
    ))
}

fn read_identity_file(path: &Path) -> Result<Option<String>, RuntimeError> {
    match std::fs::read_to_string(path) {
        Ok(value) => Ok(Some(value.trim().to_string())),
        Err(error) if error.kind() == ErrorKind::NotFound => Ok(None),
        Err(error) => Err(RuntimeError::Bootstrap(format!(
            "failed reading source identity file {}: {error}",
            path.display()
        ))),
    }
}

fn write_identity_file(path: &Path, value: &str) -> Result<(), RuntimeError> {
    let parent = path.parent().ok_or_else(|| {
        RuntimeError::Bootstrap(format!(
            "source identity path has no parent: {}",
            path.display()
        ))
    })?;
    std::fs::create_dir_all(parent).map_err(|error| {
        RuntimeError::Bootstrap(format!(
            "failed creating source identity directory {}: {error}",
            parent.display()
        ))
    })?;
    let temp = parent.join(format!(".source-identity-{}.tmp", random_identifier("tmp")));
    let mut options = std::fs::OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let mut file = options.open(&temp).map_err(|error| {
        RuntimeError::Bootstrap(format!(
            "failed creating source identity temp file {}: {error}",
            temp.display()
        ))
    })?;
    file.write_all(format!("{value}\n").as_bytes())
        .and_then(|_| file.sync_all())
        .map_err(|error| {
            RuntimeError::Bootstrap(format!(
                "failed syncing source identity temp file {}: {error}",
                temp.display()
            ))
        })?;
    std::fs::rename(&temp, path).map_err(|error| {
        RuntimeError::Bootstrap(format!(
            "failed atomically replacing source identity file {}: {error}",
            path.display()
        ))
    })?;
    #[cfg(unix)]
    std::fs::File::open(parent)
        .and_then(|dir| dir.sync_all())
        .map_err(|error| {
            RuntimeError::Bootstrap(format!(
                "failed syncing source identity directory {}: {error}",
                parent.display()
            ))
        })?;
    Ok(())
}

fn valid_identifier(value: &str, prefix: &str) -> bool {
    value.len() == prefix.len() + 1 + 32
        && value.starts_with(prefix)
        && value.as_bytes().get(prefix.len()) == Some(&b'_')
        && value[prefix.len() + 1..]
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit())
}

#[cfg(unix)]
fn database_fingerprint(path: &Path) -> Result<String, RuntimeError> {
    use std::os::unix::fs::MetadataExt;

    let metadata = std::fs::metadata(path).map_err(|error| {
        RuntimeError::Bootstrap(format!(
            "failed reading database identity {}: {error}",
            path.display()
        ))
    })?;
    Ok(format!("unix:{}:{}", metadata.dev(), metadata.ino()))
}

#[cfg(not(unix))]
fn database_fingerprint(path: &Path) -> Result<String, RuntimeError> {
    path.canonicalize()
        .map(|path| format!("path:{}", path.display()))
        .map_err(|error| {
            RuntimeError::Bootstrap(format!(
                "failed canonicalizing database identity {}: {error}",
                path.display()
            ))
        })
}

pub(crate) fn random_identifier(prefix: &str) -> String {
    let mut bytes = [0_u8; 16];
    OsRng.fill_bytes(&mut bytes);
    let hex = bytes
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    format!("{prefix}_{hex}")
}

pub(crate) fn fetch_runtime_event_by_event_id(
    connection: &Connection,
    event_id: &str,
) -> Result<Option<RuntimeEventRecord>, RuntimeError> {
    connection
        .query_row(
            "SELECT id, event_id, scope, scope_id, session_id, team_id, turn_id,
                    seq, kind, critical, payload_json, provider, provider_seq, created_at
             FROM runtime_events
             WHERE event_id = ?1",
            params![event_id],
            runtime_event_from_row,
        )
        .optional()
        .map_err(|error| db_error("failed querying runtime event by event_id", error))
}

pub(crate) fn runtime_event_from_row(
    row: &rusqlite::Row<'_>,
) -> rusqlite::Result<RuntimeEventRecord> {
    let scope_text: String = row.get(2)?;
    let critical_value: i64 = row.get(9)?;

    let scope = RuntimeEventScope::from_str(&scope_text).ok_or_else(|| {
        rusqlite::Error::FromSqlConversionFailure(
            2,
            rusqlite::types::Type::Text,
            Box::new(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("invalid runtime event scope '{scope_text}'"),
            )),
        )
    })?;

    let criticality = RuntimeEventCriticality::from_i64(critical_value).ok_or_else(|| {
        rusqlite::Error::FromSqlConversionFailure(
            9,
            rusqlite::types::Type::Integer,
            Box::new(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("invalid runtime event criticality value '{critical_value}'"),
            )),
        )
    })?;

    let payload_json: String = row.get(10)?;
    let payload = serde_json::from_str::<Value>(&payload_json).map_err(|error| {
        rusqlite::Error::FromSqlConversionFailure(10, rusqlite::types::Type::Text, Box::new(error))
    })?;

    Ok(RuntimeEventRecord {
        row_id: row.get(0)?,
        event_id: row.get(1)?,
        scope,
        scope_id: row.get(3)?,
        session_id: row.get(4)?,
        team_id: row.get(5)?,
        turn_id: row.get(6)?,
        seq: row.get(7)?,
        kind: row.get(8)?,
        criticality,
        payload,
        provider: row.get(11)?,
        provider_seq: row.get(12)?,
        created_at: row.get(13)?,
    })
}

pub(crate) fn collect_rows<T>(
    rows: rusqlite::MappedRows<'_, impl FnMut(&rusqlite::Row<'_>) -> rusqlite::Result<T>>,
) -> Result<Vec<T>, RuntimeError> {
    rows.collect::<Result<Vec<_>, _>>()
        .map_err(|error| db_error("failed collecting sqlite rows", error))
}

pub(crate) fn json_to_string(value: &Value) -> Result<String, RuntimeError> {
    serde_json::to_string(value)
        .map_err(|error| RuntimeError::Bootstrap(format!("failed serializing JSON value: {error}")))
}

pub(crate) fn opt_json_to_string(value: Option<&Value>) -> Result<Option<String>, RuntimeError> {
    match value {
        Some(value) => Ok(Some(json_to_string(value)?)),
        None => Ok(None),
    }
}

pub(crate) fn string_to_json(value: String) -> rusqlite::Result<Value> {
    serde_json::from_str::<Value>(&value).map_err(|error| {
        rusqlite::Error::FromSqlConversionFailure(0, rusqlite::types::Type::Text, Box::new(error))
    })
}

pub(crate) fn opt_string_to_json(value: Option<String>) -> rusqlite::Result<Option<Value>> {
    match value {
        Some(raw) => Ok(Some(string_to_json(raw)?)),
        None => Ok(None),
    }
}

pub(crate) fn db_error(context: impl AsRef<str>, error: rusqlite::Error) -> RuntimeError {
    RuntimeError::Bootstrap(format!("{}: {error}", context.as_ref()))
}
