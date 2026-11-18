use std::ffi::{CString, OsString};
use std::fs::{File, Metadata};
use std::os::fd::{AsRawFd, FromRawFd, RawFd};
use std::os::unix::ffi::OsStrExt;
use std::os::unix::fs::MetadataExt;
use std::path::{Path, PathBuf};
use std::time::Duration;

use runtime_core::EventObservationFailure as Failure;
use rusqlite::{Connection, DatabaseName, OpenFlags};

use super::unavailable;
#[cfg(test)]
use super::{Phase, TestControl};

fn identity(metadata: &Metadata) -> (u64, u64) {
    (metadata.dev(), metadata.ino())
}

#[derive(Debug)]
pub(super) struct DirectoryPin {
    pub(super) path: PathBuf,
    pub(super) name: OsString,
    parent_path: PathBuf,
    parent: File,
    parent_identity: (u64, u64),
}

impl DirectoryPin {
    pub(super) fn open(path: &Path) -> Result<Self, Failure> {
        let path = path
            .canonicalize()
            .map_err(|e| unavailable("resolve existing event database", e))?;
        let parent_path = path
            .parent()
            .ok_or_else(|| Failure::unavailable("database has no parent directory"))?
            .to_path_buf();
        let name = path
            .file_name()
            .ok_or_else(|| Failure::unavailable("database has no filename"))?
            .to_os_string();
        let name_c = CString::new(parent_path.as_os_str().as_bytes())
            .map_err(|e| unavailable("encode database directory", e))?;
        // O_PATH pins metadata without creating an ordinary database fd whose
        // close could release SQLite's process-owned POSIX record locks.
        let fd = unsafe {
            libc::open(
                name_c.as_ptr(),
                libc::O_PATH | libc::O_DIRECTORY | libc::O_NOFOLLOW | libc::O_CLOEXEC,
            )
        };
        if fd < 0 {
            return Err(unavailable(
                "pin database directory",
                std::io::Error::last_os_error(),
            ));
        }
        let parent = unsafe { File::from_raw_fd(fd) };
        let parent_identity = identity(
            &parent
                .metadata()
                .map_err(|e| unavailable("inspect pinned directory", e))?,
        );
        Ok(Self {
            path,
            name,
            parent_path,
            parent,
            parent_identity,
        })
    }

    pub(super) fn fd(&self) -> RawFd {
        self.parent.as_raw_fd()
    }

    pub(super) fn pin_database(self) -> Result<DatabasePin, Failure> {
        let name = CString::new(self.name.as_bytes())
            .map_err(|e| unavailable("encode database name", e))?;
        let fd = unsafe {
            libc::openat(
                self.fd(),
                name.as_ptr(),
                libc::O_PATH | libc::O_NOFOLLOW | libc::O_CLOEXEC,
            )
        };
        if fd < 0 {
            return Err(unavailable(
                "pin existing database",
                std::io::Error::last_os_error(),
            ));
        }
        let file = unsafe { File::from_raw_fd(fd) };
        let metadata = file
            .metadata()
            .map_err(|e| unavailable("inspect pinned database", e))?;
        if !metadata.is_file() || metadata.nlink() != 1 {
            return Err(Failure::generation_changed(
                "event observation requires a regular database with exactly one hard link",
            ));
        }
        Ok(DatabasePin {
            directory: self,
            file,
            identity: identity(&metadata),
        })
    }
}

#[derive(Debug)]
pub(super) struct DatabasePin {
    pub(super) directory: DirectoryPin,
    file: File,
    identity: (u64, u64),
}

impl DatabasePin {
    pub(super) fn identity(&self) -> (u64, u64) {
        self.identity
    }
    pub(super) fn fd(&self) -> RawFd {
        self.file.as_raw_fd()
    }

    pub(super) fn verify_path(&self) -> Result<(), Failure> {
        let file = self
            .file
            .metadata()
            .map_err(|e| unavailable("inspect database pin", e))?;
        let current = std::fs::symlink_metadata(&self.directory.path).map_err(|_| {
            Failure::generation_changed("event database path is missing or inaccessible")
        })?;
        let parent = std::fs::symlink_metadata(&self.directory.parent_path).map_err(|_| {
            Failure::generation_changed("event database directory is missing or inaccessible")
        })?;
        if !file.is_file()
            || file.nlink() != 1
            || !current.is_file()
            || identity(&file) != self.identity
            || identity(&current) != self.identity
            || current.nlink() != 1
            || !parent.is_dir()
            || identity(&parent) != self.directory.parent_identity
        {
            return Err(Failure::generation_changed(
                "event database or its directory changed identity",
            ));
        }
        Ok(())
    }

    pub(super) fn open_connection(
        &self,
        #[cfg(test)] control: &TestControl,
    ) -> Result<Connection, Failure> {
        self.verify_path()?;
        #[cfg(test)]
        control.checkpoint(Phase::BeforeSqliteOpen)?;
        let flags = OpenFlags::SQLITE_OPEN_READ_WRITE
            | OpenFlags::SQLITE_OPEN_NO_MUTEX
            | OpenFlags::SQLITE_OPEN_PRIVATE_CACHE
            | OpenFlags::SQLITE_OPEN_NOFOLLOW;
        let connection =
            Connection::open_with_flags_and_vfs(&self.directory.path, flags, "unix")
                .map_err(|e| unavailable("open existing event database with Unix locking", e))?;
        #[cfg(test)]
        control.checkpoint(Phase::AfterSqliteOpen)?;
        // Verify the actual opened handle before even querying journal mode.
        // Path metadata alone cannot identify a handle opened during ABA.
        self.verify(&connection)?;
        connection
            .busy_timeout(Duration::from_secs(5))
            .map_err(|e| unavailable("bound event observer lock wait", e))?;
        if connection
            .is_readonly(DatabaseName::Main)
            .map_err(|e| unavailable("inspect event observer write access", e))?
        {
            return Err(Failure::unavailable(
                "event observation requires writable SQLite lock access",
            ));
        }
        let mode: String = connection
            .query_row("PRAGMA journal_mode", [], |r| r.get(0))
            .map_err(|e| unavailable("inspect existing event journal mode", e))?;
        if !mode.eq_ignore_ascii_case("wal") {
            return Err(Failure::unavailable(
                "event observation requires an existing WAL database; its mode was not changed",
            ));
        }
        self.verify(&connection)?;
        Ok(connection)
    }

    pub(super) fn verify(&self, connection: &Connection) -> Result<(), Failure> {
        self.verify_path()?;
        let mut moved: libc::c_int = 0;
        // The connection is serialized by the observer. This supported file
        // control supplements the sticky lifecycle guard; it is not an ABA
        // proof alone, and no private sqlite3_file layout is inspected.
        let result = unsafe {
            rusqlite::ffi::sqlite3_file_control(
                connection.handle(),
                c"main".as_ptr(),
                rusqlite::ffi::SQLITE_FCNTL_HAS_MOVED,
                (&mut moved as *mut libc::c_int).cast(),
            )
        };
        if result != rusqlite::ffi::SQLITE_OK {
            return Err(Failure::unavailable(
                "SQLite cannot verify the opened event database identity",
            ));
        }
        if moved != 0 {
            return Err(Failure::generation_changed(
                "the opened event database moved or was replaced",
            ));
        }
        self.verify_path()
    }
}
