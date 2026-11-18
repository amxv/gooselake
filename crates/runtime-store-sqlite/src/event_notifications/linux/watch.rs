//! Separate lossy write hints from the lifecycle evidence required to reject
//! database replacement. Lost write hints permit catch-up; lost identity
//! evidence cannot certify that a hidden rename-and-restore never happened.
use std::ffi::{CString, OsStr};
use std::io;
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd, RawFd};
use std::os::unix::ffi::OsStrExt;

use runtime_core::EventObservationFailure as Failure;

use super::unavailable;

#[derive(Debug)]
pub(super) struct Watch {
    lifecycle: OwnedFd,
    hints: OwnedFd,
    directory_watch: i32,
    inode_watch: Option<i32>,
    hint_watch: i32,
    database_name: Vec<u8>,
    wal_name: Vec<u8>,
}

#[derive(Default)]
pub(super) struct Scan {
    pub(super) dirty: bool,
    pub(super) overflows: u64,
}

fn inotify() -> Result<OwnedFd, Failure> {
    let fd = unsafe { libc::inotify_init1(libc::IN_CLOEXEC | libc::IN_NONBLOCK) };
    if fd < 0 {
        return Err(unavailable(
            "create event directory watcher",
            io::Error::last_os_error(),
        ));
    }
    Ok(unsafe { OwnedFd::from_raw_fd(fd) })
}

fn add_watch(fd: RawFd, object: RawFd, mask: u32) -> Result<i32, Failure> {
    // Follow this proc magic link to the pinned object. IN_DONT_FOLLOW would
    // watch the proc link instead and defeat the namespace binding.
    let path = CString::new(format!("/proc/self/fd/{object}")).expect("numeric fd path");
    let wd = unsafe { libc::inotify_add_watch(fd, path.as_ptr(), mask) };
    if wd < 0 {
        return Err(unavailable(
            "watch pinned event database object",
            io::Error::last_os_error(),
        ));
    }
    Ok(wd)
}

impl Watch {
    pub(super) fn new(parent: RawFd, name: &OsStr) -> Result<Self, Failure> {
        let lifecycle = inotify()?;
        let hints = inotify()?;
        let directory_watch = add_watch(
            lifecycle.as_raw_fd(),
            parent,
            libc::IN_ONLYDIR
                | libc::IN_CREATE
                | libc::IN_DELETE
                | libc::IN_MOVED_FROM
                | libc::IN_MOVED_TO
                | libc::IN_DELETE_SELF
                | libc::IN_MOVE_SELF,
        )?;
        let hint_watch = add_watch(
            hints.as_raw_fd(),
            parent,
            libc::IN_ONLYDIR | libc::IN_MODIFY,
        )?;
        let database_name = name.as_bytes().to_vec();
        let mut wal_name = database_name.clone();
        wal_name.extend_from_slice(b"-wal");
        Ok(Self {
            lifecycle,
            hints,
            directory_watch,
            inode_watch: None,
            hint_watch,
            database_name,
            wal_name,
        })
    }

    pub(super) fn watch_inode(&mut self, fd: RawFd) -> Result<(), Failure> {
        self.inode_watch = Some(add_watch(
            self.lifecycle.as_raw_fd(),
            fd,
            libc::IN_ATTRIB | libc::IN_DELETE_SELF | libc::IN_MOVE_SELF,
        )?);
        Ok(())
    }

    pub(super) fn fds(&self) -> [RawFd; 2] {
        [self.lifecycle.as_raw_fd(), self.hints.as_raw_fd()]
    }

    #[cfg(test)]
    pub(super) fn remove_lifecycle_watch(&self) {
        assert_eq!(
            unsafe { libc::inotify_rm_watch(self.lifecycle.as_raw_fd(), self.directory_watch) },
            0
        );
    }

    /// Caller holds the same guard as connection use. No worker may consume
    /// bytes into a private buffer and hide a lifecycle event from a reader.
    pub(super) fn scan(&self) -> Result<Scan, Failure> {
        let mut scan = Scan::default();
        self.drain(self.lifecycle.as_raw_fd(), true, &mut scan)?;
        self.drain(self.hints.as_raw_fd(), false, &mut scan)?;
        Ok(scan)
    }

    fn drain(&self, fd: RawFd, lifecycle: bool, scan: &mut Scan) -> Result<(), Failure> {
        let mut bytes = [0u8; 65536];
        // Bound time spent on a hostile/unbounded directory stream, not just
        // memory. Exhaustion fails closed; no unexamined page is released.
        for _ in 0..256 {
            let count = unsafe { libc::read(fd, bytes.as_mut_ptr().cast(), bytes.len()) };
            if count < 0 {
                let error = io::Error::last_os_error();
                if error.kind() == io::ErrorKind::WouldBlock {
                    return Ok(());
                }
                if error.kind() == io::ErrorKind::Interrupted {
                    continue;
                }
                return Err(unavailable("read event watcher", error));
            }
            if count == 0 {
                return Err(Failure::unavailable("event watcher unexpectedly closed"));
            }
            self.parse(&bytes[..count as usize], lifecycle, scan)?;
        }
        Err(Failure::unavailable(
            "event watcher backlog exceeds the bounded identity scan",
        ))
    }

    fn parse(&self, bytes: &[u8], lifecycle: bool, scan: &mut Scan) -> Result<(), Failure> {
        let mut offset = 0usize;
        let header = std::mem::size_of::<libc::inotify_event>();
        while offset < bytes.len() {
            if bytes.len() - offset < header {
                return Err(Failure::unavailable("truncated event watcher header"));
            }
            let event = unsafe {
                std::ptr::read_unaligned(bytes.as_ptr().add(offset).cast::<libc::inotify_event>())
            };
            let end = offset
                .checked_add(header)
                .and_then(|n| n.checked_add(event.len as usize))
                .filter(|n| *n <= bytes.len())
                .ok_or_else(|| Failure::unavailable("invalid event watcher record length"))?;
            let name = &bytes[offset + header..end];
            let name = &name[..name
                .iter()
                .position(|byte| *byte == 0)
                .unwrap_or(name.len())];
            offset = end;
            if event.mask & libc::IN_Q_OVERFLOW != 0 {
                if lifecycle {
                    return Err(Failure::generation_changed(
                        "database lifecycle evidence overflowed; generation continuity is unknown",
                    ));
                }
                scan.dirty = true;
                scan.overflows += 1;
                continue;
            }
            if event.mask
                & (libc::IN_UNMOUNT | libc::IN_IGNORED | libc::IN_DELETE_SELF | libc::IN_MOVE_SELF)
                != 0
            {
                return Err(Failure::generation_changed(
                    "database object or directory watch was lost or moved",
                ));
            }
            if lifecycle && Some(event.wd) == self.inode_watch {
                if event.mask & libc::IN_ATTRIB != 0 {
                    return Err(Failure::generation_changed(
                        "database inode attributes or hard-link topology changed",
                    ));
                }
                continue;
            }
            let expected = if lifecycle {
                self.directory_watch
            } else {
                self.hint_watch
            };
            if event.wd != expected {
                return Err(Failure::unavailable("event arrived for an unknown watcher"));
            }
            if lifecycle && name == self.database_name {
                return Err(Failure::generation_changed(
                    "event database pathname was created, removed or replaced",
                ));
            }
            if name == self.wal_name || (!lifecycle && name == self.database_name) {
                scan.dirty = true;
            }
        }
        Ok(())
    }
}

#[derive(Debug)]
pub(super) struct Wake(OwnedFd);

impl Wake {
    pub(super) fn new() -> Result<Self, Failure> {
        let fd = unsafe { libc::eventfd(0, libc::EFD_CLOEXEC | libc::EFD_NONBLOCK) };
        if fd < 0 {
            return Err(unavailable(
                "create observer wake descriptor",
                io::Error::last_os_error(),
            ));
        }
        Ok(Self(unsafe { OwnedFd::from_raw_fd(fd) }))
    }
    pub(super) fn fd(&self) -> RawFd {
        self.0.as_raw_fd()
    }

    pub(super) fn signal(&self) -> Result<(), Failure> {
        let value: u64 = 1;
        loop {
            let count = unsafe { libc::write(self.fd(), (&value as *const u64).cast(), 8) };
            if count == 8 {
                return Ok(());
            }
            if count >= 0 {
                return Err(Failure::unavailable("short observer wake write"));
            }
            let error = io::Error::last_os_error();
            if error.kind() == io::ErrorKind::WouldBlock {
                return Ok(());
            }
            if error.kind() != io::ErrorKind::Interrupted {
                return Err(unavailable("wake event observer", error));
            }
        }
    }

    pub(super) fn drain(&self) -> Result<(), Failure> {
        let mut value: u64 = 0;
        loop {
            let count = unsafe { libc::read(self.fd(), (&mut value as *mut u64).cast(), 8) };
            if count == 8 {
                continue;
            }
            if count >= 0 {
                return Err(Failure::unavailable("short observer wake read"));
            }
            let error = io::Error::last_os_error();
            if error.kind() == io::ErrorKind::WouldBlock {
                return Ok(());
            }
            if error.kind() != io::ErrorKind::Interrupted {
                return Err(unavailable("read observer wake descriptor", error));
            }
        }
    }
}
