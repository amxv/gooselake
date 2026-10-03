use std::io;

#[cfg(target_os = "macos")]
fn macos_process_identity(pid: u32) -> io::Result<String> {
    let pid = i32::try_from(pid)
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "process id exceeds i32"))?;
    let mut info = std::mem::MaybeUninit::<libc::proc_bsdinfo>::zeroed();
    let expected = std::mem::size_of::<libc::proc_bsdinfo>();
    // SAFETY: `info` is a writable exact-size proc_bsdinfo buffer and `pid` fits the ABI.
    let read = unsafe {
        libc::proc_pidinfo(
            pid,
            libc::PROC_PIDTBSDINFO,
            0,
            info.as_mut_ptr().cast(),
            i32::try_from(expected).expect("proc_bsdinfo size fits i32"),
        )
    };
    if read == 0 {
        return Err(io::Error::new(
            io::ErrorKind::NotFound,
            format!("process {pid} no longer exists"),
        ));
    }
    if read != i32::try_from(expected).expect("proc_bsdinfo size fits i32") {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: the exact-size return proves the kernel initialized the complete value.
    let info = unsafe { info.assume_init() };
    Ok(format!(
        "macos:{}:{}:{}:{}",
        info.pbi_pid, info.pbi_pgid, info.pbi_start_tvsec, info.pbi_start_tvusec
    ))
}

#[cfg(target_os = "linux")]
fn linux_process_identity(pid: u32) -> io::Result<String> {
    let stat = std::fs::read_to_string(format!("/proc/{pid}/stat"))?;
    let fields = stat
        .rsplit_once(") ")
        .map(|(_, fields)| fields)
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "invalid /proc stat"))?
        .split_whitespace()
        .collect::<Vec<_>>();
    let process_group = fields
        .get(2)
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "missing process group"))?;
    let start_ticks = fields
        .get(19)
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "missing process start time"))?;
    Ok(format!("linux:{pid}:{process_group}:{start_ticks}"))
}

pub(crate) fn capture_process_identity(pid: u32) -> io::Result<String> {
    #[cfg(target_os = "macos")]
    {
        macos_process_identity(pid)
    }
    #[cfg(target_os = "linux")]
    {
        linux_process_identity(pid)
    }
    #[cfg(all(unix, not(any(target_os = "macos", target_os = "linux"))))]
    {
        let pid = i32::try_from(pid)
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "process id exceeds i32"))?;
        // SAFETY: `pid` was range checked and getpgid has no pointer arguments.
        let process_group = unsafe { libc::getpgid(pid) };
        if process_group < 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(format!("unix:{pid}:{process_group}"))
    }
    #[cfg(not(unix))]
    {
        Ok(format!("process:{pid}"))
    }
}

#[cfg(unix)]
fn identity_process_group(identity: &str) -> Option<u32> {
    identity.split(':').nth(2)?.parse().ok()
}

#[cfg(not(unix))]
fn identity_process_group(_identity: &str) -> Option<u32> {
    None
}

pub(crate) fn capture_managed_process_identity(pid: u32) -> io::Result<String> {
    let identity = capture_process_identity(pid)?;
    #[cfg(unix)]
    if identity_process_group(&identity) != Some(pid) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("process {pid} does not own its process group"),
        ));
    }
    Ok(identity)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ProcessTerminationOutcome {
    Terminated,
    Absent,
    IdentityMismatch,
}

#[cfg(unix)]
fn signal_process_group(pid: u32) -> io::Result<ProcessTerminationOutcome> {
    let group = i32::try_from(pid)
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "process id exceeds i32"))?;
    // SAFETY: the caller verified this PID still owns the persisted process group.
    let result = unsafe { libc::kill(-group, libc::SIGKILL) };
    if result == 0 {
        return Ok(ProcessTerminationOutcome::Terminated);
    }
    let error = io::Error::last_os_error();
    if error.raw_os_error() == Some(libc::ESRCH) {
        return Ok(ProcessTerminationOutcome::Absent);
    }
    Err(error)
}

#[cfg(not(unix))]
fn signal_process_group(_pid: u32) -> io::Result<ProcessTerminationOutcome> {
    Ok(ProcessTerminationOutcome::Absent)
}

pub(crate) fn terminate_process_group(
    pid: u32,
    expected: &str,
) -> io::Result<ProcessTerminationOutcome> {
    let actual = match capture_process_identity(pid) {
        Ok(actual) => actual,
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            return Ok(ProcessTerminationOutcome::Absent);
        }
        #[cfg(unix)]
        Err(error) if error.raw_os_error() == Some(libc::ESRCH) => {
            return Ok(ProcessTerminationOutcome::Absent);
        }
        Err(error) => return Err(error),
    };
    if actual != expected {
        return Ok(ProcessTerminationOutcome::IdentityMismatch);
    }
    #[cfg(unix)]
    if identity_process_group(&actual) != Some(pid) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("process {pid} no longer owns its process group"),
        ));
    }
    signal_process_group(pid)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn current_process_identity_rejects_spoofed_evidence() {
        let pid = std::process::id();
        let identity = capture_process_identity(pid).expect("current process identity");
        assert_ne!(identity, "spoofed-process-identity");
        assert_eq!(
            terminate_process_group(pid, "spoofed-process-identity").unwrap(),
            ProcessTerminationOutcome::IdentityMismatch
        );
    }
}
