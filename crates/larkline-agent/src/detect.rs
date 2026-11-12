//! Executable discovery.
//!
//! Mirrors `src/cli/agent-detection.ts`: honour `CLAUDE_BIN` / `CODEX_BIN`,
//! search `PATH`, and on Windows try `PATHEXT` suffixes (`claude.cmd`).
//! Rust's `std::process::Command` cannot execute a `.cmd` directly, so callers
//! must route `.cmd`/`.bat` through `cmd.exe /d /s /c` — see `spawn.rs` in the
//! bridge crate.

use std::path::{Path, PathBuf};

use thiserror::Error;

#[derive(Debug, Error)]
pub enum DetectError {
    #[error("executable not found: {0}")]
    NotFound(String),
    #[error("executable is not runnable: {0}")]
    NotExecutable(PathBuf),
    #[error("cannot resolve PATH: {0}")]
    PathUnavailable(String),
}

#[derive(Debug, Clone)]
pub struct ResolvedBinary {
    pub path: PathBuf,
}

/// Resolve `command` to an absolute, executable path.
pub fn resolve_executable(command: &str) -> Result<ResolvedBinary, DetectError> {
    let candidate = Path::new(command);
    if candidate.is_absolute() || command.contains(std::path::MAIN_SEPARATOR) {
        return if is_executable(candidate) {
            Ok(ResolvedBinary {
                path: candidate.to_path_buf(),
            })
        } else {
            Err(DetectError::NotExecutable(candidate.to_path_buf()))
        };
    }

    let path_var = std::env::var_os("PATH")
        .ok_or_else(|| DetectError::PathUnavailable("PATH is not set".into()))?;

    for dir in std::env::split_paths(&path_var) {
        if dir.as_os_str().is_empty() {
            continue;
        }
        for name in candidate_names(command) {
            let full = dir.join(name);
            if is_executable(&full) {
                return Ok(ResolvedBinary { path: full });
            }
        }
    }
    Err(DetectError::NotFound(command.to_string()))
}

/// Candidate file names for a bare command, honouring `PATHEXT`.
fn candidate_names(command: &str) -> Vec<String> {
    #[allow(unused_mut)]
    let mut names = vec![command.to_string()];
    #[cfg(windows)]
    {
        let has_extension = Path::new(command).extension().is_some();
        if !has_extension {
            let pathext =
                std::env::var("PATHEXT").unwrap_or_else(|_| ".COM;.EXE;.BAT;.CMD".to_string());
            for ext in pathext.split(';').filter(|e| !e.is_empty()) {
                names.push(format!("{command}{}", ext.to_ascii_lowercase()));
            }
        }
    }
    names
}

#[cfg(unix)]
fn is_executable(path: &Path) -> bool {
    use std::os::unix::fs::PermissionsExt;
    match std::fs::metadata(path) {
        Ok(meta) => meta.is_file() && meta.permissions().mode() & 0o111 != 0,
        Err(_) => false,
    }
}

#[cfg(not(unix))]
fn is_executable(path: &Path) -> bool {
    match std::fs::metadata(path) {
        Ok(meta) => meta.is_file(),
        Err(_) => false,
    }
}

/// A `.cmd`/`.bat` shim needs `cmd.exe` on Windows. On Unix this is always
/// false, so callers can keep one code path.
pub fn needs_cmd_shim(path: &Path) -> bool {
    #[cfg(windows)]
    {
        matches!(
            path.extension().and_then(|e| e.to_str()).map(|e| e.to_ascii_lowercase()),
            Some(ext) if ext == "cmd" || ext == "bat"
        )
    }
    #[cfg(not(windows))]
    {
        let _ = path;
        false
    }
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;
    use std::os::unix::fs::PermissionsExt;

    #[test]
    fn finds_executable_on_path() {
        let dir = tempfile::tempdir().unwrap();
        let exe = dir.path().join("fake-agent");
        std::fs::write(&exe, "#!/bin/sh\n").unwrap();
        std::fs::set_permissions(&exe, std::fs::Permissions::from_mode(0o755)).unwrap();

        let old = std::env::var_os("PATH");
        // SAFETY: tests run single-threaded here; PATH is only read by us.
        unsafe { std::env::set_var("PATH", dir.path()) };
        let resolved = resolve_executable("fake-agent").unwrap();
        match old {
            Some(value) => unsafe { std::env::set_var("PATH", value) },
            None => unsafe { std::env::remove_var("PATH") },
        }
        assert_eq!(resolved.path, exe);
    }

    #[test]
    fn rejects_non_executable() {
        let dir = tempfile::tempdir().unwrap();
        let exe = dir.path().join("plain");
        std::fs::write(&exe, "data").unwrap();
        std::fs::set_permissions(&exe, std::fs::Permissions::from_mode(0o644)).unwrap();
        assert!(matches!(
            resolve_executable(exe.to_str().unwrap()),
            Err(DetectError::NotExecutable(_))
        ));
    }
}
