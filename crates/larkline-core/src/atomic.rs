//! Crash-safe file writes.
//!
//! Every state file (config, sessions, keystore) is written by creating a
//! sibling temp file, fsyncing it, then renaming it over the destination.
//! On Unix the temp file is created with the final mode so secret-bearing
//! bytes never exist at a looser permission.

use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::Path;

use crate::error::{CoreError, Result};

/// Atomically replace `path` with `bytes`, using `mode` (e.g. 0o600).
pub fn write_atomic(path: &Path, bytes: &[u8], mode: u32) -> Result<()> {
    let parent = path
        .parent()
        .ok_or_else(|| CoreError::Message(format!("path has no parent: {}", path.display())))?;
    fs::create_dir_all(parent).map_err(|e| CoreError::io(parent, e))?;

    let file_name = path
        .file_name()
        .map(|n| n.to_string_lossy().into_owned())
        .unwrap_or_else(|| "state".to_string());
    let tmp = parent.join(format!(".{file_name}.tmp-{}", std::process::id()));

    {
        let mut opts = OpenOptions::new();
        opts.write(true).create(true).truncate(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            opts.mode(mode);
        }
        #[cfg(not(unix))]
        let _ = mode;

        let mut file = opts.open(&tmp).map_err(|e| CoreError::io(&tmp, e))?;
        file.write_all(bytes).map_err(|e| CoreError::io(&tmp, e))?;
        file.sync_all().map_err(|e| CoreError::io(&tmp, e))?;
    }

    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(&tmp, fs::Permissions::from_mode(mode))
            .map_err(|e| CoreError::io(&tmp, e))?;
    }

    replace(&tmp, path)?;

    // Fsync the directory so the rename itself survives a power loss. This is
    // best effort: some filesystems refuse to open a directory for fsync.
    #[cfg(unix)]
    {
        if let Ok(dir) = fs::File::open(parent) {
            let _ = dir.sync_all();
        }
    }
    Ok(())
}

fn replace(from: &Path, to: &Path) -> Result<()> {
    match fs::rename(from, to) {
        Ok(()) => Ok(()),
        Err(err) if cfg!(windows) && to.exists() => {
            // Windows cannot atomically replace an existing file. Removing the
            // destination first trades atomicity for correctness; the caller
            // holds the config lock, so a concurrent reader is not expected.
            fs::remove_file(to).map_err(|e| CoreError::io(to, e))?;
            fs::rename(from, to).map_err(|e| CoreError::io(to, e))
        }
        Err(err) => Err(CoreError::io(to, err)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn replaces_existing_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("state.json");
        write_atomic(&path, b"one", 0o600).unwrap();
        write_atomic(&path, b"two", 0o600).unwrap();
        assert_eq!(fs::read(&path).unwrap(), b"two");
        let leftovers: Vec<_> = fs::read_dir(dir.path())
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| e.file_name().to_string_lossy().contains(".tmp-"))
            .collect();
        assert!(leftovers.is_empty());
    }

    #[cfg(unix)]
    #[test]
    fn applies_mode() {
        use std::os::unix::fs::PermissionsExt;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("secret");
        write_atomic(&path, b"x", 0o600).unwrap();
        let mode = fs::metadata(&path).unwrap().permissions().mode() & 0o777;
        assert_eq!(mode, 0o600);
    }
}
