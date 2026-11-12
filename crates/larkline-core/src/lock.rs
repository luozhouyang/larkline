//! Advisory file locks.
//!
//! Both the TypeScript bridge (`proper-lockfile`) and this one take an
//! exclusive lock before mutating shared state. The wire format is just a
//! file, so a mixed-version rollout does not corrupt config: the lock file
//! path is identical and the lock is advisory on both sides.

use std::fs::{self, File, OpenOptions};
use std::path::{Path, PathBuf};
use std::thread::sleep;
use std::time::{Duration, Instant};

use fs4::fs_std::FileExt;

use crate::error::{CoreError, Result};

/// An exclusive advisory lock held for the lifetime of the value.
#[derive(Debug)]
pub struct FileLock {
    file: File,
    path: PathBuf,
}

impl FileLock {
    /// Acquire `path`, retrying until `timeout` elapses.
    pub fn acquire(path: &Path, timeout: Duration) -> Result<Self> {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).map_err(|e| CoreError::io(parent, e))?;
        }

        let deadline = Instant::now() + timeout;
        let mut backoff = Duration::from_millis(10);
        loop {
            let file = OpenOptions::new()
                .create(true)
                .read(true)
                .write(true)
                .truncate(false)
                .open(path)
                .map_err(|e| CoreError::io(path, e))?;

            match file.try_lock_exclusive() {
                Ok(true) => {
                    set_private(&file, path)?;
                    return Ok(Self {
                        file,
                        path: path.to_path_buf(),
                    });
                }
                Ok(false) => {
                    if Instant::now() >= deadline {
                        return Err(CoreError::LockTimeout(path.to_path_buf()));
                    }
                    sleep(backoff);
                    backoff = (backoff * 2).min(Duration::from_millis(250));
                }
                Err(err) if is_contended(&err) => {
                    if Instant::now() >= deadline {
                        return Err(CoreError::LockTimeout(path.to_path_buf()));
                    }
                    sleep(backoff);
                    backoff = (backoff * 2).min(Duration::from_millis(250));
                }
                Err(err) => return Err(CoreError::io(path, err)),
            }
        }
    }

    /// Acquire with the default 5s budget.
    pub fn acquire_default(path: &Path) -> Result<Self> {
        Self::acquire(path, Duration::from_secs(5))
    }

    pub fn path(&self) -> &Path {
        &self.path
    }
}

impl Drop for FileLock {
    fn drop(&mut self) {
        let _ = FileExt::unlock(&self.file);
    }
}

fn is_contended(err: &std::io::Error) -> bool {
    matches!(
        err.kind(),
        std::io::ErrorKind::WouldBlock | std::io::ErrorKind::PermissionDenied
    )
}

#[cfg(unix)]
fn set_private(file: &File, path: &Path) -> Result<()> {
    use std::os::unix::fs::PermissionsExt;
    let _ = file.set_permissions(fs::Permissions::from_mode(0o600));
    let _ = path;
    Ok(())
}

#[cfg(not(unix))]
fn set_private(_file: &File, _path: &Path) -> Result<()> {
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn second_lock_times_out() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("x.lock");
        let _held = FileLock::acquire(&path, Duration::from_millis(200)).unwrap();
        let err = FileLock::acquire(&path, Duration::from_millis(100)).unwrap_err();
        assert!(matches!(err, CoreError::LockTimeout(_)));
    }

    #[test]
    fn released_lock_can_be_reacquired() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("x.lock");
        {
            let _held = FileLock::acquire(&path, Duration::from_millis(200)).unwrap();
        }
        let _again = FileLock::acquire(&path, Duration::from_millis(200)).unwrap();
    }
}
