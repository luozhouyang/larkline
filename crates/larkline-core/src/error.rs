use std::path::PathBuf;

/// Errors raised by the state layer (paths, config, keystore, locks).
#[derive(Debug, thiserror::Error)]
pub enum CoreError {
    #[error("io error at {path}: {source}")]
    Io {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },

    #[error("invalid json at {path}: {source}")]
    Json {
        path: PathBuf,
        #[source]
        source: serde_json::Error,
    },

    #[error("invalid profile name: {0}")]
    InvalidProfileName(String),

    #[error("unknown profile: {0}")]
    UnknownProfile(String),

    #[error("no active profile (run `larkline profile create <name>` first)")]
    NoActiveProfile,

    /// Keystore failures deliberately carry no secret material.
    #[error("keystore error: {0}")]
    Keystore(String),

    #[error("timed out waiting for lock {0}")]
    LockTimeout(PathBuf),

    #[error("{0}")]
    Message(String),
}

pub type Result<T> = std::result::Result<T, CoreError>;

impl CoreError {
    pub(crate) fn io(path: impl Into<PathBuf>, source: std::io::Error) -> Self {
        Self::Io {
            path: path.into(),
            source,
        }
    }

    pub(crate) fn json(path: impl Into<PathBuf>, source: serde_json::Error) -> Self {
        Self::Json {
            path: path.into(),
            source,
        }
    }
}
