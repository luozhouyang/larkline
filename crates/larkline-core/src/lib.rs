//! Core state for larkline: paths, config, keystore, locks and atomic writes.
//!
//! This crate is deliberately dependency-light: it has no async runtime and no
//! network access, so the rules that decide where state lives and how secrets
//! are encrypted can be unit-tested and audited in isolation.

pub mod atomic;
pub mod config;
pub mod env;
pub mod error;
pub mod keystore;
pub mod lock;
pub mod paths;
pub mod session;
pub mod store;

pub use config::{
    AgentKind, MessageReplyMode, Preferences, ProfileConfig, RootConfig, SecretInput, SecretRef,
    TenantBrand,
};
pub use error::{CoreError, Result};
pub use keystore::Keystore;
pub use lock::FileLock;
pub use paths::AppPaths;
pub use session::{SessionEntry, SessionStore};

/// Crate version, embedded at compile time.
pub const VERSION: &str = env!("CARGO_PKG_VERSION");
