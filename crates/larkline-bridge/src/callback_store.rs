//! Persistent used/revoked callback nonces.
//!
//! Port of `src/card/callback-store.ts`. The TypeScript bridge keeps a
//! `callback-nonces.json` file next to `sessions.json` (`dirname(mediaDir)`)
//! so a replay of an already-accepted card token is still rejected after a
//! restart. The Rust bridge used to keep nonces in memory only; this module
//! adds the same durable replay protection while keeping the in-memory
//! `HashMap` as the fast path.
//!
//! ## File format
//!
//! The on-disk shape is the TypeScript one (a flat `nonce -> "used" |
//! "revoked"` object) plus one reserved key carrying the per-nonce expiry:
//!
//! ```json
//! {
//!   "hK3…": "used",
//!   "9xQ…": "revoked",
//!   "__larkline_nonce_expiry": { "hK3…": 1767225600000, "9xQ…": 1767225600000 }
//! }
//! ```
//!
//! The reserved key is an object, so the TypeScript loader skips it while
//! still loading every nonce state — a downgrade keeps replay protection
//! (TS never prunes, so the expiry sidecar is simply ignored there). Entries
//! written by TypeScript have no expiry; they are loaded as [`NO_EXPIRY`] and,
//! like TS, retained forever. Writes are atomic and mode `0600` through
//! [`larkline_core::atomic`].
//!
//! Persistence is **write-through**: every accepted nonce is written under
//! the verifier's nonce lock before verification returns, so there is no
//! shutdown hook to miss and a crash can never lose an accepted nonce. Card
//! clicks are rare, so the fsync cost is irrelevant. Expired entries are
//! pruned on load (and the pruned file is rewritten) and on every verify.

use std::collections::HashMap;
use std::path::{Path, PathBuf};

use serde_json::{Map, Value};

use larkline_core::CoreError;
use larkline_core::atomic;

/// Expiry marker for entries loaded from a TypeScript file: that format has
/// no expiry, and TS never prunes its map, so those nonces are kept forever.
pub const NO_EXPIRY: i64 = i64::MAX;

/// Reserved key holding the Rust-only expiry sidecar (`nonce -> unix ms`).
const EXPIRY_SIDECAR: &str = "__larkline_nonce_expiry";

/// State of a callback nonce (TS `NonceState`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NonceState {
    /// A click consumed the nonce.
    Used,
    /// The nonce was explicitly revoked.
    Revoked,
}

impl NonceState {
    fn as_str(self) -> &'static str {
        match self {
            Self::Used => "used",
            Self::Revoked => "revoked",
        }
    }

    fn parse(value: &str) -> Option<Self> {
        match value {
            "used" => Some(Self::Used),
            "revoked" => Some(Self::Revoked),
            _ => None,
        }
    }
}

/// One persisted nonce: its state plus the token expiry that bounds it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NonceEntry {
    /// Whether the nonce was used or revoked.
    pub state: NonceState,
    /// Unix-millisecond expiry; [`NO_EXPIRY`] for entries from a TS file.
    pub expires_at_ms: i64,
}

/// File-backed callback nonce store.
///
/// The store owns the path only; the live map stays in [`crate::CallbackAuth`]
/// (the "in-memory fast path") and is handed to [`CallbackNonceStore::persist`]
/// on every accepted nonce.
#[derive(Debug, Clone)]
pub struct CallbackNonceStore {
    path: PathBuf,
}

impl CallbackNonceStore {
    /// A store bound to `path`. The file is not touched until [`Self::load`]
    /// or [`Self::persist`].
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self { path: path.into() }
    }

    /// The state file path.
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Load the persisted nonces, dropping entries whose expiry has passed.
    ///
    /// A missing file is a fresh install (empty state). A corrupt or
    /// unreadable file is tolerated: a warning is logged and the store starts
    /// empty, because a bad nonce file must never stop the bridge from
    /// answering cards (the next accepted nonce rewrites it). When pruning
    /// dropped entries the pruned map is written back immediately.
    pub fn load(&self, now_ms: i64) -> HashMap<String, NonceEntry> {
        let bytes = match std::fs::read(&self.path) {
            Ok(bytes) => bytes,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => return HashMap::new(),
            Err(err) => {
                tracing::warn!(
                    path = %self.path.display(),
                    error = %err,
                    "cannot read callback nonce store; starting empty"
                );
                return HashMap::new();
            }
        };
        match parse(&bytes, now_ms) {
            Some((nonces, 0)) => nonces,
            Some((nonces, pruned)) => {
                tracing::info!(
                    path = %self.path.display(),
                    pruned,
                    "pruned expired callback nonces"
                );
                if let Err(err) = self.persist(&nonces) {
                    tracing::warn!(
                        path = %self.path.display(),
                        error = %err,
                        "cannot rewrite the pruned callback nonce store"
                    );
                }
                nonces
            }
            None => {
                tracing::warn!(
                    path = %self.path.display(),
                    "ignoring a corrupt callback nonce store"
                );
                HashMap::new()
            }
        }
    }

    /// Atomically write `nonces` (mode `0600`).
    ///
    /// The TS-compatible flat map is sorted by nonce for a stable file; the
    /// expiry sidecar accompanies it whenever at least one entry has a known
    /// expiry.
    pub fn persist(&self, nonces: &HashMap<String, NonceEntry>) -> larkline_core::Result<()> {
        let mut entries: Vec<(&String, &NonceEntry)> = nonces.iter().collect();
        entries.sort_by(|left, right| left.0.cmp(right.0));

        let mut object = Map::new();
        let mut expiry = Map::new();
        for (nonce, entry) in entries {
            object.insert(
                nonce.clone(),
                Value::String(entry.state.as_str().to_string()),
            );
            if entry.expires_at_ms != NO_EXPIRY {
                expiry.insert(nonce.clone(), Value::from(entry.expires_at_ms));
            }
        }
        if !expiry.is_empty() {
            object.insert(EXPIRY_SIDECAR.to_string(), Value::Object(expiry));
        }

        let mut bytes = serde_json::to_vec_pretty(&Value::Object(object))
            .map_err(|err| CoreError::Message(format!("cannot encode callback nonces: {err}")))?;
        bytes.push(b'\n');
        atomic::write_atomic(&self.path, &bytes, 0o600)
    }
}

/// Parse a store file. `None` means the bytes are not a JSON object (the
/// caller treats that as a corrupt file); `Some((map, pruned))` carries the
/// live entries and how many expired entries were dropped.
fn parse(bytes: &[u8], now_ms: i64) -> Option<(HashMap<String, NonceEntry>, usize)> {
    let raw: Value = serde_json::from_slice(bytes).ok()?;
    let object = raw.as_object()?;
    // Only an *object* at the reserved key is the expiry sidecar. A real
    // nonce that happens to equal the reserved string is a JSON string and is
    // therefore loaded as a normal nonce.
    let sidecar = object.get(EXPIRY_SIDECAR).and_then(Value::as_object);

    let mut nonces = HashMap::new();
    let mut pruned = 0usize;
    for (nonce, value) in object {
        if nonce == EXPIRY_SIDECAR && sidecar.is_some() {
            continue;
        }
        let Some(state) = value.as_str().and_then(NonceState::parse) else {
            // Unknown values are ignored, exactly like the TS loader.
            continue;
        };
        let expires_at_ms = sidecar
            .and_then(|entries| entries.get(nonce))
            .and_then(json_i64)
            .unwrap_or(NO_EXPIRY);
        if expires_at_ms != NO_EXPIRY && expires_at_ms <= now_ms {
            pruned += 1;
            continue;
        }
        nonces.insert(
            nonce.clone(),
            NonceEntry {
                state,
                expires_at_ms,
            },
        );
    }
    Some((nonces, pruned))
}

/// Read a JSON number as `i64`, accepting the unsigned range too.
fn json_i64(value: &Value) -> Option<i64> {
    value
        .as_i64()
        .or_else(|| value.as_u64().and_then(|value| i64::try_from(value).ok()))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn entry(state: NonceState, expires_at_ms: i64) -> NonceEntry {
        NonceEntry {
            state,
            expires_at_ms,
        }
    }

    fn temp_store() -> (tempfile::TempDir, CallbackNonceStore) {
        let dir = tempfile::tempdir().unwrap();
        let store = CallbackNonceStore::new(dir.path().join("callback-nonces.json"));
        (dir, store)
    }

    #[test]
    fn persisted_nonces_roundtrip_with_state_and_expiry() {
        let (_dir, store) = temp_store();
        let nonces = HashMap::from([
            ("n_used".to_string(), entry(NonceState::Used, 2_000)),
            ("n_revoked".to_string(), entry(NonceState::Revoked, 3_000)),
        ]);
        store.persist(&nonces).unwrap();
        assert_eq!(store.load(1_000), nonces);
    }

    #[test]
    fn expired_nonces_are_pruned_and_the_file_is_rewritten() {
        let (_dir, store) = temp_store();
        let nonces = HashMap::from([
            ("n_expired".to_string(), entry(NonceState::Used, 2_000)),
            ("n_live".to_string(), entry(NonceState::Used, 9_000)),
        ]);
        store.persist(&nonces).unwrap();

        let loaded = store.load(5_000);
        assert_eq!(loaded.len(), 1);
        assert!(loaded.contains_key("n_live"));

        // The pruned map was written back, so a second load cannot resurrect
        // the expired nonce and the file no longer mentions it.
        let raw = std::fs::read_to_string(store.path()).unwrap();
        assert!(!raw.contains("n_expired"), "{raw}");
        assert!(raw.contains("n_live"), "{raw}");
        assert_eq!(store.load(5_000), loaded);
    }

    #[test]
    fn the_typescript_flat_format_still_loads() {
        // A file written by the TypeScript bridge has no expiry sidecar.
        let (_dir, store) = temp_store();
        std::fs::write(
            store.path(),
            br#"{"n_used":"used","n_revoked":"revoked","n_junk":42}"#,
        )
        .unwrap();
        let loaded = store.load(1_000);
        assert_eq!(loaded.len(), 2);
        assert_eq!(
            loaded.get("n_used"),
            Some(&entry(NonceState::Used, NO_EXPIRY))
        );
        assert_eq!(
            loaded.get("n_revoked"),
            Some(&entry(NonceState::Revoked, NO_EXPIRY))
        );
    }

    #[test]
    fn a_corrupt_or_non_object_file_is_tolerated() {
        let (_dir, store) = temp_store();
        for junk in [&b"not json"[..], b"[]", b"42", b"\xff\xfe"] {
            std::fs::write(store.path(), junk).unwrap();
            assert!(store.load(1_000).is_empty(), "{junk:?}");
        }
        // A missing file is the fresh-install case.
        std::fs::remove_file(store.path()).unwrap();
        assert!(store.load(1_000).is_empty());
        // The next persist replaces the corrupt content.
        store.persist(&HashMap::new()).unwrap();
        assert_eq!(std::fs::read_to_string(store.path()).unwrap().trim(), "{}");
    }

    #[cfg(unix)]
    #[test]
    fn the_store_file_is_written_with_mode_0600() {
        use std::os::unix::fs::PermissionsExt;

        let (_dir, store) = temp_store();
        store
            .persist(&HashMap::from([(
                "n_used".to_string(),
                entry(NonceState::Used, 2_000),
            )]))
            .unwrap();
        let mode = std::fs::metadata(store.path())
            .unwrap()
            .permissions()
            .mode()
            & 0o777;
        assert_eq!(mode, 0o600);
    }
}
