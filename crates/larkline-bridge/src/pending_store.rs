//! Durable pending batches.
//!
//! The in-memory queue in [`crate::pending`] is lost when the process exits.
//! A long run can hold messages for up to [`crate::run::RUN_TIMEOUT`], and the
//! service manager restarts a crashed bridge automatically, so a queued
//! message that is never executed is the worst kind of loss: silent.
//!
//! This module writes the queue through to `profiles/<name>/pending.json`
//! after every mutation (enqueue, dequeue, cancel) and restores it when the
//! bridge is built. The file is Rust-only; the TypeScript bridge ignores it.
//!
//! ## Contract
//!
//! * One file per profile, a `scope -> [message]` map. The scope is the same
//!   key used by the sessions/workspaces/queue (`chatId` or
//!   `chatId:threadId`); a restored batch therefore lands in exactly the
//!   conversation that queued it.
//! * Only the fields needed to rebuild the prompt are kept. The normalized
//!   event's `raw`, `content` and `raw_content` fields are stripped before the
//!   message is written: they carry the full platform payload and are not
//!   needed to run the agent.
//! * The file is atomic (temp file + fsync + rename) and mode `0600`, like the
//!   session and callback-nonce stores.
//! * A corrupt, unreadable or future-version file is logged and treated as
//!   empty. Persistence must never stop the bridge from starting.
//! * TTL is measured from the file's `savedAt`. The bridge rewrites the file
//!   on every queue change, so the age of a restored batch is bounded by the
//!   queue's last mutation; a profile that has been down longer than the TTL
//!   starts with an empty queue.
//!
//! ## Delivery semantics
//!
//! Restoration is **at-most-once**: a batch is removed from the file when the
//! queue takes it for a run, before the agent starts. A crash between the
//! dequeue and the agent's first turn loses that batch rather than replaying
//! it and producing a duplicate answer. This mirrors the user's expectation
//! that a resend is safer than a duplicate turn.

use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::time::Duration;

use lark_channel::message::NormalizedMessage;
use larkline_core::atomic::write_atomic;
use serde::{Deserialize, Serialize};
use serde_json::Value;

/// Current on-disk format version.
pub const PENDING_FILE_VERSION: u32 = 1;

/// How long a persisted batch is replayed after the last queue mutation.
pub const DEFAULT_PENDING_TTL: Duration = Duration::from_secs(24 * 60 * 60);

/// File mode: queued messages may contain conversation text and open ids.
const PENDING_FILE_MODE: u32 = 0o600;

/// Keys removed from a normalized message before it is written.
///
/// `raw` is the whole platform event; `content` / `raw_content` are the
/// platform message body. The bridge only needs `text`, identity, mentions and
/// resource descriptors to rebuild the prompt.
const STRIPPED_MESSAGE_KEYS: [&str; 3] = ["raw", "content", "raw_content"];

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct StoredMessage {
    queued_at_ms: i64,
    message: Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct PendingFile {
    version: u32,
    saved_at_ms: i64,
    #[serde(default)]
    scopes: BTreeMap<String, Vec<StoredMessage>>,
}

/// Result of a load, including counts for the startup log.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct PendingLoadStats {
    pub restored: usize,
    pub expired: usize,
    pub invalid: usize,
    /// The file existed but could not be parsed (treated as empty).
    pub corrupt: bool,
}

/// Result of a load: the per-scope batches plus what happened to the rest.
#[derive(Debug, Default)]
pub struct LoadedPending {
    pub scopes: Vec<(String, Vec<NormalizedMessage>)>,
    pub stats: PendingLoadStats,
}

/// File-backed pending queue.
#[derive(Debug, Clone)]
pub struct PendingStore {
    path: PathBuf,
    ttl: Duration,
}

impl PendingStore {
    /// A store bound to `path` with the default TTL.
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self {
            path: path.into(),
            ttl: DEFAULT_PENDING_TTL,
        }
    }

    /// A store with an explicit TTL. A zero TTL disables expiry.
    pub fn with_ttl(mut self, ttl: Duration) -> Self {
        self.ttl = ttl;
        self
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn ttl(&self) -> Duration {
        self.ttl
    }

    /// Load and validate the persisted batches.
    ///
    /// A missing file is a fresh install. A corrupt or future-version file is
    /// logged and treated as empty so a bad state file can never block the
    /// bridge from starting.
    pub fn load(&self, now_ms: i64) -> LoadedPending {
        let mut loaded = LoadedPending::default();
        let bytes = match fs::read(&self.path) {
            Ok(bytes) => bytes,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => return loaded,
            Err(err) => {
                tracing::warn!(
                    path = %self.path.display(),
                    error = %err,
                    "cannot read the pending queue; starting empty"
                );
                loaded.stats.corrupt = true;
                return loaded;
            }
        };
        if bytes.iter().all(|byte| byte.is_ascii_whitespace()) {
            return loaded;
        }
        let file: PendingFile = match serde_json::from_slice(&bytes) {
            Ok(file) => file,
            Err(err) => {
                tracing::warn!(
                    path = %self.path.display(),
                    error = %err,
                    "corrupt pending queue; starting empty"
                );
                loaded.stats.corrupt = true;
                return loaded;
            }
        };
        if file.version > PENDING_FILE_VERSION {
            tracing::warn!(
                path = %self.path.display(),
                version = file.version,
                current = PENDING_FILE_VERSION,
                "pending queue was written by a newer bridge; starting empty"
            );
            loaded.stats.corrupt = true;
            return loaded;
        }
        if !self.ttl.is_zero() && file.saved_at_ms > 0 {
            let age_ms = now_ms.saturating_sub(file.saved_at_ms);
            if age_ms > self.ttl.as_millis() as i64 {
                tracing::info!(
                    age_ms,
                    ttl_ms = self.ttl.as_millis() as i64,
                    "pending queue is older than its TTL; dropping it"
                );
                loaded.stats.expired = file.scopes.values().map(|messages| messages.len()).sum();
                return loaded;
            }
        }

        for (scope, stored) in file.scopes {
            let mut messages = Vec::with_capacity(stored.len());
            for entry in stored {
                match serde_json::from_value::<NormalizedMessage>(entry.message) {
                    Ok(message) => messages.push(message),
                    Err(err) => {
                        loaded.stats.invalid += 1;
                        tracing::warn!(
                            scope,
                            error = %err,
                            "dropping an unreadable pending message"
                        );
                    }
                }
            }
            if messages.is_empty() {
                continue;
            }
            loaded.stats.restored += messages.len();
            loaded.scopes.push((scope, messages));
        }
        loaded
    }

    /// Write the current batches atomically.
    ///
    /// Scopes with no messages are omitted. `now_ms` is written as the file's
    /// `savedAt` and bounds the TTL on the next load.
    pub fn save(
        &self,
        scopes: &[(String, Vec<NormalizedMessage>)],
        now_ms: i64,
    ) -> Result<(), String> {
        let mut file = PendingFile {
            version: PENDING_FILE_VERSION,
            saved_at_ms: now_ms,
            scopes: BTreeMap::new(),
        };
        for (scope, messages) in scopes {
            if messages.is_empty() {
                continue;
            }
            let mut stored = Vec::with_capacity(messages.len());
            for message in messages {
                let mut value = serde_json::to_value(message)
                    .map_err(|err| format!("cannot serialize a pending message: {err}"))?;
                if let Some(object) = value.as_object_mut() {
                    for key in STRIPPED_MESSAGE_KEYS {
                        object.remove(key);
                    }
                }
                stored.push(StoredMessage {
                    queued_at_ms: now_ms,
                    message: value,
                });
            }
            file.scopes.insert(scope.clone(), stored);
        }
        let bytes = serde_json::to_vec_pretty(&file)
            .map_err(|err| format!("cannot encode the pending queue: {err}"))?;
        write_atomic(&self.path, &bytes, PENDING_FILE_MODE)
            .map_err(|err| format!("cannot write {}: {err}", self.path.display()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn message(id: &str, chat_id: &str, text: &str) -> NormalizedMessage {
        serde_json::from_value(json!({
            "message_id": id,
            "chat_id": chat_id,
            "chat_type": "p2p",
            "sender_id": "ou_user",
            "sender": { "open_id": "ou_user", "sender_type": "user" },
            "text": text,
            "raw": { "event": { "secret": "do-not-persist" } },
            "raw_content": "{\"secret\":\"do-not-persist\"}"
        }))
        .expect("normalized message fixture")
    }

    #[test]
    fn round_trips_scopes_and_order() {
        let dir = tempfile::tempdir().unwrap();
        let store = PendingStore::new(dir.path().join("pending.json"));
        let now = 1_700_000_000_000;
        let scopes = vec![
            (
                "oc_a".to_string(),
                vec![
                    message("om_1", "oc_a", "first"),
                    message("om_2", "oc_a", "second"),
                ],
            ),
            (
                "oc_b:omt_1".to_string(),
                vec![message("om_3", "oc_b", "third")],
            ),
        ];
        store.save(&scopes, now).unwrap();

        let loaded = store.load(now + 1_000);
        assert_eq!(loaded.stats.restored, 3);
        assert_eq!(loaded.stats.expired, 0);
        assert_eq!(loaded.stats.invalid, 0);
        assert!(!loaded.stats.corrupt);
        assert_eq!(loaded.scopes.len(), 2);
        let (scope, messages) = &loaded.scopes[0];
        assert_eq!(scope, "oc_a");
        assert_eq!(
            messages
                .iter()
                .map(|message| message.text.as_str())
                .collect::<Vec<_>>(),
            vec!["first", "second"]
        );

        // The raw platform payload never reaches the file.
        let raw_file = fs::read_to_string(store.path()).unwrap();
        let parsed: Value = serde_json::from_str(&raw_file).unwrap();
        let stored_message = &parsed["scopes"]["oc_a"][0]["message"];
        assert!(stored_message.get("raw").is_none());
        assert!(stored_message.get("raw_content").is_none());
        assert!(stored_message.get("content").is_none());
        assert!(!raw_file.contains("do-not-persist"));
    }

    #[test]
    fn expires_a_stale_file() {
        let dir = tempfile::tempdir().unwrap();
        let store =
            PendingStore::new(dir.path().join("pending.json")).with_ttl(Duration::from_secs(60));
        let then = 1_700_000_000_000;
        store
            .save(
                &[("oc_a".to_string(), vec![message("om_1", "oc_a", "old")])],
                then,
            )
            .unwrap();

        let loaded = store.load(then + 61_000);
        assert!(loaded.scopes.is_empty());
        assert_eq!(loaded.stats.expired, 1);
    }

    #[test]
    fn a_corrupt_file_is_empty_and_does_not_panic() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("pending.json");
        fs::write(&path, "{not json").unwrap();
        let loaded = PendingStore::new(&path).load(0);
        assert!(loaded.scopes.is_empty());
        assert!(loaded.stats.corrupt);
    }

    #[test]
    fn a_future_version_is_rejected() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("pending.json");
        fs::write(
            &path,
            serde_json::to_vec(&json!({
                "version": PENDING_FILE_VERSION + 1,
                "savedAtMs": 1,
                "scopes": {}
            }))
            .unwrap(),
        )
        .unwrap();
        let loaded = PendingStore::new(&path).load(2);
        assert!(loaded.scopes.is_empty());
        assert!(loaded.stats.corrupt);
    }

    #[test]
    fn an_unreadable_message_is_skipped_without_losing_the_rest() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("pending.json");
        let valid = serde_json::to_value(message("om_1", "oc_a", "ok")).unwrap();
        fs::write(
            &path,
            serde_json::to_vec(&json!({
                "version": PENDING_FILE_VERSION,
                "savedAtMs": 1_000,
                "scopes": { "oc_a": [
                    { "queuedAtMs": 1_000, "message": valid },
                    { "queuedAtMs": 1_000, "message": { "nope": true } }
                ] }
            }))
            .unwrap(),
        )
        .unwrap();
        let loaded = PendingStore::new(&path).load(2_000);
        assert_eq!(loaded.stats.restored, 1);
        assert_eq!(loaded.stats.invalid, 1);
        assert_eq!(loaded.scopes[0].1[0].text, "ok");
    }
}
