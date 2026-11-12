//! Resumable agent sessions, keyed by chat.
//!
//! Format-compatible with `src/session/store.ts`:
//!
//! ```json
//! { "oc_xxx": { "sessionId": "...", "cwd": "/work", "updatedAt": 1700000000000 } }
//! ```
//!
//! A session is only resumable from the cwd it was created in — resuming from
//! a different working directory would point the agent at a session file it
//! cannot find.

use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};

use crate::atomic::write_atomic;
use crate::error::{CoreError, Result};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct SessionEntry {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub session_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cwd: Option<String>,
    pub updated_at: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub idle_timeout_minutes: Option<u64>,
}

impl SessionEntry {
    fn is_resumable(&self) -> bool {
        self.session_id.is_some() && self.cwd.is_some()
    }
}

#[derive(Debug, Clone, Default)]
pub struct SessionStore {
    path: PathBuf,
    data: BTreeMap<String, SessionEntry>,
}

impl SessionStore {
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self {
            path: path.into(),
            data: BTreeMap::new(),
        }
    }

    pub fn load(path: impl Into<PathBuf>) -> Result<Self> {
        let path = path.into();
        let mut store = Self::new(path.clone());
        match fs::read(&path) {
            Ok(bytes) => {
                if bytes.iter().all(|b| b.is_ascii_whitespace()) {
                    return Ok(store);
                }
                let raw: BTreeMap<String, SessionEntry> =
                    serde_json::from_slice(&bytes).map_err(|e| CoreError::json(&path, e))?;
                for (chat_id, entry) in raw {
                    // Keep entries that are resumable or carry a timeout
                    // preference; discard empty shells.
                    if entry.is_resumable() || entry.idle_timeout_minutes.is_some() {
                        store.data.insert(chat_id, entry);
                    }
                }
                Ok(store)
            }
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(store),
            Err(err) => Err(CoreError::io(&path, err)),
        }
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Session id for `chat_id` if it was created in `cwd`.
    pub fn resume_for(&self, chat_id: &str, cwd: &str) -> Option<&str> {
        self.data
            .get(chat_id)
            .filter(|entry| entry.cwd.as_deref() == Some(cwd))
            .and_then(|entry| entry.session_id.as_deref())
    }

    pub fn raw(&self, chat_id: &str) -> Option<&SessionEntry> {
        self.data.get(chat_id)
    }

    /// Record a session, preserving a per-scope idle-timeout override.
    pub fn set(&mut self, chat_id: &str, session_id: &str, cwd: &str) -> Result<()> {
        let idle = self
            .data
            .get(chat_id)
            .and_then(|entry| entry.idle_timeout_minutes);
        self.data.insert(
            chat_id.to_string(),
            SessionEntry {
                session_id: Some(session_id.to_string()),
                cwd: Some(cwd.to_string()),
                updated_at: now_ms(),
                idle_timeout_minutes: idle,
            },
        );
        self.save()
    }

    /// Forget the resumable session but keep the timeout preference.
    pub fn clear(&mut self, chat_id: &str) -> Result<()> {
        match self.data.get(chat_id) {
            None => Ok(()),
            Some(entry) if entry.idle_timeout_minutes.is_some() => {
                self.data.insert(
                    chat_id.to_string(),
                    SessionEntry {
                        session_id: None,
                        cwd: None,
                        updated_at: now_ms(),
                        idle_timeout_minutes: entry.idle_timeout_minutes,
                    },
                );
                self.save()
            }
            Some(_) => {
                self.data.remove(chat_id);
                self.save()
            }
        }
    }

    fn save(&self) -> Result<()> {
        let mut bytes =
            serde_json::to_vec_pretty(&self.data).map_err(|e| CoreError::json(&self.path, e))?;
        bytes.push(b'\n');
        write_atomic(&self.path, &bytes, 0o600)
    }
}

pub fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip_and_resume() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("sessions.json");
        let mut store = SessionStore::load(&path).unwrap();
        store.set("oc_1", "sess-1", "/work").unwrap();

        let reloaded = SessionStore::load(&path).unwrap();
        assert_eq!(reloaded.resume_for("oc_1", "/work"), Some("sess-1"));
        assert_eq!(reloaded.resume_for("oc_1", "/other"), None);
    }

    #[test]
    fn clear_keeps_timeout_override() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("sessions.json");
        let mut store = SessionStore::load(&path).unwrap();
        store.set("oc_1", "sess-1", "/work").unwrap();
        store.data.get_mut("oc_1").unwrap().idle_timeout_minutes = Some(5);
        store.clear("oc_1").unwrap();
        let entry = store.raw("oc_1").unwrap();
        assert_eq!(entry.session_id, None);
        assert_eq!(entry.idle_timeout_minutes, Some(5));
    }

    #[test]
    fn drops_empty_entries_on_load() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("sessions.json");
        fs::write(
            &path,
            r#"{"a":{"updatedAt":1},"b":{"sessionId":"s","cwd":"/w","updatedAt":2}}"#,
        )
        .unwrap();
        let store = SessionStore::load(&path).unwrap();
        assert!(store.raw("a").is_none());
        assert!(store.raw("b").is_some());
    }
}
