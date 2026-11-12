//! Per-chat working directories and named workspace aliases.
//!
//! Format-compatible with `src/workspace/store.ts`:
//!
//! ```json
//! {
//!   "chats": {
//!     "oc_xxx": { "cwd": "/work" }
//!   },
//!   "named": {
//!     "alias": "/work"
//!   }
//! }
//! ```
//!
//! The file only stores explicit per-chat overrides and aliases. The profile's
//! default working directory lives in `config.json` (`workspaces.default`), so
//! it is supplied to [`WorkspaceStore::load_with_default`] at runtime rather
//! than persisted here; [`WorkspaceStore::workspace_for`] uses it as the
//! fallback for chats that never ran `/cd`.
//!
//! Workspaces are convenience state, not secrets or credentials: a malformed
//! or partially hand-edited `workspaces.json` must not stop the bridge from
//! starting. Syntactically invalid JSON and invalid entries are therefore
//! ignored on load (with a warning); only real I/O errors are returned.

use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::atomic::write_atomic;
use crate::error::{CoreError, Result};

/// One chat's working directory, serialized as `{ "cwd": "..." }`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ChatWorkspace {
    pub cwd: String,
}

impl ChatWorkspace {
    fn new(cwd: impl Into<String>) -> Self {
        Self { cwd: cwd.into() }
    }
}

/// `workspaces.json`: per-chat cwds plus named aliases.
#[derive(Debug, Clone, Default)]
pub struct WorkspaceStore {
    path: PathBuf,
    /// Profile default cwd, used when a chat has no explicit override.
    ///
    /// Deliberately not part of the on-disk format: `config.json` owns
    /// `workspaces.default`, and the TypeScript bridge would drop unknown
    /// fields if it ever rewrote this file.
    default_dir: Option<String>,
    chats: BTreeMap<String, ChatWorkspace>,
    named: BTreeMap<String, String>,
}

/// Serialization view so only the two compat fields are written.
#[derive(Serialize)]
struct WorkspaceData<'a> {
    chats: &'a BTreeMap<String, ChatWorkspace>,
    named: &'a BTreeMap<String, String>,
}

impl WorkspaceStore {
    /// Load `workspaces.json`, tolerating missing files and bad entries.
    pub fn load(path: impl Into<PathBuf>) -> Result<Self> {
        Self::load_at(path.into(), None)
    }

    /// Load `workspaces.json` and remember the profile default cwd.
    pub fn load_with_default(
        path: impl Into<PathBuf>,
        default_dir: impl AsRef<Path>,
    ) -> Result<Self> {
        Self::load_at(path.into(), Some(path_string(default_dir.as_ref())?))
    }

    /// Set the fallback cwd without touching the file.
    pub fn with_default_dir(mut self, default_dir: impl AsRef<Path>) -> Result<Self> {
        self.set_default_dir(default_dir)?;
        Ok(self)
    }

    /// Replace the fallback cwd. Empty values clear it.
    ///
    /// This is runtime state only; it is never written to `workspaces.json`.
    pub fn set_default_dir(&mut self, default_dir: impl AsRef<Path>) -> Result<()> {
        let value = default_dir.as_ref();
        self.default_dir = if value.as_os_str().is_empty() {
            None
        } else {
            Some(path_string(value)?)
        };
        Ok(())
    }

    /// Fallback cwd, if the store was loaded with one.
    pub fn default_dir(&self) -> Option<&str> {
        self.default_dir.as_deref()
    }

    /// Path of the backing `workspaces.json`.
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Effective cwd for `chat_id`: its override, else the profile default.
    ///
    /// Returns `None` only when neither exists.
    pub fn workspace_for(&self, chat_id: &str) -> Option<&str> {
        self.cwd_for(chat_id).or(self.default_dir.as_deref())
    }

    /// Explicit per-chat override, without the default fallback.
    ///
    /// Mirrors `cwdFor()` in the TypeScript store.
    pub fn cwd_for(&self, chat_id: &str) -> Option<&str> {
        self.chats.get(chat_id).map(|entry| entry.cwd.as_str())
    }

    /// Raw entry for `chat_id`, if one is stored.
    pub fn entry(&self, chat_id: &str) -> Option<&ChatWorkspace> {
        self.chats.get(chat_id)
    }

    /// Persist `chat_id -> cwd`, replacing any existing override.
    pub fn set(&mut self, chat_id: &str, path: impl AsRef<Path>) -> Result<()> {
        validate_key("chat id", chat_id)?;
        let cwd = path_string(path.as_ref())?;
        self.chats
            .insert(chat_id.to_string(), ChatWorkspace::new(cwd));
        self.save()
    }

    /// Remove a chat's override. Returns whether an entry existed.
    ///
    /// The chat then falls back to [`Self::default_dir`].
    pub fn clear(&mut self, chat_id: &str) -> Result<bool> {
        if self.chats.remove(chat_id).is_none() {
            return Ok(false);
        }
        self.save()?;
        Ok(true)
    }

    /// Every explicit override, optionally filtered by chat-id prefix.
    pub fn list_cwds(&self, prefix: Option<&str>) -> BTreeMap<String, String> {
        self.chats
            .iter()
            .filter(|(chat_id, _)| prefix.is_none_or(|p| chat_id.starts_with(p)))
            .map(|(chat_id, entry)| (chat_id.clone(), entry.cwd.clone()))
            .collect()
    }

    /// Named workspace aliases (`/ws save`), copied like `listNamed()`.
    pub fn list_named(&self) -> BTreeMap<String, String> {
        self.named.clone()
    }

    /// Alias -> cwd.
    pub fn get_named(&self, name: &str) -> Option<&str> {
        self.named.get(name).map(String::as_str)
    }

    /// Create or replace a named alias.
    pub fn save_named(&mut self, name: &str, path: impl AsRef<Path>) -> Result<()> {
        validate_key("workspace name", name)?;
        let cwd = path_string(path.as_ref())?;
        self.named.insert(name.to_string(), cwd);
        self.save()
    }

    /// Remove a named alias. Returns whether it existed.
    pub fn remove_named(&mut self, name: &str) -> Result<bool> {
        if self.named.remove(name).is_none() {
            return Ok(false);
        }
        self.save()?;
        Ok(true)
    }

    /// No-op: mutations are written synchronously, so there is nothing to
    /// await. Kept for parity with the TypeScript store's shutdown path.
    pub fn flush(&self) -> Result<()> {
        Ok(())
    }

    fn load_at(path: PathBuf, default_dir: Option<String>) -> Result<Self> {
        let mut store = Self {
            path,
            default_dir,
            chats: BTreeMap::new(),
            named: BTreeMap::new(),
        };

        let bytes = match fs::read(&store.path) {
            Ok(bytes) => bytes,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(store),
            Err(err) => return Err(CoreError::io(&store.path, err)),
        };
        if bytes.iter().all(u8::is_ascii_whitespace) {
            return Ok(store);
        }

        let value: Value = match serde_json::from_slice(&bytes) {
            Ok(value) => value,
            Err(err) => {
                // A hand-edited/corrupt file must not take the bridge down;
                // the next mutation rewrites it atomically.
                tracing::warn!(
                    path = %store.path.display(),
                    error = %err,
                    "ignoring malformed workspaces file"
                );
                return Ok(store);
            }
        };
        store.ingest(&value);
        Ok(store)
    }

    /// Copy well-formed entries out of parsed JSON; drop everything else.
    fn ingest(&mut self, value: &Value) {
        let Some(root) = value.as_object() else {
            return;
        };

        if let Some(Value::Object(chats)) = root.get("chats") {
            for (chat_id, raw) in chats {
                let Some(cwd) = raw.get("cwd").and_then(Value::as_str) else {
                    continue;
                };
                if !is_valid_key(chat_id) || cwd.trim().is_empty() {
                    continue;
                }
                self.chats.insert(chat_id.clone(), ChatWorkspace::new(cwd));
            }
        }

        if let Some(Value::Object(named)) = root.get("named") {
            for (name, raw) in named {
                let Some(cwd) = raw.as_str() else {
                    continue;
                };
                if !is_valid_key(name) || cwd.trim().is_empty() {
                    continue;
                }
                self.named.insert(name.clone(), cwd.to_string());
            }
        }
    }

    fn save(&self) -> Result<()> {
        let data = WorkspaceData {
            chats: &self.chats,
            named: &self.named,
        };
        let mut bytes =
            serde_json::to_vec_pretty(&data).map_err(|e| CoreError::json(&self.path, e))?;
        bytes.push(b'\n');
        write_atomic(&self.path, &bytes, 0o600)
    }
}

fn validate_key(kind: &str, value: &str) -> Result<()> {
    if is_valid_key(value) {
        Ok(())
    } else {
        Err(CoreError::Message(format!("{kind} is empty")))
    }
}

fn is_valid_key(value: &str) -> bool {
    !value.trim().is_empty()
}

fn path_string(path: &Path) -> Result<String> {
    let value = path.to_str().ok_or_else(|| {
        CoreError::Message(format!(
            "workspace path is not valid utf-8: {}",
            path.display()
        ))
    })?;
    if value.trim().is_empty() {
        return Err(CoreError::Message("workspace path is empty".into()));
    }
    Ok(value.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn temp_path() -> (tempfile::TempDir, PathBuf) {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("workspaces.json");
        (dir, path)
    }

    fn read_json(path: &Path) -> Value {
        serde_json::from_slice(&fs::read(path).unwrap()).unwrap()
    }

    #[test]
    fn round_trips_chats_and_named_aliases() {
        let (_dir, path) = temp_path();
        let mut store = WorkspaceStore::load(&path).unwrap();
        assert!(store.path().ends_with("workspaces.json"));
        store.set("oc_1", "/work/one").unwrap();
        store.set("oc_2", "/work/two").unwrap();
        store.save_named("alias", "/work/one").unwrap();
        store.flush().unwrap();

        let reloaded = WorkspaceStore::load(&path).unwrap();
        assert_eq!(reloaded.cwd_for("oc_1"), Some("/work/one"));
        assert_eq!(reloaded.workspace_for("oc_2"), Some("/work/two"));
        assert_eq!(reloaded.get_named("alias"), Some("/work/one"));
        assert_eq!(
            reloaded.list_cwds(None),
            BTreeMap::from([
                ("oc_1".to_string(), "/work/one".to_string()),
                ("oc_2".to_string(), "/work/two".to_string()),
            ])
        );
        assert_eq!(
            reloaded.list_cwds(Some("oc_2")),
            BTreeMap::from([("oc_2".to_string(), "/work/two".to_string())])
        );
        assert_eq!(
            reloaded.list_named(),
            BTreeMap::from([("alias".to_string(), "/work/one".to_string())])
        );

        // Exact TS-compatible shape.
        let json = read_json(&path);
        assert_eq!(json["chats"]["oc_1"]["cwd"], "/work/one");
        assert_eq!(json["named"]["alias"], "/work/one");
        assert!(json.get("chats").unwrap().is_object());
        assert_eq!(json.as_object().unwrap().len(), 2);

        let text = fs::read_to_string(&path).unwrap();
        assert!(text.ends_with("}\n"));
    }

    #[test]
    fn default_fallback_and_per_chat_override() {
        let (_dir, path) = temp_path();
        let mut store = WorkspaceStore::load_with_default(&path, "/profile/default").unwrap();
        assert_eq!(store.default_dir(), Some("/profile/default"));
        assert_eq!(store.workspace_for("oc_1"), Some("/profile/default"));
        assert_eq!(store.cwd_for("oc_1"), None);

        store.set("oc_1", "/explicit").unwrap();
        assert_eq!(store.workspace_for("oc_1"), Some("/explicit"));
        assert_eq!(store.workspace_for("oc_2"), Some("/profile/default"));

        // The override survives a reload; the default is runtime-only.
        let reloaded = WorkspaceStore::load(&path).unwrap();
        assert_eq!(reloaded.cwd_for("oc_1"), Some("/explicit"));
        assert_eq!(reloaded.default_dir(), None);
        assert_eq!(reloaded.workspace_for("oc_1"), Some("/explicit"));
        assert_eq!(reloaded.workspace_for("oc_2"), None);

        assert!(store.clear("oc_1").unwrap());
        assert!(!store.clear("oc_1").unwrap());
        assert_eq!(store.workspace_for("oc_1"), Some("/profile/default"));
    }

    #[test]
    fn default_can_be_replaced_or_cleared() {
        let (_dir, path) = temp_path();
        let mut store = WorkspaceStore::load_with_default(&path, "/one").unwrap();
        store.set_default_dir("/two").unwrap();
        assert_eq!(store.workspace_for("missing"), Some("/two"));
        store.set_default_dir("").unwrap();
        assert_eq!(store.default_dir(), None);
        assert_eq!(store.workspace_for("missing"), None);

        let store = store.with_default_dir("/three").unwrap();
        assert_eq!(store.default_dir(), Some("/three"));
    }

    #[test]
    fn drops_invalid_entries_instead_of_failing() {
        let (_dir, path) = temp_path();
        fs::write(
            &path,
            r#"{
                "chats": {
                    "good": { "cwd": "/work/good", "extra": 1 },
                    "bad-type": { "cwd": 7 },
                    "empty-cwd": { "cwd": "   " },
                    "scalar": "/not-an-entry",
                    "": { "cwd": "/empty-key" }
                },
                "named": {
                    "ok": "/work/good",
                    "bad": 3,
                    "empty": ""
                },
                "unknown": { "whatever": true }
            }"#,
        )
        .unwrap();

        let store = WorkspaceStore::load(&path).unwrap();
        assert_eq!(store.list_cwds(None).len(), 1);
        assert_eq!(store.cwd_for("good"), Some("/work/good"));
        assert_eq!(store.entry("good").unwrap().cwd, "/work/good");
        assert_eq!(
            store.list_named(),
            BTreeMap::from([("ok".to_string(), "/work/good".to_string())])
        );
    }

    #[test]
    fn malformed_json_is_tolerated() {
        let (_dir, path) = temp_path();
        for garbage in ["not json", "{", "[]", "null", "{\"chats\": 3}"] {
            fs::write(&path, garbage).unwrap();
            let mut store = WorkspaceStore::load(&path).unwrap();
            assert!(store.list_cwds(None).is_empty(), "{garbage:?}");
            assert!(store.list_named().is_empty(), "{garbage:?}");
            // The next mutation repairs the file.
            store.set("oc_1", "/work").unwrap();
            assert_eq!(
                WorkspaceStore::load(&path).unwrap().cwd_for("oc_1"),
                Some("/work")
            );
        }
    }

    #[test]
    fn missing_file_is_an_empty_store() {
        let (_dir, path) = temp_path();
        let store = WorkspaceStore::load(&path).unwrap();
        assert!(store.list_cwds(None).is_empty());
        assert!(store.list_named().is_empty());
        assert!(!path.exists());
    }

    #[test]
    fn rejects_empty_keys_and_paths() {
        let (_dir, path) = temp_path();
        let mut store = WorkspaceStore::load(&path).unwrap();
        assert!(store.set("", "/work").is_err());
        assert!(store.set("oc_1", "").is_err());
        assert!(store.save_named(" ", "/work").is_err());
        assert!(store.save_named("alias", "").is_err());
        assert!(!path.exists());
    }

    #[test]
    fn atomic_save_leaves_no_temp_files() {
        let (dir, path) = temp_path();
        let mut store = WorkspaceStore::load(&path).unwrap();
        for i in 0..5 {
            store.set(&format!("oc_{i}"), format!("/work/{i}")).unwrap();
        }
        store.clear("oc_0").unwrap();
        store.save_named("alias", "/work/1").unwrap();
        store.remove_named("alias").unwrap();

        let leftovers: Vec<_> = fs::read_dir(dir.path())
            .unwrap()
            .filter_map(|entry| entry.ok())
            .filter(|entry| entry.file_name().to_string_lossy().contains(".tmp-"))
            .collect();
        assert!(leftovers.is_empty(), "leftover temp files: {leftovers:?}");

        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mode = fs::metadata(&path).unwrap().permissions().mode() & 0o777;
            assert_eq!(mode, 0o600);
        }
    }
}
