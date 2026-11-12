//! Writer for the shared process registry.
//!
//! [`crate::ps`] is the read side of `<root>/registry/processes.json`; this
//! module is the write side used by `larkline run`. The on-disk shape and the
//! pruning rules mirror `src/runtime/registry.ts`:
//!
//! * a mutating call takes an advisory lock next to the registry file, reads
//!   the current entries, drops entries whose pid is gone, then writes the
//!   result through a temp file + rename;
//! * `botName` is optional and only present after the WebSocket handshake.
//!   `lark-channel` 0.6 exposes no bot-identity accessor, so Rust entries
//!   currently never set it (see [`Registration::update_bot_name`]);
//! * the number of entries is capped ([`MAX_ENTRIES`]). Stale entries are
//!   removed first; if the cap is still exceeded, the oldest entries (the
//!   front of the file) are dropped.
//!
//! Unlike the TypeScript registry, staleness is a pid liveness probe rather
//! than runtime-lock inspection: the Rust `run` command does not acquire the
//! bridge runtime locks yet, so no lock metadata exists for its entries.
//!
//! Cross-language note: the TypeScript side holds `processes.json.lock` as a
//! `proper-lockfile` *directory*, while [`larkline_core::lock::FileLock`]
//! locks a regular file at the same path. A write from one implementation can
//! therefore not interleave with the other, but the two lock representations
//! are not interchangeable while both processes are inside a critical section.
//! Writes are short and both sides tolerate a malformed/absent file.

use std::ffi::OsString;
use std::path::{Path, PathBuf};
use std::time::Duration;

use anyhow::{Context, Result};
use chrono::{SecondsFormat, Utc};
use larkline_core::lock::FileLock;
use larkline_core::{AgentKind, TenantBrand};
use serde::Serialize;

use crate::ps::{self, ProcessEntry};

/// Maximum number of entries kept in the registry.
///
/// A machine only ever runs a handful of bridges. The cap is a guard against
/// a corrupt or hand-edited file growing without bound, so it is deliberately
/// far above any realistic fleet size.
pub const MAX_ENTRIES: usize = 64;

/// How long a writer waits for the registry lock.
pub const LOCK_TIMEOUT: Duration = Duration::from_secs(5);

/// Everything needed to build a [`ProcessEntry`] except process-local values.
#[derive(Debug, Clone)]
pub struct RegisterOptions {
    pub app_id: String,
    pub tenant: TenantBrand,
    pub profile_name: String,
    pub agent_kind: AgentKind,
    pub config_path: PathBuf,
    pub version: String,
}

/// Handle for one registered process.
///
/// `unregister` is best-effort on the shutdown path, but callers must call it:
/// a crash or SIGKILL leaves the entry behind and only the next writer prunes
/// it.
#[derive(Debug, Clone)]
pub struct Registration {
    entry: ProcessEntry,
    registry_file: PathBuf,
}

impl Registration {
    /// Prune stale entries and append an entry for this process.
    ///
    /// Atomic with respect to other Rust writers: the lock is held for the
    /// read/prune/write cycle.
    pub fn register(root_dir: &Path, options: RegisterOptions) -> Result<Self> {
        let registry_file = ps::registry_file_in(root_dir);
        let lock_path = lock_path(&registry_file);
        let _lock = FileLock::acquire(&lock_path, LOCK_TIMEOUT)
            .with_context(|| format!("cannot lock {}", lock_path.display()))?;

        let entry = ProcessEntry {
            id: generate_id(),
            pid: std::process::id(),
            app_id: options.app_id,
            tenant: options.tenant,
            profile_name: options.profile_name,
            agent_kind: options.agent_kind,
            config_path: options.config_path.display().to_string(),
            started_at: now_iso8601(),
            version: options.version,
            bot_name: None,
        };

        let existing = ps::read_registry_at(&registry_file);
        let entries = merge_entries(existing, entry.clone(), ps::is_alive, MAX_ENTRIES);
        write_entries(&registry_file, &entries)?;

        Ok(Self {
            entry,
            registry_file,
        })
    }

    /// This process's short id.
    pub fn id(&self) -> &str {
        &self.entry.id
    }

    /// Fill in `botName` once the bridge knows it.
    ///
    /// Kept for parity with `updateEntry` in the TypeScript registry. The
    /// current `lark-channel` SDK has no bot-identity accessor, so no caller
    /// invokes it from `run`; the field stays absent, exactly as it did for
    /// older TypeScript bridges. Unknown ids are a no-op.
    #[allow(dead_code)] // hook for when the SDK exposes a bot identity
    pub fn update_bot_name(&self, bot_name: &str) -> Result<()> {
        let lock_path = lock_path(&self.registry_file);
        let _lock = FileLock::acquire(&lock_path, LOCK_TIMEOUT)
            .with_context(|| format!("cannot lock {}", lock_path.display()))?;

        let raw = ps::read_registry_at(&self.registry_file);
        let total = raw.len();
        let live = prune_entries(raw, ps::is_alive);
        let pruned = live.len() != total;
        let (entries, changed) = set_bot_name(live, &self.entry.id, bot_name);
        if changed || pruned {
            write_entries(&self.registry_file, &entries)?;
        }
        Ok(())
    }

    /// Remove this process's entry (and prune dead ones while we are here).
    ///
    /// A missing entry is not an error: a concurrent process may already have
    /// pruned it.
    pub fn unregister(&self) -> Result<()> {
        let lock_path = lock_path(&self.registry_file);
        let _lock = FileLock::acquire(&lock_path, LOCK_TIMEOUT)
            .with_context(|| format!("cannot lock {}", lock_path.display()))?;

        let raw = ps::read_registry_at(&self.registry_file);
        let total = raw.len();
        let had_entry = raw
            .iter()
            .any(|entry| entry.id == self.entry.id && entry.pid == self.entry.pid);
        let live = prune_entries(raw, ps::is_alive);
        let pruned = live.len() != total;
        let entries = remove_entry(live, &self.entry.id, self.entry.pid);
        if had_entry || pruned {
            write_entries(&self.registry_file, &entries)?;
        }
        Ok(())
    }
}

/// Generate a 4-character lowercase hex id, like `randomBytes(2).toString('hex')`.
pub fn generate_id() -> String {
    let mut bytes = [0u8; 2];
    if getrandom::fill(&mut bytes).is_err() {
        // Randomness failure must not prevent the bridge from starting. The
        // fallback only needs to be unique enough for a handful of local
        // processes; dead entries are pruned on every write anyway.
        let nanos = Utc::now().timestamp_nanos_opt().unwrap_or_default() as u64;
        let mixed = nanos ^ (std::process::id() as u64) << 32;
        bytes = [(mixed >> 8) as u8, mixed as u8];
    }
    format!("{:02x}{:02x}", bytes[0], bytes[1])
}

/// ISO-8601 with milliseconds and a `Z` suffix, matching
/// `new Date().toISOString()`.
fn now_iso8601() -> String {
    Utc::now().to_rfc3339_opts(SecondsFormat::Millis, true)
}

// ---------------------------------------------------------------------------
// Pure registry-state helpers
// ---------------------------------------------------------------------------

/// Drop entries whose process is no longer alive.
pub fn prune_entries(entries: Vec<ProcessEntry>, alive: impl Fn(u32) -> bool) -> Vec<ProcessEntry> {
    entries
        .into_iter()
        .filter(|entry| alive(entry.pid))
        .collect()
}

/// Prepare the entry list for a new registration.
///
/// 1. prune dead pids;
/// 2. drop any earlier entry for the same pid (this process's own previous
///    registration; a live pid cannot belong to two processes);
/// 3. enforce the cap, oldest first;
/// 4. append `incoming`.
///
/// Existing entries are never matched by id: a freshly generated random id
/// colliding with a live process must not evict that process. Resolution by id
/// then has the same first-match behaviour as the TypeScript registry.
pub fn merge_entries(
    existing: Vec<ProcessEntry>,
    incoming: ProcessEntry,
    alive: impl Fn(u32) -> bool,
    max_entries: usize,
) -> Vec<ProcessEntry> {
    let max_entries = max_entries.max(1);
    let mut entries: Vec<ProcessEntry> = existing
        .into_iter()
        .filter(|entry| alive(entry.pid))
        .filter(|entry| entry.pid != incoming.pid)
        .collect();

    if entries.len() + 1 > max_entries {
        let drop_count = entries.len() + 1 - max_entries;
        entries.drain(..drop_count);
    }
    entries.push(incoming);
    entries
}

/// Remove this process's entry, preserving the order of the others.
///
/// Matching both id and pid means an unlucky 4-hex id collision cannot delete
/// another live process from the list.
pub fn remove_entry(entries: Vec<ProcessEntry>, id: &str, pid: u32) -> Vec<ProcessEntry> {
    entries
        .into_iter()
        .filter(|entry| entry.id != id || entry.pid != pid)
        .collect()
}

/// Set `botName` on the entry with `id`.
///
/// Returns the (possibly unchanged) entries and whether a write is needed.
#[allow(dead_code)] // only `update_bot_name` calls this until the SDK exposes an identity
pub fn set_bot_name(
    mut entries: Vec<ProcessEntry>,
    id: &str,
    bot_name: &str,
) -> (Vec<ProcessEntry>, bool) {
    let mut changed = false;
    for entry in &mut entries {
        if entry.id == id && entry.bot_name.as_deref() != Some(bot_name) {
            entry.bot_name = Some(bot_name.to_string());
            changed = true;
        }
    }
    (entries, changed)
}

// ---------------------------------------------------------------------------
// I/O
// ---------------------------------------------------------------------------

/// `processes.json.lock`, the same path `proper-lockfile` uses on the
/// TypeScript side.
fn lock_path(registry_file: &Path) -> PathBuf {
    let mut raw = OsString::from(registry_file.as_os_str());
    raw.push(".lock");
    PathBuf::from(raw)
}

#[derive(Serialize)]
struct RegistryFile<'a> {
    entries: &'a [ProcessEntry],
}

fn write_entries(registry_file: &Path, entries: &[ProcessEntry]) -> Result<()> {
    let body = RegistryFile { entries };
    let mut bytes =
        serde_json::to_vec_pretty(&body).context("cannot serialize the process registry")?;
    bytes.push(b'\n');
    larkline_core::atomic::write_atomic(registry_file, &bytes, 0o600)
        .with_context(|| format!("cannot write {}", registry_file.display()))
}

#[cfg(test)]
mod tests {
    use std::fs;

    use chrono::DateTime;

    use super::*;

    fn entry(id: &str, pid: u32) -> ProcessEntry {
        ProcessEntry {
            id: id.to_string(),
            pid,
            app_id: "cli_a1b2c3d4".to_string(),
            tenant: TenantBrand::Feishu,
            profile_name: "claude".to_string(),
            agent_kind: AgentKind::Claude,
            config_path: "/home/u/.larkline/config.json".to_string(),
            started_at: "2026-01-01T00:00:00.000Z".to_string(),
            version: "0.1.0".to_string(),
            bot_name: None,
        }
    }

    fn options() -> RegisterOptions {
        RegisterOptions {
            app_id: "cli_test".to_string(),
            tenant: TenantBrand::Lark,
            profile_name: "claude".to_string(),
            agent_kind: AgentKind::Codex,
            config_path: PathBuf::from("/home/u/.larkline/config.json"),
            version: "9.9.9".to_string(),
        }
    }

    #[test]
    fn generated_ids_are_four_lowercase_hex_digits() {
        for _ in 0..32 {
            let id = generate_id();
            assert_eq!(id.len(), 4, "unexpected id {id:?}");
            assert!(
                id.chars()
                    .all(|ch| ch.is_ascii_hexdigit() && !ch.is_ascii_uppercase()),
                "unexpected id {id:?}"
            );
        }
    }

    #[test]
    fn lock_path_sits_next_to_the_registry_file() {
        assert_eq!(
            lock_path(Path::new("/home/u/.larkline/registry/processes.json")),
            PathBuf::from("/home/u/.larkline/registry/processes.json.lock")
        );
    }

    #[test]
    fn prune_drops_dead_pids_and_keeps_order() {
        let entries = vec![entry("aa", 1), entry("bb", 2), entry("cc", 3)];
        let live = prune_entries(entries, |pid| pid != 2);
        assert_eq!(
            live.iter().map(|e| e.id.as_str()).collect::<Vec<_>>(),
            ["aa", "cc"]
        );
    }

    #[test]
    fn merge_prunes_replaces_and_appends() {
        let existing = vec![entry("dead", 10), entry("me", 20), entry("other", 30)];
        let incoming = entry("new1", 20);
        let merged = merge_entries(existing, incoming, |pid| pid != 10, MAX_ENTRIES);

        // Dead pid pruned, the old entry for our pid replaced, order preserved.
        assert_eq!(
            merged.iter().map(|e| e.id.as_str()).collect::<Vec<_>>(),
            ["other", "new1"]
        );
    }

    #[test]
    fn merge_never_evicts_a_live_entry_that_shares_the_new_id() {
        let existing = vec![entry("dup", 10), entry("other", 11)];
        let incoming = entry("dup", 12);
        let merged = merge_entries(existing, incoming, |_| true, MAX_ENTRIES);

        assert_eq!(
            merged.iter().map(|e| e.id.as_str()).collect::<Vec<_>>(),
            ["dup", "other", "dup"]
        );
    }

    #[test]
    fn merge_enforces_the_entry_cap_oldest_first() {
        let existing = (0..5)
            .map(|index| entry(&format!("id{index}"), 100 + index))
            .collect();
        let merged = merge_entries(existing, entry("new1", 999), |_| true, 3);

        assert_eq!(merged.len(), 3);
        assert_eq!(
            merged.iter().map(|e| e.id.as_str()).collect::<Vec<_>>(),
            ["id3", "id4", "new1"]
        );
    }

    #[test]
    fn merge_with_a_zero_cap_still_keeps_the_new_entry() {
        let merged = merge_entries(Vec::new(), entry("new1", 1), |_| true, 0);
        assert_eq!(merged.len(), 1);
        assert_eq!(merged[0].id, "new1");
    }

    #[test]
    fn remove_keeps_the_other_entries() {
        let entries = vec![entry("aa", 1), entry("bb", 2), entry("cc", 3)];
        let kept = remove_entry(entries, "bb", 2);
        assert_eq!(
            kept.iter().map(|e| e.id.as_str()).collect::<Vec<_>>(),
            ["aa", "cc"]
        );
    }

    #[test]
    fn remove_does_not_delete_a_colliding_id_from_another_pid() {
        let entries = vec![entry("dup", 1), entry("dup", 2)];
        let kept = remove_entry(entries, "dup", 2);
        assert_eq!(kept.len(), 1);
        assert_eq!(kept[0].pid, 1);
    }

    #[test]
    fn set_bot_name_reports_whether_a_write_is_needed() {
        let entries = vec![entry("aa", 1), entry("bb", 2)];

        let (updated, changed) = set_bot_name(entries, "aa", "小助手");
        assert!(changed);
        assert_eq!(updated[0].bot_name.as_deref(), Some("小助手"));
        assert_eq!(updated[1].bot_name, None);

        let (unchanged, changed) = set_bot_name(updated, "aa", "小助手");
        assert!(!changed);
        assert_eq!(unchanged[0].bot_name.as_deref(), Some("小助手"));

        let (unknown, changed) = set_bot_name(unchanged, "zz", "nobody");
        assert!(!changed);
        assert_eq!(unknown.len(), 2);
    }

    #[test]
    fn register_update_and_unregister_round_trip_through_a_temp_file() {
        let dir = tempfile::tempdir().unwrap();
        let registration = Registration::register(dir.path(), options()).unwrap();

        let registry_file = ps::registry_file_in(dir.path());

        let entries = ps::read_registry_at(&registry_file);
        assert_eq!(entries.len(), 1);
        let saved = &entries[0];
        assert_eq!(saved.id, registration.id());
        assert_eq!(saved.pid, std::process::id());
        assert_eq!(saved.app_id, "cli_test");
        assert_eq!(saved.tenant, TenantBrand::Lark);
        assert_eq!(saved.profile_name, "claude");
        assert_eq!(saved.agent_kind, AgentKind::Codex);
        assert_eq!(saved.config_path, "/home/u/.larkline/config.json");
        assert_eq!(saved.version, "9.9.9");
        assert_eq!(saved.bot_name, None);
        assert!(DateTime::parse_from_rfc3339(&saved.started_at).is_ok());

        // The on-disk JSON must stay camelCase for the TypeScript reader.
        let text = fs::read_to_string(&registry_file).unwrap();
        assert!(text.contains("\"appId\""));
        assert!(text.contains("\"startedAt\""));
        assert!(!text.contains("\"app_id\""));
        assert!(!text.contains("\"botName\""));
        assert!(text.ends_with('\n'));

        registration.update_bot_name("小助手").unwrap();
        let entries = ps::read_registry_at(&registry_file);
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].bot_name.as_deref(), Some("小助手"));

        registration.unregister().unwrap();
        assert!(ps::read_registry_at(&registry_file).is_empty());
    }

    #[cfg(unix)]
    #[test]
    fn register_prunes_dead_entries_from_the_file() {
        let dir = tempfile::tempdir().unwrap();
        let registry_file = ps::registry_file_in(dir.path());
        fs::create_dir_all(registry_file.parent().unwrap()).unwrap();
        let body = serde_json::json!({
            "entries": [
                {
                    "id": "dead",
                    "pid": 999_999_999u32,
                    "appId": "cli_dead",
                    "tenant": "feishu",
                    "profileName": "claude",
                    "agentKind": "claude",
                    "configPath": "/c",
                    "startedAt": "2026-01-01T00:00:00.000Z",
                    "version": "0.1.0"
                }
            ]
        });
        fs::write(&registry_file, serde_json::to_vec(&body).unwrap()).unwrap();

        let registration = Registration::register(dir.path(), options()).unwrap();
        let entries = ps::read_registry_at(&registry_file);
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].id, registration.id());
        assert!(!ps::is_alive(999_999_999));
    }

    #[test]
    fn unregister_leaves_foreign_entries_alone() {
        let dir = tempfile::tempdir().unwrap();
        let registration = Registration::register(dir.path(), options()).unwrap();
        let registry_file = ps::registry_file_in(dir.path());

        // Same pid, different id: `unregister` must filter by id only. (The
        // register-time merge is what replaces an older entry for this pid.)
        let foreign = entry("keep", std::process::id());
        let both = vec![registration.entry.clone(), foreign];
        let body = RegistryFile { entries: &both };
        fs::write(&registry_file, serde_json::to_vec(&body).unwrap()).unwrap();

        registration.unregister().unwrap();

        let entries = ps::read_registry_at(&registry_file);
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].id, "keep");
    }
}
