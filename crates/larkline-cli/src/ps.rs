//! Local process registry (`~/.larkline/registry/processes.json`).
//!
//! Ported from `src/runtime/registry.ts` and `src/cli/commands/ps.ts`.
//!
//! Each running `larkline run` appends an entry to the registry; the bridge's
//! exit handler removes it again. A crashed process leaves a stale entry
//! behind, so [`list`] annotates every entry with a liveness probe instead of
//! blindly reporting it as running.
//!
//! The on-disk shape is a hard compatibility contract with the TypeScript
//! bridge:
//!
//! ```json
//! {
//!   "entries": [
//!     {
//!       "id": "ab12",
//!       "pid": 12345,
//!       "appId": "cli_a1b2c3d4",
//!       "tenant": "feishu",
//!       "profileName": "claude",
//!       "agentKind": "claude",
//!       "configPath": "/home/u/.larkline/config.json",
//!       "startedAt": "2026-01-01T00:00:00.000Z",
//!       "version": "0.1.0",
//!       "botName": "小助手"
//!     }
//!   ]
//! }
//! ```
//!
//! `botName` is only filled in after the WebSocket handshake succeeds, so it
//! is optional. Entries that fail the shape check are dropped, exactly like
//! `isValidEntry` in the TypeScript module.
//!
//! Older versions kept the file at `~/.larkline/processes.json` (one level
//! above `registry/`); reads fall back to that location when the new file does
//! not exist.
//!
//! Liveness:
//!
//! * Unix: `kill(pid, 0)`; `EPERM` means the process exists but belongs to
//!   another user.
//! * Windows: a conservative `tasklist /FI "PID eq <pid>"` check; if the tool
//!   is unavailable the process is assumed alive.
//!
//! There are no write helpers here: the daemon itself is responsible for
//! registering/unregistering, and read-only views must never rewrite state.

use std::fs;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use anyhow::{Result, anyhow, bail};
use larkline_core::{AgentKind, TenantBrand};
use serde::{Deserialize, Serialize};

/// How long the CLI waits after `SIGTERM` before escalating to `SIGKILL`.
pub const DEFAULT_GRACE: Duration = Duration::from_secs(2);

/// One running bridge instance, as persisted by the daemon.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ProcessEntry {
    /// 4-char random hex id, stable for the process lifetime.
    pub id: String,
    /// OS process id (`process.pid` on the TypeScript side).
    pub pid: u32,
    /// Feishu/Lark app id (`cli_...`).
    pub app_id: String,
    /// Tenant brand: `feishu` or `lark`.
    pub tenant: TenantBrand,
    /// Profile the bridge runs for.
    pub profile_name: String,
    /// `claude` or `codex`.
    pub agent_kind: AgentKind,
    /// Path to the config file that was loaded.
    pub config_path: String,
    /// ISO-8601 start time (`new Date().toISOString()`).
    pub started_at: String,
    /// larkline version that registered the entry.
    pub version: String,
    /// Bot display name, filled in after the WebSocket handshake.
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "lenient_bot_name"
    )]
    pub bot_name: Option<String>,
}

/// The legacy validator ignored `botName` entirely; a non-string value must
/// not make an otherwise valid (and possibly live) entry disappear from `ps`.
fn lenient_bot_name<'de, D>(deserializer: D) -> std::result::Result<Option<String>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let value = Option::<serde_json::Value>::deserialize(deserializer)?;
    Ok(match value {
        Some(serde_json::Value::String(text)) => Some(text),
        _ => None,
    })
}

/// A registry entry plus its observed liveness.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ProcessRecord {
    pub entry: ProcessEntry,
    /// `true` when the pid is still alive at the time of the check.
    pub alive: bool,
}

/// How a process reacted to termination.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StopSignal {
    /// Exited after the graceful signal (SIGTERM / plain `taskkill`).
    Terminated,
    /// Had to be forced (SIGKILL / `taskkill /F`).
    Killed,
}

impl StopSignal {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Terminated => "terminated",
            Self::Killed => "killed",
        }
    }
}

impl std::fmt::Display for StopSignal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Result of [`kill`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KillOutcome {
    pub entry: ProcessEntry,
    pub signal: StopSignal,
}

// ---------------------------------------------------------------------------
// Registry paths and parsing
// ---------------------------------------------------------------------------

/// Default registry path: `<root>/registry/processes.json`, where `<root>` is
/// `$LARKLINE_HOME` or `~/.larkline`.
pub fn registry_file() -> PathBuf {
    registry_file_in(&larkline_core::paths::default_root_dir())
}

/// Registry path for an explicit state root (the CLI's `--home`).
pub fn registry_file_in(root_dir: &Path) -> PathBuf {
    root_dir.join("registry").join("processes.json")
}

/// Read + validate the registry without checking liveness.
///
/// Missing files, malformed JSON and entries with the wrong shape all yield an
/// empty list rather than an error: `ps` must never fail because a file was
/// torn or hand-edited.
pub fn read_registry_at(path: &Path) -> Vec<ProcessEntry> {
    if let Some(entries) = read_registry_file(path) {
        return entries;
    }
    if let Some(legacy) = legacy_registry_file(path) {
        if let Some(entries) = read_registry_file(&legacy) {
            return entries;
        }
    }
    Vec::new()
}

/// Parse registry JSON text. Pure and exported for tests.
pub fn parse_registry(text: &str) -> Vec<ProcessEntry> {
    let Ok(value) = serde_json::from_str::<serde_json::Value>(text) else {
        return Vec::new();
    };
    let Some(entries) = value.get("entries").and_then(|entries| entries.as_array()) else {
        return Vec::new();
    };
    entries
        .iter()
        .filter_map(|entry| serde_json::from_value::<ProcessEntry>(entry.clone()).ok())
        .collect()
}

fn read_registry_file(path: &Path) -> Option<Vec<ProcessEntry>> {
    match fs::read_to_string(path) {
        Ok(text) => Some(parse_registry(&text)),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => None,
        // Permission/IO errors are treated like the TypeScript reader's
        // catch-all: an empty registry, not a hard failure.
        Err(_) => Some(Vec::new()),
    }
}

/// `~/.larkline/processes.json` for `~/.larkline/registry/processes.json`.
fn legacy_registry_file(path: &Path) -> Option<PathBuf> {
    if path.file_name()?.to_str()? != "processes.json" {
        return None;
    }
    let parent = path.parent()?;
    if parent.file_name()?.to_str()? != "registry" {
        return None;
    }
    let legacy = parent.parent()?.join("processes.json");
    if path.exists() { None } else { Some(legacy) }
}

// ---------------------------------------------------------------------------
// Listing and target resolution
// ---------------------------------------------------------------------------

/// Every registry entry for the default root, annotated with liveness.
///
/// Dead entries are returned too (the CLI can display them as stale); filter
/// with [`ProcessRecord::alive`] when only live processes are wanted.
pub fn list() -> Vec<ProcessRecord> {
    list_from(&registry_file())
}

/// [`list`] for an explicit registry file (e.g. `AppPaths::user_registry_file`).
pub fn list_from(registry_file: &Path) -> Vec<ProcessRecord> {
    read_registry_at(registry_file)
        .into_iter()
        .map(|entry| ProcessRecord {
            alive: is_alive(entry.pid),
            entry,
        })
        .collect()
}

/// Resolve a target against the default registry: an exact entry id, or a
/// 1-based index in [`list`] order (the same order `ps` prints).
pub fn resolve(target: &str) -> Option<ProcessRecord> {
    resolve_from(&registry_file(), target)
}

/// [`resolve`] against an explicit registry file.
pub fn resolve_from(registry_file: &Path, target: &str) -> Option<ProcessRecord> {
    resolve_in(&list_from(registry_file), target)
}

fn resolve_in(records: &[ProcessRecord], target: &str) -> Option<ProcessRecord> {
    let target = target.trim();
    if let Some(found) = records.iter().find(|record| record.entry.id == target) {
        return Some(found.clone());
    }
    let index = leading_index(target)?;
    records.get(index.checked_sub(1)?).cloned()
}

/// Mimic `Number.parseInt(target, 10)`: leading ASCII digits only.
fn leading_index(target: &str) -> Option<usize> {
    let digits: String = target
        .trim_start()
        .chars()
        .take_while(|ch| ch.is_ascii_digit())
        .collect();
    if digits.is_empty() {
        return None;
    }
    digits.parse().ok()
}

// ---------------------------------------------------------------------------
// Liveness and termination
// ---------------------------------------------------------------------------

/// Liveness probe for a pid.
///
/// Unix uses `kill(pid, 0)`; `EPERM` proves the process exists. Windows uses
/// `tasklist` conservatively (unknown output or a missing tool means "alive");
/// other platforms assume alive because there is no portable probe.
pub fn is_alive(pid: u32) -> bool {
    #[cfg(unix)]
    {
        // SAFETY: kill(2) with signal 0 only performs the existence/permission
        // check; it has no memory-safety preconditions.
        let result = unsafe { libc::kill(pid as libc::pid_t, 0) };
        if result == 0 {
            return true;
        }
        std::io::Error::last_os_error().raw_os_error() == Some(libc::EPERM)
    }
    #[cfg(windows)]
    {
        windows_is_alive(pid)
    }
    #[cfg(not(any(unix, windows)))]
    {
        let _ = pid;
        true
    }
}

#[cfg(windows)]
fn windows_is_alive(pid: u32) -> bool {
    let filter = format!("PID eq {pid}");
    let output = std::process::Command::new("tasklist")
        .args(["/FI", filter.as_str(), "/NH", "/FO", "CSV"])
        .output();
    let Ok(output) = output else {
        // Conservative: never claim a process is dead just because the probe
        // tool failed.
        return true;
    };
    let text = String::from_utf8_lossy(&output.stdout);
    let mut saw_row = false;
    for line in text.lines() {
        let line = line.trim();
        if line.is_empty() || line.to_ascii_lowercase().starts_with("info:") {
            continue;
        }
        let mut fields = line.split(',');
        let _image_name = fields.next();
        let Some(raw_pid) = fields.next() else {
            continue;
        };
        saw_row = true;
        if raw_pid.trim().trim_matches('"') == pid.to_string() {
            return true;
        }
    }
    !saw_row
}

/// Find a registry entry by id or 1-based index and terminate it.
pub fn kill(target: &str) -> Result<KillOutcome> {
    kill_from_with_timeout(&registry_file(), target, DEFAULT_GRACE)
}

/// [`kill`] against an explicit registry file.
pub fn kill_from(registry_file: &Path, target: &str) -> Result<KillOutcome> {
    kill_from_with_timeout(registry_file, target, DEFAULT_GRACE)
}

/// [`kill_from`] with a custom grace period for both SIGTERM and SIGKILL.
pub fn kill_from_with_timeout(
    registry_file: &Path,
    target: &str,
    grace: Duration,
) -> Result<KillOutcome> {
    let record = resolve_from(registry_file, target)
        .ok_or_else(|| anyhow!("no larkline process matches `{target}`"))?;
    if !record.alive {
        bail!(
            "process {} (pid {}) is not running",
            record.entry.id,
            record.entry.pid
        );
    }
    let signal = terminate_pid_with_timeout(record.entry.pid, grace)?;
    Ok(KillOutcome {
        entry: record.entry,
        signal,
    })
}

/// Terminate a bare pid: SIGTERM, wait `DEFAULT_GRACE`, then SIGKILL.
pub fn terminate_pid(pid: u32) -> Result<StopSignal> {
    terminate_pid_with_timeout(pid, DEFAULT_GRACE)
}

/// [`terminate_pid`] with a custom grace period for both waits.
pub fn terminate_pid_with_timeout(pid: u32, grace: Duration) -> Result<StopSignal> {
    send_terminate(pid).map_err(|err| anyhow!("failed to send SIGTERM to pid {pid}: {err}"))?;
    if wait_until_dead(pid, grace) {
        return Ok(StopSignal::Terminated);
    }

    send_kill(pid).map_err(|err| anyhow!("failed to send SIGKILL to pid {pid}: {err}"))?;
    if wait_until_dead(pid, grace) {
        Ok(StopSignal::Killed)
    } else {
        bail!("process {pid} did not exit after SIGKILL")
    }
}

fn wait_until_dead(pid: u32, timeout: Duration) -> bool {
    let deadline = Instant::now() + timeout;
    loop {
        if !is_alive(pid) {
            return true;
        }
        if Instant::now() >= deadline {
            return false;
        }
        std::thread::sleep(Duration::from_millis(100));
    }
}

fn send_terminate(pid: u32) -> std::io::Result<()> {
    #[cfg(unix)]
    {
        signal_process(pid, libc::SIGTERM)
    }
    #[cfg(windows)]
    {
        taskkill(pid, false)
    }
    #[cfg(not(any(unix, windows)))]
    {
        let _ = pid;
        Err(std::io::Error::other(
            "process termination is not supported on this platform",
        ))
    }
}

fn send_kill(pid: u32) -> std::io::Result<()> {
    #[cfg(unix)]
    {
        signal_process(pid, libc::SIGKILL)
    }
    #[cfg(windows)]
    {
        taskkill(pid, true)
    }
    #[cfg(not(any(unix, windows)))]
    {
        let _ = pid;
        Err(std::io::Error::other(
            "process termination is not supported on this platform",
        ))
    }
}

#[cfg(unix)]
fn signal_process(pid: u32, signal: i32) -> std::io::Result<()> {
    // SAFETY: kill(2) has no memory-safety preconditions. The pid is cast to
    // pid_t; registry pids originate from the OS and fit.
    let result = unsafe { libc::kill(pid as libc::pid_t, signal) };
    if result == 0 {
        Ok(())
    } else {
        Err(std::io::Error::last_os_error())
    }
}

#[cfg(windows)]
fn taskkill(pid: u32, force: bool) -> std::io::Result<()> {
    let pid_arg = pid.to_string();
    let mut command = std::process::Command::new("taskkill");
    command.args(["/PID", pid_arg.as_str(), "/T"]);
    if force {
        command.arg("/F");
    }
    let status = command.status()?;
    if status.success() {
        Ok(())
    } else {
        Err(std::io::Error::other(format!(
            "taskkill exited with {status}"
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn write_registry(dir: &Path, relative: &str, body: &str) -> PathBuf {
        let path = dir.join(relative);
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        fs::write(&path, body).unwrap();
        path
    }

    fn entry(id: &str, pid: u32) -> String {
        format!(
            r#"{{
  "id": "{id}",
  "pid": {pid},
  "appId": "cli_a1b2c3d4",
  "tenant": "feishu",
  "profileName": "claude",
  "agentKind": "claude",
  "configPath": "/home/u/.larkline/config.json",
  "startedAt": "2026-01-01T00:00:00.000Z",
  "version": "0.1.0"
}}"#
        )
    }

    #[test]
    fn parses_valid_entries_and_optional_bot_name() {
        let dir = tempfile::tempdir().unwrap();
        let path = write_registry(
            dir.path(),
            "registry/processes.json",
            r#"{"entries": [
                {"id":"ab12","pid":123,"appId":"cli_x","tenant":"feishu","profileName":"claude","agentKind":"codex","configPath":"/c","startedAt":"2026-01-01T00:00:00.000Z","version":"0.1.0","botName":"小助手","unknown":"ignored"},
                {"id":"cd34","pid":456,"appId":"cli_y","tenant":"lark","profileName":"p","agentKind":"claude","configPath":"/c","startedAt":"2026-01-01T00:00:00.000Z","version":"0.1.0"},
                {"id":"ef56","pid":789,"appId":"cli_z","tenant":"lark","profileName":"p","agentKind":"claude","configPath":"/c","startedAt":"2026-01-01T00:00:00.000Z","version":"0.1.0","botName":42}
            ]}"#,
        );

        let entries = read_registry_at(&path);
        assert_eq!(entries.len(), 3);

        assert_eq!(entries[0].id, "ab12");
        assert_eq!(entries[0].bot_name.as_deref(), Some("小助手"));
        assert_eq!(entries[0].agent_kind, AgentKind::Codex);

        assert_eq!(entries[1].tenant, TenantBrand::Lark);
        assert_eq!(entries[1].bot_name, None);

        // A non-string botName degrades to None instead of dropping the entry.
        assert_eq!(entries[2].id, "ef56");
        assert_eq!(entries[2].bot_name, None);
    }

    #[test]
    fn drops_entries_with_the_wrong_shape() {
        let dir = tempfile::tempdir().unwrap();
        let path = write_registry(
            dir.path(),
            "registry/processes.json",
            r#"{"entries": [
                {"pid": 1},
                {"id":"aa","pid":"not-a-number","appId":"cli","tenant":"feishu","profileName":"p","agentKind":"claude","configPath":"/c","startedAt":"t","version":"v"},
                {"id":"bb","pid":2,"appId":"cli","tenant":"wechat","profileName":"p","agentKind":"claude","configPath":"/c","startedAt":"t","version":"v"},
                {"id":"cc","pid":3,"appId":"cli","tenant":"feishu","profileName":"p","agentKind":"gpt","configPath":"/c","startedAt":"t","version":"v"},
                {"id":"dd","pid":4,"appId":"cli","tenant":"feishu","profileName":"p","agentKind":"claude","configPath":"/c","startedAt":"t"},
                {"id":"ee","pid":5,"appId":"cli","tenant":"feishu","profileName":"p","agentKind":"claude","configPath":"/c","startedAt":"t","version":"v"}
            ]}"#,
        );

        let entries = read_registry_at(&path);
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].id, "ee");
    }

    #[test]
    fn missing_or_malformed_registry_is_empty() {
        let dir = tempfile::tempdir().unwrap();
        let missing = dir.path().join("registry/processes.json");
        assert!(read_registry_at(&missing).is_empty());

        let malformed = write_registry(dir.path(), "registry/processes.json", "not json");
        assert!(read_registry_at(&malformed).is_empty());

        fs::write(&malformed, "[]").unwrap();
        assert!(read_registry_at(&malformed).is_empty());

        fs::write(&malformed, r#"{"entries": {}}"#).unwrap();
        assert!(read_registry_at(&malformed).is_empty());
    }

    #[test]
    fn falls_back_to_the_legacy_registry_location() {
        let dir = tempfile::tempdir().unwrap();
        let legacy = dir.path().join("processes.json");
        fs::write(
            &legacy,
            format!(r#"{{"entries": [{}]}}"#, entry("ab12", 123)),
        )
        .unwrap();
        let current = dir.path().join("registry/processes.json");

        let entries = read_registry_at(&current);
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].id, "ab12");
    }

    #[test]
    fn current_registry_wins_over_legacy() {
        let dir = tempfile::tempdir().unwrap();
        fs::write(
            dir.path().join("processes.json"),
            format!(r#"{{"entries": [{}]}}"#, entry("old1", 1)),
        )
        .unwrap();
        let current = write_registry(
            dir.path(),
            "registry/processes.json",
            &format!(r#"{{"entries": [{}]}}"#, entry("new1", 2)),
        );

        let entries = read_registry_at(&current);
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].id, "new1");
    }

    #[test]
    fn registry_path_layout() {
        assert_eq!(
            registry_file_in(Path::new("/home/u/.larkline")),
            PathBuf::from("/home/u/.larkline/registry/processes.json")
        );
    }

    #[cfg(unix)]
    #[test]
    fn list_marks_liveness() {
        let dir = tempfile::tempdir().unwrap();
        let path = write_registry(
            dir.path(),
            "registry/processes.json",
            &format!(
                r#"{{"entries": [{}, {}]}}"#,
                entry("live", std::process::id()),
                entry("dead", 999_999_999)
            ),
        );

        let records = list_from(&path);
        assert_eq!(records.len(), 2);
        assert!(records[0].alive);
        assert_eq!(records[0].entry.id, "live");
        assert!(!records[1].alive);
        assert_eq!(records[1].entry.id, "dead");
        assert!(is_alive(std::process::id()));
        assert!(!is_alive(999_999_999));
    }

    fn record(id: &str, pid: u32, alive: bool) -> ProcessRecord {
        ProcessRecord {
            entry: ProcessEntry {
                id: id.to_string(),
                pid,
                app_id: "cli".to_string(),
                tenant: TenantBrand::Feishu,
                profile_name: "claude".to_string(),
                agent_kind: AgentKind::Claude,
                config_path: "/c".to_string(),
                started_at: "t".to_string(),
                version: "v".to_string(),
                bot_name: None,
            },
            alive,
        }
    }

    #[test]
    fn resolves_by_id_then_one_based_index() {
        let records = vec![
            record("aa", 1, true),
            record("bb", 2, true),
            record("cc", 3, false),
        ];

        assert_eq!(resolve_in(&records, "bb").unwrap().entry.id, "bb");
        assert_eq!(resolve_in(&records, "2").unwrap().entry.id, "bb");
        assert_eq!(resolve_in(&records, "3").unwrap().entry.id, "cc");
        assert!(resolve_in(&records, "0").is_none());
        assert!(resolve_in(&records, "9").is_none());
        assert!(resolve_in(&records, "zz").is_none());
        assert!(resolve_in(&records, "").is_none());

        // Id lookup wins over index lookup.
        let records = vec![record("2", 1, true), record("bb", 2, true)];
        assert_eq!(resolve_in(&records, "2").unwrap().entry.id, "2");

        // `parseInt`-style prefix parsing: "2abc" resolves as index 2.
        assert_eq!(resolve_in(&records, "2abc").unwrap().entry.id, "bb");
    }

    #[test]
    fn stop_signal_display() {
        assert_eq!(StopSignal::Terminated.as_str(), "terminated");
        assert_eq!(StopSignal::Killed.to_string(), "killed");
    }
}
