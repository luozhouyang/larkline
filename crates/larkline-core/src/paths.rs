//! On-disk layout.
//!
//! The layout is a hard compatibility contract with the TypeScript bridge:
//! an upgraded install must find its profiles, sessions and secrets in the
//! same places. See `src/config/app-paths.ts` for the original definition.
//!
//! ```text
//! ~/.larkline/
//!   config.json                  # root config: activeProfile + profiles{}
//!   profiles/<name>/
//!     sessions.json
//!     workspaces.json
//!     callback-nonces.json
//!     secrets.enc
//!     .keystore.salt
//!     media/
//!     logs/
//!     lark-cli/
//!   registry/
//!     processes.json
//!     locks/
//! ```

use std::path::{Path, PathBuf};

use crate::env;
use crate::error::{CoreError, Result};

pub const DEFAULT_PROFILE: &str = "claude";

#[derive(Debug, Clone)]
pub struct AppPaths {
    pub root_dir: PathBuf,
    pub profile: String,
    pub profile_dir: PathBuf,
    pub default_workspace_dir: PathBuf,
    pub config_file: PathBuf,
    pub active_profile_file: PathBuf,
    pub sessions_file: PathBuf,
    pub workspaces_file: PathBuf,
    /// Used/revoked card-callback nonces. TS writes this next to
    /// `sessions.json` (`dirname(mediaDir)/callback-nonces.json`).
    pub callback_nonces_file: PathBuf,
    pub secrets_file: PathBuf,
    pub keystore_salt_file: PathBuf,
    pub secrets_getter_script: PathBuf,
    pub lark_cli_config_dir: PathBuf,
    pub lark_cli_source_dir: PathBuf,
    pub lark_cli_source_config_file: PathBuf,
    pub lark_cli_target_config_file: PathBuf,
    pub media_dir: PathBuf,
    pub logs_dir: PathBuf,
    pub host_logs_dir: PathBuf,
    pub host_lock_file: PathBuf,
    pub registry_dir: PathBuf,
    pub user_registry_file: PathBuf,
    pub user_lock_dir: PathBuf,
    pub profile_lock_file: PathBuf,
    pub ui_sidecar_file: PathBuf,
}

impl AppPaths {
    /// Resolve every path for `profile` under `root`.
    pub fn resolve(root: impl Into<PathBuf>, profile: &str) -> Result<Self> {
        let root_dir = root.into();
        let profile = normalize_profile_name(profile)?;
        let profile_dir = root_dir.join("profiles").join(&profile);
        let registry_dir = root_dir.join("registry");
        let user_lock_dir = registry_dir.join("locks");

        Ok(Self {
            default_workspace_dir: PathBuf::from(format!("{}-workspaces", root_dir.display()))
                .join(&profile)
                .join("default"),
            config_file: root_dir.join("config.json"),
            active_profile_file: root_dir.join("active-profile"),
            sessions_file: profile_dir.join("sessions.json"),
            workspaces_file: profile_dir.join("workspaces.json"),
            callback_nonces_file: profile_dir.join("callback-nonces.json"),
            secrets_file: profile_dir.join("secrets.enc"),
            keystore_salt_file: profile_dir.join(".keystore.salt"),
            secrets_getter_script: root_dir.join("secrets-getter"),
            lark_cli_config_dir: profile_dir.join("lark-cli"),
            lark_cli_source_dir: profile_dir.join("lark-cli-source"),
            lark_cli_source_config_file: profile_dir.join("lark-cli-source").join("config.json"),
            // `lark-channel` is the *source identifier* the external lark-cli
            // accepts via `config bind --source`; it is a protocol name, not
            // this project's name, so it never follows a project rename.
            lark_cli_target_config_file: profile_dir
                .join("lark-cli")
                .join("lark-channel")
                .join("config.json"),
            media_dir: profile_dir.join("media"),
            logs_dir: profile_dir.join("logs"),
            host_logs_dir: root_dir.join("logs"),
            host_lock_file: user_lock_dir.join("supervisor.lock"),
            user_registry_file: registry_dir.join("processes.json"),
            profile_lock_file: user_lock_dir
                .join("profile")
                .join(format!("{profile}.lock")),
            ui_sidecar_file: root_dir.join("ui.json"),
            registry_dir,
            user_lock_dir,
            profile_dir,
            profile,
            root_dir,
        })
    }

    /// Resolve using `LARKLINE_HOME`/`LARK_CHANNEL_HOME` and the default root.
    pub fn for_profile(profile: &str) -> Result<Self> {
        Self::resolve(default_root_dir(), profile)
    }

    pub fn app_lock_file(&self, app_id: &str) -> PathBuf {
        self.user_lock_dir
            .join("app")
            .join(format!("{}.lock", lock_safe_name(app_id)))
    }
}

/// Resolve the state root without knowing the profile yet.
///
/// Ordering matches the TypeScript bridge: explicit env override, then an
/// existing `~/.larkline`, then the legacy `~/.lark-channel`, then the new
/// directory (which the caller will create).
pub fn default_root_dir() -> PathBuf {
    if let Some(explicit) = env::home_override() {
        return PathBuf::from(explicit);
    }
    let home = dirs::home_dir().unwrap_or_else(|| PathBuf::from("."));
    let current = home.join(".larkline");
    if current.exists() {
        return current;
    }
    let legacy = home.join(".lark-channel");
    if legacy.exists() {
        return legacy;
    }
    current
}

/// Validate a profile name: it must be usable as a single path segment.
///
/// Unicode letters are allowed on purpose so a non-ASCII bot name can be used
/// directly. Whitespace, path separators, control characters and the Windows
/// reserved set are rejected.
pub fn normalize_profile_name(raw: &str) -> Result<String> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return Err(CoreError::InvalidProfileName(raw.to_string()));
    }
    if trimmed == "." || trimmed == ".." {
        return Err(CoreError::InvalidProfileName(raw.to_string()));
    }
    let bad = trimmed.chars().any(|c| {
        c.is_control()
            || c.is_whitespace()
            || matches!(c, '/' | '\\' | ':' | '*' | '?' | '"' | '<' | '>' | '|')
    });
    if bad {
        return Err(CoreError::InvalidProfileName(raw.to_string()));
    }
    Ok(trimmed.to_string())
}

fn lock_safe_name(value: &str) -> String {
    value
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || matches!(c, '.' | '_' | '-') {
                c
            } else {
                '_'
            }
        })
        .collect()
}

/// Best-effort legacy `~/.lark-channel` -> `~/.larkline` rename.
///
/// The TypeScript launcher (`bin/larkline.mjs`) did this before the CLI loaded
/// its path helpers. The Rust binary keeps the same behaviour in `main()` so
/// an upgrade needs no manual step.
pub fn migrate_legacy_home() -> std::io::Result<bool> {
    if env::home_override().is_some() {
        return Ok(false);
    }
    let home = match dirs::home_dir() {
        Some(home) => home,
        None => return Ok(false),
    };
    let current = home.join(".larkline");
    let legacy = home.join(".lark-channel");
    if current.exists() || !legacy.exists() {
        return Ok(false);
    }
    std::fs::rename(&legacy, &current)?;
    let legacy_workspaces = home.join(".lark-channel-workspaces");
    let current_workspaces = home.join(".larkline-workspaces");
    if legacy_workspaces.exists() && !current_workspaces.exists() {
        // Best effort: a failure here must not abort the upgrade.
        let _ = std::fs::rename(&legacy_workspaces, &current_workspaces);
    }
    Ok(true)
}

pub fn is_dir_writable(path: &Path) -> bool {
    if path.is_dir() {
        return true;
    }
    path.parent().map(Path::is_dir).unwrap_or(false)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn accepts_unicode_names() {
        assert_eq!(normalize_profile_name("小助手").unwrap(), "小助手");
        assert_eq!(normalize_profile_name("bot-2").unwrap(), "bot-2");
    }

    #[test]
    fn rejects_path_traversal() {
        for bad in ["..", ".", "a/b", "a\\b", "a b", "a:b", "a\0b"] {
            assert!(normalize_profile_name(bad).is_err(), "{bad:?} should fail");
        }
    }

    #[test]
    fn layout_matches_typescript() {
        let p = AppPaths::resolve("/home/u/.larkline", "claude").unwrap();
        assert_eq!(
            p.config_file,
            PathBuf::from("/home/u/.larkline/config.json")
        );
        assert_eq!(
            p.secrets_file,
            PathBuf::from("/home/u/.larkline/profiles/claude/secrets.enc")
        );
        assert_eq!(
            p.callback_nonces_file,
            PathBuf::from("/home/u/.larkline/profiles/claude/callback-nonces.json")
        );
        assert_eq!(
            p.default_workspace_dir,
            PathBuf::from("/home/u/.larkline-workspaces/claude/default")
        );
        assert_eq!(
            p.lark_cli_target_config_file,
            PathBuf::from("/home/u/.larkline/profiles/claude/lark-cli/lark-channel/config.json")
        );
        assert_eq!(
            p.app_lock_file("cli_abc/def"),
            PathBuf::from("/home/u/.larkline/registry/locks/app/cli_abc_def.lock")
        );
    }
}
