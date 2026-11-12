//! Root config persistence and profile management.
//!
//! `config.json` is a single file holding `activeProfile` and a map of every
//! profile (schema v2). Writes go through the config lock + atomic replace so
//! the Rust binary and the still-running TypeScript bridge cannot corrupt it.

use std::collections::BTreeMap;
use std::fs;
use std::io::Write as _;
use std::path::{Path, PathBuf};
use std::time::Duration;

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value, json};

use crate::atomic::write_atomic;
use crate::config::{
    Access, AgentKind, ProfileConfig, RootConfig, SecretInput, SecretRef, SecretSource, TenantBrand,
};
use crate::error::{CoreError, Result};
use crate::keystore::Keystore;
use crate::lock::FileLock;
use crate::paths::{DEFAULT_PROFILE, normalize_profile_name};

const CONFIG_LOCK_TIMEOUT: Duration = Duration::from_secs(5);

/// `load_root` returning `None` when the file does not exist.
pub fn load_root(path: &Path) -> Result<Option<RootConfig>> {
    match fs::read(path) {
        Ok(bytes) => {
            if bytes.iter().all(|b| b.is_ascii_whitespace()) {
                return Ok(None);
            }
            let root = serde_json::from_slice(&bytes).map_err(|e| CoreError::json(path, e))?;
            Ok(Some(root))
        }
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(err) => Err(CoreError::io(path, err)),
    }
}

pub fn load_or_default(path: &Path) -> Result<RootConfig> {
    Ok(load_root(path)?.unwrap_or_default())
}

pub fn save_root(path: &Path, root: &RootConfig) -> Result<()> {
    let mut bytes = serde_json::to_vec_pretty(root).map_err(|e| CoreError::json(path, e))?;
    bytes.push(b'\n');
    write_atomic(path, &bytes, 0o600)
}

/// Run `f` against the config while holding `config.json.lock`.
pub fn with_config_lock<T>(path: &Path, f: impl FnOnce(&mut RootConfig) -> Result<T>) -> Result<T> {
    let lock_path = PathBuf::from(format!("{}.lock", path.display()));
    let _lock = FileLock::acquire(&lock_path, CONFIG_LOCK_TIMEOUT)?;
    let mut root = load_or_default(path)?;
    let value = f(&mut root)?;
    save_root(path, &root)?;
    Ok(value)
}

/// Resolve the profile to operate on: `LARKLINE_PROFILE`, then `activeProfile`,
/// then the historical default.
pub fn active_profile_name(root: &RootConfig) -> String {
    if let Some(explicit) = crate::env::profile_override() {
        return explicit;
    }
    if !root.active_profile.is_empty() {
        return root.active_profile.clone();
    }
    DEFAULT_PROFILE.to_string()
}

pub fn require_active_profile(root: &RootConfig) -> Result<(String, ProfileConfig)> {
    let name = active_profile_name(root);
    match root.profile(&name) {
        Some(profile) => Ok((name, profile.clone())),
        None if root.profiles.is_empty() => Err(CoreError::NoActiveProfile),
        None => Err(CoreError::UnknownProfile(name)),
    }
}

pub fn list_profile_names(root: &RootConfig) -> Vec<&str> {
    root.profiles.keys().map(String::as_str).collect()
}

/// Create (or replace) a profile, pointing its App Secret at the encrypted
/// keystore through the exec-provider wrapper.
pub fn create_profile(
    root: &mut RootConfig,
    name: &str,
    agent: AgentKind,
    app_id: &str,
    tenant: TenantBrand,
    wrapper_path: &Path,
) -> Result<()> {
    let name = normalize_profile_name(name)?;
    let secret_id = secret_key_for_app(app_id);
    let mut profile = ProfileConfig::new(
        agent,
        app_id,
        SecretInput::Reference(SecretRef {
            source: SecretSource::Exec,
            provider: Some("bridge".into()),
            id: secret_id,
        }),
        tenant,
    );
    profile.access = Some(Access::default());

    let mut providers = Map::new();
    providers.insert(
        "bridge".into(),
        json!({
            "source": "exec",
            "command": wrapper_path.to_string_lossy(),
            "args": []
        }),
    );
    let mut secrets = Map::new();
    secrets.insert("providers".into(), Value::Object(providers));
    profile.secrets = Some(Value::Object(secrets));

    root.profiles.insert(name.clone(), profile);
    // Activation is the caller's decision: `profile create --no-use` must not
    // silently steal the active slot.
    Ok(())
}

pub fn delete_profile(root: &mut RootConfig, name: &str) -> Result<bool> {
    let removed = root.profiles.remove(name).is_some();
    if removed {
        if root.active_profile == name {
            root.active_profile = root.profiles.keys().next().cloned().unwrap_or_default();
        }
        if root.active_profile.is_empty() {
            root.active_profile = DEFAULT_PROFILE.to_string();
        }
    }
    Ok(removed)
}

pub fn set_active_profile(root: &mut RootConfig, name: &str) -> Result<()> {
    let name = normalize_profile_name(name)?;
    if !root.profiles.contains_key(&name) {
        return Err(CoreError::UnknownProfile(name));
    }
    root.active_profile = name;
    Ok(())
}

/// Build a fresh profile config with the default access policy.
pub fn default_profile(agent: AgentKind) -> ProfileConfig {
    ProfileConfig {
        agent_kind: agent,
        ..ProfileConfig::default()
    }
}

pub fn secret_key_for_app(app_id: &str) -> String {
    format!("app-{app_id}")
}

// ---------------------------------------------------------------------------
// Secret resolution
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ProviderConfig {
    source: SecretSource,
    #[serde(default)]
    command: Option<String>,
    #[serde(default)]
    args: Option<Vec<String>>,
    #[serde(default)]
    path: Option<String>,
    #[serde(default)]
    allowlist: Option<Vec<String>>,
    #[serde(flatten)]
    extra: Map<String, Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct SecretsBlock {
    #[serde(default)]
    providers: BTreeMap<String, ProviderConfig>,
    #[serde(flatten)]
    extra: Map<String, Value>,
}

fn provider_block(profile: &ProfileConfig, root: &RootConfig) -> Option<SecretsBlock> {
    profile
        .secrets
        .as_ref()
        .and_then(|v| serde_json::from_value(v.clone()).ok())
        .or_else(|| {
            root.secrets
                .as_ref()
                .and_then(|v| serde_json::from_value(v.clone()).ok())
        })
}

/// Resolve the profile's App Secret to plaintext.
pub fn resolve_app_secret(
    root: &RootConfig,
    profile: &ProfileConfig,
    keystore: &Keystore,
) -> Result<String> {
    let app = profile.app().ok_or(CoreError::NoActiveProfile)?;
    resolve_secret_input(root, profile, &app.secret, keystore)
}

/// Resolve one `SecretInput` (plain string, `${VAR}`, or a provider ref).
pub fn resolve_secret_input(
    root: &RootConfig,
    profile: &ProfileConfig,
    input: &SecretInput,
    keystore: &Keystore,
) -> Result<String> {
    match input {
        SecretInput::Plain(value) => match expand_env_template(value) {
            Some(from_env) => Ok(from_env),
            None => Ok(value.clone()),
        },
        SecretInput::Reference(reference) => {
            let block = provider_block(profile, root);
            let provider = reference
                .provider
                .as_ref()
                .and_then(|name| block.as_ref().and_then(|b| b.providers.get(name)));

            match (reference.source, provider) {
                (SecretSource::Env, _) => env_secret(reference, provider),
                (SecretSource::File, _) => file_secret(reference, provider),
                (SecretSource::Exec, Some(provider)) => {
                    if is_own_secrets_wrapper(provider.command.as_deref()) {
                        keystore.get(&reference.id)?.ok_or_else(|| {
                            CoreError::Message("secret not found in keystore".into())
                        })
                    } else {
                        exec_secret(reference, provider)
                    }
                }
                (SecretSource::Exec, None) => keystore.get(&reference.id)?.ok_or_else(|| {
                    CoreError::Message(format!(
                        "secret provider {:?} is not configured",
                        reference.provider
                    ))
                }),
            }
        }
    }
}

/// Resolve any id across profiles — the `larkline secrets get` protocol.
pub fn resolve_secret_across_profiles(
    root: &RootConfig,
    keystore_for: impl Fn(&str) -> Keystore,
    id: &str,
) -> Result<Option<String>> {
    for (name, profile) in &root.profiles {
        let keystore = keystore_for(name);
        if let Some(value) = keystore.get(id)? {
            return Ok(Some(value));
        }
        // A profile may declare a non-keystore provider for this id.
        if let Some(app) = profile.app() {
            if let SecretInput::Reference(reference) = &app.secret {
                if reference.id == id {
                    if let Ok(value) = resolve_secret_input(root, profile, &app.secret, &keystore) {
                        return Ok(Some(value));
                    }
                }
            }
        }
    }
    Ok(None)
}

fn env_secret(reference: &SecretRef, provider: Option<&ProviderConfig>) -> Result<String> {
    if let Some(allow) = provider.and_then(|p| p.allowlist.as_ref()) {
        if !allow.iter().any(|allowed| allowed == &reference.id) {
            return Err(CoreError::Message(format!(
                "env secret {} is not in the provider allowlist",
                reference.id
            )));
        }
    }
    std::env::var(&reference.id)
        .map_err(|_| CoreError::Message(format!("env var {} is not set", reference.id)))
}

fn file_secret(reference: &SecretRef, provider: Option<&ProviderConfig>) -> Result<String> {
    let path = match provider.and_then(|p| p.path.as_ref()) {
        Some(base) => Path::new(base).join(&reference.id),
        None => PathBuf::from(&reference.id),
    };
    let value = fs::read_to_string(&path).map_err(|e| CoreError::io(&path, e))?;
    Ok(value.trim_end_matches(['\r', '\n']).to_string())
}

fn exec_secret(reference: &SecretRef, provider: &ProviderConfig) -> Result<String> {
    let command = provider
        .command
        .as_ref()
        .ok_or_else(|| CoreError::Message("exec provider has no command".into()))?;
    let request = json!({
        "protocolVersion": 1,
        "provider": reference.provider.clone().unwrap_or_default(),
        "ids": [reference.id],
    });

    let mut child = std::process::Command::new(command)
        .args(provider.args.clone().unwrap_or_default())
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .map_err(|e| CoreError::io(command, e))?;

    if let Some(stdin) = child.stdin.as_mut() {
        stdin
            .write_all(format!("{request}\n").as_bytes())
            .map_err(|e| CoreError::io(command, e))?;
    }
    let output = child
        .wait_with_output()
        .map_err(|e| CoreError::io(command, e))?;
    if !output.status.success() {
        return Err(CoreError::Message(format!(
            "secret provider {command} exited with {}",
            output.status
        )));
    }
    let response: Value = serde_json::from_slice(&output.stdout)
        .map_err(|e| CoreError::Message(format!("invalid provider response: {e}")))?;
    response
        .get("values")
        .and_then(|v| v.get(&reference.id))
        .and_then(|v| v.as_str())
        .map(str::to_string)
        .ok_or_else(|| CoreError::Message(format!("secret {} not found", reference.id)))
}

/// `${VAR}` expansion. Returns `None` when the value is not a single template
/// (a literal secret containing `${...}` is left untouched).
fn expand_env_template(value: &str) -> Option<String> {
    let trimmed = value.trim();
    if !(trimmed.starts_with("${") && trimmed.ends_with('}')) {
        return None;
    }
    let name = &trimmed[2..trimmed.len() - 1];
    if name.is_empty() || name.contains('}') {
        return None;
    }
    std::env::var(name).ok()
}

fn is_own_secrets_wrapper(command: Option<&str>) -> bool {
    command
        .and_then(|c| Path::new(c).file_name())
        .map(|name| {
            let name = name.to_string_lossy();
            name == "secrets-getter" || name == "secrets-getter.cmd"
        })
        .unwrap_or(false)
}

// ---------------------------------------------------------------------------
// secrets-getter wrapper
// ---------------------------------------------------------------------------

/// Points lark-cli's exec-provider audit at a stable, user-owned script.
///
/// Unlike the TypeScript bridge the wrapper no longer needs to know where
/// Node lives: it execs this binary.
pub fn ensure_secrets_getter_wrapper(root_dir: &Path, binary: &Path) -> Result<PathBuf> {
    fs::create_dir_all(root_dir).map_err(|e| CoreError::io(root_dir, e))?;
    let binary = binary
        .to_str()
        .ok_or_else(|| CoreError::Message("binary path is not valid utf-8".into()))?;

    #[cfg(windows)]
    {
        let path = root_dir.join("secrets-getter.cmd");
        let content = format!(
            "@echo off\r\nrem Auto-generated by larkline. Do not edit.\r\n\"{binary}\" secrets get %*\r\n"
        );
        write_atomic(&path, content.as_bytes(), 0o600)?;
        Ok(path)
    }
    #[cfg(not(windows))]
    {
        let path = root_dir.join("secrets-getter");
        let quoted = binary.replace('\'', "'\\''");
        let content = format!(
            "#!/bin/sh\n# Auto-generated by larkline. Do not edit.\nexec '{quoted}' secrets get \"$@\"\n"
        );
        write_atomic(&path, content.as_bytes(), 0o700)?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let _ = fs::set_permissions(&path, fs::Permissions::from_mode(0o700));
        }
        Ok(path)
    }
}

/// Read the current binary path (used by the wrapper).
pub fn current_binary() -> Result<PathBuf> {
    std::env::current_exe().map_err(io_current_exe)
}

fn io_current_exe(err: std::io::Error) -> CoreError {
    CoreError::Message(format!("cannot resolve current executable: {err}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::MessageReplyMode;

    fn temp_root() -> (tempfile::TempDir, PathBuf) {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config.json");
        (dir, path)
    }

    #[test]
    fn missing_file_is_none() {
        let (_dir, path) = temp_root();
        assert!(load_root(&path).unwrap().is_none());
    }

    #[test]
    fn profile_crud() {
        let (_dir, path) = temp_root();
        with_config_lock(&path, |root| {
            create_profile(
                root,
                "bot-a",
                AgentKind::Codex,
                "cli_a",
                TenantBrand::Feishu,
                Path::new("/tmp/secrets-getter"),
            )?;
            set_active_profile(root, "bot-a")?;
            assert_eq!(root.active_profile, "bot-a");
            assert_eq!(root.profiles.len(), 1);
            Ok(())
        })
        .unwrap();

        let root = load_root(&path).unwrap().unwrap();
        let profile = root.profile("bot-a").unwrap();
        assert_eq!(profile.agent_kind, AgentKind::Codex);
        assert_eq!(profile.message_reply(), MessageReplyMode::Markdown);
        assert_eq!(profile.app().unwrap().id, "cli_a");
        assert!(matches!(
            profile.app().unwrap().secret,
            SecretInput::Reference(_)
        ));
        // Unknown profile -> typed error.
        assert!(matches!(
            require_active_profile(&RootConfig::default()),
            Err(CoreError::NoActiveProfile)
        ));
    }

    #[test]
    fn delete_active_profile_falls_back() {
        let (_dir, path) = temp_root();
        with_config_lock(&path, |root| {
            create_profile(
                root,
                "a",
                AgentKind::Claude,
                "cli_a",
                TenantBrand::Feishu,
                Path::new("/w"),
            )?;
            create_profile(
                root,
                "b",
                AgentKind::Claude,
                "cli_b",
                TenantBrand::Feishu,
                Path::new("/w"),
            )?;
            set_active_profile(root, "a")?;
            set_active_profile(root, "b")?;
            assert!(delete_profile(root, "b")?);
            assert_eq!(root.active_profile, "a");
            Ok(())
        })
        .unwrap();
    }

    #[test]
    fn env_template_expansion() {
        // SAFETY: single-threaded test setup, no other thread reads this var.
        unsafe { std::env::set_var("LARKLINE_TEST_SECRET", "hunter2") };
        assert_eq!(
            expand_env_template("${LARKLINE_TEST_SECRET}").as_deref(),
            Some("hunter2")
        );
        assert_eq!(expand_env_template("hunter2"), None);
        assert_eq!(expand_env_template("${UNSET_VAR_FOR_TEST_XYZ}"), None);
    }

    #[test]
    fn wrapper_is_created() {
        let dir = tempfile::tempdir().unwrap();
        let path = ensure_secrets_getter_wrapper(dir.path(), Path::new("/usr/local/bin/larkline"))
            .unwrap();
        let text = fs::read_to_string(&path).unwrap();
        assert!(text.contains("secrets get"));
    }
}
