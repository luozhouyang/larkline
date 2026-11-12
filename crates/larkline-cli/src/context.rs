//! Shared command context: paths, root config and keystore helpers.

use std::path::{Path, PathBuf};

use larkline_core::keystore::Keystore;
use larkline_core::paths::AppPaths;
use larkline_core::store::{active_profile_name, load_or_default};
use larkline_core::{CoreError, RootConfig};

use crate::cli::Cli;

pub struct AppContext {
    pub root_dir: PathBuf,
    pub config_file: PathBuf,
    pub paths: AppPaths,
    pub root: RootConfig,
}

impl AppContext {
    pub fn load(cli: &Cli) -> anyhow::Result<Self> {
        let root_dir = cli
            .home
            .clone()
            .unwrap_or_else(larkline_core::paths::default_root_dir);
        let config_file = root_dir.join("config.json");
        let root = load_or_default(&config_file)?;

        let profile = cli
            .profile
            .clone()
            .or_else(larkline_core::env::profile_override)
            .unwrap_or_else(|| active_profile_name(&root));

        let paths = AppPaths::resolve(root_dir.clone(), &profile)?;
        Ok(Self {
            root_dir,
            config_file,
            paths,
            root,
        })
    }

    pub fn profile_name(&self) -> &str {
        &self.paths.profile
    }

    pub fn profile(&self) -> Option<&larkline_core::ProfileConfig> {
        self.root.profile(&self.paths.profile)
    }

    pub fn require_profile(&self) -> anyhow::Result<&larkline_core::ProfileConfig> {
        self.profile()
            .ok_or_else(|| CoreError::UnknownProfile(self.paths.profile.clone()).into())
    }

    pub fn keystore(&self) -> Keystore {
        Keystore::new(&self.paths.secrets_file, &self.paths.keystore_salt_file)
    }

    /// Resolve the profile's App Secret to plaintext.
    pub fn app_secret(&self) -> anyhow::Result<String> {
        let profile = self.require_profile()?;
        let keystore = self.keystore();
        Ok(larkline_core::store::resolve_app_secret(
            &self.root, profile, &keystore,
        )?)
    }
}

/// Keystore for an arbitrary profile under the same root.
pub fn keystore_for(root_dir: &Path, profile: &str) -> anyhow::Result<Keystore> {
    let paths = AppPaths::resolve(root_dir.to_path_buf(), profile)?;
    Ok(Keystore::new(
        &paths.secrets_file,
        &paths.keystore_salt_file,
    ))
}

/// Prompt for a value without echoing it (used for App Secrets).
pub fn prompt_secret(prompt: impl AsRef<str>) -> anyhow::Result<String> {
    let value = rpassword::prompt_password(prompt.as_ref())?;
    if value.trim().is_empty() {
        anyhow::bail!("empty secret");
    }
    Ok(value)
}
