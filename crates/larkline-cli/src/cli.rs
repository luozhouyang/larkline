//! Command line surface.
//!
//! Kept declarative: every command parses into a small struct, and the logic
//! lives in `commands/`. `larkline --help` is the user-facing contract.

use std::path::PathBuf;

use clap::{ArgAction, Args, Parser, Subcommand, ValueEnum};

#[derive(Debug, Parser)]
#[command(
    name = "larkline",
    version = larkline_core::VERSION,
    about = "Bridge Feishu/Lark messenger with local CLI coding agents",
    long_about = None,
    propagate_version = true
)]
pub struct Cli {
    /// State directory (defaults to $LARKLINE_HOME or ~/.larkline).
    #[arg(long, global = true, env = "LARKLINE_HOME", hide_env_values = true)]
    pub home: Option<PathBuf>,

    /// Profile to operate on (defaults to the active profile).
    #[arg(long, global = true, env = "LARKLINE_PROFILE")]
    pub profile: Option<String>,

    /// Increase log verbosity (-v, -vv).
    #[arg(short, long, global = true, action = ArgAction::Count)]
    pub verbose: u8,

    #[command(subcommand)]
    pub command: Commands,
}

#[derive(Debug, Subcommand)]
pub enum Commands {
    /// Run the bridge in the foreground.
    Run(RunArgs),
    /// Check config, credentials, keystore and agent binaries.
    Doctor,
    /// Create a profile by scanning a QR code (app registration device flow).
    Onboard(OnboardArgs),
    /// Manage profiles.
    Profile(ProfileArgs),
    /// Manage the encrypted secret keystore.
    Secrets(SecretsArgs),
    /// Upgrade the larkline binary (explicit; never automatic).
    Upgrade(UpgradeArgs),
    /// Install (if needed) and start the bridge as an OS-managed daemon.
    Start,
    /// Stop the OS-managed daemon and disable autostart.
    Stop,
    /// Restart the OS-managed daemon.
    Restart,
    /// Show OS service status (pid, last exit, log paths).
    Status,
    /// Remove the OS service registration.
    Unregister,
    /// List running bridge processes on this machine.
    Ps,
    /// Kill a running bridge process by short id or list index.
    Kill {
        target: String,
    },
}

#[derive(Debug, Args)]
pub struct RunArgs {
    /// Working directory for the agent (defaults to the profile workspace).
    #[arg(long)]
    pub cwd: Option<PathBuf>,

    /// Validate and print what would run, without connecting.
    #[arg(long)]
    pub dry_run: bool,
}

#[derive(Debug, Args)]
pub struct ProfileArgs {
    #[command(subcommand)]
    pub command: ProfileCommand,
}

#[derive(Debug, Subcommand)]
pub enum ProfileCommand {
    /// List profiles.
    List,
    /// Create a profile and store its App Secret in the keystore.
    Create(CreateArgs),
    /// Set the active profile.
    Use { name: String },
    /// Delete a profile (keeps the encrypted secret unless --purge-secret).
    Remove {
        name: String,
        #[arg(long)]
        yes: bool,
        #[arg(long)]
        purge_secret: bool,
    },
    /// Print one profile as JSON.
    Show { name: Option<String> },
}

#[derive(Debug, Args)]
pub struct OnboardArgs {
    /// Profile name (defaults to a free name such as `bot`).
    #[arg(long)]
    pub name: Option<String>,

    #[arg(long, value_enum, default_value_t = AgentArg::Claude)]
    pub agent: AgentArg,

    #[arg(long, value_enum, default_value_t = TenantArg::Feishu)]
    pub tenant: TenantArg,

    /// Create the profile without making it active.
    #[arg(long)]
    pub no_use: bool,
}

#[derive(Debug, Args)]
pub struct CreateArgs {
    pub name: String,

    #[arg(long, value_enum, default_value_t = AgentArg::Claude)]
    pub agent: AgentArg,

    /// Feishu/Lark app id, e.g. cli_a1b2c3d4.
    #[arg(long)]
    pub app_id: String,

    /// App Secret; prompted for without echo when omitted.
    #[arg(long, hide_env_values = true)]
    pub app_secret: Option<String>,

    #[arg(long, value_enum, default_value_t = TenantArg::Feishu)]
    pub tenant: TenantArg,

    /// Create the profile without making it active.
    #[arg(long)]
    pub no_use: bool,
}

#[derive(Debug, Clone, Copy, ValueEnum)]
pub enum AgentArg {
    Claude,
    Codex,
}

impl From<AgentArg> for larkline_core::AgentKind {
    fn from(value: AgentArg) -> Self {
        match value {
            AgentArg::Claude => larkline_core::AgentKind::Claude,
            AgentArg::Codex => larkline_core::AgentKind::Codex,
        }
    }
}

#[derive(Debug, Clone, Copy, ValueEnum)]
pub enum TenantArg {
    Feishu,
    Lark,
}

impl From<TenantArg> for larkline_core::TenantBrand {
    fn from(value: TenantArg) -> Self {
        match value {
            TenantArg::Feishu => larkline_core::TenantBrand::Feishu,
            TenantArg::Lark => larkline_core::TenantBrand::Lark,
        }
    }
}

#[derive(Debug, Args)]
pub struct SecretsArgs {
    #[command(subcommand)]
    pub command: SecretsCommand,
}

#[derive(Debug, Subcommand)]
pub enum SecretsCommand {
    /// Exec-provider protocol: JSON request on stdin, JSON response on stdout.
    Get,
    /// Encrypt and store a secret.
    Set {
        id: String,
        #[arg(long, hide_env_values = true)]
        value: Option<String>,
    },
    /// List secret ids (never values).
    List,
    /// Delete a secret.
    Remove { id: String },
}

#[derive(Debug, Args)]
pub struct UpgradeArgs {
    /// Only report whether a newer release exists.
    #[arg(long)]
    pub check: bool,

    /// Pin a specific version instead of the latest release.
    #[arg(long)]
    pub version: Option<String>,
}
