//! larkline CLI entry point.
//!
//! Startup order matters:
//! 1. migrate the legacy `~/.lark-channel` directory before anything reads it;
//! 2. parse args (clap handles `--help` / `--version`);
//! 3. initialise tracing;
//! 4. run the requested command on a Tokio runtime.

mod cli;
mod commands;
mod context;

use clap::Parser;
use cli::{Cli, Commands};
use context::AppContext;

fn main() -> anyhow::Result<()> {
    let _ = larkline_core::paths::migrate_legacy_home();

    let cli = Cli::parse();
    init_tracing(cli.verbose);

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;
    runtime.block_on(dispatch(cli))
}

async fn dispatch(cli: Cli) -> anyhow::Result<()> {
    match &cli.command {
        Commands::Run(args) => commands::run::execute(&cli, args).await,
        Commands::Doctor => {
            let ctx = AppContext::load(&cli)?;
            commands::doctor::execute(&ctx)
        }
        Commands::Profile(args) => commands::profile::execute(&cli, args),
        Commands::Secrets(args) => commands::secrets::execute(&cli, args),
        Commands::Upgrade(args) => commands::upgrade::execute(args).await,
    }
}

fn init_tracing(verbose: u8) {
    use tracing_subscriber::EnvFilter;

    let level = match verbose {
        0 => "warn",
        1 => "info",
        _ => "debug",
    };
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(level));
    let _ = tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_writer(std::io::stderr)
        .try_init();
}
