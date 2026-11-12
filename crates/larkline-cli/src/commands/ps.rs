//! `larkline ps` / `larkline kill` — machine-level process registry.

use crate::cli::Cli;
use crate::context::AppContext;
use crate::ps;

pub fn list(cli: &Cli) -> anyhow::Result<()> {
    let ctx = AppContext::load(cli)?;
    let records = ps::list_from(&ctx.paths.user_registry_file);
    if records.is_empty() {
        println!("no bridge processes registered");
        return Ok(());
    }

    println!(
        "{:<4} {:<8} {:<18} {:<8} {:<8} {}",
        "#", "ID", "PROFILE", "AGENT", "PID", "STARTED"
    );
    for (index, record) in records.iter().enumerate() {
        let agent = match record.entry.agent_kind {
            larkline_core::AgentKind::Claude => "claude",
            larkline_core::AgentKind::Codex => "codex",
        };
        let status = if record.alive { "" } else { "  (stale)" };
        println!(
            "{:<4} {:<8} {:<18} {:<8} {:<8} {}{}",
            index + 1,
            record.entry.id,
            record.entry.profile_name,
            agent,
            record.entry.pid,
            record.entry.started_at,
            status
        );
    }
    Ok(())
}

pub fn kill(cli: &Cli, target: &str) -> anyhow::Result<()> {
    let ctx = AppContext::load(cli)?;
    let outcome = ps::kill_from(&ctx.paths.user_registry_file, target)?;
    println!(
        "stopped {} (pid {}, {:?})",
        outcome.entry.profile_name, outcome.entry.pid, outcome.signal
    );
    Ok(())
}
