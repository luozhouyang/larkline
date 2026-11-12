//! `larkline start|stop|restart|status|unregister` — OS service lifecycle.
//!
//! The service definition is written by `daemon::install`; the manager then
//! owns the process. `start` deliberately stops a live instance first so an
//! upgrade never leaves two bridges holding the same app lock.

use std::time::{Duration, Instant};

use crate::cli::Cli;
use crate::context::AppContext;
use crate::daemon;

fn context_and_spec(cli: &Cli) -> anyhow::Result<(AppContext, daemon::ServiceSpec)> {
    let ctx = AppContext::load(cli)?;
    let program = std::env::current_exe()?;
    let spec =
        daemon::ServiceSpec::for_profile(ctx.profile_name(), program, ctx.root_dir.clone())?;
    Ok((ctx, spec))
}

fn ensure(action: daemon::ServiceAction, what: &str) -> anyhow::Result<()> {
    if action.ok {
        return Ok(());
    }
    let detail = if action.stderr.trim().is_empty() {
        action.detail.clone()
    } else {
        action.stderr.trim().to_string()
    };
    anyhow::bail!("{what} failed: {detail}");
}

pub fn start(cli: &Cli) -> anyhow::Result<()> {
    let (_ctx, spec) = context_and_spec(cli)?;
    ensure(daemon::install(&spec)?, "install")?;

    if daemon::status(&spec)?.running {
        let stopped = daemon::stop(&spec)?;
        if !stopped.ok {
            eprintln!("warning: stop before start failed: {}", stopped.stderr.trim());
        }
        // launchd bootout is asynchronous; give it a moment before starting.
        let deadline = Instant::now() + Duration::from_secs(5);
        while daemon::status(&spec)?.running && Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(200));
        }
    }

    ensure(daemon::start(&spec)?, "start")?;
    println!("started {} ({})", spec.name, spec.service_id);
    Ok(())
}

pub fn stop(cli: &Cli) -> anyhow::Result<()> {
    let (_ctx, spec) = context_and_spec(cli)?;
    if !daemon::is_installed(&spec)? {
        println!("{} is not installed", spec.name);
        return Ok(());
    }
    ensure(daemon::stop(&spec)?, "stop")?;
    println!("stopped {} (autostart disabled)", spec.name);
    Ok(())
}

pub fn restart(cli: &Cli) -> anyhow::Result<()> {
    let (_ctx, spec) = context_and_spec(cli)?;
    if !daemon::is_installed(&spec)? {
        anyhow::bail!("{} is not installed; run `larkline start` first", spec.name);
    }
    ensure(daemon::restart(&spec)?, "restart")?;
    println!("restarted {}", spec.name);
    Ok(())
}

pub fn status(cli: &Cli) -> anyhow::Result<()> {
    let (_ctx, spec) = context_and_spec(cli)?;
    let status = daemon::status(&spec)?;
    println!("service:    {}", status.service_name);
    println!("platform:   {:?}", status.platform);
    println!("definition: {}", status.definition_path.display());
    println!("installed:  {}", status.installed);
    println!("running:    {}", status.running);
    println!(
        "pid:        {}",
        status
            .pid
            .map(|pid| pid.to_string())
            .unwrap_or_else(|| "-".to_string())
    );
    println!(
        "last exit:  {}",
        status
            .last_exit
            .map(|code| code.to_string())
            .unwrap_or_else(|| "-".to_string())
    );
    if !status.detail.is_empty() {
        println!("detail:     {}", status.detail);
    }
    Ok(())
}

pub fn unregister(cli: &Cli) -> anyhow::Result<()> {
    let (_ctx, spec) = context_and_spec(cli)?;
    if !daemon::is_installed(&spec)? {
        println!("{} is not installed", spec.name);
        return Ok(());
    }
    ensure(daemon::uninstall(&spec)?, "unregister")?;
    println!("removed {}", spec.name);
    Ok(())
}
