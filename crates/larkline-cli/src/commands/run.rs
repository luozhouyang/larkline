//! `larkline run` — foreground bridge.
//!
//! Milestone 1 wires the real WebSocket event stream end to end and prints
//! inbound events. The run flow (policy -> card -> agent -> session) lands on
//! top of this in the next milestone; `--dry-run` validates everything the
//! loop needs without connecting.

use std::sync::Arc;

use anyhow::{Context as _, bail};
use lark_channel::event::{
    ChannelEvent, EventLoop, EventLoopOptions, OpenApiWebSocketEventConnector,
};
use lark_channel::lark_openapi::{TokioTungsteniteWebSocketTransport, WebSocketEventAck};
use larkline_agent::resolve_executable;
use larkline_bridge::Bridge;
use larkline_lark::Channel;

use crate::cli::{Cli, RunArgs};
use crate::context::AppContext;

pub async fn execute(cli: &Cli, args: &RunArgs) -> anyhow::Result<()> {
    let ctx = AppContext::load(cli)?;
    let profile = ctx.require_profile()?.clone();
    let app = profile
        .app()
        .cloned()
        .ok_or_else(|| anyhow::anyhow!("profile {} has no app credentials", ctx.profile_name()))?;
    if app.id.is_empty() {
        bail!("profile {} has an empty app id", ctx.profile_name());
    }

    let secret = ctx.app_secret()?;
    let agent_binary = resolve_executable(profile.agent_kind.binary_name()).with_context(|| {
        format!(
            "{} is not installed; install it or set {}_BIN",
            profile.agent_kind.display_name(),
            profile.agent_kind.binary_name().to_ascii_uppercase()
        )
    })?;

    let cwd = args
        .cwd
        .clone()
        .unwrap_or_else(|| ctx.paths.default_workspace_dir.clone());
    std::fs::create_dir_all(&cwd).ok();

    println!(
        "larkline {} — profile={} app={} tenant={:?} agent={} ({})",
        larkline_core::VERSION,
        ctx.profile_name(),
        app.id,
        app.tenant,
        profile.agent_kind.display_name(),
        agent_binary.path.display()
    );
    println!("workspace: {}", cwd.display());
    println!(
        "reply mode: {:?}  max runs: {}",
        profile.message_reply(),
        profile.max_concurrent_runs()
    );

    if args.dry_run {
        println!("\ndry run ok; no connection made");
        return Ok(());
    }

    let channel = Channel::new(app.id.clone(), secret, app.tenant);
    let bridge = Arc::new(Bridge::new(
        channel.clone(),
        profile.clone(),
        agent_binary.path.clone(),
        cwd.clone(),
        None,
        None,
        ctx.paths.clone(),
    )?);
    println!("sessions: {}", bridge.sessions_path().display());

    let connector = OpenApiWebSocketEventConnector::new(
        channel.openapi().clone(),
        TokioTungsteniteWebSocketTransport::new(),
    );
    let mut event_loop = EventLoop::with_options(
        connector,
        EventLoopOptions::default().with_unlimited_reconnects(),
    );

    println!("\nconnecting to the Feishu long connection… (ctrl-c to stop)");
    tokio::select! {
        result = event_loop.run({
            let bridge = Arc::clone(&bridge);
            move |event| {
                let bridge = Arc::clone(&bridge);
                async move {
                    if let ChannelEvent::Message(message) = event.event.clone() {
                        tracing::info!(
                            chat = %message.chat_id,
                            sender = %message.sender_id,
                            "inbound message"
                        );
                        bridge.handle_message(*message).await;
                    }
                    Ok(WebSocketEventAck::ok())
                }
            }
        }) => {
            if let Err(err) = result {
                return Err(anyhow::Error::from(err));
            }
        }
        _ = tokio::signal::ctrl_c() => {
            println!("\nstopping");
        }
    }
    Ok(())
}
