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
    let meeting_config = larkline_bridge::meeting::config_from_profile(&profile);
    println!(
        "meeting: {} (trigger {:?}, poll {}ms, respondIn {:?})",
        if meeting_config.enabled {
            "enabled"
        } else {
            "disabled"
        },
        meeting_config.trigger,
        meeting_config.poll_interval_ms,
        meeting_config.respond_in
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

    // The in-meeting agent polls `bots/events` on its own Tokio task. Starting
    // it here (rather than inside `Bridge::new`) keeps construction synchronous
    // and makes the runtime dependency explicit.
    if meeting_config.enabled {
        if bridge.start_meeting() {
            println!("meeting poller: started");
        } else {
            println!("meeting poller: could not start (no tokio runtime?)");
        }
    }

    let connector = OpenApiWebSocketEventConnector::new(
        channel.openapi().clone(),
        TokioTungsteniteWebSocketTransport::new(),
    );
    let mut event_loop = EventLoop::with_options(
        connector,
        EventLoopOptions::default().with_unlimited_reconnects(),
    );

    println!("\nconnecting to the Feishu long connection… (ctrl-c to stop)");
    let outcome: anyhow::Result<()> = tokio::select! {
        result = event_loop.run({
            let bridge = Arc::clone(&bridge);
            move |event| {
                let bridge = Arc::clone(&bridge);
                async move {
                    match event.event.clone() {
                        ChannelEvent::Message(message) => {
                            tracing::info!(
                                chat = %message.chat_id,
                                sender = %message.sender_id,
                                "inbound message"
                            );
                            bridge.handle_message(*message).await;
                        }
                        ChannelEvent::CardAction(action) => {
                            if let Some(status) = bridge.handle_card_action(&action).await {
                                tracing::info!(%status, "card action handled");
                            }
                        }
                        ChannelEvent::Unknown { raw, .. } => {
                            // The SDK types only messages and card actions.
                            if larkline_bridge::comments::is_file_comment_event(&raw) {
                                // Cloud-doc comment @-mentions
                                // (`drive.notice.comment_add_v1`) arrive here
                                // with the whole platform payload; hand them to
                                // the comment flow, which parses it itself.
                                tracing::info!("inbound file-comment event");
                                // Detached on purpose: a comment run can take
                                // minutes, and the event loop must keep
                                // acknowledging frames and heartbeats.
                                let bridge = Arc::clone(&bridge);
                                tokio::spawn(async move {
                                    bridge.handle_comment_event(&raw).await;
                                });
                            } else if bridge.handle_meeting_event(&raw) {
                                // `vc.bot.meeting_*` frames feed the meeting
                                // manager; the poller is the gap-fill lane.
                                tracing::debug!("routed meeting event");
                            } else {
                                tracing::debug!("ignoring unhandled event type");
                            }
                        }
                    }
                    Ok(WebSocketEventAck::ok())
                }
            }
        }) => match result {
            Ok(_) => Ok(()),
            Err(err) => Err(anyhow::Error::from(err)),
        },
        _ = tokio::signal::ctrl_c() => {
            println!("\nstopping");
            Ok(())
        }
    };

    // Leave the meetings we joined before dropping the bridge, so the bot does
    // not linger as a participant after the process exits.
    bridge.stop_meeting().await;
    outcome
}
