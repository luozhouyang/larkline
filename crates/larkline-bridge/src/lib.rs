//! Bridge runtime.
//!
//! Owns the run lifecycle: receive an inbound message, decide whether to act
//! (policy), render a card, spawn the agent, translate its events into card
//! updates, and persist the session so the next message can resume.

pub mod agent_runner;
pub mod callback;
pub mod commands;
pub mod comments;
pub mod keepalive;
pub mod meeting;
pub mod owner;
pub mod pending;
pub mod permissions;
pub mod policy;
pub mod render;
pub mod reply;
pub mod run;
pub mod run_limit;
pub mod text_render;

pub use callback::{
    CallbackAuth, CallbackExpected, CallbackKey, CallbackPayload, CallbackRejection,
    CallbackSignInput, CallbackTokens,
};
pub use commands::{CommandEffect, CommandOutcome};
pub use meeting::{
    ImReplyTarget, MeetingApi, MeetingConfig, MeetingHandle, MeetingHost, MeetingManager,
    MeetingPush, MeetingPushHealth, MeetingSessionStatus, RestMeetingApi, can_run_meeting_command,
    config_from_profile, describe_meeting_error, meeting_enabled, parse_meeting_push,
};
pub use owner::{
    OwnerGate, OwnerRefreshState, OwnerSnapshot, OwnerSource, RestOwnerSource,
    can_run_admin_command, parse_owner_id,
};
pub use pending::{PendingQueue, PushOutcome, QueueLimits};
pub use permissions::{AccessMode, ClaudePermissionMode, RunPermissions};
pub use run::{Bridge, scope_key};
pub use run_limit::{MAX_CONCURRENT_RUNS, RunLimiter, RunPermit};
pub use text_render::{TextFormat, TextRenderOptions, mask_emails, render_text};

/// Serializes fake-agent script creation and spawning in unit tests.
///
/// Several tests write a `#!/bin/sh` fake agent and then spawn it. On Unix a
/// concurrent `fork` can inherit the script's open-for-write fd and make a
/// later `exec` of that script fail with `ETXTBSY` under parallel test
/// execution. Taking this lock around write-then-spawn (and holding it until
/// the child is running) keeps those test-only scripts reliable without
/// affecting production code.
#[cfg(test)]
pub(crate) static TEST_FAKE_AGENT_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());
