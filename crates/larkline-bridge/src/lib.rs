//! Bridge runtime.
//!
//! Owns the run lifecycle: receive an inbound message, decide whether to act
//! (policy), render a card, spawn the agent, translate its events into card
//! updates, and persist the session so the next message can resume.

pub mod agent_runner;
pub mod callback;
pub mod comments;
pub mod keepalive;
pub mod meeting;
pub mod owner;
pub mod pending;
pub mod policy;
pub mod render;
pub mod run;
pub mod run_limit;
pub mod text_render;

pub use callback::{
    CallbackAuth, CallbackExpected, CallbackKey, CallbackPayload, CallbackRejection,
    CallbackSignInput, CallbackTokens,
};
pub use meeting::{
    MeetingApi, MeetingConfig, MeetingHandle, MeetingHost, MeetingManager, MeetingPush,
    MeetingPushHealth, MeetingSessionStatus, RestMeetingApi, can_run_meeting_command,
    config_from_profile, describe_meeting_error, meeting_enabled, parse_meeting_push,
};
pub use owner::{
    OwnerGate, OwnerRefreshState, OwnerSnapshot, OwnerSource, RestOwnerSource,
    can_run_admin_command, parse_owner_id,
};
pub use pending::{PendingQueue, PushOutcome, QueueLimits};
pub use run::Bridge;
pub use run_limit::{MAX_CONCURRENT_RUNS, RunLimiter, RunPermit};
pub use text_render::{TextFormat, TextRenderOptions, mask_emails, render_text};
