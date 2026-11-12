//! Feishu/Lark transport glue.
//!
//! Wraps the `lark-channel` crate (the Rust counterpart of the npm
//! `@larksuite/channel` SDK) and fills the gaps the bridge needs through the
//! raw OpenAPI: reactions, chat metadata, file comments, VC bot endpoints and
//! the QR app-registration device flow.

pub mod cards;
pub mod client;
pub mod comments;
pub mod meeting;
pub mod qr;
pub mod reactions;
pub mod rest;
pub mod streaming;

pub use cards::{error_card, final_card, running_card};
pub use client::{Channel, LarkResult, domain_for};
pub use meeting::{EventsPage, JoinResult, MeetingActivityItem, VcBotClient, is_not_in_gray};
pub use qr::{QrError, RegistrationOptions, RegistrationResult, RegistrationSession};
pub use rest::{RestClient, RestError, RestResult};
pub use streaming::{
    CARD_SAFE_AGE, FinishPlan, STREAMING_ELEMENT_ID, SkipReason, StreamingCard, StreamingState,
    UpdatePlan, enable_streaming, enable_streaming_with_config,
};
