//! Feishu/Lark transport glue.
//!
//! Wraps the `lark-channel` crate (the Rust counterpart of the npm
//! `@larksuite/channel` SDK) and fills the gaps the bridge needs through the
//! raw OpenAPI: reactions, chat metadata, file comments, VC bot endpoints and
//! the QR app-registration device flow.

pub mod cards;
pub mod client;
pub mod rest;

pub use cards::{error_card, final_card, running_card};
pub use client::{Channel, LarkResult, domain_for};
pub use rest::{RestClient, RestError, RestResult};
