//! Bridge runtime.
//!
//! Owns the run lifecycle: receive an inbound message, decide whether to act
//! (policy), render a card, spawn the agent, translate its events into card
//! updates, and persist the session so the next message can resume.

pub mod agent_runner;
pub mod policy;
pub mod render;
pub mod run;

pub use run::Bridge;
