//! Agent adapters.
//!
//! The bridge never embeds an agent. It spawns the locally installed CLI
//! (`claude` or `codex`), feeds the prompt on stdin, and translates the
//! child's line-delimited JSON into a small, runtime-agnostic event stream.
//! Nothing in this crate depends on Node.

pub mod claude;
pub mod codex;
pub mod detect;
pub mod prompt;
pub mod types;

pub use claude::{ClaudeStreamTranslator, build_claude_args};
pub use codex::{CodexFinishReason, CodexJsonlTranslator, build_codex_args};
pub use detect::{ResolvedBinary, resolve_executable};
pub use prompt::{BridgePromptContext, build_bridge_system_prompt};
pub use types::{AgentEvent, SandboxMode, TerminationReason, usage_total_tokens};
