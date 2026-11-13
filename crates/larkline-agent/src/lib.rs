//! Agent adapters.
//!
//! The bridge never embeds an agent. It spawns the locally installed CLI
//! (`claude` or `codex`), feeds the prompt on stdin, and translates the
//! child's line-delimited JSON into a small, runtime-agnostic event stream.
//! Nothing in this crate depends on Node.

pub mod claude;
pub mod codex;
pub mod codex_app_server;
pub mod detect;
pub mod prompt;
pub mod types;

pub use claude::{ClaudeArgsInput, ClaudeStreamTranslator, build_claude_args, user_message_json};
pub use codex::{CodexArgsInput, CodexFinishReason, CodexJsonlTranslator, build_codex_args};
pub use codex_app_server::{
    CodexAppServerTranslator, CodexThreadConfig, approval_response, error_response,
    initialize_request, permissions_response, request_id_string, success_response,
    thread_resume_request, thread_start_request, turn_interrupt_request, turn_start_request,
};
pub use detect::{ResolvedBinary, resolve_executable};
pub use prompt::{
    BridgePromptContext, PromptAttachment, PromptContext, PromptMention, build_agent_prompt,
    build_agent_prompt_with_attachments, build_bridge_system_prompt, prefix_bridge_system_prompt,
};
pub use types::{
    AgentEvent, PermissionResponse, SandboxMode, TerminationReason, usage_total_tokens,
};
