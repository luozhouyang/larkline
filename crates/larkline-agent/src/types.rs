//! Shared agent event model.
//!
//! Both adapters translate their CLI's wire format into these events; the
//! card renderer and the session store only ever see this enum. That is what
//! keeps the rest of the bridge independent of which agent is installed.

use serde_json::Value;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SandboxMode {
    ReadOnly,
    WorkspaceWrite,
    DangerFullAccess,
}

impl Default for SandboxMode {
    /// The safe default, matching the TypeScript profile default.
    fn default() -> Self {
        Self::WorkspaceWrite
    }
}

impl SandboxMode {
    pub fn as_str(self) -> &'static str {
        match self {
            SandboxMode::ReadOnly => "read-only",
            SandboxMode::WorkspaceWrite => "workspace-write",
            SandboxMode::DangerFullAccess => "danger-full-access",
        }
    }

    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "read-only" => Some(Self::ReadOnly),
            "workspace-write" => Some(Self::WorkspaceWrite),
            "danger-full-access" => Some(Self::DangerFullAccess),
            _ => None,
        }
    }
}

/// Why a run ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TerminationReason {
    /// The agent finished normally.
    Normal,
    /// The agent or the bridge failed.
    Failed,
    /// The user pressed stop.
    Interrupted,
    /// The idle timeout fired.
    Timeout,
}

#[derive(Debug, Clone, PartialEq)]
pub enum AgentEvent {
    /// The agent reported its identity / session.
    System {
        session_id: Option<String>,
        thread_id: Option<String>,
        cwd: Option<String>,
        model: Option<String>,
    },
    /// Streaming text (progress commentary).
    Text { delta: String },
    /// Streaming reasoning/thinking.
    Thinking { delta: String },
    /// The agent started a tool call.
    ToolUse {
        id: String,
        name: String,
        input: Value,
    },
    /// A tool call finished.
    ToolResult {
        id: String,
        output: String,
        is_error: bool,
    },
    /// The final answer, when the protocol distinguishes it from commentary.
    FinalText { content: String },
    Usage {
        input_tokens: Option<u64>,
        output_tokens: Option<u64>,
        cached_input_tokens: Option<u64>,
        reasoning_output_tokens: Option<u64>,
        cost_usd: Option<f64>,
    },
    Done {
        session_id: Option<String>,
        thread_id: Option<String>,
        termination_reason: TerminationReason,
    },
    Error {
        message: String,
        termination_reason: TerminationReason,
    },
}

/// Total token count used for the card footer. Missing values are zero.
pub fn usage_total_tokens(event: &AgentEvent) -> Option<u64> {
    match event {
        AgentEvent::Usage {
            input_tokens,
            output_tokens,
            ..
        } => Some(input_tokens.unwrap_or(0) + output_tokens.unwrap_or(0)),
        _ => None,
    }
}

/// Truncate a message for logs and IM replies, keeping char boundaries sane.
pub fn truncate_chars(value: &str, max_chars: usize) -> String {
    if value.chars().count() <= max_chars {
        return value.to_string();
    }
    let mut out: String = value.chars().take(max_chars).collect();
    out.push('…');
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sandbox_roundtrip() {
        for mode in [
            SandboxMode::ReadOnly,
            SandboxMode::WorkspaceWrite,
            SandboxMode::DangerFullAccess,
        ] {
            assert_eq!(SandboxMode::parse(mode.as_str()), Some(mode));
        }
        assert_eq!(SandboxMode::parse("root"), None);
    }

    #[test]
    fn truncation_respects_char_boundaries() {
        assert_eq!(truncate_chars("小助手", 2), "小助…");
        assert_eq!(truncate_chars("ok", 5), "ok");
    }
}
