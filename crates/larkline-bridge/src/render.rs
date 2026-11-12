//! Run view: fold agent events into the state a card renders.
//!
//! Kept pure so the fold is unit-testable without a network or an agent.

use larkline_agent::{AgentEvent, TerminationReason};

#[derive(Debug, Default, Clone)]
pub struct RunView {
    /// Visible answer text (streamed commentary plus the final answer).
    pub answer: String,
    /// The agent's final answer when the protocol distinguishes it.
    pub final_answer: Option<String>,
    pub reasoning: String,
    pub tools: Vec<String>,
    pub tool_errors: usize,
    pub session_id: Option<String>,
    pub thread_id: Option<String>,
    pub error: Option<String>,
    pub termination: Option<TerminationReason>,
    pub input_tokens: u64,
    pub output_tokens: u64,
    pub finished: bool,
}

impl RunView {
    pub fn apply(&mut self, event: &AgentEvent) {
        match event {
            AgentEvent::System {
                session_id,
                thread_id,
                ..
            } => {
                if let Some(id) = session_id {
                    self.session_id = Some(id.clone());
                }
                if let Some(id) = thread_id {
                    self.thread_id = Some(id.clone());
                }
            }
            AgentEvent::Text { delta } => self.answer.push_str(delta),
            AgentEvent::Thinking { delta } => self.reasoning.push_str(delta),
            AgentEvent::ToolUse { name, input, .. } => {
                self.tools.push(tool_line(name, input));
            }
            AgentEvent::ToolResult { is_error, .. } => {
                if *is_error {
                    self.tool_errors += 1;
                }
            }
            AgentEvent::FinalText { content } => {
                self.final_answer = Some(content.clone());
                self.answer = content.clone();
            }
            AgentEvent::Usage {
                input_tokens,
                output_tokens,
                ..
            } => {
                self.input_tokens = input_tokens.unwrap_or(0);
                self.output_tokens = output_tokens.unwrap_or(0);
            }
            AgentEvent::Done {
                session_id,
                thread_id,
                termination_reason,
            } => {
                if let Some(id) = session_id {
                    self.session_id = Some(id.clone());
                }
                if let Some(id) = thread_id {
                    self.thread_id = Some(id.clone());
                }
                self.termination = Some(*termination_reason);
                self.finished = true;
            }
            AgentEvent::Error {
                message,
                termination_reason,
            } => {
                self.error = Some(message.clone());
                self.termination = Some(*termination_reason);
                self.finished = true;
            }
        }
    }

    /// The text to show as the answer.
    pub fn best_answer(&self) -> &str {
        if let Some(error) = &self.error {
            return error;
        }
        if let Some(final_answer) = &self.final_answer {
            return final_answer;
        }
        &self.answer
    }

    pub fn session_key(&self) -> Option<&str> {
        self.session_id.as_deref().or(self.thread_id.as_deref())
    }

    /// One-line status for the card footer.
    pub fn footer(&self, elapsed: std::time::Duration) -> String {
        let mut parts = Vec::new();
        parts.push(format!("{:.1}s", elapsed.as_secs_f64()));
        if self.input_tokens > 0 || self.output_tokens > 0 {
            parts.push(format!(
                "{} in / {} out",
                self.input_tokens, self.output_tokens
            ));
        }
        if !self.tools.is_empty() {
            parts.push(format!("{} tool call(s)", self.tools.len()));
        }
        if self.tool_errors > 0 {
            parts.push(format!("{} failed", self.tool_errors));
        }
        parts.join(" · ")
    }
}

fn tool_line(name: &str, input: &serde_json::Value) -> String {
    let detail = input
        .get("command")
        .and_then(|v| v.as_str())
        .or_else(|| input.get("path").and_then(|v| v.as_str()))
        .or_else(|| input.get("file_path").and_then(|v| v.as_str()))
        .unwrap_or_default();
    if detail.is_empty() {
        name.to_string()
    } else {
        format!("{name}: {}", truncate(detail, 80))
    }
}

fn truncate(value: &str, max: usize) -> String {
    if value.chars().count() <= max {
        value.to_string()
    } else {
        let mut out: String = value.chars().take(max).collect();
        out.push('…');
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn folds_text_reasoning_and_tools() {
        let mut view = RunView::default();
        view.apply(&AgentEvent::Text {
            delta: "hello ".into(),
        });
        view.apply(&AgentEvent::Thinking {
            delta: "thinking".into(),
        });
        view.apply(&AgentEvent::ToolUse {
            id: "t1".into(),
            name: "Bash".into(),
            input: json!({ "command": "ls -la" }),
        });
        view.apply(&AgentEvent::Done {
            session_id: Some("s1".into()),
            thread_id: None,
            termination_reason: TerminationReason::Normal,
        });

        assert_eq!(view.best_answer(), "hello ");
        assert_eq!(view.session_key(), Some("s1"));
        assert_eq!(view.tools.len(), 1);
        assert!(view.finished);
        assert!(
            view.footer(std::time::Duration::from_secs(2))
                .contains("2.0s")
        );
    }

    #[test]
    fn final_text_replaces_streamed_commentary() {
        let mut view = RunView::default();
        view.apply(&AgentEvent::Text {
            delta: "working".into(),
        });
        view.apply(&AgentEvent::FinalText {
            content: "answer".into(),
        });
        assert_eq!(view.best_answer(), "answer");
    }

    #[test]
    fn error_becomes_the_answer() {
        let mut view = RunView::default();
        view.apply(&AgentEvent::Error {
            message: "boom".into(),
            termination_reason: TerminationReason::Failed,
        });
        assert_eq!(view.best_answer(), "boom");
        assert!(view.finished);
    }
}
