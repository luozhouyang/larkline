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

    /// Mark a still-running view as interrupted.
    ///
    /// This is the cooperative-stop equivalent of the agent reporting
    /// `terminationReason: interrupted`: the partial answer is preserved and
    /// no error is recorded (an error would replace the answer in
    /// [`RunView::best_answer`] and render a "失败" card). A view that already
    /// reached a terminal state keeps that real terminal, mirroring the TS
    /// `markInterrupted` guard.
    pub fn mark_interrupted(&mut self) {
        if self.finished {
            return;
        }
        self.termination = Some(TerminationReason::Interrupted);
        self.finished = true;
    }

    /// Whether the run ended because the user stopped it.
    pub fn is_interrupted(&self) -> bool {
        self.termination == Some(TerminationReason::Interrupted)
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

    fn applied(events: &[AgentEvent]) -> RunView {
        let mut view = RunView::default();
        for event in events {
            view.apply(event);
        }
        view
    }

    fn text(delta: &str) -> AgentEvent {
        AgentEvent::Text {
            delta: delta.into(),
        }
    }

    fn thinking(delta: &str) -> AgentEvent {
        AgentEvent::Thinking {
            delta: delta.into(),
        }
    }

    fn tool_use(id: &str, name: &str, input: serde_json::Value) -> AgentEvent {
        AgentEvent::ToolUse {
            id: id.into(),
            name: name.into(),
            input,
        }
    }

    fn tool_result(id: &str, output: &str, is_error: bool) -> AgentEvent {
        AgentEvent::ToolResult {
            id: id.into(),
            output: output.into(),
            is_error,
        }
    }

    fn done(reason: TerminationReason) -> AgentEvent {
        AgentEvent::Done {
            session_id: None,
            thread_id: None,
            termination_reason: reason,
        }
    }

    fn error(message: &str, reason: TerminationReason) -> AgentEvent {
        AgentEvent::Error {
            message: message.into(),
            termination_reason: reason,
        }
    }

    #[test]
    fn initial_view_is_running_and_empty() {
        let view = RunView::default();
        assert_eq!(view.best_answer(), "");
        assert!(view.reasoning.is_empty());
        assert!(view.tools.is_empty());
        assert_eq!(view.tool_errors, 0);
        assert!(view.session_key().is_none());
        assert!(view.error.is_none());
        assert_eq!(view.termination, None);
        assert!(!view.finished);
        assert_eq!(view.footer(std::time::Duration::ZERO), "0.0s");
    }

    #[test]
    fn folds_thinking_before_and_after_the_answer() {
        let mut view = applied(&[thinking("checking options")]);
        assert_eq!(view.reasoning, "checking options");
        assert!(!view.finished);

        view.apply(&text("final answer"));
        view.apply(&done(TerminationReason::Normal));
        assert_eq!(view.reasoning, "checking options");
        assert_eq!(view.best_answer(), "final answer");
        assert_eq!(view.termination, Some(TerminationReason::Normal));
        assert!(view.finished);
    }

    #[test]
    fn folds_tool_running_done_and_error_states() {
        let running = applied(&[tool_use("tool-1", "Bash", json!({ "command": "pwd" }))]);
        assert_eq!(running.tools, vec!["Bash: pwd"]);
        assert_eq!(running.tool_errors, 0);
        assert!(!running.finished);
        assert!(
            running
                .footer(std::time::Duration::from_secs(1))
                .contains("1 tool call(s)")
        );

        let completed = applied(&[
            tool_use("tool-1", "Bash", json!({ "command": "pwd" })),
            tool_result("tool-1", "/repo", false),
            done(TerminationReason::Normal),
        ]);
        assert_eq!(completed.tool_errors, 0);
        assert_eq!(completed.termination, Some(TerminationReason::Normal));
        assert!(completed.finished);

        let failed = applied(&[
            tool_use("tool-2", "Read", json!({ "file_path": "/missing.ts" })),
            tool_result("tool-2", "ENOENT", true),
            done(TerminationReason::Normal),
        ]);
        assert_eq!(failed.tool_errors, 1);
        assert!(
            failed
                .footer(std::time::Duration::from_secs(1))
                .contains("1 failed")
        );
        assert!(failed.finished);
    }

    #[test]
    fn keeps_consecutive_tool_calls_in_order() {
        let events = [
            tool_use("tool-1", "Bash", json!({ "command": "pwd" })),
            tool_result("tool-1", "/repo", false),
            tool_use("tool-2", "Read", json!({ "file_path": "/repo/a.ts" })),
            tool_result("tool-2", "a", false),
            tool_use("tool-3", "Edit", json!({ "file_path": "/repo/a.ts" })),
        ];
        let running = applied(&events);
        assert_eq!(
            running.tools,
            vec!["Bash: pwd", "Read: /repo/a.ts", "Edit: /repo/a.ts"]
        );
        assert_eq!(running.tool_errors, 0);
        assert!(!running.finished);

        let mut with_result = events.to_vec();
        with_result.push(tool_result("tool-3", "ok", false));
        with_result.push(done(TerminationReason::Normal));
        let completed = applied(&with_result);
        assert_eq!(completed.tools.len(), 3);
        assert_eq!(completed.tool_errors, 0);
        assert!(completed.finished);
    }

    #[test]
    fn records_done_error_interrupted_and_idle_timeout_terminals() {
        let finished = applied(&[done(TerminationReason::Normal)]);
        assert_eq!(finished.termination, Some(TerminationReason::Normal));
        assert!(finished.finished);

        let failed = applied(&[AgentEvent::Error {
            message: "process failed".into(),
            termination_reason: TerminationReason::Failed,
        }]);
        assert_eq!(failed.best_answer(), "process failed");
        assert_eq!(failed.error.as_deref(), Some("process failed"));
        assert_eq!(failed.termination, Some(TerminationReason::Failed));
        assert!(failed.finished);

        let interrupted = applied(&[text("partial"), done(TerminationReason::Interrupted)]);
        assert_eq!(interrupted.best_answer(), "partial");
        assert_eq!(
            interrupted.termination,
            Some(TerminationReason::Interrupted)
        );
        assert!(interrupted.finished);

        // `done` can itself carry the idle-timeout terminal (the TS
        // `run-state-schema` contract maps it to `idle_timeout`).
        let done_timeout = applied(&[done(TerminationReason::Timeout)]);
        assert_eq!(done_timeout.termination, Some(TerminationReason::Timeout));
        assert!(done_timeout.finished);

        let timed_out = applied(&[AgentEvent::Error {
            message: "run timed out".into(),
            termination_reason: TerminationReason::Timeout,
        }]);
        assert_eq!(timed_out.termination, Some(TerminationReason::Timeout));
        assert!(timed_out.finished);
    }

    #[test]
    fn keeps_local_paths_in_answers_and_tool_lines() {
        let path = "/Users/example/private/customer/repo/secret.txt";
        let view = applied(&[
            text(&format!("I read {path}")),
            tool_use("tool-1", "Read", json!({ "file_path": path })),
            tool_result("tool-1", &format!("content from {path}"), false),
            done(TerminationReason::Normal),
        ]);

        assert!(view.best_answer().contains(path));
        assert_eq!(view.tools, vec![format!("Read: {path}")]);
    }

    // ---------------------------------------------------------------------
    // Additional fold contracts from the TS run-state / card suite.
    // ---------------------------------------------------------------------

    #[test]
    fn footer_reports_elapsed_tokens_tools_and_failures_in_order() {
        let view = applied(&[
            AgentEvent::Usage {
                input_tokens: Some(10),
                output_tokens: Some(20),
                cached_input_tokens: None,
                reasoning_output_tokens: None,
                cost_usd: None,
            },
            tool_use("tool-1", "Bash", json!({ "command": "pwd" })),
            tool_result("tool-1", "/repo", false),
            tool_use("tool-2", "Read", json!({ "file_path": "/missing.ts" })),
            tool_result("tool-2", "ENOENT", true),
        ]);

        assert_eq!(
            view.footer(std::time::Duration::from_millis(1_500)),
            "1.5s · 10 in / 20 out · 2 tool call(s) · 1 failed"
        );
        // With no tokens or tools the footer is just the elapsed time.
        assert_eq!(
            applied(&[done(TerminationReason::Normal)]).footer(std::time::Duration::from_secs(2)),
            "2.0s"
        );
    }

    #[test]
    fn tool_lines_truncate_long_details_with_a_single_ellipsis() {
        let long = "a".repeat(100);
        let view = applied(&[tool_use("tool-1", "Bash", json!({ "command": long }))]);

        assert_eq!(view.tools, vec![format!("Bash: {}…", "a".repeat(80))]);
    }

    #[test]
    fn tool_lines_prefer_command_then_path_then_file_path_then_the_name() {
        let command = applied(&[tool_use(
            "tool-1",
            "Bash",
            json!({ "command": "pwd", "path": "/p", "file_path": "/f" }),
        )]);
        assert_eq!(command.tools, vec!["Bash: pwd"]);

        let path = applied(&[tool_use(
            "tool-1",
            "Read",
            json!({ "path": "/p", "file_path": "/f" }),
        )]);
        assert_eq!(path.tools, vec!["Read: /p"]);

        let file_path = applied(&[tool_use("tool-1", "Read", json!({ "file_path": "/f" }))]);
        assert_eq!(file_path.tools, vec!["Read: /f"]);

        let name_only = applied(&[tool_use("tool-1", "TodoWrite", json!({ "todos": [] }))]);
        assert_eq!(name_only.tools, vec!["TodoWrite"]);
    }

    #[test]
    fn system_and_done_events_keep_both_id_flavours() {
        let system = applied(&[AgentEvent::System {
            session_id: Some("sess-1".into()),
            thread_id: Some("thread-1".into()),
            cwd: None,
            model: None,
        }]);
        assert_eq!(system.session_id.as_deref(), Some("sess-1"));
        assert_eq!(system.thread_id.as_deref(), Some("thread-1"));
        // A System event is informational; it must not finish the view.
        assert!(!system.finished);
        assert_eq!(system.session_key(), Some("sess-1"));

        let mut with_ids = applied(&[done(TerminationReason::Normal)]);
        with_ids.apply(&AgentEvent::System {
            session_id: None,
            thread_id: Some("thread-only".into()),
            cwd: None,
            model: None,
        });
        assert_eq!(with_ids.session_key(), Some("thread-only"));
    }

    #[test]
    fn session_key_prefers_the_session_id_over_the_thread_id() {
        let view = applied(&[AgentEvent::System {
            session_id: Some("sess".into()),
            thread_id: Some("thread".into()),
            cwd: None,
            model: None,
        }]);
        assert_eq!(view.session_key(), Some("sess"));

        let thread_only = applied(&[AgentEvent::System {
            session_id: None,
            thread_id: Some("thread".into()),
            cwd: None,
            model: None,
        }]);
        assert_eq!(thread_only.session_key(), Some("thread"));
    }

    #[test]
    fn an_error_overrides_a_final_answer_for_the_visible_answer() {
        let view = applied(&[
            AgentEvent::FinalText {
                content: "answer".into(),
            },
            AgentEvent::Error {
                message: "boom".into(),
                termination_reason: TerminationReason::Failed,
            },
        ]);

        assert_eq!(view.final_answer.as_deref(), Some("answer"));
        assert_eq!(view.best_answer(), "boom");
        assert_eq!(view.error.as_deref(), Some("boom"));
        assert_eq!(view.termination, Some(TerminationReason::Failed));
        assert!(view.finished);
    }

    #[test]
    fn mark_interrupted_keeps_the_partial_answer_and_a_real_terminal_wins() {
        let mut partial = applied(&[text("partial answer")]);
        partial.mark_interrupted();
        assert_eq!(partial.best_answer(), "partial answer");
        assert_eq!(partial.termination, Some(TerminationReason::Interrupted));
        assert!(partial.is_interrupted());
        assert!(partial.finished);

        // A normal completion that already landed is authoritative.
        let mut done = applied(&[text("answer"), done(TerminationReason::Normal)]);
        done.mark_interrupted();
        assert_eq!(done.termination, Some(TerminationReason::Normal));
        assert!(!done.is_interrupted());
        assert_eq!(done.best_answer(), "answer");

        // An error terminal is not overwritten either.
        let mut failed = applied(&[error("boom", TerminationReason::Failed)]);
        failed.mark_interrupted();
        assert_eq!(failed.termination, Some(TerminationReason::Failed));
        assert_eq!(failed.best_answer(), "boom");
    }

    #[test]
    fn usage_with_missing_numbers_is_zero() {
        let view = applied(&[AgentEvent::Usage {
            input_tokens: None,
            output_tokens: None,
            cached_input_tokens: None,
            reasoning_output_tokens: None,
            cost_usd: None,
        }]);
        assert_eq!(view.input_tokens, 0);
        assert_eq!(view.output_tokens, 0);
        assert_eq!(view.footer(std::time::Duration::ZERO), "0.0s");
    }
}
