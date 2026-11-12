//! `codex exec --json` argument construction.
//!
//! Direct port of `src/agent/codex/argv.ts`. Argument order is load-bearing:
//! `exec` flags must precede `resume`/`-`, and the prompt itself is always fed
//! on stdin (`-`), never as argv, so no shell metacharacters reach a shim.

use crate::types::SandboxMode;

#[derive(Debug, Clone, Default)]
pub struct CodexArgsInput {
    pub cwd: String,
    pub sandbox: SandboxMode,
    pub thread_id: Option<String>,
    pub images: Vec<String>,
    pub ignore_user_config: bool,
    /// `None` means "ignore rules" (the default); `Some(false)` keeps them.
    pub ignore_rules: Option<bool>,
    pub model: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CodexArgError {
    InvalidSandbox,
}

impl std::fmt::Display for CodexArgError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CodexArgError::InvalidSandbox => f.write_str("unsafe sandbox mode"),
        }
    }
}

impl std::error::Error for CodexArgError {}

pub fn build_codex_args(input: &CodexArgsInput) -> Result<Vec<String>, CodexArgError> {
    // `SandboxMode` is already a closed enum, but keep the same guard the
    // TypeScript builder had so a future stringly-typed caller cannot bypass it.
    if SandboxMode::parse(input.sandbox.as_str()) != Some(input.sandbox) {
        return Err(CodexArgError::InvalidSandbox);
    }

    let mut global: Vec<String> = Vec::new();
    global.push("--sandbox".into());
    global.push(input.sandbox.as_str().into());
    if let Some(model) = &input.model {
        global.push("--model".into());
        global.push(model.clone());
    }
    global.push("-c".into());
    global.push("approval_policy=\"never\"".into());
    global.push("-c".into());
    global.push("shell_environment_policy.inherit=\"all\"".into());
    if input.ignore_user_config {
        global.push("--ignore-user-config".into());
    }
    if input.ignore_rules != Some(false) {
        global.push("--ignore-rules".into());
    }
    global.push("--skip-git-repo-check".into());
    global.push("-C".into());
    global.push(input.cwd.clone());

    let mut image_flags: Vec<String> = Vec::new();
    for path in &input.images {
        image_flags.push("--image".into());
        image_flags.push(path.clone());
    }

    let mut args: Vec<String> = Vec::new();
    match &input.thread_id {
        Some(thread_id) => {
            args.push("exec".into());
            args.extend(global);
            args.push("resume".into());
            args.push("--json".into());
            args.extend(image_flags);
            args.push(thread_id.clone());
            args.push("-".into());
        }
        None => {
            args.push("exec".into());
            args.push("--json".into());
            args.extend(global);
            let had_images = !image_flags.is_empty();
            args.extend(image_flags);
            if had_images {
                args.push("--".into());
            }
            args.push("-".into());
        }
    }
    Ok(args)
}

// ---------------------------------------------------------------------------
// `codex exec --json` stream translation
// ---------------------------------------------------------------------------

use std::collections::HashSet;

use serde_json::{Map, Value};

use crate::types::{AgentEvent, TerminationReason, truncate_chars};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CodexFinishReason {
    Failed,
    Interrupted,
    Timeout,
}

/// Stateful translator for Codex's line-delimited JSON.
///
/// Port of `src/agent/codex/jsonl.ts`. The subtle parts are deliberate:
///
/// * Codex announces a final message in more than one shape across versions;
///   an identical repeat is deduplicated rather than streamed twice.
/// * A message that arrives before a tool event is progress commentary, so it
///   is flushed as `Text` ahead of the tool event.
/// * Anything other than a terminal event sets `terminal`, which caps the
///   stream and lets the caller report a clean failure instead of hanging.
#[derive(Debug, Default)]
pub struct CodexJsonlTranslator {
    thread_id: Option<String>,
    terminal: bool,
    last_non_terminal_error: Option<String>,
    pending_agent_message: Option<String>,
    started_items: HashSet<String>,
    seen_reasoning: HashSet<String>,
    unknown_events: u32,
    anomalies: u32,
}

impl CodexJsonlTranslator {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn translate(&mut self, raw: &Value) -> Vec<AgentEvent> {
        if self.terminal {
            return Vec::new();
        }
        let Some(obj) = raw.as_object() else {
            self.anomalies += 1;
            return Vec::new();
        };
        let Some(event_type) = string_field(obj, "type") else {
            self.anomalies += 1;
            return Vec::new();
        };

        match event_type.as_str() {
            "thread.started" => self.translate_thread_started(obj),
            "turn.started" => Vec::new(),
            "item.started" => {
                let events = self.translate_item_started(obj);
                self.prepend_pending_text(events)
            }
            "item.completed" => self.translate_item_completed(obj),
            "agent_message" => self.translate_agent_message(obj),
            "turn.completed" => self.translate_turn_completed(obj),
            "turn.failed" => {
                let events = self.translate_terminal_error(obj, "codex turn failed");
                self.prepend_pending_text(events)
            }
            "error" => {
                self.translate_non_terminal_error(obj, "codex error");
                Vec::new()
            }
            other => {
                self.unknown_events += 1;
                tracing::warn!(event_type = other, "unknown codex event");
                Vec::new()
            }
        }
    }

    /// Close the stream when the child exits without a terminal event.
    pub fn finish(&mut self, reason: CodexFinishReason) -> Vec<AgentEvent> {
        if self.terminal {
            return Vec::new();
        }
        self.terminal = true;
        match reason {
            CodexFinishReason::Failed => {
                let detail = self
                    .last_non_terminal_error
                    .as_ref()
                    .map(|e| format!(": {e}"))
                    .unwrap_or_default();
                let message = truncate_chars(
                    &format!("codex stream ended before a terminal event{detail}"),
                    4096,
                );
                self.prepend_pending_text(vec![AgentEvent::Error {
                    message,
                    termination_reason: TerminationReason::Failed,
                }])
            }
            CodexFinishReason::Interrupted | CodexFinishReason::Timeout => {
                let termination_reason = match reason {
                    CodexFinishReason::Timeout => TerminationReason::Timeout,
                    _ => TerminationReason::Interrupted,
                };
                self.prepend_pending_text(vec![AgentEvent::Done {
                    session_id: None,
                    thread_id: self.thread_id.clone(),
                    termination_reason,
                }])
            }
        }
    }

    pub fn fail(&mut self, message: &str) -> Vec<AgentEvent> {
        if self.terminal {
            return Vec::new();
        }
        self.terminal = true;
        self.prepend_pending_text(vec![AgentEvent::Error {
            message: truncate_chars(message, 4096),
            termination_reason: TerminationReason::Failed,
        }])
    }

    pub fn unknown_events(&self) -> u32 {
        self.unknown_events
    }

    pub fn anomalies(&self) -> u32 {
        self.anomalies
    }

    pub fn terminal_emitted(&self) -> bool {
        self.terminal
    }

    fn translate_thread_started(&mut self, obj: &Map<String, Value>) -> Vec<AgentEvent> {
        let Some(thread_id) =
            string_field(obj, "thread_id").or_else(|| string_field(obj, "threadId"))
        else {
            self.anomalies += 1;
            return Vec::new();
        };
        self.thread_id = Some(thread_id.clone());
        vec![AgentEvent::System {
            session_id: None,
            thread_id: Some(thread_id),
            cwd: None,
            model: None,
        }]
    }

    fn translate_item_started(&mut self, obj: &Map<String, Value>) -> Vec<AgentEvent> {
        let Some(item) = obj.get("item").and_then(Value::as_object) else {
            return Vec::new();
        };
        if string_field(item, "type").as_deref() != Some("command_execution") {
            return Vec::new();
        }
        let Some(id) = string_field(item, "id") else {
            self.anomalies += 1;
            return Vec::new();
        };
        self.started_items.insert(id.clone());
        vec![AgentEvent::ToolUse {
            id,
            name: "command_execution".into(),
            input: serde_json::json!({
                "command": string_field(item, "command").unwrap_or_default()
            }),
        }]
    }

    fn translate_item_completed(&mut self, obj: &Map<String, Value>) -> Vec<AgentEvent> {
        let Some(item) = obj.get("item").and_then(Value::as_object) else {
            return Vec::new();
        };
        match string_field(item, "type").as_deref() {
            Some("agent_message") => {
                let message = string_field(item, "text").or_else(|| string_field(item, "message"));
                match message {
                    Some(message) => self.queue_agent_message(message),
                    None => Vec::new(),
                }
            }
            Some("reasoning") | Some("reasoning_summary") => {
                let text = string_field(item, "text").or_else(|| string_field(item, "summary"));
                let Some(text) = text.filter(|t| !t.trim().is_empty()) else {
                    return Vec::new();
                };
                let id = string_field(item, "id");
                let text_key = format!("text:{text}");
                let id_text_key = format!("idtext:{text}");
                if let Some(id) = id {
                    if self.seen_reasoning.contains(&format!("id:{id}"))
                        || self.seen_reasoning.contains(&text_key)
                    {
                        return Vec::new();
                    }
                    self.seen_reasoning.insert(format!("id:{id}"));
                    self.seen_reasoning.insert(id_text_key);
                } else {
                    if self.seen_reasoning.contains(&text_key)
                        || self.seen_reasoning.contains(&id_text_key)
                    {
                        return Vec::new();
                    }
                    self.seen_reasoning.insert(text_key);
                }
                self.prepend_pending_text(vec![AgentEvent::Thinking { delta: text }])
            }
            Some("command_execution") => {
                let Some(id) = string_field(item, "id") else {
                    self.anomalies += 1;
                    return Vec::new();
                };
                if !self.started_items.remove(&id) {
                    self.anomalies += 1;
                }
                let exit_code = item.get("exit_code").and_then(Value::as_i64);
                let output = string_field(item, "output")
                    .or_else(|| string_field(item, "aggregated_output"))
                    .or_else(|| string_field(item, "stdout"))
                    .unwrap_or_default();
                self.prepend_pending_text(vec![AgentEvent::ToolResult {
                    id,
                    output,
                    is_error: exit_code.map(|code| code != 0).unwrap_or(false),
                }])
            }
            _ => Vec::new(),
        }
    }

    fn translate_agent_message(&mut self, obj: &Map<String, Value>) -> Vec<AgentEvent> {
        let message = string_field(obj, "message").or_else(|| string_field(obj, "text"));
        match message {
            Some(message) => self.queue_agent_message(message),
            None => Vec::new(),
        }
    }

    fn translate_turn_completed(&mut self, obj: &Map<String, Value>) -> Vec<AgentEvent> {
        self.terminal = true;
        let mut events = Vec::new();
        if let Some(pending) = self.pending_agent_message.take() {
            events.push(AgentEvent::FinalText { content: pending });
        }
        if let Some(usage) = obj.get("usage").and_then(Value::as_object) {
            events.push(AgentEvent::Usage {
                input_tokens: number_field(usage, "input_tokens")
                    .or_else(|| number_field(usage, "inputTokens")),
                output_tokens: number_field(usage, "output_tokens")
                    .or_else(|| number_field(usage, "outputTokens")),
                cached_input_tokens: number_field(usage, "cached_input_tokens")
                    .or_else(|| number_field(usage, "cachedInputTokens")),
                reasoning_output_tokens: number_field(usage, "reasoning_output_tokens")
                    .or_else(|| number_field(usage, "reasoningOutputTokens")),
                cost_usd: None,
            });
        }
        events.push(AgentEvent::Done {
            session_id: None,
            thread_id: self.thread_id.clone(),
            termination_reason: TerminationReason::Normal,
        });
        events
    }

    fn queue_agent_message(&mut self, message: String) -> Vec<AgentEvent> {
        if self.pending_agent_message.as_deref() == Some(message.as_str()) {
            return Vec::new();
        }
        let mut events = Vec::new();
        if let Some(previous) = self.pending_agent_message.take() {
            events.push(AgentEvent::Text { delta: previous });
        }
        self.pending_agent_message = Some(message);
        events
    }

    fn prepend_pending_text(&mut self, mut events: Vec<AgentEvent>) -> Vec<AgentEvent> {
        if events.is_empty() {
            return events;
        }
        let Some(pending) = self.pending_agent_message.take() else {
            return events;
        };
        let mut out = vec![AgentEvent::Text { delta: pending }];
        out.append(&mut events);
        out
    }

    fn translate_terminal_error(
        &mut self,
        obj: &Map<String, Value>,
        fallback: &str,
    ) -> Vec<AgentEvent> {
        self.terminal = true;
        vec![AgentEvent::Error {
            message: truncate_chars(&error_message(obj, fallback), 4096),
            termination_reason: TerminationReason::Failed,
        }]
    }

    fn translate_non_terminal_error(&mut self, obj: &Map<String, Value>, fallback: &str) {
        let message = error_message(obj, fallback);
        tracing::warn!(message = %truncate_chars(&message, 500), "codex error event");
        self.last_non_terminal_error = Some(message);
    }
}

fn string_field(obj: &Map<String, Value>, key: &str) -> Option<String> {
    obj.get(key).and_then(Value::as_str).map(str::to_string)
}

fn number_field(obj: &Map<String, Value>, key: &str) -> Option<u64> {
    obj.get(key).and_then(Value::as_u64)
}

fn error_message(obj: &Map<String, Value>, fallback: &str) -> String {
    let nested = obj.get("error").and_then(Value::as_object);
    string_field(obj, "message")
        .or_else(|| nested.and_then(|e| string_field(e, "message")))
        .or_else(|| obj.get("error").and_then(Value::as_str).map(str::to_string))
        .unwrap_or_else(|| fallback.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn base() -> CodexArgsInput {
        CodexArgsInput {
            cwd: "/work".into(),
            sandbox: SandboxMode::DangerFullAccess,
            ..Default::default()
        }
    }

    #[test]
    fn fresh_run_matches_typescript_order() {
        let args = build_codex_args(&base()).unwrap();
        assert_eq!(
            args,
            vec![
                "exec",
                "--json",
                "--sandbox",
                "danger-full-access",
                "-c",
                "approval_policy=\"never\"",
                "-c",
                "shell_environment_policy.inherit=\"all\"",
                "--ignore-rules",
                "--skip-git-repo-check",
                "-C",
                "/work",
                "-",
            ]
        );
    }

    #[test]
    fn resume_places_json_after_resume() {
        let mut input = base();
        input.thread_id = Some("t-1".into());
        let args = build_codex_args(&input).unwrap();
        let resume = args.iter().position(|a| a == "resume").unwrap();
        assert_eq!(args[resume + 1], "--json");
        assert_eq!(args.last().unwrap(), "-");
    }

    #[test]
    fn images_add_separator_only_when_present() {
        let mut input = base();
        input.images = vec!["/tmp/a.png".into()];
        let args = build_codex_args(&input).unwrap();
        let dash = args.iter().position(|a| a == "--").unwrap();
        assert_eq!(args[dash - 1], "/tmp/a.png");
        assert_eq!(args[dash + 1], "-");
    }

    #[test]
    fn model_and_ignore_user_config() {
        let mut input = base();
        input.model = Some("gpt-5".into());
        input.ignore_user_config = true;
        input.ignore_rules = Some(false);
        let args = build_codex_args(&input).unwrap();
        assert!(args.windows(2).any(|w| w == ["--model", "gpt-5"]));
        assert!(args.iter().any(|a| a == "--ignore-user-config"));
        assert!(!args.iter().any(|a| a == "--ignore-rules"));
    }

    #[test]
    fn thread_started_reports_id() {
        let mut t = CodexJsonlTranslator::new();
        let events = t.translate(&json!({ "type": "thread.started", "thread_id": "th-1" }));
        assert!(matches!(
            &events[0],
            AgentEvent::System { thread_id: Some(id), .. } if id == "th-1"
        ));
    }

    #[test]
    fn duplicate_agent_message_is_dropped() {
        let mut t = CodexJsonlTranslator::new();
        assert!(
            t.translate(&json!({ "type": "agent_message", "message": "hello" }))
                .is_empty()
        );
        assert!(
            t.translate(&json!({ "type": "agent_message", "message": "hello" }))
                .is_empty()
        );

        let events = t.translate(&json!({
            "type": "turn.completed",
            "usage": { "input_tokens": 3, "output_tokens": 4 }
        }));
        assert_eq!(
            events[0],
            AgentEvent::FinalText {
                content: "hello".into()
            }
        );
        assert!(matches!(
            events[1],
            AgentEvent::Usage {
                input_tokens: Some(3),
                output_tokens: Some(4),
                ..
            }
        ));
        assert!(matches!(
            events[2],
            AgentEvent::Done {
                termination_reason: TerminationReason::Normal,
                ..
            }
        ));
    }

    #[test]
    fn pending_message_flushes_as_text_before_tool_result() {
        let mut t = CodexJsonlTranslator::new();
        let _ = t.translate(&json!({ "type": "agent_message", "message": "working" }));
        let events = t.translate(&json!({
            "type": "item.completed",
            "item": { "type": "command_execution", "id": "c1", "exit_code": 0, "output": "ok" }
        }));
        assert_eq!(
            events[0],
            AgentEvent::Text {
                delta: "working".into()
            }
        );
        assert!(matches!(
            &events[1],
            AgentEvent::ToolResult {
                is_error: false,
                ..
            }
        ));
    }

    #[test]
    fn reasoning_is_deduped_by_text() {
        let mut t = CodexJsonlTranslator::new();
        let first = t.translate(&json!({
            "type": "item.completed",
            "item": { "type": "reasoning", "text": "thinking" }
        }));
        assert_eq!(
            first,
            vec![AgentEvent::Thinking {
                delta: "thinking".into()
            }]
        );
        let again = t.translate(&json!({
            "type": "item.completed",
            "item": { "type": "reasoning_summary", "text": "thinking" }
        }));
        assert!(again.is_empty());
    }

    #[test]
    fn finish_without_terminal_event_is_an_error() {
        let mut t = CodexJsonlTranslator::new();
        let _ = t.translate(&json!({ "type": "error", "message": "boom" }));
        let events = t.finish(CodexFinishReason::Failed);
        match &events[0] {
            AgentEvent::Error {
                message,
                termination_reason,
            } => {
                assert!(message.contains("boom"));
                assert_eq!(*termination_reason, TerminationReason::Failed);
            }
            other => panic!("unexpected: {other:?}"),
        }
        assert!(t.terminal_emitted());
        assert!(t.finish(CodexFinishReason::Failed).is_empty());
    }

    #[test]
    fn unknown_events_are_counted_not_fatal() {
        let mut t = CodexJsonlTranslator::new();
        assert!(t.translate(&json!({ "type": "future.event" })).is_empty());
        assert_eq!(t.unknown_events(), 1);
        assert!(!t.terminal_emitted());
    }
}
