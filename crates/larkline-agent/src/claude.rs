//! Claude Code adapter: argv construction and `stream-json` translation.
//!
//! Port of `src/agent/claude/adapter.ts` + `src/agent/claude/stream-json.ts`.
//!
//! The prompt is *never* passed through argv. On Windows `claude` resolves to
//! a `claude.cmd` shim, and `cmd.exe` would interpret `<bridge_context>` in
//! the prompt as redirection. The prompt goes on stdin; the appended system
//! prompt goes through a temp file.

use std::collections::HashSet;
use std::path::Path;

use serde_json::{Map, Value};

use crate::types::{AgentEvent, TerminationReason};

pub const CLAUDE_DEFAULT_PERMISSION_MODE: &str = "bypassPermissions";

#[derive(Debug, Clone, Default)]
pub struct ClaudeArgsInput<'a> {
    pub permission_mode: Option<&'a str>,
    pub system_prompt_file: Option<&'a Path>,
    pub session_id: Option<&'a str>,
    pub model: Option<&'a str>,
}

pub fn build_claude_args(input: &ClaudeArgsInput<'_>) -> Vec<String> {
    let mut args: Vec<String> = vec![
        "-p".into(),
        "--output-format".into(),
        "stream-json".into(),
        "--verbose".into(),
        "--include-partial-messages".into(),
        "--permission-mode".into(),
        input
            .permission_mode
            .unwrap_or(CLAUDE_DEFAULT_PERMISSION_MODE)
            .into(),
    ];
    if let Some(path) = input.system_prompt_file {
        args.push("--append-system-prompt-file".into());
        args.push(path.to_string_lossy().into_owned());
    }
    if let Some(session_id) = input.session_id {
        args.push("--resume".into());
        args.push(session_id.into());
    }
    if let Some(model) = input.model {
        args.push("--model".into());
        args.push(model.into());
    }
    args
}

/// Stateful translator for `--output-format stream-json`.
///
/// With `--include-partial-messages`, thinking arrives twice: as
/// `content_block_delta/thinking_delta` chunks and again inside the completed
/// `assistant` message. The deltas are streamed; the duplicates are skipped
/// by message id.
#[derive(Debug, Default)]
pub struct ClaudeStreamTranslator {
    current_message_id: Option<String>,
    partial_thinking_messages: HashSet<String>,
}

impl ClaudeStreamTranslator {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn translate(&mut self, raw: &Value) -> Vec<AgentEvent> {
        let mut out = Vec::new();
        let Some(obj) = raw.as_object() else {
            return out;
        };
        match string_field(obj, "type").as_deref() {
            Some("stream_event") => {
                if let Some(event) = obj.get("event") {
                    self.translate_stream_event(event, &mut out);
                }
            }
            Some("system") if string_field(obj, "subtype").as_deref() == Some("init") => {
                out.push(AgentEvent::System {
                    session_id: string_field(obj, "session_id"),
                    thread_id: None,
                    cwd: string_field(obj, "cwd"),
                    model: string_field(obj, "model"),
                });
            }
            Some("assistant") => self.translate_assistant(obj, &mut out),
            Some("user") => self.translate_user(obj, &mut out),
            Some("result") => {
                if let Some(usage) = obj.get("usage").and_then(Value::as_object) {
                    out.push(AgentEvent::Usage {
                        input_tokens: number_field(usage, "input_tokens"),
                        output_tokens: number_field(usage, "output_tokens"),
                        cached_input_tokens: number_field(usage, "cache_read_input_tokens"),
                        reasoning_output_tokens: None,
                        cost_usd: obj.get("total_cost_usd").and_then(Value::as_f64),
                    });
                }
                out.push(AgentEvent::Done {
                    session_id: string_field(obj, "session_id"),
                    thread_id: None,
                    termination_reason: TerminationReason::Normal,
                });
            }
            _ => {}
        }
        out
    }

    fn translate_stream_event(&mut self, event: &Value, out: &mut Vec<AgentEvent>) {
        let Some(event) = event.as_object() else {
            return;
        };
        match string_field(event, "type").as_deref() {
            Some("message_start") => {
                self.current_message_id = event
                    .get("message")
                    .and_then(Value::as_object)
                    .and_then(|m| string_field(m, "id"));
            }
            Some("content_block_delta") => {
                let Some(delta) = event.get("delta").and_then(Value::as_object) else {
                    return;
                };
                if string_field(delta, "type").as_deref() != Some("thinking_delta") {
                    return;
                }
                let Some(text) = string_field(delta, "thinking") else {
                    return;
                };
                if text.is_empty() {
                    return;
                }
                if let Some(message_id) = &self.current_message_id {
                    self.partial_thinking_messages.insert(message_id.clone());
                }
                out.push(AgentEvent::Thinking { delta: text });
            }
            _ => {}
        }
    }

    fn translate_assistant(&mut self, obj: &Map<String, Value>, out: &mut Vec<AgentEvent>) {
        let Some(message) = obj.get("message").and_then(Value::as_object) else {
            return;
        };
        let Some(content) = message.get("content").and_then(Value::as_array) else {
            return;
        };
        let message_id = string_field(message, "id");
        for block in content {
            let Some(block) = block.as_object() else {
                continue;
            };
            match string_field(block, "type").as_deref() {
                Some("text") => {
                    if let Some(text) = string_field(block, "text") {
                        if !text.is_empty() {
                            out.push(AgentEvent::Text { delta: text });
                        }
                    }
                }
                Some("thinking") => {
                    let Some(text) = string_field(block, "thinking") else {
                        continue;
                    };
                    if text.is_empty() {
                        continue;
                    }
                    if message_id
                        .as_ref()
                        .map(|id| self.partial_thinking_messages.contains(id))
                        .unwrap_or(false)
                    {
                        continue;
                    }
                    out.push(AgentEvent::Thinking { delta: text });
                }
                Some("tool_use") => {
                    if let (Some(id), Some(name)) =
                        (string_field(block, "id"), string_field(block, "name"))
                    {
                        out.push(AgentEvent::ToolUse {
                            id,
                            name,
                            input: block.get("input").cloned().unwrap_or(Value::Null),
                        });
                    }
                }
                _ => {}
            }
        }
    }

    fn translate_user(&mut self, obj: &Map<String, Value>, out: &mut Vec<AgentEvent>) {
        let Some(message) = obj.get("message").and_then(Value::as_object) else {
            return;
        };
        let Some(content) = message.get("content").and_then(Value::as_array) else {
            return;
        };
        for block in content {
            let Some(block) = block.as_object() else {
                continue;
            };
            if string_field(block, "type").as_deref() != Some("tool_result") {
                continue;
            }
            let Some(id) = string_field(block, "tool_use_id") else {
                continue;
            };
            let output = match block.get("content") {
                Some(Value::String(text)) => text.clone(),
                Some(other) => serde_json::to_string(other).unwrap_or_default(),
                None => String::new(),
            };
            out.push(AgentEvent::ToolResult {
                id,
                output,
                is_error: block.get("is_error").and_then(Value::as_bool) == Some(true),
            });
        }
    }
}

pub(crate) fn string_field(obj: &Map<String, Value>, key: &str) -> Option<String> {
    obj.get(key).and_then(Value::as_str).map(str::to_string)
}

pub(crate) fn number_field(obj: &Map<String, Value>, key: &str) -> Option<u64> {
    obj.get(key).and_then(Value::as_u64)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn args_put_prompt_on_stdin() {
        let args = build_claude_args(&ClaudeArgsInput {
            permission_mode: None,
            system_prompt_file: Some(Path::new("/tmp/sys.txt")),
            session_id: Some("s-1"),
            model: Some("claude-sonnet-4-5"),
        });
        assert_eq!(args[0], "-p");
        assert!(!args.iter().any(|a| a.contains("bridge_context")));
        assert!(
            args.windows(2)
                .any(|w| w == ["--append-system-prompt-file", "/tmp/sys.txt"])
        );
        assert!(args.windows(2).any(|w| w == ["--resume", "s-1"]));
        assert!(
            args.windows(2)
                .any(|w| w == ["--model", "claude-sonnet-4-5"])
        );
        assert!(
            args.windows(2)
                .any(|w| w == ["--permission-mode", "bypassPermissions"])
        );
    }

    #[test]
    fn init_event_reports_session() {
        let mut translator = ClaudeStreamTranslator::new();
        let events = translator.translate(&json!({
            "type": "system",
            "subtype": "init",
            "session_id": "sess-1",
            "cwd": "/work",
            "model": "opus"
        }));
        assert_eq!(
            events,
            vec![AgentEvent::System {
                session_id: Some("sess-1".into()),
                thread_id: None,
                cwd: Some("/work".into()),
                model: Some("opus".into()),
            }]
        );
    }

    #[test]
    fn partial_thinking_is_not_duplicated() {
        let mut translator = ClaudeStreamTranslator::new();
        let first = translator.translate(&json!({
            "type": "stream_event",
            "event": { "type": "message_start", "message": { "id": "m1" } }
        }));
        assert!(first.is_empty());

        let delta = translator.translate(&json!({
            "type": "stream_event",
            "event": {
                "type": "content_block_delta",
                "delta": { "type": "thinking_delta", "thinking": "hmm" }
            }
        }));
        assert_eq!(
            delta,
            vec![AgentEvent::Thinking {
                delta: "hmm".into()
            }]
        );

        let completed = translator.translate(&json!({
            "type": "assistant",
            "message": {
                "id": "m1",
                "content": [
                    { "type": "thinking", "thinking": "hmm" },
                    { "type": "text", "text": "done" }
                ]
            }
        }));
        assert_eq!(
            completed,
            vec![AgentEvent::Text {
                delta: "done".into()
            }]
        );
    }

    #[test]
    fn tool_use_and_result() {
        let mut translator = ClaudeStreamTranslator::new();
        let events = translator.translate(&json!({
            "type": "assistant",
            "message": {
                "id": "m2",
                "content": [{ "type": "tool_use", "id": "t1", "name": "Bash", "input": { "command": "ls" } }]
            }
        }));
        assert!(matches!(&events[0], AgentEvent::ToolUse { name, .. } if name == "Bash"));

        let results = translator.translate(&json!({
            "type": "user",
            "message": {
                "content": [{ "type": "tool_result", "tool_use_id": "t1", "content": "a\nb", "is_error": true }]
            }
        }));
        assert_eq!(
            results,
            vec![AgentEvent::ToolResult {
                id: "t1".into(),
                output: "a\nb".into(),
                is_error: true
            }]
        );
    }

    #[test]
    fn result_emits_usage_then_done() {
        let mut translator = ClaudeStreamTranslator::new();
        let events = translator.translate(&json!({
            "type": "result",
            "session_id": "s",
            "usage": { "input_tokens": 10, "output_tokens": 5, "cache_read_input_tokens": 3 },
            "total_cost_usd": 0.02
        }));
        assert_eq!(events.len(), 2);
        match &events[0] {
            AgentEvent::Usage {
                input_tokens,
                output_tokens,
                cached_input_tokens,
                cost_usd,
                ..
            } => {
                assert_eq!(*input_tokens, Some(10));
                assert_eq!(*output_tokens, Some(5));
                assert_eq!(*cached_input_tokens, Some(3));
                assert_eq!(*cost_usd, Some(0.02));
            }
            other => panic!("unexpected: {other:?}"),
        }
        assert!(matches!(
            &events[1],
            AgentEvent::Done {
                termination_reason: TerminationReason::Normal,
                ..
            }
        ));
    }

    /// TS `translateEvent` builds a fresh translator per call; many of the
    /// ported cases only need that stateless view.
    fn translate(raw: Value) -> Vec<AgentEvent> {
        ClaudeStreamTranslator::new().translate(&raw)
    }

    #[test]
    fn init_without_optional_fields_has_no_thread_id() {
        // Port of `translates system init metadata` (the partial identity
        // branch): a Claude session id never leaks into Codex's thread field.
        let events = translate(json!({
            "type": "system",
            "subtype": "init",
            "session_id": "sess-1"
        }));
        assert_eq!(
            events,
            vec![AgentEvent::System {
                session_id: Some("sess-1".into()),
                thread_id: None,
                cwd: None,
                model: None,
            }]
        );
    }

    #[test]
    fn assistant_blocks_keep_text_thinking_and_tool_order() {
        let events = translate(json!({
            "type": "assistant",
            "message": {
                "content": [
                    { "type": "text", "text": "hello" },
                    { "type": "thinking", "thinking": "checking" },
                    { "type": "tool_use", "id": "tool-1", "name": "Bash", "input": { "command": "pwd" } }
                ]
            }
        }));
        assert_eq!(
            events,
            vec![
                AgentEvent::Text {
                    delta: "hello".into()
                },
                AgentEvent::Thinking {
                    delta: "checking".into()
                },
                AgentEvent::ToolUse {
                    id: "tool-1".into(),
                    name: "Bash".into(),
                    input: json!({ "command": "pwd" }),
                },
            ]
        );
    }

    #[test]
    fn user_tool_results_cover_structured_output_and_error_flags() {
        let events = translate(json!({
            "type": "user",
            "message": {
                "content": [
                    { "type": "tool_result", "tool_use_id": "tool-1", "content": "ok" },
                    {
                        "type": "tool_result",
                        "tool_use_id": "tool-2",
                        "content": [{ "type": "text", "text": "bad" }],
                        "is_error": true
                    }
                ]
            }
        }));
        assert_eq!(
            events,
            vec![
                AgentEvent::ToolResult {
                    id: "tool-1".into(),
                    output: "ok".into(),
                    is_error: false,
                },
                AgentEvent::ToolResult {
                    id: "tool-2".into(),
                    // Non-string content is re-serialized as JSON, exactly like
                    // `JSON.stringify(block.content)` in the TypeScript port.
                    output: r#"[{"type":"text","text":"bad"}]"#.into(),
                    is_error: true,
                },
            ]
        );
    }

    #[test]
    fn result_without_usage_only_emits_done() {
        let events = translate(json!({ "type": "result", "session_id": "sess-2" }));
        assert_eq!(
            events,
            vec![AgentEvent::Done {
                session_id: Some("sess-2".into()),
                thread_id: None,
                termination_reason: TerminationReason::Normal,
            }]
        );
    }

    #[test]
    fn partial_thinking_streams_and_the_duplicate_assistant_block_is_skipped() {
        let mut translator = ClaudeStreamTranslator::new();
        let raw_events = [
            json!({
                "type": "stream_event",
                "event": { "type": "message_start", "message": { "id": "msg-1" } }
            }),
            json!({
                "type": "stream_event",
                "event": {
                    "type": "content_block_delta",
                    "delta": { "type": "thinking_delta", "thinking": "first " }
                }
            }),
            json!({
                "type": "stream_event",
                "event": {
                    "type": "content_block_delta",
                    "delta": { "type": "thinking_delta", "thinking": "second" }
                }
            }),
            // Partial text deltas are ignored; the completed assistant message
            // is still the single source of user-visible text.
            json!({
                "type": "stream_event",
                "event": { "type": "content_block_delta", "delta": { "type": "text_delta", "text": "ignored" } }
            }),
            json!({
                "type": "assistant",
                "message": {
                    "id": "msg-1",
                    "content": [
                        { "type": "thinking", "thinking": "first second" },
                        { "type": "text", "text": "answer" }
                    ]
                }
            }),
        ];

        let mut out = Vec::new();
        for raw in raw_events {
            out.extend(translator.translate(&raw));
        }
        assert_eq!(
            out,
            vec![
                AgentEvent::Thinking {
                    delta: "first ".into()
                },
                AgentEvent::Thinking {
                    delta: "second".into()
                },
                AgentEvent::Text {
                    delta: "answer".into()
                },
            ]
        );
    }

    #[test]
    fn assistant_thinking_is_emitted_without_a_partial_delta() {
        let events = translate(json!({
            "type": "assistant",
            "message": {
                "id": "msg-2",
                "content": [{ "type": "thinking", "thinking": "whole block" }]
            }
        }));
        assert_eq!(
            events,
            vec![AgentEvent::Thinking {
                delta: "whole block".into()
            }]
        );
    }

    #[test]
    fn unknown_empty_and_incomplete_raw_events_are_ignored() {
        assert!(translate(Value::Null).is_empty());
        // Empty text blocks never produce a visible delta.
        assert!(
            translate(json!({
                "type": "assistant",
                "message": { "content": [{ "type": "text", "text": "" }] }
            }))
            .is_empty()
        );
        // tool_use without a name is incomplete and dropped.
        assert!(
            translate(json!({
                "type": "assistant",
                "message": { "content": [{ "type": "tool_use", "id": "t" }] }
            }))
            .is_empty()
        );
        // Only the `init` system subtype is a session announcement.
        assert!(translate(json!({ "type": "system", "subtype": "other" })).is_empty());
    }
}
