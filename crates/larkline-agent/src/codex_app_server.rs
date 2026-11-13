//! `codex app-server` JSON-RPC protocol.
//!
//! `codex exec` has no inbound channel for approvals, so the bridge uses the
//! app-server transport whenever Codex runs under a sandbox that can ask for
//! approval (read-only / workspace-write). The server speaks newline-delimited
//! JSON-RPC without a `jsonrpc` field: requests carry `id` + `method`, responses
//! carry `id` + `result`/`error`, and notifications carry `method` only.
//!
//! This module is deliberately runtime-free: it builds frames and translates
//! notifications/approval requests into [`AgentEvent`]s. The bridge owns the
//! child process and the stdin/stdout plumbing.

use serde_json::{Value, json};

use crate::types::{AgentEvent, SandboxMode, TerminationReason, truncate_chars};

/// Client name/version reported by `initialize`.
pub const CLIENT_NAME: &str = "larkline";
pub const CLIENT_VERSION: &str = env!("CARGO_PKG_VERSION");

/// Approval policy for threads the bridge drives: Codex asks only when it
/// needs to leave its sandbox or perform a sensitive action.
pub const APPROVAL_POLICY: &str = "on-request";

/// Everything needed to start or resume a Codex thread.
#[derive(Debug, Clone)]
pub struct CodexThreadConfig {
    pub cwd: String,
    pub sandbox: SandboxMode,
    pub model: Option<String>,
    /// Bridge instructions; sent as `developerInstructions` so Codex keeps its
    /// own base instructions.
    pub developer_instructions: Option<String>,
}

pub fn initialize_request(id: u64) -> Value {
    json!({
        "id": id,
        "method": "initialize",
        "params": {
            "clientInfo": { "name": CLIENT_NAME, "version": CLIENT_VERSION },
            "capabilities": { "experimentalApi": true }
        }
    })
}

pub fn thread_start_request(id: u64, config: &CodexThreadConfig) -> Value {
    let mut params = json!({
        "cwd": config.cwd,
        "sandbox": config.sandbox.as_str(),
        "approvalPolicy": APPROVAL_POLICY,
        "ephemeral": false,
    });
    if let Some(model) = &config.model {
        params["model"] = json!(model);
    }
    if let Some(instructions) = &config.developer_instructions {
        params["developerInstructions"] = json!(instructions);
    }
    json!({ "id": id, "method": "thread/start", "params": params })
}

pub fn thread_resume_request(id: u64, thread_id: &str, config: &CodexThreadConfig) -> Value {
    let mut params = json!({
        "threadId": thread_id,
        "cwd": config.cwd,
        "sandbox": config.sandbox.as_str(),
        "approvalPolicy": APPROVAL_POLICY,
    });
    if let Some(model) = &config.model {
        params["model"] = json!(model);
    }
    if let Some(instructions) = &config.developer_instructions {
        params["developerInstructions"] = json!(instructions);
    }
    json!({ "id": id, "method": "thread/resume", "params": params })
}

pub fn turn_start_request(id: u64, thread_id: &str, prompt: &str, images: &[String]) -> Value {
    let mut input = vec![json!({ "type": "text", "text": prompt })];
    for path in images {
        input.push(json!({ "type": "localImage", "path": path }));
    }
    json!({
        "id": id,
        "method": "turn/start",
        "params": { "threadId": thread_id, "input": input }
    })
}

pub fn turn_interrupt_request(id: u64, thread_id: &str, turn_id: &str) -> Value {
    json!({
        "id": id,
        "method": "turn/interrupt",
        "params": { "threadId": thread_id, "turnId": turn_id }
    })
}

/// Successful response to a server-initiated request.
pub fn success_response(id: &Value, result: Value) -> Value {
    json!({ "id": id, "result": result })
}

/// Error response to a server request the bridge does not implement.
pub fn error_response(id: &Value, code: i64, message: &str) -> Value {
    json!({
        "id": id,
        "error": { "code": code, "message": message }
    })
}

/// Approval response for command/file requests.
pub fn approval_response(id: &Value, allow: bool) -> Value {
    success_response(
        id,
        json!({ "decision": if allow { "accept" } else { "decline" } }),
    )
}

/// Response to `item/permissions/requestApproval`.
///
/// An allowed request grants exactly the profile Codex asked for; a denied one
/// grants an empty profile, which leaves the sandbox unchanged.
pub fn permissions_response(id: &Value, allow: bool, requested: &Value) -> Value {
    let permissions = if allow { requested.clone() } else { json!({}) };
    success_response(id, json!({ "permissions": permissions, "scope": "turn" }))
}

/// Translate one server notification into agent events.
#[derive(Debug, Default)]
pub struct CodexAppServerTranslator {
    thread_id: Option<String>,
    turn_id: Option<String>,
    finished: bool,
    started_items: std::collections::HashSet<String>,
    completed_items: std::collections::HashSet<String>,
}

impl CodexAppServerTranslator {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn thread_id(&self) -> Option<&str> {
        self.thread_id.as_deref()
    }

    pub fn turn_id(&self) -> Option<&str> {
        self.turn_id.as_deref()
    }

    pub fn is_finished(&self) -> bool {
        self.finished
    }

    pub fn translate_notification(&mut self, method: &str, params: &Value) -> Vec<AgentEvent> {
        if self.thread_id.is_none() {
            if let Some(thread_id) = params.get("threadId").and_then(Value::as_str) {
                self.thread_id = Some(thread_id.to_string());
            }
        }
        match method {
            "thread/started" => {
                let thread = params.get("thread").unwrap_or(params);
                let id = thread.get("id").and_then(Value::as_str);
                if self.thread_id.is_none() {
                    self.thread_id = id.map(str::to_string);
                }
                Vec::new()
            }
            "turn/started" => {
                self.turn_id = params
                    .get("turn")
                    .and_then(|turn| turn.get("id"))
                    .and_then(Value::as_str)
                    .map(str::to_string);
                Vec::new()
            }
            "turn/completed" => {
                if self.finished {
                    return Vec::new();
                }
                self.finished = true;
                let turn = params.get("turn").unwrap_or(params);
                let status = turn
                    .get("status")
                    .and_then(Value::as_str)
                    .unwrap_or("failed");
                match status {
                    "completed" => vec![AgentEvent::Done {
                        session_id: self.thread_id.clone(),
                        thread_id: self.thread_id.clone(),
                        termination_reason: TerminationReason::Normal,
                    }],
                    "interrupted" => vec![AgentEvent::Done {
                        session_id: self.thread_id.clone(),
                        thread_id: self.thread_id.clone(),
                        termination_reason: TerminationReason::Interrupted,
                    }],
                    _ => {
                        let message = turn
                            .get("error")
                            .and_then(|error| error.get("message"))
                            .and_then(Value::as_str)
                            .unwrap_or("codex turn failed");
                        vec![AgentEvent::Error {
                            message: truncate_chars(message, 400),
                            termination_reason: TerminationReason::Failed,
                        }]
                    }
                }
            }
            "item/agentMessage/delta" => {
                let Some(delta) = params.get("delta").and_then(Value::as_str) else {
                    return Vec::new();
                };
                if delta.is_empty() {
                    return Vec::new();
                }
                vec![AgentEvent::Text {
                    delta: delta.to_string(),
                }]
            }
            "item/reasoning/summaryTextDelta" | "item/reasoning/textDelta" => {
                let Some(delta) = params.get("delta").and_then(Value::as_str) else {
                    return Vec::new();
                };
                if delta.is_empty() {
                    return Vec::new();
                }
                vec![AgentEvent::Thinking {
                    delta: delta.to_string(),
                }]
            }
            "item/started" => self.translate_item(params, false),
            "item/completed" => self.translate_item(params, true),
            _ => Vec::new(),
        }
    }

    fn translate_item(&mut self, params: &Value, completed: bool) -> Vec<AgentEvent> {
        let Some(item) = params.get("item") else {
            return Vec::new();
        };
        let id = item.get("id").and_then(Value::as_str).unwrap_or_default();
        let kind = item.get("type").and_then(Value::as_str).unwrap_or_default();
        match kind {
            "agentMessage" => {
                if completed {
                    let text = item.get("text").and_then(Value::as_str).unwrap_or_default();
                    if text.is_empty() || !self.completed_items.insert(id.to_string()) {
                        return Vec::new();
                    }
                    // Deltas already streamed this text; the final copy lets
                    // the view prefer an exact answer without duplicating the
                    // visible stream.
                    vec![AgentEvent::FinalText {
                        content: text.to_string(),
                    }]
                } else {
                    Vec::new()
                }
            }
            "reasoning" => {
                if completed {
                    let text = join_strings(item.get("summary"))
                        .or_else(|| join_strings(item.get("content")))
                        .unwrap_or_default();
                    if text.is_empty() || !self.completed_items.insert(id.to_string()) {
                        Vec::new()
                    } else {
                        vec![AgentEvent::Thinking { delta: text }]
                    }
                } else {
                    Vec::new()
                }
            }
            "commandExecution" => {
                if completed {
                    if !self.completed_items.insert(id.to_string()) {
                        return Vec::new();
                    }
                    let status = item
                        .get("status")
                        .and_then(Value::as_str)
                        .unwrap_or_default();
                    let output = item
                        .get("aggregatedOutput")
                        .and_then(Value::as_str)
                        .unwrap_or_default();
                    let output = if output.is_empty() {
                        item.get("exitCode")
                            .map(|code| format!("exit code: {code}"))
                            .unwrap_or_default()
                    } else {
                        output.to_string()
                    };
                    vec![AgentEvent::ToolResult {
                        id: id.to_string(),
                        output,
                        is_error: status != "completed",
                    }]
                } else {
                    if !self.started_items.insert(id.to_string()) {
                        return Vec::new();
                    }
                    vec![AgentEvent::ToolUse {
                        id: id.to_string(),
                        name: "Bash".to_string(),
                        input: json!({
                            "command": item.get("command").cloned().unwrap_or(Value::Null),
                            "cwd": item.get("cwd").cloned().unwrap_or(Value::Null),
                        }),
                    }]
                }
            }
            "fileChange" => {
                if completed {
                    if !self.completed_items.insert(id.to_string()) {
                        return Vec::new();
                    }
                    let status = item
                        .get("status")
                        .and_then(Value::as_str)
                        .unwrap_or_default();
                    let files = item
                        .get("changes")
                        .and_then(Value::as_array)
                        .map(|changes| changes.len())
                        .unwrap_or(0);
                    vec![AgentEvent::ToolResult {
                        id: id.to_string(),
                        output: format!("已应用 {files} 个文件变更"),
                        is_error: status == "failed",
                    }]
                } else {
                    if !self.started_items.insert(id.to_string()) {
                        return Vec::new();
                    }
                    vec![AgentEvent::ToolUse {
                        id: id.to_string(),
                        name: "apply_patch".to_string(),
                        input: item.get("changes").cloned().unwrap_or(Value::Null),
                    }]
                }
            }
            "mcpToolCall" => {
                let name = format!(
                    "{}:{}",
                    item.get("server").and_then(Value::as_str).unwrap_or("mcp"),
                    item.get("tool").and_then(Value::as_str).unwrap_or("tool")
                );
                if completed {
                    if !self.completed_items.insert(id.to_string()) {
                        return Vec::new();
                    }
                    let status = item
                        .get("status")
                        .and_then(Value::as_str)
                        .unwrap_or_default();
                    let output = item
                        .get("result")
                        .or_else(|| item.get("error"))
                        .map(|value| value.to_string())
                        .unwrap_or_default();
                    vec![AgentEvent::ToolResult {
                        id: id.to_string(),
                        output: truncate_chars(&output, 2_000),
                        is_error: status == "failed",
                    }]
                } else {
                    if !self.started_items.insert(id.to_string()) {
                        return Vec::new();
                    }
                    vec![AgentEvent::ToolUse {
                        id: id.to_string(),
                        name,
                        input: item.get("arguments").cloned().unwrap_or(Value::Null),
                    }]
                }
            }
            "webSearch" => {
                if completed {
                    if !self.completed_items.insert(id.to_string()) {
                        return Vec::new();
                    }
                    vec![AgentEvent::ToolResult {
                        id: id.to_string(),
                        output: "搜索完成".to_string(),
                        is_error: false,
                    }]
                } else {
                    if !self.started_items.insert(id.to_string()) {
                        return Vec::new();
                    }
                    vec![AgentEvent::ToolUse {
                        id: id.to_string(),
                        name: "web_search".to_string(),
                        input: json!({ "query": item.get("query").cloned().unwrap_or(Value::Null) }),
                    }]
                }
            }
            _ => Vec::new(),
        }
    }

    /// Translate a server request into a permission event.
    ///
    /// Returns `None` for requests the bridge does not surface (user input,
    /// MCP elicitation); the caller must still answer those so the server does
    /// not wait forever.
    pub fn translate_server_request(
        &mut self,
        id: &Value,
        method: &str,
        params: &Value,
    ) -> Option<AgentEvent> {
        let request_id = request_id_string(id);
        match method {
            "item/commandExecution/requestApproval" => Some(AgentEvent::PermissionRequest {
                request_id,
                tool_name: "Bash".to_string(),
                display_name: Some("Bash".to_string()),
                input: json!({
                    "command": params.get("command").cloned().unwrap_or(Value::Null),
                    "cwd": params.get("cwd").cloned().unwrap_or(Value::Null),
                }),
                description: params
                    .get("reason")
                    .and_then(Value::as_str)
                    .map(str::to_string),
                suggestions: Vec::new(),
                tool_use_id: params
                    .get("itemId")
                    .and_then(Value::as_str)
                    .map(str::to_string),
            }),
            "item/fileChange/requestApproval" => Some(AgentEvent::PermissionRequest {
                request_id,
                tool_name: "apply_patch".to_string(),
                display_name: Some("apply_patch".to_string()),
                input: json!({
                    "itemId": params.get("itemId").cloned().unwrap_or(Value::Null),
                    "grantRoot": params.get("grantRoot").cloned().unwrap_or(Value::Null),
                }),
                description: params
                    .get("reason")
                    .and_then(Value::as_str)
                    .map(str::to_string),
                suggestions: Vec::new(),
                tool_use_id: params
                    .get("itemId")
                    .and_then(Value::as_str)
                    .map(str::to_string),
            }),
            "item/permissions/requestApproval" => Some(AgentEvent::PermissionRequest {
                request_id,
                tool_name: "permissions".to_string(),
                display_name: Some("permissions".to_string()),
                input: params.get("permissions").cloned().unwrap_or(Value::Null),
                description: params
                    .get("reason")
                    .and_then(Value::as_str)
                    .map(str::to_string),
                suggestions: Vec::new(),
                tool_use_id: params
                    .get("itemId")
                    .and_then(Value::as_str)
                    .map(str::to_string),
            }),
            _ => None,
        }
    }

    /// Event emitted when the process exits without `turn/completed`.
    pub fn finish(&mut self) -> Vec<AgentEvent> {
        if self.finished {
            return Vec::new();
        }
        self.finished = true;
        vec![AgentEvent::Done {
            session_id: self.thread_id.clone(),
            thread_id: self.thread_id.clone(),
            termination_reason: TerminationReason::Normal,
        }]
    }
}

fn join_strings(value: Option<&Value>) -> Option<String> {
    let values = value?.as_array()?;
    let joined = values
        .iter()
        .filter_map(Value::as_str)
        .filter(|text| !text.is_empty())
        .collect::<Vec<_>>()
        .join("\n");
    (!joined.is_empty()).then_some(joined)
}

/// Request ids are strings or integers; the card stores one string and the
/// process keeps the original value for the response.
pub fn request_id_string(id: &Value) -> String {
    match id {
        Value::String(text) => text.clone(),
        Value::Number(number) => number.to_string(),
        other => other.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn thread_and_turn_requests_use_the_protocol_shape() {
        let config = CodexThreadConfig {
            cwd: "/work".into(),
            sandbox: SandboxMode::WorkspaceWrite,
            model: Some("gpt-5".into()),
            developer_instructions: Some("bridge".into()),
        };
        let start = thread_start_request(2, &config);
        assert_eq!(start["method"], "thread/start");
        assert_eq!(start["params"]["cwd"], "/work");
        assert_eq!(start["params"]["sandbox"], "workspace-write");
        assert_eq!(start["params"]["approvalPolicy"], "on-request");
        assert_eq!(start["params"]["developerInstructions"], "bridge");
        assert_eq!(start["params"]["model"], "gpt-5");

        let resume = thread_resume_request(3, "thr_1", &config);
        assert_eq!(resume["method"], "thread/resume");
        assert_eq!(resume["params"]["threadId"], "thr_1");

        let turn = turn_start_request(4, "thr_1", "hello", &["/tmp/a.png".into()]);
        assert_eq!(turn["method"], "turn/start");
        assert_eq!(turn["params"]["input"][0]["text"], "hello");
        assert_eq!(turn["params"]["input"][1]["type"], "localImage");
    }

    #[test]
    fn command_approval_becomes_a_permission_request() {
        let mut translator = CodexAppServerTranslator::new();
        let event = translator
            .translate_server_request(
                &json!(77),
                "item/commandExecution/requestApproval",
                &json!({
                    "threadId": "thr_1",
                    "turnId": "turn_1",
                    "itemId": "item_1",
                    "command": "git status",
                    "cwd": "/work",
                    "reason": "needs write access"
                }),
            )
            .expect("permission");
        match event {
            AgentEvent::PermissionRequest {
                request_id,
                tool_name,
                input,
                description,
                tool_use_id,
                ..
            } => {
                assert_eq!(request_id, "77");
                assert_eq!(tool_name, "Bash");
                assert_eq!(input["command"], "git status");
                assert_eq!(description.as_deref(), Some("needs write access"));
                assert_eq!(tool_use_id.as_deref(), Some("item_1"));
            }
            other => panic!("unexpected: {other:?}"),
        }

        let response = approval_response(&json!(77), true);
        assert_eq!(response["id"], 77);
        assert_eq!(response["result"]["decision"], "accept");
        let denied = approval_response(&json!(77), false);
        assert_eq!(denied["result"]["decision"], "decline");
    }

    #[test]
    fn permissions_and_file_requests_have_their_own_shapes() {
        let mut translator = CodexAppServerTranslator::new();
        let file = translator
            .translate_server_request(
                &json!("srv-1"),
                "item/fileChange/requestApproval",
                &json!({ "itemId": "item_2", "grantRoot": "/work", "reason": "write" }),
            )
            .expect("file permission");
        assert!(matches!(
            file,
            AgentEvent::PermissionRequest { ref tool_name, .. } if tool_name == "apply_patch"
        ));

        let permissions = translator
            .translate_server_request(
                &json!(9),
                "item/permissions/requestApproval",
                &json!({
                    "itemId": "item_3",
                    "permissions": { "network": { "enabled": true } }
                }),
            )
            .expect("permissions");
        assert!(matches!(
            permissions,
            AgentEvent::PermissionRequest { ref tool_name, .. } if tool_name == "permissions"
        ));
        let response =
            permissions_response(&json!(9), true, &json!({ "network": { "enabled": true } }));
        assert_eq!(
            response["result"]["permissions"]["network"]["enabled"],
            true
        );
        let denied =
            permissions_response(&json!(9), false, &json!({ "network": { "enabled": true } }));
        assert_eq!(denied["result"]["permissions"], json!({}));
    }

    #[test]
    fn notifications_stream_text_tools_and_terminal_status() {
        let mut translator = CodexAppServerTranslator::new();
        assert!(
            translator
                .translate_notification("thread/started", &json!({ "thread": { "id": "thr_1" } }))
                .is_empty()
        );
        assert_eq!(translator.thread_id(), Some("thr_1"));
        translator.translate_notification("turn/started", &json!({ "turn": { "id": "turn_1" } }));
        assert_eq!(translator.turn_id(), Some("turn_1"));

        let delta =
            translator.translate_notification("item/agentMessage/delta", &json!({ "delta": "hi" }));
        assert_eq!(delta, vec![AgentEvent::Text { delta: "hi".into() }]);
        let started = translator.translate_notification(
            "item/started",
            &json!({
                "item": {
                    "id": "cmd_1",
                    "type": "commandExecution",
                    "command": "pwd",
                    "cwd": "/work",
                    "status": "inProgress"
                }
            }),
        );
        assert!(matches!(&started[0], AgentEvent::ToolUse { name, .. } if name == "Bash"));
        let completed = translator.translate_notification(
            "item/completed",
            &json!({
                "item": {
                    "id": "cmd_1",
                    "type": "commandExecution",
                    "command": "pwd",
                    "cwd": "/work",
                    "status": "completed",
                    "aggregatedOutput": "/work\\n",
                    "exitCode": 0
                }
            }),
        );
        assert_eq!(
            completed,
            vec![AgentEvent::ToolResult {
                id: "cmd_1".into(),
                output: "/work\\n".into(),
                is_error: false,
            }]
        );

        let done = translator.translate_notification(
            "turn/completed",
            &json!({ "turn": { "id": "turn_1", "status": "completed" } }),
        );
        assert!(matches!(
            &done[0],
            AgentEvent::Done {
                termination_reason: TerminationReason::Normal,
                ..
            }
        ));
        assert!(translator.is_finished());
    }

    #[test]
    fn failed_turn_reports_an_error() {
        let mut translator = CodexAppServerTranslator::new();
        let events = translator.translate_notification(
            "turn/completed",
            &json!({
                "turn": {
                    "id": "t",
                    "status": "failed",
                    "error": { "message": "model overloaded" }
                }
            }),
        );
        assert!(matches!(
            &events[0],
            AgentEvent::Error {
                termination_reason: TerminationReason::Failed,
                ..
            }
        ));
    }
}
