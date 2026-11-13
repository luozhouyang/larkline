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
        // `codex exec` was launched with this override so tools inherit the
        // bridge's environment (PATH, proxies, credentials). App-server threads
        // need the same setting explicitly.
        "config": { "shell_environment_policy": { "inherit": "all" } },
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
        "config": { "shell_environment_policy": { "inherit": "all" } },
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

/// Session-scoped approval response for command/file requests.
pub fn approval_response_for_session(id: &Value, allow: bool) -> Value {
    success_response(
        id,
        json!({ "decision": if allow { "acceptForSession" } else { "decline" } }),
    )
}

/// Response to `item/permissions/requestApproval`.
///
/// An allowed request grants exactly the profile Codex asked for; a denied one
/// grants an empty profile, which leaves the sandbox unchanged.
pub fn permissions_response(id: &Value, allow: bool, requested: &Value) -> Value {
    permissions_response_with_scope(id, allow, requested, "turn")
}

/// Session-scoped permission grant for `item/permissions/requestApproval`.
pub fn permissions_response_for_session(id: &Value, allow: bool, requested: &Value) -> Value {
    permissions_response_with_scope(id, allow, requested, "session")
}

fn permissions_response_with_scope(
    id: &Value,
    allow: bool,
    requested: &Value,
    scope: &str,
) -> Value {
    let permissions = if allow { requested.clone() } else { json!({}) };
    success_response(id, json!({ "permissions": permissions, "scope": scope }))
}

/// Translate one server notification into agent events.
#[derive(Debug, Default)]
pub struct CodexAppServerTranslator {
    thread_id: Option<String>,
    turn_id: Option<String>,
    finished: bool,
    started_items: std::collections::HashSet<String>,
    completed_items: std::collections::HashSet<String>,
    reasoning_items_with_deltas: std::collections::HashSet<String>,
    /// `changes` of file-change items, keyed by item id. The approval request
    /// only carries the item id; keeping the item lets the card show the files
    /// (and their diffs) the user is approving.
    file_changes: std::collections::HashMap<String, Value>,
    /// Text of the last completed `agentMessage`. Codex may emit commentary
    /// messages before tools; only the last one before `turn/completed` is the
    /// final answer, so it is emitted as `FinalText` at the terminal event
    /// rather than replacing the streamed blocks on every item.
    last_agent_message: Option<String>,
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
                    "completed" => {
                        let mut events = Vec::new();
                        if let Some(text) = self.last_agent_message.take() {
                            if !text.is_empty() {
                                events.push(AgentEvent::FinalText { content: text });
                            }
                        }
                        events.push(AgentEvent::Done {
                            session_id: self.thread_id.clone(),
                            thread_id: self.thread_id.clone(),
                            termination_reason: TerminationReason::Normal,
                        });
                        events
                    }
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
            "error" => {
                let message = params
                    .get("error")
                    .and_then(|error| error.get("message"))
                    .and_then(Value::as_str)
                    .unwrap_or("Codex returned an error");
                let will_retry = params
                    .get("willRetry")
                    .and_then(Value::as_bool)
                    .unwrap_or(false);
                if will_retry {
                    vec![AgentEvent::Notice {
                        message: format!("Codex：{}（将自动重试）", truncate_chars(message, 300)),
                    }]
                } else {
                    self.finished = true;
                    vec![AgentEvent::Error {
                        message: truncate_chars(message, 400),
                        termination_reason: TerminationReason::Failed,
                    }]
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
                if let Some(item_id) = params.get("itemId").and_then(Value::as_str) {
                    self.reasoning_items_with_deltas.insert(item_id.to_string());
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
                    // The completed item may be commentary; the terminal event
                    // promotes the last one to the final answer.
                    self.last_agent_message = Some(text.to_string());
                }
                Vec::new()
            }
            "reasoning" => {
                if completed {
                    // Deltas already streamed this item's text.
                    if self.reasoning_items_with_deltas.contains(id) {
                        return Vec::new();
                    }
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
                    let (output, is_error) = match status {
                        "completed" => (format!("已应用 {files} 个文件变更"), false),
                        "declined" => ("用户拒绝了文件变更".to_string(), true),
                        _ => (format!("文件变更失败（{files} 个文件）"), true),
                    };
                    vec![AgentEvent::ToolResult {
                        id: id.to_string(),
                        output,
                        is_error,
                    }]
                } else {
                    if !self.started_items.insert(id.to_string()) {
                        return Vec::new();
                    }
                    let changes = item.get("changes").cloned().unwrap_or(Value::Null);
                    self.file_changes.insert(id.to_string(), changes.clone());
                    vec![AgentEvent::ToolUse {
                        id: id.to_string(),
                        name: "apply_patch".to_string(),
                        input: changes,
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
                        is_error: status != "completed",
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
            "item/fileChange/requestApproval" => {
                let item_id = params.get("itemId").cloned().unwrap_or(Value::Null);
                let changes = item_id
                    .as_str()
                    .and_then(|id| self.file_changes.get(id))
                    .cloned()
                    .unwrap_or(Value::Null);
                Some(AgentEvent::PermissionRequest {
                    request_id,
                    tool_name: "apply_patch".to_string(),
                    display_name: Some("apply_patch".to_string()),
                    input: json!({
                        "itemId": item_id,
                        "grantRoot": params.get("grantRoot").cloned().unwrap_or(Value::Null),
                        "changes": changes,
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
                })
            }
            "item/tool/requestUserInput" => {
                let questions: Vec<crate::types::UserQuestion> = params
                    .get("questions")
                    .and_then(Value::as_array)
                    .map(|questions| {
                        questions
                            .iter()
                            .filter_map(|question| {
                                let question = question.as_object()?;
                                let id = question.get("id").and_then(Value::as_str)?;
                                let text = question.get("question").and_then(Value::as_str)?;
                                let options = question
                                    .get("options")
                                    .and_then(Value::as_array)
                                    .map(|options| {
                                        options
                                            .iter()
                                            .filter_map(|option| {
                                                let option = option.as_object()?;
                                                Some(crate::types::UserQuestionOption {
                                                    label: option
                                                        .get("label")
                                                        .and_then(Value::as_str)?
                                                        .to_string(),
                                                    description: option
                                                        .get("description")
                                                        .and_then(Value::as_str)
                                                        .unwrap_or_default()
                                                        .to_string(),
                                                })
                                            })
                                            .collect()
                                    })
                                    .unwrap_or_default();
                                Some(crate::types::UserQuestion {
                                    id: id.to_string(),
                                    header: question
                                        .get("header")
                                        .and_then(Value::as_str)
                                        .unwrap_or_default()
                                        .to_string(),
                                    question: text.to_string(),
                                    options,
                                    multi_select: false,
                                    is_other: question
                                        .get("isOther")
                                        .and_then(Value::as_bool)
                                        .unwrap_or(false),
                                    is_secret: question
                                        .get("isSecret")
                                        .and_then(Value::as_bool)
                                        .unwrap_or(false),
                                })
                            })
                            .collect()
                    })
                    .unwrap_or_default();
                (!questions.is_empty()).then(|| AgentEvent::QuestionRequest {
                    request_id,
                    tool_name: "request_user_input".to_string(),
                    questions,
                    raw_input: params.clone(),
                })
            }
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
        assert_eq!(
            start["params"]["config"]["shell_environment_policy"]["inherit"],
            "all"
        );

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
        let session = approval_response_for_session(&json!(77), true);
        assert_eq!(session["result"]["decision"], "acceptForSession");
        let session_permissions = permissions_response_for_session(
            &json!(78),
            true,
            &json!({ "network": { "enabled": true } }),
        );
        assert_eq!(session_permissions["result"]["scope"], "session");
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
    fn request_user_input_becomes_a_question_request() {
        let mut translator = CodexAppServerTranslator::new();
        let event = translator
            .translate_server_request(
                &json!(12),
                "item/tool/requestUserInput",
                &json!({
                    "threadId": "thr_1",
                    "turnId": "turn_1",
                    "itemId": "item_1",
                    "questions": [{
                        "id": "q1",
                        "header": "Choice",
                        "question": "Pick one",
                        "isOther": true,
                        "isSecret": false,
                        "options": [
                            { "label": "A", "description": "first" },
                            { "label": "B", "description": "second" }
                        ]
                    }]
                }),
            )
            .expect("question");
        match event {
            AgentEvent::QuestionRequest {
                request_id,
                questions,
                ..
            } => {
                assert_eq!(request_id, "12");
                assert_eq!(questions[0].id, "q1");
                assert_eq!(questions[0].options[1].description, "second");
                assert!(questions[0].is_other);
            }
            other => panic!("unexpected: {other:?}"),
        }
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
    fn only_the_last_completed_message_becomes_the_final_answer() {
        let mut translator = CodexAppServerTranslator::new();
        let mut events = Vec::new();
        events.extend(translator.translate_notification(
            "item/agentMessage/delta",
            &json!({ "delta": "working on it" }),
        ));
        events.extend(translator.translate_notification(
            "item/completed",
            &json!({
                "item": { "id": "m1", "type": "agentMessage", "text": "working on it" }
            }),
        ));
        assert!(
            events
                .iter()
                .all(|event| !matches!(event, AgentEvent::FinalText { .. }))
        );
        events.extend(
            translator.translate_notification(
                "item/agentMessage/delta",
                &json!({ "delta": "the answer" }),
            ),
        );
        events.extend(translator.translate_notification(
            "item/completed",
            &json!({
                "item": { "id": "m2", "type": "agentMessage", "text": "the answer" }
            }),
        ));
        events.extend(translator.translate_notification(
            "turn/completed",
            &json!({ "turn": { "id": "t", "status": "completed" } }),
        ));
        assert!(matches!(
            events.get(events.len() - 2),
            Some(AgentEvent::FinalText { content }) if content == "the answer"
        ));
        assert!(matches!(events.last(), Some(AgentEvent::Done { .. })));
    }

    #[test]
    fn reasoning_deltas_are_not_repeated_by_the_completed_item() {
        let mut translator = CodexAppServerTranslator::new();
        let delta = translator.translate_notification(
            "item/reasoning/textDelta",
            &json!({ "itemId": "r1", "delta": "hmm" }),
        );
        assert_eq!(
            delta,
            vec![AgentEvent::Thinking {
                delta: "hmm".into()
            }]
        );
        let completed = translator.translate_notification(
            "item/completed",
            &json!({
                "item": { "id": "r1", "type": "reasoning", "summary": ["hmm"], "content": [] }
            }),
        );
        assert!(completed.is_empty());
    }

    #[test]
    fn error_notifications_are_retry_notices_or_terminal_errors() {
        let mut translator = CodexAppServerTranslator::new();
        let retry = translator.translate_notification(
            "error",
            &json!({
                "threadId": "thr_1",
                "turnId": "turn_1",
                "willRetry": true,
                "error": { "message": "model overloaded" }
            }),
        );
        assert!(
            matches!(&retry[0], AgentEvent::Notice { message } if message.contains("model overloaded"))
        );
        assert!(!translator.is_finished());

        let mut translator = CodexAppServerTranslator::new();
        let failed = translator.translate_notification(
            "error",
            &json!({
                "threadId": "thr_1",
                "turnId": "turn_1",
                "willRetry": false,
                "error": { "message": "auth failed" }
            }),
        );
        assert!(matches!(
            &failed[0],
            AgentEvent::Error {
                termination_reason: TerminationReason::Failed,
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
