//! Spawn and stream a local agent CLI.
//!
//! The bridge never links an agent: it runs the installed `claude`/`codex`
//! binary, writes the prompt on stdin, and translates line-delimited JSON.
//! The process is killed when the handle is dropped, which is what makes
//! `/stop` and daemon shutdown predictable.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::time::Duration;

use anyhow::{Context as _, anyhow};
use larkline_agent::{
    AgentEvent, BridgePromptContext, ClaudeArgsInput, ClaudeStreamTranslator,
    CodexAppServerTranslator, CodexArgsInput, CodexFinishReason, CodexJsonlTranslator,
    CodexThreadConfig, PermissionResponse, QuestionAnswer, approval_response,
    approval_response_for_session, build_bridge_system_prompt, build_claude_args, build_codex_args,
    error_response, initialize_request, permissions_response, permissions_response_for_session,
    prefix_bridge_system_prompt, request_id_string, success_response, thread_resume_request,
    thread_start_request, turn_start_request, user_message_json,
};
use larkline_core::AgentKind;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::process::{Child, ChildStdin, ChildStdout, Command};

use crate::permissions::RunPermissions;

#[derive(Debug, Clone)]
pub struct AgentSpec {
    pub kind: AgentKind,
    /// Whether this run can present an approval card.
    ///
    /// IM runs do; comment and meeting runs have their own loops and no card
    /// flow, so they keep the agent's one-shot behavior (Claude print-mode
    /// auto-deny / Codex `approval_policy=never`) instead of blocking on a
    /// prompt nobody can answer.
    pub approvals: bool,
    pub binary: PathBuf,
    pub cwd: PathBuf,
    pub prompt: String,
    pub session_id: Option<String>,
    pub model: Option<String>,
    /// Profile-resolved access policy for this run. The adapters turn it into
    /// Codex's `--sandbox` and Claude's `--permission-mode`.
    pub permissions: RunPermissions,
    /// Image paths for Codex's `--image`. Empty for Claude: the TS bridge
    /// passes image paths to Claude in the prompt text instead.
    pub images: Vec<String>,
    pub bot_open_id: Option<String>,
    pub bot_name: Option<String>,
}

enum Translator {
    Claude(ClaudeStreamTranslator),
    Codex(CodexJsonlTranslator),
}

impl Translator {
    fn translate(&mut self, line: &str) -> Vec<AgentEvent> {
        let Ok(value) = serde_json::from_str::<serde_json::Value>(line) else {
            tracing::debug!(line = %line, "non-json agent output");
            return Vec::new();
        };
        match self {
            Translator::Claude(translator) => translator.translate(&value),
            Translator::Codex(translator) => translator.translate(&value),
        }
    }

    fn finish(&mut self) -> Vec<AgentEvent> {
        match self {
            Translator::Claude(_) => Vec::new(),
            Translator::Codex(translator) => translator.finish(CodexFinishReason::Failed),
        }
    }
}

pub struct AgentProcess {
    child: Child,
    transport: Transport,
    /// Removed on drop: the agent has already read it by then.
    _system_prompt: Option<TempFile>,
}

// One instance lives inside `AgentProcess`; the size difference between the
// line and app-server states is irrelevant next to the child process.
#[allow(clippy::large_enum_variant)]
enum Transport {
    /// `claude -p` or `codex exec --json`: newline-delimited output, optional
    /// control-protocol stdin for Claude approvals.
    Line {
        stdout: BufReader<ChildStdout>,
        translator: Translator,
        stdin: Option<ChildStdin>,
    },
    /// `codex app-server`: JSON-RPC over stdio with server-initiated approval
    /// requests.
    CodexAppServer(Box<CodexAppServer>),
}

struct CodexServerRequest {
    id: serde_json::Value,
    method: String,
    params: serde_json::Value,
}

struct CodexAppServer {
    /// `None` after the run closes stdin (dropping the handle is what lets the
    /// child see EOF; `shutdown` alone does not close a process pipe).
    stdin: Option<ChildStdin>,
    stdout: BufReader<ChildStdout>,
    translator: CodexAppServerTranslator,
    /// Server requests waiting for a card decision, keyed by the request id as
    /// rendered into the card payload.
    pending: HashMap<String, CodexServerRequest>,
    /// Messages read while waiting for a setup response; processed in order by
    /// `next_events` so nothing is lost.
    buffered: std::collections::VecDeque<serde_json::Value>,
    /// A synthetic `System` event emitted after `thread/start` succeeds.
    pending_events: std::collections::VecDeque<AgentEvent>,
    next_id: u64,
}

impl CodexAppServer {
    fn new(stdin: ChildStdin, stdout: BufReader<ChildStdout>) -> Self {
        Self {
            stdin: Some(stdin),
            stdout,
            translator: CodexAppServerTranslator::new(),
            pending: HashMap::new(),
            buffered: std::collections::VecDeque::new(),
            pending_events: std::collections::VecDeque::new(),
            // 1 is used by `initialize`.
            next_id: 2,
        }
    }

    fn alloc_id(&mut self) -> u64 {
        let id = self.next_id;
        self.next_id += 1;
        id
    }

    async fn send(&mut self, frame: &serde_json::Value) -> anyhow::Result<()> {
        let Some(stdin) = self.stdin.as_mut() else {
            anyhow::bail!("codex app-server stdin is closed");
        };
        let mut line = frame.to_string();
        line.push('\n');
        stdin
            .write_all(line.as_bytes())
            .await
            .context("failed to write a codex app-server frame")?;
        stdin.flush().await.ok();
        Ok(())
    }

    async fn read_value(&mut self) -> anyhow::Result<serde_json::Value> {
        let mut line = String::new();
        let read = self
            .stdout
            .read_line(&mut line)
            .await
            .context("failed to read codex app-server stdout")?;
        if read == 0 {
            anyhow::bail!("codex app-server closed stdout");
        }
        serde_json::from_str(line.trim())
            .with_context(|| format!("invalid codex app-server JSON: {}", line.trim()))
    }

    /// Send one request and wait for its response, buffering notifications and
    /// server requests that arrive in between.
    async fn request(
        &mut self,
        frame: serde_json::Value,
        timeout: Duration,
    ) -> anyhow::Result<serde_json::Value> {
        let id = frame.get("id").cloned().unwrap_or(serde_json::Value::Null);
        self.send(&frame).await?;
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            let value = tokio::time::timeout_at(deadline, self.read_value())
                .await
                .context("timed out waiting for a codex app-server response")??;
            if value.get("method").is_none() && value.get("id") == Some(&id) {
                if let Some(error) = value.get("error") {
                    anyhow::bail!("codex app-server error: {error}");
                }
                return Ok(value);
            }
            self.buffered.push_back(value);
        }
    }

    async fn respond_to_permission(
        &mut self,
        request_id: &str,
        response: PermissionResponse,
    ) -> anyhow::Result<()> {
        let Some(pending) = self.pending.remove(request_id) else {
            anyhow::bail!("no pending codex approval request with id {request_id}");
        };
        let allow = !matches!(response, PermissionResponse::Deny { .. });
        let session = matches!(response, PermissionResponse::AllowForSession { .. });
        let frame = match pending.method.as_str() {
            "item/commandExecution/requestApproval" | "item/fileChange/requestApproval" => {
                if session {
                    approval_response_for_session(&pending.id, allow)
                } else {
                    approval_response(&pending.id, allow)
                }
            }
            "item/permissions/requestApproval" => {
                let requested = pending
                    .params
                    .get("permissions")
                    .unwrap_or(&serde_json::Value::Null);
                if session {
                    permissions_response_for_session(&pending.id, allow, requested)
                } else {
                    permissions_response(&pending.id, allow, requested)
                }
            }
            other => error_response(
                &pending.id,
                -32601,
                &format!("unsupported codex approval request: {other}"),
            ),
        };
        self.send(&frame).await
    }

    async fn respond_to_question(
        &mut self,
        request_id: &str,
        answers: &[QuestionAnswer],
    ) -> anyhow::Result<()> {
        let Some(pending) = self.pending.remove(request_id) else {
            anyhow::bail!("no pending codex question request with id {request_id}");
        };
        if pending.method != "item/tool/requestUserInput" {
            anyhow::bail!(
                "codex request {request_id} is not a user-input request: {}",
                pending.method
            );
        }
        let answers: serde_json::Map<String, serde_json::Value> = answers
            .iter()
            .map(|answer| {
                (
                    answer.question_id.clone(),
                    serde_json::json!({ "answers": answer.answers }),
                )
            })
            .collect();
        let frame = success_response(&pending.id, serde_json::json!({ "answers": answers }));
        self.send(&frame).await
    }

    async fn next_events(&mut self) -> Option<Vec<AgentEvent>> {
        if let Some(event) = self.pending_events.pop_front() {
            return Some(vec![event]);
        }
        loop {
            let value = if let Some(value) = self.buffered.pop_front() {
                value
            } else {
                match self.read_value().await {
                    Ok(value) => value,
                    Err(err) => {
                        tracing::warn!(error = %err, "codex app-server read failed");
                        return None;
                    }
                }
            };
            let Some(method) = value.get("method").and_then(serde_json::Value::as_str) else {
                // A response to a client request that setup did not consume.
                tracing::debug!(?value, "unexpected codex app-server response");
                continue;
            };
            let params = value
                .get("params")
                .cloned()
                .unwrap_or(serde_json::Value::Null);
            if let Some(id) = value.get("id") {
                let key = request_id_string(id);
                self.pending.insert(
                    key.clone(),
                    CodexServerRequest {
                        id: id.clone(),
                        method: method.to_string(),
                        params: params.clone(),
                    },
                );
                if let Some(event) = self
                    .translator
                    .translate_server_request(id, method, &params)
                {
                    return Some(vec![event]);
                }
                // A request the bridge cannot surface still needs an answer,
                // otherwise the turn waits forever.
                let response =
                    error_response(id, -32601, "larkline does not support this codex request");
                if let Err(err) = self.send(&response).await {
                    tracing::warn!(error = %err, "cannot decline an unsupported codex request");
                }
                self.pending.remove(&key);
                continue;
            }
            let events = self.translator.translate_notification(method, &params);
            if !events.is_empty() {
                return Some(events);
            }
        }
    }
}

/// Build the adapter argv and the stdin prompt for one run.
///
/// Extracted from [`AgentProcess::spawn`] so argv construction is testable
/// without spawning a process. The permission policy flows through
/// [`AgentSpec::permissions`] into `build_claude_args` /
/// `build_codex_args`; TypeScript does the same through `AgentRunOptions`.
fn build_agent_args(
    spec: &AgentSpec,
    system_prompt_file: &Path,
) -> anyhow::Result<(Vec<String>, String)> {
    match spec.kind {
        AgentKind::Claude => {
            let approvals = spec.approvals && spec.permissions.claude_approvals_enabled();
            Ok((
                build_claude_args(&ClaudeArgsInput {
                    permission_mode: Some(spec.permissions.claude_permission_mode().as_str()),
                    system_prompt_file: Some(system_prompt_file),
                    session_id: spec.session_id.as_deref(),
                    model: spec.model.as_deref(),
                    stream_json_input: approvals,
                    permission_prompt_tool: approvals,
                }),
                if approvals {
                    user_message_json(&spec.prompt)
                } else {
                    spec.prompt.clone()
                },
            ))
        }
        AgentKind::Codex => {
            let args = build_codex_args(&CodexArgsInput {
                cwd: spec.cwd.to_string_lossy().into_owned(),
                sandbox: spec.permissions.sandbox(),
                thread_id: spec.session_id.clone(),
                images: spec.images.clone(),
                ignore_user_config: false,
                ignore_rules: None,
                model: spec.model.clone(),
            })
            .map_err(|e| anyhow!("invalid codex arguments: {e}"))?;
            // Codex has no `--append-system-prompt-file`; the system prompt is
            // prepended to the stdin prompt instead.
            Ok((
                args,
                prefix_bridge_system_prompt(
                    &spec.prompt,
                    Some(&BridgePromptContext {
                        bot_open_id: spec.bot_open_id.clone(),
                        bot_name: spec.bot_name.clone(),
                    }),
                ),
            ))
        }
    }
}

fn response_thread_id(response: &serde_json::Value) -> Option<String> {
    response
        .pointer("/result/thread/id")
        .and_then(serde_json::Value::as_str)
        .map(str::to_string)
}

impl AgentProcess {
    pub async fn spawn(spec: AgentSpec) -> anyhow::Result<Self> {
        let system_prompt = build_bridge_system_prompt(Some(&BridgePromptContext {
            bot_open_id: spec.bot_open_id.clone(),
            bot_name: spec.bot_name.clone(),
        }));

        let system_prompt_file = TempFile::new(&system_prompt)?;
        if spec.approvals
            && spec.kind == AgentKind::Codex
            && spec.permissions.codex_approvals_enabled()
        {
            return Self::spawn_codex_app_server(spec, system_prompt, system_prompt_file).await;
        }
        let (args, stdin_prompt) = build_agent_args(&spec, &system_prompt_file.path)?;

        tracing::info!(
            agent = spec.kind.binary_name(),
            cwd = %spec.cwd.display(),
            resume = spec.session_id.is_some(),
            access = spec.permissions.effective_access().as_str(),
            sandbox = spec.permissions.sandbox().as_str(),
            claude_permission_mode = spec.permissions.claude_permission_mode().as_str(),
            "spawning agent"
        );

        let mut command = command_for(&spec.binary, &args, &spec.cwd);
        command
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true);

        let mut child = command
            .spawn()
            .with_context(|| format!("failed to spawn {}", spec.binary.display()))?;

        let stdout = child
            .stdout
            .take()
            .ok_or_else(|| anyhow!("agent stdout was not captured"))?;
        if let Some(stderr) = child.stderr.take() {
            tokio::spawn(async move {
                let mut lines = BufReader::new(stderr).lines();
                while let Ok(Some(line)) = lines.next_line().await {
                    if !line.trim().is_empty() {
                        tracing::warn!(target: "agent", line = %line, "agent stderr");
                    }
                }
            });
        }
        // Claude approval runs keep stdin open: the control protocol writes
        // `control_response` frames there while the agent is blocked on a
        // permission prompt. Everything else remains a one-shot prompt.
        let approvals = spec.approvals
            && spec.kind == AgentKind::Claude
            && spec.permissions.claude_approvals_enabled();
        let stdin = if let Some(mut stdin) = child.stdin.take() {
            // `stream-json` input is newline-delimited; a one-shot text prompt
            // is written verbatim (the CLI reads it until EOF).
            let mut payload = stdin_prompt.into_bytes();
            if approvals {
                payload.push(b'\n');
            }
            stdin
                .write_all(&payload)
                .await
                .context("failed to write prompt to agent stdin")?;
            if approvals {
                Some(stdin)
            } else {
                stdin.shutdown().await.ok();
                None
            }
        } else {
            None
        };

        let translator = match spec.kind {
            AgentKind::Claude => Translator::Claude(ClaudeStreamTranslator::new()),
            AgentKind::Codex => Translator::Codex(CodexJsonlTranslator::new()),
        };

        Ok(Self {
            child,
            transport: Transport::Line {
                stdout: BufReader::new(stdout),
                translator,
                stdin,
            },
            _system_prompt: Some(system_prompt_file),
        })
    }

    /// Start `codex app-server`, initialize it, open (or resume) a thread and
    /// start the turn. The prompt travels in `turn/start`, not on stdin.
    async fn spawn_codex_app_server(
        spec: AgentSpec,
        system_prompt: String,
        system_prompt_file: TempFile,
    ) -> anyhow::Result<Self> {
        tracing::info!(
            agent = "codex",
            cwd = %spec.cwd.display(),
            resume = spec.session_id.is_some(),
            access = spec.permissions.effective_access().as_str(),
            sandbox = spec.permissions.sandbox().as_str(),
            approval_policy = "on-request",
            "spawning codex app-server"
        );
        let args = vec!["app-server".to_string()];
        let mut command = command_for(&spec.binary, &args, &spec.cwd);
        command
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true);
        let mut child = command
            .spawn()
            .with_context(|| format!("failed to spawn {} app-server", spec.binary.display()))?;
        let stdout = child
            .stdout
            .take()
            .ok_or_else(|| anyhow!("codex app-server stdout was not captured"))?;
        if let Some(stderr) = child.stderr.take() {
            tokio::spawn(async move {
                let mut lines = BufReader::new(stderr).lines();
                while let Ok(Some(line)) = lines.next_line().await {
                    if !line.trim().is_empty() {
                        tracing::warn!(target: "agent", line = %line, "codex app-server stderr");
                    }
                }
            });
        }
        let stdin = child
            .stdin
            .take()
            .ok_or_else(|| anyhow!("codex app-server stdin was not captured"))?;
        let mut server = CodexAppServer::new(stdin, BufReader::new(stdout));

        const SETUP_TIMEOUT: Duration = Duration::from_secs(30);
        server
            .request(initialize_request(1), SETUP_TIMEOUT)
            .await
            .context("codex app-server initialize failed")?;

        let config = CodexThreadConfig {
            cwd: spec.cwd.to_string_lossy().into_owned(),
            sandbox: spec.permissions.sandbox(),
            model: spec.model.clone(),
            developer_instructions: Some(system_prompt),
        };
        let thread_id = match spec.session_id.as_deref() {
            Some(session_id) => {
                let id = server.alloc_id();
                match server
                    .request(
                        thread_resume_request(id, session_id, &config),
                        SETUP_TIMEOUT,
                    )
                    .await
                {
                    Ok(response) => response_thread_id(&response)
                        .ok_or_else(|| anyhow!("codex app-server resume returned no thread id"))?,
                    Err(err) => {
                        tracing::warn!(
                            error = %err,
                            "cannot resume the codex thread; starting a new one"
                        );
                        let id = server.alloc_id();
                        let response = server
                            .request(thread_start_request(id, &config), SETUP_TIMEOUT)
                            .await
                            .context("codex app-server thread/start failed")?;
                        response_thread_id(&response).ok_or_else(|| {
                            anyhow!("codex app-server start returned no thread id")
                        })?
                    }
                }
            }
            None => {
                let id = server.alloc_id();
                let response = server
                    .request(thread_start_request(id, &config), SETUP_TIMEOUT)
                    .await
                    .context("codex app-server thread/start failed")?;
                response_thread_id(&response)
                    .ok_or_else(|| anyhow!("codex app-server start returned no thread id"))?
            }
        };

        let turn_request_id = server.alloc_id();
        server
            .request(
                turn_start_request(turn_request_id, &thread_id, &spec.prompt, &spec.images),
                SETUP_TIMEOUT,
            )
            .await
            .context("codex app-server turn/start failed")?;

        server.pending_events.push_back(AgentEvent::System {
            session_id: Some(thread_id.clone()),
            thread_id: Some(thread_id),
            cwd: Some(spec.cwd.to_string_lossy().into_owned()),
            model: spec.model,
        });

        Ok(Self {
            child,
            transport: Transport::CodexAppServer(Box::new(server)),
            _system_prompt: Some(system_prompt_file),
        })
    }

    /// Next translated event batch, or `None` at EOF.
    pub async fn next_events(&mut self) -> Option<Vec<AgentEvent>> {
        match &mut self.transport {
            Transport::Line {
                stdout, translator, ..
            } => {
                let mut line = String::new();
                match stdout.read_line(&mut line).await {
                    Ok(0) => None,
                    Ok(_) => Some(translator.translate(line.trim_end())),
                    Err(err) => {
                        tracing::warn!(error = %err, "agent stdout read failed");
                        None
                    }
                }
            }
            Transport::CodexAppServer(server) => server.next_events().await,
        }
    }

    /// Answer a pending permission prompt.
    pub async fn respond_to_permission(
        &mut self,
        request_id: &str,
        response: PermissionResponse,
    ) -> anyhow::Result<()> {
        match &mut self.transport {
            Transport::Line { stdin, .. } => {
                let Some(stdin) = stdin.as_mut() else {
                    anyhow::bail!("agent stdin is closed; cannot answer the permission request");
                };
                let decision = match response {
                    PermissionResponse::Allow { updated_input } => {
                        serde_json::json!({ "behavior": "allow", "updatedInput": updated_input })
                    }
                    PermissionResponse::AllowForSession {
                        updated_input,
                        updated_permissions,
                    } => {
                        let mut value = serde_json::json!({
                            "behavior": "allow",
                            "updatedInput": updated_input,
                        });
                        if !updated_permissions.is_empty() {
                            value["updatedPermissions"] = serde_json::json!(updated_permissions);
                        }
                        value
                    }
                    PermissionResponse::Deny { message } => {
                        serde_json::json!({ "behavior": "deny", "message": message })
                    }
                };
                let frame = serde_json::json!({
                    "type": "control_response",
                    "response": {
                        "subtype": "success",
                        "request_id": request_id,
                        "response": decision
                    }
                });
                let mut line = frame.to_string();
                line.push('\n');
                stdin
                    .write_all(line.as_bytes())
                    .await
                    .context("failed to write a permission response to agent stdin")?;
                stdin.flush().await.ok();
                Ok(())
            }
            Transport::CodexAppServer(server) => {
                server.respond_to_permission(request_id, response).await
            }
        }
    }

    /// Answer a user-question request (Claude `AskUserQuestion`, Codex
    /// `request_user_input`).
    pub async fn respond_to_question(
        &mut self,
        request_id: &str,
        raw_input: &serde_json::Value,
        answers: &[QuestionAnswer],
    ) -> anyhow::Result<()> {
        match &mut self.transport {
            Transport::Line { stdin, .. } => {
                let Some(stdin) = stdin.as_mut() else {
                    anyhow::bail!("agent stdin is closed; cannot answer the question");
                };
                let mut updated = raw_input.clone();
                let map: serde_json::Map<String, serde_json::Value> = answers
                    .iter()
                    .map(|answer| {
                        (
                            answer.question_id.clone(),
                            serde_json::Value::String(answer.answers.join(", ")),
                        )
                    })
                    .collect();
                updated["answers"] = serde_json::Value::Object(map);
                let frame = serde_json::json!({
                    "type": "control_response",
                    "response": {
                        "subtype": "success",
                        "request_id": request_id,
                        "response": { "behavior": "allow", "updatedInput": updated }
                    }
                });
                let mut line = frame.to_string();
                line.push('\n');
                stdin
                    .write_all(line.as_bytes())
                    .await
                    .context("failed to write a question answer to agent stdin")?;
                stdin.flush().await.ok();
                Ok(())
            }
            Transport::CodexAppServer(server) => {
                server.respond_to_question(request_id, answers).await
            }
        }
    }

    /// Close the agent stdin so a finished process can exit.
    ///
    /// Idempotent; a non-approval run already closed stdin at spawn.
    pub async fn close_stdin(&mut self) {
        match &mut self.transport {
            Transport::Line { stdin, .. } => {
                if let Some(mut stdin) = stdin.take() {
                    stdin.shutdown().await.ok();
                }
            }
            Transport::CodexAppServer(server) => {
                if let Some(mut stdin) = server.stdin.take() {
                    stdin.shutdown().await.ok();
                }
            }
        }
    }

    /// Flush a terminal event when the child exits without one.
    pub async fn finish(&mut self) -> Vec<AgentEvent> {
        let events = match &mut self.transport {
            Transport::Line { translator, .. } => translator.finish(),
            Transport::CodexAppServer(server) => server.translator.finish(),
        };
        match self.child.wait().await {
            Ok(status) => tracing::info!(%status, "agent exited"),
            Err(err) => tracing::warn!(error = %err, "agent wait failed"),
        }
        events
    }

    pub async fn kill(&mut self) {
        let _ = self.child.kill().await;
    }
}

fn command_for(binary: &Path, args: &[String], cwd: &Path) -> Command {
    #[cfg(windows)]
    {
        if larkline_agent::detect::needs_cmd_shim(binary) {
            // `.cmd` shims cannot be executed directly by CreateProcess, and
            // cmd.exe would reinterpret shell metacharacters in the prompt.
            // The prompt travels on stdin, so quoting argv only is safe.
            let mut command = Command::new("cmd.exe");
            command.arg("/d").arg("/s").arg("/c");
            let mut line = format!("\"{}\"", binary.display());
            for arg in args {
                line.push(' ');
                line.push('"');
                line.push_str(&arg.replace('"', "\"\""));
                line.push('"');
            }
            command.arg(line);
            command.current_dir(cwd);
            return command;
        }
    }
    let mut command = Command::new(binary);
    command.args(args).current_dir(cwd);
    command
}

struct TempFile {
    path: PathBuf,
}

impl TempFile {
    fn new(contents: &str) -> std::io::Result<Self> {
        let path = std::env::temp_dir().join(format!(
            "larkline-system-{}-{}.md",
            std::process::id(),
            uuid::Uuid::new_v4()
        ));
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            let mut file = std::fs::OpenOptions::new()
                .write(true)
                .create_new(true)
                .mode(0o600)
                .open(&path)?;
            use std::io::Write as _;
            file.write_all(contents.as_bytes())?;
        }
        #[cfg(not(unix))]
        {
            std::fs::write(&path, contents)?;
        }
        Ok(Self { path })
    }
}

impl Drop for TempFile {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.path);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::permissions::AccessMode;

    fn spec(kind: AgentKind, permissions: RunPermissions) -> AgentSpec {
        AgentSpec {
            kind,
            approvals: true,
            binary: PathBuf::from("/nonexistent/agent"),
            cwd: PathBuf::from("/work"),
            prompt: "hello".into(),
            session_id: None,
            model: None,
            permissions,
            images: Vec::new(),
            bot_open_id: None,
            bot_name: None,
        }
    }

    fn permissions(default_access: AccessMode, max_access: AccessMode) -> RunPermissions {
        RunPermissions {
            default_access,
            max_access,
            claude_permission_mode: None,
        }
    }

    fn arg_after<'a>(args: &'a [String], flag: &str) -> Option<&'a str> {
        args.windows(2)
            .find(|window| window[0] == flag)
            .map(|window| window[1].as_str())
    }

    #[test]
    fn claude_argv_reflects_the_effective_access() {
        for (access, expected) in [
            (AccessMode::ReadOnly, "plan"),
            (AccessMode::Workspace, "acceptEdits"),
            (AccessMode::Full, "bypassPermissions"),
        ] {
            let (args, stdin) = build_agent_args(
                &spec(AgentKind::Claude, permissions(access, access)),
                Path::new("/tmp/system.md"),
            )
            .expect("claude args");
            assert_eq!(
                arg_after(&args, "--permission-mode"),
                Some(expected),
                "access {access:?}"
            );
            // Claude gets the prompt on stdin, not argv. Workspace/acceptEdits
            // enables the approval protocol, so the prompt is a stream-json
            // user frame instead of plain text.
            if access == AccessMode::Workspace {
                assert!(stdin.contains("\"text\":\"hello\""), "{stdin}");
                assert!(
                    args.windows(2)
                        .any(|w| w == ["--input-format", "stream-json"])
                );
            } else {
                assert_eq!(stdin, "hello");
                assert!(!args.iter().any(|arg| arg == "--input-format"));
            }
            assert!(!args.iter().any(|arg| arg == "hello"));
        }
    }

    #[test]
    fn non_im_runs_disable_the_approval_protocol() {
        let mut spec = spec(
            AgentKind::Claude,
            permissions(AccessMode::Workspace, AccessMode::Workspace),
        );
        spec.approvals = false;
        let (args, stdin) =
            build_agent_args(&spec, Path::new("/tmp/system.md")).expect("claude args");
        assert_eq!(stdin, "hello");
        assert!(!args.iter().any(|arg| arg == "--input-format"));
        assert!(!args.iter().any(|arg| arg == "--permission-prompt-tool"));
    }

    #[test]
    fn codex_argv_reflects_the_effective_access() {
        for (access, expected) in [
            (AccessMode::ReadOnly, "read-only"),
            (AccessMode::Workspace, "workspace-write"),
            (AccessMode::Full, "danger-full-access"),
        ] {
            let (args, stdin) = build_agent_args(
                &spec(AgentKind::Codex, permissions(access, access)),
                Path::new("/tmp/system.md"),
            )
            .expect("codex args");
            assert_eq!(
                arg_after(&args, "--sandbox"),
                Some(expected),
                "access {access:?}"
            );
            // The Codex system prompt is prepended to stdin.
            assert!(stdin.contains("hello"));
        }
    }

    #[test]
    fn codex_argv_carries_attachment_images_before_the_prompt() {
        let mut spec = spec(
            AgentKind::Codex,
            permissions(AccessMode::Workspace, AccessMode::Workspace),
        );
        spec.images = vec!["/tmp/a.png".into(), "/tmp/b.jpg".into()];
        let (args, _) = build_agent_args(&spec, Path::new("/tmp/system.md")).expect("codex args");

        let images: Vec<_> = args
            .windows(2)
            .filter(|window| window[0] == "--image")
            .map(|window| window[1].as_str())
            .collect();
        assert_eq!(images, ["/tmp/a.png", "/tmp/b.jpg"]);
        // Images live before the `--` separator, which itself precedes stdin.
        let first_image = args.iter().position(|arg| arg == "--image").unwrap();
        let separator = args.iter().position(|arg| arg == "--").unwrap();
        assert!(first_image < separator);
        assert_eq!(args.last().map(String::as_str), Some("-"));
    }

    #[test]
    fn codex_resume_carries_attachment_images_before_the_thread_id() {
        let mut spec = spec(
            AgentKind::Codex,
            permissions(AccessMode::Workspace, AccessMode::Workspace),
        );
        spec.session_id = Some("thread-1".into());
        spec.images = vec!["/tmp/a.png".into()];
        let (args, _) = build_agent_args(&spec, Path::new("/tmp/system.md")).expect("codex args");

        let image = args.iter().position(|arg| arg == "--image").unwrap();
        let thread = args.iter().position(|arg| arg == "thread-1").unwrap();
        assert!(image < thread);
        assert_eq!(args.get(image + 1).map(String::as_str), Some("/tmp/a.png"));
    }

    #[test]
    fn claude_argv_never_receives_image_flags() {
        let mut spec = spec(
            AgentKind::Claude,
            permissions(AccessMode::Workspace, AccessMode::Workspace),
        );
        // Even if the caller fills the field, Claude only gets paths in the
        // prompt text (the TS bridge does the same).
        spec.images = vec!["/tmp/a.png".into()];
        let (args, _) = build_agent_args(&spec, Path::new("/tmp/system.md")).expect("claude args");
        assert!(!args.iter().any(|arg| arg == "--image"));
    }

    #[test]
    fn clamping_applies_before_argv_is_built() {
        // Requested full, capped at workspace: both adapters must see the cap.
        let capped = permissions(AccessMode::Full, AccessMode::Workspace);
        let (claude, _) = build_agent_args(
            &spec(AgentKind::Claude, capped),
            Path::new("/tmp/system.md"),
        )
        .expect("claude args");
        assert_eq!(arg_after(&claude, "--permission-mode"), Some("acceptEdits"));

        let (codex, _) =
            build_agent_args(&spec(AgentKind::Codex, capped), Path::new("/tmp/system.md"))
                .expect("codex args");
        assert_eq!(arg_after(&codex, "--sandbox"), Some("workspace-write"));
    }
}
