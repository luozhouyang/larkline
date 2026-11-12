//! Spawn and stream a local agent CLI.
//!
//! The bridge never links an agent: it runs the installed `claude`/`codex`
//! binary, writes the prompt on stdin, and translates line-delimited JSON.
//! The process is killed when the handle is dropped, which is what makes
//! `/stop` and daemon shutdown predictable.

use std::path::{Path, PathBuf};
use std::process::Stdio;

use anyhow::{Context as _, anyhow};
use larkline_agent::{
    AgentEvent, BridgePromptContext, ClaudeArgsInput, ClaudeStreamTranslator, CodexArgsInput,
    CodexFinishReason, CodexJsonlTranslator, SandboxMode, build_bridge_system_prompt,
    build_claude_args, build_codex_args, prefix_bridge_system_prompt,
};
use larkline_core::AgentKind;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::process::{Child, ChildStdout, Command};

#[derive(Debug, Clone)]
pub struct AgentSpec {
    pub kind: AgentKind,
    pub binary: PathBuf,
    pub cwd: PathBuf,
    pub prompt: String,
    pub session_id: Option<String>,
    pub model: Option<String>,
    pub sandbox: SandboxMode,
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
    stdout: BufReader<ChildStdout>,
    translator: Translator,
    /// Removed on drop: the agent has already read it by then.
    _system_prompt: Option<TempFile>,
}

impl AgentProcess {
    pub async fn spawn(spec: AgentSpec) -> anyhow::Result<Self> {
        let system_prompt = build_bridge_system_prompt(Some(&BridgePromptContext {
            bot_open_id: spec.bot_open_id.clone(),
            bot_name: spec.bot_name.clone(),
        }));

        let system_prompt_file = TempFile::new(&system_prompt)?;
        let (args, stdin_prompt) = match spec.kind {
            AgentKind::Claude => (
                build_claude_args(&ClaudeArgsInput {
                    permission_mode: None,
                    system_prompt_file: Some(&system_prompt_file.path),
                    session_id: spec.session_id.as_deref(),
                    model: spec.model.as_deref(),
                }),
                spec.prompt.clone(),
            ),
            AgentKind::Codex => {
                let args = build_codex_args(&CodexArgsInput {
                    cwd: spec.cwd.to_string_lossy().into_owned(),
                    sandbox: spec.sandbox,
                    thread_id: spec.session_id.clone(),
                    images: Vec::new(),
                    ignore_user_config: false,
                    ignore_rules: None,
                    model: spec.model.clone(),
                })
                .map_err(|e| anyhow!("invalid codex arguments: {e}"))?;
                // Codex has no `--append-system-prompt-file`; the system
                // prompt is prepended to the stdin prompt instead.
                (
                    args,
                    prefix_bridge_system_prompt(
                        &spec.prompt,
                        Some(&BridgePromptContext {
                            bot_open_id: spec.bot_open_id.clone(),
                            bot_name: spec.bot_name.clone(),
                        }),
                    ),
                )
            }
        };

        tracing::info!(
            agent = spec.kind.binary_name(),
            cwd = %spec.cwd.display(),
            resume = spec.session_id.is_some(),
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
        if let Some(mut stdin) = child.stdin.take() {
            stdin
                .write_all(stdin_prompt.as_bytes())
                .await
                .context("failed to write prompt to agent stdin")?;
            stdin.shutdown().await.ok();
        }

        let translator = match spec.kind {
            AgentKind::Claude => Translator::Claude(ClaudeStreamTranslator::new()),
            AgentKind::Codex => Translator::Codex(CodexJsonlTranslator::new()),
        };

        Ok(Self {
            child,
            stdout: BufReader::new(stdout),
            translator,
            _system_prompt: Some(system_prompt_file),
        })
    }

    /// Next translated event batch, or `None` at EOF.
    pub async fn next_events(&mut self) -> Option<Vec<AgentEvent>> {
        let mut line = String::new();
        match self.stdout.read_line(&mut line).await {
            Ok(0) => None,
            Ok(_) => Some(self.translator.translate(line.trim_end())),
            Err(err) => {
                tracing::warn!(error = %err, "agent stdout read failed");
                None
            }
        }
    }

    /// Flush a terminal event when the child exits without one.
    pub async fn finish(&mut self) -> Vec<AgentEvent> {
        let events = self.translator.finish();
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
