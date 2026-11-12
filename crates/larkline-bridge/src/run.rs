//! Run flow: policy -> queue -> card -> agent -> card updates -> session.
//!
//! Working directories are per chat: `/cd` persists an override in the
//! workspace store, and every run snapshots the effective cwd before spawning
//! the agent. Changing the cwd mid-run therefore never moves an already
//! running agent; the next message picks up the new directory.
//!
//! Every chat is also a debounce scope (see [`crate::pending`], a port of
//! `src/bot/pending-queue.ts`): a message opens a 600ms quiet window and
//! messages arriving inside it merge into one agent run. While that run is
//! live the scope is blocked, so later messages keep accumulating instead of
//! spawning a competing agent; when the run ends the window re-arms and the
//! next batch starts. `/stop` aborts the live run and drops the queued batch.
//!
//! The TypeScript keepalive watchdog is **not** started here: its decisions
//! need a WebSocket connection status and a force-reconnect hook that
//! `larkline-lark` does not expose yet. See
//! [`crate::keepalive::KEEPALIVE_GAPS`] for the typed gap.

use std::collections::{HashMap, HashSet};
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex as StdMutex};
use std::time::{Duration, Instant};

use anyhow::Context as _;
use lark_channel::event::CardActionEvent;
use lark_channel::message::{MessageChatType, MessageSenderType, NormalizedMessage};
use larkline_agent::{
    AgentEvent, PromptContext, PromptMention, SandboxMode, TerminationReason, build_agent_prompt,
};
use larkline_core::{
    AppPaths, MessageReplyMode, ProfileConfig, SessionStore, TenantBrand, WorkspaceStore,
};
use larkline_lark::reactions::{TYPING, add_reaction, remove_reaction};
use larkline_lark::{CARD_SAFE_AGE, Channel, RestClient, STREAMING_ELEMENT_ID, StreamingCard};
use serde_json::{Value, json};
use tokio::sync::{Mutex, Notify};
use tokio::task::JoinHandle;

use crate::agent_runner::{AgentProcess, AgentSpec};
use crate::callback::CallbackTokens;
use crate::pending::{PendingQueue, QueueLimits};
use crate::policy;
use crate::render::RunView;

/// Card updates are throttled; Feishu rate-limits card writes and a token
/// stream would otherwise produce dozens of updates per second.
const CARD_UPDATE_INTERVAL: Duration = Duration::from_millis(900);
/// Hard cap for one agent run.
const RUN_TIMEOUT: Duration = Duration::from_secs(30 * 60);
/// Cards over this many characters are truncated (CardKit caps JSON size).
const MAX_CARD_CHARS: usize = 8_000;
/// Quiet window before a batch of messages starts a run. The TypeScript bridge
/// uses the same 600ms (`DEBOUNCE_MS` in `src/bot/channel.ts`).
const DEBOUNCE: Duration = Duration::from_millis(600);
/// Upper bound on messages queued per chat.
///
/// The TypeScript queue is unbounded; this is a Rust-only memory guard for a
/// bridge whose runs may last up to [`RUN_TIMEOUT`]. On overflow the oldest
/// message is dropped (the newest user intent survives) and a warning is
/// logged; [`QueueLimits::unbounded`] restores exact TS semantics.
const MAX_PENDING_PER_SCOPE: usize = 100;

pub struct Bridge {
    pub channel: Channel,
    pub profile: ProfileConfig,
    pub agent_binary: PathBuf,
    pub bot_open_id: Option<String>,
    pub bot_name: Option<String>,
    paths: AppPaths,
    tokens: CallbackTokens,
    sessions: Mutex<SessionStore>,
    workspaces: Mutex<WorkspaceStore>,
    /// Live run per chat. A run keeps its chat's pending scope blocked.
    active: StdMutex<HashMap<String, ActiveRun>>,
    /// Debounced per-chat message queue (pure structure; no timers inside).
    pending: StdMutex<PendingQueue<NormalizedMessage>>,
    /// Wakes the queue driver when a batch is armed, cancelled or unblocked.
    pending_wakeup: Notify,
    /// Whether the queue driver task has been spawned (lazily, on first push).
    pending_driver_started: AtomicBool,
    /// Id for the next run; guards [`Bridge::finish_run`] against a late guard
    /// from an aborted run clearing its successor's block.
    next_run_id: AtomicU64,
}

/// One live agent run owned by a chat.
struct ActiveRun {
    id: u64,
    handle: JoinHandle<()>,
}

/// Clears the queue block when a run ends, including an abort (`/stop`) that
/// never reaches the end of the spawned future.
struct RunGuard {
    bridge: Arc<Bridge>,
    scope: String,
    id: u64,
}

impl Drop for RunGuard {
    fn drop(&mut self) {
        self.bridge.finish_run(&self.scope, self.id);
    }
}

impl Bridge {
    pub fn new(
        channel: Channel,
        profile: ProfileConfig,
        agent_binary: PathBuf,
        cwd: PathBuf,
        bot_open_id: Option<String>,
        bot_name: Option<String>,
        paths: AppPaths,
    ) -> anyhow::Result<Self> {
        let sessions = SessionStore::load(&paths.sessions_file)
            .with_context(|| format!("cannot load {}", paths.sessions_file.display()))?;

        // `workspaces.default` in the profile is authoritative. The CLI
        // resolves its `--cwd` (otherwise `paths.default_workspace_dir`)
        // before constructing the bridge, so `cwd` is the runtime fallback for
        // profiles that do not define a default.
        let default_dir = match profile_default_workspace(&profile) {
            Some(default_dir) => default_dir,
            None if !cwd.as_os_str().is_empty() => cwd,
            None => paths.default_workspace_dir.clone(),
        };
        let workspaces = WorkspaceStore::load_with_default(&paths.workspaces_file, &default_dir)
            .with_context(|| format!("cannot load {}", paths.workspaces_file.display()))?;

        Ok(Self {
            channel,
            profile,
            agent_binary,
            bot_open_id,
            bot_name,
            paths,
            tokens: CallbackTokens::new(),
            sessions: Mutex::new(sessions),
            workspaces: Mutex::new(workspaces),
            active: StdMutex::new(HashMap::new()),
            pending: StdMutex::new(PendingQueue::with_limits(
                DEBOUNCE,
                QueueLimits::per_scope(MAX_PENDING_PER_SCOPE),
            )),
            pending_wakeup: Notify::new(),
            pending_driver_started: AtomicBool::new(false),
            next_run_id: AtomicU64::new(1),
        })
    }

    pub fn sessions_path(&self) -> &std::path::Path {
        self.paths.sessions_file.as_path()
    }

    pub async fn handle_message(self: &Arc<Self>, message: NormalizedMessage) {
        if !policy::allowed(&self.profile, &message, self.bot_open_id.as_deref()) {
            return;
        }
        let text = message.text.trim().to_string();
        if text.is_empty() {
            return;
        }

        if let Some(reply) = self.handle_command(&message, &text).await {
            // Commands bypass the debounce queue and drop anything that was
            // waiting for this chat (TS: `pending.cancel(scope)`).
            self.discard_pending(&message.chat_id);
            let _ = self.channel.send_text(&message.chat_id, reply).await;
            return;
        }

        self.enqueue(message);
    }

    /// Queue a message for its chat and make sure the debounce driver runs.
    fn enqueue(self: &Arc<Self>, message: NormalizedMessage) {
        // Scope = chat id: the Rust bridge has no per-topic session split yet.
        // `PendingQueue` accepts any scope string, so a topic scope is a
        // one-line change once sessions/active learn about threads (the TS
        // bridge uses `chatId:threadId` for topic groups).
        let scope = message.chat_id.clone();
        let outcome = {
            let mut pending = lock(&self.pending);
            pending.push(&scope, message, Instant::now())
        };
        if outcome.dropped > 0 {
            tracing::warn!(
                chat = %scope,
                queued = outcome.queued,
                dropped = outcome.dropped,
                "pending queue full; dropped the oldest queued message"
            );
        }
        tracing::debug!(chat = %scope, queued = outcome.queued, "message queued");
        self.ensure_pending_driver();
        self.pending_wakeup.notify_one();
    }

    /// Start the one queue driver task on first use.
    fn ensure_pending_driver(self: &Arc<Self>) {
        if self.pending_driver_started.swap(true, Ordering::AcqRel) {
            return;
        }
        let bridge = Arc::clone(self);
        match tokio::runtime::Handle::try_current() {
            Ok(runtime) => {
                runtime.spawn(bridge.pending_driver());
            }
            Err(_) => {
                self.pending_driver_started.store(false, Ordering::Release);
                tracing::warn!("no tokio runtime available to start the pending queue driver");
            }
        }
    }

    /// Sleep until the earliest quiet window ends, then start every due batch.
    async fn pending_driver(self: Arc<Self>) {
        loop {
            let deadline = lock(&self.pending).next_deadline();
            match deadline {
                Some(deadline) => {
                    tokio::select! {
                        _ = tokio::time::sleep_until(tokio::time::Instant::from_std(deadline)) => {}
                        _ = self.pending_wakeup.notified() => {}
                    }
                }
                None => self.pending_wakeup.notified().await,
            }

            let now = Instant::now();
            while self.start_due_batch(now) {}
        }
    }

    /// Take the next due batch and spawn its run, atomically with respect to
    /// `/stop`.
    ///
    /// The pending lock is held until the run is registered in `active`, so a
    /// concurrent stop either sees no run at all (and the batch stays queued)
    /// or sees the registered run and aborts it. `take_due` marks the scope
    /// blocked while it is still under the same lock.
    fn start_due_batch(self: &Arc<Self>, now: Instant) -> bool {
        let mut pending = lock(&self.pending);
        let Some((scope, batch)) = pending.take_due(now) else {
            return false;
        };

        let id = self.next_run_id.fetch_add(1, Ordering::Relaxed);
        // The task waits until it is registered before doing any work, so a
        // run that finishes immediately cannot strand its block.
        let (start, ready) = tokio::sync::oneshot::channel();
        let bridge = Arc::clone(self);
        let task_scope = scope.clone();
        let handle = tokio::spawn(async move {
            let _ = ready.await;
            let _guard = RunGuard {
                bridge: Arc::clone(&bridge),
                scope: task_scope,
                id,
            };
            if let Err(err) = bridge.run_batch(batch).await {
                tracing::error!(error = %err, "agent run failed");
            }
        });

        let mut active = lock(&self.active);
        active.retain(|_, run| !run.handle.is_finished());
        active.insert(scope, ActiveRun { id, handle });
        drop(active);
        drop(pending);
        let _ = start.send(());
        true
    }

    /// Run finished (or was aborted): release its chat for the next batch.
    ///
    /// Only the run that still owns the chat may unblock it, so a guard from
    /// a delayed abort can never clear its successor's block.
    fn finish_run(&self, scope: &str, id: u64) {
        let mut pending = lock(&self.pending);
        let owns = {
            let mut active = lock(&self.active);
            match active.get(scope) {
                Some(run) if run.id == id => {
                    active.remove(scope);
                    true
                }
                _ => false,
            }
        };
        if !owns {
            return;
        }
        pending.unblock(scope, Instant::now());
        drop(pending);
        self.pending_wakeup.notify_one();
    }

    /// Abort a live run. The caller must hold the pending lock (lock order is
    /// pending -> active).
    fn abort_run(&self, scope: &str) -> bool {
        let mut active = lock(&self.active);
        match active.remove(scope) {
            Some(run) => {
                run.handle.abort();
                true
            }
            None => false,
        }
    }

    /// `/stop` and the card stop button: abort the run, drop its queued
    /// follow-up batch, and clear the block so the chat can be used again.
    fn stop_scope(&self, scope: &str) -> (bool, usize) {
        // Holding the pending lock makes stop atomic with respect to a driver
        // that is registering a new batch (same lock order as `start_due_batch`).
        let mut pending = lock(&self.pending);
        let stopped = self.abort_run(scope);
        let dropped = pending.cancel(scope).len();
        pending.unblock(scope, Instant::now());
        drop(pending);
        self.pending_wakeup.notify_one();
        (stopped, dropped)
    }

    /// Drop the chat's queued batch (any handled command does this).
    fn discard_pending(&self, scope: &str) -> usize {
        let dropped = {
            let mut pending = lock(&self.pending);
            pending.cancel(scope).len()
        };
        self.pending_wakeup.notify_one();
        dropped
    }

    async fn handle_command(&self, message: &NormalizedMessage, text: &str) -> Option<String> {
        let command = text.split_whitespace().next().unwrap_or_default();
        let args = text.get(command.len()..).unwrap_or_default().trim();
        match command {
            "/stop" => {
                let (stopped, _dropped) = self.stop_scope(&message.chat_id);
                Some(if stopped {
                    "已停止当前任务。".to_string()
                } else {
                    "当前没有正在运行的任务。".to_string()
                })
            }
            "/new" | "/reset" => match self.sessions.lock().await.clear(&message.chat_id) {
                Ok(()) => Some("已开启新会话。".to_string()),
                Err(err) => Some(format!("重置失败：{err}")),
            },
            "/cd" => Some(self.handle_cd(&message.chat_id, args).await),
            "/pwd" => Some(self.handle_pwd(&message.chat_id).await),
            "/status" => Some(self.handle_status(&message.chat_id).await),
            "/help" => Some(
                "可用命令：\n\
                 /new 或 /reset 开启新会话\n\
                 /cd <绝对路径> 切换工作目录\n\
                 /pwd 显示当前工作目录\n\
                 /status 显示当前状态\n\
                 /stop 中断当前任务\n\
                 /help 显示帮助"
                    .to_string(),
            ),
            _ => None,
        }
    }

    /// `/cd <absolute path>`: canonicalize and persist a per-chat cwd.
    async fn handle_cd(&self, chat_id: &str, args: &str) -> String {
        let path = match validate_cd_target(args) {
            Ok(path) => path,
            Err(reply) => return reply,
        };
        let mut workspaces = self.workspaces.lock().await;
        match workspaces.set(chat_id, &path) {
            Ok(()) => format!("✓ 已切换 cwd 到 {}", path.display()),
            Err(err) => format!("保存工作目录失败：{err}"),
        }
    }

    /// `/pwd`: the effective cwd for this chat.
    async fn handle_pwd(&self, chat_id: &str) -> String {
        let workspaces = self.workspaces.lock().await;
        let cwd = effective_cwd(&workspaces, chat_id, &self.paths.default_workspace_dir);
        format!("当前 cwd：{}", cwd.display())
    }

    /// `/status`: profile/agent/app/cwd/reply-mode/resumable-session summary.
    async fn handle_status(&self, chat_id: &str) -> String {
        let cwd = {
            let workspaces = self.workspaces.lock().await;
            effective_cwd(&workspaces, chat_id, &self.paths.default_workspace_dir)
        };
        let cwd_str = cwd.to_string_lossy().into_owned();
        let resume = {
            let sessions = self.sessions.lock().await;
            sessions.resume_for(chat_id, &cwd_str).map(str::to_string)
        };

        let app = self.profile.app();
        let app_id = app.map(|app| app.id.as_str()).unwrap_or("(未配置)");
        let tenant = app
            .map(|app| tenant_label(app.tenant))
            .unwrap_or("(未配置)");
        let session = match resume {
            Some(id) => format!("可恢复（{}…）", id.chars().take(8).collect::<String>()),
            None => "不可恢复（当前 cwd 下没有会话）".to_string(),
        };

        format!(
            "📊 当前状态\n\
             profile: {}\n\
             agent: {}\n\
             app id: {app_id}\n\
             tenant: {tenant}\n\
             cwd: {}\n\
             reply mode: {}\n\
             session: {session}",
            self.paths.profile,
            self.profile.agent_kind.display_name(),
            cwd.display(),
            reply_mode_label(self.profile.message_reply()),
        )
    }

    /// Handle a click on a bridge-rendered button.
    ///
    /// Returns a short user-facing status when the click was ours, `None` for
    /// foreign callbacks (agent-sent cards) which are not forwarded yet.
    pub async fn handle_card_action(&self, action: &CardActionEvent) -> Option<String> {
        let value = action.action.value.as_object()?;
        if value.get("__bridge_cb").and_then(|v| v.as_bool()) != Some(true) {
            return None;
        }
        let action_name = value.get("action")?.as_str()?;
        let scope = value
            .get("scope")
            .and_then(|v| v.as_str())
            .unwrap_or_default();
        let token = value
            .get("token")
            .and_then(|v| v.as_str())
            .unwrap_or_default();
        if !self.tokens.verify(scope, action_name, token) {
            tracing::warn!(scope, action_name, "rejected card callback with bad token");
            return Some("操作已失效，请重新发起。".to_string());
        }

        match action_name {
            "stop" => {
                // The stop button is the card equivalent of `/stop`: abort the
                // run and drop the batch queued while it was live.
                let (stopped, _dropped) = self.stop_scope(scope);
                Some(if stopped {
                    "已停止当前任务。".to_string()
                } else {
                    "当前没有正在运行的任务。".to_string()
                })
            }
            _ => None,
        }
    }

    /// Effective cwd for a run, creating the directory when needed.
    ///
    /// A `/cd` target that cannot be created (deleted, permissions, replaced
    /// by a file) is reported and the profile default is used instead, so the
    /// run still has a chance to start.
    async fn workspace_cwd(&self, chat_id: &str) -> PathBuf {
        let (cwd, default) = {
            let workspaces = self.workspaces.lock().await;
            (
                effective_cwd(&workspaces, chat_id, &self.paths.default_workspace_dir),
                default_cwd(&workspaces, &self.paths.default_workspace_dir),
            )
        };

        match fs::create_dir_all(&cwd) {
            Ok(()) => cwd,
            Err(err) => {
                tracing::warn!(
                    cwd = %cwd.display(),
                    error = %err,
                    "cannot create workspace; falling back to the default"
                );
                if cwd != default {
                    if let Err(err) = fs::create_dir_all(&default) {
                        tracing::warn!(
                            cwd = %default.display(),
                            error = %err,
                            "cannot create the default workspace"
                        );
                    }
                }
                default
            }
        }
    }

    async fn run_batch(self: Arc<Self>, batch: Vec<NormalizedMessage>) -> anyhow::Result<()> {
        let Some(first) = batch.first() else {
            return Ok(());
        };
        let chat_id = first.chat_id.clone();
        let last_message_id = batch
            .last()
            .map(|message| message.message_id.clone())
            .unwrap_or_else(|| first.message_id.clone());
        let started = Instant::now();
        // Decorate the most recent message of the batch (the TS bridge also
        // reacts to `lastMsg`) with the "Typing" reaction for as long as the
        // run is live. Failures are logged and never fail the run; the guard
        // removes the reaction from `Drop`, which also covers `/stop`.
        let _working_reaction =
            WorkingReaction::start(self.channel.rest().clone(), last_message_id);
        // Snapshot the effective cwd for the whole run: session resume/record
        // and the agent process must agree even if `/cd` lands mid-run.
        let cwd = self.workspace_cwd(&chat_id).await;
        let cwd_str = cwd.to_string_lossy().into_owned();
        let resume = {
            let sessions = self.sessions.lock().await;
            sessions.resume_for(&chat_id, &cwd_str).map(str::to_string)
        };

        let context = self.prompt_context(&batch);
        let prompt = build_agent_prompt(&context, &batch_user_input(&batch));
        let use_card = self.profile.message_reply() != MessageReplyMode::Text;

        let mut view = RunView::default();
        let mut streaming: Option<StreamingCard> = None;
        let mut message_id: Option<String> = None;
        if use_card {
            let card = build_card(&self.profile, &view, "启动中", started, None);
            // Prefer a CardKit 2.0 streaming entity: every later update is one
            // whole-card PUT with its own sequence. If either the entity or the
            // reference message cannot be created, the plain message path below
            // is the fallback (and is the only path when streaming is off).
            match StreamingCard::start(&self.channel, &chat_id, card.clone(), STREAMING_ELEMENT_ID)
                .await
            {
                Ok(handle) => {
                    message_id = Some(handle.message_id().0.clone());
                    streaming = Some(handle);
                }
                Err(err) => {
                    tracing::warn!(
                        error = %err,
                        "cannot start streaming card; falling back to message card"
                    );
                    match self.channel.send_card(&chat_id, &card).await {
                        Ok(id) => message_id = Some(id.0),
                        Err(err) => {
                            tracing::warn!(error = %err, "cannot send run card; falling back to text");
                        }
                    }
                }
            }
        }

        let spec = AgentSpec {
            kind: self.profile.agent_kind,
            binary: self.agent_binary.clone(),
            cwd,
            prompt,
            session_id: resume,
            model: None,
            sandbox: SandboxMode::DangerFullAccess,
            bot_open_id: self.bot_open_id.clone(),
            bot_name: self.bot_name.clone(),
        };

        let mut process = match AgentProcess::spawn(spec).await {
            Ok(process) => process,
            Err(err) => {
                let message = err.to_string();
                view.apply(&AgentEvent::Error {
                    message: message.clone(),
                    termination_reason: TerminationReason::Failed,
                });
                self.finalize(
                    &chat_id,
                    &mut streaming,
                    message_id.as_deref(),
                    &view,
                    use_card,
                    started,
                )
                .await;
                return Err(err);
            }
        };

        let deadline = tokio::time::Instant::now() + RUN_TIMEOUT;
        let mut last_update = Instant::now();
        loop {
            match tokio::time::timeout_at(deadline, process.next_events()).await {
                Err(_) => {
                    process.kill().await;
                    view.apply(&AgentEvent::Error {
                        message: "运行超时（30 分钟），已终止。".to_string(),
                        termination_reason: TerminationReason::Timeout,
                    });
                    break;
                }
                Ok(None) => {
                    for event in process.finish().await {
                        view.apply(&event);
                    }
                    break;
                }
                Ok(Some(events)) => {
                    for event in &events {
                        view.apply(event);
                    }
                    if view.finished {
                        break;
                    }
                    if use_card && last_update.elapsed() >= CARD_UPDATE_INTERVAL {
                        self.refresh_card(
                            &chat_id,
                            &mut streaming,
                            message_id.as_deref(),
                            &view,
                            started,
                        )
                        .await;
                        last_update = Instant::now();
                    }
                }
            }
        }

        if let Some(key) = view.session_key().map(str::to_string) {
            let mut sessions = self.sessions.lock().await;
            if let Err(err) = sessions.set(&chat_id, &key, &cwd_str) {
                tracing::warn!(error = %err, "cannot persist session");
            }
        }

        self.finalize(
            &chat_id,
            &mut streaming,
            message_id.as_deref(),
            &view,
            use_card,
            started,
        )
        .await;
        Ok(())
    }

    async fn finalize(
        &self,
        chat_id: &str,
        streaming: &mut Option<StreamingCard>,
        message_id: Option<&str>,
        view: &RunView,
        use_card: bool,
        started: Instant,
    ) {
        if use_card {
            if let Some(handle) = streaming.as_mut() {
                // The final frame is a whole-card PUT with streaming mode off
                // and no stop button (build_card omits the button when `stop`
                // is None). set_card applies the CARD_SAFE_AGE guard; when the
                // window is gone or the card is dead the answer goes out as a
                // separate message instead.
                let card = self.final_card(view, started);
                match handle.set_card(&card).await {
                    Ok(true) => return,
                    Ok(false) => {
                        tracing::debug!(
                            "streaming card window closed; sending the answer as a message"
                        );
                    }
                    Err(err) => {
                        tracing::warn!(
                            error = %err,
                            "final streaming card update failed; sending the answer as a message"
                        );
                    }
                }
            } else if started.elapsed() < CARD_SAFE_AGE {
                if let Some(message_id) = message_id {
                    self.update_card(chat_id, Some(message_id), view, started)
                        .await;
                    return;
                }
            }
        }
        let answer = view.best_answer().trim();
        let body = if answer.is_empty() {
            "（没有输出）".to_string()
        } else {
            truncate_chars(answer, 4_000)
        };
        let _ = self.channel.send_text(chat_id, body).await;
    }

    /// Push the current view to the run card.
    ///
    /// While the CardKit entity is live the whole card is PUT back through
    /// [`StreamingCard::set_card`]. Once it is expired, finished, or a write
    /// failed, the same message is updated through the message API for the
    /// rest of the run (and only inside the safe window).
    async fn refresh_card(
        &self,
        chat_id: &str,
        streaming: &mut Option<StreamingCard>,
        message_id: Option<&str>,
        view: &RunView,
        started: Instant,
    ) {
        if let Some(handle) = streaming.as_mut() {
            if handle.is_live() {
                let card = larkline_lark::enable_streaming(self.live_card(chat_id, view, started))
                    .expect("builder-produced run card is valid");
                match handle.set_card(&card).await {
                    Ok(true) => return,
                    Ok(false) => {
                        tracing::debug!(
                            "streaming card window closed; falling back to message updates"
                        );
                    }
                    Err(err) => {
                        tracing::warn!(
                            error = %err,
                            "streaming card update failed; falling back to message updates"
                        );
                    }
                }
            }
        }
        if started.elapsed() < CARD_SAFE_AGE {
            self.update_card(chat_id, message_id, view, started).await;
        }
    }

    /// Card for a running frame (streaming mode stays on, signed stop button).
    fn live_card(&self, chat_id: &str, view: &RunView, started: Instant) -> lark_channel::Card {
        let token = if view.finished {
            None
        } else {
            Some(self.tokens.sign(chat_id, "stop"))
        };
        build_card(
            &self.profile,
            view,
            card_status(view),
            started,
            token.as_deref().map(|token| (chat_id, token)),
        )
    }

    /// Card for the last frame (streaming mode off, no stop button).
    fn final_card(&self, view: &RunView, started: Instant) -> lark_channel::Card {
        build_card(&self.profile, view, card_status(view), started, None)
    }

    async fn update_card(
        &self,
        chat_id: &str,
        message_id: Option<&str>,
        view: &RunView,
        started: Instant,
    ) {
        let Some(message_id) = message_id else {
            return;
        };
        let card = self.live_card(chat_id, view, started);
        if let Err(err) = self.channel.update_card_message(message_id, &card).await {
            tracing::warn!(error = %err, "card update failed");
        }
    }

    /// Prompt context for a debounced batch: identity from the first message,
    /// mentions merged across the batch, and every message id recorded.
    fn prompt_context(&self, batch: &[NormalizedMessage]) -> PromptContext {
        let first = &batch[0];
        let chat_type = match first.chat_type {
            MessageChatType::P2p => "p2p",
            MessageChatType::Group => "group",
            MessageChatType::Unknown => "unknown",
        };
        let sender_type = match first.sender.sender_type {
            MessageSenderType::User => "user",
            MessageSenderType::Bot => "bot",
            MessageSenderType::Unknown => "unknown",
        };
        let mentions = merge_mentions(batch);

        PromptContext {
            chat_id: first.chat_id.clone(),
            chat_type: chat_type.to_string(),
            sender_id: first.sender_id.clone(),
            // `NormalizedMessage` carries no display name (the TS SDK enriches
            // it); the bridge prompt gets the open id only.
            sender_name: None,
            sender_type: Some(sender_type.to_string()),
            bot_open_id: self.bot_open_id.clone(),
            mentions: if mentions.is_empty() {
                None
            } else {
                Some(mentions)
            },
            thread_id: first.thread_id.clone(),
            message_ids: Some(
                batch
                    .iter()
                    .map(|message| message.message_id.clone())
                    .collect(),
            ),
            source: "im".to_string(),
        }
    }
}

/// Join a batch's text into the `<user_input>` body.
///
/// Mirrors the TS `buildPrompt`: single-message batches stay verbatim; merged
/// batches prefix each non-empty segment with `[sender]:` so the agent can tell
/// who said what, and segments are separated by a blank line.
fn batch_user_input(batch: &[NormalizedMessage]) -> String {
    let annotate = batch.len() > 1;
    let texts: Vec<String> = batch
        .iter()
        .filter_map(|message| {
            let text = message.text.trim();
            if text.is_empty() {
                return None;
            }
            Some(if annotate {
                format!("{} {text}", sender_annotation(message))
            } else {
                text.to_string()
            })
        })
        .collect();

    if texts.is_empty() {
        // Also the TS fallback; the bridge drops empty-text messages at intake,
        // so this mainly keeps the helper total.
        "（对方发来一条没有正文的消息——通常是只 @ 了你的唤醒（ping）。请简短回应。）".to_string()
    } else {
        texts.join("\n\n")
    }
}

/// `[name (type)]:` prefix used when a batch merges several senders.
///
/// The Rust `NormalizedMessage` has no `senderName`, so the sender open id is
/// the stable label.
fn sender_annotation(message: &NormalizedMessage) -> String {
    let name = message.sender_id.as_str();
    match message.sender.sender_type {
        MessageSenderType::User => format!("[{name} (user)]:"),
        MessageSenderType::Bot => format!("[{name} (bot)]:"),
        MessageSenderType::Unknown => format!("[{name}]:"),
    }
}

/// Merge mentions from every message in the batch, deduplicating like the TS
/// `mergeMentions` (by `open_id`, else by `name:key`).
fn merge_mentions(batch: &[NormalizedMessage]) -> Vec<PromptMention> {
    let mut seen = HashSet::new();
    let mut mentions = Vec::new();
    for message in batch {
        for mention in &message.mentions {
            let key = if mention.open_id.is_empty() {
                format!(
                    "{}:{}",
                    mention.name.as_deref().unwrap_or_default(),
                    mention.key
                )
            } else {
                mention.open_id.clone()
            };
            if !seen.insert(key) {
                continue;
            }
            mentions.push(PromptMention {
                open_id: Some(mention.open_id.clone()).filter(|id| !id.is_empty()),
                name: mention.name.clone(),
                is_bot: Some(mention.mentioned_type == MessageSenderType::Bot),
            });
        }
    }
    mentions
}

/// Lifecycle of the "working" reaction on the triggering message.
///
/// The add request is asynchronous and the run can end before it completes —
/// notably `/stop`, which aborts the run task and skips every remaining
/// `await`. This state machine decides who deletes the reaction: the run's
/// cleanup normally, or the add task itself when it observes that the run is
/// already over.
#[derive(Debug, Default, PartialEq, Eq)]
enum ReactionState {
    /// No reaction was requested yet.
    #[default]
    Idle,
    /// The add request is in flight; no reaction id is known yet.
    Adding,
    /// The reaction exists and must be deleted on cleanup.
    Active(String),
    /// The run ended while the add was in flight; the add task deletes it.
    Abandoned,
    /// Cleanup finished (or is owned by the add task).
    Done,
}

impl ReactionState {
    fn begin_add(&mut self) {
        *self = Self::Adding;
    }

    /// Record the id returned by the add call.
    ///
    /// Returns `true` when the run already ended while the request was in
    /// flight, so the caller must delete the reaction itself.
    fn added(&mut self, reaction_id: &str) -> bool {
        match self {
            Self::Adding => {
                *self = Self::Active(reaction_id.to_string());
                false
            }
            // Abandoned, or an unexpected state: never leave a live reaction
            // behind, so let the caller delete it right away.
            _ => {
                *self = Self::Done;
                true
            }
        }
    }

    fn add_failed(&mut self) {
        *self = Self::Done;
    }

    /// End the run and return the reaction id to delete now, if known.
    ///
    /// When the add is still in flight the state becomes [`Abandoned`] and
    /// the add task is responsible for the deletion once it lands.
    ///
    /// [`Abandoned`]: ReactionState::Abandoned
    fn finish(&mut self) -> Option<String> {
        match std::mem::replace(self, Self::Done) {
            Self::Active(reaction_id) => Some(reaction_id),
            Self::Adding => {
                *self = Self::Abandoned;
                None
            }
            Self::Abandoned | Self::Idle | Self::Done => None,
        }
    }
}

/// Owns the "Typing" reaction for one run.
///
/// [`WorkingReaction::start`] launches the add without delaying the run; the
/// id is remembered in shared state. Dropping the guard ends the decoration,
/// so every exit path of [`Bridge::run_batch`] — success, error, timeout and
/// task abort — is covered. `Drop` cannot await, so the delete is detached:
/// it is best-effort, and a process exit before it runs can leave the
/// reaction behind.
struct WorkingReaction {
    rest: RestClient,
    message_id: String,
    state: Arc<StdMutex<ReactionState>>,
}

impl WorkingReaction {
    fn start(rest: RestClient, message_id: String) -> Self {
        let guard = Self {
            rest,
            message_id,
            state: Arc::new(StdMutex::new(ReactionState::default())),
        };
        lock_state(&guard.state).begin_add();

        let rest = guard.rest.clone();
        let message_id = guard.message_id.clone();
        let state = Arc::clone(&guard.state);
        tokio::spawn(async move {
            match add_reaction(&rest, &message_id, TYPING).await {
                Ok(reaction_id) => {
                    tracing::debug!(
                        message_id = %message_id,
                        reaction_id = %reaction_id,
                        "added working reaction"
                    );
                    let remove_now = lock_state(&state).added(&reaction_id);
                    if remove_now {
                        // The run ended before the add landed; clean up here.
                        if let Err(err) = remove_reaction(&rest, &message_id, &reaction_id).await {
                            tracing::warn!(
                                error = %err,
                                message_id = %message_id,
                                "cannot remove working reaction"
                            );
                        }
                    }
                }
                Err(err) => {
                    lock_state(&state).add_failed();
                    tracing::warn!(
                        error = %err,
                        message_id = %message_id,
                        "cannot add working reaction"
                    );
                }
            }
        });
        guard
    }
}

impl Drop for WorkingReaction {
    fn drop(&mut self) {
        let Some(reaction_id) = lock_state(&self.state).finish() else {
            return;
        };
        let rest = self.rest.clone();
        let message_id = self.message_id.clone();
        let removal = async move {
            if let Err(err) = remove_reaction(&rest, &message_id, &reaction_id).await {
                tracing::warn!(
                    error = %err,
                    message_id = %message_id,
                    "cannot remove working reaction"
                );
            }
        };
        // `/stop` aborts the run task, so this detached task is the only
        // cleanup path available there.
        match tokio::runtime::Handle::try_current() {
            Ok(handle) => {
                handle.spawn(removal);
            }
            Err(_) => {
                tracing::warn!(
                    message_id = %self.message_id,
                    "no tokio runtime to remove working reaction"
                );
            }
        }
    }
}

/// Lock the reaction state, recovering the guard if a panic poisoned it.
fn lock_state(state: &StdMutex<ReactionState>) -> std::sync::MutexGuard<'_, ReactionState> {
    state
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

/// Lock a queue/map, recovering the guard if a panic poisoned it.
fn lock<T>(mutex: &StdMutex<T>) -> std::sync::MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

/// Build the card for the current view.
fn build_card(
    profile: &ProfileConfig,
    view: &RunView,
    status: &str,
    started: Instant,
    stop: Option<(&str, &str)>,
) -> lark_channel::Card {
    let answer = view.best_answer().trim();
    let body = if answer.is_empty() {
        "…".to_string()
    } else {
        truncate_chars(answer, MAX_CARD_CHARS)
    };

    let mut elements = vec![json!({
        "tag": "markdown",
        "content": body,
        // Stable id so CardKit can also stream just the answer element later;
        // the run flow currently uses whole-card updates.
        "element_id": STREAMING_ELEMENT_ID,
    })];
    if !view.tools.is_empty() {
        let tools: Vec<String> = view
            .tools
            .iter()
            .rev()
            .take(6)
            .rev()
            .map(|tool| format!("• {tool}"))
            .collect();
        elements.push(json!({
            "tag": "markdown",
            "content": format!("**工具**\n{}", tools.join("\n"))
        }));
    }
    if let Some((scope, token)) = stop {
        elements.push(json!({
            "tag": "button",
            "text": { "tag": "plain_text", "content": "⏹ 停止" },
            "type": "danger",
            "behaviors": [{
                "type": "callback",
                "value": {
                    "__bridge_cb": true,
                    "action": "stop",
                    "scope": scope,
                    "token": token
                }
            }]
        }));
    }
    elements.push(json!({
        "tag": "note",
        "elements": [{
            "tag": "plain_text",
            "content": format!(
                "{} · {} · {}",
                profile.agent_kind.display_name(),
                status,
                view.footer(started.elapsed())
            )
        }]
    }));

    let summary = if answer.is_empty() {
        format!("{} · {}", profile.agent_kind.display_name(), status)
    } else {
        truncate_chars(answer.lines().next().unwrap_or_default(), 80)
    };
    let value = json!({
        "schema": "2.0",
        "config": { "update_multi": true, "summary": { "content": summary } },
        "body": { "elements": elements }
    });
    lark_channel::Card::from_value(value).expect("run card is valid")
}

/// One-line status for the card footer.
fn card_status(view: &RunView) -> &'static str {
    if view.error.is_some() {
        "失败"
    } else if view.finished {
        "完成"
    } else {
        "运行中"
    }
}

fn truncate_chars(value: &str, max: usize) -> String {
    if value.chars().count() <= max {
        value.to_string()
    } else {
        let mut out: String = value.chars().take(max).collect();
        out.push_str("\n\n…（内容过长已截断）");
        out
    }
}

/// Profile-configured default cwd (`workspaces.default`), if any.
///
/// The profile schema keeps untyped fields in `extra`; empty/blank values are
/// ignored, matching the TypeScript normalizer.
fn profile_default_workspace(profile: &ProfileConfig) -> Option<PathBuf> {
    profile
        .extra
        .get("workspaces")
        .and_then(Value::as_object)
        .and_then(|workspaces| workspaces.get("default"))
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(PathBuf::from)
}

/// Validate and canonicalize a `/cd` argument.
///
/// Only absolute paths are accepted; the path must exist and be a directory.
/// Returns the canonical path (symlinks and `..` resolved) on success, or a
/// user-facing message on failure.
fn validate_cd_target(input: &str) -> Result<PathBuf, String> {
    let input = input.trim();
    if input.is_empty() {
        return Err("用法：`/cd <绝对路径>`".to_string());
    }
    if !Path::new(input).is_absolute() {
        return Err(format!(
            "请使用绝对路径：`{input}`（例如 `/Users/me/project`）"
        ));
    }

    let canonical =
        fs::canonicalize(input).map_err(|_| format!("工作目录不存在或不可访问：`{input}`"))?;
    if !canonical.is_dir() {
        return Err(format!("路径不是目录：`{}`", canonical.display()));
    }
    Ok(canonical)
}

/// Effective cwd for `chat_id`: explicit `/cd` override, else the profile
/// default, else `fallback` when the store was loaded without a default.
fn effective_cwd(store: &WorkspaceStore, chat_id: &str, fallback: &Path) -> PathBuf {
    store
        .workspace_for(chat_id)
        .map(PathBuf::from)
        .unwrap_or_else(|| fallback.to_path_buf())
}

/// The store's profile default, with the same fallback as [`effective_cwd`].
fn default_cwd(store: &WorkspaceStore, fallback: &Path) -> PathBuf {
    store
        .default_dir()
        .map(PathBuf::from)
        .unwrap_or_else(|| fallback.to_path_buf())
}

fn tenant_label(tenant: TenantBrand) -> &'static str {
    match tenant {
        TenantBrand::Feishu => "feishu",
        TenantBrand::Lark => "lark",
    }
}

fn reply_mode_label(mode: MessageReplyMode) -> &'static str {
    match mode {
        MessageReplyMode::Card => "card",
        MessageReplyMode::Markdown => "markdown",
        MessageReplyMode::Text => "text",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use larkline_core::config::AgentKind;

    fn profile() -> ProfileConfig {
        ProfileConfig {
            agent_kind: AgentKind::Claude,
            ..ProfileConfig::default()
        }
    }

    /// Minimal `NormalizedMessage` fixture for queue/prompt-merge tests.
    fn message(id: &str, sender_id: &str, sender_type: &str, text: &str) -> NormalizedMessage {
        serde_json::from_value(json!({
            "message_id": id,
            "chat_id": "oc_1",
            "chat_type": "group",
            "sender_id": sender_id,
            "sender": { "open_id": sender_id, "sender_type": sender_type },
            "text": text,
        }))
        .expect("normalized message fixture")
    }

    #[test]
    fn single_message_batches_keep_the_text_verbatim() {
        let batch = vec![message("om_1", "ou_1", "user", "  hello  ")];
        assert_eq!(batch_user_input(&batch), "hello");
    }

    #[test]
    fn merged_batches_annotate_each_sender_and_join_with_blank_lines() {
        let batch = vec![
            message("om_1", "ou_1", "user", "first"),
            message("om_2", "ou_2", "bot", " second "),
        ];
        assert_eq!(
            batch_user_input(&batch),
            "[ou_1 (user)]: first\n\n[ou_2 (bot)]: second"
        );
    }

    #[test]
    fn unknown_sender_types_use_the_plain_annotation() {
        let batch = vec![
            message("om_1", "ou_1", "user", "a"),
            message("om_2", "ou_2", "unknown", "b"),
        ];
        assert_eq!(batch_user_input(&batch), "[ou_1 (user)]: a\n\n[ou_2]: b");
    }

    #[test]
    fn merge_mentions_deduplicates_by_open_id_and_keeps_order() {
        let mut first = message("om_1", "ou_1", "user", "a");
        first.mentions = vec![
            serde_json::from_value(json!({
                "key": "@_user_1",
                "open_id": "ou_bot",
                "name": "Bot",
                "mentioned_type": "bot"
            }))
            .unwrap(),
        ];
        let mut second = message("om_2", "ou_2", "user", "b");
        second.mentions = vec![
            serde_json::from_value(json!({
                "key": "@_user_2",
                "open_id": "ou_bot",
                "name": "Bot",
                "mentioned_type": "bot"
            }))
            .unwrap(),
            serde_json::from_value(json!({
                "key": "@_user_3",
                "open_id": "ou_other",
                "name": "Other",
                "mentioned_type": "user"
            }))
            .unwrap(),
        ];

        let mentions = merge_mentions(&[first, second]);
        assert_eq!(mentions.len(), 2);
        assert_eq!(mentions[0].open_id.as_deref(), Some("ou_bot"));
        assert_eq!(mentions[0].is_bot, Some(true));
        assert_eq!(mentions[1].open_id.as_deref(), Some("ou_other"));
        assert_eq!(mentions[1].is_bot, Some(false));
    }

    #[test]
    fn reaction_lifecycle_removes_once_on_normal_cleanup() {
        let mut state = ReactionState::default();
        state.begin_add();
        assert!(!state.added("re_1"));
        assert_eq!(state.finish(), Some("re_1".to_string()));
        // Dropping the guard a second time must not delete anything.
        assert_eq!(state.finish(), None);
    }

    #[test]
    fn reaction_lifecycle_hands_cleanup_to_the_add_task_after_abort() {
        let mut state = ReactionState::default();
        state.begin_add();
        // The run ended (e.g. `/stop` aborted the task) before the add landed.
        assert_eq!(state.finish(), None);
        // The add task must delete the reaction itself.
        assert!(state.added("re_2"));
        assert_eq!(state.finish(), None);
    }

    #[test]
    fn reaction_lifecycle_ignores_failed_adds() {
        let mut state = ReactionState::default();
        state.begin_add();
        state.add_failed();
        assert_eq!(state.finish(), None);
    }

    #[test]
    fn reaction_lifecycle_without_an_add_needs_no_cleanup() {
        let mut state = ReactionState::default();
        assert_eq!(state.finish(), None);
    }

    #[test]
    fn card_contains_answer_tools_and_footer() {
        let mut view = RunView::default();
        view.apply(&AgentEvent::Text {
            delta: "hello".into(),
        });
        view.apply(&AgentEvent::ToolUse {
            id: "t1".into(),
            name: "Bash".into(),
            input: json!({ "command": "ls" }),
        });
        view.apply(&AgentEvent::Done {
            session_id: None,
            thread_id: None,
            termination_reason: TerminationReason::Normal,
        });
        let card = build_card(&profile(), &view, "完成", Instant::now(), None);
        let text = serde_json::to_string(card.as_value()).unwrap();
        assert!(text.contains("hello"));
        assert!(text.contains("Bash"));
        assert!(text.contains("完成"));
    }

    #[test]
    fn run_card_exposes_the_streaming_element_and_safe_window() {
        let card = build_card(
            &profile(),
            &RunView::default(),
            "运行中",
            Instant::now(),
            Some(("oc_1", "deadbeef")),
        );
        assert_eq!(
            card.as_value()["body"]["elements"][0]["element_id"],
            STREAMING_ELEMENT_ID
        );

        // The bridge guard is the crate's CardKit constant, below Feishu's
        // ~10 minute server cutoff.
        assert_eq!(CARD_SAFE_AGE, Duration::from_secs(8 * 60 + 30));
        assert!(CARD_SAFE_AGE < Duration::from_secs(10 * 60));
    }

    #[test]
    fn running_card_carries_signed_stop_button() {
        let view = RunView::default();
        let card = build_card(
            &profile(),
            &view,
            "运行中",
            Instant::now(),
            Some(("oc_1", "deadbeef")),
        );
        let text = serde_json::to_string(card.as_value()).unwrap();
        assert!(text.contains("__bridge_cb"));
        assert!(text.contains("deadbeef"));
        assert!(text.contains("停止"));

        let finished = build_card(&profile(), &view, "完成", Instant::now(), None);
        let text = serde_json::to_string(finished.as_value()).unwrap();
        assert!(!text.contains("__bridge_cb"));
    }

    #[test]
    fn huge_answers_are_truncated() {
        let mut view = RunView::default();
        view.apply(&AgentEvent::Text {
            delta: "x".repeat(MAX_CARD_CHARS + 100),
        });
        let card = build_card(&profile(), &view, "完成", Instant::now(), None);
        let text = serde_json::to_string(card.as_value()).unwrap();
        assert!(text.contains("已截断"));
    }

    #[test]
    fn profile_default_workspace_reads_the_extra_map() {
        let mut profile = profile();
        assert_eq!(profile_default_workspace(&profile), None);

        profile.extra.insert(
            "workspaces".to_string(),
            json!({ "default": "  /profile/work  " }),
        );
        assert_eq!(
            profile_default_workspace(&profile),
            Some(PathBuf::from("/profile/work"))
        );

        // Blank and non-string values are ignored, like the TS normalizer.
        profile
            .extra
            .insert("workspaces".to_string(), json!({ "default": "   " }));
        assert_eq!(profile_default_workspace(&profile), None);
        profile
            .extra
            .insert("workspaces".to_string(), json!({ "default": 7 }));
        assert_eq!(profile_default_workspace(&profile), None);
    }

    #[test]
    fn validate_cd_target_canonicalizes_an_existing_directory() {
        let root = tempfile::tempdir().unwrap();
        let nested = root.path().join("nested");
        fs::create_dir_all(&nested).unwrap();

        // `nested/..` exercises the canonicalization (and must resolve to the
        // temp root, not to a path containing `..`).
        let requested = nested.join("..");
        let resolved = validate_cd_target(requested.to_str().unwrap()).unwrap();
        assert_eq!(resolved, root.path().canonicalize().unwrap());
        assert!(resolved.is_absolute());
    }

    #[test]
    fn validate_cd_target_rejects_relative_paths_and_empty_arguments() {
        for input in ["", "   "] {
            let err = validate_cd_target(input).unwrap_err();
            assert!(err.contains("用法"), "{err}");
        }
        for input in ["relative/path", "~/work", "../work"] {
            let err = validate_cd_target(input).unwrap_err();
            assert!(err.contains("绝对路径"), "{err}");
        }
    }

    #[test]
    fn validate_cd_target_rejects_missing_paths() {
        let root = tempfile::tempdir().unwrap();
        let missing = root.path().join("missing");
        let err = validate_cd_target(missing.to_str().unwrap()).unwrap_err();
        assert!(err.contains("不存在"), "{err}");
    }

    #[test]
    fn validate_cd_target_rejects_files() {
        let root = tempfile::tempdir().unwrap();
        let file = root.path().join("notes.txt");
        fs::write(&file, "hello").unwrap();
        let err = validate_cd_target(file.to_str().unwrap()).unwrap_err();
        assert!(err.contains("不是目录"), "{err}");
    }

    #[test]
    fn effective_cwd_prefers_the_chat_override_over_the_profile_default() {
        let root = tempfile::tempdir().unwrap();
        let mut store = WorkspaceStore::load_with_default(
            root.path().join("workspaces.json"),
            "/profile/default",
        )
        .unwrap();
        store.set("oc_1", "/work/one").unwrap();

        assert_eq!(
            effective_cwd(&store, "oc_1", Path::new("/cli/default")),
            PathBuf::from("/work/one")
        );
        assert_eq!(
            effective_cwd(&store, "oc_2", Path::new("/cli/default")),
            PathBuf::from("/profile/default")
        );
    }

    #[test]
    fn effective_cwd_uses_the_fallback_when_the_store_has_no_default() {
        let root = tempfile::tempdir().unwrap();
        let store = WorkspaceStore::load(root.path().join("workspaces.json")).unwrap();
        assert_eq!(
            effective_cwd(&store, "oc_1", Path::new("/cli/default")),
            PathBuf::from("/cli/default")
        );
        assert_eq!(
            default_cwd(&store, Path::new("/cli/default")),
            PathBuf::from("/cli/default")
        );
    }
}
