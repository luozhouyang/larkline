//! Run flow: policy -> queue -> card -> agent -> card updates -> session.
//!
//! Working directories are per session scope: `/cd` persists an override in
//! the workspace store, and every run snapshots the effective cwd before
//! spawning the agent. Changing the cwd mid-run therefore never moves an
//! already running agent; the next message picks up the new directory.
//!
//! A scope is the chat id for p2p / regular-group messages and
//! `chatId:threadId` for topic-group messages (see [`scope_key`]); each topic
//! thread therefore has its own session, cwd, pending queue and live run.
//! Every scope is also a debounce scope (see [`crate::pending`], a port of
//! `src/bot/pending-queue.ts`): a message opens a 600ms quiet window and
//! messages arriving inside it merge into one agent run. While that run is
//! live the scope is blocked, so later messages keep accumulating instead of
//! spawning a competing agent; when the run ends the window re-arms and the
//! next batch starts. `/stop` aborts the live run and drops the queued batch.
//!
//! ## `/stop` targeting
//!
//! Every live run is registered in [`Bridge::active`] by scope, so `/stop`
//! works for all three run kinds:
//!
//! * **Plain `/stop`** aborts exactly the scope of the message that sent it:
//!   the chat for p2p / regular groups, or `chatId:threadId` for a topic, and
//!   drops that scope's queued batch. This matches the TypeScript bridge.
//! * **`/stop <scope>`** (admin-only, like TypeScript's `handleStop`) aborts
//!   the run registered under that exact scope. This is how a user aborts a
//!   run that has no chat scope of its own: a cloud-doc comment run under
//!   `comment:<16-hex digest>` or a meeting run under `meeting:<meeting_id>`.
//!   `/status` lists every live scope so the id is discoverable.
//! * **The card stop button** signs the run scope into its callback token and
//!   calls the same `stop_scope`, so it aborts the same runs (IM runs have the
//!   only cards; comment/meeting runs have no stop button of their own).
//!
//! The abort path is shared: the task handle is aborted, the run guard removes
//! the `active` entry, the future drops its global run permit, the agent child
//! is killed by `kill_on_drop`, and the working reaction is removed by its
//! guard. Comment and meeting runs acquire their permit inside the run future,
//! so cancellation releases it just like normal completion.
//!
//! A second, global limit caps how many agent runs may be live at once across
//! every scope ([`crate::run_limit`], the port of `src/bot/process-pool.ts`).
//! The cap comes from the live profile's `preferences.maxConcurrentRuns` and is
//! refreshed by `reload_profile` after `/config set maxConcurrentRuns`. A due
//! batch that cannot get a slot stays in the pending queue — blocked only once
//! it actually starts — and the queue driver retries when a run releases its
//! slot. Comment and meeting runs, which have no pending queue, take a permit
//! with a bounded wait instead. Per-scope serialization is unchanged: one run
//! per scope, always.
//!
//! `/config set` and `/meeting` use a shared admin gate
//! ([`crate::owner::can_run_admin_command`]): configured admins (or the legacy
//! allowed-users fallback) plus the runtime app owner resolved from
//! `application/v6/applications/<app_id>`. The owner lookup fails closed for
//! non-admins; see [`crate::owner`] for the cache/refresh semantics.
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
use async_trait::async_trait;
use lark_channel::event::CardActionEvent;
use lark_channel::message::{
    MessageChatType, MessageSenderInfo, MessageSenderType, NormalizedMessage, Recipient,
};
use larkline_agent::{
    AgentEvent, PromptContext, PromptMention, SandboxMode, TerminationReason, build_agent_prompt,
};
use larkline_core::{
    AppPaths, CoreError, MessageReplyMode, Preferences, ProfileConfig, SessionStore, TenantBrand,
    WorkspaceStore,
    config::{ProfileMode, ReasoningDisplay},
};
use larkline_lark::reactions::{TYPING, add_reaction, remove_reaction};
use larkline_lark::{CARD_SAFE_AGE, Channel, RestClient, STREAMING_ELEMENT_ID, StreamingCard};
use serde_json::{Value, json};
use tokio::sync::{Mutex, Notify};
use tokio::task::{AbortHandle, JoinHandle};

use crate::agent_runner::{AgentProcess, AgentSpec};
use crate::callback::{CallbackAuth, CallbackExpected, CallbackTokens};
use crate::meeting::{self, AgentSettings, MeetingHandle, MeetingHost, MeetingManager};
use crate::owner::{self, OwnerGate, RestOwnerSource};
use crate::pending::{PendingQueue, QueueLimits};
use crate::policy;
use crate::render::RunView;
use crate::reply;
use crate::run_limit::{RunLimiter, RunPermit};
use crate::text_render::{TextFormat, TextRenderOptions, mask_emails, render_text};

/// Card updates are throttled; Feishu rate-limits card writes and a token
/// stream would otherwise produce dozens of updates per second.
const CARD_UPDATE_INTERVAL: Duration = Duration::from_millis(900);
/// Hard cap for one agent run.
pub(crate) const RUN_TIMEOUT: Duration = Duration::from_secs(30 * 60);
/// How long a comment / meeting run waits for a global run slot before it
/// gives up. Those runs have no pending queue, so waiting forever would leave
/// the user without an answer; the queue-driven IM path still waits
/// indefinitely because its messages stay queued.
pub(crate) const RUN_SLOT_WAIT: Duration = Duration::from_secs(30);
/// User-visible result when [`RUN_SLOT_WAIT`] expires.
pub(crate) const RUN_SLOT_TIMEOUT_REPLY: &str =
    "⏳ 当前并发任务已达上限，等待空闲运行槽超时，请稍后重试。";
/// Cards over this many characters are truncated (CardKit caps JSON size).
const MAX_CARD_CHARS: usize = 8_000;
/// Quiet window before a batch of messages starts a run. The TypeScript bridge
/// uses the same 600ms (`DEBOUNCE_MS` in `src/bot/channel.ts`).
const DEBOUNCE: Duration = Duration::from_millis(600);
/// Upper bound on messages queued per scope.
///
/// The TypeScript queue is unbounded; this is a Rust-only memory guard for a
/// bridge whose runs may last up to [`RUN_TIMEOUT`]. On overflow the oldest
/// message is dropped (the newest user intent survives) and a warning is
/// logged; [`QueueLimits::unbounded`] restores exact TS semantics.
const MAX_PENDING_PER_SCOPE: usize = 100;

/// Marker the bridge (and bridge-aware agents) put on callback button values.
const BRIDGE_CALLBACK_MARKER: &str = crate::callback::BRIDGE_CALLBACK_MARKER;
/// Token field of an agent-rendered callback button.
const BRIDGE_TOKEN_FIELD: &str = crate::callback::BRIDGE_TOKEN_FIELD;
/// Action bound into v1 tokens for agent-rendered callbacks.
const AGENT_CALLBACK_ACTION: &str = crate::callback::AGENT_CALLBACK_ACTION;

/// `/config` usage text (also returned for unknown keys/values).
const CONFIG_USAGE: &str = "用法：\n\
     /config 查看当前配置\n\
     /config set messageReply <card|markdown|text>\n\
     /config set showToolCalls <true|false>\n\
     /config set maxConcurrentRuns <1-50>\n\
     （修改需管理员）";

/// Reply for a `/config set` from a sender outside the admin gate.
const CONFIG_DENIED_REPLY: &str = "❌ 只有管理员可以修改配置。";

/// Reply for `/stop <scope>` from a sender outside the admin gate.
const STOP_SCOPE_DENIED_REPLY: &str = "❌ 指定 scope 停止任务仅管理员可用。";

/// Session / pending-queue / active-run scope for an inbound message.
///
/// Port of the TypeScript `scopeFor` (`src/bot/scope.ts`), minus the chat-mode
/// API lookup: the Rust SDK already normalizes `thread_id` from the message
/// event, and a present thread id is the authoritative topic signal there too.
///
/// * **p2p / regular group** (no thread id): the chat id, exactly the key
///   `sessions.json` and `workspaces.json` have always used.
/// * **topic group** (thread id present): `chatId:threadId`, so every topic
///   thread gets its own session, cwd, pending queue and live run.
///
/// A topic-group message whose event dropped `thread_id` falls back to the
/// chat id (the same fallback `scopeFor` applies; the TypeScript intake path
/// additionally backfills the id through a message lookup, which the Rust
/// bridge does not do). Pure: no I/O, no cache lookup.
pub fn scope_key(message: &NormalizedMessage) -> String {
    match message.thread_id.as_deref().filter(|id| !id.is_empty()) {
        Some(thread_id) => format!("{}:{thread_id}", message.chat_id),
        None => message.chat_id.clone(),
    }
}

/// Whether a run scope belongs to `chat_id` (the chat itself or a topic in it).
///
/// Card callbacks carry only the chat id, while a topic run's stop button is
/// signed with the full `${chatId}:${threadId}` scope, so the click check has
/// to accept both shapes without letting a different chat through.
fn scope_belongs_to_chat(scope: &str, chat_id: &str) -> bool {
    scope == chat_id
        || scope
            .strip_prefix(chat_id)
            .is_some_and(|rest| rest.starts_with(':'))
}

pub struct Bridge {
    pub channel: Channel,
    pub profile: ProfileConfig,
    pub agent_binary: PathBuf,
    pub bot_open_id: Option<String>,
    pub bot_name: Option<String>,
    paths: AppPaths,
    tokens: CallbackTokens,
    /// Verifier for agent-rendered (lark-cli signed) card callbacks. `None`
    /// when no callback key is configured: those clicks are then rejected.
    callback_auth: Option<CallbackAuth>,
    /// Live profile used by the run flow. `/config set` persists to
    /// `config.json` and swaps this snapshot, so preference changes apply
    /// without a restart. The public `profile` field stays the startup
    /// snapshot for modules that only need the agent identity/kind.
    live_profile: StdMutex<ProfileConfig>,
    /// Global cap on concurrent bridge runs (port of `ProcessPool`). Sized
    /// from the live profile and resized by `/config set maxConcurrentRuns`.
    run_limit: Arc<RunLimiter>,
    /// Cached application-owner lookup used by the admin command gate.
    owner: OwnerGate,
    /// Sessions are keyed by scope — the chat id, `chatId:threadId` for topic
    /// messages, or `doc:`/`comment:` scopes for comment runs. See
    /// [`scope_key`] and [`crate::comments`].
    pub(crate) sessions: Mutex<SessionStore>,
    workspaces: Mutex<WorkspaceStore>,
    /// Live run per scope. A run keeps its scope's pending queue blocked.
    active: StdMutex<HashMap<String, ActiveRun>>,
    /// Debounced per-scope message queue (pure structure; no timers inside).
    pending: StdMutex<PendingQueue<NormalizedMessage>>,
    /// Wakes the queue driver when a batch is armed, cancelled or unblocked.
    pending_wakeup: Notify,
    /// Whether the queue driver task has been spawned (lazily, on first push).
    pending_driver_started: AtomicBool,
    /// Id for the next run; guards [`Bridge::finish_run`] against a late guard
    /// from an aborted run clearing its successor's block.
    next_run_id: AtomicU64,
    /// In-meeting agent runtime; `Some` only while `extra.meeting.enabled` is
    /// on and [`Bridge::start_meeting`] ran inside a Tokio runtime.
    meeting: StdMutex<Option<MeetingRuntime>>,
}

/// The running VC-bot manager and the handle that stops its poll task.
struct MeetingRuntime {
    manager: Arc<MeetingManager>,
    handle: MeetingHandle,
}

/// One live agent run, keyed by its run scope.
///
/// IM batch runs (`start_due_batch`), comment runs (`comment:<hash>`) and
/// meeting runs (`meeting:<id>`) all live in [`Bridge::active`], so `/stop`
/// and the card stop button can abort any of them by scope. The handle is an
/// [`AbortHandle`] rather than a [`JoinHandle`] so a caller that already owns
/// the task handle (the meeting manager) can hand over abort rights without
/// giving up its own handle.
struct ActiveRun {
    id: u64,
    abort: AbortHandle,
}

/// A batch that owns a global run slot and may be spawned.
///
/// Held under the pending lock between `take_due` and registration in
/// `active`, so `/stop` can never observe a half-started run.
struct ClaimedBatch {
    scope: String,
    batch: Vec<NormalizedMessage>,
    permit: RunPermit,
}

/// Result of asking the pending queue for the next runnable batch.
enum BatchClaim {
    /// A due batch (and a slot) is ready.
    Claimed(ClaimedBatch),
    /// Nothing has finished its quiet window yet.
    NoDueBatch,
    /// A batch is due, but every global run slot is in use. The batch stays
    /// queued and the driver waits for a slot release.
    AtCapacity,
}

/// Result of [`Bridge::start_due_batch`], consumed by the queue driver.
enum BatchStart {
    Started,
    NoDueBatch,
    AtCapacity,
}

/// Clears the queue block when a run ends, including an abort (`/stop`) that
/// never reaches the end of the spawned future. Also owns the global run slot:
/// the permit is dropped *before* `finish_run` so a waking driver sees the
/// freed capacity (declaration order alone would make that fragile).
struct RunGuard {
    bridge: Arc<Bridge>,
    scope: String,
    id: u64,
    permit: Option<RunPermit>,
}

impl Drop for RunGuard {
    fn drop(&mut self) {
        drop(self.permit.take());
        self.bridge.finish_run(&self.scope, self.id);
    }
}

/// Removes a non-IM (comment / meeting) run from [`Bridge::active`].
///
/// Mirrors [`RunGuard`] for runs that acquire their global slot inside the
/// future: the future's drop releases the permit, then this guard removes the
/// active entry. Dropped on normal completion and on task abort (`/stop`).
struct TrackedRunGuard {
    bridge: Arc<Bridge>,
    scope: String,
    id: u64,
}

impl Drop for TrackedRunGuard {
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

        let callback_auth = CallbackAuth::from_profile(&profile);
        match callback_auth.as_ref() {
            Some(auth) => tracing::info!(
                key_versions = ?auth.key_versions(),
                "agent card callbacks enabled"
            ),
            None => tracing::info!(
                "agent card callbacks are disabled: no callback key configured \
                 (profile extra.callback.secret or {})",
                crate::callback::CALLBACK_SECRET_ENV
            ),
        }
        let live_profile = profile.clone();
        let run_limit = Arc::new(RunLimiter::new(profile.max_concurrent_runs()));
        tracing::info!(cap = run_limit.cap(), "global agent run concurrency cap");
        // The owner lookup reuses the bridge's tenant token cache. It stays
        // lazy: no request is made until an admin-gated command needs it.
        let owner = OwnerGate::new(Arc::new(RestOwnerSource::new(channel.rest().clone())));

        Ok(Self {
            channel,
            profile,
            agent_binary,
            bot_open_id,
            bot_name,
            paths,
            tokens: CallbackTokens::new(),
            callback_auth,
            live_profile: StdMutex::new(live_profile),
            run_limit,
            owner,
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
            meeting: StdMutex::new(None),
        })
    }

    /// Start the in-meeting agent (VC bot) manager.
    ///
    /// No-op (returns `false`) when `extra.meeting.enabled` is not set, when
    /// there is no Tokio runtime to host the poll task, or when a manager is
    /// already live (returns `true` then). The manager is stopped and its
    /// meetings are left by [`Bridge::stop_meeting`].
    pub fn start_meeting(self: &Arc<Self>) -> bool {
        let profile = self.live_profile();
        let config = meeting::config_from_profile(&profile);
        if !config.enabled {
            return false;
        }
        let mut slot = lock(&self.meeting);
        if slot.is_some() {
            return true;
        }
        if tokio::runtime::Handle::try_current().is_err() {
            tracing::warn!("no tokio runtime available to start the meeting manager");
            return false;
        }

        let api: Arc<dyn meeting::MeetingApi> =
            Arc::new(meeting::RestMeetingApi::new(self.channel.rest().clone()));
        let host: Arc<dyn MeetingHost> = Arc::new(BridgeMeetingHost {
            bridge: Arc::downgrade(self),
        });
        // The CLI routes `vc.bot.*` frames off its long connection, so the push
        // lane is wired; `received` only moves once a frame actually arrives.
        let agent = AgentSettings {
            kind: profile.agent_kind,
            binary: self.agent_binary.clone(),
            bot_open_id: self
                .bot_open_id
                .clone()
                .or_else(|| config.bot_open_id.clone()),
            bot_name: self.bot_name.clone().or_else(|| config.bot_name.clone()),
        };
        let manager = Arc::new(
            MeetingManager::new(config, agent, api, host)
                .with_run_limit(Arc::clone(&self.run_limit)),
        );
        manager.set_push_hooked(true, None);
        let handle = manager.start();
        tracing::info!(
            trigger = %manager.config().trigger,
            poll_interval_ms = manager.config().poll_interval_ms,
            "meeting agent started"
        );
        *slot = Some(MeetingRuntime { manager, handle });
        true
    }

    /// Stop the meeting manager and leave every meeting (5s budget).
    pub async fn stop_meeting(self: &Arc<Self>) {
        let runtime = lock(&self.meeting).take();
        let Some(runtime) = runtime else {
            return;
        };
        runtime.handle.stop();
        runtime.manager.interrupt_all();
        if tokio::time::timeout(Duration::from_secs(5), runtime.manager.leave_all())
            .await
            .is_err()
        {
            tracing::warn!("timed out leaving meetings on shutdown");
        }
    }

    /// The live meeting manager, when the capability is enabled and started.
    pub fn meeting_manager(&self) -> Option<Arc<MeetingManager>> {
        lock(&self.meeting)
            .as_ref()
            .map(|runtime| Arc::clone(&runtime.manager))
    }

    /// Route one `vc.bot.*` long-connection frame to the meeting manager.
    ///
    /// Called for `ChannelEvent::Unknown` payloads by the CLI event loop;
    /// returns `true` when the frame belonged to the in-meeting agent.
    pub fn handle_meeting_event(self: &Arc<Self>, raw: &Value) -> bool {
        let Some(manager) = self.meeting_manager() else {
            return false;
        };
        manager.handle_push(raw)
    }

    /// `/meeting …`: config check, admin gate, then the manager's command.
    async fn handle_meeting_command(
        self: &Arc<Self>,
        message: &NormalizedMessage,
        args: &str,
    ) -> String {
        let profile = self.live_profile();
        let config = meeting::config_from_profile(&profile);
        if !config.enabled {
            return meeting::MEETING_DISABLED_REPLY.to_string();
        }
        let sender_id = message_sender_id(message);
        if !self.can_run_admin_command(sender_id).await {
            tracing::info!(command = "/meeting", "admin-deny");
            return meeting::MEETING_DENIED_REPLY.to_string();
        }
        if !self.start_meeting() {
            return meeting::MEETING_NOT_READY_REPLY.to_string();
        }
        let Some(manager) = self.meeting_manager() else {
            return meeting::MEETING_NOT_READY_REPLY.to_string();
        };
        let scope = scope_key(message);
        manager
            .handle_command_with_reply(
                args,
                sender_id,
                &message.chat_id,
                meeting::ImReplyTarget {
                    scope: Some(&scope),
                    parent_message_id: Some(&message.message_id),
                },
            )
            .await
    }

    pub fn sessions_path(&self) -> &std::path::Path {
        self.paths.sessions_file.as_path()
    }

    /// Shared global run limiter. Comment and meeting runs use this to take a
    /// slot with [`RunLimiter::acquire_within`].
    pub(crate) fn run_limiter(&self) -> &Arc<RunLimiter> {
        &self.run_limit
    }

    /// Current profile snapshot: the startup profile, replaced by
    /// [`Bridge::save_preference`] after `/config set` writes `config.json`.
    fn live_profile(&self) -> ProfileConfig {
        lock(&self.live_profile).clone()
    }

    /// Resolve the runtime owner id for the profile's app, with the 30-minute
    /// cache from [`crate::owner`].
    ///
    /// Returns `None` when the profile has no app id or the lookup fails; the
    /// caller must treat that as "no owner exemption" (fail closed).
    async fn runtime_owner_id(&self, profile: &ProfileConfig) -> Option<String> {
        let app_id = profile
            .app()
            .map(|app| app.id.trim())
            .filter(|id| !id.is_empty());
        let Some(app_id) = app_id else {
            tracing::warn!(
                "admin command denied: profile has no app id for runtime-owner resolution"
            );
            return None;
        };
        self.owner.owner_id(app_id).await
    }

    /// Shared admin gate for `/config set` and `/meeting`.
    ///
    /// Order matters: the configured lists are checked first (no network), and
    /// the runtime owner is only consulted when they deny. An unknown/failed
    /// owner resolution leaves non-admins denied — the fail-closed behaviour
    /// the old static gate had.
    async fn can_run_admin_command(&self, sender_id: &str) -> bool {
        let profile = self.live_profile();
        if owner::can_run_admin_command(&profile, None, sender_id) {
            return true;
        }
        let owner_id = self.runtime_owner_id(&profile).await;
        if owner::can_run_admin_command(&profile, owner_id.as_deref(), sender_id) {
            tracing::info!(
                sender = sender_id,
                owner = owner_id.as_deref().unwrap_or("-"),
                "admin command allowed for runtime owner"
            );
            return true;
        }
        tracing::info!(sender = sender_id, "admin-deny");
        false
    }

    /// Persist one `/config set` preference to the root config file and reload
    /// the in-memory profile from it.
    ///
    /// The write goes through the shared config lock + atomic replace, so a
    /// concurrently running TypeScript bridge (or another `larkline` process)
    /// cannot interleave with it.
    fn save_preference(&self, edit: PreferenceEdit) -> Result<(), String> {
        let config_file = self.paths.config_file.clone();
        let profile_name = self.paths.profile.clone();
        larkline_core::store::with_config_lock(&config_file, |root| {
            let profile = root
                .profiles
                .get_mut(&profile_name)
                .ok_or_else(|| CoreError::Message(format!("unknown profile: {profile_name}")))?;
            let preferences = profile.preferences.get_or_insert_with(Preferences::default);
            match edit {
                PreferenceEdit::MessageReply(mode) => {
                    preferences.message_reply = Some(mode);
                    // Without this flag `ProfileConfig::message_reply` keeps
                    // coercing `text` back to `markdown` (pre-0.1.27 compat).
                    preferences.message_reply_migrated = Some(true);
                }
                PreferenceEdit::ShowToolCalls(show) => preferences.show_tool_calls = Some(show),
                PreferenceEdit::MaxConcurrentRuns(cap) => {
                    preferences.max_concurrent_runs = Some(cap);
                }
            }
            Ok(())
        })
        .map_err(|err| err.to_string())?;
        self.reload_profile()
    }

    /// Re-read this bridge's profile from the root config into the live
    /// snapshot. Leaves the old snapshot untouched on any failure.
    ///
    /// The live profile and the global run cap move together, so a
    /// `/config set maxConcurrentRuns` update is visible to the next
    /// queue-driver pass; the driver is woken when the cap changes.
    fn reload_profile(&self) -> Result<(), String> {
        let root = larkline_core::store::load_root(&self.paths.config_file)
            .map_err(|err| err.to_string())?;
        let profile = root
            .as_ref()
            .and_then(|root| root.profile(&self.paths.profile))
            .cloned()
            .ok_or_else(|| {
                format!(
                    "{} 中没有 profile {}",
                    self.paths.config_file.display(),
                    self.paths.profile
                )
            })?;
        let cap = profile.max_concurrent_runs();
        *lock(&self.live_profile) = profile;
        if self.run_limit.set_cap(cap) {
            tracing::info!(cap, "global agent run concurrency cap updated");
            // Wake the driver: a raised cap may make an already-due batch
            // startable, and a lowered cap must re-check before the next run.
            self.pending_wakeup.notify_one();
        }
        Ok(())
    }

    pub async fn handle_message(self: &Arc<Self>, message: NormalizedMessage) {
        if !policy::allowed(&self.live_profile(), &message, self.bot_open_id.as_deref()) {
            return;
        }
        let text = message.text.trim().to_string();
        if text.is_empty() {
            return;
        }
        // Resolve once for command side effects; `scope_key` is pure, so the
        // command handlers can resolve the same scope from the same message.
        let scope = scope_key(&message);

        // `/meeting ask|notes` can run the agent for up to `RUN_TIMEOUT`. The
        // CLI awaits this handler before it can answer a WebSocket heartbeat,
        // so meeting commands are detached and send their own reply — the same
        // reason file-comment runs are spawned by the event loop.
        if is_meeting_command(&text) {
            self.discard_pending(&scope);
            let bridge = Arc::clone(self);
            let meeting_scope = scope.clone();
            tokio::spawn(async move {
                if let Some(reply) = bridge.handle_command(&message, &text).await {
                    if let Err(err) = reply::send_text(
                        &bridge.channel,
                        &meeting_scope,
                        &message.chat_id,
                        Some(&message.message_id),
                        reply,
                    )
                    .await
                    {
                        tracing::warn!(error = %err, "cannot send /meeting reply");
                    }
                }
            });
            return;
        }

        if let Some(reply) = self.handle_command(&message, &text).await {
            // Commands bypass the debounce queue and drop anything that was
            // waiting for this scope (TS: `pending.cancel(scope)`).
            self.discard_pending(&scope);
            // Topic scopes reply to the triggering message with
            // `reply_in_thread`; everything else stays a top-level send.
            if let Err(err) = reply::send_text(
                &self.channel,
                &scope,
                &message.chat_id,
                Some(&message.message_id),
                reply,
            )
            .await
            {
                tracing::warn!(error = %err, "cannot send command reply");
            }
            return;
        }

        self.enqueue(message);
    }

    /// Handle a cloud-doc file-comment event the SDK could not type.
    ///
    /// The event arrives as [`lark_channel::event::ChannelEvent::Unknown`] and
    /// keeps its full platform payload; parsing, gating, the agent run and the
    /// reply into the comment thread live in [`crate::comments`].
    ///
    /// Callers spawn this instead of awaiting it: an agent run may take up to
    /// `RUN_TIMEOUT`, and the event loop must stay free to acknowledge
    /// frames and answer heartbeats.
    pub async fn handle_comment_event(self: &Arc<Self>, raw: &Value) {
        crate::comments::handle_comment_mention(self, raw).await;
    }

    /// Queue a message for its scope and make sure the debounce driver runs.
    fn enqueue(self: &Arc<Self>, message: NormalizedMessage) {
        // Topic groups key their queue by `chatId:threadId`, so each thread
        // debounces and runs independently; everything else stays on chat id.
        let scope = scope_key(&message);
        let outcome = {
            let mut pending = lock(&self.pending);
            pending.push(&scope, message, Instant::now())
        };
        if outcome.dropped > 0 {
            tracing::warn!(
                scope = %scope,
                queued = outcome.queued,
                dropped = outcome.dropped,
                "pending queue full; dropped the oldest queued message"
            );
        }
        tracing::debug!(scope = %scope, queued = outcome.queued, "message queued");
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

    /// Wake for the earliest quiet window, then start every due batch that can
    /// get a global slot.
    ///
    /// When the cap is reached the driver parks on `pending_wakeup` instead of
    /// re-polling a deadline that is already in the past. The wake can come
    /// from a [`RunGuard`] releasing its slot, from `/config` changing the cap,
    /// or from a newly queued message (which is then re-checked harmlessly).
    async fn pending_driver(self: Arc<Self>) {
        loop {
            loop {
                match self.start_due_batch(Instant::now()) {
                    BatchStart::Started => continue,
                    BatchStart::NoDueBatch => break,
                    BatchStart::AtCapacity => {
                        // A slot release has its own signal in addition to the
                        // pending wakeup, so a run future aborted before its
                        // RunGuard exists still wakes us.
                        tokio::select! {
                            _ = self.pending_wakeup.notified() => {}
                            _ = self.run_limit.slot_released() => {}
                        }
                        break;
                    }
                }
            }

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
        }
    }

    /// Claim the next due batch *and* a global run slot, without spawning.
    ///
    /// The caller must hold the pending lock: `take_due` marks the scope
    /// blocked, and the claim has to stay atomic with `/stop` and with the
    /// registration in `active` that `start_due_batch` performs.
    ///
    /// A due batch with no free slot is **not** removed from the queue, so its
    /// messages are never dropped and the scope stays unblocked until a real
    /// run takes it.
    fn claim_due_batch_locked(
        &self,
        pending: &mut PendingQueue<NormalizedMessage>,
        now: Instant,
    ) -> BatchClaim {
        let due = pending
            .next_deadline()
            .is_some_and(|deadline| deadline <= now);
        if !due {
            return BatchClaim::NoDueBatch;
        }

        let Some(permit) = self.run_limit.try_acquire() else {
            tracing::debug!(
                active = self.run_limit.active(),
                cap = self.run_limit.cap(),
                "global run cap reached; leaving due batch queued"
            );
            return BatchClaim::AtCapacity;
        };

        let Some((scope, batch)) = pending.take_due(now) else {
            // The deadline disappeared between the check and take_due (only
            // possible if another caller violates the pending-lock contract).
            drop(permit);
            return BatchClaim::NoDueBatch;
        };

        BatchClaim::Claimed(ClaimedBatch {
            scope,
            batch,
            permit,
        })
    }

    /// Take the next due batch and spawn its run, atomically with respect to
    /// `/stop`.
    ///
    /// The pending lock is held until the run is registered in `active`, so a
    /// concurrent stop either sees no run at all (and the batch stays queued)
    /// or sees the registered run and aborts it. The global slot is moved into
    /// [`RunGuard`], which releases it on normal completion *and* on abort.
    fn start_due_batch(self: &Arc<Self>, now: Instant) -> BatchStart {
        let mut pending = lock(&self.pending);
        let ClaimedBatch {
            scope,
            batch,
            permit,
        } = match self.claim_due_batch_locked(&mut pending, now) {
            BatchClaim::Claimed(claimed) => claimed,
            BatchClaim::NoDueBatch => return BatchStart::NoDueBatch,
            BatchClaim::AtCapacity => return BatchStart::AtCapacity,
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
                permit: Some(permit),
            };
            if let Err(err) = bridge.run_batch(batch).await {
                tracing::error!(error = %err, "agent run failed");
            }
        });

        let mut active = lock(&self.active);
        active.retain(|_, run| !run.abort.is_finished());
        active.insert(
            scope,
            ActiveRun {
                id,
                abort: handle.abort_handle(),
            },
        );
        drop(active);
        drop(pending);
        let _ = start.send(());
        BatchStart::Started
    }

    /// Spawn and register a run that has no pending queue (comment / meeting).
    ///
    /// The task is gated on a `ready` signal until its entry is visible in
    /// [`Bridge::active`], so `/stop` either sees the run and aborts it or sees
    /// nothing at all. The returned guard inside the task removes the entry on
    /// normal completion *and* on abort; the run future owns its global
    /// [`RunPermit`](crate::run_limit::RunPermit), so aborting also releases
    /// the slot and drops the agent child (`kill_on_drop`).
    ///
    /// Returns the task handle so the meeting manager can keep its own
    /// per-meeting abort map in sync (it aborts the same task).
    pub(crate) fn spawn_tracked<F>(self: &Arc<Self>, scope: &str, future: F) -> JoinHandle<()>
    where
        F: std::future::Future<Output = ()> + Send + 'static,
    {
        let id = self.next_run_id.fetch_add(1, Ordering::Relaxed);
        let (start, ready) = tokio::sync::oneshot::channel();
        let bridge = Arc::clone(self);
        let task_scope = scope.to_string();
        let handle = tokio::spawn(async move {
            let _ = ready.await;
            let _guard = TrackedRunGuard {
                bridge: Arc::clone(&bridge),
                scope: task_scope,
                id,
            };
            future.await;
        });

        let mut active = lock(&self.active);
        active.retain(|_, run| !run.abort.is_finished());
        if let Some(previous) = active.insert(
            scope.to_string(),
            ActiveRun {
                id,
                abort: handle.abort_handle(),
            },
        ) {
            // Callers serialize per scope (the comment guard / meeting map),
            // but never leave an old task running unregistered if they race.
            tracing::warn!(scope, "replacing an already active run for this scope");
            previous.abort.abort();
        }
        drop(active);
        let _ = start.send(());
        handle
    }

    /// Run finished (or was aborted): release its scope for the next batch.
    ///
    /// Only the run that still owns the scope may unblock it, so a guard from
    /// a delayed abort can never clear its successor's block. Every guard
    /// still wakes the driver, because it may just have released the global
    /// run slot (the permit is dropped before this call) even when a newer run
    /// already owns the scope.
    pub(crate) fn finish_run(&self, scope: &str, id: u64) {
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
        if owns {
            pending.unblock(scope, Instant::now());
        }
        drop(pending);
        self.pending_wakeup.notify_one();
    }

    /// Abort a live run. The caller must hold the pending lock (lock order is
    /// pending -> active).
    fn abort_run(&self, scope: &str) -> bool {
        let mut active = lock(&self.active);
        match active.remove(scope) {
            Some(run) => {
                run.abort.abort();
                true
            }
            None => false,
        }
    }

    /// `/stop` and the card stop button: abort the run, drop its queued
    /// follow-up batch, and clear the block so the chat can be used again.
    ///
    /// Works for every run kind registered in [`Bridge::active`]: an IM batch
    /// under its chat/topic scope, a comment run under `comment:<hash>`, or a
    /// meeting run under `meeting:<id>`. The pending-queue cleanup is a no-op
    /// for scopes that have no queue.
    pub(crate) fn stop_scope(&self, scope: &str) -> (bool, usize) {
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

    /// Whether a run is currently registered under `scope`.
    ///
    /// Used by `/status` (through the collected scope list) and by tests that
    /// assert an abort removed the entry. Finished tasks are treated as gone
    /// even if their guard has not run yet.
    pub(crate) fn is_scope_active(&self, scope: &str) -> bool {
        lock(&self.active)
            .get(scope)
            .is_some_and(|run| !run.abort.is_finished())
    }

    /// Fallback abort for `/stop meeting:<id>` when the mirror entry is gone.
    ///
    /// The meeting manager's own map is the authority for its runs; normally
    /// [`Bridge::stop_scope`] already aborted the mirrored task, but calling
    /// the manager keeps `/meeting stop` and `/stop meeting:<id>` consistent
    /// even if the two maps ever diverge.
    fn interrupt_meeting_scope(&self, scope: &str) -> bool {
        let Some(meeting_id) = scope.strip_prefix("meeting:") else {
            return false;
        };
        self.meeting_manager()
            .is_some_and(|manager| manager.interrupt(meeting_id))
    }

    /// Drop the scope's queued batch (any handled command does this).
    fn discard_pending(&self, scope: &str) -> usize {
        let dropped = {
            let mut pending = lock(&self.pending);
            pending.cancel(scope).len()
        };
        self.pending_wakeup.notify_one();
        dropped
    }

    async fn handle_command(
        self: &Arc<Self>,
        message: &NormalizedMessage,
        text: &str,
    ) -> Option<String> {
        let command = text.split_whitespace().next().unwrap_or_default();
        let args = text.get(command.len()..).unwrap_or_default().trim();
        // Commands must target exactly the scope the message belongs to: in a
        // topic group `/stop` aborts that topic's run and `/new` clears that
        // topic's session, never a sibling topic's.
        let scope = scope_key(message);
        match command {
            "/stop" => {
                let target = args.trim();
                if target.is_empty() {
                    let (stopped, _dropped) = self.stop_scope(&scope);
                    return Some(if stopped {
                        "已停止当前任务。".to_string()
                    } else {
                        "当前没有正在运行的任务。".to_string()
                    });
                }
                // Explicit scopes are admin-only, matching the TypeScript
                // `handleStop` gate. They let a user abort a run that has no
                // chat scope: a cloud-doc comment run (`comment:<hash>`) or a
                // meeting run (`meeting:<id>`).
                if !self.can_run_admin_command(message_sender_id(message)).await {
                    tracing::info!(command = "/stop", target, "admin-deny");
                    return Some(STOP_SCOPE_DENIED_REPLY.to_string());
                }
                let (stopped, _dropped) = self.stop_scope(target);
                // The meeting manager keeps its own handle for `/meeting stop`
                // and the in-meeting "stop" trigger. It is mirrored in
                // `active`, but fall back to it if the mirror is ever absent
                // (e.g. a manager started before this bridge).
                let stopped = stopped || self.interrupt_meeting_scope(target);
                Some(if stopped {
                    format!("已请求停止 `{target}`。")
                } else {
                    format!("未找到正在运行的任务：`{target}`。")
                })
            }
            "/new" | "/reset" => match self.sessions.lock().await.clear(&scope) {
                Ok(()) => Some("已开启新会话。".to_string()),
                Err(err) => Some(format!("重置失败：{err}")),
            },
            "/cd" => Some(self.handle_cd(&scope, args).await),
            "/pwd" => Some(self.handle_pwd(&scope).await),
            "/status" => Some(self.handle_status(&scope).await),
            "/meeting" => Some(self.handle_meeting_command(message, args).await),
            "/config" => Some(self.handle_config(message, args).await),
            "/help" => Some(
                "可用命令：\n\
                 /new 或 /reset 开启新会话\n\
                 /cd <绝对路径> 切换工作目录\n\
                 /pwd 显示当前工作目录\n\
                 /status 显示当前状态\n\
                 /config 查看/修改回复偏好（修改需管理员）\n\
                 /stop 中断当前任务\n\
                 /stop <scope> 中断指定运行（如 comment:… / meeting:…，需管理员）\n\
                 /meeting 会议智能体（入会/离会/纪要，需管理员）\n\
                 /help 显示帮助"
                    .to_string(),
            ),
            _ => None,
        }
    }

    /// `/cd <absolute path>`: canonicalize and persist a per-scope cwd.
    async fn handle_cd(&self, scope: &str, args: &str) -> String {
        let path = match validate_cd_target(args) {
            Ok(path) => path,
            Err(reply) => return reply,
        };
        let mut workspaces = self.workspaces.lock().await;
        match workspaces.set(scope, &path) {
            Ok(()) => format!("✓ 已切换 cwd 到 {}", path.display()),
            Err(err) => format!("保存工作目录失败：{err}"),
        }
    }

    /// `/pwd`: the effective cwd for this scope.
    async fn handle_pwd(&self, scope: &str) -> String {
        let workspaces = self.workspaces.lock().await;
        let cwd = effective_cwd(&workspaces, scope, &self.paths.default_workspace_dir);
        format!("当前 cwd：{}", cwd.display())
    }

    /// `/status`: profile/agent/app/cwd/reply-mode/resumable-session summary.
    async fn handle_status(&self, scope: &str) -> String {
        let cwd = {
            let workspaces = self.workspaces.lock().await;
            effective_cwd(&workspaces, scope, &self.paths.default_workspace_dir)
        };
        let cwd_str = cwd.to_string_lossy().into_owned();
        let resume = {
            let sessions = self.sessions.lock().await;
            sessions.resume_for(scope, &cwd_str).map(str::to_string)
        };

        let profile = self.live_profile();
        let app = profile.app();
        let app_id = app.map(|app| app.id.as_str()).unwrap_or("(未配置)");
        let tenant = app
            .map(|app| tenant_label(app.tenant))
            .unwrap_or("(未配置)");
        let session = match resume {
            Some(id) => format!("可恢复（{}…）", id.chars().take(8).collect::<String>()),
            None => "不可恢复（当前 cwd 下没有会话）".to_string(),
        };
        // Expose every live run scope, including the comment / meeting scopes
        // that `/stop <scope>` can target. Without this, those ids would be
        // impossible to discover from IM.
        let mut active_scopes: Vec<String> = {
            let active = lock(&self.active);
            active
                .iter()
                .filter(|(_, run)| !run.abort.is_finished())
                .map(|(scope, _)| scope.clone())
                .collect()
        };
        active_scopes.sort();
        let active_run = self.is_scope_active(scope);
        let active_scopes = if active_scopes.is_empty() {
            "(无)".to_string()
        } else {
            active_scopes.join(", ")
        };

        format!(
            "📊 当前状态\n\
             profile: {}\n\
             agent: {}\n\
             app id: {app_id}\n\
             tenant: {tenant}\n\
             cwd: {}\n\
             reply mode: {}\n\
             session: {session}\n\
             active run: {}\n\
             active scopes: {active_scopes}",
            self.paths.profile,
            profile.agent_kind.display_name(),
            cwd.display(),
            reply_mode_label(profile.message_reply()),
            if active_run { "yes" } else { "no" },
        )
    }

    /// `/config` and `/config set <key> <value>`.
    ///
    /// The TypeScript command opens a CardKit form with every preference; the
    /// Rust bridge implements the text subset the run flow consumes. Reads are
    /// open to any user the message policy already admitted. Writes reuse the
    /// `/meeting` admin gate: `access.admins`, the legacy `access.allowed_users`
    /// fallback while the admin list is empty, or the runtime app owner
    /// resolved by [`crate::owner`]. A denied gate never writes; an allowed one
    /// persists to the root config and reloads the live profile (including the
    /// run limiter when `maxConcurrentRuns` changed).
    async fn handle_config(&self, message: &NormalizedMessage, args: &str) -> String {
        let mut words = args.split_whitespace();
        match words.next() {
            None => self.render_config(),
            Some("set") => {
                let key = words.next().unwrap_or_default();
                let value = words.next().unwrap_or_default();
                if words.next().is_some() {
                    return CONFIG_USAGE.to_string();
                }
                let edit = match parse_config_set(key, value) {
                    Ok(edit) => edit,
                    Err(reply) => return reply,
                };
                let sender_id = message_sender_id(message);
                if !self.can_run_admin_command(sender_id).await {
                    tracing::info!(command = "/config set", key, "admin-deny");
                    return CONFIG_DENIED_REPLY.to_string();
                }
                match self.save_preference(edit) {
                    Ok(()) => format!("✓ 已保存 {key} = {value}（已立即生效）"),
                    Err(err) => format!("保存配置失败：{err}"),
                }
            }
            Some(_) => CONFIG_USAGE.to_string(),
        }
    }

    /// `/config` reply body.
    fn render_config(&self) -> String {
        let profile = self.live_profile();
        format!(
            "⚙️ 当前配置（profile: {}）\n\
             messageReply: {}\n\
             showToolCalls: {}\n\
             maxConcurrentRuns: {}\n\n\
             {CONFIG_USAGE}",
            self.paths.profile,
            reply_mode_label(profile.message_reply()),
            profile.show_tool_calls(),
            profile.max_concurrent_runs(),
        )
    }

    /// Handle a click on a card button.
    ///
    /// Two contracts are served:
    ///
    /// * **bridge-rendered buttons** (`action` + `scope` + process-local
    ///   `token`), notably the stop button: the click aborts the run and drops
    ///   the batch queued while it was live;
    /// * **agent-rendered callbacks** (`__bridge_cb: true` + a v1
    ///   `bridge_token`): after verification the markers are stripped and the
    ///   rest of the value is queued as a `[card-click] {…}` message in the
    ///   same chat scope, so the agent resumes its session with the click as a
    ///   follow-up user message.
    ///
    /// Returns a short user-facing status when the bridge handled the click
    /// itself; `None` for forwarded callbacks (the agent replies) and for
    /// foreign clicks.
    pub async fn handle_card_action(self: &Arc<Self>, action: &CardActionEvent) -> Option<String> {
        let value = action.action.value.as_object()?;
        if value.get(BRIDGE_CALLBACK_MARKER).and_then(Value::as_bool) != Some(true) {
            return None;
        }

        // A `bridge_token` selects the agent-rendered contract. Checked before
        // the native fields because an agent payload may legitimately carry
        // state fields named `action`/`scope`; those must not be mistaken for
        // a bridge-rendered button.
        if value.get(BRIDGE_TOKEN_FIELD).is_some() {
            return self.handle_agent_callback(action, value).await;
        }

        // Bridge-rendered button: signed with the process-local CallbackTokens
        // key. These values always carry `action` + `scope`.
        if let (Some(action_name), Some(scope)) = (
            value.get("action").and_then(Value::as_str),
            value.get("scope").and_then(Value::as_str),
        ) {
            // The token binds the scope, but a card can be forwarded to
            // another chat: refuse a click whose carrier message lives
            // elsewhere (the event carries the real chat id when available).
            // A topic run's stop button carries `${chatId}:${threadId}`, so
            // accept that shape for the click's chat while still rejecting a
            // different chat.
            if let Some(clicked_chat) = action
                .card_context
                .as_ref()
                .and_then(|context| context.open_chat_id.as_deref())
                .filter(|chat| !chat.is_empty())
            {
                if !scope_belongs_to_chat(scope, clicked_chat) {
                    tracing::warn!(
                        scope,
                        clicked_chat,
                        "rejected card callback clicked from another chat"
                    );
                    return Some("操作已失效，请重新发起。".to_string());
                }
            }
            let token = value
                .get("token")
                .and_then(Value::as_str)
                .unwrap_or_default();
            if !self.tokens.verify(scope, action_name, token) {
                tracing::warn!(scope, action_name, "rejected card callback with bad token");
                return Some("操作已失效，请重新发起。".to_string());
            }
            return match action_name {
                "stop" => {
                    // The stop button is the card equivalent of `/stop`: abort
                    // the run and drop the batch queued while it was live.
                    let (stopped, _dropped) = self.stop_scope(scope);
                    Some(if stopped {
                        "已停止当前任务。".to_string()
                    } else {
                        "当前没有正在运行的任务。".to_string()
                    })
                }
                _ => None,
            };
        }

        None
    }

    /// Verify and forward an agent-rendered card callback.
    ///
    /// Fails closed: a missing callback key, missing chat/operator context, a
    /// malformed/expired/replayed token, or a denied operator all drop the
    /// click with a warning and never reach the agent.
    async fn handle_agent_callback(
        self: &Arc<Self>,
        action: &CardActionEvent,
        value: &serde_json::Map<String, Value>,
    ) -> Option<String> {
        let token = value
            .get(BRIDGE_TOKEN_FIELD)
            .and_then(Value::as_str)
            .unwrap_or_default();
        if token.is_empty() {
            tracing::warn!("ignoring agent card callback without a usable bridge_token");
            return None;
        }
        let Some(auth) = self.callback_auth.as_ref() else {
            tracing::warn!("rejected agent card callback: no callback key configured");
            return None;
        };
        // The card action event carries the chat and message it was rendered
        // on through `card_context`; without them the token cannot be bound to
        // a scope and the click is dropped.
        let chat_id = action
            .card_context
            .as_ref()
            .and_then(|context| context.open_chat_id.as_deref())
            .unwrap_or_default();
        let operator = action.operator.open_id.as_deref().unwrap_or_default();
        if chat_id.is_empty() || operator.is_empty() {
            tracing::warn!("rejected agent card callback without chat/operator context");
            return None;
        }

        let expected = CallbackExpected {
            // The Rust bridge keeps no TypeScript run ids or policy
            // fingerprints, so it cannot bind those; see the callback docs.
            run_id: None,
            scope: chat_id.to_string(),
            chat_id: chat_id.to_string(),
            operator_open_id: operator.to_string(),
            action: AGENT_CALLBACK_ACTION.to_string(),
            policy_fingerprint: None,
        };
        match auth.verify(token, &expected) {
            Ok(payload) => tracing::info!(
                scope = %payload.s,
                operator = %operator,
                run_id = %payload.r,
                "verified agent card callback"
            ),
            Err(reason) => {
                tracing::warn!(
                    scope = chat_id,
                    operator,
                    reason = reason.as_str(),
                    "rejected agent card callback"
                );
                return None;
            }
        }

        if !card_click_allowed(&self.live_profile(), chat_id, operator) {
            tracing::info!(
                scope = chat_id,
                operator,
                "card-click denied by access policy"
            );
            return None;
        }

        // Strip the bridge markers so the agent only sees the fields it set
        // (plus CardKit form values, mirroring the TypeScript forwarder).
        let payload = strip_callback_markers(value, action.action.form_value.as_ref());
        let text = format!("[card-click] {payload}");
        tracing::info!(
            scope = chat_id,
            chars = text.chars().count(),
            "forwarding card click to the agent"
        );
        self.enqueue(card_click_message(action, chat_id, operator, text));
        None
    }

    /// Effective cwd for a run, creating the directory when needed.
    ///
    /// A `/cd` target that cannot be created (deleted, permissions, replaced
    /// by a file) is reported and the profile default is used instead, so the
    /// run still has a chance to start. Comment runs pass their document
    /// scope as the key, so the same rules apply per document; IM runs pass
    /// the session scope (chat id or `chatId:threadId`).
    pub(crate) async fn workspace_cwd(&self, scope: &str) -> PathBuf {
        let (cwd, default) = {
            let workspaces = self.workspaces.lock().await;
            (
                effective_cwd(&workspaces, scope, &self.paths.default_workspace_dir),
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

    /// Resumable session id for a run scope, if it was recorded in `cwd`.
    async fn resume_scope_session(&self, scope: &str, cwd: &str) -> Option<String> {
        self.sessions
            .lock()
            .await
            .resume_for(scope, cwd)
            .map(str::to_string)
    }

    /// Persist the agent's session id under the run scope.
    async fn save_scope_session(&self, scope: &str, session_key: &str, cwd: &str) {
        if let Err(err) = self.sessions.lock().await.set(scope, session_key, cwd) {
            tracing::warn!(error = %err, "cannot persist session");
        }
    }

    async fn run_batch(self: Arc<Self>, batch: Vec<NormalizedMessage>) -> anyhow::Result<()> {
        let Some(first) = batch.first() else {
            return Ok(());
        };
        let chat_id = first.chat_id.clone();
        // Topic groups run under `chatId:threadId`; p2p/regular groups keep
        // the plain chat id, preserving existing sessions.json keys.
        let scope = scope_key(first);
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
            WorkingReaction::start(self.channel.rest().clone(), last_message_id.clone());
        // Snapshot the effective cwd for the whole run: session resume/record
        // and the agent process must agree even if `/cd` lands mid-run. Topic
        // scopes get their own cwd entry in the workspace store.
        let cwd = self.workspace_cwd(&scope).await;
        let cwd_str = cwd.to_string_lossy().into_owned();
        let resume = self.resume_scope_session(&scope, &cwd_str).await;

        let context = self.prompt_context(&batch);
        let prompt = build_agent_prompt(&context, &batch_user_input(&batch));
        let profile = self.live_profile();
        let reply_mode = profile.message_reply();
        let use_card = reply_mode != MessageReplyMode::Text;

        let mut view = RunView::default();
        let mut streaming: Option<StreamingCard> = None;
        let mut message_id: Option<String> = None;
        if use_card {
            let card = build_card(&profile, &view, "启动中", started, None);
            // Prefer a CardKit 2.0 streaming entity: every later update is one
            // whole-card PUT with its own sequence. If either the entity or the
            // reference message cannot be created, the plain message path below
            // is the fallback (and is the only path when streaming is off).
            match reply::start_streaming_card(
                &self.channel,
                &scope,
                &chat_id,
                Some(&last_message_id),
                card.clone(),
                STREAMING_ELEMENT_ID,
            )
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
                    match reply::send_card(
                        &self.channel,
                        &scope,
                        &chat_id,
                        Some(&last_message_id),
                        &card,
                    )
                    .await
                    {
                        Ok(id) => message_id = Some(id.0),
                        Err(err) => {
                            tracing::warn!(error = %err, "cannot send run card; falling back to text");
                        }
                    }
                }
            }
        }

        let spec = AgentSpec {
            kind: profile.agent_kind,
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
                    &scope,
                    &chat_id,
                    &mut streaming,
                    message_id.as_deref(),
                    Some(&last_message_id),
                    &view,
                    reply_mode,
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
                            &scope,
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
            self.save_scope_session(&scope, &key, &cwd_str).await;
        }

        self.finalize(
            &scope,
            &chat_id,
            &mut streaming,
            message_id.as_deref(),
            Some(&last_message_id),
            &view,
            reply_mode,
            started,
        )
        .await;
        Ok(())
    }

    // `scope` (run/card-stop scope) and `chat_id` (reply target) differ for
    // topic runs, so both stay explicit rather than being re-derived.
    #[allow(clippy::too_many_arguments)]
    async fn finalize(
        &self,
        scope: &str,
        chat_id: &str,
        streaming: &mut Option<StreamingCard>,
        message_id: Option<&str>,
        source_message_id: Option<&str>,
        view: &RunView,
        reply_mode: MessageReplyMode,
        started: Instant,
    ) {
        let use_card = reply_mode != MessageReplyMode::Text;
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
                    self.update_card(scope, Some(message_id), view, started)
                        .await;
                    return;
                }
            }
        }
        let body = if reply_mode == MessageReplyMode::Text {
            // Text replies get the full TS renderer: final answer selection,
            // reasoning/tool summaries according to the profile, terminal
            // notices, email masking and the IM length guard.
            text_final_body(&self.live_profile(), view)
        } else {
            // Markdown still uses the card path above; this is only the
            // message fallback when the card can no longer be updated. Mask
            // emails for the same tenant-audit reason as the card renderer.
            let answer = mask_emails(view.best_answer().trim());
            if answer.is_empty() {
                "（没有输出）".to_string()
            } else {
                truncate_chars(&answer, 4_000)
            }
        };
        if let Err(err) =
            reply::send_text(&self.channel, scope, chat_id, source_message_id, body).await
        {
            tracing::warn!(error = %err, chat = %chat_id, "cannot send final answer");
        }
    }

    /// Push the current view to the run card.
    ///
    /// While the CardKit entity is live the whole card is PUT back through
    /// [`StreamingCard::set_card`]. Once it is expired, finished, or a write
    /// failed, the same message is updated through the message API for the
    /// rest of the run (and only inside the safe window).
    async fn refresh_card(
        &self,
        scope: &str,
        streaming: &mut Option<StreamingCard>,
        message_id: Option<&str>,
        view: &RunView,
        started: Instant,
    ) {
        if let Some(handle) = streaming.as_mut() {
            if handle.is_live() {
                let card = larkline_lark::enable_streaming(self.live_card(scope, view, started))
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
            self.update_card(scope, message_id, view, started).await;
        }
    }

    /// Card for a running frame (streaming mode stays on, signed stop button).
    ///
    /// The stop token is bound to the run scope, not just the chat, so the
    /// button in a topic run aborts that topic's run and leaves sibling topics
    /// alone.
    fn live_card(&self, scope: &str, view: &RunView, started: Instant) -> lark_channel::Card {
        let token = if view.finished {
            None
        } else {
            Some(self.tokens.sign(scope, "stop"))
        };
        build_card(
            &self.live_profile(),
            view,
            card_status(view),
            started,
            token.as_deref().map(|token| (scope, token)),
        )
    }

    /// Card for the last frame (streaming mode off, no stop button).
    fn final_card(&self, view: &RunView, started: Instant) -> lark_channel::Card {
        build_card(&self.live_profile(), view, card_status(view), started, None)
    }

    async fn update_card(
        &self,
        scope: &str,
        message_id: Option<&str>,
        view: &RunView,
        started: Instant,
    ) {
        let Some(message_id) = message_id else {
            return;
        };
        let card = self.live_card(scope, view, started);
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

/// [`MeetingHost`] adapter that borrows the bridge weakly.
///
/// The manager lives inside the bridge, so a strong `Arc<Bridge>` here would
/// be a reference cycle (bridge -> manager -> bridge). The CLI stops the
/// manager explicitly; the weak handle also keeps the cycle from leaking when
/// a bridge is dropped first.
pub(crate) struct BridgeMeetingHost {
    pub(crate) bridge: std::sync::Weak<Bridge>,
}

#[async_trait]
impl MeetingHost for BridgeMeetingHost {
    async fn workspace_cwd(&self, scope: &str) -> PathBuf {
        match self.bridge.upgrade() {
            Some(bridge) => Bridge::workspace_cwd(&bridge, scope).await,
            None => std::env::temp_dir(),
        }
    }

    async fn resume_session(&self, scope: &str, cwd: &str) -> Option<String> {
        let bridge = self.bridge.upgrade()?;
        bridge
            .sessions
            .lock()
            .await
            .resume_for(scope, cwd)
            .map(str::to_string)
    }

    async fn save_session(&self, scope: &str, session_key: &str, cwd: &str) {
        let Some(bridge) = self.bridge.upgrade() else {
            return;
        };
        if let Err(err) = bridge.sessions.lock().await.set(scope, session_key, cwd) {
            tracing::warn!(error = %err, "cannot persist meeting session");
        }
    }

    fn spawn_tracked_run(
        &self,
        scope: &str,
        future: std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>>,
    ) -> JoinHandle<()> {
        match self.bridge.upgrade() {
            Some(bridge) => bridge.spawn_tracked(scope, future),
            None => tokio::spawn(future),
        }
    }

    async fn send_im_text_reply(
        &self,
        to: &str,
        target: crate::meeting::ImReplyTarget<'_>,
        text: &str,
    ) {
        let Some(bridge) = self.bridge.upgrade() else {
            return;
        };
        // The thread path only applies to chat ids; `ou_*` is a DM and has no
        // thread to reply into.
        if let (Some(scope), true) = (target.scope, to.starts_with("oc_")) {
            match reply::send_text(&bridge.channel, scope, to, target.parent_message_id, text).await
            {
                Ok(_) => tracing::info!(
                    to = %to,
                    chars = text.chars().count(),
                    "meeting reply sent over IM"
                ),
                Err(err) => tracing::warn!(
                    to = %to,
                    error = %err,
                    "cannot send meeting reply over IM"
                ),
            }
            return;
        }
        self.send_im_text(to, text).await;
    }

    async fn send_im_text(&self, to: &str, text: &str) {
        let Some(bridge) = self.bridge.upgrade() else {
            return;
        };
        // `ou_*` is an open id (a direct message), anything else is a chat id —
        // the TypeScript `channel.send` accepts both in one call.
        let recipient = if to.starts_with("ou_") {
            Recipient::User(to.to_string())
        } else {
            Recipient::Chat(to.to_string())
        };
        match bridge
            .channel
            .sender()
            .text_message(recipient, text.to_string())
            .send()
            .await
        {
            Ok(_) => tracing::info!(
                to = %to,
                chars = text.chars().count(),
                "meeting message sent over IM"
            ),
            Err(err) => tracing::warn!(
                to = %to,
                error = %err,
                "cannot send meeting message over IM"
            ),
        }
    }
}

/// True for `/meeting` and `/meeting <sub…>` — the command whose `ask`/`notes`
/// subcommands await an agent run.
fn is_meeting_command(text: &str) -> bool {
    text.split_whitespace().next() == Some("/meeting")
}

/// Join a batch's text into the `<user_input>` body.
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
pub(crate) enum ReactionState {
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
    pub(crate) fn begin_add(&mut self) {
        *self = Self::Adding;
    }

    /// Record the id returned by the add call.
    ///
    /// Returns `true` when the run already ended while the request was in
    /// flight, so the caller must delete the reaction itself.
    pub(crate) fn added(&mut self, reaction_id: &str) -> bool {
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

    pub(crate) fn add_failed(&mut self) {
        *self = Self::Done;
    }

    /// End the run and return the reaction id to delete now, if known.
    ///
    /// When the add is still in flight the state becomes [`Abandoned`] and
    /// the add task is responsible for the deletion once it lands.
    ///
    /// [`Abandoned`]: ReactionState::Abandoned
    pub(crate) fn finish(&mut self) -> Option<String> {
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
    // Feishu's tenant audit rejects cards containing a raw email address, so
    // mask at this boundary exactly like `renderCard`/`deepMaskEmails` in TS.
    // Mask before truncating: a cut can then never expose a raw address.
    let answer = mask_emails(view.best_answer().trim());
    let body = if answer.is_empty() {
        "…".to_string()
    } else {
        truncate_chars(&answer, MAX_CARD_CHARS)
    };

    let mut elements = vec![json!({
        "tag": "markdown",
        "content": body,
        // Stable id so CardKit can also stream just the answer element later;
        // the run flow currently uses whole-card updates.
        "element_id": STREAMING_ELEMENT_ID,
    })];
    // `/config set showToolCalls false` hides the tool feed from cards, like
    // the TypeScript card renderer.
    if profile.show_tool_calls() && !view.tools.is_empty() {
        let tools: Vec<String> = view
            .tools
            .iter()
            .rev()
            .take(6)
            .rev()
            .map(|tool| format!("• {}", mask_emails(tool)))
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

/// Map the profile's reply preferences onto text-renderer options.
///
/// Text replies are plain-text messages, so markdown markers added by the
/// renderer would show up literally. Reasoning follows the TS text renderer:
/// only `inline` is included; `auto` is card-only, `off` disables it, and
/// `bubble` is streamed through the COT client instead.
fn text_render_options(profile: &ProfileConfig) -> TextRenderOptions {
    let reasoning = matches!(
        profile
            .preferences
            .as_ref()
            .and_then(|prefs| prefs.reasoning_display),
        Some(ReasoningDisplay::Inline)
    );
    TextRenderOptions {
        format: TextFormat::Plain,
        show_tool_calls: profile.show_tool_calls(),
        reasoning,
        ..TextRenderOptions::default()
    }
}

/// Final body for `MessageReplyMode::Text`.
fn text_final_body(profile: &ProfileConfig, view: &RunView) -> String {
    let body = render_text(view, text_render_options(profile));
    if body.trim().is_empty() {
        "（没有输出）".to_string()
    } else {
        body
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

/// Effective cwd for a scope: explicit `/cd` override, else the profile
/// default, else `fallback` when the store was loaded without a default.
fn effective_cwd(store: &WorkspaceStore, scope: &str, fallback: &Path) -> PathBuf {
    store
        .workspace_for(scope)
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

/// One preference that `/config set` may persist.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PreferenceEdit {
    MessageReply(MessageReplyMode),
    ShowToolCalls(bool),
    MaxConcurrentRuns(u32),
}

/// Validate a `/config set <key> <value>` pair.
///
/// Only the writable subset is accepted; everything else returns the usage
/// text or a value-specific error so a typo never silently no-ops.
fn parse_config_set(key: &str, value: &str) -> Result<PreferenceEdit, String> {
    let value = value.trim();
    match key {
        "messageReply" => {
            let mode = match value.to_ascii_lowercase().as_str() {
                "card" => MessageReplyMode::Card,
                "markdown" => MessageReplyMode::Markdown,
                "text" => MessageReplyMode::Text,
                _ => {
                    return Err(format!(
                        "messageReply 只接受 card|markdown|text，收到 `{value}`。\n\n{CONFIG_USAGE}"
                    ));
                }
            };
            Ok(PreferenceEdit::MessageReply(mode))
        }
        "showToolCalls" => match value.to_ascii_lowercase().as_str() {
            "true" => Ok(PreferenceEdit::ShowToolCalls(true)),
            "false" => Ok(PreferenceEdit::ShowToolCalls(false)),
            _ => Err(format!(
                "showToolCalls 只接受 true|false，收到 `{value}`。\n\n{CONFIG_USAGE}"
            )),
        },
        "maxConcurrentRuns" => {
            // Match the TypeScript config card: numbers are floored and
            // clamped to 1..=50. A non-number is a typo, not a clamp target.
            let parsed = value.parse::<f64>().map_err(|_| {
                format!("maxConcurrentRuns 只接受 1-50 的整数，收到 `{value}`。\n\n{CONFIG_USAGE}")
            })?;
            if !parsed.is_finite() || parsed < 1.0 {
                return Err(format!(
                    "maxConcurrentRuns 只接受 1-50 的整数，收到 `{value}`。\n\n{CONFIG_USAGE}"
                ));
            }
            Ok(PreferenceEdit::MaxConcurrentRuns(
                (parsed.floor() as u64).clamp(1, 50) as u32,
            ))
        }
        "" => Err(CONFIG_USAGE.to_string()),
        other => Err(format!("未知配置项 `{other}`。\n\n{CONFIG_USAGE}")),
    }
}

/// Access check for an agent-rendered card click.
///
/// Mirrors the TypeScript dispatcher's `canUseDm`/`canUseGroup` union: the card
/// action event carries no chat mode, so the operator may pass through either
/// the DM allowlist (`allowedUsers`) or the group allowlist (`allowedChats`).
/// Team mode opens usage to everyone. Empty lists fail closed. The runtime app
/// owner is not wired into card-click checks yet ([`crate::owner`] currently
/// gates admin commands only), so this stays a static-list check.
fn card_click_allowed(profile: &ProfileConfig, chat_id: &str, operator: &str) -> bool {
    if profile.mode == ProfileMode::Team {
        return true;
    }
    let Some(access) = profile.access.as_ref() else {
        return false;
    };
    access.admins.iter().any(|id| id == operator)
        || access.allowed_users.iter().any(|id| id == operator)
        || access.allowed_chats.iter().any(|id| id == chat_id)
}

/// Remove the bridge markers from a signed agent callback and merge CardKit
/// form values, matching the TypeScript `forwardToAgent`.
fn strip_callback_markers(
    value: &serde_json::Map<String, Value>,
    form_value: Option<&Value>,
) -> Value {
    let mut payload: serde_json::Map<String, Value> = value
        .iter()
        .filter(|(key, _)| {
            key.as_str() != BRIDGE_CALLBACK_MARKER && key.as_str() != BRIDGE_TOKEN_FIELD
        })
        .map(|(key, value)| (key.clone(), value.clone()))
        .collect();
    if let Some(form_value) = form_value {
        payload.insert("form_value".to_string(), form_value.clone());
    }
    Value::Object(payload)
}

/// Synthetic user message carrying a verified card click into the run flow.
fn card_click_message(
    action: &CardActionEvent,
    chat_id: &str,
    operator: &str,
    text: String,
) -> NormalizedMessage {
    NormalizedMessage {
        message_id: action
            .card_context
            .as_ref()
            .and_then(|context| context.open_message_id.clone())
            .unwrap_or_default(),
        chat_id: chat_id.to_string(),
        // Card action events carry no chat mode; the run prompt reports
        // "unknown" rather than guessing p2p/group.
        chat_type: MessageChatType::Unknown,
        sender_id: operator.to_string(),
        sender: MessageSenderInfo {
            open_id: operator.to_string(),
            user_id: action.operator.user_id.clone(),
            union_id: action.operator.union_id.clone(),
            sender_type: MessageSenderType::User,
        },
        message_type: "interactive".to_string(),
        text,
        raw_content: String::new(),
        content: None,
        root_id: None,
        parent_id: None,
        thread_id: None,
        mentions: Vec::new(),
        resources: Vec::new(),
        raw: Value::Null,
    }
}

/// Sender id used by commands: the open id when present, else `sender_id`.
fn message_sender_id(message: &NormalizedMessage) -> &str {
    if message.sender.open_id.is_empty() {
        message.sender_id.as_str()
    } else {
        message.sender.open_id.as_str()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicUsize;

    use super::*;
    use async_trait::async_trait;
    use lark_channel::event::{
        CardActionContext, CardActionOperator, CardActionPayload, EventContext,
    };
    use larkline_core::RootConfig;
    use larkline_core::config::{Access, AgentKind};

    use crate::callback::{CallbackKey, CallbackSignInput};
    use crate::owner::OwnerSource;

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

    fn message_in(
        chat_id: &str,
        id: &str,
        sender_id: &str,
        sender_type: &str,
        text: &str,
    ) -> NormalizedMessage {
        let mut message = message(id, sender_id, sender_type, text);
        message.chat_id = chat_id.to_string();
        message
    }

    /// Topic-group message: same chat, explicit `thread_id`.
    fn topic_message(
        chat_id: &str,
        thread_id: &str,
        id: &str,
        sender_id: &str,
        text: &str,
    ) -> NormalizedMessage {
        let mut message = message_in(chat_id, id, sender_id, "user", text);
        message.thread_id = Some(thread_id.to_string());
        message
    }

    /// Profile with an app id, so runtime-owner resolution has an endpoint.
    fn profile_with_app() -> ProfileConfig {
        ProfileConfig::new(
            AgentKind::Claude,
            "cli_test_app",
            larkline_core::SecretInput::Plain("secret".into()),
            TenantBrand::Feishu,
        )
    }

    /// Deterministic owner source; no network and no timing dependency.
    struct FakeOwnerSource {
        result: std::sync::Mutex<Result<Option<String>, String>>,
        calls: AtomicUsize,
    }

    impl FakeOwnerSource {
        fn owner(id: &str) -> Arc<Self> {
            Arc::new(Self {
                result: std::sync::Mutex::new(Ok(Some(id.to_string()))),
                calls: AtomicUsize::new(0),
            })
        }

        fn failure(message: &str) -> Arc<Self> {
            Arc::new(Self {
                result: std::sync::Mutex::new(Err(message.to_string())),
                calls: AtomicUsize::new(0),
            })
        }

        fn calls(&self) -> usize {
            self.calls.load(Ordering::SeqCst)
        }
    }

    #[async_trait]
    impl OwnerSource for FakeOwnerSource {
        async fn fetch_owner(&self, _app_id: &str) -> Result<Option<String>, String> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            self.result.lock().expect("fake owner result").clone()
        }
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
    fn show_tool_calls_false_hides_the_tools_block() {
        let mut view = RunView::default();
        view.apply(&AgentEvent::ToolUse {
            id: "t1".into(),
            name: "Bash".into(),
            input: json!({ "command": "ls" }),
        });
        let shown = serde_json::to_string(
            build_card(&profile(), &view, "运行中", Instant::now(), None).as_value(),
        )
        .unwrap();
        assert!(shown.contains("Bash"));

        let mut hidden = profile();
        hidden
            .preferences
            .get_or_insert_with(Preferences::default)
            .show_tool_calls = Some(false);
        let shown = serde_json::to_string(
            build_card(&hidden, &view, "运行中", Instant::now(), None).as_value(),
        )
        .unwrap();
        assert!(!shown.contains("Bash"));
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
    fn text_render_options_follow_profile_preferences() {
        let options = text_render_options(&profile());
        assert_eq!(options.format, TextFormat::Plain);
        assert!(options.show_tool_calls);
        assert!(!options.reasoning);

        // Only `inline` shows reasoning in a text reply; `auto` is card-only,
        // `off` disables it and `bubble` is the separate COT stream.
        for (display, expected) in [
            ("auto", false),
            ("off", false),
            ("inline", true),
            ("bubble", false),
        ] {
            let profile: ProfileConfig = serde_json::from_value(json!({
                "preferences": {
                    "reasoningDisplay": display,
                    "showToolCalls": false
                }
            }))
            .expect("profile fixture");
            let options = text_render_options(&profile);
            assert_eq!(options.reasoning, expected, "reasoningDisplay={display}");
            assert!(!options.show_tool_calls, "showToolCalls=false");
        }
    }

    #[test]
    fn text_final_body_uses_the_text_renderer() {
        let mut view = RunView::default();
        view.apply(&AgentEvent::Text {
            delta: "hello".into(),
        });
        view.apply(&AgentEvent::ToolUse {
            id: "t1".into(),
            name: "Bash".into(),
            input: json!({ "command": "pwd" }),
        });
        view.apply(&AgentEvent::Done {
            session_id: None,
            thread_id: None,
            termination_reason: TerminationReason::Normal,
        });

        assert_eq!(text_final_body(&profile(), &view), "hello\n\n• Bash — pwd");

        let mut hidden_tools = profile();
        hidden_tools
            .preferences
            .as_mut()
            .expect("default preferences")
            .show_tool_calls = Some(false);
        assert_eq!(text_final_body(&hidden_tools, &view), "hello");

        // Emails are neutralized in the final text path too.
        let mut email_view = RunView::default();
        email_view.apply(&AgentEvent::Text {
            delta: "mail team@example.org".into(),
        });
        email_view.apply(&AgentEvent::Done {
            session_id: None,
            thread_id: None,
            termination_reason: TerminationReason::Normal,
        });
        let body = text_final_body(&profile(), &email_view);
        assert!(body.contains("team[at]example.org"));
        assert!(!body.contains('@'));

        // No answer at all keeps the bridge's historical placeholder.
        let mut empty = RunView::default();
        empty.apply(&AgentEvent::Done {
            session_id: None,
            thread_id: None,
            termination_reason: TerminationReason::Normal,
        });
        assert_eq!(text_final_body(&profile(), &empty), "（没有输出）");
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

    #[tokio::test]
    async fn meeting_manager_starts_only_when_the_profile_opts_in() {
        let root = tempfile::tempdir().expect("temp home");
        let paths = AppPaths::resolve(root.path(), "test").expect("app paths");
        let workspace = root.path().join("workspace");
        let channel = Channel::new("cli_test_app", "secret", TenantBrand::Feishu);
        let agent = PathBuf::from("/nonexistent/larkline-test-agent");

        // Disabled by default: no manager, and pushes are not consumed.
        let bridge = Arc::new(
            Bridge::new(
                channel.clone(),
                profile(),
                agent.clone(),
                workspace.clone(),
                None,
                None,
                paths.clone(),
            )
            .expect("bridge"),
        );
        assert!(!bridge.start_meeting());
        assert!(bridge.meeting_manager().is_none());
        assert!(!bridge.handle_meeting_event(&json!({
            "header": {"event_type": "vc.bot.meeting_ended_v1"},
            "event": {"meeting": {"id": "70001"}}
        })));

        // Enabling it in `extra.meeting` starts the poll task and routes pushes.
        let mut enabled = profile();
        enabled.extra.insert(
            "meeting".to_string(),
            json!({"enabled": true, "trigger": "@小助手"}),
        );
        let bridge = Arc::new(
            Bridge::new(channel, enabled, agent, workspace, None, None, paths).expect("bridge"),
        );
        assert!(bridge.start_meeting());
        let manager = bridge.meeting_manager().expect("manager");
        assert_eq!(manager.config().trigger, "@小助手");
        assert!(bridge.handle_meeting_event(&json!({
            "header": {"event_type": "vc.bot.meeting_ended_v1"},
            "event": {"meeting": {"id": "70001"}}
        })));
        assert_eq!(manager.push_health().received, 1);
        assert!(!bridge.handle_meeting_event(&json!({
            "header": {"event_type": "im.message.receive_v1"}
        })));

        bridge.stop_meeting().await;
        assert!(bridge.meeting_manager().is_none());
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

    // ---- card callbacks -------------------------------------------------

    fn test_bridge(root: &Path, profile: ProfileConfig) -> Arc<Bridge> {
        // Never hit the owner API from a unit test: the real source would try
        // to reach Feishu. Tests that exercise owner gating install their own
        // FakeOwnerSource with `test_bridge_with_owner`.
        test_bridge_with_owner(root, profile, FakeOwnerSource::failure("offline test"))
    }

    fn test_bridge_with_owner(
        root: &Path,
        profile: ProfileConfig,
        owner_source: Arc<dyn OwnerSource>,
    ) -> Arc<Bridge> {
        let paths = AppPaths::resolve(root, "test").expect("app paths");
        let channel = Channel::new("cli_test_app", "secret", TenantBrand::Feishu);
        let mut bridge = Bridge::new(
            channel,
            profile,
            PathBuf::from("/nonexistent/larkline-test-agent"),
            root.join("workspace"),
            None,
            None,
            paths,
        )
        .expect("bridge");
        bridge.owner = OwnerGate::new(owner_source);
        Arc::new(bridge)
    }

    fn callback_profile(secret: &str) -> ProfileConfig {
        let mut profile = profile();
        profile
            .extra
            .insert("callback".to_string(), json!({ "secret": secret }));
        // Card-click access uses the TS union; allow the fixture clicker.
        profile.access = Some(Access {
            allowed_users: vec!["ou_1".into()],
            ..Access::default()
        });
        profile
    }

    fn sign_callback(secret: &str, nonce: &str) -> String {
        CallbackAuth::new(vec![CallbackKey {
            version: 1,
            secret: secret.to_string(),
            retired: false,
        }])
        .expect("auth")
        .sign(&CallbackSignInput {
            run_id: "run_1".to_string(),
            scope: "oc_1".to_string(),
            chat_id: "oc_1".to_string(),
            operator_open_id: "ou_1".to_string(),
            action: AGENT_CALLBACK_ACTION.to_string(),
            policy_fingerprint: "fp_1".to_string(),
            ttl_ms: 60_000,
            nonce: Some(nonce.to_string()),
        })
        .expect("token")
    }

    fn card_action_event(value: Value) -> CardActionEvent {
        card_action_event_in(value, "oc_1")
    }

    fn card_action_event_in(value: Value, chat_id: &str) -> CardActionEvent {
        CardActionEvent {
            context: EventContext {
                event_id: "evt_1".to_string(),
                tenant_key: None,
                create_time: None,
            },
            operator: CardActionOperator {
                tenant_key: None,
                user_id: Some("on_1".to_string()),
                open_id: Some("ou_1".to_string()),
                union_id: None,
            },
            token: None,
            action: CardActionPayload {
                value,
                tag: None,
                timezone: None,
                name: None,
                form_value: None,
                input_value: None,
                option: None,
                options: Vec::new(),
                checked: None,
                raw: Value::Null,
            },
            host: None,
            delivery_type: None,
            card_context: Some(CardActionContext {
                url: None,
                preview_token: None,
                open_message_id: Some("om_card".to_string()),
                open_chat_id: Some(chat_id.to_string()),
                raw: Value::Null,
            }),
            raw: Value::Null,
        }
    }

    /// Pull the debounced batch the click was queued into.
    fn take_batch(bridge: &Bridge) -> Vec<NormalizedMessage> {
        let now = Instant::now() + DEBOUNCE + Duration::from_millis(5);
        lock(&bridge.pending)
            .take_due(now)
            .map(|(_, batch)| batch)
            .unwrap_or_default()
    }

    #[test]
    fn callback_markers_and_form_values_are_stripped() {
        let value = json!({
            "__bridge_cb": true,
            "bridge_token": "signed",
            "choice": "a",
            "ticket_id": 42
        });
        let stripped =
            strip_callback_markers(value.as_object().unwrap(), Some(&json!({ "input_1": "x" })));
        assert_eq!(
            serde_json::to_string(&stripped).unwrap(),
            r#"{"choice":"a","ticket_id":42,"form_value":{"input_1":"x"}}"#
        );

        let without_form = strip_callback_markers(value.as_object().unwrap(), None);
        assert_eq!(
            serde_json::to_string(&without_form).unwrap(),
            r#"{"choice":"a","ticket_id":42}"#
        );
    }

    #[test]
    fn card_click_access_mirrors_the_ts_union_and_fails_closed() {
        let mut profile = profile();
        profile.access = Some(Access::default());
        assert!(!card_click_allowed(&profile, "oc_1", "ou_1"));

        profile.access = Some(Access {
            allowed_users: vec!["ou_dm".into()],
            ..Access::default()
        });
        assert!(card_click_allowed(&profile, "oc_1", "ou_dm"));
        assert!(!card_click_allowed(&profile, "oc_1", "ou_other"));

        profile.access = Some(Access {
            allowed_chats: vec!["oc_1".into()],
            ..Access::default()
        });
        assert!(card_click_allowed(&profile, "oc_1", "ou_anyone"));
        assert!(!card_click_allowed(&profile, "oc_2", "ou_anyone"));

        profile.access = Some(Access {
            admins: vec!["ou_admin".into()],
            ..Access::default()
        });
        assert!(card_click_allowed(&profile, "oc_2", "ou_admin"));

        profile.mode = ProfileMode::Team;
        profile.access = Some(Access::default());
        assert!(card_click_allowed(&profile, "oc_1", "ou_anyone"));
    }

    #[test]
    fn config_set_validates_keys_and_values() {
        assert_eq!(
            parse_config_set("messageReply", "card").unwrap(),
            PreferenceEdit::MessageReply(MessageReplyMode::Card)
        );
        assert_eq!(
            parse_config_set("messageReply", "MARKDOWN").unwrap(),
            PreferenceEdit::MessageReply(MessageReplyMode::Markdown)
        );
        assert_eq!(
            parse_config_set("showToolCalls", "false").unwrap(),
            PreferenceEdit::ShowToolCalls(false)
        );
        assert_eq!(
            parse_config_set("showToolCalls", "TRUE").unwrap(),
            PreferenceEdit::ShowToolCalls(true)
        );
        assert_eq!(
            parse_config_set("maxConcurrentRuns", "5").unwrap(),
            PreferenceEdit::MaxConcurrentRuns(5)
        );
        // Same normalisation as the TypeScript config card: floor then clamp.
        assert_eq!(
            parse_config_set("maxConcurrentRuns", "2.9").unwrap(),
            PreferenceEdit::MaxConcurrentRuns(2)
        );
        assert_eq!(
            parse_config_set("maxConcurrentRuns", "99").unwrap(),
            PreferenceEdit::MaxConcurrentRuns(50)
        );
        for (key, value) in [
            ("messageReply", "html"),
            ("showToolCalls", "yes"),
            ("maxConcurrentRuns", "abc"),
            ("maxConcurrentRuns", "0"),
            ("unknown", "x"),
            ("", ""),
        ] {
            let err = parse_config_set(key, value).unwrap_err();
            assert!(
                err.contains("用法") || err.contains("1-50"),
                "{key}={value}: {err}"
            );
        }
    }

    #[tokio::test]
    async fn agent_callback_is_forwarded_to_the_agent_as_a_card_click() {
        let root = tempfile::tempdir().unwrap();
        let bridge = test_bridge(root.path(), callback_profile("s3cret"));
        let token = sign_callback("s3cret", "nonce_forward");
        let event = card_action_event(json!({
            "__bridge_cb": true,
            "bridge_token": token,
            "choice": "a",
            "ticket_id": 42
        }));

        assert!(bridge.handle_card_action(&event).await.is_none());

        let batch = take_batch(&bridge);
        assert_eq!(batch.len(), 1);
        assert_eq!(batch[0].chat_id, "oc_1");
        assert_eq!(batch[0].sender_id, "ou_1");
        assert_eq!(batch[0].message_id, "om_card");
        assert_eq!(
            batch[0].text,
            r#"[card-click] {"choice":"a","ticket_id":42}"#
        );
        assert!(!batch[0].text.contains("__bridge_cb"));
        assert!(!batch[0].text.contains("bridge_token"));
    }

    #[tokio::test]
    async fn agent_callback_payload_fields_named_action_are_still_forwarded() {
        // An agent button may carry arbitrary state, including `action` and
        // `scope`; the presence of `bridge_token` must win over the native
        // button fields.
        let root = tempfile::tempdir().unwrap();
        let bridge = test_bridge(root.path(), callback_profile("s3cret"));
        let token = sign_callback("s3cret", "nonce_state_fields");
        let event = card_action_event(json!({
            "__bridge_cb": true,
            "bridge_token": token,
            "action": "approve",
            "scope": "oc_other",
            "choice": "a"
        }));

        assert!(bridge.handle_card_action(&event).await.is_none());
        let batch = take_batch(&bridge);
        assert_eq!(batch.len(), 1);
        assert_eq!(
            batch[0].text,
            r#"[card-click] {"action":"approve","scope":"oc_other","choice":"a"}"#
        );
    }

    #[tokio::test]
    async fn agent_callback_with_a_forged_token_is_dropped() {
        let root = tempfile::tempdir().unwrap();
        let bridge = test_bridge(root.path(), callback_profile("s3cret"));
        let forged = sign_callback("attacker", "nonce_forged");
        let event = card_action_event(json!({
            "__bridge_cb": true,
            "bridge_token": forged,
            "choice": "a"
        }));

        assert!(bridge.handle_card_action(&event).await.is_none());
        assert!(take_batch(&bridge).is_empty());
    }

    #[tokio::test]
    async fn agent_callback_without_a_configured_key_is_dropped() {
        let root = tempfile::tempdir().unwrap();
        let bridge = test_bridge(root.path(), profile());
        let token = sign_callback("s3cret", "nonce_disabled");
        let event = card_action_event(json!({
            "__bridge_cb": true,
            "bridge_token": token
        }));

        assert!(bridge.handle_card_action(&event).await.is_none());
        assert!(take_batch(&bridge).is_empty());
    }

    #[tokio::test]
    async fn bridge_rendered_stop_button_still_works() {
        let root = tempfile::tempdir().unwrap();
        let bridge = test_bridge(root.path(), profile());
        let token = bridge.tokens.sign("oc_1", "stop");
        let event = card_action_event(json!({
            "__bridge_cb": true,
            "action": "stop",
            "scope": "oc_1",
            "token": token
        }));
        assert_eq!(
            bridge.handle_card_action(&event).await.as_deref(),
            Some("当前没有正在运行的任务。")
        );

        let forged = card_action_event(json!({
            "__bridge_cb": true,
            "action": "stop",
            "scope": "oc_1",
            "token": "0".repeat(64)
        }));
        assert_eq!(
            bridge.handle_card_action(&forged).await.as_deref(),
            Some("操作已失效，请重新发起。")
        );
    }

    #[tokio::test]
    async fn bridge_stop_button_from_another_chat_is_rejected() {
        let root = tempfile::tempdir().unwrap();
        let bridge = test_bridge(root.path(), profile());
        let token = bridge.tokens.sign("oc_1", "stop");
        // Legitimate token for chat oc_1, but the card now lives in oc_2.
        let event = card_action_event_in(
            json!({
                "__bridge_cb": true,
                "action": "stop",
                "scope": "oc_1",
                "token": token
            }),
            "oc_2",
        );
        assert_eq!(
            bridge.handle_card_action(&event).await.as_deref(),
            Some("操作已失效，请重新发起。")
        );
    }

    // ---- topic-scoped sessions, queues and runs -------------------------

    #[test]
    fn scope_key_is_chat_for_direct_and_group_messages_and_chat_thread_for_topics() {
        let mut direct = message("om_1", "ou_1", "user", "hi");
        direct.chat_type = MessageChatType::P2p;
        assert_eq!(scope_key(&direct), "oc_1");

        let group = message("om_1", "ou_1", "user", "hi");
        assert_eq!(scope_key(&group), "oc_1");

        let topic = topic_message("oc_1", "omt_1", "om_1", "ou_1", "hi");
        assert_eq!(scope_key(&topic), "oc_1:omt_1");

        // An empty thread id is the same as an absent one: the key must stay
        // exactly the chat id so pre-topic sessions.json entries keep working.
        let mut empty_thread = group.clone();
        empty_thread.thread_id = Some(String::new());
        assert_eq!(scope_key(&empty_thread), "oc_1");
    }

    #[tokio::test]
    async fn topic_messages_queue_per_thread_while_plain_messages_stay_on_the_chat() {
        let root = tempfile::tempdir().unwrap();
        let bridge = test_bridge(root.path(), profile());

        bridge.enqueue(topic_message("oc_1", "omt_a", "om_a", "ou_1", "a"));
        bridge.enqueue(topic_message("oc_1", "omt_b", "om_b", "ou_1", "b"));
        bridge.enqueue(message_in("oc_1", "om_group", "ou_1", "user", "group"));

        {
            let pending = lock(&bridge.pending);
            assert_eq!(pending.queued("oc_1:omt_a"), 1);
            assert_eq!(pending.queued("oc_1:omt_b"), 1);
            assert_eq!(pending.queued("oc_1"), 1);
        }

        bridge.discard_pending("oc_1:omt_a");
        {
            let pending = lock(&bridge.pending);
            assert_eq!(pending.queued("oc_1:omt_a"), 0);
            assert_eq!(pending.queued("oc_1:omt_b"), 1);
            assert_eq!(pending.queued("oc_1"), 1);
        }
    }

    #[tokio::test]
    async fn topic_sessions_are_isolated_from_each_other_and_from_the_chat() {
        let root = tempfile::tempdir().unwrap();
        let bridge = test_bridge(root.path(), profile());
        let scope_a = scope_key(&topic_message("oc_1", "omt_a", "om_a", "ou_1", "a"));
        let scope_b = scope_key(&topic_message("oc_1", "omt_b", "om_b", "ou_1", "b"));

        bridge
            .save_scope_session(&scope_a, "sess-a", "/work/topic-a")
            .await;
        bridge
            .save_scope_session(&scope_b, "sess-b", "/work/topic-b")
            .await;
        bridge
            .save_scope_session("oc_1", "sess-chat", "/work/chat")
            .await;

        assert_eq!(
            bridge
                .resume_scope_session(&scope_a, "/work/topic-a")
                .await
                .as_deref(),
            Some("sess-a")
        );
        assert_eq!(
            bridge
                .resume_scope_session(&scope_b, "/work/topic-b")
                .await
                .as_deref(),
            Some("sess-b")
        );
        assert_eq!(
            bridge
                .resume_scope_session("oc_1", "/work/chat")
                .await
                .as_deref(),
            Some("sess-chat")
        );
        // A topic never resumes a sibling's session (or the chat-level one),
        // and a stale cwd still invalidates the entry.
        assert_eq!(
            bridge.resume_scope_session(&scope_a, "/work/topic-b").await,
            None
        );
        assert_eq!(
            bridge.resume_scope_session(&scope_b, "/work/chat").await,
            None
        );
        assert_eq!(
            bridge.resume_scope_session("oc_1", "/work/topic-a").await,
            None
        );
    }

    #[tokio::test]
    async fn new_command_clears_only_the_topic_session() {
        let root = tempfile::tempdir().unwrap();
        let bridge = test_bridge(root.path(), profile());
        let scope_a = scope_key(&topic_message("oc_1", "omt_a", "om_a", "ou_1", "a"));
        let scope_b = scope_key(&topic_message("oc_1", "omt_b", "om_b", "ou_1", "b"));
        bridge.save_scope_session(&scope_a, "sess-a", "/work").await;
        bridge.save_scope_session(&scope_b, "sess-b", "/work").await;

        let reply = bridge
            .handle_command(
                &topic_message("oc_1", "omt_a", "om_new", "ou_1", "/new"),
                "/new",
            )
            .await
            .expect("handled");
        assert!(reply.contains("新会话"), "{reply}");

        assert_eq!(bridge.resume_scope_session(&scope_a, "/work").await, None);
        assert_eq!(
            bridge
                .resume_scope_session(&scope_b, "/work")
                .await
                .as_deref(),
            Some("sess-b")
        );
    }

    #[tokio::test]
    async fn stop_command_aborts_only_the_topic_that_sent_it() {
        let root = tempfile::tempdir().unwrap();
        let bridge = test_bridge(root.path(), profile());
        let scope_a = "oc_1:omt_a";
        let scope_b = "oc_1:omt_b";
        let handle_a = tokio::spawn(std::future::pending::<()>());
        let handle_b = tokio::spawn(std::future::pending::<()>());
        {
            let mut active = lock(&bridge.active);
            active.insert(
                scope_a.to_string(),
                ActiveRun {
                    id: 1,
                    abort: handle_a.abort_handle(),
                },
            );
            active.insert(
                scope_b.to_string(),
                ActiveRun {
                    id: 2,
                    abort: handle_b.abort_handle(),
                },
            );
        }
        {
            let mut pending = lock(&bridge.pending);
            let now = Instant::now();
            pending.block(scope_a);
            pending.push(
                scope_a,
                topic_message("oc_1", "omt_a", "om_queued_a", "ou_1", "queued a"),
                now,
            );
            pending.block(scope_b);
            pending.push(
                scope_b,
                topic_message("oc_1", "omt_b", "om_queued_b", "ou_1", "queued b"),
                now,
            );
        }

        let reply = bridge
            .handle_command(
                &topic_message("oc_1", "omt_a", "om_stop", "ou_1", "/stop"),
                "/stop",
            )
            .await
            .expect("handled");
        assert_eq!(reply, "已停止当前任务。");

        {
            let active = lock(&bridge.active);
            assert!(!active.contains_key(scope_a), "topic A must be aborted");
            assert!(active.contains_key(scope_b), "topic B must keep running");
        }
        {
            let pending = lock(&bridge.pending);
            assert_eq!(pending.queued(scope_a), 0);
            assert!(!pending.is_blocked(scope_a));
            assert_eq!(pending.queued(scope_b), 1);
            assert!(pending.is_blocked(scope_b));
        }

        // Cleanup: abort the sibling topic's fake run.
        assert!(bridge.stop_scope(scope_b).0);
    }

    #[tokio::test]
    async fn topic_stop_button_is_bound_to_its_own_scope() {
        let root = tempfile::tempdir().unwrap();
        let bridge = test_bridge(root.path(), profile());
        let scope_a = "oc_1:omt_a";
        let scope_b = "oc_1:omt_b";
        let handle_b = tokio::spawn(std::future::pending::<()>());
        {
            let mut active = lock(&bridge.active);
            active.insert(
                scope_a.to_string(),
                ActiveRun {
                    id: 1,
                    abort: tokio::spawn(std::future::pending::<()>()).abort_handle(),
                },
            );
            active.insert(
                scope_b.to_string(),
                ActiveRun {
                    id: 2,
                    abort: handle_b.abort_handle(),
                },
            );
        }

        let event = card_action_event(json!({
            "__bridge_cb": true,
            "action": "stop",
            "scope": scope_a,
            "token": bridge.tokens.sign(scope_a, "stop"),
        }));
        assert_eq!(
            bridge.handle_card_action(&event).await.as_deref(),
            Some("已停止当前任务。")
        );
        {
            let active = lock(&bridge.active);
            assert!(!active.contains_key(scope_a));
            assert!(active.contains_key(scope_b));
        }
        assert!(bridge.stop_scope(scope_b).0);
    }

    #[tokio::test]
    async fn explicit_stop_aborts_comment_and_meeting_scopes_by_id() {
        let root = tempfile::tempdir().unwrap();
        let mut profile = profile();
        profile.access = Some(Access {
            admins: vec!["ou_admin".into()],
            ..Access::default()
        });
        let bridge = test_bridge(root.path(), profile);
        let comment_scope = "comment:0123456789abcdef";
        let meeting_scope = "meeting:70001";
        let other_scope = "oc_2";

        let comment_run = bridge.spawn_tracked(comment_scope, std::future::pending::<()>());
        let meeting_run = bridge.spawn_tracked(meeting_scope, std::future::pending::<()>());
        let other_run = bridge.spawn_tracked(other_scope, std::future::pending::<()>());
        assert!(bridge.is_scope_active(comment_scope));
        assert!(bridge.is_scope_active(meeting_scope));

        // A non-admin cannot target an arbitrary scope.
        let denied = bridge
            .handle_command(
                &message(
                    "om_stop",
                    "ou_user",
                    "user",
                    "/stop comment:0123456789abcdef",
                ),
                "/stop comment:0123456789abcdef",
            )
            .await
            .expect("handled");
        assert_eq!(denied, STOP_SCOPE_DENIED_REPLY);
        assert!(bridge.is_scope_active(comment_scope));

        let stopped = bridge
            .handle_command(
                &message(
                    "om_stop",
                    "ou_admin",
                    "user",
                    "/stop comment:0123456789abcdef",
                ),
                "/stop comment:0123456789abcdef",
            )
            .await
            .expect("handled");
        assert_eq!(stopped, "已请求停止 `comment:0123456789abcdef`。");
        assert!(!bridge.is_scope_active(comment_scope));
        assert!(
            bridge.is_scope_active(meeting_scope),
            "stopping a comment scope must not abort a meeting run"
        );
        assert!(bridge.is_scope_active(other_scope));

        let stopped = bridge
            .handle_command(
                &message("om_stop", "ou_admin", "user", "/stop meeting:70001"),
                "/stop meeting:70001",
            )
            .await
            .expect("handled");
        assert_eq!(stopped, "已请求停止 `meeting:70001`。");
        assert!(!bridge.is_scope_active(meeting_scope));
        assert!(
            bridge.is_scope_active(other_scope),
            "stopping a meeting scope must not abort an IM run"
        );

        let missing = bridge
            .handle_command(
                &message("om_stop", "ou_admin", "user", "/stop comment:missing"),
                "/stop comment:missing",
            )
            .await
            .expect("handled");
        assert_eq!(missing, "未找到正在运行的任务：`comment:missing`。");

        // Aborted tasks are cancelled, not left running.
        for handle in [comment_run, meeting_run] {
            let error = tokio::time::timeout(Duration::from_secs(2), handle)
                .await
                .expect("aborted task finishes")
                .expect_err("abort yields JoinError");
            assert!(error.is_cancelled());
        }
        assert!(bridge.stop_scope(other_scope).0);
        assert!(!bridge.is_scope_active(other_scope));
        let _ = other_run.await;
    }

    #[tokio::test]
    async fn plain_stop_does_not_abort_other_run_kinds() {
        let root = tempfile::tempdir().unwrap();
        let bridge = test_bridge(root.path(), profile());
        let comment_scope = "comment:fedcba9876543210";
        let meeting_scope = "meeting:70002";
        let chat_scope = "oc_1";

        let comment_run = bridge.spawn_tracked(comment_scope, std::future::pending::<()>());
        let meeting_run = bridge.spawn_tracked(meeting_scope, std::future::pending::<()>());
        let chat_run = bridge.spawn_tracked(chat_scope, std::future::pending::<()>());

        let reply = bridge
            .handle_command(&message("om_stop", "ou_1", "user", "/stop"), "/stop")
            .await
            .expect("handled");
        assert_eq!(reply, "已停止当前任务。");
        assert!(!bridge.is_scope_active(chat_scope));
        assert!(bridge.is_scope_active(comment_scope));
        assert!(bridge.is_scope_active(meeting_scope));

        assert!(bridge.stop_scope(comment_scope).0);
        assert!(bridge.stop_scope(meeting_scope).0);
        for handle in [comment_run, meeting_run, chat_run] {
            let _ = tokio::time::timeout(Duration::from_secs(2), handle).await;
        }
    }

    #[tokio::test]
    async fn config_shows_and_persists_the_writable_preferences() {
        let root = tempfile::tempdir().unwrap();
        let mut profile = profile();
        profile.access = Some(Access {
            admins: vec!["ou_admin".into()],
            ..Access::default()
        });
        let paths = AppPaths::resolve(root.path(), "test").unwrap();
        let mut root_config = RootConfig::default();
        root_config.active_profile = "test".into();
        root_config.profiles.insert("test".into(), profile.clone());
        larkline_core::store::save_root(&paths.config_file, &root_config).unwrap();
        let bridge = test_bridge(root.path(), profile);
        let admin = message("om_1", "ou_admin", "user", "");

        let shown = bridge.handle_config(&admin, "").await;
        assert!(shown.contains("messageReply: markdown"), "{shown}");
        assert!(shown.contains("showToolCalls: true"), "{shown}");
        assert!(shown.contains("maxConcurrentRuns: 10"), "{shown}");

        let saved = bridge.handle_config(&admin, "set messageReply card").await;
        assert!(saved.starts_with('✓'), "{saved}");
        assert_eq!(
            bridge.live_profile().message_reply(),
            MessageReplyMode::Card
        );

        let saved = bridge
            .handle_config(&admin, "set showToolCalls false")
            .await;
        assert!(saved.starts_with('✓'), "{saved}");
        assert!(!bridge.live_profile().show_tool_calls());

        let saved = bridge
            .handle_config(&admin, "set maxConcurrentRuns 3")
            .await;
        assert!(saved.starts_with('✓'), "{saved}");
        assert_eq!(bridge.live_profile().max_concurrent_runs(), 3);
        assert_eq!(bridge.run_limit.cap(), 3);

        let persisted = larkline_core::store::load_root(&paths.config_file)
            .unwrap()
            .expect("root config");
        let prefs = persisted
            .profile("test")
            .expect("profile")
            .preferences
            .clone()
            .expect("preferences");
        assert_eq!(prefs.message_reply, Some(MessageReplyMode::Card));
        assert_eq!(prefs.message_reply_migrated, Some(true));
        assert_eq!(prefs.show_tool_calls, Some(false));
        assert_eq!(prefs.max_concurrent_runs, Some(3));
    }

    #[tokio::test]
    async fn config_set_requires_an_admin_while_show_is_open() {
        let root = tempfile::tempdir().unwrap();
        let mut profile = profile();
        profile.access = Some(Access {
            admins: vec!["ou_admin".into()],
            allowed_users: vec!["ou_user".into()],
            ..Access::default()
        });
        let paths = AppPaths::resolve(root.path(), "test").unwrap();
        let mut root_config = RootConfig::default();
        root_config.active_profile = "test".into();
        root_config.profiles.insert("test".into(), profile.clone());
        larkline_core::store::save_root(&paths.config_file, &root_config).unwrap();
        let bridge = test_bridge(root.path(), profile);
        let user = message("om_1", "ou_user", "user", "");
        let admin = message("om_1", "ou_admin", "user", "");

        // Reads stay open to admitted users.
        let shown = bridge.handle_config(&user, "").await;
        assert!(shown.contains("messageReply"), "{shown}");

        let denied = bridge.handle_config(&user, "set messageReply card").await;
        assert!(denied.contains("管理员"), "{denied}");
        assert_eq!(
            bridge.live_profile().message_reply(),
            MessageReplyMode::Markdown
        );

        let unknown = bridge.handle_config(&admin, "set nope 1").await;
        assert!(unknown.contains("用法"), "{unknown}");

        // maxConcurrentRuns is writable now; the cap change is live.
        let saved = bridge
            .handle_config(&admin, "set maxConcurrentRuns 5")
            .await;
        assert!(saved.starts_with('✓'), "{saved}");
        assert_eq!(bridge.run_limit.cap(), 5);
    }

    #[tokio::test]
    async fn config_change_resizes_the_live_run_cap() {
        let root = tempfile::tempdir().unwrap();
        let mut profile = profile();
        profile.access = Some(Access {
            admins: vec!["ou_admin".into()],
            ..Access::default()
        });
        let paths = AppPaths::resolve(root.path(), "test").unwrap();
        let mut root_config = RootConfig::default();
        root_config.active_profile = "test".into();
        root_config.profiles.insert("test".into(), profile.clone());
        larkline_core::store::save_root(&paths.config_file, &root_config).unwrap();
        let bridge = test_bridge(root.path(), profile);
        let admin = message("om_1", "ou_admin", "user", "");

        // One run holds a slot at the old cap (10).
        let held = bridge.run_limit.try_acquire().expect("initial slot");
        let saved = bridge
            .handle_config(&admin, "set maxConcurrentRuns 1")
            .await;
        assert!(saved.starts_with('✓'), "{saved}");
        assert_eq!(bridge.run_limit.cap(), 1);
        // Lowering below the number of live runs throttles new starts instead
        // of killing the live run, exactly like ProcessPool.release().
        assert!(bridge.run_limit.try_acquire().is_none());

        drop(held);
        let replacement = bridge.run_limit.try_acquire().expect("slot after run");
        assert!(bridge.run_limit.try_acquire().is_none());
        drop(replacement);
    }

    // ---- global run limiter / queue interaction -------------------------

    fn seed_due(bridge: &Bridge, chat_id: &str, message_id: &str, text: &str, age: Duration) {
        let message = message_in(chat_id, message_id, "ou_1", "user", text);
        let mut pending = lock(&bridge.pending);
        pending.push(chat_id, message, Instant::now() - age);
    }

    fn claim_due(bridge: &Bridge) -> BatchClaim {
        let mut pending = lock(&bridge.pending);
        bridge.claim_due_batch_locked(&mut pending, Instant::now())
    }

    #[tokio::test]
    async fn global_cap_leaves_due_batches_queued_until_a_slot_is_released() {
        let root = tempfile::tempdir().unwrap();
        let mut profile = profile();
        profile
            .preferences
            .get_or_insert_with(Preferences::default)
            .max_concurrent_runs = Some(1);
        let bridge = test_bridge(root.path(), profile);

        // Both quiet windows elapsed; `oc_a` is due first.
        seed_due(&bridge, "oc_a", "om_a", "first", Duration::from_secs(2));
        seed_due(&bridge, "oc_b", "om_b", "second", Duration::from_secs(1));

        let first = match claim_due(&bridge) {
            BatchClaim::Claimed(claimed) => claimed,
            _ => panic!("first batch should be startable"),
        };
        assert_eq!(first.scope, "oc_a");
        assert_eq!(first.batch.len(), 1);
        assert_eq!(bridge.run_limit.active(), 1);

        // The second batch is due but there is no slot: it must stay queued
        // and unblocked rather than be dropped or started.
        assert!(matches!(claim_due(&bridge), BatchClaim::AtCapacity));
        {
            let pending = lock(&bridge.pending);
            assert_eq!(pending.queued("oc_b"), 1);
            assert!(!pending.is_blocked("oc_b"));
            assert_eq!(pending.queued("oc_a"), 0);
            assert!(pending.is_blocked("oc_a"));
        }

        // Simulate the run ending: RunGuard drops the permit then unblocks the
        // scope (no task is spawned by this test, so unblock explicitly).
        drop(first);
        assert_eq!(bridge.run_limit.active(), 0);
        lock(&bridge.pending).unblock("oc_a", Instant::now());

        let second = match claim_due(&bridge) {
            BatchClaim::Claimed(claimed) => claimed,
            _ => panic!("queued batch should start after the slot is released"),
        };
        assert_eq!(second.scope, "oc_b");
        assert_eq!(second.batch[0].text, "second");
        drop(second);
    }

    /// Test-only wrapper that releases the global slot before signalling, so
    /// the test never observes the notification before the semaphore count.
    struct PermitSignal {
        permit: Option<RunPermit>,
        signal: Arc<Notify>,
    }

    impl Drop for PermitSignal {
        fn drop(&mut self) {
            drop(self.permit.take());
            self.signal.notify_one();
        }
    }

    #[tokio::test]
    async fn aborting_a_run_releases_its_global_slot() {
        let root = tempfile::tempdir().unwrap();
        let mut profile = profile();
        profile
            .preferences
            .get_or_insert_with(Preferences::default)
            .max_concurrent_runs = Some(1);
        let bridge = test_bridge(root.path(), profile);

        let permit = bridge.run_limit.try_acquire().expect("only slot");
        let signal = Arc::new(Notify::new());
        let guard = PermitSignal {
            permit: Some(permit),
            signal: Arc::clone(&signal),
        };
        let handle = tokio::spawn(async move {
            let _guard = guard;
            std::future::pending::<()>().await;
        });
        let scope = "oc_1";
        lock(&bridge.pending).block(scope);
        lock(&bridge.active).insert(
            scope.to_string(),
            ActiveRun {
                id: 7,
                abort: handle.abort_handle(),
            },
        );

        let (stopped, dropped) = bridge.stop_scope(scope);
        assert!(stopped);
        assert_eq!(dropped, 0);
        tokio::time::timeout(Duration::from_secs(2), signal.notified())
            .await
            .expect("aborted run dropped its future");
        assert_eq!(bridge.run_limit.active(), 0);
        assert!(bridge.run_limit.try_acquire().is_some());
    }

    // ---- runtime owner gating -------------------------------------------

    fn owner_bridge(
        root: &Path,
        profile: ProfileConfig,
        source: Arc<FakeOwnerSource>,
    ) -> Arc<Bridge> {
        let paths = AppPaths::resolve(root, "test").unwrap();
        let mut root_config = RootConfig::default();
        root_config.active_profile = "test".into();
        root_config.profiles.insert("test".into(), profile.clone());
        larkline_core::store::save_root(&paths.config_file, &root_config).unwrap();
        test_bridge_with_owner(root, profile, source)
    }

    #[tokio::test]
    async fn runtime_owner_can_write_config_without_being_in_the_allowlists() {
        let root = tempfile::tempdir().unwrap();
        let profile = profile_with_app();
        let source = FakeOwnerSource::owner("ou_owner");
        let bridge = owner_bridge(root.path(), profile, Arc::clone(&source));
        let owner = message("om_1", "ou_owner", "user", "");
        let stranger = message("om_2", "ou_stranger", "user", "");

        let saved = bridge.handle_config(&owner, "set messageReply card").await;
        assert!(saved.starts_with('✓'), "{saved}");
        assert_eq!(
            bridge.live_profile().message_reply(),
            MessageReplyMode::Card
        );
        assert_eq!(source.calls(), 1);

        let denied = bridge
            .handle_config(&stranger, "set messageReply markdown")
            .await;
        assert!(denied.contains("管理员"), "{denied}");
        // The owner lookup is cached: a denial does not re-hit the API.
        assert_eq!(source.calls(), 1);
    }

    #[tokio::test]
    async fn owner_lookup_failure_fails_closed_for_non_admins() {
        let root = tempfile::tempdir().unwrap();
        let mut profile = profile_with_app();
        profile.access = Some(Access {
            admins: vec!["ou_admin".into()],
            ..Access::default()
        });
        let source = FakeOwnerSource::failure("permission denied");
        let bridge = owner_bridge(root.path(), profile, Arc::clone(&source));

        // Admins never trigger the owner lookup.
        assert!(bridge.can_run_admin_command("ou_admin").await);
        assert_eq!(source.calls(), 0);

        // A non-admin is denied, and the failed lookup is cached (no retry
        // storm). This is the fail-closed path for missing API scopes.
        assert!(!bridge.can_run_admin_command("ou_user").await);
        assert!(!bridge.can_run_admin_command("ou_other").await);
        assert_eq!(source.calls(), 1);

        let denied = bridge
            .handle_config(
                &message("om_1", "ou_user", "user", ""),
                "set messageReply card",
            )
            .await;
        assert!(denied.contains("管理员"), "{denied}");
        assert_eq!(source.calls(), 1);
    }

    #[tokio::test]
    async fn no_app_id_keeps_admin_gating_fail_closed_without_an_api_call() {
        let root = tempfile::tempdir().unwrap();
        let mut profile = profile();
        profile.access = Some(Access {
            admins: vec!["ou_admin".into()],
            ..Access::default()
        });
        let source = FakeOwnerSource::owner("ou_owner");
        let bridge = owner_bridge(root.path(), profile, Arc::clone(&source));

        assert!(bridge.can_run_admin_command("ou_admin").await);
        assert!(!bridge.can_run_admin_command("ou_owner").await);
        assert_eq!(source.calls(), 0);
    }

    #[tokio::test]
    async fn legacy_allowed_users_gate_still_works_without_an_owner_lookup() {
        let root = tempfile::tempdir().unwrap();
        let mut profile = profile_with_app();
        profile.access = Some(Access {
            allowed_users: vec!["ou_user".into()],
            ..Access::default()
        });
        let source = FakeOwnerSource::owner("ou_owner");
        let bridge = owner_bridge(root.path(), profile, Arc::clone(&source));

        assert!(bridge.can_run_admin_command("ou_user").await);
        assert_eq!(source.calls(), 0);
    }

    // ---- additional card contracts --------------------------------------

    #[test]
    fn card_payload_masks_raw_emails_end_to_end() {
        // Port of the TS "renderCard emits no raw email anywhere in the
        // payload" regression: Feishu's tenant audit rejects a card with an
        // unmasked address, so the card boundary must deep-mask like
        // `renderCard` does.
        let mut view = RunView::default();
        view.apply(&AgentEvent::Text {
            delta: "mail team@example.org and noreply@example.com".into(),
        });
        view.apply(&AgentEvent::ToolUse {
            id: "t1".into(),
            name: "Bash".into(),
            input: json!({ "command": "git log --author=a@example.net" }),
        });
        view.apply(&AgentEvent::Done {
            session_id: None,
            thread_id: None,
            termination_reason: TerminationReason::Normal,
        });

        let card = build_card(&profile(), &view, "完成", Instant::now(), None);
        let payload = serde_json::to_string(card.as_value()).unwrap();

        assert!(!payload.contains('@'), "{payload}");
        assert!(payload.contains("team[at]example.org"), "{payload}");
        assert!(payload.contains("noreply[at]example.com"), "{payload}");
        assert!(payload.contains("a[at]example.net"), "{payload}");
        for email in ["team@example.org", "noreply@example.com", "a@example.net"] {
            assert!(!payload.contains(email), "{email} leaked: {payload}");
        }
    }

    #[test]
    fn card_status_distinguishes_running_done_and_failed() {
        assert_eq!(card_status(&RunView::default()), "运行中");

        let mut done = RunView::default();
        done.apply(&AgentEvent::Done {
            session_id: None,
            thread_id: None,
            termination_reason: TerminationReason::Normal,
        });
        assert_eq!(card_status(&done), "完成");
        let text = serde_json::to_string(
            build_card(&profile(), &done, card_status(&done), Instant::now(), None).as_value(),
        )
        .unwrap();
        assert!(text.contains("完成"), "{text}");

        let mut failed = RunView::default();
        failed.apply(&AgentEvent::Error {
            message: "boom".into(),
            termination_reason: TerminationReason::Failed,
        });
        assert_eq!(card_status(&failed), "失败");
        let text = serde_json::to_string(
            build_card(
                &profile(),
                &failed,
                card_status(&failed),
                Instant::now(),
                None,
            )
            .as_value(),
        )
        .unwrap();
        assert!(text.contains("失败"), "{text}");
    }

    #[test]
    fn card_tool_feed_keeps_the_latest_six_calls_in_order() {
        let mut view = RunView::default();
        for index in 1..=7 {
            view.apply(&AgentEvent::ToolUse {
                id: format!("t{index}"),
                name: "Bash".into(),
                input: json!({ "command": format!("step-{index}") }),
            });
        }
        let card = build_card(&profile(), &view, "运行中", Instant::now(), None);
        let payload = serde_json::to_string(card.as_value()).unwrap();

        // The oldest call falls out of the six-line window.
        assert!(!payload.contains("step-1"), "{payload}");
        let mut previous = 0;
        for index in 2..=7 {
            let marker = format!("step-{index}");
            let position = payload.find(&marker).unwrap_or_else(|| {
                panic!("{marker} missing from card: {payload}");
            });
            assert!(position > previous, "{marker} is out of order: {payload}");
            previous = position;
        }
    }

    #[test]
    fn empty_finished_card_uses_the_placeholder_and_completed_summary() {
        let mut view = RunView::default();
        view.apply(&AgentEvent::Done {
            session_id: None,
            thread_id: None,
            termination_reason: TerminationReason::Normal,
        });
        let card = build_card(&profile(), &view, card_status(&view), Instant::now(), None);

        assert_eq!(
            card.as_value()["body"]["elements"][0]["content"],
            "…",
            "empty answer should render the placeholder"
        );
        let payload = serde_json::to_string(card.as_value()).unwrap();
        assert!(payload.contains("完成"), "{payload}");
    }

    #[test]
    fn card_keeps_local_paths_visible() {
        // Port of the TS "keeps local paths in user-visible cards and text
        // fallbacks" contract: cwd is not a redaction boundary.
        let path = "/Users/example/private/customer/repo/secret.txt";
        let mut view = RunView::default();
        view.apply(&AgentEvent::Text {
            delta: format!("I read {path}"),
        });
        view.apply(&AgentEvent::ToolUse {
            id: "t1".into(),
            name: "Read".into(),
            input: json!({ "file_path": path }),
        });
        view.apply(&AgentEvent::Done {
            session_id: None,
            thread_id: None,
            termination_reason: TerminationReason::Normal,
        });

        let payload = serde_json::to_string(
            build_card(&profile(), &view, "完成", Instant::now(), None).as_value(),
        )
        .unwrap();
        assert!(payload.contains(path), "{payload}");
    }

    // ---- additional command contracts -----------------------------------

    #[tokio::test]
    async fn workspace_cwd_creates_missing_directories_and_falls_back_on_failure() {
        let root = tempfile::tempdir().unwrap();
        let bridge = test_bridge(root.path(), profile());
        let default = root.path().join("workspace");

        // A scope override that does not exist yet is created on demand.
        let nested = root.path().join("client/nested");
        bridge.workspaces.lock().await.set("oc_1", &nested).unwrap();
        assert_eq!(bridge.workspace_cwd("oc_1").await, nested);
        assert!(nested.is_dir());

        // A path replaced by a file cannot be created; the run still gets a
        // usable directory instead of failing or falling back to $HOME.
        let occupied = root.path().join("occupied");
        fs::write(&occupied, "not a directory").unwrap();
        bridge
            .workspaces
            .lock()
            .await
            .set("oc_2", &occupied)
            .unwrap();
        let cwd = bridge.workspace_cwd("oc_2").await;
        assert_eq!(cwd, default);
        assert!(cwd.is_dir());
    }

    #[tokio::test]
    async fn cd_command_canonicalizes_and_persists_the_scope_directory() {
        let root = tempfile::tempdir().unwrap();
        let bridge = test_bridge(root.path(), profile());
        // Spaces are legal in a path and must survive the whitespace-split
        // command parser.
        let target = root.path().join("client work");
        fs::create_dir_all(&target).unwrap();

        let text = format!("/cd {}", target.display());
        let reply = bridge
            .handle_command(&message("om_1", "ou_1", "user", &text), &text)
            .await
            .expect("handled");
        assert!(reply.contains("已切换 cwd"), "{reply}");

        let canonical = target.canonicalize().unwrap();
        assert_eq!(
            bridge
                .workspaces
                .lock()
                .await
                .cwd_for("oc_1")
                .map(str::to_string),
            Some(canonical.to_string_lossy().into_owned())
        );

        let relative = "/cd relative";
        let reply = bridge
            .handle_command(&message("om_2", "ou_1", "user", relative), relative)
            .await
            .expect("handled");
        assert!(reply.contains("绝对路径"), "{reply}");
    }

    #[tokio::test]
    async fn pwd_command_reports_the_scope_override_then_the_profile_default() {
        let root = tempfile::tempdir().unwrap();
        let bridge = test_bridge(root.path(), profile());
        let default = root.path().join("workspace");

        let reply = bridge
            .handle_command(&message("om_1", "ou_1", "user", "/pwd"), "/pwd")
            .await
            .expect("handled");
        assert_eq!(reply, format!("当前 cwd：{}", default.display()));

        let target = root.path().join("override");
        bridge.workspaces.lock().await.set("oc_1", &target).unwrap();
        let reply = bridge
            .handle_command(&message("om_2", "ou_1", "user", "/pwd"), "/pwd")
            .await
            .expect("handled");
        assert_eq!(reply, format!("当前 cwd：{}", target.display()));
    }

    #[tokio::test]
    async fn status_command_reports_implemented_fields_and_active_scopes() {
        let root = tempfile::tempdir().unwrap();
        let bridge = test_bridge(root.path(), profile());
        let cwd = root.path().join("workspace");
        bridge
            .sessions
            .lock()
            .await
            .set("oc_1", "sess-123456789", &cwd.to_string_lossy())
            .unwrap();

        let reply = bridge
            .handle_command(&message("om_1", "ou_1", "user", "/status"), "/status")
            .await
            .expect("handled");
        for expected in [
            "profile: test",
            "agent: Claude Code",
            "app id: (未配置)",
            "tenant: (未配置)",
            "reply mode: markdown",
            "session: 可恢复（sess-123…）",
            "active run: no",
            "active scopes: (无)",
        ] {
            assert!(reply.contains(expected), "{expected} missing: {reply}");
        }
        assert!(
            reply.contains(&format!("cwd: {}", cwd.display())),
            "{reply}"
        );

        // Active scopes are sorted and include non-IM run kinds that
        // `/stop <scope>` can target.
        let chat_run = tokio::spawn(std::future::pending::<()>());
        let comment_run = tokio::spawn(std::future::pending::<()>());
        {
            let mut active = lock(&bridge.active);
            active.insert(
                "comment:abcdef".to_string(),
                ActiveRun {
                    id: 1,
                    abort: comment_run.abort_handle(),
                },
            );
            active.insert(
                "oc_1".to_string(),
                ActiveRun {
                    id: 2,
                    abort: chat_run.abort_handle(),
                },
            );
        }
        let reply = bridge
            .handle_command(&message("om_2", "ou_1", "user", "/status"), "/status")
            .await
            .expect("handled");
        assert!(reply.contains("active run: yes"), "{reply}");
        assert!(
            reply.contains("active scopes: comment:abcdef, oc_1"),
            "{reply}"
        );
        chat_run.abort();
        comment_run.abort();
    }

    #[tokio::test]
    async fn reset_command_clears_only_the_current_scope_session() {
        let root = tempfile::tempdir().unwrap();
        let bridge = test_bridge(root.path(), profile());
        let cwd = root.path().join("workspace").to_string_lossy().into_owned();
        bridge
            .sessions
            .lock()
            .await
            .set("oc_1", "sess-a", &cwd)
            .unwrap();
        bridge
            .sessions
            .lock()
            .await
            .set("oc_2", "sess-b", &cwd)
            .unwrap();

        let reply = bridge
            .handle_command(&message("om_1", "ou_1", "user", "/reset"), "/reset")
            .await
            .expect("handled");
        assert!(reply.contains("新会话"), "{reply}");
        assert_eq!(bridge.resume_scope_session("oc_1", &cwd).await, None);
        assert_eq!(
            bridge.resume_scope_session("oc_2", &cwd).await.as_deref(),
            Some("sess-b")
        );
    }

    #[tokio::test]
    async fn help_advertises_only_implemented_commands() {
        let root = tempfile::tempdir().unwrap();
        let bridge = test_bridge(root.path(), profile());
        let reply = bridge
            .handle_command(&message("om_1", "ou_1", "user", "/help"), "/help")
            .await
            .expect("handled");

        for command in [
            "/new", "/reset", "/cd", "/pwd", "/status", "/config", "/stop", "/meeting", "/help",
        ] {
            assert!(reply.contains(command), "{command} missing: {reply}");
        }
        for command in [
            "/ws",
            "/resume",
            "/timeout",
            "/ps",
            "/exit",
            "/doctor",
            "/reconnect",
            "/invite",
            "/remove",
        ] {
            assert!(
                !reply.contains(command),
                "{command} is not implemented but is advertised: {reply}"
            );
        }
    }

    #[tokio::test]
    async fn unknown_commands_are_left_for_the_agent() {
        let root = tempfile::tempdir().unwrap();
        let bridge = test_bridge(root.path(), profile());

        for text in ["/nope", "/ws", "hello"] {
            assert!(
                bridge
                    .handle_command(&message("om_1", "ou_1", "user", text), text)
                    .await
                    .is_none(),
                "{text} should fall through to the agent"
            );
        }
    }

    #[tokio::test]
    async fn config_usage_errors_do_not_write_and_show_excludes_access_lists() {
        let root = tempfile::tempdir().unwrap();
        let bridge = test_bridge(root.path(), profile());
        let admin = message("om_1", "ou_admin", "user", "");

        for args in [
            "set messageReply",
            "set messageReply card extra",
            "nope",
            "set unknown x",
        ] {
            let reply = bridge.handle_config(&admin, args).await;
            assert!(
                reply.contains("用法") || reply.contains("未知配置项"),
                "{args}: {reply}"
            );
        }
        assert_eq!(
            bridge.live_profile().message_reply(),
            MessageReplyMode::Markdown
        );

        let shown = bridge.handle_config(&admin, "").await;
        for hidden in ["allowed_users", "allowed_chats", "admins"] {
            assert!(!shown.contains(hidden), "{hidden} leaked: {shown}");
        }
        assert!(shown.contains("messageReply: markdown"), "{shown}");
    }

    #[tokio::test]
    async fn config_set_text_mode_stays_text_after_reload() {
        let root = tempfile::tempdir().unwrap();
        let mut profile = profile();
        profile.access = Some(Access {
            admins: vec!["ou_admin".into()],
            ..Access::default()
        });
        let paths = AppPaths::resolve(root.path(), "test").unwrap();
        let mut root_config = RootConfig {
            active_profile: "test".into(),
            ..RootConfig::default()
        };
        root_config.profiles.insert("test".into(), profile.clone());
        larkline_core::store::save_root(&paths.config_file, &root_config).unwrap();
        let bridge = test_bridge(root.path(), profile);
        let admin = message("om_1", "ou_admin", "user", "");

        let saved = bridge.handle_config(&admin, "set messageReply text").await;
        assert!(saved.starts_with('✓'), "{saved}");
        assert_eq!(
            bridge.live_profile().message_reply(),
            MessageReplyMode::Text,
            "text must survive the live reload instead of being coerced back to markdown"
        );

        let persisted = larkline_core::store::load_root(&paths.config_file)
            .unwrap()
            .expect("root config");
        let prefs = persisted
            .profile("test")
            .expect("profile")
            .preferences
            .clone()
            .expect("preferences");
        assert_eq!(prefs.message_reply, Some(MessageReplyMode::Text));
        assert_eq!(prefs.message_reply_migrated, Some(true));
    }

    #[tokio::test]
    async fn stop_command_trims_an_explicit_target() {
        let root = tempfile::tempdir().unwrap();
        let mut profile = profile();
        profile.access = Some(Access {
            admins: vec!["ou_admin".into()],
            ..Access::default()
        });
        let bridge = test_bridge(root.path(), profile);

        let text = "/stop   comment:missing  ";
        let reply = bridge
            .handle_command(&message("om_1", "ou_admin", "user", text), text)
            .await
            .expect("handled");
        assert_eq!(reply, "未找到正在运行的任务：`comment:missing`。");
    }

    #[test]
    fn scope_belongs_to_chat_accepts_chat_and_topic_scopes_only() {
        assert!(scope_belongs_to_chat("oc_1", "oc_1"));
        assert!(scope_belongs_to_chat("oc_1:omt_a", "oc_1"));
        assert!(!scope_belongs_to_chat("oc_1:omt_a", "oc_2"));
        assert!(!scope_belongs_to_chat("oc_2", "oc_1"));
        assert!(!scope_belongs_to_chat("oc_10", "oc_1"));
        assert!(!scope_belongs_to_chat("comment:abcdef", "oc_1"));
    }

    // ---- additional callback contracts ----------------------------------

    #[tokio::test]
    async fn legacy_claude_callback_markers_are_ignored() {
        let root = tempfile::tempdir().unwrap();
        let bridge = test_bridge(root.path(), callback_profile("s3cret"));

        let event = card_action_event(json!({
            "__claude_cb": true,
            "cmd": "stop"
        }));
        assert!(bridge.handle_card_action(&event).await.is_none());
        assert!(take_batch(&bridge).is_empty());
    }

    #[tokio::test]
    async fn unsigned_bridge_button_values_are_ignored() {
        let root = tempfile::tempdir().unwrap();
        let bridge = test_bridge(root.path(), profile());
        let token = bridge.tokens.sign("oc_1", "stop");
        // The bridge marker is mandatory; a token alone is not a contract.
        let event = card_action_event(json!({
            "action": "stop",
            "scope": "oc_1",
            "token": token
        }));
        assert!(bridge.handle_card_action(&event).await.is_none());
        assert!(take_batch(&bridge).is_empty());
    }

    #[tokio::test]
    async fn bridge_button_token_is_bound_to_the_action() {
        let root = tempfile::tempdir().unwrap();
        let bridge = test_bridge(root.path(), profile());
        let token = bridge.tokens.sign("oc_1", "stop");
        let event = card_action_event(json!({
            "__bridge_cb": true,
            "action": "resume",
            "scope": "oc_1",
            "token": token
        }));
        assert_eq!(
            bridge.handle_card_action(&event).await.as_deref(),
            Some("操作已失效，请重新发起。")
        );
    }

    #[tokio::test]
    async fn agent_callback_without_a_token_is_dropped() {
        let root = tempfile::tempdir().unwrap();
        let bridge = test_bridge(root.path(), callback_profile("s3cret"));

        let event = card_action_event(json!({
            "__bridge_cb": true,
            "bridge_token": "",
            "choice": "a"
        }));
        assert!(bridge.handle_card_action(&event).await.is_none());
        assert!(take_batch(&bridge).is_empty());
    }

    #[tokio::test]
    async fn agent_callback_without_chat_or_operator_context_is_dropped() {
        let root = tempfile::tempdir().unwrap();
        let bridge = test_bridge(root.path(), callback_profile("s3cret"));
        let token = sign_callback("s3cret", "nonce_no_context");

        let mut without_chat = card_action_event(json!({
            "__bridge_cb": true,
            "bridge_token": token,
            "choice": "a"
        }));
        without_chat.card_context = None;
        assert!(bridge.handle_card_action(&without_chat).await.is_none());

        let mut without_operator = card_action_event(json!({
            "__bridge_cb": true,
            "bridge_token": sign_callback("s3cret", "nonce_no_operator"),
            "choice": "a"
        }));
        without_operator.operator.open_id = None;
        assert!(bridge.handle_card_action(&without_operator).await.is_none());
        assert!(take_batch(&bridge).is_empty());
    }

    #[tokio::test]
    async fn agent_callback_is_dropped_for_a_denied_operator() {
        let root = tempfile::tempdir().unwrap();
        let mut profile = callback_profile("s3cret");
        profile.access = Some(Access {
            allowed_users: vec!["ou_allowed".into()],
            ..Access::default()
        });
        let bridge = test_bridge(root.path(), profile);
        let token = sign_callback("s3cret", "nonce_denied_operator");

        let event = card_action_event(json!({
            "__bridge_cb": true,
            "bridge_token": token,
            "choice": "a"
        }));
        assert!(bridge.handle_card_action(&event).await.is_none());
        assert!(
            take_batch(&bridge).is_empty(),
            "a denied click must never be queued for the agent"
        );
    }
}
