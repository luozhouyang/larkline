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
//! next batch starts. `/stop` interrupts the live run and drops the queued
//! batch. The run flow itself (`run_batch`) is cooperative: the stop signal, the
//! idle watchdog, the 30-minute wall-clock safety net and the 8.5-minute
//! progress-card pause are all waited on with `tokio::select!`, and an
//! interrupt always reaches `finalize`.
//!
//! ## Idle watchdog
//!
//! A run is stopped when the agent stays silent for the configured idle
//! timeout (port of `runIdleTimeoutMs` / `/timeout` in `src/bot/channel.ts`):
//! the per-scope `/timeout` override (`0` = off) wins over
//! `preferences.runIdleTimeoutMinutes` (clamped 1-120, absent/0 = off). The
//! timer re-arms on every agent event and pauses while any `tool_use` id has
//! no matching `tool_result` — a long-running tool (for example an OAuth flow
//! blocked on a browser) is legitimate agent silence, not a hang. On expiry
//! the agent child is killed, the partial answer is kept and the card reports
//! `⏱ N 分钟无响应,已自动终止` (TS `markIdleTimeout`). TypeScript has no wall
//! clock at all; the Rust bridge keeps [`RUN_TIMEOUT`] only as a final safety
//! net for runs where the watchdog is disabled, so an actively streaming run
//! is never cut at 30 minutes when a timeout is configured.
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
//! IM runs are interrupted cooperatively: `stop_scope` sets the run's
//! [`RunInterrupt`], the loop kills the agent child, keeps the partial answer,
//! finalizes the progress card with `⏹ 已被中断` and the run guard then removes
//! the `active` entry and releases the global permit. A 5-second abort watchdog
//! covers a hung finalize. Comment and meeting runs keep the immediate abort
//! path (their own loops own the agent); their guard and permit cleanup is
//! unchanged.
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
//! `/config set`, `/cd`, `/ws` and `/meeting` use a shared admin gate
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
use std::future::Future;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex as StdMutex};
use std::time::{Duration, Instant};

use anyhow::Context as _;
use async_trait::async_trait;
use lark_channel::event::CardActionEvent;
use lark_channel::message::{
    MessageChatType, MessageSenderInfo, MessageSenderType, NormalizedMessage, Recipient,
};
use larkline_agent::{
    AgentEvent, PermissionResponse, PromptContext, PromptMention, TerminationReason,
    build_agent_prompt_with_attachments,
};
use larkline_core::{
    AppPaths, CoreError, MessageReplyMode, Preferences, ProfileConfig, SessionStore, TenantBrand,
    WorkspaceStore,
    config::{ProfileMode, ReasoningDisplay},
};
use larkline_lark::cot::{CotClient, CotTransport};
use larkline_lark::reactions::TYPING;
use larkline_lark::{CARD_SAFE_AGE, Channel, RestClient, STREAMING_ELEMENT_ID};
use serde_json::{Value, json};
use tokio::sync::{Mutex, Notify};
use tokio::task::{AbortHandle, JoinHandle};

use crate::agent_runner::{AgentProcess, AgentSpec};
use crate::callback::{CallbackAuth, CallbackExpected, CallbackTokens};
use crate::cot::{COT_DEGRADED_NOTICE, CotPlan, CotRun, CotRunParams};
use crate::media::{
    AttachmentDecision, AttachmentKind, MediaCache, MediaPolicy, NormalizedAttachment,
    to_prompt_attachments,
};
use crate::meeting::{self, AgentSettings, MeetingHandle, MeetingHost, MeetingManager};
use crate::owner::{self, OwnerGate, RestOwnerSource};
use crate::pending::{PendingQueue, QueueLimits};
use crate::pending_store::PendingStore;
use crate::permissions::RunPermissions;
use crate::policy;
use crate::render::{
    ApprovalCardInput, Block, CardRenderOptions, MAX_CARD_TEXT_CHARS, RunView, approval_card,
    approval_decided_card, card_status, card_summary, render_card,
};
use crate::reply;
use crate::run_limit::{RunLimiter, RunPermit};
use crate::text_render::{TextFormat, TextRenderOptions, render_text, render_text_without_footer};
use crate::transport::{BridgeStream, BridgeTransport, ReactionTransport};

/// Card updates are throttled; Feishu rate-limits card writes and a token
/// stream would otherwise produce dozens of updates per second.
const CARD_UPDATE_INTERVAL: Duration = Duration::from_millis(900);
/// Hard wall-clock cap for one agent run.
///
/// TypeScript has no wall clock: it relies on the idle watchdog alone. The
/// Rust bridge keeps this cap only as a final safety net when the watchdog is
/// disabled for the run (no `/timeout` override and no
/// `preferences.runIdleTimeoutMinutes`); with the watchdog enabled the run is
/// governed by agent activity instead, so a long but active run is not cut at
/// 30 minutes. Comment and meeting runs (which have their own loops) still use
/// it as their only cap.
pub(crate) const RUN_TIMEOUT: Duration = Duration::from_secs(30 * 60);
/// How long a comment / meeting run waits for a global run slot before it
/// gives up. Those runs have no pending queue, so waiting forever would leave
/// the user without an answer; the queue-driven IM path still waits
/// indefinitely because its messages stay queued.
pub(crate) const RUN_SLOT_WAIT: Duration = Duration::from_secs(30);
/// User-visible result when [`RUN_SLOT_WAIT`] expires.
pub(crate) const RUN_SLOT_TIMEOUT_REPLY: &str =
    "⏳ 当前并发任务已达上限，等待空闲运行槽超时，请稍后重试。";
/// Quiet window before a batch of messages starts a run. The TypeScript bridge
/// uses the same 600ms (`DEBOUNCE_MS` in `src/bot/channel.ts`).
const DEBOUNCE: Duration = Duration::from_millis(600);
/// Last-resort abort if a cooperatively interrupted run has not finished its
/// finalize within this window. Normal `/stop` always goes through finalize;
/// this only covers a hung agent kill or card write.
#[cfg(not(test))]
const STOP_WATCHDOG: Duration = Duration::from_secs(5);
/// Unit tests must not sleep for the production watchdog window; the behaviour
/// (abort only while the same run is still registered) is identical.
#[cfg(test)]
const STOP_WATCHDOG: Duration = Duration::from_millis(50);
/// Final frame written when a progress card reaches [`CARD_SAFE_AGE`] while the
/// run is still live (port of TS `STREAM_CARD_PAUSE_NOTE`). The answer is then
/// delivered as a separate message/card when the run finishes.
const STREAM_PAUSED_NOTE: &str = "⏳ 任务仍在运行，过程消息已停止更新；完成后会单独发送最终答案。";
/// Final note on a progress card whose safe age elapsed while the run is
/// still live: the next card continues the same run.
const STREAM_ROLLOVER_NOTE: &str = "⏭ 过程已接续到下一张卡片。";
/// First line of a continuation card.
const STREAM_CONTINUATION_NOTE: &str = "↪ 接续上一张卡片。";
/// Upper bound on messages queued per scope.
///
/// The TypeScript queue is unbounded; this is a Rust-only memory guard for a
/// bridge whose runs may last up to [`RUN_TIMEOUT`]. On overflow the oldest
/// message is dropped (the newest user intent survives) and a warning is
/// logged; [`QueueLimits::unbounded`] restores exact TS semantics.
const MAX_PENDING_PER_SCOPE: usize = 100;
/// How long a permission prompt waits for a user decision before the bridge
/// denies it automatically. The approval card is a separate non-streaming
/// message, so this is unrelated to the 8.5-minute progress-card window.
const DEFAULT_APPROVAL_TIMEOUT: Duration = Duration::from_secs(10 * 60);

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
    /// Outbound message/card transport. Production wraps the `lark-channel`
    /// [`Channel`]; tests inject a fake so the run flow stays offline.
    pub transport: Arc<dyn BridgeTransport>,
    /// Authenticated REST client for comments, COT, meetings and owner lookup.
    rest: RestClient,
    /// Working-reaction calls. Production reuses [`Bridge::rest`]; tests inject
    /// a no-op so a run never touches the network.
    reaction_transport: Arc<dyn ReactionTransport>,
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
    /// Inbound attachment downloader + bounded on-disk cache
    /// (`<profile>/media/`). Downloads are best-effort and never fail a run.
    media: MediaCache,
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
    /// Per-scope session generation. `/new`, `/cd` and other session resets
    /// bump it, and a finishing run only persists its session when the
    /// generation it started with is still current. This stops a slow run from
    /// resurrecting the session `/new` just cleared.
    run_generations: StdMutex<HashMap<String, u64>>,
    /// In-meeting agent runtime; `Some` only while `extra.meeting.enabled` is
    /// on and [`Bridge::start_meeting`] ran inside a Tokio runtime.
    meeting: StdMutex<Option<MeetingRuntime>>,
    /// Transport for COT process messages (`im/v1/message_cot`). The default
    /// wraps the channel's REST client; tests inject a fake so no COT call
    /// ever reaches the network.
    cot_transport: Arc<dyn CotTransport>,
    /// Wall-clock length of one configured idle-timeout minute.
    ///
    /// Production always uses [`Duration::from_secs(60)`]; the run-flow
    /// integration tests set a short unit so the watchdog can be exercised
    /// deterministically without waiting a real minute.
    idle_timeout_unit: Duration,
    /// How long one progress card stays updatable before it is rolled over
    /// (or paused). Production uses [`CARD_SAFE_AGE`]; run-flow tests shorten
    /// it to exercise the rollover without waiting 8.5 minutes.
    card_safe_age: Duration,
    /// Durable copy of the pending queue. Written through on every queue
    /// mutation so a process restart (or a crash followed by the service
    /// manager's restart) does not silently lose queued messages.
    pending_store: PendingStore,
    /// Messages restored from `pending.json` at startup, for diagnostics.
    restored_pending: AtomicUsize,
    /// How long one permission prompt waits for a user decision.
    approval_timeout: Duration,
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
    /// Cooperative stop signal for runs driven by [`Bridge::run_batch`].
    ///
    /// `None` for tracked runs (comment / meeting) that own their own loop and
    /// cancellation semantics; those keep the historic immediate abort.
    interrupt: Option<Arc<RunInterrupt>>,
    /// Pending permission prompts for an IM run. `None` for comment/meeting
    /// runs, which have no card approval flow.
    approvals: Option<Arc<ApprovalBroker>>,
}

/// One click on an approval card.
#[derive(Debug, Clone, PartialEq, Eq)]
struct ApprovalDecision {
    request_id: String,
    allow: bool,
    by: String,
}

/// The bridge side of one IM run's approval queue.
///
/// The run loop owns the receiver; this broker is registered in
/// [`Bridge::active`] so [`Bridge::handle_card_action`] can deliver a decision
/// without knowing the run task.
struct ApprovalBroker {
    run_id: u64,
    operator: String,
    tx: tokio::sync::mpsc::UnboundedSender<ApprovalDecision>,
}

/// A permission request the run loop is waiting on.
struct PendingApproval {
    request_id: String,
    tool_name: String,
    input: Value,
    description: Option<String>,
}

/// Result of waiting for one approval decision.
enum ApprovalWait {
    Answered { allow: bool, by: String },
    TimedOut,
    Interrupted,
}

/// Per-run cooperative stop signal.
///
/// `/stop` and the card stop button flip the flag and wake the run loop, so
/// the run can kill its agent child, keep the partial answer and run
/// [`Bridge::finalize`] (terminal card / partial text). The abort handle stays
/// as a watchdog for a finalize that does not make progress.
#[derive(Debug, Default)]
struct RunInterrupt {
    requested: AtomicBool,
    notify: Notify,
}

impl RunInterrupt {
    fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    fn request(&self) {
        self.requested.store(true, Ordering::SeqCst);
        // `notify_one` stores a permit when the run loop has not registered a
        // waiter yet, so a stop that races the wait preamble can never be lost
        // (`notify_waiters` would only wake already-registered waiters).
        self.notify.notify_one();
    }

    fn is_requested(&self) -> bool {
        self.requested.load(Ordering::SeqCst)
    }

    /// Resolve once an interrupt was requested (including before this call).
    async fn wait(&self) {
        let notified = self.notify.notified();
        if self.is_requested() {
            return;
        }
        notified.await;
    }
}

/// How the progress message is rendered for a reply mode.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CardFormat {
    /// Full interactive card: answer, tool panel, footer and stop button.
    Rich,
    /// Lightweight streaming markdown card: one markdown element, no controls.
    Markdown,
}

impl CardFormat {
    fn for_reply_mode(mode: MessageReplyMode) -> Option<Self> {
        match mode {
            MessageReplyMode::Card => Some(Self::Rich),
            MessageReplyMode::Markdown => Some(Self::Markdown),
            MessageReplyMode::Text => None,
        }
    }
}

/// One tick of the agent run loop.
#[derive(Debug)]
enum LoopTick {
    /// A batch of translated agent events.
    Events(Vec<AgentEvent>),
    /// The agent closed stdout.
    Eof,
    /// The user asked to stop the run.
    Interrupted,
    /// The 8.5-minute progress-card safe age elapsed.
    PauseElapsed,
    /// The idle watchdog expired (no agent activity for the configured
    /// timeout and no tool call in flight).
    IdleTimeout,
    /// [`RUN_TIMEOUT`] elapsed; only armed when the idle watchdog is disabled.
    TimedOut,
}

/// The agent event source the run loop drains.
///
/// [`AgentProcess`] is the production implementation; tests use a fake source
/// to exercise the cooperative interrupt and pause decisions without an agent
/// binary or a network.
#[async_trait]
trait EventSource: Send {
    async fn next_events(&mut self) -> Option<Vec<AgentEvent>>;
}

#[async_trait]
impl EventSource for AgentProcess {
    async fn next_events(&mut self) -> Option<Vec<AgentEvent>> {
        AgentProcess::next_events(self).await
    }
}

/// Wait for the next event while honouring the interrupt, the run deadline,
/// the progress-card pause deadline and the idle watchdog.
///
/// `pending` is the pinned `next_events()` future, created by the caller and
/// kept across timer ticks. It must not be recreated when a timer wins:
/// dropping a partially-read line would lose part of a JSON event at the
/// 8.5-minute boundary (or on an idle re-arm).
///
/// `deadline` is `None` while the idle watchdog is enabled (the watchdog
/// replaces the wall clock, like TS; see [`RUN_TIMEOUT`]). `pause_deadline` is
/// `None` once the pause frame was written, so a paused run never writes a
/// second frame. `idle_deadline` is `None` while a tool call is in flight or
/// the watchdog is disabled. `biased` makes a pending stop win over a stream
/// event that arrived in the same poll.
async fn next_tick(
    pending: &mut Pin<Box<dyn Future<Output = Option<Vec<AgentEvent>>> + Send + '_>>,
    interrupt: &RunInterrupt,
    deadline: Option<tokio::time::Instant>,
    pause_deadline: Option<tokio::time::Instant>,
    idle_deadline: Option<tokio::time::Instant>,
) -> LoopTick {
    if interrupt.is_requested() {
        return LoopTick::Interrupted;
    }
    let events = async {
        match deadline {
            Some(deadline) => match tokio::time::timeout_at(deadline, &mut *pending).await {
                Err(_) => LoopTick::TimedOut,
                Ok(None) => LoopTick::Eof,
                Ok(Some(events)) => LoopTick::Events(events),
            },
            None => match (&mut *pending).await {
                None => LoopTick::Eof,
                Some(events) => LoopTick::Events(events),
            },
        }
    };
    tokio::select! {
        biased;
        _ = interrupt.wait() => LoopTick::Interrupted,
        _ = wait_until_optional(idle_deadline) => LoopTick::IdleTimeout,
        _ = wait_until_optional(pause_deadline) => LoopTick::PauseElapsed,
        tick = events => tick,
    }
}

/// Resolve at `deadline`, or never when the timer is not armed.
///
/// `tokio::select!` has no optional branches; a pending future models an
/// unarmed timer without a second select variant for every combination.
async fn wait_until_optional(deadline: Option<tokio::time::Instant>) {
    match deadline {
        Some(at) => tokio::time::sleep_until(at).await,
        None => std::future::pending().await,
    }
}

/// Resolve this run's idle watchdog, in minutes.
///
/// Port of the resolution in `src/bot/channel.ts` (the `scopeOverride` block):
/// the per-scope `/timeout` override wins over
/// `preferences.runIdleTimeoutMinutes`. `0` disables the watchdog for either
/// source; the preference is clamped to 1-120 exactly like
/// `getRunIdleTimeoutMs`. A hand-edited sessions file can carry an override
/// above 120; TS does not clamp that value either, so it is kept as written
/// and the deadline computation saturates instead of panicking.
fn resolve_idle_timeout_minutes(
    scope_override: Option<u64>,
    preference: Option<u64>,
) -> Option<u64> {
    match scope_override {
        Some(0) => None,
        Some(minutes) => Some(minutes),
        None => preference
            .filter(|minutes| *minutes > 0)
            .map(|minutes| minutes.clamp(1, 120)),
    }
}

/// Convert configured idle-timeout minutes into a wall-clock duration.
///
/// `unit` is one minute in production (`60s`); the run-flow integration tests
/// use a short unit so a one-minute scope override expires in tens of
/// milliseconds while the rendered notice still reports the configured
/// minutes. An absurd hand-edited value saturates instead of panicking.
fn idle_timeout_duration(minutes: Option<u64>, unit: Duration) -> Option<Duration> {
    minutes.map(|minutes| unit.saturating_mul(u32::try_from(minutes).unwrap_or(u32::MAX)))
}

/// The wall-clock deadline for a run.
///
/// TS has no wall clock, so the 30-minute cap is armed only when the idle
/// watchdog is disabled; otherwise the idle deadline governs the run.
fn wall_clock_deadline(
    idle_minutes: Option<u64>,
    now: tokio::time::Instant,
) -> Option<tokio::time::Instant> {
    idle_minutes.is_none().then(|| now + RUN_TIMEOUT)
}

/// Per-run idle watchdog (port of the `armOrPauseIdle` closure in TS
/// `processAgentStream`).
///
/// The timer re-arms after every agent event as long as no tool call is in
/// flight. `tool_use` opens a window and the matching `tool_result` closes it;
/// while any tool id is unmatched the watchdog is paused, because a long
/// tool call (an OAuth flow waiting on the user, a slow build) produces no
/// agent events but is not a hang. Once the last `tool_result` drains the set,
/// the next event re-arms the timer. `None` minutes disables the watchdog.
#[derive(Debug)]
struct IdleWatchdog {
    /// Configured idle timeout in minutes; `None` = off.
    minutes: Option<u64>,
    /// The same timeout as a wall-clock duration.
    timeout: Option<Duration>,
    /// `tool_use` ids without a matching `tool_result` yet.
    in_flight_tools: HashSet<String>,
    /// When the watchdog would fire; `None` while paused or disabled.
    deadline: Option<tokio::time::Instant>,
}

impl IdleWatchdog {
    fn new(minutes: Option<u64>, unit: Duration, now: tokio::time::Instant) -> Self {
        let mut watchdog = Self {
            minutes,
            timeout: idle_timeout_duration(minutes, unit),
            in_flight_tools: HashSet::new(),
            deadline: None,
        };
        watchdog.rearm(now);
        watchdog
    }

    /// Configured timeout in minutes (`None` when the watchdog is off).
    fn minutes(&self) -> Option<u64> {
        self.minutes
    }

    /// The next expiry instant, or `None` while paused/disabled.
    fn deadline(&self) -> Option<tokio::time::Instant> {
        self.deadline
    }

    /// Fold one agent event into the in-flight tool set, then re-arm.
    ///
    /// This is the exact TS order: the tool set is updated *before* the timer
    /// is re-armed, so a `tool_use` pauses the watchdog immediately and a
    /// `tool_result` that drains the set re-arms it.
    fn observe(&mut self, event: &AgentEvent, now: tokio::time::Instant) {
        match event {
            AgentEvent::ToolUse { id, .. } => {
                self.in_flight_tools.insert(id.clone());
            }
            AgentEvent::ToolResult { id, .. } => {
                self.in_flight_tools.remove(id);
            }
            _ => {}
        }
        self.rearm(now);
    }

    fn rearm(&mut self, now: tokio::time::Instant) {
        self.deadline = if self.in_flight_tools.is_empty() {
            // `checked_add` keeps a hand-edited, absurd override from
            // panicking: an unrepresentable deadline behaves as disabled.
            self.timeout.and_then(|timeout| now.checked_add(timeout))
        } else {
            None
        };
    }
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
    /// Build a bridge.
    ///
    /// `app_secret` is the profile's resolved App Secret. It becomes callback
    /// key version 1 (the same key TypeScript uses), so `lark-cli`-minted
    /// `bridge_token` values verify by default. Explicit `extra.callback.*`
    /// keys or `LARKLINE_CALLBACK_SECRET` still take precedence. Pass `None`
    /// when no secret could be resolved; agent callbacks then fail closed
    /// unless a key is configured explicitly. The value is consumed into the
    /// (redacted) [`CallbackAuth`], where it stays for token verification, and
    /// is never logged as plaintext.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        channel: Channel,
        profile: ProfileConfig,
        agent_binary: PathBuf,
        cwd: PathBuf,
        bot_open_id: Option<String>,
        bot_name: Option<String>,
        paths: AppPaths,
        app_secret: Option<&str>,
    ) -> anyhow::Result<Self> {
        // The production transport is the `lark-channel` channel itself; the
        // media cache reuses its authenticated OpenAPI client.
        let media = MediaCache::for_channel(&paths, &channel);
        let transport: Arc<dyn BridgeTransport> = Arc::new(channel.clone());
        Self::build(
            transport,
            channel.rest().clone(),
            media,
            profile,
            agent_binary,
            cwd,
            bot_open_id,
            bot_name,
            paths,
            app_secret,
        )
    }

    /// Build a bridge with an injected outbound transport and REST client.
    ///
    /// This is the constructor used by the end-to-end run-flow tests: the fake
    /// transport records text/card sends and streaming frames, while the REST
    /// client is never called because the COT transport, attachment downloader
    /// and reaction transport are injected too. `media` owns the attachment
    /// downloader, so callers can start with a failing/offline one.
    #[allow(clippy::too_many_arguments)]
    pub fn new_with_transport(
        transport: Arc<dyn BridgeTransport>,
        rest: RestClient,
        media: MediaCache,
        profile: ProfileConfig,
        agent_binary: PathBuf,
        cwd: PathBuf,
        bot_open_id: Option<String>,
        bot_name: Option<String>,
        paths: AppPaths,
        app_secret: Option<&str>,
    ) -> anyhow::Result<Self> {
        Self::build(
            transport,
            rest,
            media,
            profile,
            agent_binary,
            cwd,
            bot_open_id,
            bot_name,
            paths,
            app_secret,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn build(
        transport: Arc<dyn BridgeTransport>,
        rest: RestClient,
        media: MediaCache,
        profile: ProfileConfig,
        agent_binary: PathBuf,
        cwd: PathBuf,
        bot_open_id: Option<String>,
        bot_name: Option<String>,
        paths: AppPaths,
        app_secret: Option<&str>,
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

        // Restore messages queued by the previous process. The access policy is
        // re-checked so a user removed from the allowlist cannot replay a
        // prompt, and a corrupt/expired file is treated as empty.
        let pending_store = PendingStore::new(paths.pending_file.clone());
        let loaded_pending = pending_store.load(chrono::Utc::now().timestamp_millis());
        let mut pending =
            PendingQueue::with_limits(DEBOUNCE, QueueLimits::per_scope(MAX_PENDING_PER_SCOPE));
        let mut restored_pending = 0usize;
        let mut restored_denied = 0usize;
        for (scope, messages) in loaded_pending.scopes {
            let kept: Vec<NormalizedMessage> = messages
                .into_iter()
                .filter(|message| {
                    if policy::allowed(&profile, message, bot_open_id.as_deref()) {
                        true
                    } else {
                        restored_denied += 1;
                        false
                    }
                })
                .collect();
            restored_pending += kept.len();
            pending.restore(&scope, kept, Instant::now());
        }
        if restored_pending > 0
            || restored_denied > 0
            || loaded_pending.stats.corrupt
            || loaded_pending.stats.expired > 0
            || loaded_pending.stats.invalid > 0
        {
            tracing::info!(
                path = %paths.pending_file.display(),
                restored = restored_pending,
                denied = restored_denied,
                expired = loaded_pending.stats.expired,
                invalid = loaded_pending.stats.invalid,
                corrupt = loaded_pending.stats.corrupt,
                "pending queue restored"
            );
        }

        let mut callback_auth = CallbackAuth::from_profile_and_app_secret(&profile, app_secret);
        if let Some(auth) = callback_auth.as_mut() {
            // Durable replay protection (TS `CallbackNonceStore`): load the
            // used/revoked nonces persisted by a previous run, then write
            // every acceptance through before verify returns.
            let loaded = auth.load_nonce_store(paths.callback_nonces_file.clone());
            tracing::info!(
                path = %paths.callback_nonces_file.display(),
                loaded,
                "callback nonce store ready"
            );
        }
        match callback_auth.as_ref() {
            Some(auth) => tracing::info!(
                key_versions = ?auth.key_versions(),
                "agent card callbacks enabled"
            ),
            None => tracing::info!(
                "agent card callbacks are disabled: no callback key configured \
                 (profile extra.callback.secret, {}, or the profile App Secret)",
                crate::callback::CALLBACK_SECRET_ENV
            ),
        }
        let live_profile = profile.clone();
        let run_limit = Arc::new(RunLimiter::new(profile.max_concurrent_runs()));
        tracing::info!(cap = run_limit.cap(), "global agent run concurrency cap");
        // The owner lookup reuses the bridge's tenant token cache. It stays
        // lazy: no request is made until an admin-gated command needs it.
        let owner = OwnerGate::new(Arc::new(RestOwnerSource::new(rest.clone())));
        // COT messages reuse the same tenant token cache as every other REST
        // call; the transport is injectable so tests stay offline.
        let cot_transport: Arc<dyn CotTransport> = Arc::new(CotClient::new(rest.clone()));
        let reaction_transport: Arc<dyn ReactionTransport> = Arc::new(rest.clone());

        Ok(Self {
            transport,
            rest,
            reaction_transport,
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
            media,
            active: StdMutex::new(HashMap::new()),
            pending: StdMutex::new(pending),
            pending_wakeup: Notify::new(),
            pending_driver_started: AtomicBool::new(false),
            next_run_id: AtomicU64::new(1),
            run_generations: StdMutex::new(HashMap::new()),
            meeting: StdMutex::new(None),
            cot_transport,
            idle_timeout_unit: Duration::from_secs(60),
            card_safe_age: CARD_SAFE_AGE,
            pending_store,
            restored_pending: AtomicUsize::new(restored_pending),
            approval_timeout: DEFAULT_APPROVAL_TIMEOUT,
        })
    }

    /// Override the progress-card safe age.
    ///
    /// Production keeps the crate's 8.5-minute guard. This exists for the
    /// run-flow integration tests, which force a rollover without waiting the
    /// real interval; it is also the knob a tenant with a shorter CardKit
    /// lifetime should use.
    pub fn with_card_safe_age(mut self, safe_age: Duration) -> Self {
        self.card_safe_age = safe_age;
        self
    }

    /// Override the wall-clock length of one idle-timeout minute.
    ///
    /// Production keeps the default of 60 seconds. This exists for the
    /// run-flow integration tests: a `/timeout 1` scope override then expires
    /// after `unit` instead of after a real minute, while the rendered notice
    /// still reports the configured minutes.
    pub fn with_idle_timeout_unit(mut self, unit: Duration) -> Self {
        self.idle_timeout_unit = unit;
        self
    }

    /// Override how long an approval card waits for a decision.
    ///
    /// Production uses ten minutes; tests shorten it to exercise the automatic
    /// denial without waiting.
    pub fn with_approval_timeout(mut self, timeout: Duration) -> Self {
        self.approval_timeout = timeout;
        self
    }

    /// Replace the working-reaction transport.
    ///
    /// Production reuses the REST client created by [`Bridge::new`]; tests
    /// inject a no-op so a run never calls the reaction endpoints.
    pub fn with_reaction_transport(mut self, transport: Arc<dyn ReactionTransport>) -> Self {
        self.reaction_transport = transport;
        self
    }

    /// Authenticated REST client shared by comments, COT, meetings and the
    /// owner lookup. Comment runs also use it directly for in-thread replies.
    pub(crate) fn rest(&self) -> &RestClient {
        &self.rest
    }

    /// Replace the COT transport.
    ///
    /// Production uses the REST-backed [`CotClient`] created in
    /// [`Bridge::new`]; tests inject a fake so COT publishing never touches
    /// the network.
    pub fn with_cot_transport(mut self, transport: Arc<dyn CotTransport>) -> Self {
        self.cot_transport = transport;
        self
    }

    /// Replace the attachment downloader.
    ///
    /// Production uses the SDK-backed downloader created in [`Bridge::new`];
    /// tests inject a fake so the attachment path never touches the network.
    pub fn with_attachment_downloader(
        mut self,
        downloader: Arc<dyn crate::media::ResourceDownloader>,
    ) -> Self {
        self.media = self.media.with_downloader(downloader);
        self
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
            Arc::new(meeting::RestMeetingApi::new(self.rest().clone()));
        let host: Arc<dyn MeetingHost> = Arc::new(BridgeMeetingHost {
            bridge: Arc::downgrade(self),
        });
        // The CLI routes `vc.bot.*` frames off its long connection, so the push
        // lane is wired; `received` only moves once a frame actually arrives.
        let agent = AgentSettings {
            kind: profile.agent_kind,
            binary: self.agent_binary.clone(),
            permissions: RunPermissions::from_profile(&profile),
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

    /// Resolved application paths (config/profile files) for command handlers.
    pub(crate) fn app_paths(&self) -> &AppPaths {
        &self.paths
    }

    /// Live workspace store. Commands mutate it through the store API so the
    /// change is persisted immediately; runs read the same store.
    pub(crate) fn workspace_store(&self) -> &Mutex<WorkspaceStore> {
        &self.workspaces
    }

    /// Scopes with a live run, sorted. Used by `/ps` and `/status`.
    pub(crate) fn active_scope_list(&self) -> Vec<String> {
        let mut scopes: Vec<String> = lock(&self.active)
            .iter()
            .filter(|(_, run)| !run.abort.is_finished())
            .map(|(scope, _)| scope.clone())
            .collect();
        scopes.sort();
        scopes
    }

    /// Scopes with a queued (not yet running) batch, sorted, with counts.
    pub(crate) fn queued_scope_list(&self) -> Vec<(String, usize)> {
        lock(&self.pending).queued_scopes()
    }

    /// Interrupt the run registered under `scope`, without touching its pending
    /// queue. `/resume` uses this: the next message should start in the
    /// resumed session, but messages already queued in this scope stay.
    ///
    /// Cooperative for IM runs (they finalize the current card); comment and
    /// meeting runs keep their immediate abort.
    pub(crate) fn interrupt_scope(self: &Arc<Self>, scope: &str) -> bool {
        self.request_stop(scope).0
    }

    /// Last-known runtime-owner cache state, without triggering a lookup.
    pub(crate) async fn owner_state(&self) -> owner::OwnerSnapshot {
        self.owner.snapshot().await
    }

    /// Shared global run limiter. Comment and meeting runs use this to take a
    /// slot with [`RunLimiter::acquire_within`].
    pub(crate) fn run_limiter(&self) -> &Arc<RunLimiter> {
        &self.run_limit
    }

    /// Current profile snapshot: the startup profile, replaced by
    /// [`Bridge::save_preference`] after `/config set` writes `config.json`.
    pub(crate) fn live_profile(&self) -> ProfileConfig {
        lock(&self.live_profile).clone()
    }

    /// Idle watchdog for a run scope, in minutes.
    ///
    /// The per-scope `/timeout` override (persisted in `sessions.json`) wins
    /// over the live profile's `preferences.runIdleTimeoutMinutes`. `0` from
    /// either source disables the watchdog. Resolved once per run, so a
    /// `/timeout` issued while a run is live applies from the next run —
    /// exactly like the TypeScript bridge, which computes `idleTimeoutMs` when
    /// the run starts.
    async fn run_idle_timeout_minutes(&self, scope: &str) -> Option<u64> {
        let scope_override = self.sessions.lock().await.idle_timeout_minutes(scope);
        let preference = self
            .live_profile()
            .preferences
            .as_ref()
            .and_then(|preferences| preferences.run_idle_timeout_minutes);
        resolve_idle_timeout_minutes(scope_override, preference)
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
    pub(crate) async fn can_run_admin_command(&self, sender_id: &str) -> bool {
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
    pub(crate) fn reload_profile(&self) -> Result<(), String> {
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
                        &*bridge.transport,
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
                &*self.transport,
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
    pub(crate) fn enqueue(self: &Arc<Self>, message: NormalizedMessage) {
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
        self.persist_pending();
        self.ensure_pending_driver();
        self.pending_wakeup.notify_one();
    }

    /// Start the queue driver when a persisted batch was restored at startup.
    ///
    /// [`Bridge::new`] restores the messages eagerly (so `/ps` and `/status`
    /// see them immediately) but does not spawn a task in a constructor. The
    /// CLI calls this once the bridge is inside its runtime.
    pub fn resume_pending(self: &Arc<Self>) {
        if lock(&self.pending).queued_scopes().is_empty() {
            return;
        }
        self.ensure_pending_driver();
        self.pending_wakeup.notify_one();
        tracing::info!(
            restored = self.restored_pending.load(Ordering::Relaxed),
            "resuming the persisted pending queue"
        );
    }

    /// Messages restored from the previous process, for diagnostics.
    pub fn restored_pending_count(&self) -> usize {
        self.restored_pending.load(Ordering::Relaxed)
    }

    /// Write the in-memory queue through to disk.
    ///
    /// Best effort: a failed write is logged and must never fail the run or
    /// the inbound message that triggered it.
    fn persist_pending(&self) {
        let snapshot = lock(&self.pending).snapshot();
        if let Err(err) = self
            .pending_store
            .save(&snapshot, chrono::Utc::now().timestamp_millis())
        {
            tracing::warn!(error = %err, "cannot persist the pending queue");
        }
    }

    /// Queue a message and block its scope so no run starts.
    ///
    /// Test-only fixture for `/ps`: a blocked batch is exactly the state a
    /// message reaches while another run owns the scope, and unlike a plain
    /// [`Bridge::enqueue`] it cannot race the 600ms driver timer.
    #[cfg(test)]
    pub(crate) fn queue_blocked_for_test(self: &Arc<Self>, message: NormalizedMessage) {
        let scope = scope_key(&message);
        let mut pending = lock(&self.pending);
        pending.block(&scope);
        pending.push(&scope, message, Instant::now());
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
        let interrupt = RunInterrupt::new();
        let run_interrupt = Arc::clone(&interrupt);
        // Approval decisions are delivered to the run task through this
        // channel; the broker is registered in `active` for card clicks.
        let operator = batch
            .first()
            .map(message_sender_id)
            .unwrap_or_default()
            .to_string();
        let (approval_tx, approval_rx) = tokio::sync::mpsc::unbounded_channel();
        let approvals = Arc::new(ApprovalBroker {
            run_id: id,
            operator: operator.clone(),
            tx: approval_tx,
        });
        // Snapshot the session generation while the run is still atomically
        // registered: a `/new` that lands after this point bumps the
        // generation and the finishing run must not re-save its session.
        let generation = self.scope_generation(&scope);
        let handle = tokio::spawn(async move {
            let _ = ready.await;
            let _guard = RunGuard {
                bridge: Arc::clone(&bridge),
                scope: task_scope,
                id,
                permit: Some(permit),
            };
            if let Err(err) = bridge
                .run_batch(batch, run_interrupt, generation, id, operator, approval_rx)
                .await
            {
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
                interrupt: Some(interrupt),
                approvals: Some(approvals),
            },
        );
        drop(active);
        drop(pending);
        // The batch is now owned by the run; remove it from the durable queue
        // before the agent starts (at-most-once delivery).
        self.persist_pending();
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
                // Comment/meeting runs own their agent loop and keep the
                // historic immediate abort; there is no shared loop to signal.
                interrupt: None,
                approvals: None,
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

    /// Abort or cooperatively interrupt a live run.
    ///
    /// Runs driven by [`Bridge::run_batch`] carry a [`RunInterrupt`]: the flag
    /// is set and the run loop finalizes normally. Comment/meeting runs have no
    /// shared loop, so they keep the immediate abort semantics.
    ///
    /// The caller must hold the pending lock (lock order is pending -> active)
    /// when it also wants the queue cleanup to be atomic with registration.
    /// Returns `(stopped, cooperative)`.
    fn request_stop(self: &Arc<Self>, scope: &str) -> (bool, bool) {
        let active = lock(&self.active);
        let Some((id, interrupt)) = active
            .get(scope)
            .filter(|run| !run.abort.is_finished())
            .map(|run| (run.id, run.interrupt.clone()))
        else {
            return (false, false);
        };
        drop(active);
        match interrupt {
            Some(signal) => {
                signal.request();
                self.spawn_stop_watchdog(scope.to_string(), id);
                (true, true)
            }
            None => {
                let mut active = lock(&self.active);
                // Re-check the id: a run may have finished and a successor
                // registered between the decision above and this lock.
                if active.get(scope).is_some_and(|run| run.id == id) {
                    if let Some(run) = active.remove(scope) {
                        run.abort.abort();
                    }
                }
                (true, false)
            }
        }
    }

    /// Last-resort abort for a cooperative stop.
    ///
    /// Fires only when the same run is still registered after
    /// [`STOP_WATCHDOG`] — a normal stop has already been removed by its
    /// [`RunGuard`] and must not be touched. This exists for a hung agent kill
    /// or card write during `finalize`, not for the normal path.
    fn spawn_stop_watchdog(self: &Arc<Self>, scope: String, id: u64) {
        let Ok(runtime) = tokio::runtime::Handle::try_current() else {
            tracing::warn!(scope, "no tokio runtime to arm the stop watchdog");
            return;
        };
        let bridge = Arc::clone(self);
        runtime.spawn(async move {
            tokio::time::sleep(STOP_WATCHDOG).await;
            let active = lock(&bridge.active);
            if let Some(run) = active.get(&scope) {
                if run.id == id && !run.abort.is_finished() {
                    tracing::warn!(
                        scope,
                        run_id = id,
                        "cooperative stop did not finalize in time; aborting the run"
                    );
                    run.abort.abort();
                }
            }
        });
    }

    /// Scope-level session generation, bumped by `/new`, `/cd` and other
    /// session resets. A run captures it at claim time and only saves a session
    /// while the generation is unchanged.
    pub(crate) fn scope_generation(&self, scope: &str) -> u64 {
        lock(&self.run_generations).get(scope).copied().unwrap_or(0)
    }

    /// Invalidate session saves from runs that started before a reset.
    pub(crate) fn bump_scope_generation(&self, scope: &str) {
        let mut generations = lock(&self.run_generations);
        let next = generations.get(scope).copied().unwrap_or(0) + 1;
        generations.insert(scope.to_string(), next);
    }

    /// Clear the scope's session and invalidate any in-flight run's save.
    pub(crate) async fn reset_scope_session(&self, scope: &str) -> Result<(), String> {
        self.bump_scope_generation(scope);
        self.sessions
            .lock()
            .await
            .clear(scope)
            .map_err(|err| err.to_string())
    }

    /// `/stop` and the card stop button: abort the run, drop its queued
    /// follow-up batch, and clear the block so the chat can be used again.
    ///
    /// Works for every run kind registered in [`Bridge::active`]: an IM batch
    /// under its chat/topic scope, a comment run under `comment:<hash>`, or a
    /// meeting run under `meeting:<id>`. The pending-queue cleanup is a no-op
    /// for scopes that have no queue.
    pub(crate) fn stop_scope(self: &Arc<Self>, scope: &str) -> (bool, usize) {
        // Holding the pending lock makes stop atomic with respect to a driver
        // that is registering a new batch (same lock order as `start_due_batch`).
        let mut pending = lock(&self.pending);
        let (stopped, cooperative) = self.request_stop(scope);
        let dropped = pending.cancel(scope).len();
        // A cooperative run keeps the scope blocked until its `finalize` ends
        // (`RunGuard::finish_run` unblocks it). Unblocking here would let a new
        // batch replace the active entry while the stopped run is still
        // finalizing, which would also disarm its last-resort watchdog.
        if !cooperative {
            pending.unblock(scope, Instant::now());
        }
        drop(pending);
        if dropped > 0 {
            self.persist_pending();
        }
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
        if dropped > 0 {
            self.persist_pending();
        }
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

        // Commands ported later live in `commands.rs`; it returns `None` for
        // everything still handled inline below (and for unknown slash text,
        // which keeps falling through to the agent).
        if let Some(outcome) = crate::commands::handle(self, message, command, args).await {
            if outcome.effect != crate::commands::CommandEffect::None {
                // Debug level: an access-list effect contains open ids.
                tracing::debug!(
                    command,
                    effect = ?outcome.effect,
                    "command changed bridge state"
                );
            }
            return Some(outcome.into_reply());
        }

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
            "/pwd" => Some(self.handle_pwd(&scope).await),
            "/status" => Some(self.handle_status(&scope).await),
            "/meeting" => Some(self.handle_meeting_command(message, args).await),
            "/config" => Some(self.handle_config(message, args).await),
            "/help" => Some(
                "可用命令：\n\
                 /new 或 /reset 中断当前任务并开启新会话\n\
                 /cd <绝对路径|~/子路径> 切换工作目录（需管理员，会重置 session）\n\
                 /pwd 显示当前工作目录\n\
                 /ws [list|save <name>|use <name>|remove <name>] 命名工作目录别名（/work 同义，需管理员）\n\
                 /resume [use <session-id>] 查看/恢复会话\n\
                 /timeout [<1-120>|off|default] 当前 session 探活（指定 comment scope 需管理员）\n\
                 /status 显示当前状态\n\
                 /config 查看/修改回复偏好（修改需管理员）\n\
                 /doctor 自检（需管理员）\n\
                 /account 当前应用（需管理员）\n\
                 /ps 运行/队列状态（需管理员）\n\
                 /exit 关闭 bot 的 CLI 指引（需管理员）\n\
                 /reconnect 重连指引（需管理员）\n\
                 /doc 云文档评论说明\n\
                 /invite user|admin|group 维护访问名单（需管理员）\n\
                 /remove user|admin|group 维护访问名单（需管理员）\n\
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
        let active_scopes = self.active_scope_list();
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
                name if name.starts_with("approval:") => {
                    let clicker = action.operator.open_id.as_deref().unwrap_or_default();
                    self.handle_approval_action(scope, name, value, clicker)
                        .await
                }
                _ => None,
            };
        }

        None
    }

    /// Handle an allow/deny click on an approval card.
    ///
    /// The generic bridge-button check has already verified the process-local
    /// token against the full `approval:<decision>:<request>:<operator>` action.
    /// Rebuild that action from the payload so the unsigned fields cannot be
    /// swapped after verification, then require the clicker to be the run's
    /// initiator before delivering the decision to the run task.
    async fn handle_approval_action(
        &self,
        scope: &str,
        action_name: &str,
        value: &serde_json::Map<String, Value>,
        clicker: &str,
    ) -> Option<String> {
        let decision = value.get("decision").and_then(Value::as_str)?;
        let request_id = value.get("request_id").and_then(Value::as_str)?;
        let operator = value.get("operator").and_then(Value::as_str)?;
        let run_id = value.get("run_id").and_then(Value::as_u64)?;
        if decision != "allow" && decision != "deny" {
            return None;
        }
        if approval_action(decision, request_id, operator, run_id) != action_name {
            tracing::warn!(
                scope,
                "rejected an approval click whose action fields were changed"
            );
            return Some("操作已失效，请重新发起。".to_string());
        }
        if clicker != operator {
            tracing::info!(scope, clicker, operator, "approval click from another user");
            return Some("只有发起本次任务的人可以处理授权。".to_string());
        }
        let (run_matches, broker) = {
            let active = lock(&self.active);
            let run = active.get(scope);
            (
                run.is_some_and(|run| run.id == run_id),
                run.and_then(|run| run.approvals.clone()),
            )
        };
        let Some(broker) = broker else {
            return Some("该授权请求已失效。".to_string());
        };
        if !run_matches || broker.run_id != run_id {
            return Some("该授权请求已失效。".to_string());
        }
        if broker.operator != operator {
            return Some("只有发起本次任务的人可以处理授权。".to_string());
        }
        if broker
            .tx
            .send(ApprovalDecision {
                request_id: request_id.to_string(),
                allow: decision == "allow",
                by: clicker.to_string(),
            })
            .is_err()
        {
            return Some("该授权请求已失效。".to_string());
        }
        tracing::info!(
            scope,
            request_id,
            decision,
            operator,
            "approval decision accepted"
        );
        Some(if decision == "allow" {
            "已允许，继续执行。".to_string()
        } else {
            "已拒绝。".to_string()
        })
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
    #[cfg(test)]
    async fn save_scope_session(&self, scope: &str, session_key: &str, cwd: &str) {
        if let Err(err) = self.sessions.lock().await.set(scope, session_key, cwd) {
            tracing::warn!(error = %err, "cannot persist session");
        }
    }

    /// Persist a run's session only while its scope generation is current.
    ///
    /// `/new` (and `/cd`, `/ws use`, `/resume`) bumps the generation so a run
    /// that was already streaming when the user reset the session cannot write
    /// it back afterwards. Returns whether the session was saved.
    pub(crate) async fn save_scope_session_if_current(
        &self,
        scope: &str,
        session_key: &str,
        cwd: &str,
        generation: u64,
    ) -> bool {
        if self.scope_generation(scope) != generation {
            tracing::info!(
                scope,
                generation,
                current = self.scope_generation(scope),
                "dropping a session save from a run superseded by a session reset"
            );
            return false;
        }
        if let Err(err) = self.sessions.lock().await.set(scope, session_key, cwd) {
            tracing::warn!(error = %err, "cannot persist session");
            return false;
        }
        true
    }

    async fn run_batch(
        self: Arc<Self>,
        batch: Vec<NormalizedMessage>,
        interrupt: Arc<RunInterrupt>,
        generation: u64,
        run_id: u64,
        operator: String,
        mut approval_rx: tokio::sync::mpsc::UnboundedReceiver<ApprovalDecision>,
    ) -> anyhow::Result<()> {
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
        // Snapshot the effective cwd for the whole run: session resume/record
        // and the agent process must agree even if `/cd` lands mid-run. Topic
        // scopes get their own cwd entry in the workspace store.
        let cwd = self.workspace_cwd(&scope).await;
        let cwd_str = cwd.to_string_lossy().into_owned();
        let resume = self.resume_scope_session(&scope, &cwd_str).await;
        let profile = self.live_profile();

        // Resolve every resource on the batch before spawning: downloads are
        // best-effort (a failed attachment is logged and skipped), and the
        // policy decisions are reported in the prompt either way.
        let attachments = self.resolve_attachments(&batch, &profile).await;
        let prompt = self.build_batch_prompt(&batch, &attachments);
        let reply_mode = profile.message_reply();
        // Idle watchdog for this run (TS `runIdleTimeoutMs`, `/timeout`
        // override first). Resolved once at run start, like TS.
        let idle_minutes = self.run_idle_timeout_minutes(&scope).await;

        // COT process message (TS `cotEnabled || reasoningBubble`). When one is
        // live it *replaces* the progress card: the bubble already streams the
        // same text/tool events, so keeping both would duplicate the whole run
        // (and the card would duplicate reasoning, too).
        let mut cot = CotRun::prepare(
            CotPlan::for_profile(&profile),
            self.cot_transport.clone(),
            CotRunParams {
                chat_id: chat_id.clone(),
                // The bubble inherits this message's thread in a topic; the
                // triggering message of the batch is always in that thread.
                origin_message_id: last_message_id.clone(),
                // TS uses the per-run id (`execution.runId`) in every step/
                // message id it publishes; a fresh UUID keeps them unique.
                run_id: format!("run-{}", uuid::Uuid::new_v4()),
                scope: scope.clone(),
                input_preview: batch
                    .last()
                    .map(|message| message.text.clone())
                    .unwrap_or_default(),
            },
        );
        // A process bubble is created now; a reasoning-only bubble waits for
        // the first reasoning event. A rejected create disables the COT path
        // and the run falls back to the progress card (TS
        // `fallback-existing-reply`).
        if let Some(run) = cot.as_mut() {
            if !run.start().await {
                cot = None;
            }
        }
        let cot_path = cot.is_some();
        let card_format = if cot_path {
            None
        } else {
            CardFormat::for_reply_mode(reply_mode)
        };
        // The TS bridge skips the typing reaction while a COT bubble is the
        // progress surface (the bubble itself is the acknowledgement).
        let _working_reaction = (!cot_path).then(|| {
            WorkingReaction::start(
                Arc::clone(&self.reaction_transport),
                last_message_id.clone(),
            )
        });

        let mut view = RunView::default();
        // After the first rollover the current card renders only the events
        // that happened since the rollover; `view` keeps the full run for the
        // final answer, session and terminal state.
        let mut tail: Option<RunView> = None;
        let mut streaming: Option<Box<dyn BridgeStream>> = None;
        let mut message_id: Option<String> = None;
        if let Some(format) = card_format {
            let card = self.initial_card(&scope, &profile, &view, started, format);
            // Prefer a CardKit 2.0 streaming entity: every later update is one
            // whole-card PUT with its own sequence. If either the entity or the
            // reference message cannot be created, the plain message path below
            // is the fallback (and is the only path when streaming is off).
            match reply::start_streaming_card(
                &*self.transport,
                &scope,
                &chat_id,
                Some(&last_message_id),
                card.clone(),
                STREAMING_ELEMENT_ID,
            )
            .await
            {
                Ok(handle) => {
                    message_id = Some(handle.message_id().to_string());
                    streaming = Some(handle);
                }
                Err(err) => {
                    tracing::warn!(
                        error = %err,
                        "cannot start streaming card; falling back to message card"
                    );
                    match reply::send_card(
                        &*self.transport,
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
            approvals: true,
            binary: self.agent_binary.clone(),
            cwd,
            prompt,
            session_id: resume,
            model: None,
            permissions: RunPermissions::from_profile(&profile),
            images: accepted_image_paths(&attachments),
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
                if let Some(run) = cot.take() {
                    let _ = run.finish(TerminationReason::Failed, Some(&message)).await;
                }
                let cot_final = cot_path.then(|| final_answer_only_view(&view));
                let final_view = cot_final.as_ref().unwrap_or(&view);
                self.finalize(
                    &scope,
                    &chat_id,
                    &mut streaming,
                    message_id.as_deref(),
                    Some(&last_message_id),
                    final_view,
                    reply_mode,
                    started,
                    false,
                )
                .await;
                return Err(err);
            }
        };

        // The 30-minute wall clock is only a safety net for runs without an
        // idle timeout; TS has no wall clock, so an idle watchdog governs a
        // live-but-active run for as long as the agent keeps producing events.
        let now = tokio::time::Instant::now();
        let deadline = wall_clock_deadline(idle_minutes, now);
        let mut watchdog = IdleWatchdog::new(idle_minutes, self.idle_timeout_unit, now);
        // CardKit closes a streaming entity around ten minutes. At the safe
        // age the card is rolled over to a fresh entity; if that cannot be
        // created the old behavior (pause + separate final answer) remains.
        let mut pause_deadline =
            card_format.map(|_| tokio::time::Instant::from_std(started + self.card_safe_age));
        let mut paused = false;
        let mut last_update = Instant::now();
        // Pinned read future, kept alive across pause ticks so an in-flight
        // line is never cancelled mid-read (see `next_tick`).
        let mut pending = EventSource::next_events(&mut process);
        loop {
            let pause_at = if paused { None } else { pause_deadline };
            match next_tick(
                &mut pending,
                &interrupt,
                deadline,
                pause_at,
                watchdog.deadline(),
            )
            .await
            {
                LoopTick::Interrupted => {
                    // Cooperative stop: drop the read (and its borrow) before
                    // killing the child. The partial answer stays in `view`
                    // and the run still goes through `finalize`.
                    drop(pending);
                    process.kill().await;
                    view.mark_interrupted();
                    if let Some(tail) = tail.as_mut() {
                        tail.mark_interrupted();
                    }
                    break;
                }
                LoopTick::IdleTimeout => {
                    // Same cooperative shape as `/stop`: kill the child, keep
                    // the partial answer and render the TS timeout notice.
                    drop(pending);
                    process.kill().await;
                    let minutes = watchdog.minutes().unwrap_or_default();
                    tracing::warn!(scope = %scope, minutes, "agent idle timeout; stopping the run");
                    view.mark_idle_timeout(minutes);
                    if let Some(tail) = tail.as_mut() {
                        tail.mark_idle_timeout(minutes);
                    }
                    break;
                }
                LoopTick::TimedOut => {
                    drop(pending);
                    process.kill().await;
                    let event = AgentEvent::Error {
                        message: "运行超时（30 分钟），已终止。".to_string(),
                        termination_reason: TerminationReason::Timeout,
                    };
                    view.apply(&event);
                    if let Some(tail) = tail.as_mut() {
                        tail.apply(&event);
                    }
                    // The rich-card terminal notice reports the deadline in
                    // minutes (TS `idleTimeoutMinutes`); text/markdown replies
                    // keep using the error message above.
                    view.idle_timeout_minutes = Some(RUN_TIMEOUT.as_secs() / 60);
                    if let Some(tail) = tail.as_mut() {
                        tail.idle_timeout_minutes = Some(RUN_TIMEOUT.as_secs() / 60);
                    }
                    break;
                }
                LoopTick::PauseElapsed => {
                    let Some(format) = card_format else { continue };
                    let render_view = tail.as_ref().unwrap_or(&view);
                    if self
                        .rollover_progress(
                            &scope,
                            &chat_id,
                            &last_message_id,
                            &mut streaming,
                            &mut message_id,
                            render_view,
                            started,
                            format,
                        )
                        .await
                    {
                        // The continuation card starts empty; only events
                        // after this point are rendered into it.
                        tail = Some(RunView {
                            continuation_note: Some(STREAM_CONTINUATION_NOTE.to_string()),
                            ..RunView::default()
                        });
                        paused = false;
                        pause_deadline = Some(tokio::time::Instant::from_std(
                            Instant::now() + self.card_safe_age,
                        ));
                        last_update = Instant::now();
                        tracing::info!(scope = %scope, "progress card rolled over");
                    } else {
                        paused = true;
                        self.pause_progress(
                            &scope,
                            &mut streaming,
                            message_id.as_deref(),
                            render_view,
                            started,
                            format,
                        )
                        .await;
                        last_update = Instant::now();
                    }
                    // `pending` is untouched and is awaited again next tick.
                }
                LoopTick::Eof => {
                    drop(pending);
                    for event in process.finish().await {
                        view.apply(&event);
                        if let Some(tail) = tail.as_mut() {
                            tail.apply(&event);
                        }
                        // The publisher sees the same flushed tail as the view,
                        // so a final text/tool event still reaches the bubble.
                        if let Some(run) = cot.as_mut() {
                            run.observe(&event).await;
                        }
                    }
                    break;
                }
                LoopTick::Events(events) => {
                    drop(pending);
                    for event in &events {
                        // Track tool flight and re-arm the watchdog before
                        // folding the event (the TS `armOrPauseIdle` order).
                        watchdog.observe(event, tokio::time::Instant::now());
                        view.apply(event);
                        if let Some(tail) = tail.as_mut() {
                            tail.apply(event);
                        }
                        // A reasoning-only bubble is created on first use; the
                        // create is bounded by `COT_REQUEST_TIMEOUT` inside the
                        // publisher, so it cannot pin the event loop for long.
                        if let Some(run) = cot.as_mut() {
                            run.observe(event).await;
                        }
                    }
                    // A permission request blocks the agent until the user
                    // answers. Handle it before creating the next stdout read:
                    // the child is waiting on stdin, not producing events.
                    let mut requested: Option<PendingApproval> = None;
                    for event in &events {
                        if let AgentEvent::PermissionRequest {
                            request_id,
                            tool_name,
                            input,
                            description,
                            ..
                        } = event
                        {
                            requested = Some(PendingApproval {
                                request_id: request_id.clone(),
                                tool_name: tool_name.clone(),
                                input: input.clone(),
                                description: description.clone(),
                            });
                        }
                    }
                    if let Some(request) = requested {
                        let label = request.tool_name.clone();
                        view.pending_approval = Some(label.clone());
                        if let Some(tail) = tail.as_mut() {
                            tail.pending_approval = Some(label.clone());
                        }
                        if let Some(format) = card_format {
                            if !paused {
                                let render_view = tail.as_ref().unwrap_or(&view);
                                self.refresh_card(
                                    &scope,
                                    &mut streaming,
                                    message_id.as_deref(),
                                    render_view,
                                    started,
                                    format,
                                )
                                .await;
                                last_update = Instant::now();
                            }
                        }
                        let approval_message = if operator.is_empty() {
                            None
                        } else {
                            self.send_approval_card(
                                &scope,
                                &chat_id,
                                &last_message_id,
                                run_id,
                                &operator,
                                &request,
                            )
                            .await
                        };
                        let Some(approval_message) = approval_message else {
                            // No card means no one can answer; do not leave the
                            // agent blocked for the full timeout.
                            tracing::warn!(
                                scope,
                                request_id = %request.request_id,
                                "cannot present an approval card; denying the tool"
                            );
                            if let Err(err) = process
                                .respond_to_permission(
                                    &request.request_id,
                                    PermissionResponse::Deny {
                                        message: "无法创建授权卡片，已自动拒绝。".to_string(),
                                    },
                                )
                                .await
                            {
                                tracing::warn!(error = %err, "cannot deliver the fallback denial");
                            }
                            view.pending_approval = None;
                            if let Some(tail) = tail.as_mut() {
                                tail.pending_approval = None;
                            }
                            pending = EventSource::next_events(&mut process);
                            continue;
                        };
                        let outcome = self
                            .wait_for_approval(&mut process, &mut approval_rx, &interrupt, &request)
                            .await;
                        let (status, by) = match &outcome {
                            ApprovalWait::Answered { allow: true, by } => {
                                ("✅ 已允许", by.as_str())
                            }
                            ApprovalWait::Answered { allow: false, by } => {
                                ("⛔ 已拒绝", by.as_str())
                            }
                            ApprovalWait::TimedOut => ("⌛ 超时自动拒绝", ""),
                            ApprovalWait::Interrupted => ("⏹ 已停止", ""),
                        };
                        tracing::info!(
                            scope,
                            request_id = %request.request_id,
                            allow = matches!(outcome, ApprovalWait::Answered { allow: true, .. }),
                            by,
                            "approval decided"
                        );
                        let card = approval_decided_card(&request.tool_name, status);
                        if let Ok(card) = lark_channel::Card::from_value(card) {
                            if let Err(err) = self
                                .transport
                                .update_card_message(&approval_message, &card)
                                .await
                            {
                                tracing::warn!(
                                    error = %err,
                                    "cannot update the decided approval card"
                                );
                            }
                        }
                        view.pending_approval = None;
                        if let Some(tail) = tail.as_mut() {
                            tail.pending_approval = None;
                        }
                        if matches!(outcome, ApprovalWait::Interrupted) {
                            process.kill().await;
                            view.mark_interrupted();
                            if let Some(tail) = tail.as_mut() {
                                tail.mark_interrupted();
                            }
                            break;
                        }
                        if let Some(format) = card_format {
                            if !paused {
                                let render_view = tail.as_ref().unwrap_or(&view);
                                self.refresh_card(
                                    &scope,
                                    &mut streaming,
                                    message_id.as_deref(),
                                    render_view,
                                    started,
                                    format,
                                )
                                .await;
                                last_update = Instant::now();
                            }
                        }
                        if view.finished {
                            break;
                        }
                        pending = EventSource::next_events(&mut process);
                        continue;
                    }
                    if view.finished {
                        break;
                    }
                    if let Some(format) = card_format {
                        if !paused && last_update.elapsed() >= CARD_UPDATE_INTERVAL {
                            let render_view = tail.as_ref().unwrap_or(&view);
                            self.refresh_card(
                                &scope,
                                &mut streaming,
                                message_id.as_deref(),
                                render_view,
                                started,
                                format,
                            )
                            .await;
                            last_update = Instant::now();
                        }
                    }
                    pending = EventSource::next_events(&mut process);
                }
            }
        }

        // Approval runs keep stdin open for control responses; close it so a
        // finished Claude process can exit before `finalize` reports.
        process.close_stdin().await;

        // EOF without a protocol terminal is still a normal finish (TS
        // `finalizeIfRunning`); an interrupted/timed-out view already has its
        // terminal and must keep it.
        if !view.finished {
            view.apply(&AgentEvent::Done {
                session_id: None,
                thread_id: None,
                termination_reason: TerminationReason::Normal,
            });
        }

        if let Some(key) = view.session_key().map(str::to_string) {
            // Saves from runs that started before a `/new` (or another session
            // reset) are dropped, so a slow finish cannot resurrect the session
            // the user just cleared.
            self.save_scope_session_if_current(&scope, &key, &cwd_str, generation)
                .await;
        }

        // Every terminal path reaches here: a normal agent finish, `/stop`,
        // the idle watchdog, the wall-clock cap and EOF-without-terminal all
        // leave the view terminal, and the COT bubble is completed from it.
        if let Some(run) = cot.take() {
            let termination = view.termination.unwrap_or(TerminationReason::Normal);
            if let Some(reason) = run.finish(termination, view.error.as_deref()).await {
                // TS `sendCotDegradedNotice`: publishing stopped mid-run, so
                // explain why the process bubble never completed. The final
                // answer is still sent below; a notice failure is only logged.
                tracing::warn!(scope = %scope, reason = %reason, "COT publishing degraded");
                if let Err(err) = reply::send_markdown(
                    &*self.transport,
                    &scope,
                    &chat_id,
                    Some(&last_message_id),
                    COT_DEGRADED_NOTICE,
                )
                .await
                {
                    tracing::warn!(error = %err, "cannot send the COT degraded notice");
                }
            }
        }
        // With a COT bubble the process was already published there, so the
        // final reply carries only the answer text (TS
        // `finalAnswerOnlyState`): no reasoning/tool duplication. A rolled-over
        // run does the same: the earlier cards already show the progress, and
        // the last card receives the complete answer without replaying every
        // tool panel.
        let answer_only = (cot_path || tail.is_some()).then(|| final_answer_only_view(&view));
        let final_view = answer_only.as_ref().unwrap_or(&view);

        self.finalize(
            &scope,
            &chat_id,
            &mut streaming,
            message_id.as_deref(),
            Some(&last_message_id),
            final_view,
            reply_mode,
            started,
            paused,
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
        streaming: &mut Option<Box<dyn BridgeStream>>,
        message_id: Option<&str>,
        source_message_id: Option<&str>,
        view: &RunView,
        reply_mode: MessageReplyMode,
        started: Instant,
        paused: bool,
    ) {
        let profile = self.live_profile();
        // Text mode has no progress message at all: post the full rendered
        // answer (including terminal notices such as "⏹ 已被中断").
        if reply_mode == MessageReplyMode::Text {
            let body = text_final_body(&profile, view);
            if let Err(err) =
                reply::send_text(&*self.transport, scope, chat_id, source_message_id, body).await
            {
                tracing::warn!(error = %err, chat = %chat_id, "cannot send final answer");
            }
            return;
        }

        let format = CardFormat::for_reply_mode(reply_mode).expect("card reply mode");
        // The card became a paused frame at the safe age: it must not be
        // updated again, and the answer goes out as its own message/card.
        if !paused {
            if let Some(handle) = streaming.as_mut() {
                match format {
                    CardFormat::Markdown => {
                        let body = markdown_final_body(&profile, view);
                        match handle.finish(&body).await {
                            Ok(true) => return,
                            Ok(false) => {
                                tracing::debug!(
                                    "streaming markdown window closed; trying the message-level final frame"
                                );
                            }
                            Err(err) => {
                                tracing::warn!(
                                    error = %err,
                                    "final streaming markdown update failed; trying the message-level final frame"
                                );
                            }
                        }
                        if self
                            .update_card(scope, message_id, view, started, format)
                            .await
                        {
                            return;
                        }
                    }
                    CardFormat::Rich => {
                        // Whole-card PUT with streaming mode off and no stop
                        // button (build_card omits the button when `stop` is
                        // None). set_card applies the card safe-age guard.
                        let card = self.final_card(view, started);
                        match handle.set_card(&card).await {
                            Ok(true) => return,
                            Ok(false) => {
                                tracing::debug!(
                                    "streaming card window closed; trying the message-level final frame"
                                );
                            }
                            Err(err) => {
                                tracing::warn!(
                                    error = %err,
                                    "final streaming card update failed; trying the message-level final frame"
                                );
                            }
                        }
                        if self
                            .update_card(scope, message_id, view, started, format)
                            .await
                        {
                            return;
                        }
                    }
                }
            } else if started.elapsed() < self.card_safe_age
                && self
                    .update_card(scope, message_id, view, started, format)
                    .await
            {
                return;
            }
        }

        match format {
            CardFormat::Markdown => {
                // Full answer, no 4k bridge cap; markdown keeps the TS text
                // renderer's formatting (reasoning/tools per profile).
                let body = markdown_final_body(&profile, view);
                if let Err(err) =
                    reply::send_markdown(&*self.transport, scope, chat_id, source_message_id, body)
                        .await
                {
                    tracing::warn!(error = %err, chat = %chat_id, "cannot send final markdown answer");
                }
            }
            CardFormat::Rich => {
                // Post-expiry (or card-dead) delivery: the paused/failed card
                // keeps its note, and the complete answer goes out as a fresh
                // card. A card send failure falls back to markdown rather than
                // losing the answer.
                let card = self.final_card(view, started);
                match reply::send_card(&*self.transport, scope, chat_id, source_message_id, &card)
                    .await
                {
                    Ok(_) => {}
                    Err(err) => {
                        tracing::warn!(
                            error = %err,
                            chat = %chat_id,
                            "cannot send final card; falling back to a markdown message"
                        );
                        let body = markdown_final_body(&profile, view);
                        if let Err(err) = reply::send_markdown(
                            &*self.transport,
                            scope,
                            chat_id,
                            source_message_id,
                            body,
                        )
                        .await
                        {
                            tracing::warn!(error = %err, chat = %chat_id, "cannot send final answer");
                        }
                    }
                }
            }
        }
    }

    /// The progress card created before the agent starts.
    fn initial_card(
        &self,
        scope: &str,
        profile: &ProfileConfig,
        view: &RunView,
        started: Instant,
        format: CardFormat,
    ) -> lark_channel::Card {
        match format {
            // The rich card is interactive from the first frame: a live run
            // always has its signed stop button (TS renders it while running).
            CardFormat::Rich => {
                let token = (!view.finished).then(|| self.tokens.sign(scope, "stop"));
                build_card(
                    profile,
                    view,
                    "启动中",
                    started,
                    token.as_deref().map(|token| (scope, token)),
                )
            }
            CardFormat::Markdown => build_markdown_card(profile, view, false),
        }
    }

    /// Push the current view to the run card.
    ///
    /// While the CardKit entity is live the whole card is PUT back through
    /// [`crate::transport::BridgeStream::set_card`]. Once it is expired, finished, or a write
    /// failed, the same message is updated through the message API for the
    /// rest of the run (and only inside the safe window).
    async fn refresh_card(
        &self,
        scope: &str,
        streaming: &mut Option<Box<dyn BridgeStream>>,
        message_id: Option<&str>,
        view: &RunView,
        started: Instant,
        format: CardFormat,
    ) {
        if let Some(handle) = streaming.as_mut() {
            if handle.is_live() {
                let card = larkline_lark::enable_streaming(
                    self.live_card_for(scope, view, started, format),
                )
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
        if started.elapsed() < self.card_safe_age {
            self.update_card(scope, message_id, view, started, format)
                .await;
        }
    }

    /// Write the one-time paused frame at the configured card safe age.
    ///
    /// The run continues; after this frame the progress message is never
    /// touched again, and the full answer is sent separately by `finalize`.
    async fn pause_progress(
        &self,
        scope: &str,
        streaming: &mut Option<Box<dyn BridgeStream>>,
        message_id: Option<&str>,
        view: &RunView,
        started: Instant,
        format: CardFormat,
    ) {
        let profile = self.live_profile();
        let card = match format {
            CardFormat::Rich => paused_rich_card(&profile, view, started),
            CardFormat::Markdown => build_markdown_card(&profile, view, true),
        };
        let mut written = false;
        if let Some(handle) = streaming.as_mut() {
            match handle.set_card(&card).await {
                Ok(true) => written = true,
                Ok(false) => tracing::debug!(
                    "streaming card window closed before the pause frame was written"
                ),
                Err(err) => tracing::warn!(
                    error = %err,
                    "paused card update failed; trying the message-level update"
                ),
            }
        }
        if !written {
            if let Some(message_id) = message_id {
                if let Err(err) = self.transport.update_card_message(message_id, &card).await {
                    tracing::warn!(error = %err, "paused message-card update failed");
                }
            }
        }
        tracing::info!(
            scope,
            format = ?format,
            "progress card paused at the card safe age"
        );
    }

    /// Send the standalone permission-approval card.
    ///
    /// Two signed buttons (allow / deny) bind the scope, the operator allowed
    /// to answer, the request id and the decision. The card is a normal
    /// non-streaming message: it can outlive the 8.5-minute progress window and
    /// a new message is what notifies the user that the agent is waiting.
    async fn send_approval_card(
        &self,
        scope: &str,
        chat_id: &str,
        source_message_id: &str,
        run_id: u64,
        operator: &str,
        request: &PendingApproval,
    ) -> Option<String> {
        let allow_action = approval_action("allow", &request.request_id, operator, run_id);
        let deny_action = approval_action("deny", &request.request_id, operator, run_id);
        let input_json = serde_json::to_string_pretty(&request.input)
            .unwrap_or_else(|_| request.input.to_string());
        let input_json = truncate_chars(&input_json, 2_500);
        let input_preview = approval_input_preview(&request.tool_name, &request.input);
        let timeout_minutes = self.approval_timeout.as_secs().div_ceil(60).max(1);
        let value = approval_card(&ApprovalCardInput {
            scope,
            operator,
            run_id,
            request_id: &request.request_id,
            tool_name: &request.tool_name,
            description: request.description.as_deref(),
            input_preview: &input_preview,
            input_json: &input_json,
            allow_action: &allow_action,
            allow_token: &self.tokens.sign(scope, &allow_action),
            deny_action: &deny_action,
            deny_token: &self.tokens.sign(scope, &deny_action),
            timeout_minutes,
        });
        let Ok(card) = lark_channel::Card::from_value(value) else {
            tracing::warn!("approval card is not a valid CardKit document");
            return None;
        };
        match reply::send_card(
            &*self.transport,
            scope,
            chat_id,
            Some(source_message_id),
            &card,
        )
        .await
        {
            Ok(id) => {
                tracing::info!(
                    scope,
                    request_id = %request.request_id,
                    tool = %request.tool_name,
                    "approval card sent"
                );
                Some(id.0)
            }
            Err(err) => {
                tracing::warn!(error = %err, "cannot send the approval card");
                None
            }
        }
    }

    /// Wait for the user's decision, the timeout, or `/stop`.
    ///
    /// The run loop is not reading stdout here: the agent is blocked on the
    /// permission response, so there is no event to lose. The progress card is
    /// deliberately not rolled over mid-wait; if its safe age elapses, the next
    /// loop tick rolls it over immediately after the decision.
    async fn wait_for_approval(
        &self,
        process: &mut AgentProcess,
        rx: &mut tokio::sync::mpsc::UnboundedReceiver<ApprovalDecision>,
        interrupt: &Arc<RunInterrupt>,
        request: &PendingApproval,
    ) -> ApprovalWait {
        let deadline = tokio::time::Instant::now() + self.approval_timeout;
        loop {
            tokio::select! {
                biased;
                _ = interrupt.wait() => return ApprovalWait::Interrupted,
                _ = tokio::time::sleep_until(deadline) => {
                    if let Err(err) = process
                        .respond_to_permission(
                            &request.request_id,
                            PermissionResponse::Deny {
                                message: "审批超时，已自动拒绝。".to_string(),
                            },
                        )
                        .await
                    {
                        tracing::warn!(error = %err, "cannot deliver the timeout denial");
                        return ApprovalWait::Interrupted;
                    }
                    tracing::info!(
                        request_id = %request.request_id,
                        "approval timed out; denied automatically"
                    );
                    return ApprovalWait::TimedOut;
                }
                decision = rx.recv() => {
                    let Some(decision) = decision else {
                        // The broker was dropped (run removed): treat it as a
                        // stop so the loop kills the child and finalizes.
                        return ApprovalWait::Interrupted;
                    };
                    if decision.request_id != request.request_id {
                        // A stale click for an already-answered request.
                        continue;
                    }
                    let response = if decision.allow {
                        PermissionResponse::Allow {
                            updated_input: request.input.clone(),
                        }
                    } else {
                        PermissionResponse::Deny {
                            message: "用户拒绝了该操作。".to_string(),
                        }
                    };
                    if let Err(err) = process
                        .respond_to_permission(&request.request_id, response)
                        .await
                    {
                        tracing::warn!(error = %err, "cannot deliver the approval decision");
                        return ApprovalWait::Interrupted;
                    }
                    return ApprovalWait::Answered {
                        allow: decision.allow,
                        by: decision.by,
                    };
                }
            }
        }
    }

    /// Hand the progress over to a fresh card when the current one reaches
    /// its safe age.
    ///
    /// The new CardKit entity starts its own 8.5-minute window. The old card
    /// is finalized with a pointer to the new one and no live stop button.
    /// The new card is created first: if that fails the caller keeps the old
    /// behavior (pause the card and send the answer separately), so the old
    /// card must not claim a successor exists.
    ///
    /// Returns `true` when the continuation card is live.
    #[allow(clippy::too_many_arguments)]
    async fn rollover_progress(
        &self,
        scope: &str,
        chat_id: &str,
        source_message_id: &str,
        streaming: &mut Option<Box<dyn BridgeStream>>,
        message_id: &mut Option<String>,
        view: &RunView,
        started: Instant,
        format: CardFormat,
    ) -> bool {
        let profile = self.live_profile();
        let next_view = RunView {
            continuation_note: Some(STREAM_CONTINUATION_NOTE.to_string()),
            ..RunView::default()
        };
        let next_card = self.live_card_for(scope, &next_view, started, format);
        let next = match reply::start_streaming_card(
            &*self.transport,
            scope,
            chat_id,
            Some(source_message_id),
            next_card,
            STREAMING_ELEMENT_ID,
        )
        .await
        {
            Ok(handle) => handle,
            Err(err) => {
                tracing::warn!(
                    error = %err,
                    scope,
                    "cannot start the continuation card; pausing instead"
                );
                return false;
            }
        };

        let head = match format {
            CardFormat::Rich => rollover_rich_card(&profile, view, started),
            CardFormat::Markdown => rollover_markdown_card(&profile, view),
        };
        let mut written = false;
        if let Some(handle) = streaming.as_mut() {
            match handle.set_card(&head).await {
                Ok(true) => written = true,
                Ok(false) => tracing::debug!(
                    "streaming card window closed before the rollover frame was written"
                ),
                Err(err) => tracing::warn!(
                    error = %err,
                    "rollover frame failed; trying the message-level update"
                ),
            }
        }
        if !written {
            if let Some(old_id) = message_id.as_deref() {
                if let Err(err) = self.transport.update_card_message(old_id, &head).await {
                    tracing::warn!(error = %err, "rollover message-card update failed");
                }
            }
        }

        *message_id = Some(next.message_id().to_string());
        *streaming = Some(next);
        tracing::info!(
            scope,
            format = ?format,
            "progress card rolled over to a fresh card"
        );
        true
    }

    /// Card for a running frame.
    ///
    /// The rich card carries a signed stop button whose token is bound to the
    /// run scope (not just the chat), so a topic run's button aborts that
    /// topic and leaves sibling topics alone. The markdown card is a single
    /// markdown element and intentionally has no controls.
    fn live_card_for(
        &self,
        scope: &str,
        view: &RunView,
        started: Instant,
        format: CardFormat,
    ) -> lark_channel::Card {
        let profile = self.live_profile();
        match format {
            CardFormat::Rich => {
                let token = if view.finished {
                    None
                } else {
                    Some(self.tokens.sign(scope, "stop"))
                };
                build_card(
                    &profile,
                    view,
                    card_status(view),
                    started,
                    token.as_deref().map(|token| (scope, token)),
                )
            }
            CardFormat::Markdown => build_markdown_card(&profile, view, false),
        }
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
        format: CardFormat,
    ) -> bool {
        let Some(message_id) = message_id else {
            return false;
        };
        let card = self.live_card_for(scope, view, started, format);
        match self.transport.update_card_message(message_id, &card).await {
            Ok(()) => true,
            Err(err) => {
                tracing::warn!(error = %err, "card update failed");
                false
            }
        }
    }

    /// Download every resource attached to the batch.
    ///
    /// Failure semantics follow the TS bridge: individual download errors are
    /// logged and omitted by [`MediaCache::resolve`], and the run continues
    /// with whatever resolved (possibly nothing). Policy rejections stay in
    /// the result so the prompt can report them with their reason.
    async fn resolve_attachments(
        &self,
        batch: &[NormalizedMessage],
        profile: &ProfileConfig,
    ) -> Vec<NormalizedAttachment> {
        let mut resources = Vec::new();
        for message in batch {
            for resource in &message.resources {
                let mut resource = resource.clone();
                // The SDK normalizer stamps the message id already; keep the
                // fallback for synthetic messages (card clicks, fixtures).
                if resource.message_id.is_empty() {
                    resource.message_id = message.message_id.clone();
                }
                resources.push(resource);
            }
        }
        if resources.is_empty() {
            return Vec::new();
        }

        let policy = MediaPolicy::from_profile(profile);
        let attachments = self.media.resolve(&resources, &policy).await;
        for attachment in &attachments {
            tracing::info!(
                decision = attachment.decision.as_str(),
                kind = attachment.kind.as_str(),
                hash = %attachment.hash,
                size = attachment.size,
                source_message_id = %attachment.source_message_id,
                reason = attachment.rejection_reason.as_deref().unwrap_or(""),
                "attachment decision"
            );
        }
        attachments
    }

    /// Prompt for one debounced batch, including its `<attachments>` section.
    fn build_batch_prompt(
        &self,
        batch: &[NormalizedMessage],
        attachments: &[NormalizedAttachment],
    ) -> String {
        let context = self.prompt_context(batch);
        build_agent_prompt_with_attachments(
            &context,
            &batch_user_input(batch, !attachments.is_empty()),
            &to_prompt_attachments(attachments),
        )
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
            match reply::send_text(
                &*bridge.transport,
                scope,
                to,
                target.parent_message_id,
                text,
            )
            .await
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
        let result = match recipient {
            Recipient::User(open_id) => {
                bridge
                    .transport
                    .send_dm_text(&open_id, text.to_string())
                    .await
            }
            Recipient::Chat(chat_id) => {
                bridge.transport.send_text(&chat_id, text.to_string()).await
            }
        };
        match result {
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
/// who said what, and segments are separated by a blank line. A batch whose
/// messages carry only attachments gets the TS "请看下面的附件。" fallback.
fn batch_user_input(batch: &[NormalizedMessage], has_attachments: bool) -> String {
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

    if !texts.is_empty() {
        texts.join("\n\n")
    } else if has_attachments {
        "请看下面的附件。".to_string()
    } else {
        // Also the TS fallback; the bridge drops empty-text messages at intake,
        // so this mainly keeps the helper total.
        "（对方发来一条没有正文的消息——通常是只 @ 了你的唤醒（ping）。请简短回应。）".to_string()
    }
}

/// Image paths forwarded to Codex via `--image`.
///
/// TS (`run-flow.ts`) filters the policy attachments to accepted images and
/// passes them only to Codex; Claude receives the paths in the prompt text
/// instead. Skipped/rejected images never reach argv.
fn accepted_image_paths(attachments: &[NormalizedAttachment]) -> Vec<String> {
    attachments
        .iter()
        .filter(|attachment| {
            attachment.kind == AttachmentKind::Image
                && attachment.decision == AttachmentDecision::Accepted
        })
        .map(|attachment| attachment.path.to_string_lossy().into_owned())
        .collect()
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
    reactions: Arc<dyn ReactionTransport>,
    message_id: String,
    state: Arc<StdMutex<ReactionState>>,
}

impl WorkingReaction {
    fn start(reactions: Arc<dyn ReactionTransport>, message_id: String) -> Self {
        let guard = Self {
            reactions,
            message_id,
            state: Arc::new(StdMutex::new(ReactionState::default())),
        };
        lock_state(&guard.state).begin_add();

        let reactions = Arc::clone(&guard.reactions);
        let message_id = guard.message_id.clone();
        let state = Arc::clone(&guard.state);
        tokio::spawn(async move {
            match reactions.add(&message_id, TYPING).await {
                Ok(reaction_id) => {
                    tracing::debug!(
                        message_id = %message_id,
                        reaction_id = %reaction_id,
                        "added working reaction"
                    );
                    let remove_now = lock_state(&state).added(&reaction_id);
                    if remove_now {
                        // The run ended before the add landed; clean up here.
                        if let Err(err) = reactions.remove(&message_id, &reaction_id).await {
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
        let reactions = Arc::clone(&self.reactions);
        let message_id = self.message_id.clone();
        let removal = async move {
            if let Err(err) = reactions.remove(&message_id, &reaction_id).await {
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

/// Build the rich card for the current view.
///
/// Thin adapter around [`render::render_card`]: it maps the profile preferences
/// and the bridge's signed stop token onto [`CardRenderOptions`] and appends the
/// bridge-only metadata note (agent · status · elapsed/tokens). The 30KB
/// degradation policy and every panel/collapse rule live in the renderer; the
/// final/paused frames keep `streaming_mode: false`, and the live CardKit path
/// re-enables streaming with `enable_streaming`.
fn build_card(
    profile: &ProfileConfig,
    view: &RunView,
    status: &str,
    started: Instant,
    stop: Option<(&str, &str)>,
) -> lark_channel::Card {
    let options = CardRenderOptions {
        reasoning: card_reasoning(profile),
        tool_panels: profile.show_tool_calls(),
        metadata_note: Some(format!(
            "{} · {} · {}",
            profile.agent_kind.display_name(),
            status,
            view.footer(started.elapsed())
        )),
        stop_value: stop.map(|(scope, token)| {
            json!({
                "__bridge_cb": true,
                "action": "stop",
                "scope": scope,
                "token": token
            })
        }),
        // Stable id so CardKit can also stream just the answer element later;
        // the run flow currently uses whole-card updates.
        answer_element_id: Some(STREAMING_ELEMENT_ID.to_string()),
        ..CardRenderOptions::default()
    };
    let value = render_card(view, &options);
    lark_channel::Card::from_value(value).expect("run card is valid")
}

/// Lightweight markdown progress card (`MessageReplyMode::Markdown`).
///
/// A single markdown element rendered by [`render_text`]: no tool panel, no
/// footer element and no stop button. `paused` renders the one-time 8.5-minute
/// pause frame (footer removed, pause note appended) and finalizes streaming.
fn build_markdown_card(
    profile: &ProfileConfig,
    view: &RunView,
    paused: bool,
) -> lark_channel::Card {
    let options = markdown_render_options(profile);
    let mut body = if paused {
        render_text_without_footer(view, options)
    } else {
        render_text(view, options)
    };
    if let Some(note) = view
        .continuation_note
        .as_deref()
        .filter(|note| !note.trim().is_empty())
    {
        body = if body.trim().is_empty() {
            note.to_string()
        } else {
            format!("{note}\n\n{body}")
        };
    }
    if let Some(tool) = view
        .pending_approval
        .as_deref()
        .filter(|tool| !tool.trim().is_empty())
    {
        body = if body.trim().is_empty() {
            format!("⏳ 等待授权：{tool}")
        } else {
            format!("⏳ 等待授权：{tool}\n\n{body}")
        };
    }
    if paused {
        body = append_pause_note(body);
    }
    if body.trim().is_empty() {
        body = "（没有输出）".to_string();
    }
    let summary = if paused {
        "仍在运行".to_string()
    } else {
        card_summary(view).to_string()
    };
    markdown_card_from_body(body, &summary)
}

/// Final frame of a markdown progress card that is handing over to the next
/// card: the running footer is dropped and a short pointer is appended.
fn rollover_markdown_card(profile: &ProfileConfig, view: &RunView) -> lark_channel::Card {
    let options = markdown_render_options(profile);
    let body = render_text_without_footer(view, options);
    let body = if body.trim().is_empty() {
        STREAM_ROLLOVER_NOTE.to_string()
    } else {
        format!("{body}\n\n{STREAM_ROLLOVER_NOTE}")
    };
    markdown_card_from_body(body, "仍在运行")
}

fn markdown_card_from_body(body: String, summary: &str) -> lark_channel::Card {
    let body = truncate_chars(&body, MAX_CARD_TEXT_CHARS);
    let value = json!({
        "schema": "2.0",
        "config": {
            "update_multi": true,
            "streaming_mode": false,
            "summary": { "content": summary }
        },
        "body": {
            "elements": [{
                "tag": "markdown",
                "content": body,
                "element_id": STREAMING_ELEMENT_ID,
            }]
        }
    });
    lark_channel::Card::from_value(value).expect("markdown run card is valid")
}

/// The one-time paused frame for the rich card.
fn paused_rich_card(
    profile: &ProfileConfig,
    view: &RunView,
    started: Instant,
) -> lark_channel::Card {
    closed_rich_card(profile, view, started, STREAM_PAUSED_NOTE)
}

/// A final progress frame written just before a new card takes over: footer
/// removed, streaming off, no live button, and a short pointer to the card
/// that continues the run.
fn rollover_rich_card(
    profile: &ProfileConfig,
    view: &RunView,
    started: Instant,
) -> lark_channel::Card {
    closed_rich_card(profile, view, started, STREAM_ROLLOVER_NOTE)
}

fn closed_rich_card(
    profile: &ProfileConfig,
    view: &RunView,
    started: Instant,
    note: &str,
) -> lark_channel::Card {
    // The TS pause frame renders with `footer: null` so a paused card never
    // claims the process is still updating: clone the view without the status
    // phrase instead of teaching `build_card` about paused frames.
    let mut paused_view = view.clone();
    paused_view.footer_status = None;
    paused_view.continuation_note = None;
    let mut value =
        build_card(profile, &paused_view, card_status(view), started, None).into_value();
    if let Some(config) = value["config"].as_object_mut() {
        config.insert("streaming_mode".to_string(), Value::Bool(false));
        config.insert("summary".to_string(), json!({ "content": "仍在运行" }));
    }
    if let Some(elements) = value["body"]["elements"].as_array_mut() {
        // A live stop button would outlive the paused frame; build_card was
        // called without one, but keep the filter explicit like the TS code.
        elements.retain(|element| element["tag"] != "button");
        elements.push(json!({
            "tag": "markdown",
            "content": note,
            "text_size": "notation"
        }));
    }
    lark_channel::Card::from_value(value).expect("paused run card is valid")
}

fn append_pause_note(body: String) -> String {
    if body.trim().is_empty() {
        STREAM_PAUSED_NOTE.to_string()
    } else {
        format!("{body}\n\n{STREAM_PAUSED_NOTE}")
    }
}

/// Whether the rich card renders the reasoning panel.
///
/// Mirrors the TS `showCardReasoning = reasoningDisplay === 'auto' || 'inline'`:
/// the default (`None`, i.e. auto) and `inline` show the panel, `off` and
/// `bubble` (COT stream) do not.
fn card_reasoning(profile: &ProfileConfig) -> bool {
    matches!(
        profile
            .preferences
            .as_ref()
            .and_then(|prefs| prefs.reasoning_display),
        None | Some(ReasoningDisplay::Auto) | Some(ReasoningDisplay::Inline)
    )
}

/// Whether the profile shows inline reasoning outside the card panel.
fn inline_reasoning(profile: &ProfileConfig) -> bool {
    matches!(
        profile
            .preferences
            .as_ref()
            .and_then(|prefs| prefs.reasoning_display),
        Some(ReasoningDisplay::Inline)
    )
}

/// Map the profile's reply preferences onto text-renderer options.
///
/// Text replies are plain-text messages, so markdown markers added by the
/// renderer would show up literally. Reasoning follows the TS text renderer:
/// only `inline` is included; `auto` is card-only, `off` disables it, and
/// `bubble` is streamed through the COT client instead.
fn text_render_options(profile: &ProfileConfig) -> TextRenderOptions {
    TextRenderOptions {
        format: TextFormat::Plain,
        show_tool_calls: profile.show_tool_calls(),
        reasoning: inline_reasoning(profile),
        ..TextRenderOptions::default()
    }
}

/// Markdown options for `MessageReplyMode::Markdown` progress cards.
fn markdown_render_options(profile: &ProfileConfig) -> TextRenderOptions {
    TextRenderOptions {
        format: TextFormat::Markdown,
        show_tool_calls: profile.show_tool_calls(),
        reasoning: inline_reasoning(profile),
        ..TextRenderOptions::default()
    }
}

/// Final body for `MessageReplyMode::Text`.
fn text_final_body(profile: &ProfileConfig, view: &RunView) -> String {
    // TS `renderText` reads `state.idleTimeoutMinutes` itself, so the notice
    // reports the watchdog minutes rather than the options default.
    let options = TextRenderOptions {
        idle_timeout_minutes: view.idle_timeout_minutes.unwrap_or(0),
        ..text_render_options(profile)
    };
    let body = render_text(view, options);
    if body.trim().is_empty() {
        "（没有输出）".to_string()
    } else {
        body
    }
}

/// The state a COT run's final reply is rendered from (TS
/// `finalAnswerOnlyState`).
///
/// The COT bubble already streamed the reasoning and every tool call, so the
/// final answer keeps only the text blocks: reasoning is cleared, tool panels
/// are dropped and the running footer is removed (the run is terminal anyway).
fn final_answer_only_view(view: &RunView) -> RunView {
    let mut final_view = view.clone();
    final_view.reasoning.clear();
    final_view.reasoning_active = false;
    final_view.footer_status = None;
    final_view.continuation_note = None;
    final_view.pending_approval = None;
    final_view.tools.clear();
    final_view.tool_calls.clear();
    final_view.tool_errors = 0;
    final_view
        .blocks
        .retain(|block| matches!(block, Block::Text { .. }));
    final_view
}

/// Final body for `MessageReplyMode::Markdown`.
fn markdown_final_body(profile: &ProfileConfig, view: &RunView) -> String {
    let options = TextRenderOptions {
        idle_timeout_minutes: view.idle_timeout_minutes.unwrap_or(0),
        ..markdown_render_options(profile)
    };
    let body = render_text(view, options);
    if body.trim().is_empty() {
        "（没有输出）".to_string()
    } else {
        body
    }
}

/// Action string signed into an approval button token.
fn approval_action(decision: &str, request_id: &str, operator: &str, run_id: u64) -> String {
    format!("approval:{decision}:{request_id}:{operator}:{run_id}")
}

/// One-line preview of the tool input for the approval card.
fn approval_input_preview(tool_name: &str, input: &Value) -> String {
    let Some(object) = input.as_object() else {
        return String::new();
    };
    let pick = |key: &str| object.get(key).and_then(Value::as_str).unwrap_or_default();
    let preview = match tool_name {
        "Bash" => pick("command").to_string(),
        "Read" | "Edit" | "Write" | "NotebookEdit" => pick("file_path").to_string(),
        "apply_patch" => input
            .get("changes")
            .and_then(Value::as_array)
            .map(|changes| {
                changes
                    .iter()
                    .filter_map(|change| change.get("path").and_then(Value::as_str))
                    .collect::<Vec<_>>()
                    .join("\n")
            })
            .unwrap_or_default(),
        "WebFetch" => pick("url").to_string(),
        "WebSearch" => pick("query").to_string(),
        _ => String::new(),
    };
    if preview.trim().is_empty() {
        truncate_chars(&serde_json::to_string(input).unwrap_or_default(), 400)
    } else {
        truncate_chars(&preview, 400)
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
pub(crate) fn profile_default_workspace(profile: &ProfileConfig) -> Option<PathBuf> {
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
/// Accepts an absolute path or the TS-style `~` / `~/<subpath>` form (expanded
/// with the current user's home). The path must exist and be a directory.
/// Returns the canonical path (symlinks and `..` resolved) on success, or a
/// user-facing message on failure.
pub(crate) fn validate_cd_target(input: &str) -> Result<PathBuf, String> {
    validate_cd_target_with_home(input, home_directory().as_deref())
}

/// The current user's home directory for `~` expansion.
fn home_directory() -> Option<PathBuf> {
    fn non_empty(value: std::ffi::OsString) -> Option<PathBuf> {
        let path = PathBuf::from(value);
        (!path.as_os_str().is_empty()).then_some(path)
    }
    #[cfg(windows)]
    {
        if let Some(profile) = std::env::var_os("USERPROFILE").and_then(non_empty) {
            return Some(profile);
        }
        if let (Some(drive), Some(tail)) =
            (std::env::var_os("HOMEDRIVE"), std::env::var_os("HOMEPATH"))
        {
            return Some(PathBuf::from(drive).join(tail));
        }
        None
    }
    #[cfg(not(windows))]
    {
        std::env::var_os("HOME").and_then(non_empty)
    }
}

/// Expand a leading `~` / `~/…` with an explicitly supplied home directory.
///
/// Pure so the tilde contract is testable without mutating `HOME`; the real
/// `/cd` path calls [`validate_cd_target`], which uses [`home_directory`].
pub(crate) fn expand_tilde_with_home(input: &str, home: Option<&Path>) -> Result<PathBuf, String> {
    let input = input.trim();
    if input == "~" {
        return home
            .map(Path::to_path_buf)
            .ok_or_else(|| "无法定位用户主目录，请使用绝对路径。".to_string());
    }
    if let Some(rest) = input.strip_prefix("~/") {
        let Some(home) = home else {
            return Err("无法定位用户主目录，请使用绝对路径。".to_string());
        };
        return Ok(home.join(rest));
    }
    if input.starts_with('~') {
        return Err(format!(
            "请使用绝对路径，或 `~/xxx` 表示 home 下的子路径：`{input}`"
        ));
    }
    Ok(PathBuf::from(input))
}

/// [`validate_cd_target`] with an explicit home, for tests.
pub(crate) fn validate_cd_target_with_home(
    input: &str,
    home: Option<&Path>,
) -> Result<PathBuf, String> {
    let input = input.trim();
    if input.is_empty() {
        return Err("用法：`/cd <绝对路径>` 或 `/cd ~/子路径`".to_string());
    }
    let expanded = expand_tilde_with_home(input, home)?;
    if !expanded.is_absolute() {
        return Err(format!(
            "请使用绝对路径，或 `~/xxx` 表示 home 下的子路径：`{input}`"
        ));
    }

    let canonical = fs::canonicalize(&expanded)
        .map_err(|_| format!("工作目录不存在或不可访问：`{}`", expanded.display()))?;
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
pub(crate) fn message_sender_id(message: &NormalizedMessage) -> &str {
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
    use crate::render::Terminal;

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
        assert_eq!(batch_user_input(&batch, false), "hello");
    }

    #[test]
    fn merged_batches_annotate_each_sender_and_join_with_blank_lines() {
        let batch = vec![
            message("om_1", "ou_1", "user", "first"),
            message("om_2", "ou_2", "bot", " second "),
        ];
        assert_eq!(
            batch_user_input(&batch, false),
            "[ou_1 (user)]: first\n\n[ou_2 (bot)]: second"
        );
    }

    #[test]
    fn unknown_sender_types_use_the_plain_annotation() {
        let batch = vec![
            message("om_1", "ou_1", "user", "a"),
            message("om_2", "ou_2", "unknown", "b"),
        ];
        assert_eq!(
            batch_user_input(&batch, false),
            "[ou_1 (user)]: a\n\n[ou_2]: b"
        );
    }

    #[test]
    fn attachment_only_batches_use_the_ts_attachment_fallback() {
        let batch = vec![message("om_1", "ou_1", "user", "  ")];
        assert_eq!(batch_user_input(&batch, true), "请看下面的附件。");
        assert!(batch_user_input(&batch, false).contains("没有正文"));
    }

    fn attachment(
        path: &str,
        kind: AttachmentKind,
        decision: AttachmentDecision,
        source_message_id: &str,
    ) -> NormalizedAttachment {
        NormalizedAttachment {
            path: PathBuf::from(path),
            kind,
            size: 5,
            mime: "image/png".to_string(),
            hash: "hash".to_string(),
            source_message_id: source_message_id.to_string(),
            source_file_key: "file_key".to_string(),
            original_name: None,
            requiredness: crate::media::AttachmentRequiredness::Optional,
            decision,
            rejection_reason: None,
        }
    }

    #[test]
    fn accepted_image_paths_ignore_rejected_and_non_image_attachments() {
        let attachments = vec![
            attachment(
                "/tmp/a.png",
                AttachmentKind::Image,
                AttachmentDecision::Accepted,
                "om_1",
            ),
            attachment(
                "/tmp/b.png",
                AttachmentKind::Image,
                AttachmentDecision::Rejected,
                "om_2",
            ),
            attachment(
                "/tmp/c.pdf",
                AttachmentKind::File,
                AttachmentDecision::Accepted,
                "om_3",
            ),
            attachment(
                "/tmp/d.png",
                AttachmentKind::Image,
                AttachmentDecision::Skipped,
                "om_4",
            ),
        ];
        assert_eq!(accepted_image_paths(&attachments), ["/tmp/a.png"]);
    }

    #[tokio::test]
    async fn batch_prompt_reports_attachments_and_merges_the_batch() {
        let root = tempfile::tempdir().expect("temp home");
        let bridge = test_bridge(root.path(), profile());
        let first = message("om_1", "ou_1", "user", "look");
        let second = message("om_2", "ou_2", "user", "  ");
        let attachments = vec![attachment(
            "/tmp/media/abc.png",
            AttachmentKind::Image,
            AttachmentDecision::Accepted,
            "om_1",
        )];

        let prompt = bridge.build_batch_prompt(&[first, second], &attachments);

        let context_at = prompt.find("<bridge_context>").expect("context");
        let attachments_at = prompt.find("<attachments>").expect("attachments");
        let user_at = prompt.find("<user_input>").expect("user input");
        assert!(context_at < attachments_at);
        assert!(attachments_at < user_at);
        assert!(prompt.contains("\"sourceMessageId\":\"om_1\""));
        assert!(prompt.contains("\"path\":\"/tmp/media/abc.png\""));
        assert!(prompt.contains("\"kind\":\"image\""));
        assert!(prompt.contains("[ou_1 (user)]: look"));
    }

    struct FakeAttachmentDownloader;

    #[async_trait]
    impl crate::media::ResourceDownloader for FakeAttachmentDownloader {
        async fn download(
            &self,
            resource: &lark_channel::media::ResourceDescriptor,
        ) -> Result<lark_channel::media::DownloadedResource, crate::media::MediaError> {
            let key = resource
                .image_key
                .clone()
                .or_else(|| resource.file_key.clone())
                .unwrap_or_default();
            if key == "img_bad" {
                return Err(crate::media::MediaError::Download("not found".into()));
            }
            Ok(lark_channel::media::DownloadedResource {
                bytes: b"bytes".to_vec(),
                content_type: Some("image/png".to_string()),
                content_disposition: None,
            })
        }
    }

    fn resource(
        message_id: &str,
        resource_type: lark_channel::media::ResourceType,
        key: &str,
    ) -> lark_channel::media::ResourceDescriptor {
        let (file_key, image_key) = match resource_type {
            lark_channel::media::ResourceType::Image => (None, Some(key.to_string())),
            _ => (Some(key.to_string()), None),
        };
        lark_channel::media::ResourceDescriptor {
            message_id: message_id.to_string(),
            resource_type,
            file_key,
            image_key,
            file_name: None,
            duration_ms: None,
        }
    }

    #[tokio::test]
    async fn resolve_attachments_downloads_batch_resources_and_tolerates_failures() {
        let root = tempfile::tempdir().expect("temp home");
        let paths = AppPaths::resolve(root.path(), "test").expect("app paths");
        let bridge = Bridge::new(
            Channel::new("cli_test_app", "secret", TenantBrand::Feishu),
            profile(),
            PathBuf::from("/nonexistent/larkline-test-agent"),
            root.path().join("workspace"),
            None,
            None,
            paths,
            None,
        )
        .expect("bridge")
        .with_attachment_downloader(Arc::new(FakeAttachmentDownloader));

        let mut first = message("om_1", "ou_1", "user", "look");
        first.resources = vec![resource(
            "om_1",
            lark_channel::media::ResourceType::Image,
            "img_ok",
        )];
        let mut second = message("om_2", "ou_1", "user", "");
        second.resources = vec![
            resource("om_2", lark_channel::media::ResourceType::Image, "img_bad"),
            resource("om_2", lark_channel::media::ResourceType::File, "file_ok"),
        ];

        let attachments = bridge
            .resolve_attachments(&[first, second], &profile())
            .await;
        assert_eq!(attachments.len(), 2, "the failed image is omitted");
        assert_eq!(attachments[0].kind, AttachmentKind::Image);
        assert_eq!(attachments[0].source_message_id, "om_1");
        assert_eq!(attachments[1].kind, AttachmentKind::File);
        assert_eq!(attachments[1].source_message_id, "om_2");
        assert!(attachments.iter().all(NormalizedAttachment::is_accepted));

        let prompt =
            bridge.build_batch_prompt(&[message("om_1", "ou_1", "user", "look")], &attachments);
        assert!(prompt.contains("<attachments>"));
        assert!(prompt.contains("\"sourceMessageId\":\"om_2\""));
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

    /// Event source that never produces an event; used to prove a pending
    /// agent read is preempted by the cooperative interrupt / pause timer.
    struct PendingSource;

    #[async_trait]
    impl EventSource for PendingSource {
        async fn next_events(&mut self) -> Option<Vec<AgentEvent>> {
            std::future::pending().await
        }
    }

    #[tokio::test]
    async fn interrupt_requested_before_the_wait_is_not_lost() {
        let interrupt = RunInterrupt::new();
        interrupt.request();
        assert!(
            tokio::time::timeout(Duration::from_millis(100), interrupt.wait())
                .await
                .is_ok(),
            "a request that races the wait preamble must still wake the loop"
        );
        assert!(interrupt.is_requested());
    }

    // ---- idle watchdog ---------------------------------------------------

    #[test]
    fn idle_timeout_resolution_prefers_the_scope_override_over_the_preference() {
        // No override: the global preference applies (TS `getRunIdleTimeoutMs`).
        assert_eq!(resolve_idle_timeout_minutes(None, Some(15)), Some(15));
        // `/timeout 5` wins over a different global default.
        assert_eq!(resolve_idle_timeout_minutes(Some(5), Some(30)), Some(5));
        // `/timeout off` (0) disables the watchdog even with a global default.
        assert_eq!(resolve_idle_timeout_minutes(Some(0), Some(30)), None);
        // Absent / 0 preference means no watchdog.
        assert_eq!(resolve_idle_timeout_minutes(None, None), None);
        assert_eq!(resolve_idle_timeout_minutes(None, Some(0)), None);
        // The global preference is clamped to 1-120, like TS.
        assert_eq!(resolve_idle_timeout_minutes(None, Some(999)), Some(120));
    }

    #[tokio::test]
    async fn bridge_resolves_the_idle_timeout_from_preferences_and_the_scope_override() {
        let root = tempfile::tempdir().unwrap();
        let mut profile = profile();
        profile
            .preferences
            .get_or_insert_with(Preferences::default)
            .run_idle_timeout_minutes = Some(20);
        let bridge = test_bridge(root.path(), profile);
        let scope = "oc_idle";

        // No `/timeout` override: the profile preference is the default.
        assert_eq!(bridge.run_idle_timeout_minutes(scope).await, Some(20));

        // A per-scope override wins over the preference.
        bridge
            .sessions
            .lock()
            .await
            .set_idle_timeout_minutes(scope, 5)
            .unwrap();
        assert_eq!(bridge.run_idle_timeout_minutes(scope).await, Some(5));

        // `/timeout 0` disables the watchdog for this scope only.
        bridge
            .sessions
            .lock()
            .await
            .set_idle_timeout_minutes(scope, 0)
            .unwrap();
        assert_eq!(bridge.run_idle_timeout_minutes(scope).await, None);

        // `/timeout default` clears the override and falls back to the profile.
        assert!(
            bridge
                .sessions
                .lock()
                .await
                .clear_idle_timeout_override(scope)
                .unwrap()
        );
        assert_eq!(bridge.run_idle_timeout_minutes(scope).await, Some(20));
    }

    #[test]
    fn idle_watchdog_pauses_while_a_tool_is_in_flight_and_rearms_when_it_returns() {
        let start = tokio::time::Instant::now();
        let mut watchdog = IdleWatchdog::new(Some(1), Duration::from_secs(60), start);
        assert_eq!(
            watchdog.deadline(),
            Some(start + Duration::from_secs(60)),
            "a fresh run arms the idle timer immediately"
        );

        // A tool call pauses the watchdog: the agent may legitimately be
        // silent while the tool runs (an OAuth flow waiting on the user).
        watchdog.observe(
            &AgentEvent::ToolUse {
                id: "t1".into(),
                name: "Bash".into(),
                input: json!({ "command": "sleep 600" }),
            },
            start,
        );
        assert_eq!(watchdog.deadline(), None);

        // Other events do not re-arm it while the tool window is open.
        watchdog.observe(
            &AgentEvent::Thinking {
                delta: "waiting for the tool".into(),
            },
            start,
        );
        assert_eq!(watchdog.deadline(), None);

        // The matching tool_result drains the set and re-arms the timer.
        let returned = start + Duration::from_secs(5);
        watchdog.observe(
            &AgentEvent::ToolResult {
                id: "t1".into(),
                output: "done".into(),
                is_error: false,
            },
            returned,
        );
        assert_eq!(
            watchdog.deadline(),
            Some(returned + Duration::from_secs(60))
        );

        // Overlapping tools keep it paused until the last one returns.
        for id in ["t2", "t3"] {
            watchdog.observe(
                &AgentEvent::ToolUse {
                    id: id.into(),
                    name: "Read".into(),
                    input: json!({}),
                },
                returned,
            );
        }
        watchdog.observe(
            &AgentEvent::ToolResult {
                id: "t2".into(),
                output: String::new(),
                is_error: false,
            },
            returned,
        );
        assert_eq!(watchdog.deadline(), None);
        watchdog.observe(
            &AgentEvent::ToolResult {
                id: "t3".into(),
                output: String::new(),
                is_error: false,
            },
            returned,
        );
        assert!(watchdog.deadline().is_some());
    }

    #[test]
    fn idle_watchdog_rearms_on_text_and_thinking_activity() {
        let start = tokio::time::Instant::now();
        let mut watchdog = IdleWatchdog::new(Some(2), Duration::from_secs(60), start);
        let first = watchdog.deadline().unwrap();
        let text_at = start + Duration::from_secs(30);
        watchdog.observe(&AgentEvent::Text { delta: "hi".into() }, text_at);
        assert_eq!(
            watchdog.deadline(),
            Some(text_at + Duration::from_secs(120))
        );
        assert!(watchdog.deadline().unwrap() > first);
        let thinking_at = text_at + Duration::from_secs(10);
        watchdog.observe(&AgentEvent::Thinking { delta: "hm".into() }, thinking_at);
        assert_eq!(
            watchdog.deadline(),
            Some(thinking_at + Duration::from_secs(120))
        );
        // Text/thinking events are irrelevant to the tool window.
        assert!(watchdog.in_flight_tools.is_empty());
    }

    #[test]
    fn the_wall_clock_is_only_armed_when_the_idle_watchdog_is_disabled() {
        let now = tokio::time::Instant::now();
        // Idle watchdog on: no wall-clock deadline (TS has none either).
        assert_eq!(wall_clock_deadline(Some(15), now), None);
        // Idle watchdog off: the 30-minute cap stays as the safety net.
        assert_eq!(wall_clock_deadline(None, now), Some(now + RUN_TIMEOUT));
        assert_eq!(
            IdleWatchdog::new(None, Duration::from_secs(60), now).deadline(),
            None
        );
    }

    #[tokio::test]
    async fn idle_expiry_stops_the_run_and_renders_the_ts_timeout_notice() {
        let start = tokio::time::Instant::now();
        let watchdog = IdleWatchdog::new(Some(15), Duration::from_secs(60), start);
        let mut source = PendingSource;
        let mut pending = EventSource::next_events(&mut source);
        let interrupt = RunInterrupt::new();
        // The watchdog deadline has been reached: no agent event arrived and
        // no tool is in flight, so the tick must fire instead of waiting.
        let tick = next_tick(&mut pending, &interrupt, None, None, Some(start)).await;
        assert!(
            matches!(tick, LoopTick::IdleTimeout),
            "an idle run must yield to the idle watchdog"
        );
        drop(pending);

        // What the run loop does on `IdleTimeout`: keep the partial answer,
        // mark the TS terminal, then render the final card.
        let mut view = RunView::default();
        view.apply(&AgentEvent::Text {
            delta: "partial answer".into(),
        });
        view.mark_idle_timeout(watchdog.minutes().unwrap());
        assert_eq!(view.terminal(), Terminal::IdleTimeout);
        assert_eq!(view.idle_timeout_minutes, Some(15));
        assert_eq!(view.best_answer(), "partial answer");

        let card = build_card(&profile(), &view, card_status(&view), Instant::now(), None);
        let payload = serde_json::to_string(card.as_value()).unwrap();
        assert!(payload.contains("partial answer"), "{payload}");
        assert!(payload.contains("⏱ 15 分钟无响应,已自动终止"), "{payload}");
        assert!(
            !payload.contains("__bridge_cb"),
            "a timed-out run must not keep a stop button: {payload}"
        );
        assert_eq!(card.as_value()["config"]["summary"]["content"], "已超时");
        assert_eq!(card_status(&view), "已超时");

        // Text and markdown replies use the view's watchdog minutes for the
        // notice, exactly like TS `renderText` reading `state.idleTimeoutMinutes`.
        assert!(
            text_final_body(&profile(), &view).contains("⏱ 15 分钟无响应,已自动终止"),
            "text notice must report the watchdog minutes"
        );
        assert!(
            markdown_final_body(&profile(), &view).contains("⏱ 15 分钟无响应,已自动终止"),
            "markdown notice must report the watchdog minutes"
        );
    }

    #[tokio::test]
    async fn cooperative_interrupt_keeps_the_partial_answer_and_a_terminal_card() {
        let interrupt = RunInterrupt::new();
        let signal = Arc::clone(&interrupt);
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(20)).await;
            signal.request();
        });

        let mut source = PendingSource;
        let mut pending = EventSource::next_events(&mut source);
        let deadline = Some(tokio::time::Instant::now() + RUN_TIMEOUT);
        let tick = next_tick(&mut pending, &interrupt, deadline, None, None).await;
        assert!(
            matches!(tick, LoopTick::Interrupted),
            "a pending agent read must yield to the stop signal"
        );

        // What the run loop does after the tick: keep the streamed text, mark
        // the terminal, then render the final card.
        let mut view = RunView::default();
        view.apply(&AgentEvent::Text {
            delta: "partial answer".into(),
        });
        view.mark_interrupted();
        assert_eq!(view.best_answer(), "partial answer");

        let card = build_card(&profile(), &view, card_status(&view), Instant::now(), None);
        let payload = serde_json::to_string(card.as_value()).unwrap();
        assert!(payload.contains("partial answer"), "{payload}");
        assert!(payload.contains("⏹ 已被中断"), "{payload}");
        assert!(!payload.contains("运行中"), "stale running card: {payload}");
        assert!(
            !payload.contains("__bridge_cb"),
            "live button left: {payload}"
        );
        assert_eq!(card.as_value()["config"]["summary"]["content"], "已中断");
        assert_eq!(card_status(&view), "已中断");
    }

    #[tokio::test]
    async fn stop_button_requests_a_cooperative_interrupt_instead_of_aborting() {
        let root = tempfile::tempdir().unwrap();
        let mut profile = profile();
        profile.access = Some(Access {
            admins: vec!["ou_admin".into()],
            ..Access::default()
        });
        let bridge = test_bridge(root.path(), profile);
        let scope = "oc_1";
        let interrupt = RunInterrupt::new();
        let task = tokio::spawn(std::future::pending::<()>());
        {
            let mut active = lock(&bridge.active);
            active.insert(
                scope.to_string(),
                ActiveRun {
                    id: 42,
                    abort: task.abort_handle(),
                    interrupt: Some(Arc::clone(&interrupt)),
                    approvals: None,
                },
            );
        }

        let event = card_action_event(json!({
            "__bridge_cb": true,
            "action": "stop",
            "scope": scope,
            "token": bridge.tokens.sign(scope, "stop"),
        }));
        assert_eq!(
            bridge.handle_card_action(&event).await.as_deref(),
            Some("已停止当前任务。")
        );
        assert!(
            interrupt.is_requested(),
            "the stop button must signal the run, not silently abort it"
        );
        assert!(
            bridge.is_scope_active(scope),
            "the run stays registered until its finalize completes"
        );

        // The run loop finishes finalize and its guard releases the scope.
        bridge.finish_run(scope, 42);
        assert!(!bridge.is_scope_active(scope));
        task.abort();
    }

    #[tokio::test]
    async fn stop_command_signals_a_cooperative_run() {
        let root = tempfile::tempdir().unwrap();
        let bridge = test_bridge(root.path(), profile());
        let scope = "oc_1";
        let interrupt = RunInterrupt::new();
        let task = tokio::spawn(std::future::pending::<()>());
        {
            let mut active = lock(&bridge.active);
            active.insert(
                scope.to_string(),
                ActiveRun {
                    id: 7,
                    abort: task.abort_handle(),
                    interrupt: Some(Arc::clone(&interrupt)),
                    approvals: None,
                },
            );
        }

        let text = "/stop";
        let reply = bridge
            .handle_command(&message("om_stop", "ou_1", "user", text), text)
            .await
            .expect("handled");
        assert_eq!(reply, "已停止当前任务。");
        assert!(interrupt.is_requested());
        assert!(
            bridge.is_scope_active(scope),
            "the cooperative run finalizes before its guard releases the scope"
        );
        bridge.finish_run(scope, 7);
        assert!(!bridge.is_scope_active(scope));
        task.abort();
    }

    #[tokio::test]
    async fn stop_watchdog_aborts_a_run_whose_finalize_hangs() {
        let root = tempfile::tempdir().unwrap();
        let bridge = test_bridge(root.path(), profile());
        let scope = "oc_1".to_string();
        let interrupt = RunInterrupt::new();

        let bridge_task = Arc::clone(&bridge);
        let task_scope = scope.clone();
        let hung = tokio::spawn(async move {
            let _guard = RunGuard {
                bridge: bridge_task,
                scope: task_scope,
                id: 9,
                permit: None,
            };
            std::future::pending::<()>().await;
        });
        {
            let mut active = lock(&bridge.active);
            active.insert(
                scope.clone(),
                ActiveRun {
                    id: 9,
                    abort: hung.abort_handle(),
                    interrupt: Some(Arc::clone(&interrupt)),
                    approvals: None,
                },
            );
        }

        assert!(bridge.stop_scope(&scope).0);
        assert!(interrupt.is_requested());
        assert!(bridge.is_scope_active(&scope));

        let error = tokio::time::timeout(Duration::from_secs(5), hung)
            .await
            .expect("watchdog aborts the hung run")
            .expect_err("watchdog abort yields JoinError");
        assert!(error.is_cancelled());
        assert!(
            !bridge.is_scope_active(&scope),
            "the run guard must clear the scope after the watchdog abort"
        );
    }

    #[test]
    fn markdown_mode_card_is_a_single_markdown_element_without_controls() {
        let mut view = RunView::default();
        view.apply(&AgentEvent::Thinking {
            delta: "inline reasoning".into(),
        });
        view.apply(&AgentEvent::Text {
            delta: "answer".into(),
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
        let mut profile = profile();
        profile
            .preferences
            .get_or_insert_with(Preferences::default)
            .reasoning_display = Some(ReasoningDisplay::Inline);

        let card = build_markdown_card(&profile, &view, false);
        let value = card.as_value();
        let elements = value["body"]["elements"].as_array().unwrap();
        assert_eq!(elements.len(), 1, "only the markdown element: {value}");
        assert_eq!(elements[0]["tag"], "markdown");
        assert_eq!(elements[0]["element_id"], STREAMING_ELEMENT_ID);
        assert_eq!(value["config"]["streaming_mode"], false);

        let payload = serde_json::to_string(value).unwrap();
        assert!(payload.contains("answer"), "{payload}");
        assert!(payload.contains("inline reasoning"), "{payload}");
        assert!(!payload.contains("__bridge_cb"), "{payload}");
        assert!(!payload.contains("\"tag\":\"button\""), "{payload}");
        // No rich-card tool panel / footer note elements.
        assert!(!payload.contains("**工具**"), "{payload}");
        assert!(!payload.contains("\"tag\":\"note\""), "{payload}");
    }

    #[test]
    fn card_mode_keeps_the_tool_panel_status_footer_and_signed_stop_button() {
        let mut view = RunView::default();
        view.apply(&AgentEvent::Text {
            delta: "answer".into(),
        });
        view.apply(&AgentEvent::ToolUse {
            id: "t1".into(),
            name: "Bash".into(),
            input: json!({ "command": "pwd" }),
        });
        let card = build_card(
            &profile(),
            &view,
            card_status(&view),
            Instant::now(),
            Some(("oc_1", "deadbeef")),
        );
        let payload = serde_json::to_string(card.as_value()).unwrap();
        // The flat 6-line "**工具**" block is gone: each tool call is a panel.
        assert!(!payload.contains("**工具**"), "{payload}");
        assert!(
            payload.contains("\"tag\":\"collapsible_panel\""),
            "{payload}"
        );
        assert!(payload.contains("⏳ **Bash** — pwd"), "{payload}");
        // The live footer phrase precedes the bridge metadata note and button.
        assert!(payload.contains("🧰 正在调用工具"), "{payload}");
        assert!(payload.contains("\"tag\":\"note\""), "{payload}");
        assert!(payload.contains("__bridge_cb"), "{payload}");
        assert!(payload.contains("deadbeef"), "{payload}");
        assert!(payload.contains("⏹ 终止"), "{payload}");
        let phrase = payload.find("🧰 正在调用工具").unwrap();
        let meta = payload.find("Claude Code").unwrap();
        let button = payload.find("⏹ 终止").unwrap();
        assert!(phrase < meta && meta < button, "{payload}");
    }

    #[test]
    fn paused_progress_frames_are_final_and_carry_the_pause_note_once() {
        let mut view = RunView::default();
        view.apply(&AgentEvent::Text {
            delta: "partial answer".into(),
        });

        let markdown = build_markdown_card(&profile(), &view, true);
        let payload = serde_json::to_string(markdown.as_value()).unwrap();
        assert_eq!(markdown.as_value()["config"]["streaming_mode"], false);
        assert_eq!(
            markdown.as_value()["config"]["summary"]["content"],
            "仍在运行"
        );
        assert!(payload.contains("partial answer"), "{payload}");
        assert!(payload.contains(STREAM_PAUSED_NOTE), "{payload}");
        assert!(!payload.contains("正在输出"), "{payload}");
        assert!(!payload.contains("__bridge_cb"), "{payload}");
        assert!(!payload.contains("\"tag\":\"button\""), "{payload}");
        assert_eq!(payload.matches(STREAM_PAUSED_NOTE).count(), 1);

        let rich = paused_rich_card(&profile(), &view, Instant::now());
        let payload = serde_json::to_string(rich.as_value()).unwrap();
        assert_eq!(rich.as_value()["config"]["streaming_mode"], false);
        assert_eq!(rich.as_value()["config"]["summary"]["content"], "仍在运行");
        assert!(payload.contains("partial answer"), "{payload}");
        assert!(payload.contains(STREAM_PAUSED_NOTE), "{payload}");
        assert!(!payload.contains("__bridge_cb"), "{payload}");
        assert!(!payload.contains("\"tag\":\"button\""), "{payload}");
        assert_eq!(payload.matches(STREAM_PAUSED_NOTE).count(), 1);
    }

    #[tokio::test]
    async fn expiry_pause_tick_fires_once_then_the_loop_stops_pausing() {
        let interrupt = RunInterrupt::new();
        let mut source = PendingSource;
        let mut pending = EventSource::next_events(&mut source);
        let deadline = Some(tokio::time::Instant::now() + RUN_TIMEOUT);
        let already_due = tokio::time::Instant::now();

        assert!(matches!(
            next_tick(&mut pending, &interrupt, deadline, Some(already_due), None).await,
            LoopTick::PauseElapsed
        ));
        // The run loop passes `None` once paused, so no second pause frame can
        // be produced while the run keeps streaming.
        let after_pause = tokio::time::timeout(
            Duration::from_millis(30),
            next_tick(&mut pending, &interrupt, deadline, None, None),
        )
        .await;
        assert!(
            after_pause.is_err(),
            "a paused loop must not emit another pause tick"
        );
    }

    #[test]
    fn interrupted_text_mode_keeps_the_partial_answer() {
        let mut view = RunView::default();
        view.apply(&AgentEvent::Text {
            delta: "partial answer".into(),
        });
        view.mark_interrupted();
        assert_eq!(
            text_final_body(&profile(), &view),
            "partial answer\n\n⏹ 已被中断"
        );
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
        let mut view = RunView::default();
        view.apply(&AgentEvent::Text {
            delta: "hello".into(),
        });
        let card = build_card(
            &profile(),
            &view,
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
        assert!(text.contains("⏹ 终止"));

        let finished = build_card(&profile(), &view, "完成", Instant::now(), None);
        let text = serde_json::to_string(finished.as_value()).unwrap();
        assert!(!text.contains("__bridge_cb"));
    }

    #[test]
    fn show_tool_calls_false_hides_only_the_tool_panels() {
        let mut view = RunView::default();
        view.apply(&AgentEvent::Text {
            delta: "the answer".into(),
        });
        view.apply(&AgentEvent::ToolUse {
            id: "t1".into(),
            name: "Bash".into(),
            input: json!({ "command": "ls" }),
        });
        let shown = serde_json::to_string(
            build_card(&profile(), &view, "运行中", Instant::now(), None).as_value(),
        )
        .unwrap();
        assert!(shown.contains("Bash"), "{shown}");
        assert!(shown.contains("the answer"), "{shown}");

        let mut hidden = profile();
        hidden
            .preferences
            .get_or_insert_with(Preferences::default)
            .show_tool_calls = Some(false);
        let hidden_payload = serde_json::to_string(
            build_card(&hidden, &view, "运行中", Instant::now(), None).as_value(),
        )
        .unwrap();
        assert!(!hidden_payload.contains("Bash"), "{hidden_payload}");
        // Hiding tools must not hide the answer or the stop/footer plumbing.
        assert!(hidden_payload.contains("the answer"), "{hidden_payload}");
        assert!(hidden_payload.contains("正在调用工具"), "{hidden_payload}");
    }

    #[test]
    fn huge_answers_are_kept_whole() {
        // Regression for the old 8000-char card cap: a long answer must render
        // in full (the only remaining cap is a 100k CardKit sanity guard).
        let answer = "x".repeat(9_000);
        let mut view = RunView::default();
        view.apply(&AgentEvent::Text {
            delta: answer.clone(),
        });
        view.apply(&AgentEvent::Done {
            session_id: None,
            thread_id: None,
            termination_reason: TerminationReason::Normal,
        });
        let card = build_card(&profile(), &view, card_status(&view), Instant::now(), None);
        let text = serde_json::to_string(card.as_value()).unwrap();
        assert!(text.contains(&answer), "answer was truncated: {text}");
        assert!(!text.contains("内容过长已截断"), "{text}");
        assert_eq!(text_final_body(&profile(), &view), answer);
        assert_eq!(markdown_final_body(&profile(), &view), answer);
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
                None,
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
            Bridge::new(channel, enabled, agent, workspace, None, None, paths, None)
                .expect("bridge"),
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
        for input in ["relative/path", "../work", "~user/work"] {
            let err = validate_cd_target(input).unwrap_err();
            assert!(err.contains("绝对路径"), "{err}");
        }
        // A bare `~` with no home available is a distinct, actionable error.
        let err = validate_cd_target_with_home("~", None).unwrap_err();
        assert!(err.contains("主目录"), "{err}");
    }

    #[test]
    fn validate_cd_target_expands_tilde_against_home() {
        let root = tempfile::tempdir().unwrap();
        let home = root.path().join("home");
        let work = home.join("work");
        fs::create_dir_all(&work).unwrap();

        assert_eq!(
            validate_cd_target_with_home("~", Some(&home)).unwrap(),
            home.canonicalize().unwrap()
        );
        assert_eq!(
            validate_cd_target_with_home("~/work", Some(&home)).unwrap(),
            work.canonicalize().unwrap()
        );
        assert_eq!(
            expand_tilde_with_home("~/client work", Some(&home)).unwrap(),
            home.join("client work")
        );

        // Only the `~` / `~/…` forms are accepted, matching TS.
        let err = validate_cd_target_with_home("~user/work", Some(&home)).unwrap_err();
        assert!(err.contains("绝对路径"), "{err}");
        let err = validate_cd_target_with_home("~/missing", Some(&home)).unwrap_err();
        assert!(err.contains("不存在"), "{err}");
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
        test_bridge_with_app_secret(root, profile, None)
    }

    /// Bridge with the profile's resolved App Secret threaded through exactly
    /// as `larkline run` does.
    fn test_bridge_with_app_secret(
        root: &Path,
        profile: ProfileConfig,
        app_secret: Option<&str>,
    ) -> Arc<Bridge> {
        // Never hit the owner API from a unit test: the real source would try
        // to reach Feishu. Tests that exercise owner gating install their own
        // FakeOwnerSource with `test_bridge_with_owner`.
        test_bridge_with_owner_and_secret(
            root,
            profile,
            FakeOwnerSource::failure("offline test"),
            app_secret,
        )
    }

    fn test_bridge_with_owner(
        root: &Path,
        profile: ProfileConfig,
        owner_source: Arc<dyn OwnerSource>,
    ) -> Arc<Bridge> {
        test_bridge_with_owner_and_secret(root, profile, owner_source, None)
    }

    fn test_bridge_with_owner_and_secret(
        root: &Path,
        profile: ProfileConfig,
        owner_source: Arc<dyn OwnerSource>,
        app_secret: Option<&str>,
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
            app_secret,
        )
        .expect("bridge");
        bridge.owner = OwnerGate::new(owner_source);
        Arc::new(bridge)
    }

    /// Profile whose access policy accepts the fixture clicker (`ou_1`).
    fn clickable_profile() -> ProfileConfig {
        // Card-click access uses the TS union; allow the fixture clicker.
        let mut profile = profile();
        profile.access = Some(Access {
            allowed_users: vec!["ou_1".into()],
            ..Access::default()
        });
        profile
    }

    fn callback_profile(secret: &str) -> ProfileConfig {
        let mut profile = clickable_profile();
        profile
            .extra
            .insert("callback".to_string(), json!({ "secret": secret }));
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
    async fn agent_callback_verifies_with_the_profile_app_secret() {
        // `larkline run` resolves the App Secret and hands it to `Bridge::new`;
        // the bridge installs it as key v1 exactly like TypeScript, so a
        // token minted by lark-cli verifies with no `extra.callback.*`.
        let root = tempfile::tempdir().unwrap();
        let bridge =
            test_bridge_with_app_secret(root.path(), clickable_profile(), Some("app-s3cret"));
        let token = sign_callback("app-s3cret", "nonce_app_secret");
        let event = card_action_event(json!({
            "__bridge_cb": true,
            "bridge_token": token,
            "choice": "b"
        }));

        assert!(bridge.handle_card_action(&event).await.is_none());
        let batch = take_batch(&bridge);
        assert_eq!(batch.len(), 1);
        assert_eq!(batch[0].text, r#"[card-click] {"choice":"b"}"#);
    }

    #[tokio::test]
    async fn explicit_callback_key_wins_over_the_profile_app_secret() {
        let root = tempfile::tempdir().unwrap();
        let bridge = test_bridge_with_app_secret(
            root.path(),
            callback_profile("explicit"),
            Some("app-s3cret"),
        );

        // The App Secret is not added as a fallback key version when an
        // explicit key exists: its token is rejected.
        let app_token = sign_callback("app-s3cret", "nonce_app_fallback");
        let app_event = card_action_event(json!({
            "__bridge_cb": true,
            "bridge_token": app_token
        }));
        assert!(bridge.handle_card_action(&app_event).await.is_none());
        assert!(take_batch(&bridge).is_empty());

        // The explicitly configured key still verifies.
        let explicit_token = sign_callback("explicit", "nonce_explicit");
        let explicit_event = card_action_event(json!({
            "__bridge_cb": true,
            "bridge_token": explicit_token,
            "choice": "a"
        }));
        assert!(bridge.handle_card_action(&explicit_event).await.is_none());
        let batch = take_batch(&bridge);
        assert_eq!(batch.len(), 1);
        assert_eq!(batch[0].text, r#"[card-click] {"choice":"a"}"#);
    }

    #[tokio::test]
    async fn used_callback_nonce_survives_a_bridge_restart() {
        // TS persists accepted nonces in `callback-nonces.json`, so a restart
        // cannot replay a token. The bridge must load that file on
        // construction and reject the same token from the second instance.
        let root = tempfile::tempdir().unwrap();
        let token = sign_callback("s3cret", "nonce_restart");
        let event = || {
            card_action_event(json!({
                "__bridge_cb": true,
                "bridge_token": token,
                "choice": "a"
            }))
        };
        {
            let bridge = test_bridge(root.path(), callback_profile("s3cret"));
            assert!(bridge.handle_card_action(&event()).await.is_none());
            assert_eq!(take_batch(&bridge).len(), 1, "first click must forward");
            // Write-through: the nonce file exists before the bridge drops.
            let path = root
                .path()
                .join("profiles")
                .join("test")
                .join("callback-nonces.json");
            let raw = std::fs::read_to_string(&path).unwrap();
            assert!(raw.contains("nonce_restart"), "{raw}");
            assert!(raw.contains("\"used\""), "{raw}");
        }

        let bridge = test_bridge(root.path(), callback_profile("s3cret"));
        assert!(bridge.handle_card_action(&event()).await.is_none());
        assert!(
            take_batch(&bridge).is_empty(),
            "a nonce accepted before the restart must not replay afterwards"
        );
    }

    #[tokio::test]
    async fn expired_callback_nonces_are_pruned_when_the_bridge_starts() {
        let root = tempfile::tempdir().unwrap();
        let path = root
            .path()
            .join("profiles")
            .join("test")
            .join("callback-nonces.json");
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64;
        std::fs::write(
            &path,
            format!(
                r#"{{"n_expired":"used","n_live":"used","__larkline_nonce_expiry":{{"n_expired":1,"n_live":{}}} }}"#,
                now + 60_000
            ),
        )
        .unwrap();

        let _bridge = test_bridge(root.path(), callback_profile("s3cret"));
        let raw = std::fs::read_to_string(&path).unwrap();
        assert!(!raw.contains("n_expired"), "{raw}");
        assert!(raw.contains("n_live"), "{raw}");
    }

    #[tokio::test]
    async fn corrupt_callback_nonce_file_does_not_disable_callbacks() {
        let root = tempfile::tempdir().unwrap();
        let path = root
            .path()
            .join("profiles")
            .join("test")
            .join("callback-nonces.json");
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(&path, b"this is not json").unwrap();

        let bridge = test_bridge(root.path(), callback_profile("s3cret"));
        let event = card_action_event(json!({
            "__bridge_cb": true,
            "bridge_token": sign_callback("s3cret", "nonce_after_corrupt"),
            "choice": "a"
        }));
        assert!(bridge.handle_card_action(&event).await.is_none());
        assert_eq!(
            take_batch(&bridge).len(),
            1,
            "a corrupt nonce store must not disable callbacks"
        );
        // The accepted nonce rewrote the file in the valid format.
        let raw = std::fs::read_to_string(&path).unwrap();
        assert!(raw.contains("nonce_after_corrupt"), "{raw}");
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

                    interrupt: None,
                    approvals: None,
                },
            );
            active.insert(
                scope_b.to_string(),
                ActiveRun {
                    id: 2,
                    abort: handle_b.abort_handle(),

                    interrupt: None,
                    approvals: None,
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

                    interrupt: None,
                    approvals: None,
                },
            );
            active.insert(
                scope_b.to_string(),
                ActiveRun {
                    id: 2,
                    abort: handle_b.abort_handle(),

                    interrupt: None,
                    approvals: None,
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

                interrupt: None,
                approvals: None,
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
        assert_eq!(card_status(&done), "已完成");
        let text = serde_json::to_string(
            build_card(&profile(), &done, card_status(&done), Instant::now(), None).as_value(),
        )
        .unwrap();
        assert!(text.contains("已完成"), "{text}");
        assert!(text.contains("_（未返回内容）_"), "{text}");

        let mut failed = RunView::default();
        failed.apply(&AgentEvent::Error {
            message: "boom".into(),
            termination_reason: TerminationReason::Failed,
        });
        assert_eq!(card_status(&failed), "出错");
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
        assert!(text.contains("出错"), "{text}");
        assert!(text.contains("⚠️ agent 失败：boom"), "{text}");
    }

    #[test]
    fn card_tool_groups_follow_the_ts_collapse_rule() {
        // Fewer than three calls: one panel each, no summary.
        let mut two = RunView::default();
        for (index, name, input) in [
            ("t1", "Bash", json!({ "command": "pwd" })),
            ("t2", "Read", json!({ "file_path": "/a.ts" })),
        ] {
            two.apply(&AgentEvent::ToolUse {
                id: index.into(),
                name: name.into(),
                input,
            });
            two.apply(&AgentEvent::ToolResult {
                id: index.into(),
                output: "ok".into(),
                is_error: false,
            });
        }
        let payload = serde_json::to_string(
            build_card(&profile(), &two, "运行中", Instant::now(), None).as_value(),
        )
        .unwrap();
        assert!(!payload.contains("个工具调用"), "{payload}");
        assert!(payload.contains("✅ **Bash** — pwd"), "{payload}");
        assert!(payload.contains("✅ **Read** — /a.ts"), "{payload}");
        assert_eq!(
            payload.matches("\"tag\":\"collapsible_panel\"").count(),
            2,
            "{payload}"
        );

        // Three or more while running: prior calls collapse into the summary,
        // the latest stays expanded with its body.
        let mut four = RunView::default();
        for index in 1..=4 {
            four.apply(&AgentEvent::ToolUse {
                id: format!("t{index}"),
                name: "Bash".into(),
                input: json!({ "command": format!("step-{index}") }),
            });
        }
        let payload = serde_json::to_string(
            build_card(&profile(), &four, "运行中", Instant::now(), None).as_value(),
        )
        .unwrap();
        assert!(payload.contains("☕ **3 个工具调用**"), "{payload}");
        assert!(payload.contains("⏳ **Bash** — step-4"), "{payload}");
        assert_eq!(
            payload.matches("\"tag\":\"collapsible_panel\"").count(),
            2,
            "{payload}"
        );
        // Only the latest panel keeps a body.
        assert_eq!(payload.matches("**Command**").count(), 1, "{payload}");
        assert!(
            payload.contains("step-1"),
            "summary keeps headers: {payload}"
        );

        // A finished run collapses every call into one summary.
        four.apply(&AgentEvent::Done {
            session_id: None,
            thread_id: None,
            termination_reason: TerminationReason::Normal,
        });
        let payload = serde_json::to_string(
            build_card(&profile(), &four, card_status(&four), Instant::now(), None).as_value(),
        )
        .unwrap();
        assert!(
            payload.contains("☕ **4 个工具调用（已结束）**"),
            "{payload}"
        );
        assert_eq!(
            payload.matches("\"tag\":\"collapsible_panel\"").count(),
            1,
            "{payload}"
        );
        assert!(!payload.contains("\"expanded\":true"), "{payload}");
    }

    #[test]
    fn rich_card_reasoning_panel_follows_the_profile() {
        let mut view = RunView::default();
        view.apply(&AgentEvent::Thinking {
            delta: "checking options".into(),
        });

        for display in [
            None,
            Some(ReasoningDisplay::Auto),
            Some(ReasoningDisplay::Inline),
        ] {
            let mut configured = profile();
            configured
                .preferences
                .get_or_insert_with(Preferences::default)
                .reasoning_display = display;
            let payload = serde_json::to_string(
                build_card(&configured, &view, "运行中", Instant::now(), None).as_value(),
            )
            .unwrap();
            assert!(payload.contains("🧠 **思考中**"), "{display:?}: {payload}");
            assert!(
                payload.contains("checking options"),
                "{display:?}: {payload}"
            );
        }

        for display in [ReasoningDisplay::Off, ReasoningDisplay::Bubble] {
            let mut configured = profile();
            configured
                .preferences
                .get_or_insert_with(Preferences::default)
                .reasoning_display = Some(display);
            let payload = serde_json::to_string(
                build_card(&configured, &view, "运行中", Instant::now(), None).as_value(),
            )
            .unwrap();
            assert!(
                !payload.contains("🧠 **"),
                "{display:?} rendered the panel: {payload}"
            );
            assert!(
                !payload.contains("checking options"),
                "{display:?}: {payload}"
            );
        }
    }

    #[test]
    fn rich_card_timeout_notice_reports_minutes_and_has_no_stop_button() {
        let mut view = RunView::default();
        view.apply(&AgentEvent::Text {
            delta: "partial".into(),
        });
        view.mark_idle_timeout(15);
        let card = build_card(
            &profile(),
            &view,
            card_status(&view),
            Instant::now(),
            Some(("oc_1", "deadbeef")),
        );
        let payload = serde_json::to_string(card.as_value()).unwrap();
        assert!(
            payload.contains("_⏱ 15 分钟无响应,已自动终止_"),
            "{payload}"
        );
        assert_eq!(card.as_value()["config"]["summary"]["content"], "已超时");
        assert!(!payload.contains("__bridge_cb"), "{payload}");
        assert!(!payload.contains("正在输出"), "{payload}");

        // The run loop's 30-minute deadline applies an Error event and pins
        // the minutes, so the card explains the wall-clock kill.
        let mut deadline = RunView::default();
        deadline.apply(&AgentEvent::Error {
            message: "运行超时（30 分钟），已终止。".into(),
            termination_reason: TerminationReason::Timeout,
        });
        deadline.idle_timeout_minutes = Some(RUN_TIMEOUT.as_secs() / 60);
        let payload = serde_json::to_string(
            build_card(
                &profile(),
                &deadline,
                card_status(&deadline),
                Instant::now(),
                None,
            )
            .as_value(),
        )
        .unwrap();
        assert!(
            payload.contains("_⏱ 30 分钟无响应,已自动终止_"),
            "{payload}"
        );
    }

    #[test]
    fn empty_finished_card_uses_the_ts_no_content_note() {
        let mut view = RunView::default();
        view.apply(&AgentEvent::Done {
            session_id: None,
            thread_id: None,
            termination_reason: TerminationReason::Normal,
        });
        let card = build_card(&profile(), &view, card_status(&view), Instant::now(), None);

        // TS `renderCard` renders `_（未返回内容）_`, not the old bare "…".
        assert_eq!(
            card.as_value()["body"]["elements"][0]["content"],
            "_（未返回内容）_"
        );
        assert_ne!(card.as_value()["body"]["elements"][0]["content"], "…");
        assert_eq!(card.as_value()["config"]["summary"]["content"], "已完成");
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
        // `/cd` is an admin command (same gate as `/config set`/`/ws`).
        let mut admin = profile();
        admin.access = Some(Access {
            admins: vec!["ou_1".into()],
            ..Access::default()
        });
        let bridge = test_bridge(root.path(), admin);
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

                    interrupt: None,
                    approvals: None,
                },
            );
            active.insert(
                "oc_1".to_string(),
                ActiveRun {
                    id: 2,
                    abort: chat_run.abort_handle(),

                    interrupt: None,
                    approvals: None,
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
    async fn help_advertises_parity_commands_and_admin_notes() {
        let root = tempfile::tempdir().unwrap();
        let bridge = test_bridge(root.path(), profile());
        let reply = bridge
            .handle_command(&message("om_1", "ou_1", "user", "/help"), "/help")
            .await
            .expect("handled");

        for command in [
            "/new",
            "/reset",
            "/cd",
            "/pwd",
            "/ws",
            "/work",
            "/resume",
            "/timeout",
            "/status",
            "/config",
            "/doctor",
            "/account",
            "/ps",
            "/exit",
            "/reconnect",
            "/doc",
            "/invite",
            "/remove",
            "/stop",
            "/meeting",
            "/help",
        ] {
            assert!(reply.contains(command), "{command} missing: {reply}");
        }
        // The parity fixes are visible in the help text too: `/cd` is
        // admin-gated, supports `~`, and resets the session; `/new` interrupts.
        assert!(reply.contains("/cd <绝对路径|~/子路径>"), "{reply}");
        assert!(reply.contains("会重置 session"), "{reply}");
        assert!(reply.contains("中断当前任务并开启新会话"), "{reply}");
    }

    #[tokio::test]
    async fn unknown_commands_are_left_for_the_agent() {
        let root = tempfile::tempdir().unwrap();
        let bridge = test_bridge(root.path(), profile());

        for text in ["/nope", "/unknown-command", "hello"] {
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

    #[test]
    fn cot_final_view_keeps_only_the_answer_text() {
        let mut view = RunView::default();
        view.apply(&AgentEvent::Thinking {
            delta: "hidden reasoning".into(),
        });
        view.apply(&AgentEvent::ToolUse {
            id: "tool-1".into(),
            name: "Bash".into(),
            input: json!({ "command": "pwd" }),
        });
        view.apply(&AgentEvent::ToolResult {
            id: "tool-1".into(),
            output: "repo".into(),
            is_error: false,
        });
        view.apply(&AgentEvent::Text {
            delta: "the answer".into(),
        });
        view.apply(&AgentEvent::Done {
            session_id: None,
            thread_id: None,
            termination_reason: TerminationReason::Normal,
        });

        // TS `finalAnswerOnlyState`: the COT bubble already showed the
        // process, so the final reply carries only the answer text.
        let final_view = final_answer_only_view(&view);
        assert_eq!(final_view.best_answer(), "the answer");
        assert!(final_view.reasoning.is_empty());
        assert!(!final_view.reasoning_active);
        assert!(final_view.tool_calls.is_empty());
        assert!(final_view.tools.is_empty());
        assert!(final_view.footer_status.is_none());
        assert!(
            final_view
                .blocks
                .iter()
                .all(|block| matches!(block, Block::Text { .. })),
            "tool blocks must be dropped: {:?}",
            final_view.blocks
        );
        // The terminal state is preserved so the final reply still reports
        // interrupts/timeouts.
        assert_eq!(final_view.terminal(), view.terminal());
    }

    #[test]
    fn cot_final_view_preserves_an_interrupted_terminal() {
        let mut view = RunView::default();
        view.apply(&AgentEvent::Text {
            delta: "partial".into(),
        });
        view.mark_interrupted();
        let final_view = final_answer_only_view(&view);
        assert_eq!(final_view.terminal(), Terminal::Interrupted);
        assert_eq!(final_view.best_answer(), "partial");
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
    #[tokio::test]
    async fn pending_queue_survives_a_bridge_restart() {
        let root = tempfile::tempdir().unwrap();
        let message: NormalizedMessage = serde_json::from_value(json!({
            "message_id": "om_persisted",
            "chat_id": "oc_1",
            "chat_type": "p2p",
            "sender_id": "ou_user",
            "sender": { "open_id": "ou_user", "sender_type": "user" },
            "text": "queued while the agent was busy",
        }))
        .expect("message fixture");
        {
            let bridge = test_bridge(root.path(), profile());
            {
                let mut pending = lock(&bridge.pending);
                pending.push("oc_1", message, Instant::now());
            }
            bridge.persist_pending();
            let file = AppPaths::resolve(root.path(), "test")
                .expect("paths")
                .pending_file;
            assert!(file.exists(), "the queue was written through");
        }

        // A fresh bridge on the same state directory restores the message.
        let bridge = test_bridge(root.path(), profile());
        assert_eq!(bridge.restored_pending_count(), 1);
        assert_eq!(
            bridge.queued_scope_list(),
            vec![("oc_1".to_string(), 1)],
            "the restored batch keeps its scope"
        );

        // The access policy is re-checked on restore.
        let mut denied = profile();
        denied.access = Some(Access {
            allowed_users: vec!["ou_someone_else".into()],
            ..Access::default()
        });
        let denied_bridge = test_bridge(root.path(), denied);
        assert_eq!(
            denied_bridge.restored_pending_count(),
            0,
            "a sender removed from the allowlist is not replayed"
        );
    }
}
