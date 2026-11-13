//! COT (chain-of-thought) process-message publishing.
//!
//! Port of `CotPublisher` + `consumeCotEvents` from `src/bot/cot.ts`. One
//! publisher exists per agent run:
//!
//! 1. it creates a single `message_cot` bubble addressed to the chat, with the
//!    triggering message as `origin_message_id` (that is what places the
//!    bubble inside a topic — `message_cot` has no `thread_id` receive type);
//! 2. it folds agent events into COT events (see [`CotStream`]) and ships them
//!    to the bubble in **batches**, at most one `update` per
//!    [`COT_UPDATE_THROTTLE`] window, so a fast reasoning stream cannot turn
//!    into one HTTP request per token;
//! 3. when the run settles it flushes the pending batch and completes the
//!    bubble (`done` for a normal finish, `error` for failure/interrupt/
//!    timeout).
//!
//! Failure is always *soft*. A rejected `create` disables the publisher and
//! the run falls back to the normal progress card; a rejected `update`
//! disables it and no further calls are made (a retry could duplicate or
//! corrupt the bubble); a rejected `complete` is logged. None of these paths
//! return an error to the run, and every HTTP call is bounded by
//! [`COT_REQUEST_TIMEOUT`] so a hung COT endpoint can never pin a run — the
//! same guard the TypeScript `AbortSignal.timeout(COT_REQUEST_TIMEOUT_MS)`
//! provides.
//!
//! ## Which runs publish
//!
//! [`CotPlan::for_profile`] resolves the TypeScript interaction between
//! `cotMessages` and `reasoningDisplay`:
//!
//! | `cotMessages` | `reasoningDisplay` | plan |
//! | --- | --- | --- |
//! | `off` | anything but `bubble` | none |
//! | `off` | `bubble` | reasoning-only bubble, lazily created |
//! | `brief`/`detailed` | `off` | process bubble without reasoning |
//! | `brief`/`detailed` | `auto`/`inline`/`bubble` | process bubble with reasoning |
//!
//! A reasoning-only bubble is created on the first reasoning event so a run
//! without thinking leaves no empty "理解用户问题" bubble behind (TS
//! `ensureStarted`). When a process bubble is live, reasoning is *not*
//! published twice: the caller skips the progress card for the run (the
//! TypeScript COT branch returns before the card branch), and the final answer
//! is rendered without the reasoning/tool panels.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex as StdMutex};
use std::time::Duration;

use larkline_agent::{AgentEvent, TerminationReason};
use larkline_core::ProfileConfig;
use larkline_core::config::{CotMessagesMode, ReasoningDisplay};
use larkline_lark::cot::{CotCompleteReason, CotEvent, CotEventType, CotRef, CotTransport};
use serde_json::{Value, json};
use tokio::sync::{mpsc, oneshot};

use crate::render::{ToolCall, ToolInput, ToolStatus, tool_header_text};

/// Minimum spacing between two `update` calls (TS `COT_UPDATE_THROTTLE_MS`).
pub const COT_UPDATE_THROTTLE: Duration = Duration::from_millis(600);
/// Cap on one tool output in `detailed` mode (TS `COT_TOOL_OUTPUT_MAX`).
pub const COT_TOOL_OUTPUT_MAX_CHARS: usize = 1_200;
/// Cap on one text/reasoning delta (TS `COT_TEXT_MAX`).
pub const COT_TEXT_MAX_CHARS: usize = 4_000;
/// Bound for every COT HTTP call (TS `COT_REQUEST_TIMEOUT_MS`).
pub const COT_REQUEST_TIMEOUT: Duration = Duration::from_secs(15);
/// Bound on waiting for the worker to flush + complete during [`CotPublisher::finish`].
const COT_FINISH_TIMEOUT: Duration = Duration::from_secs(35);

/// Notice sent when COT publishing degraded mid-run (TS
/// `sendCotDegradedNotice`).
pub const COT_DEGRADED_NOTICE: &str =
    "COT 过程消息更新失败，已停止展示过程；最终答案仍会继续发送。";

const UNDERSTAND_STEP_NAME: &str = "理解用户问题";
const FINAL_STEP_NAME: &str = "输出过程";
const TOOL_FALLBACK_RESULT: &str = "工具调用已完成";

/// How the run is mirrored into the bubble.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CotRunMode {
    /// Full process: steps, tools, text and reasoning.
    Process,
    /// Reasoning stream only (`reasoningDisplay: bubble` with COT off).
    ReasoningOnly,
}

/// Resolved COT behavior for one run.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CotPlan {
    /// Tool/text detail level. Always `Brief` for a reasoning-only bubble.
    pub detail: CotMessagesMode,
    /// What the bubble carries.
    pub mode: CotRunMode,
    /// Whether `thinking` events are published at all.
    pub include_reasoning: bool,
}

impl CotPlan {
    /// Resolve the profile preferences into a plan, or `None` when the run
    /// publishes no COT message at all.
    ///
    /// This is the TypeScript block in `channel.ts`:
    ///
    /// ```text
    /// cotMessages   = getCotMessages(cfg)
    /// cotEnabled    = cotMessages !== 'off'
    /// reasoningBubble = !cotEnabled && reasoningDisplay === 'bubble'
    /// detail        = cotEnabled ? cotMessages : 'brief'
    /// includeReasoning = reasoningDisplay !== 'off'
    /// ```
    pub fn for_profile(profile: &ProfileConfig) -> Option<Self> {
        let detail = profile.cot_messages();
        let display = profile.reasoning_display();
        if detail.is_enabled() {
            Some(Self {
                detail,
                mode: CotRunMode::Process,
                // `reasoningDisplay: off` hides reasoning everywhere, including
                // the process bubble; tools still stream.
                include_reasoning: display != ReasoningDisplay::Off,
            })
        } else if display == ReasoningDisplay::Bubble {
            // COT off + bubble: a dedicated reasoning-only bubble. This never
            // coexists with a process bubble, so reasoning is not duplicated.
            Some(Self {
                detail: CotMessagesMode::Brief,
                mode: CotRunMode::ReasoningOnly,
                include_reasoning: true,
            })
        } else {
            None
        }
    }
}

/// Identity of one COT run.
#[derive(Debug, Clone)]
pub struct CotRunParams {
    /// Chat the bubble is addressed to (`receive_id_type=chat_id`).
    pub chat_id: String,
    /// Triggering message; the bubble inherits its thread in a topic group.
    pub origin_message_id: String,
    /// Run id used in step/message ids (`run-…`).
    pub run_id: String,
    /// Run scope (`chatId` or `chatId:threadId`).
    pub scope: String,
    /// Preview of the user's message(s), shown in `RUN_STARTED.input.query`.
    pub input_preview: String,
}

/// One tool call remembered until its result arrives (TS `toolBrief`).
struct ToolBrief {
    name: String,
    input: Value,
}

/// Folds [`AgentEvent`]s into COT events.
///
/// Pure and synchronous: the publisher only adds the transport and the
/// batching, which keeps the formatting (brief vs detailed, truncation,
/// reasoning/text/tool state machine) unit-testable without a network.
pub struct CotStream {
    run_id: String,
    scope: String,
    input_preview: String,
    detail: CotMessagesMode,
    mode: CotRunMode,
    include_reasoning: bool,
    reasoning_open: bool,
    text_step_open: bool,
    text_message_open: bool,
    text_message_index: usize,
    text_message_id: Option<String>,
    tool_brief: HashMap<String, ToolBrief>,
    reasoning_message_id: String,
    final_step_id: String,
}

impl CotStream {
    /// Build the stream state for one run.
    pub fn new(params: &CotRunParams, plan: CotPlan) -> Self {
        Self {
            run_id: params.run_id.clone(),
            scope: params.scope.clone(),
            input_preview: params.input_preview.clone(),
            detail: plan.detail,
            mode: plan.mode,
            include_reasoning: plan.include_reasoning,
            reasoning_open: false,
            text_step_open: false,
            text_message_open: false,
            text_message_index: 0,
            text_message_id: None,
            tool_brief: HashMap::new(),
            reasoning_message_id: format!("reasoning-{}", params.run_id),
            final_step_id: format!("step-process-{}", params.run_id),
        }
    }

    /// Events the bubble receives right after `create` (TS `start`).
    pub fn started_events(&self) -> Vec<CotEvent> {
        vec![
            CotEvent::now(
                CotEventType::RunStarted,
                json!({
                    "threadId": self.scope,
                    "runId": self.run_id,
                    "input": { "query": self.input_preview },
                }),
            ),
            CotEvent::now(
                CotEventType::StepStarted,
                json!({
                    "stepId": format!("step-understand-{}", self.run_id),
                    "stepName": UNDERSTAND_STEP_NAME,
                }),
            ),
        ]
    }

    /// Fold one agent event into zero or more COT events.
    ///
    /// `done` / `error` events produce nothing here: the run loop reports the
    /// terminal through [`CotStream::terminal`] because Rust also has to
    /// synthesize terminals for interrupts and timeouts that never arrive as
    /// agent events.
    pub fn observe(&mut self, event: &AgentEvent) -> Vec<CotEvent> {
        match event {
            AgentEvent::System { .. } | AgentEvent::Usage { .. } => Vec::new(),
            AgentEvent::Thinking { delta } => self.observe_thinking(delta),
            _ if self.mode == CotRunMode::ReasoningOnly => Vec::new(),
            AgentEvent::ToolUse { id, name, input } => self.observe_tool_use(id, name, input),
            AgentEvent::ToolResult {
                id,
                output,
                is_error,
            } => self.observe_tool_result(id, output, *is_error),
            AgentEvent::Text { delta } => self.observe_text(delta),
            AgentEvent::FinalText { .. } => Vec::new(),
            AgentEvent::PermissionRequest { .. } | AgentEvent::PermissionCancelled { .. } => {
                Vec::new()
            }
            AgentEvent::Done { .. } | AgentEvent::Error { .. } => Vec::new(),
        }
    }

    /// Close the open segments and emit the run terminal.
    ///
    /// Returns the events that must be appended before completing the bubble.
    /// The complete reason is resolved separately through
    /// [`CotStream::complete_reason`].
    pub fn terminal(
        &mut self,
        termination: TerminationReason,
        error_message: Option<&str>,
    ) -> Vec<CotEvent> {
        let mut events = Vec::new();
        events.extend(self.close_reasoning());
        events.extend(self.close_text());
        if self.mode == CotRunMode::Process && self.text_step_open {
            self.text_step_open = false;
            events.push(CotEvent::now(
                CotEventType::StepFinished,
                json!({
                    "stepId": self.final_step_id,
                    "stepName": FINAL_STEP_NAME,
                }),
            ));
        }
        // TS skips the terminal event in `reasoning-only` mode; the run status
        // is not part of a reasoning bubble.
        if self.mode == CotRunMode::Process {
            match termination {
                TerminationReason::Normal => events.push(CotEvent::now(
                    CotEventType::RunFinished,
                    json!({
                        "threadId": self.scope,
                        "runId": self.run_id,
                        "status": "done",
                    }),
                )),
                TerminationReason::Interrupted => events.push(CotEvent::now(
                    CotEventType::RunFinished,
                    json!({
                        "threadId": self.scope,
                        "runId": self.run_id,
                        "status": "interrupted",
                    }),
                )),
                TerminationReason::Timeout => events.push(CotEvent::now(
                    CotEventType::RunFinished,
                    json!({
                        "threadId": self.scope,
                        "runId": self.run_id,
                        "status": "timeout",
                    }),
                )),
                TerminationReason::Failed => events.push(CotEvent::now(
                    CotEventType::RunError,
                    json!({
                        "message": error_message.unwrap_or("运行失败"),
                        "code": "failed",
                    }),
                )),
            }
        }
        events
    }

    /// The completion reason for a terminal.
    ///
    /// Only `done` and `error` exist on the wire; the TypeScript publisher
    /// uses exactly this rule (`finish(status === 'done' ? 'done' : 'error')`).
    pub fn complete_reason(termination: TerminationReason) -> CotCompleteReason {
        match termination {
            TerminationReason::Normal => CotCompleteReason::Done,
            TerminationReason::Failed
            | TerminationReason::Interrupted
            | TerminationReason::Timeout => CotCompleteReason::Error,
        }
    }

    fn observe_thinking(&mut self, delta: &str) -> Vec<CotEvent> {
        if !self.include_reasoning {
            return Vec::new();
        }
        let mut events = Vec::new();
        if self.mode == CotRunMode::Process {
            // TS closes the text message before re-entering reasoning; the
            // next text delta starts a new message id.
            events.extend(self.close_text());
        }
        if !self.reasoning_open {
            self.reasoning_open = true;
            events.push(CotEvent::now(
                CotEventType::ReasoningStart,
                json!({ "messageId": self.reasoning_message_id }),
            ));
            events.push(CotEvent::now(
                CotEventType::ReasoningMessageStart,
                json!({
                    "messageId": self.reasoning_message_id,
                    "role": "reasoning",
                }),
            ));
        }
        events.push(CotEvent::now(
            CotEventType::ReasoningMessageContent,
            json!({
                "messageId": self.reasoning_message_id,
                "delta": truncate_cot(delta, COT_TEXT_MAX_CHARS),
            }),
        ));
        events
    }

    fn observe_tool_use(&mut self, id: &str, name: &str, input: &Value) -> Vec<CotEvent> {
        let mut events = Vec::new();
        events.extend(self.close_reasoning());
        events.extend(self.close_text());
        let detailed = self.detail == CotMessagesMode::Detailed;
        // TS `showSummary` is `brief || detailed`, i.e. always true, so the
        // title is always the legacy tool header.
        let title = cot_brief_tool_title(name, input, ToolStatus::Running);
        self.tool_brief.insert(
            id.to_string(),
            ToolBrief {
                name: name.to_string(),
                input: input.clone(),
            },
        );
        events.push(CotEvent::now(
            CotEventType::ToolCallStart,
            json!({
                "toolCallId": id,
                "icon": cot_tool_icon(name),
                "title": title,
                "toolCallName": name,
            }),
        ));
        if detailed {
            // TS `TOOL_CALL_ARGS.delta` is `JSON.stringify(evt.input)`.
            events.push(CotEvent::now(
                CotEventType::ToolCallArgs,
                json!({ "toolCallId": id, "delta": input.to_string() }),
            ));
        }
        events.push(CotEvent::now(
            CotEventType::ToolCallEnd,
            json!({ "toolCallId": id }),
        ));
        events
    }

    fn observe_tool_result(&mut self, id: &str, output: &str, is_error: bool) -> Vec<CotEvent> {
        let detailed = self.detail == CotMessagesMode::Detailed;
        let brief = self.tool_brief.remove(id);
        let content = if detailed {
            truncate_cot(output, COT_TOOL_OUTPUT_MAX_CHARS)
        } else if let Some(brief) = brief {
            let status = if is_error {
                ToolStatus::Error
            } else {
                ToolStatus::Done
            };
            cot_brief_tool_title(&brief.name, &brief.input, status)
        } else {
            TOOL_FALLBACK_RESULT.to_string()
        };
        vec![CotEvent::now(
            CotEventType::ToolCallResult,
            json!({
                "messageId": format!("tool-result-{id}"),
                "toolCallId": id,
                "role": "tool",
                "content": content,
            }),
        )]
    }

    fn observe_text(&mut self, delta: &str) -> Vec<CotEvent> {
        let mut events = self.close_reasoning();
        if !self.text_step_open {
            self.text_step_open = true;
            events.push(CotEvent::now(
                CotEventType::StepStarted,
                json!({
                    "stepId": self.final_step_id,
                    "stepName": FINAL_STEP_NAME,
                }),
            ));
        }
        if !self.text_message_open {
            self.text_message_open = true;
            self.text_message_index += 1;
            let message_id = format!("text-{}-{}", self.run_id, self.text_message_index);
            self.text_message_id = Some(message_id.clone());
            events.push(CotEvent::now(
                CotEventType::TextMessageStart,
                json!({ "messageId": message_id, "role": "assistant" }),
            ));
        }
        events.push(CotEvent::now(
            CotEventType::TextMessageContent,
            json!({
                "messageId": self.text_message_id,
                "delta": truncate_cot(delta, COT_TEXT_MAX_CHARS),
            }),
        ));
        events
    }

    fn close_reasoning(&mut self) -> Vec<CotEvent> {
        if !self.reasoning_open {
            return Vec::new();
        }
        self.reasoning_open = false;
        vec![
            CotEvent::now(
                CotEventType::ReasoningMessageEnd,
                json!({ "messageId": self.reasoning_message_id }),
            ),
            CotEvent::now(
                CotEventType::ReasoningEnd,
                json!({ "messageId": self.reasoning_message_id }),
            ),
        ]
    }

    fn close_text(&mut self) -> Vec<CotEvent> {
        let Some(message_id) = self.text_message_id.take() else {
            return Vec::new();
        };
        self.text_message_open = false;
        vec![CotEvent::now(
            CotEventType::TextMessageEnd,
            json!({ "messageId": message_id }),
        )]
    }
}

/// Brief tool title: the legacy tool header without markdown bold markers.
///
/// TS `cotBriefToolTitle` = `toolHeaderText(...).replace(/\*\*/g, '')`.
pub fn cot_brief_tool_title(name: &str, input: &Value, status: ToolStatus) -> String {
    let tool = ToolCall {
        id: "cot-tool".to_string(),
        name: name.to_string(),
        input: ToolInput::from_value(input),
        status,
        output: None,
    };
    tool_header_text(&tool).replace("**", "")
}

/// The icon name for a tool call (TS `cotToolIcon`).
pub fn cot_tool_icon(name: &str) -> &'static str {
    let lower = name.to_lowercase();
    if lower.contains("search") || lower.contains("grep") || lower.contains("rg") {
        "search"
    } else if lower.contains("read") {
        "read"
    } else if lower.contains("write") || lower.contains("edit") {
        "write"
    } else if lower.contains("doc") {
        "doc"
    } else if lower.contains("calendar") {
        "calendar"
    } else if lower.contains("task") {
        "task"
    } else if lower.contains("command") || lower.contains("bash") {
        "bash"
    } else {
        "default"
    }
}

/// TS `truncateCot`: cap the text and append `...` when it was cut.
fn truncate_cot(value: &str, max: usize) -> String {
    if value.chars().count() > max {
        let mut out: String = value.chars().take(max).collect();
        out.push_str("...");
        out
    } else {
        value.to_string()
    }
}

#[derive(Default)]
struct PublishState {
    started: AtomicBool,
    disabled: AtomicBool,
    degraded_reason: StdMutex<Option<String>>,
}

enum Command {
    Events(Vec<CotEvent>),
    Finish {
        reason: CotCompleteReason,
        ack: oneshot::Sender<()>,
    },
}

/// Handle to one run's COT bubble.
///
/// Dropping the handle without [`CotPublisher::finish`] (for example when a
/// run future is aborted) flushes what was already buffered and leaves the
/// bubble open, mirroring the TypeScript process being killed.
pub struct CotPublisher {
    stream: CotStream,
    tx: Option<mpsc::UnboundedSender<Command>>,
    state: Arc<PublishState>,
    reference: Option<CotRef>,
}

impl CotPublisher {
    /// Create the bubble and start the debounce worker.
    ///
    /// Never fails: a rejected `create` returns a disabled publisher so the
    /// caller can fall back to the normal reply path.
    pub async fn start(
        transport: Arc<dyn CotTransport>,
        params: CotRunParams,
        plan: CotPlan,
    ) -> Self {
        Self::start_with_throttle(transport, params, plan, COT_UPDATE_THROTTLE).await
    }

    async fn start_with_throttle(
        transport: Arc<dyn CotTransport>,
        params: CotRunParams,
        plan: CotPlan,
        throttle: Duration,
    ) -> Self {
        let state = Arc::new(PublishState::default());
        let stream = CotStream::new(&params, plan);
        let started_events = stream.started_events();

        let created = tokio::time::timeout(
            COT_REQUEST_TIMEOUT,
            transport.create(&params.chat_id, Some(&params.origin_message_id)),
        )
        .await;
        let reference = match created {
            Ok(Ok(reference)) => reference,
            Ok(Err(error)) => {
                disable(&state, error.to_string());
                tracing::warn!(
                    chat = %params.chat_id,
                    error = %error,
                    "cannot create COT message; falling back to the normal reply path"
                );
                return Self {
                    stream,
                    tx: None,
                    state,
                    reference: None,
                };
            }
            Err(_) => {
                let reason = format!(
                    "message_cot create timed out after {}s",
                    COT_REQUEST_TIMEOUT.as_secs()
                );
                disable(&state, reason.clone());
                tracing::warn!(chat = %params.chat_id, reason = %reason, "cannot create COT message");
                return Self {
                    stream,
                    tx: None,
                    state,
                    reference: None,
                };
            }
        };

        let (tx, rx) = mpsc::unbounded_channel();
        state.started.store(true, Ordering::SeqCst);
        tracing::info!(
            cot_id = %reference.cot_id,
            message_id = %reference.message_id,
            "COT message created"
        );
        tokio::spawn(run_worker(
            transport,
            reference.clone(),
            rx,
            state.clone(),
            throttle,
        ));
        let publisher = Self {
            stream,
            tx: Some(tx),
            state,
            reference: Some(reference),
        };
        // RUN_STARTED + the "understand" step are part of `create`, like TS.
        publisher.enqueue(started_events);
        publisher
    }

    /// The live bubble reference, once created.
    pub fn reference(&self) -> Option<&CotRef> {
        self.reference.as_ref()
    }

    /// Whether `create` succeeded.
    pub fn is_started(&self) -> bool {
        self.state.started.load(Ordering::SeqCst)
    }

    /// Whether a COT call failed and publishing stopped.
    pub fn is_disabled(&self) -> bool {
        self.state.disabled.load(Ordering::SeqCst)
    }

    /// Whether there is a live, usable bubble.
    pub fn is_enabled(&self) -> bool {
        self.is_started() && !self.is_disabled()
    }

    /// The first failure that disabled the publisher, if any.
    pub fn degraded_reason(&self) -> Option<String> {
        lock(&self.state.degraded_reason).clone()
    }

    /// Fold one agent event and queue the resulting COT events.
    ///
    /// Non-blocking: the worker batches and flushes them.
    pub fn observe(&mut self, event: &AgentEvent) {
        if !self.is_enabled() {
            return;
        }
        let events = self.stream.observe(event);
        self.enqueue(events);
    }

    /// Flush the buffer and complete the bubble.
    ///
    /// Consumes the handle; call it on every terminal path (normal finish,
    /// interrupt, timeout and failure). Bounded by [`COT_FINISH_TIMEOUT`] so a
    /// hung endpoint cannot delay the final reply indefinitely. Returns the
    /// degraded reason when an update failed and the bubble was left open, so
    /// the caller can tell the user (TS `sendCotDegradedNotice`).
    pub async fn finish(
        mut self,
        termination: TerminationReason,
        error: Option<&str>,
    ) -> Option<String> {
        self.finish_inner(termination, error).await;
        self.degraded_reason()
    }

    async fn finish_inner(&mut self, termination: TerminationReason, error: Option<&str>) {
        if !self.is_started() {
            // No bubble was ever created (reasoning-only run without thinking).
            return;
        }
        let tail = self.stream.terminal(termination, error);
        let reason = CotStream::complete_reason(termination);
        let Some(tx) = self.tx.take() else {
            return;
        };
        if tx.send(Command::Events(tail)).is_err() {
            return;
        }
        let (ack, ack_rx) = oneshot::channel();
        if tx.send(Command::Finish { reason, ack }).is_err() {
            return;
        }
        match tokio::time::timeout(COT_FINISH_TIMEOUT, ack_rx).await {
            Ok(Ok(())) => {
                if self.is_disabled() {
                    tracing::warn!(
                        reason = ?self.degraded_reason(),
                        "COT message left open: publishing was disabled"
                    );
                } else {
                    tracing::info!(reason = reason.as_str(), "COT message completed");
                }
            }
            Ok(Err(_)) => {}
            Err(_) => {
                tracing::warn!(
                    timeout_secs = COT_FINISH_TIMEOUT.as_secs(),
                    "timed out waiting for the COT worker to finish"
                );
            }
        }
    }

    fn enqueue(&self, events: Vec<CotEvent>) {
        if events.is_empty() {
            return;
        }
        let Some(tx) = self.tx.as_ref() else {
            return;
        };
        // An unbounded send only fails when the worker is gone; that is not a
        // run failure, so it is ignored like every other COT error.
        let _ = tx.send(Command::Events(events));
    }
}

/// Per-run COT wiring: plan resolution, eager/lazy start and terminal completion.
///
/// This is the piece the run flow owns. It exists so the exact TypeScript
/// branch in `channel.ts` (`cotEnabled || reasoningBubble`, eager `start` for a
/// process bubble, lazy `ensureStarted` for a reasoning bubble) can be unit
/// tested with a fake transport, without a [`crate::run::Bridge`] or a network:
///
/// * `prepare` returns `None` when the profile publishes no COT message, in
///   which case the run uses the normal progress card and no COT call is ever
///   made;
/// * `start` creates the process bubble (eager) and reports whether the run
///   may use the COT path; a rejected create means "fall back to the card";
/// * `observe` forwards agent events and creates a reasoning-only bubble on
///   the first reasoning delta;
/// * `finish` completes the bubble on every terminal path.
pub struct CotRun {
    transport: Arc<dyn CotTransport>,
    params: CotRunParams,
    /// The plan, until publishing starts. A process plan is consumed by
    /// [`CotRun::start`]; a reasoning-only plan waits for the first reasoning
    /// event in [`CotRun::observe`].
    pending_plan: Option<CotPlan>,
    /// Detail of the plan that owns this run (process or reasoning-only).
    mode: CotRunMode,
    publisher: Option<CotPublisher>,
}

impl CotRun {
    /// Prepare the COT path for one run, or `None` when it publishes nothing.
    pub fn prepare(
        plan: Option<CotPlan>,
        transport: Arc<dyn CotTransport>,
        params: CotRunParams,
    ) -> Option<Self> {
        let plan = plan?;
        Some(Self {
            transport,
            params,
            mode: plan.mode,
            pending_plan: Some(plan),
            publisher: None,
        })
    }

    /// Whether the run publishes a COT message (process or reasoning-only).
    pub fn is_active(&self) -> bool {
        self.publisher.is_some() || self.pending_plan.is_some()
    }

    /// Start publishing.
    ///
    /// A process plan creates the bubble now; a reasoning-only plan stays
    /// lazy. Returns `false` only when an eager create was rejected, which is
    /// the caller's signal to fall back to the progress card (TS
    /// `fallback-existing-reply`).
    pub async fn start(&mut self) -> bool {
        if self.mode == CotRunMode::ReasoningOnly {
            return true;
        }
        let Some(plan) = self.pending_plan.take() else {
            // Already started (or a previous attempt failed); keep the current
            // state instead of creating a second bubble.
            return self
                .publisher
                .as_ref()
                .is_some_and(CotPublisher::is_enabled);
        };
        let publisher =
            CotPublisher::start(self.transport.clone(), self.params.clone(), plan).await;
        if publisher.is_enabled() {
            self.publisher = Some(publisher);
            true
        } else {
            tracing::warn!(
                reason = ?publisher.degraded_reason(),
                "COT create failed; falling back to the progress card"
            );
            false
        }
    }

    /// Fold one agent event into the bubble, starting a reasoning-only bubble
    /// on the first reasoning delta.
    pub async fn observe(&mut self, event: &AgentEvent) {
        if self.publisher.is_none()
            && self.mode == CotRunMode::ReasoningOnly
            && self.pending_plan.is_some()
            && matches!(event, AgentEvent::Thinking { .. })
        {
            if let Some(plan) = self.pending_plan.take() {
                let publisher =
                    CotPublisher::start(self.transport.clone(), self.params.clone(), plan).await;
                if publisher.is_enabled() {
                    self.publisher = Some(publisher);
                } else {
                    tracing::warn!(
                        reason = ?publisher.degraded_reason(),
                        "cannot create the reasoning COT bubble"
                    );
                }
            }
        }
        if let Some(publisher) = self.publisher.as_mut() {
            publisher.observe(event);
        }
    }

    /// Complete the bubble on a terminal path (normal, interrupt, timeout or
    /// failure). A reasoning-only run that never received reasoning has no
    /// bubble and completes nothing. Never fails the run; returns the degraded
    /// reason when publishing stopped mid-run (the caller sends the TS
    /// degraded notice).
    pub async fn finish(
        self,
        termination: TerminationReason,
        error: Option<&str>,
    ) -> Option<String> {
        self.publisher?.finish(termination, error).await
    }
}

/// Worker: batch queued events and flush at most once per `throttle` window.
async fn run_worker(
    transport: Arc<dyn CotTransport>,
    reference: CotRef,
    mut rx: mpsc::UnboundedReceiver<Command>,
    state: Arc<PublishState>,
    throttle: Duration,
) {
    let mut buffer: Vec<CotEvent> = Vec::new();
    let mut deadline: Option<tokio::time::Instant> = None;
    loop {
        let tick = async {
            match deadline {
                Some(at) => tokio::time::sleep_until(at).await,
                None => std::future::pending().await,
            }
        };
        tokio::select! {
            command = rx.recv() => {
                match command {
                    Some(Command::Events(events)) => {
                        buffer.extend(events);
                        if deadline.is_none() {
                            deadline = Some(tokio::time::Instant::now() + throttle);
                        }
                    }
                    Some(Command::Finish { reason, ack }) => {
                        flush(&transport, &reference, &mut buffer, &state).await;
                        if !state.disabled.load(Ordering::SeqCst) {
                            complete(&transport, &reference, reason).await;
                        }
                        let _ = ack.send(());
                        return;
                    }
                    None => {
                        // Handle dropped without finish: flush what was
                        // buffered and leave the bubble open.
                        flush(&transport, &reference, &mut buffer, &state).await;
                        return;
                    }
                }
            }
            _ = tick => {
                deadline = None;
                flush(&transport, &reference, &mut buffer, &state).await;
            }
        }
    }
}

async fn flush(
    transport: &Arc<dyn CotTransport>,
    reference: &CotRef,
    buffer: &mut Vec<CotEvent>,
    state: &Arc<PublishState>,
) {
    if buffer.is_empty() {
        return;
    }
    if state.disabled.load(Ordering::SeqCst) {
        buffer.clear();
        return;
    }
    let events = std::mem::take(buffer);
    match tokio::time::timeout(COT_REQUEST_TIMEOUT, transport.update(reference, &events)).await {
        Ok(Ok(())) => {}
        Ok(Err(error)) => {
            // TS disables on the first failed update and never retries: a
            // rejected batch may already have been partially applied, and a
            // retry would duplicate events in the bubble.
            disable(state, error.to_string());
            tracing::warn!(error = %error, "COT update failed; stopping process messages");
        }
        Err(_) => {
            let reason = format!(
                "message_cot update timed out after {}s",
                COT_REQUEST_TIMEOUT.as_secs()
            );
            disable(state, reason.clone());
            tracing::warn!(reason = %reason, "COT update timed out");
        }
    }
}

async fn complete(
    transport: &Arc<dyn CotTransport>,
    reference: &CotRef,
    reason: CotCompleteReason,
) {
    match tokio::time::timeout(COT_REQUEST_TIMEOUT, transport.complete(reference, reason)).await {
        Ok(Ok(())) => {}
        Ok(Err(error)) => {
            tracing::warn!(error = %error, "COT complete failed");
        }
        Err(_) => {
            tracing::warn!(
                timeout_secs = COT_REQUEST_TIMEOUT.as_secs(),
                "COT complete timed out"
            );
        }
    }
}

fn disable(state: &PublishState, reason: String) {
    state.disabled.store(true, Ordering::SeqCst);
    *lock(&state.degraded_reason) = Some(reason);
}

fn lock<T>(mutex: &StdMutex<T>) -> std::sync::MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(|poison| poison.into_inner())
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use larkline_lark::RestError;
    use std::sync::Mutex;

    fn params(run_id: &str) -> CotRunParams {
        CotRunParams {
            chat_id: "oc_chat".to_string(),
            origin_message_id: "om_origin".to_string(),
            run_id: run_id.to_string(),
            scope: "oc_chat:omt_topic".to_string(),
            input_preview: "draw a bear".to_string(),
        }
    }

    fn plan(detail: CotMessagesMode, mode: CotRunMode, include_reasoning: bool) -> CotPlan {
        CotPlan {
            detail,
            mode,
            include_reasoning,
        }
    }

    fn stream_event_types(events: &[CotEvent]) -> Vec<&str> {
        events
            .iter()
            .map(|event| event.event_type.as_str())
            .collect()
    }

    fn contents(events: &[CotEvent], event_type: CotEventType) -> Vec<Value> {
        events
            .iter()
            .filter(|event| event.event_type == event_type.as_str())
            .map(CotEvent::content_value)
            .collect()
    }

    fn profile_with(
        cot: Option<CotMessagesMode>,
        display: Option<ReasoningDisplay>,
    ) -> ProfileConfig {
        let mut profile = ProfileConfig::default();
        let prefs = profile.preferences.get_or_insert_with(Default::default);
        prefs.cot_messages = cot;
        prefs.reasoning_display = display;
        profile
    }

    // ---- plan resolution --------------------------------------------------

    #[test]
    fn cot_off_without_a_bubble_display_publishes_nothing() {
        for display in [
            None,
            Some(ReasoningDisplay::Auto),
            Some(ReasoningDisplay::Off),
            Some(ReasoningDisplay::Inline),
        ] {
            assert_eq!(
                CotPlan::for_profile(&profile_with(None, display)),
                None,
                "display={display:?}"
            );
            assert_eq!(
                CotPlan::for_profile(&profile_with(Some(CotMessagesMode::Off), display)),
                None,
                "display={display:?}"
            );
        }
    }

    #[test]
    fn cot_modes_resolve_to_a_process_plan() {
        for (detail, expected) in [
            (CotMessagesMode::Brief, CotMessagesMode::Brief),
            (CotMessagesMode::Detailed, CotMessagesMode::Detailed),
        ] {
            let plan =
                CotPlan::for_profile(&profile_with(Some(detail), Some(ReasoningDisplay::Auto)))
                    .expect("process plan");
            assert_eq!(plan.mode, CotRunMode::Process);
            assert_eq!(plan.detail, expected);
            assert!(plan.include_reasoning);
        }
    }

    #[test]
    fn reasoning_display_off_keeps_process_tools_but_drops_reasoning() {
        let plan = CotPlan::for_profile(&profile_with(
            Some(CotMessagesMode::Brief),
            Some(ReasoningDisplay::Off),
        ))
        .expect("process plan");
        assert_eq!(plan.mode, CotRunMode::Process);
        assert!(!plan.include_reasoning);

        // No reasoning events at all when the delta is hidden.
        let mut stream = CotStream::new(&params("run-1"), plan);
        let events = stream.observe(&AgentEvent::Thinking {
            delta: "hidden".into(),
        });
        assert!(events.is_empty());
    }

    #[test]
    fn bubble_display_with_cot_off_is_a_reasoning_only_bubble() {
        let plan = CotPlan::for_profile(&profile_with(None, Some(ReasoningDisplay::Bubble)))
            .expect("reasoning bubble plan");
        assert_eq!(plan.mode, CotRunMode::ReasoningOnly);
        assert_eq!(plan.detail, CotMessagesMode::Brief);
        assert!(plan.include_reasoning);

        // A process plan wins when COT is enabled: reasoning is not duplicated
        // into a second, reasoning-only bubble.
        let plan = CotPlan::for_profile(&profile_with(
            Some(CotMessagesMode::Brief),
            Some(ReasoningDisplay::Bubble),
        ))
        .expect("process plan");
        assert_eq!(plan.mode, CotRunMode::Process);
    }

    // ---- formatting -------------------------------------------------------

    #[test]
    fn brief_mode_publishes_tool_summaries_without_args_or_output() {
        let mut stream = CotStream::new(
            &params("run-1"),
            plan(CotMessagesMode::Brief, CotRunMode::Process, true),
        );
        let mut all = stream.started_events();
        all.extend(stream.observe(&AgentEvent::Text {
            delta: "我会先生成图片。".into(),
        }));
        all.extend(stream.observe(&AgentEvent::ToolUse {
            id: "tool-1".into(),
            name: "command_execution".into(),
            input: json!({ "command": "echo bear" }),
        }));
        all.extend(stream.observe(&AgentEvent::ToolResult {
            id: "tool-1".into(),
            output: "/private/secret/path".into(),
            is_error: false,
        }));
        all.extend(stream.observe(&AgentEvent::Text {
            delta: "图片已经生成。".into(),
        }));
        all.extend(stream.terminal(TerminationReason::Normal, None));

        assert_eq!(all[0].event_type, "RUN_STARTED");
        assert_eq!(all[1].event_type, "STEP_STARTED");
        let types = stream_event_types(&all);
        for expected in [
            "TEXT_MESSAGE_START",
            "TEXT_MESSAGE_CONTENT",
            "TEXT_MESSAGE_END",
            "TOOL_CALL_START",
            "TOOL_CALL_RESULT",
            "STEP_FINISHED",
            "RUN_FINISHED",
        ] {
            assert!(types.contains(&expected), "missing {expected} in {types:?}");
        }
        // Brief mode never ships raw tool args or output.
        assert!(!types.contains(&"TOOL_CALL_ARGS"));
        assert!(
            !all.iter()
                .any(|event| event.content.contains("/private/secret/path")),
            "brief mode leaked the tool output"
        );

        let tool_start = contents(&all, CotEventType::ToolCallStart);
        assert_eq!(tool_start[0]["toolCallId"], "tool-1");
        assert_eq!(tool_start[0]["icon"], "bash");
        assert_eq!(tool_start[0]["toolCallName"], "command_execution");
        // `⏳ command_execution — echo bear` with the bold markers stripped.
        assert_eq!(tool_start[0]["title"], "⏳ command_execution — echo bear");

        let results = contents(&all, CotEventType::ToolCallResult);
        assert_eq!(results[0]["toolCallId"], "tool-1");
        assert_eq!(results[0]["role"], "tool");
        assert_eq!(results[0]["content"], "✅ command_execution — echo bear");

        // The text deltas are the exact content of the two text events.
        let deltas: Vec<Value> = contents(&all, CotEventType::TextMessageContent)
            .into_iter()
            .map(|content| content["delta"].clone())
            .collect();
        assert_eq!(deltas, [json!("我会先生成图片。"), json!("图片已经生成。")]);
        // Two text segments => two message starts/ends.
        assert_eq!(contents(&all, CotEventType::TextMessageStart).len(), 2);
        assert_eq!(contents(&all, CotEventType::TextMessageEnd).len(), 2);

        let finished = contents(&all, CotEventType::RunFinished);
        assert_eq!(finished[0]["status"], "done");
        assert_eq!(finished[0]["runId"], "run-1");
        assert_eq!(finished[0]["threadId"], "oc_chat:omt_topic");
    }

    #[test]
    fn detailed_mode_includes_args_and_truncated_tool_output() {
        let mut stream = CotStream::new(
            &params("run-2"),
            plan(CotMessagesMode::Detailed, CotRunMode::Process, true),
        );
        let output = "w".repeat(COT_TOOL_OUTPUT_MAX_CHARS + 50);
        let mut all = stream.observe(&AgentEvent::ToolUse {
            id: "tool-1".into(),
            name: "Bash".into(),
            input: json!({ "command": "pwd" }),
        });
        all.extend(stream.observe(&AgentEvent::ToolResult {
            id: "tool-1".into(),
            output: output.clone(),
            is_error: false,
        }));

        assert!(stream_event_types(&all).contains(&"TOOL_CALL_ARGS"));
        let args = contents(&all, CotEventType::ToolCallArgs);
        assert_eq!(args[0]["toolCallId"], "tool-1");
        assert_eq!(args[0]["delta"], "{\"command\":\"pwd\"}");

        let result = &contents(&all, CotEventType::ToolCallResult)[0]["content"];
        let expected = format!("{}{}", "w".repeat(COT_TOOL_OUTPUT_MAX_CHARS), "...");
        assert_eq!(result, &json!(expected));
    }

    #[test]
    fn reasoning_and_text_deltas_use_the_typescript_caps() {
        let mut stream = CotStream::new(
            &params("run-3"),
            plan(CotMessagesMode::Brief, CotRunMode::Process, true),
        );
        let long = "理".repeat(COT_TEXT_MAX_CHARS + 10);
        let mut all = stream.observe(&AgentEvent::Thinking {
            delta: long.clone(),
        });
        all.extend(stream.observe(&AgentEvent::Text {
            delta: long.clone(),
        }));

        let reasoning = &contents(&all, CotEventType::ReasoningMessageContent)[0]["delta"];
        let text = &contents(&all, CotEventType::TextMessageContent)[0]["delta"];
        let expected = format!("{}{}", "理".repeat(COT_TEXT_MAX_CHARS), "...");
        assert_eq!(reasoning, &json!(expected));
        assert_eq!(text, &json!(expected));
        // Short values pass through untouched.
        let mut short = CotStream::new(
            &params("run-4"),
            plan(CotMessagesMode::Brief, CotRunMode::Process, true),
        );
        let events = short.observe(&AgentEvent::Thinking { delta: "ok".into() });
        assert_eq!(
            contents(&events, CotEventType::ReasoningMessageContent)[0]["delta"],
            "ok"
        );
    }

    #[test]
    fn reasoning_closes_before_tools_and_text_reopens_a_message() {
        let mut stream = CotStream::new(
            &params("run-5"),
            plan(CotMessagesMode::Brief, CotRunMode::Process, true),
        );
        let mut all = stream.observe(&AgentEvent::Thinking {
            delta: "step one".into(),
        });
        all.extend(stream.observe(&AgentEvent::ToolUse {
            id: "tool-1".into(),
            name: "Read".into(),
            input: json!({ "file_path": "/repo/a.rs" }),
        }));
        all.extend(stream.observe(&AgentEvent::ToolResult {
            id: "tool-1".into(),
            output: "file".into(),
            is_error: false,
        }));
        all.extend(stream.observe(&AgentEvent::Thinking {
            delta: "step two".into(),
        }));

        let types = stream_event_types(&all);
        assert_eq!(
            types,
            [
                "REASONING_START",
                "REASONING_MESSAGE_START",
                "REASONING_MESSAGE_CONTENT",
                "REASONING_MESSAGE_END",
                "REASONING_END",
                "TOOL_CALL_START",
                "TOOL_CALL_END",
                "TOOL_CALL_RESULT",
                // Second reasoning segment reuses the message id, so REASONING
                // start/end pair again instead of staying open.
                "REASONING_START",
                "REASONING_MESSAGE_START",
                "REASONING_MESSAGE_CONTENT",
            ]
        );
        let reasoning_ids = contents(&all, CotEventType::ReasoningMessageStart);
        assert_eq!(reasoning_ids[0]["messageId"], "reasoning-run-5");
        assert_eq!(reasoning_ids[1]["messageId"], "reasoning-run-5");
    }

    #[test]
    fn unknown_tool_result_without_a_tracked_call_uses_the_ts_fallback() {
        let mut stream = CotStream::new(
            &params("run-6"),
            plan(CotMessagesMode::Brief, CotRunMode::Process, true),
        );
        let events = stream.observe(&AgentEvent::ToolResult {
            id: "tool-x".into(),
            output: "ignored".into(),
            is_error: false,
        });
        assert_eq!(
            contents(&events, CotEventType::ToolCallResult)[0]["content"],
            TOOL_FALLBACK_RESULT
        );
    }

    #[test]
    fn tool_icons_follow_the_ts_name_table() {
        assert_eq!(cot_tool_icon("WebSearch"), "search");
        assert_eq!(cot_tool_icon("Grep"), "search");
        assert_eq!(cot_tool_icon("Read"), "read");
        assert_eq!(cot_tool_icon("Write"), "write");
        assert_eq!(cot_tool_icon("Edit"), "write");
        assert_eq!(cot_tool_icon("Document"), "doc");
        assert_eq!(cot_tool_icon("Calendar"), "calendar");
        assert_eq!(cot_tool_icon("Task"), "task");
        assert_eq!(cot_tool_icon("Bash"), "bash");
        assert_eq!(cot_tool_icon("command_execution"), "bash");
        assert_eq!(cot_tool_icon("mystery"), "default");
    }

    #[test]
    fn reasoning_only_mode_publishes_no_tool_or_text_events() {
        let mut stream = CotStream::new(
            &params("run-7"),
            plan(CotMessagesMode::Brief, CotRunMode::ReasoningOnly, true),
        );
        let mut all = stream.observe(&AgentEvent::Thinking {
            delta: "step one".into(),
        });
        all.extend(stream.observe(&AgentEvent::Text {
            delta: "progress text".into(),
        }));
        all.extend(stream.observe(&AgentEvent::ToolUse {
            id: "tool-1".into(),
            name: "Bash".into(),
            input: json!({ "command": "pwd" }),
        }));
        all.extend(stream.observe(&AgentEvent::ToolResult {
            id: "tool-1".into(),
            output: "/repo".into(),
            is_error: false,
        }));
        // The terminal closes the open reasoning segment but emits no
        // RUN_FINISHED: the run status is not part of a reasoning bubble.
        let tail = stream.terminal(TerminationReason::Normal, None);
        assert_eq!(
            stream_event_types(&tail),
            ["REASONING_MESSAGE_END", "REASONING_END"]
        );

        let types = stream_event_types(&all);
        assert_eq!(
            types,
            [
                "REASONING_START",
                "REASONING_MESSAGE_START",
                "REASONING_MESSAGE_CONTENT"
            ]
        );
    }

    #[test]
    fn terminal_requires_reasoning_and_text_to_close() {
        let mut stream = CotStream::new(
            &params("run-8"),
            plan(CotMessagesMode::Brief, CotRunMode::Process, true),
        );
        let mut all = stream.observe(&AgentEvent::Thinking {
            delta: "think".into(),
        });
        all.extend(stream.observe(&AgentEvent::Text {
            delta: "answer".into(),
        }));
        // The text delta already closed the reasoning segment (TS
        // `closeReasoningIfNeeded`), so only the text/step/run close here.
        let reasoning_end = stream_event_types(&all)
            .iter()
            .filter(|event_type| **event_type == "REASONING_END")
            .count();
        assert_eq!(reasoning_end, 1);
        let tail = stream.terminal(TerminationReason::Normal, None);
        assert_eq!(
            stream_event_types(&tail),
            ["TEXT_MESSAGE_END", "STEP_FINISHED", "RUN_FINISHED"]
        );
        assert_eq!(
            CotStream::complete_reason(TerminationReason::Normal),
            CotCompleteReason::Done
        );
    }

    #[test]
    fn terminal_maps_failure_interrupt_and_timeout_to_error() {
        for (termination, status) in [
            (TerminationReason::Interrupted, "interrupted"),
            (TerminationReason::Timeout, "timeout"),
        ] {
            let mut stream = CotStream::new(
                &params("run-9"),
                plan(CotMessagesMode::Brief, CotRunMode::Process, true),
            );
            let tail = stream.terminal(termination, None);
            assert_eq!(
                stream_event_types(&tail),
                ["RUN_FINISHED"],
                "termination={termination:?}"
            );
            assert_eq!(
                contents(&tail, CotEventType::RunFinished)[0]["status"],
                status
            );
            assert_eq!(
                CotStream::complete_reason(termination),
                CotCompleteReason::Error
            );
        }

        let mut stream = CotStream::new(
            &params("run-10"),
            plan(CotMessagesMode::Brief, CotRunMode::Process, true),
        );
        let tail = stream.terminal(TerminationReason::Failed, Some("agent crashed"));
        assert_eq!(stream_event_types(&tail), ["RUN_ERROR"]);
        assert_eq!(
            contents(&tail, CotEventType::RunError)[0],
            json!({ "message": "agent crashed", "code": "failed" })
        );
        assert_eq!(
            CotStream::complete_reason(TerminationReason::Failed),
            CotCompleteReason::Error
        );
    }

    // ---- publisher --------------------------------------------------------

    #[derive(Default)]
    struct FakeTransport {
        create_calls: Mutex<Vec<(String, Option<String>)>>,
        events: Mutex<Vec<CotEvent>>,
        updates: Mutex<Vec<Vec<CotEvent>>>,
        completed: Mutex<Vec<CotCompleteReason>>,
        fail_create: Mutex<Option<String>>,
        fail_update: Mutex<Option<String>>,
    }

    impl FakeTransport {
        fn with_failure_create(error: &str) -> Self {
            let fake = Self::default();
            *fake.fail_create.lock().unwrap() = Some(error.to_string());
            fake
        }

        fn with_failure_update(error: &str) -> Self {
            let fake = Self::default();
            *fake.fail_update.lock().unwrap() = Some(error.to_string());
            fake
        }

        fn update_count(&self) -> usize {
            self.updates.lock().unwrap().len()
        }

        fn completed(&self) -> Vec<CotCompleteReason> {
            self.completed.lock().unwrap().clone()
        }

        fn flat_events(&self) -> Vec<CotEvent> {
            self.events.lock().unwrap().clone()
        }
    }

    #[async_trait]
    impl CotTransport for FakeTransport {
        async fn create(
            &self,
            chat_id: &str,
            origin_message_id: Option<&str>,
        ) -> Result<CotRef, RestError> {
            self.create_calls
                .lock()
                .unwrap()
                .push((chat_id.to_string(), origin_message_id.map(str::to_string)));
            if let Some(error) = self.fail_create.lock().unwrap().clone() {
                return Err(RestError::Message(error));
            }
            Ok(CotRef::new("cot_fake", "om_cot_fake"))
        }

        async fn update(&self, _reference: &CotRef, events: &[CotEvent]) -> Result<(), RestError> {
            if let Some(error) = self.fail_update.lock().unwrap().clone() {
                return Err(RestError::Message(error));
            }
            self.updates.lock().unwrap().push(events.to_vec());
            self.events.lock().unwrap().extend_from_slice(events);
            Ok(())
        }

        async fn complete(
            &self,
            _reference: &CotRef,
            reason: CotCompleteReason,
        ) -> Result<(), RestError> {
            self.completed.lock().unwrap().push(reason);
            Ok(())
        }
    }

    fn as_transport(fake: Arc<FakeTransport>) -> Arc<dyn CotTransport> {
        fake
    }

    /// Poll until `predicate` holds or 2 seconds elapse (the worker is async).
    async fn eventually(mut predicate: impl FnMut() -> bool) {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
        while tokio::time::Instant::now() < deadline {
            if predicate() {
                return;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        panic!("condition not reached within 2s");
    }

    #[tokio::test]
    async fn publisher_batches_events_into_one_update_per_window() {
        let fake = Arc::new(FakeTransport::default());
        let mut publisher = CotPublisher::start_with_throttle(
            as_transport(fake.clone()),
            params("run-batch"),
            plan(CotMessagesMode::Brief, CotRunMode::Process, true),
            Duration::from_millis(60),
        )
        .await;
        assert!(publisher.is_enabled());
        assert_eq!(
            fake.create_calls.lock().unwrap().as_slice(),
            &[("oc_chat".to_string(), Some("om_origin".to_string()))]
        );

        // Three observations inside one throttle window. The initial
        // RUN_STARTED/STEP_STARTED batch plus these must be coalesced into at
        // most one update per window.
        publisher.observe(&AgentEvent::Thinking { delta: "a".into() });
        publisher.observe(&AgentEvent::Text { delta: "b".into() });
        publisher.observe(&AgentEvent::ToolUse {
            id: "tool-1".into(),
            name: "Bash".into(),
            input: json!({ "command": "pwd" }),
        });
        eventually(|| fake.update_count() >= 1).await;

        let updates = fake.updates.lock().unwrap().clone();
        assert_eq!(updates.len(), 1, "one window must yield one update");
        let types: Vec<&str> = updates[0]
            .iter()
            .map(|event| event.event_type.as_str())
            .collect();
        for expected in [
            "RUN_STARTED",
            "STEP_STARTED",
            "REASONING_START",
            "REASONING_MESSAGE_CONTENT",
            "TEXT_MESSAGE_CONTENT",
            "TOOL_CALL_START",
            "TOOL_CALL_END",
        ] {
            assert!(types.contains(&expected), "missing {expected} in {types:?}");
        }

        // A later observation opens a new window and a new update.
        publisher.observe(&AgentEvent::ToolResult {
            id: "tool-1".into(),
            output: "ok".into(),
            is_error: false,
        });
        eventually(|| fake.update_count() >= 2).await;

        let _ = publisher.finish(TerminationReason::Normal, None).await;
        assert_eq!(fake.completed(), [CotCompleteReason::Done]);
    }

    #[tokio::test]
    async fn create_failure_disables_the_publisher_and_never_retries() {
        let fake = Arc::new(FakeTransport::with_failure_create(
            "COT API failed: code=10002 msg=Bot/User can NOT be out of the chat.",
        ));
        let mut publisher = CotPublisher::start_with_throttle(
            as_transport(fake.clone()),
            params("run-fail-create"),
            plan(CotMessagesMode::Brief, CotRunMode::Process, true),
            Duration::from_millis(20),
        )
        .await;

        assert!(!publisher.is_enabled());
        assert!(publisher.is_disabled());
        assert_eq!(publisher.reference(), None);
        assert!(publisher.degraded_reason().unwrap().contains("code=10002"));
        // Publishing after the failure is a silent no-op.
        publisher.observe(&AgentEvent::Text {
            delta: "working".into(),
        });
        let _ = publisher.finish(TerminationReason::Normal, None).await;

        assert_eq!(fake.create_calls.lock().unwrap().len(), 1);
        assert!(fake.updates.lock().unwrap().is_empty());
        assert!(fake.completed().is_empty());
    }

    #[tokio::test]
    async fn update_failure_disables_publishing_and_skips_completion() {
        let fake = Arc::new(FakeTransport::with_failure_update(
            "field validation failed",
        ));
        let mut publisher = CotPublisher::start_with_throttle(
            as_transport(fake.clone()),
            params("run-fail-update"),
            plan(CotMessagesMode::Brief, CotRunMode::Process, true),
            Duration::from_millis(20),
        )
        .await;
        assert!(publisher.is_enabled());

        publisher.observe(&AgentEvent::Text {
            delta: "working".into(),
        });
        eventually(|| publisher.is_disabled()).await;
        assert_eq!(
            publisher.degraded_reason().as_deref(),
            Some("field validation failed")
        );

        // The run keeps going: further observations are dropped, and finish
        // does not try to complete a bubble whose updates are failing.
        publisher.observe(&AgentEvent::Text {
            delta: "more".into(),
        });
        let _ = publisher.finish(TerminationReason::Normal, None).await;
        assert!(fake.completed().is_empty());
        assert_eq!(fake.update_count(), 0);
    }

    #[tokio::test]
    async fn finish_without_a_bubble_is_a_no_op() {
        // Reasoning-only runs start lazily: a run without thinking never
        // creates the publisher, so there is nothing to complete.
        let publisher = CotPublisher {
            stream: CotStream::new(
                &params("run-lazy"),
                plan(CotMessagesMode::Brief, CotRunMode::ReasoningOnly, true),
            ),
            tx: None,
            state: Arc::new(PublishState::default()),
            reference: None,
        };
        assert!(!publisher.is_started());
        let _ = publisher.finish(TerminationReason::Normal, None).await;
    }

    #[tokio::test]
    async fn interrupted_runs_complete_with_error_and_report_the_status() {
        let fake = Arc::new(FakeTransport::default());
        let mut publisher = CotPublisher::start_with_throttle(
            as_transport(fake.clone()),
            params("run-interrupt"),
            plan(CotMessagesMode::Brief, CotRunMode::Process, true),
            Duration::from_millis(20),
        )
        .await;
        publisher.observe(&AgentEvent::Text {
            delta: "partial answer".into(),
        });

        // The run loop kills the agent, marks the view interrupted and calls
        // finish with the interrupted termination.
        let _ = publisher.finish(TerminationReason::Interrupted, None).await;

        assert_eq!(fake.completed(), [CotCompleteReason::Error]);
        let events = fake.flat_events();
        let finished: Vec<&CotEvent> = events
            .iter()
            .filter(|event| event.event_type == "RUN_FINISHED")
            .collect();
        assert_eq!(finished.len(), 1);
        assert_eq!(finished[0].content_value()["status"], "interrupted");
    }

    #[tokio::test]
    async fn dropped_handle_flushes_but_leaves_the_bubble_open() {
        let fake = Arc::new(FakeTransport::default());
        let publisher = CotPublisher::start_with_throttle(
            as_transport(fake.clone()),
            params("run-drop"),
            plan(CotMessagesMode::Brief, CotRunMode::Process, true),
            Duration::from_millis(20),
        )
        .await;
        drop(publisher);
        eventually(|| fake.update_count() >= 1).await;
        assert!(fake.completed().is_empty());
    }

    // ---- run-flow wiring (CotRun) ----------------------------------------

    fn process_run(profile: &ProfileConfig) -> Option<CotPlan> {
        CotPlan::for_profile(profile)
    }

    #[tokio::test]
    async fn cot_off_run_wiring_publishes_nothing() {
        // The default profile has cotMessages unset (off) and auto reasoning
        // display: the run must never prepare a publisher, so the card path is
        // used and no COT call is made.
        let profile = profile_with(None, None);
        assert!(
            CotRun::prepare(
                process_run(&profile),
                as_transport(Arc::new(FakeTransport::default())),
                params("run-off"),
            )
            .is_none()
        );

        // Explicitly off is the same no-op.
        let profile = profile_with(Some(CotMessagesMode::Off), Some(ReasoningDisplay::Inline));
        assert!(process_run(&profile).is_none());
    }

    #[tokio::test]
    async fn process_run_starts_eagerly_and_completes_on_interrupt() {
        let fake = Arc::new(FakeTransport::default());
        let profile = profile_with(Some(CotMessagesMode::Brief), Some(ReasoningDisplay::Auto));
        let mut run = CotRun::prepare(
            process_run(&profile),
            as_transport(fake.clone()),
            params("run-flow"),
        )
        .expect("process COT run");
        assert!(run.is_active());
        // Eager create, exactly once, addressed to the chat + origin message.
        assert!(run.start().await);
        assert_eq!(
            fake.create_calls.lock().unwrap().as_slice(),
            &[("oc_chat".to_string(), Some("om_origin".to_string()))]
        );

        run.observe(&AgentEvent::Text {
            delta: "partial answer".into(),
        })
        .await;
        // A second `start` must not create a duplicate bubble.
        assert!(run.start().await);
        assert_eq!(fake.create_calls.lock().unwrap().len(), 1);

        // The run loop stops the agent, marks the view interrupted and calls
        // `finish` with the interrupted termination.
        let _ = run.finish(TerminationReason::Interrupted, None).await;
        assert_eq!(fake.completed(), [CotCompleteReason::Error]);
        let statuses: Vec<Value> = fake
            .flat_events()
            .iter()
            .filter(|event| event.event_type == "RUN_FINISHED")
            .map(|event| event.content_value()["status"].clone())
            .collect();
        assert_eq!(statuses, [json!("interrupted")]);
    }

    #[tokio::test]
    async fn rejected_create_falls_back_to_the_card_path() {
        let fake = Arc::new(FakeTransport::with_failure_create(
            "COT API failed: code=10002 msg=Bot/User can NOT be out of the chat.",
        ));
        let profile = profile_with(Some(CotMessagesMode::Detailed), Some(ReasoningDisplay::Off));
        let mut run = CotRun::prepare(
            process_run(&profile),
            as_transport(fake.clone()),
            params("run-rejected"),
        )
        .expect("process COT run");
        // `false` is the caller's signal to keep the progress card.
        assert!(!run.start().await);
        // Further starts do not retry a create that may already have rendered.
        assert!(!run.start().await);
        // No publisher was kept, so there is no degraded notice to send.
        assert_eq!(run.finish(TerminationReason::Normal, None).await, None);
        assert_eq!(fake.create_calls.lock().unwrap().len(), 1);
        assert!(fake.completed().is_empty());
    }

    #[tokio::test]
    async fn update_failure_is_reported_to_the_caller() {
        let fake = Arc::new(FakeTransport::with_failure_update(
            "field validation failed",
        ));
        let profile = profile_with(Some(CotMessagesMode::Brief), Some(ReasoningDisplay::Auto));
        let mut run = CotRun::prepare(
            process_run(&profile),
            as_transport(fake.clone()),
            params("run-degraded"),
        )
        .expect("process COT run");
        assert!(run.start().await);
        run.observe(&AgentEvent::Text {
            delta: "working".into(),
        })
        .await;

        // The buffered update is flushed by `finish`, fails, disables the
        // publisher and is reported so the run can send the TS notice.
        let degraded = run.finish(TerminationReason::Normal, None).await;
        assert_eq!(degraded.as_deref(), Some("field validation failed"));
        // A degraded bubble is never completed (TS leaves it to the server).
        assert!(fake.completed().is_empty());
    }

    #[tokio::test]
    async fn reasoning_only_run_starts_on_the_first_thinking_event() {
        let fake = Arc::new(FakeTransport::default());
        let profile = profile_with(None, Some(ReasoningDisplay::Bubble));
        let mut run = CotRun::prepare(
            process_run(&profile),
            as_transport(fake.clone()),
            params("run-reasoning"),
        )
        .expect("reasoning COT run");
        // Lazy: `start` keeps the path active without a create.
        assert!(run.start().await);
        assert_eq!(fake.create_calls.lock().unwrap().len(), 0);

        // Text/tool events do not trigger the create in reasoning-only mode.
        run.observe(&AgentEvent::Text {
            delta: "answer".into(),
        })
        .await;
        run.observe(&AgentEvent::ToolUse {
            id: "tool-1".into(),
            name: "Bash".into(),
            input: json!({ "command": "pwd" }),
        })
        .await;
        assert_eq!(fake.create_calls.lock().unwrap().len(), 0);

        run.observe(&AgentEvent::Thinking {
            delta: "step one".into(),
        })
        .await;
        assert_eq!(fake.create_calls.lock().unwrap().len(), 1);

        let _ = run.finish(TerminationReason::Normal, None).await;
        assert_eq!(fake.completed(), [CotCompleteReason::Done]);
        let events = fake.flat_events();
        let types: Vec<&str> = events
            .iter()
            .map(|event| event.event_type.as_str())
            .collect();
        assert!(types.contains(&"REASONING_MESSAGE_CONTENT"), "{types:?}");
        assert!(!types.contains(&"TEXT_MESSAGE_CONTENT"), "{types:?}");
        assert!(!types.contains(&"TOOL_CALL_START"), "{types:?}");
    }

    #[tokio::test]
    async fn reasoning_only_run_without_thinking_leaves_no_bubble() {
        let fake = Arc::new(FakeTransport::default());
        let profile = profile_with(None, Some(ReasoningDisplay::Bubble));
        let mut run = CotRun::prepare(
            process_run(&profile),
            as_transport(fake.clone()),
            params("run-no-reasoning"),
        )
        .expect("reasoning COT run");
        assert!(run.start().await);
        run.observe(&AgentEvent::Text {
            delta: "answer".into(),
        })
        .await;
        let _ = run.finish(TerminationReason::Normal, None).await;

        // No create, no update, no complete: the run leaves no empty bubble.
        assert_eq!(fake.create_calls.lock().unwrap().len(), 0);
        assert!(fake.updates.lock().unwrap().is_empty());
        assert!(fake.completed().is_empty());
    }
}
