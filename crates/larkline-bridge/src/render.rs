//! Run view and rich-card rendering.
//!
//! This is the Rust port of `src/card/run-state.ts` (the event fold),
//! `src/card/run-renderer.ts` (the CardKit 2.0 content model) and
//! `src/card/tool-render.ts` (the per-tool header/body markdown). Everything
//! here is pure: a fold over agent events plus a JSON builder, so both are
//! unit-testable without a network, an agent, or a clock.
//!
//! # Content model
//!
//! A folded [`RunView`] keeps an ordered [`Block`] list (streamed text and
//! tool calls interleaved, exactly like the TypeScript `RunState.blocks`), the
//! aggregated reasoning text and the per-tool [`ToolCall`] records. Rendering
//! walks the blocks and emits, in order:
//!
//! 1. the reasoning panel (collapsible, open while the model is thinking);
//! 2. text blocks as markdown and consecutive tool blocks as collapsible
//!    panels (one per call, or a header-only "☕ N 个工具调用" summary);
//! 3. the terminal notice (`⏹ 已被中断`, `⏱ N 分钟无响应`, `⚠️ agent 失败`, or
//!    `（未返回内容）` for an empty clean finish);
//! 4. while the run is live only: the footer status phrase
//!    (`🧠 正在思考` / `🧰 正在调用工具` / `✍️ 正在输出`), the bridge's
//!    metadata note (agent · status · elapsed/tokens) and, last, the signed
//!    stop button (`⏹ 终止`).
//!
//! Terminal frames never carry the status phrase or the stop button.
//!
//! # Size policy
//!
//! Feishu rejects cards around 30KB, which is why the TypeScript renderer
//! drops tool bodies when it collapses a group. [`render_card`] additionally
//! measures the serialized card and degrades it until it fits
//! [`CARD_MAX_BYTES`], in this order (lowest-value content first):
//!
//! 1. collapse every tool group to its header-only summary (drops bodies);
//! 2. shorten the reasoning panel body to [`SHORT_REASONING_MAX_CHARS`];
//! 3. drop the reasoning panel;
//! 4. drop the tool groups entirely;
//! 5. drop the bridge metadata note;
//! 6. drop the running status phrase;
//! 7. truncate the answer markdown, largest block first, with the
//!    [`CARD_TRUNCATION_SUFFIX`].
//!
//! Terminal notices, the summary label and the stop button are never
//! dropped: they are the smallest and most important elements. A terminal
//! notice is only shrunk when it alone exceeds the cap (e.g. a pathological
//! agent error message). Email masking runs before measuring, so a truncation
//! can never expose a raw address.

use std::time::Duration;

use larkline_agent::{AgentEvent, TerminationReason, UserQuestion};
use serde_json::{Value, json};

use crate::text_render::mask_emails;

/// Max characters kept from the aggregated reasoning panel (TS `REASONING_MAX`).
pub const REASONING_MAX_CHARS: usize = 3_000;
/// Reasoning body cap used by the second size-degradation step.
pub const SHORT_REASONING_MAX_CHARS: usize = 600;
/// Number of tool calls in one group that triggers the collapsed summary.
pub const COLLAPSE_TOOL_THRESHOLD: usize = 3;
/// Header summary cap (TS `HEADER_SUMMARY_MAX`).
pub const TOOL_HEADER_SUMMARY_MAX_CHARS: usize = 80;
/// Per-field input cap in a tool body (TS `BODY_FIELD_MAX`).
pub const TOOL_INPUT_FIELD_MAX_CHARS: usize = 600;
/// Tool output cap (TS `OUTPUT_MAX`).
pub const TOOL_OUTPUT_MAX_CHARS: usize = 1_200;
/// Cumulative cap on a tool body (TS `BODY_TOTAL_MAX`).
pub const TOOL_BODY_TOTAL_MAX_CHARS: usize = 2_500;
/// Stored (in-memory) cap for one tool input string.
///
/// The TypeScript state keeps the raw JSON; the Rust view bounds it because a
/// bridge run can last 30 minutes. 4k characters is far above every render
/// cap, so truncation is decided by the renderer, not by this store bound.
pub const TOOL_INPUT_STORE_MAX_CHARS: usize = 4_000;
/// Stored (in-memory) cap for a tool output.
pub const TOOL_OUTPUT_STORE_MAX_CHARS: usize = 4_000;
/// Sanity cap for one rendered answer block (not a display limit).
pub const MAX_CARD_TEXT_CHARS: usize = 100_000;
/// Feishu's per-card payload limit used by the degradation policy.
///
/// The documented failure mode is a 400 for cards around 30KB; measuring the
/// serialized JSON (UTF-8 bytes) keeps a margin for the envelope the API adds.
pub const CARD_MAX_BYTES: usize = 30_000;
/// Suffix appended to an answer block that the 30KB policy had to truncate.
pub const CARD_TRUNCATION_SUFFIX: &str = "\n\n…（内容过长，卡片已截断）";
/// Suffix of the tool-body cumulative cap (TS text, kept verbatim).
const TOOL_BODY_TRUNCATION_SUFFIX: &str = "…\n\n_（body 已截断,完整内容查 `/doctor` 或日志）_";
/// Generic truncation suffix (TS `truncate` uses a bare ellipsis).
const ELLIPSIS: char = '…';

/// Per-tool status (TS `ToolStatus`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ToolStatus {
    /// The tool call started and has no result yet.
    #[default]
    Running,
    /// The tool returned successfully.
    Done,
    /// The tool returned an error.
    Error,
}

/// Footer status phrase source (TS `FooterStatus`, minus the `null` arm which
/// Rust models as `Option`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum FooterStatus {
    /// The agent is producing reasoning.
    #[default]
    Thinking,
    /// A tool call is in flight.
    ToolRunning,
    /// The agent is streaming answer text.
    Streaming,
}

/// Terminal state of a run (TS `Terminal`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Terminal {
    /// The run is still going.
    Running,
    /// Clean finish.
    Done,
    /// Stopped by the user.
    Interrupted,
    /// Failed.
    Error,
    /// The idle-timeout watchdog gave up.
    IdleTimeout,
}

/// The few input fields the card renderer understands.
///
/// TypeScript keeps the raw JSON object and reads string fields on demand.
/// Keeping only the known display fields (bounded) preserves every rendering
/// contract without retaining arbitrary tool payloads for the run's lifetime.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ToolInput {
    /// `Bash.command`
    pub command: Option<String>,
    /// `Read/Edit/Write/NotebookEdit.file_path`
    pub file_path: Option<String>,
    /// `Grep.path` (and the generic fallback)
    pub path: Option<String>,
    /// `Grep/Glob.pattern`
    pub pattern: Option<String>,
    /// `WebFetch.url`
    pub url: Option<String>,
    /// `WebSearch.query`
    pub query: Option<String>,
    /// `Agent/Task.description`
    pub description: Option<String>,
    /// `Agent/Task.subagent_type`
    pub subagent_type: Option<String>,
}

impl ToolInput {
    /// Extract the display fields from a raw tool input object.
    pub fn from_value(input: &Value) -> Self {
        Self {
            command: string_field(input, "command"),
            file_path: string_field(input, "file_path"),
            path: string_field(input, "path"),
            pattern: string_field(input, "pattern"),
            url: string_field(input, "url"),
            query: string_field(input, "query"),
            description: string_field(input, "description"),
            subagent_type: string_field(input, "subagent_type"),
        }
    }

    /// Read a display field by its JSON key (TS `rec[key]` with a string guard).
    pub fn get(&self, key: &str) -> Option<&str> {
        match key {
            "command" => self.command.as_deref(),
            "file_path" => self.file_path.as_deref(),
            "path" => self.path.as_deref(),
            "pattern" => self.pattern.as_deref(),
            "url" => self.url.as_deref(),
            "query" => self.query.as_deref(),
            "description" => self.description.as_deref(),
            "subagent_type" => self.subagent_type.as_deref(),
            _ => None,
        }
    }
}

fn string_field(input: &Value, key: &str) -> Option<String> {
    input
        .get(key)
        .and_then(Value::as_str)
        .map(|value| truncate(value, TOOL_INPUT_STORE_MAX_CHARS))
}

/// One folded tool call (TS `ToolEntry`, plus the bounded input model).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ToolCall {
    /// Protocol id used to match the result event.
    pub id: String,
    /// Tool name (`Bash`, `Read`, ...).
    pub name: String,
    /// Bounded display input.
    pub input: ToolInput,
    /// Current status.
    pub status: ToolStatus,
    /// Bounded result output, when a result arrived.
    pub output: Option<String>,
}

impl Default for ToolCall {
    fn default() -> Self {
        Self {
            id: String::new(),
            name: String::new(),
            input: ToolInput::default(),
            status: ToolStatus::Running,
            output: None,
        }
    }
}

/// One ordered content block (TS `Block`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Block {
    /// Streamed answer/commentary text. `streaming` is true while the block is
    /// still growing, so the next text delta appends instead of starting a new
    /// block.
    Text {
        /// Accumulated text.
        content: String,
        /// Whether the next text delta should append here.
        streaming: bool,
    },
    /// Index into [`RunView::tool_calls`].
    Tool(usize),
}

/// Folded state of one agent run.
///
/// The legacy flat fields (`answer`, `tools`, `tool_errors`) stay in sync with
/// the ordered view so the text renderer and other consumers keep working.
#[derive(Debug, Clone)]
pub struct RunView {
    /// Streamed answer text (the final answer replaces it, like the old view).
    pub answer: String,
    /// The agent's final answer when the protocol distinguishes it.
    pub final_answer: Option<String>,
    /// Aggregated reasoning text.
    pub reasoning: String,
    /// Whether reasoning is still streaming (drives the panel title/open state).
    pub reasoning_active: bool,
    /// Legacy one-line tool summaries (`Bash: pwd`), used by the text renderer.
    pub tools: Vec<String>,
    /// Per-tool records in call order.
    pub tool_calls: Vec<ToolCall>,
    /// Ordered text/tool blocks.
    pub blocks: Vec<Block>,
    /// Number of failed tool results.
    pub tool_errors: usize,
    /// Running status phrase source; `None` once the run is terminal.
    pub footer_status: Option<FooterStatus>,
    /// UI-only note rendered at the top of a progress card that continues a
    /// previous card after the 8.5-minute rollover. Final cards clear it; the
    /// text renderer ignores it.
    pub continuation_note: Option<String>,
    /// Tool name while the run is blocked on an approval card. Drives the
    /// "⏳ 等待授权" line in the progress card; cleared when the decision lands.
    pub pending_approval: Option<String>,
    /// Non-terminal warning shown above the run content (Codex retry, failed
    /// resume, ...); final cards keep it so the user knows what happened.
    pub notice: Option<String>,
    /// Minutes reported by the idle-timeout notice (TS `state.idleTimeoutMinutes`).
    pub idle_timeout_minutes: Option<u64>,
    pub session_id: Option<String>,
    pub thread_id: Option<String>,
    pub error: Option<String>,
    pub termination: Option<TerminationReason>,
    pub input_tokens: u64,
    pub output_tokens: u64,
    pub finished: bool,
}

impl Default for RunView {
    fn default() -> Self {
        Self {
            answer: String::new(),
            final_answer: None,
            reasoning: String::new(),
            reasoning_active: false,
            tools: Vec::new(),
            tool_calls: Vec::new(),
            blocks: Vec::new(),
            tool_errors: 0,
            footer_status: Some(FooterStatus::Thinking),
            continuation_note: None,
            pending_approval: None,
            notice: None,
            idle_timeout_minutes: None,
            session_id: None,
            thread_id: None,
            error: None,
            termination: None,
            input_tokens: 0,
            output_tokens: 0,
            finished: false,
        }
    }
}

impl RunView {
    /// A fresh running view (alias of [`RunView::default`]).
    pub fn new() -> Self {
        Self::default()
    }

    /// Fold one agent event into the state.
    pub fn apply(&mut self, event: &AgentEvent) {
        match event {
            AgentEvent::System {
                session_id,
                thread_id,
                ..
            } => {
                if let Some(id) = session_id {
                    self.session_id = Some(id.clone());
                }
                if let Some(id) = thread_id {
                    self.thread_id = Some(id.clone());
                }
            }
            AgentEvent::Text { delta } => {
                self.answer.push_str(delta);
                match self.blocks.last_mut() {
                    Some(Block::Text {
                        content,
                        streaming: true,
                    }) => content.push_str(delta),
                    _ => self.blocks.push(Block::Text {
                        content: delta.clone(),
                        streaming: true,
                    }),
                }
                self.reasoning_active = false;
                self.footer_status = Some(FooterStatus::Streaming);
            }
            AgentEvent::Thinking { delta } => {
                self.reasoning.push_str(delta);
                self.reasoning_active = true;
                self.footer_status = Some(FooterStatus::Thinking);
            }
            AgentEvent::ToolUse { id, name, input } => {
                let index = self.tool_calls.len();
                self.tool_calls.push(ToolCall {
                    id: id.clone(),
                    name: name.clone(),
                    input: ToolInput::from_value(input),
                    status: ToolStatus::Running,
                    output: None,
                });
                self.close_streaming_text();
                self.blocks.push(Block::Tool(index));
                self.tools.push(tool_line(name, input));
                self.reasoning_active = false;
                self.footer_status = Some(FooterStatus::ToolRunning);
            }
            AgentEvent::ToolResult {
                id,
                output,
                is_error,
            } => {
                let status = if *is_error {
                    ToolStatus::Error
                } else {
                    ToolStatus::Done
                };
                if let Some(tool) = self.tool_calls.iter_mut().rev().find(|tool| tool.id == *id) {
                    tool.status = status;
                    tool.output = Some(truncate(output, TOOL_OUTPUT_STORE_MAX_CHARS));
                }
                if *is_error {
                    self.tool_errors += 1;
                }
            }
            AgentEvent::FinalText { content } => {
                self.final_answer = Some(content.clone());
                self.answer = content.clone();
                self.close_streaming_text();
                // The TS renderer keeps `finalText` out of `blocks` and only
                // renders streamed text. Folding it into the trailing text
                // block keeps a final-only agent visible instead of rendering
                // an empty "（未返回内容）" card. A final text that already
                // appears in an earlier block (the common protocol shape) is
                // not appended again.
                let already_displayed = self.blocks.iter().any(|block| {
                    matches!(block, Block::Text { content: existing, .. } if existing == content)
                });
                match self.blocks.last_mut() {
                    Some(Block::Text { content: block, .. }) => block.clone_from(content),
                    _ if already_displayed => {}
                    _ => self.blocks.push(Block::Text {
                        content: content.clone(),
                        streaming: false,
                    }),
                }
                self.reasoning_active = false;
            }
            AgentEvent::Usage {
                input_tokens,
                output_tokens,
                ..
            } => {
                self.input_tokens = input_tokens.unwrap_or(0);
                self.output_tokens = output_tokens.unwrap_or(0);
            }
            AgentEvent::Done {
                session_id,
                thread_id,
                termination_reason,
            } => {
                if let Some(id) = session_id {
                    self.session_id = Some(id.clone());
                }
                if let Some(id) = thread_id {
                    self.thread_id = Some(id.clone());
                }
                self.close_streaming_text();
                self.reasoning_active = false;
                self.footer_status = None;
                self.termination = Some(*termination_reason);
                self.finished = true;
            }
            AgentEvent::Error {
                message,
                termination_reason,
            } => {
                self.error = Some(message.clone());
                self.termination = Some(*termination_reason);
                self.footer_status = None;
                self.finished = true;
            }
            // Approval prompts are rendered as a separate card and tracked by
            // the run loop; they do not change the agent output view.
            AgentEvent::Notice { message } => {
                self.notice = Some(message.clone());
            }
            AgentEvent::PermissionRequest { .. }
            | AgentEvent::QuestionRequest { .. }
            | AgentEvent::PermissionCancelled { .. } => {}
        }
    }

    /// Mark a still-running view as interrupted.
    ///
    /// This is the cooperative-stop equivalent of the agent reporting
    /// `terminationReason: interrupted`: the partial answer is preserved and
    /// no error is recorded (an error would replace the answer in
    /// [`RunView::best_answer`] and render a "失败" card). A view that already
    /// reached a terminal state keeps that real terminal, mirroring the TS
    /// `markInterrupted` guard used by the bridge.
    pub fn mark_interrupted(&mut self) {
        if self.finished {
            return;
        }
        self.close_streaming_text();
        self.reasoning_active = false;
        self.footer_status = None;
        self.pending_approval = None;
        self.termination = Some(TerminationReason::Interrupted);
        self.finished = true;
    }

    /// Mark a still-running view as idle-timed-out after `minutes`.
    ///
    /// The partial answer is preserved (TS `markIdleTimeout`); the minutes are
    /// what the terminal notice reports.
    pub fn mark_idle_timeout(&mut self, minutes: u64) {
        if self.finished {
            return;
        }
        self.close_streaming_text();
        self.reasoning_active = false;
        self.footer_status = None;
        self.pending_approval = None;
        self.termination = Some(TerminationReason::Timeout);
        self.idle_timeout_minutes = Some(minutes);
        self.finished = true;
    }

    /// Close every streaming text block (a tool call or terminal ends it).
    fn close_streaming_text(&mut self) {
        for block in &mut self.blocks {
            if let Block::Text { streaming, .. } = block {
                *streaming = false;
            }
        }
    }

    /// The terminal state the card renders.
    pub fn terminal(&self) -> Terminal {
        match self.termination {
            Some(TerminationReason::Interrupted) => Terminal::Interrupted,
            Some(TerminationReason::Timeout) => Terminal::IdleTimeout,
            _ if self.error.is_some() => Terminal::Error,
            _ if self.finished => Terminal::Done,
            _ => Terminal::Running,
        }
    }

    /// Whether the run ended because the user stopped it.
    pub fn is_interrupted(&self) -> bool {
        self.termination == Some(TerminationReason::Interrupted)
    }

    /// The text to show as the answer.
    pub fn best_answer(&self) -> &str {
        if let Some(error) = &self.error {
            return error;
        }
        if let Some(final_answer) = &self.final_answer {
            return final_answer;
        }
        &self.answer
    }

    pub fn session_key(&self) -> Option<&str> {
        self.session_id.as_deref().or(self.thread_id.as_deref())
    }

    /// One-line status for the card footer.
    pub fn footer(&self, elapsed: Duration) -> String {
        let mut parts = Vec::new();
        parts.push(format!("{:.1}s", elapsed.as_secs_f64()));
        if self.input_tokens > 0 || self.output_tokens > 0 {
            parts.push(format!(
                "{} in / {} out",
                self.input_tokens, self.output_tokens
            ));
        }
        if !self.tools.is_empty() {
            parts.push(format!("{} tool call(s)", self.tools.len()));
        }
        if self.tool_errors > 0 {
            parts.push(format!("{} failed", self.tool_errors));
        }
        parts.join(" · ")
    }
}

fn tool_line(name: &str, input: &Value) -> String {
    let detail = input
        .get("command")
        .and_then(|v| v.as_str())
        .or_else(|| input.get("path").and_then(|v| v.as_str()))
        .or_else(|| input.get("file_path").and_then(|v| v.as_str()))
        .unwrap_or_default();
    if detail.is_empty() {
        name.to_string()
    } else {
        format!("{name}: {}", truncate(detail, 80))
    }
}

/// Options for [`render_card`].
#[derive(Debug, Clone)]
pub struct CardRenderOptions {
    /// Render the aggregated reasoning panel (profile `reasoningDisplay` is
    /// `auto` or `inline`). Defaults to `true`, like the TS `reasoning !== false`.
    pub reasoning: bool,
    /// Render tool panels (profile `showToolCalls`).
    pub tool_panels: bool,
    /// Render the live footer status phrase. Paused frames turn this off.
    pub running_footer: bool,
    /// Bridge-only metadata note (`Claude Code · 运行中 · 1.2s · 1 tool call(s)`).
    pub metadata_note: Option<String>,
    /// Signed stop-button callback value, rendered last on live frames.
    pub stop_value: Option<Value>,
    /// `element_id` for CardKit to stream into (the first answer block only).
    pub answer_element_id: Option<String>,
    /// Serialized-card byte cap. `0` means [`CARD_MAX_BYTES`].
    pub max_bytes: usize,
}

impl Default for CardRenderOptions {
    fn default() -> Self {
        Self {
            reasoning: true,
            tool_panels: true,
            running_footer: true,
            metadata_note: None,
            stop_value: None,
            answer_element_id: None,
            max_bytes: CARD_MAX_BYTES,
        }
    }
}

/// Preview summary label (TS `summaryText`).
pub fn card_summary(view: &RunView) -> &'static str {
    match view.terminal() {
        Terminal::Interrupted => "已中断",
        Terminal::IdleTimeout => "已超时",
        Terminal::Error => "出错",
        Terminal::Done => "已完成",
        Terminal::Running if view.pending_approval.is_some() => "等待授权",
        Terminal::Running => match view.footer_status {
            Some(FooterStatus::ToolRunning) => "正在调用工具",
            Some(FooterStatus::Streaming) => "正在输出",
            Some(FooterStatus::Thinking) | None => "思考中",
        },
    }
}

/// Footer metadata label (bridge status column, not the chat preview).
pub fn card_status(view: &RunView) -> &'static str {
    match view.terminal() {
        Terminal::Interrupted => "已中断",
        Terminal::IdleTimeout => "已超时",
        Terminal::Error => "出错",
        Terminal::Done => "已完成",
        Terminal::Running => "运行中",
    }
}

/// The footer status phrase for a live frame (TS `footerStatus`).
pub fn footer_status_text(status: FooterStatus) -> &'static str {
    match status {
        FooterStatus::Thinking => "🧠 正在思考",
        FooterStatus::ToolRunning => "🧰 正在调用工具",
        FooterStatus::Streaming => "✍️ 正在输出",
    }
}

/// Render the rich card (schema 2.0) for `view`.
///
/// The returned card keeps `streaming_mode: false`; the live CardKit path
/// turns streaming on through `larkline_lark::enable_streaming` so the same
/// value stays valid for message-card updates. See the module docs for the
/// 30KB degradation policy.
pub fn render_card(view: &RunView, options: &CardRenderOptions) -> Value {
    let cap = if options.max_bytes == 0 {
        CARD_MAX_BYTES
    } else {
        options.max_bytes
    };
    let mut last = Value::Null;
    for detail in CardDetail::ALL {
        let mut card = card_value(view, options, detail);
        // Mask before measuring: masking can grow the payload (`@` -> `[at]`),
        // and a post-mask truncation must never cut a raw address back open.
        mask_card_emails(&mut card);
        if json_len(&card) <= cap {
            return card;
        }
        last = card;
    }
    trim_answer_text(last, cap)
}

fn card_value(view: &RunView, options: &CardRenderOptions, detail: CardDetail) -> Value {
    json!({
        "schema": "2.0",
        "config": {
            "update_multi": true,
            "streaming_mode": false,
            "summary": { "content": card_summary(view) }
        },
        "body": { "elements": build_elements(view, options, detail) }
    })
}

/// How much of the card survives the size budget.
///
/// Variants are ordered from richest to leanest; [`render_card`] picks the
/// first one that fits.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CardDetail {
    /// Every panel, full reasoning.
    Full,
    /// Tool bodies dropped (header-only summaries).
    CollapsedTools,
    /// Collapsed tools plus a short reasoning body.
    ShortReasoning,
    /// Collapsed tools, no reasoning panel.
    NoReasoning,
    /// No tool groups either.
    NoTools,
    /// Also drop the bridge metadata note.
    NoMeta,
    /// Also drop the running status phrase (text is still truncated if needed).
    Minimal,
}

impl CardDetail {
    const ALL: [Self; 7] = [
        Self::Full,
        Self::CollapsedTools,
        Self::ShortReasoning,
        Self::NoReasoning,
        Self::NoTools,
        Self::NoMeta,
        Self::Minimal,
    ];

    fn reasoning_limit(self) -> Option<usize> {
        match self {
            Self::Full | Self::CollapsedTools => Some(REASONING_MAX_CHARS),
            Self::ShortReasoning => Some(SHORT_REASONING_MAX_CHARS),
            Self::NoReasoning | Self::NoTools | Self::NoMeta | Self::Minimal => None,
        }
    }

    fn tools_mode(self) -> ToolsMode {
        match self {
            Self::Full => ToolsMode::Default,
            Self::CollapsedTools | Self::ShortReasoning | Self::NoReasoning => ToolsMode::Collapsed,
            Self::NoTools | Self::NoMeta | Self::Minimal => ToolsMode::Hidden,
        }
    }

    fn shows_metadata(self) -> bool {
        !matches!(self, Self::NoMeta | Self::Minimal)
    }

    fn shows_running_footer(self) -> bool {
        !matches!(self, Self::Minimal)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ToolsMode {
    /// Per-tool panels with the TS collapse rule.
    Default,
    /// One header-only summary per tool group.
    Collapsed,
    /// No tool content at all.
    Hidden,
}

/// A text or consecutive-tool segment of the block list.
enum Group<'a> {
    Text(&'a str),
    Tools(Vec<&'a ToolCall>),
}

fn build_elements(view: &RunView, options: &CardRenderOptions, detail: CardDetail) -> Vec<Value> {
    let terminal = view.terminal();
    let finalized = terminal != Terminal::Running;
    let mut elements = Vec::new();

    // A rolled-over progress card starts with a pointer to the card it
    // continues; it is never part of the terminal frame.
    if terminal == Terminal::Running {
        if let Some(note) = view
            .continuation_note
            .as_deref()
            .filter(|note| !note.trim().is_empty())
        {
            elements.push(notation(note));
        }
    }

    if terminal == Terminal::Running {
        if let Some(tool) = view
            .pending_approval
            .as_deref()
            .filter(|tool| !tool.trim().is_empty())
        {
            elements.push(notation(format!("⏳ 等待授权：{tool}")));
        }
    }
    if let Some(notice) = view
        .notice
        .as_deref()
        .filter(|notice| !notice.trim().is_empty())
    {
        elements.push(notation(format!("⚠️ {}", mask_emails(notice))));
    }

    if options.reasoning {
        if let Some(limit) = detail.reasoning_limit() {
            if !view.reasoning.is_empty() {
                elements.push(reasoning_panel(
                    &view.reasoning,
                    view.reasoning_active,
                    limit,
                ));
            }
        }
    }

    let mut answer_element_id = options.answer_element_id.as_deref();
    for group in group_blocks(view) {
        match group {
            Group::Text(content) => {
                if content.trim().is_empty() {
                    continue;
                }
                let mut element = markdown(truncate(content, MAX_CARD_TEXT_CHARS));
                if let Some(id) = answer_element_id.take() {
                    element["element_id"] = Value::String(id.to_string());
                }
                elements.push(element);
            }
            Group::Tools(tools) => {
                // `showToolCalls: false` only hides tool blocks; text stays.
                if !options.tool_panels {
                    continue;
                }
                match detail.tools_mode() {
                    ToolsMode::Hidden => {}
                    ToolsMode::Collapsed => {
                        elements.push(collapsed_tool_summary(&tools, finalized));
                    }
                    ToolsMode::Default => {
                        elements.extend(render_tool_group(&tools, finalized));
                    }
                }
            }
        }
    }

    // Legacy/manually-built views may only have the flat answer field.
    if view.blocks.is_empty() && !view.answer.trim().is_empty() {
        let mut element = markdown(truncate(&view.answer, MAX_CARD_TEXT_CHARS));
        if let Some(id) = answer_element_id.take() {
            element["element_id"] = Value::String(id.to_string());
        }
        elements.push(element);
    }

    let content_is_empty = elements.is_empty();
    if let Some(notice) = terminal_notice(view) {
        elements.push(notice);
    } else if terminal == Terminal::Done && content_is_empty {
        elements.push(notation("_（未返回内容）_"));
    }

    if terminal == Terminal::Running && options.running_footer && detail.shows_running_footer() {
        if let Some(status) = view.footer_status {
            elements.push(notation(footer_status_text(status)));
        }
    }

    if detail.shows_metadata() {
        if let Some(note) = options
            .metadata_note
            .as_deref()
            .filter(|note| !note.trim().is_empty())
        {
            elements.push(json!({
                "tag": "note",
                "elements": [{ "tag": "plain_text", "content": note }]
            }));
        }
    }

    // The stop button is last, after the footer, on live frames only.
    if terminal == Terminal::Running {
        if let Some(value) = &options.stop_value {
            elements.push(stop_button(value.clone()));
        }
    }

    elements
}

fn group_blocks(view: &RunView) -> Vec<Group<'_>> {
    let mut groups = Vec::new();
    let mut tools: Vec<&ToolCall> = Vec::new();
    for block in &view.blocks {
        match block {
            Block::Tool(index) => {
                if let Some(tool) = view.tool_calls.get(*index) {
                    tools.push(tool);
                }
            }
            Block::Text { content, .. } => {
                if !tools.is_empty() {
                    groups.push(Group::Tools(std::mem::take(&mut tools)));
                }
                groups.push(Group::Text(content));
            }
        }
    }
    if !tools.is_empty() {
        groups.push(Group::Tools(tools));
    }
    groups
}

fn render_tool_group(tools: &[&ToolCall], finalized: bool) -> Vec<Value> {
    if tools.is_empty() {
        return Vec::new();
    }
    if tools.len() < COLLAPSE_TOOL_THRESHOLD {
        return tools.iter().map(|tool| tool_panel(tool, false)).collect();
    }
    if finalized {
        return vec![collapsed_tool_summary(tools, true)];
    }
    // Running: collapse prior tools, keep the latest visible.
    let (latest, prior) = tools.split_last().expect("non-empty tool group");
    let mut out = Vec::new();
    if !prior.is_empty() {
        out.push(collapsed_tool_summary(prior, false));
    }
    out.push(tool_panel(latest, true));
    out
}

fn reasoning_panel(content: &str, active: bool, limit: usize) -> Value {
    let title = if active {
        "🧠 **思考中**"
    } else {
        "🧠 **思考完成，点击查看**"
    };
    collapsible_panel(title, active, "grey", &truncate(content, limit))
}

fn tool_panel(tool: &ToolCall, expanded: bool) -> Value {
    let border = if tool.status == ToolStatus::Error {
        "red"
    } else {
        "grey"
    };
    let body = tool_body_md(tool);
    let body = if body.is_empty() {
        "_无输出_".to_string()
    } else {
        body
    };
    collapsible_panel(&tool_header_text(tool), expanded, border, &body)
}

/// Render N tool calls as one collapsed, header-only panel.
///
/// Bodies are intentionally dropped: nested input/output panels can push the
/// serialized card past Feishu's ~30KB limit and abort the whole stream. The
/// latest running tool is rendered separately by [`render_tool_group`] so live
/// observation is not sacrificed.
fn collapsed_tool_summary(tools: &[&ToolCall], finalized: bool) -> Value {
    let suffix = if finalized { "（已结束）" } else { "" };
    let title = format!("☕ **{} 个工具调用{}**", tools.len(), suffix);
    let headers = tools
        .iter()
        .map(|tool| format!("- {}", tool_header_text(tool)))
        .collect::<Vec<_>>()
        .join("\n");
    collapsible_panel(&title, false, "blue", &headers)
}

fn collapsible_panel(title: &str, expanded: bool, border: &str, body: &str) -> Value {
    json!({
        "tag": "collapsible_panel",
        "expanded": expanded,
        "header": panel_header(title),
        "border": { "color": border, "corner_radius": "5px" },
        "vertical_spacing": "8px",
        "padding": "8px 8px 8px 8px",
        "elements": [{ "tag": "markdown", "content": body, "text_size": "notation" }]
    })
}

fn panel_header(title: &str) -> Value {
    json!({
        "title": { "tag": "markdown", "content": title },
        "vertical_align": "center",
        "icon": {
            "tag": "standard_icon",
            "token": "down-small-ccm_outlined",
            "size": "16px 16px"
        },
        "icon_position": "follow_text",
        "icon_expanded_angle": -180
    })
}

fn markdown(content: impl Into<String>) -> Value {
    json!({ "tag": "markdown", "content": content.into() })
}

fn notation(content: impl Into<String>) -> Value {
    json!({
        "tag": "markdown",
        "content": content.into(),
        "text_size": "notation"
    })
}

fn stop_button(value: Value) -> Value {
    json!({
        "tag": "button",
        "text": { "tag": "plain_text", "content": "⏹ 终止" },
        "type": "danger",
        "behaviors": [{ "type": "callback", "value": value }]
    })
}

fn terminal_notice(view: &RunView) -> Option<Value> {
    match view.terminal() {
        Terminal::Interrupted => Some(notation("_⏹ 已被中断_")),
        Terminal::IdleTimeout => {
            let minutes = view.idle_timeout_minutes.unwrap_or(0);
            Some(notation(format!("_⏱ {minutes} 分钟无响应,已自动终止_")))
        }
        Terminal::Error => view
            .error
            .as_deref()
            .filter(|message| !message.trim().is_empty())
            .map(|message| notation(format!("⚠️ agent 失败：{message}"))),
        Terminal::Running | Terminal::Done => None,
    }
}

/// Per-tool header line (TS `toolHeaderText`).
///
/// Public because the COT publisher reuses it for `brief` tool titles
/// (TS `cotBriefToolTitle`) before stripping the `**` markers.
pub fn tool_header_text(tool: &ToolCall) -> String {
    let icon = match tool.status {
        ToolStatus::Done => "✅",
        ToolStatus::Error => "❌",
        ToolStatus::Running => "⏳",
    };
    let summary = summarize_tool_input(tool);
    if summary.is_empty() {
        format!("{icon} **{}**", tool.name)
    } else {
        format!("{icon} **{}** — {summary}", tool.name)
    }
}

fn summarize_tool_input(tool: &ToolCall) -> String {
    let pick = |key: &str, max: usize| -> String {
        let Some(value) = tool.input.get(key) else {
            return String::new();
        };
        let one_line = value.split_whitespace().collect::<Vec<_>>().join(" ");
        truncate(&one_line, max)
    };
    match tool.name.as_str() {
        "Bash" => pick("command", TOOL_HEADER_SUMMARY_MAX_CHARS),
        "Read" | "Edit" | "Write" | "NotebookEdit" => {
            pick("file_path", TOOL_HEADER_SUMMARY_MAX_CHARS)
        }
        "Grep" => {
            let pattern = pick("pattern", 40);
            let path = pick("path", 30);
            if path.is_empty() {
                pattern
            } else {
                format!("{pattern} in {path}")
            }
        }
        "Glob" => pick("pattern", TOOL_HEADER_SUMMARY_MAX_CHARS),
        "WebFetch" => pick("url", TOOL_HEADER_SUMMARY_MAX_CHARS),
        "WebSearch" => pick("query", 60),
        "Agent" | "Task" => {
            let description = pick("description", TOOL_HEADER_SUMMARY_MAX_CHARS);
            if description.is_empty() {
                pick("subagent_type", TOOL_HEADER_SUMMARY_MAX_CHARS)
            } else {
                description
            }
        }
        _ => ["command", "file_path", "path", "query"]
            .iter()
            .map(|key| pick(key, TOOL_HEADER_SUMMARY_MAX_CHARS))
            .find(|value| !value.is_empty())
            .unwrap_or_default(),
    }
}

/// Tool panel body (TS `toolBodyMd`).
fn tool_body_md(tool: &ToolCall) -> String {
    let mut parts = Vec::new();
    let input = tool_input_md(tool);
    if !input.is_empty() {
        parts.push(input);
    }

    let output = tool.output.as_deref().filter(|output| !output.is_empty());
    if let Some(output) = output {
        let truncated = truncate(output, TOOL_OUTPUT_MAX_CHARS);
        if tool.status == ToolStatus::Error {
            parts.push(format!("**Error**\n```\n{truncated}\n```"));
        } else if tool.name == "Bash" {
            // Some agents wrap stdout/stderr in xml-like tags; keep it simple.
            parts.push(format!("**Output**\n```\n{truncated}\n```"));
        } else {
            parts.push(format!("**Output**\n```\n{truncated}\n```"));
        }
    } else if tool.status == ToolStatus::Running {
        parts.push("_运行中…_".to_string());
    }

    let body = parts.join("\n\n");
    if body.chars().count() <= TOOL_BODY_TOTAL_MAX_CHARS {
        return body;
    }
    format!(
        "{}{}",
        take_chars(&body, TOOL_BODY_TOTAL_MAX_CHARS),
        TOOL_BODY_TRUNCATION_SUFFIX
    )
}

fn tool_input_md(tool: &ToolCall) -> String {
    let field = |key: &str| tool.input.get(key).unwrap_or_default();
    match tool.name.as_str() {
        "Bash" => {
            let command = field("command");
            if command.is_empty() {
                String::new()
            } else {
                format!(
                    "**Command**\n```bash\n{}\n```",
                    truncate(command, TOOL_INPUT_FIELD_MAX_CHARS)
                )
            }
        }
        "Read" | "Edit" | "Write" | "NotebookEdit" => {
            let file_path = field("file_path");
            if file_path.is_empty() {
                String::new()
            } else {
                format!("**File** `{file_path}`")
            }
        }
        "Grep" => {
            let mut lines = Vec::new();
            if !field("pattern").is_empty() {
                lines.push(format!("**Pattern** `{}`", field("pattern")));
            }
            if !field("path").is_empty() {
                lines.push(format!("**Path** `{}`", field("path")));
            }
            lines.join("\n")
        }
        "WebFetch" => {
            let url = field("url");
            if url.is_empty() {
                String::new()
            } else {
                format!("**URL** {url}")
            }
        }
        "WebSearch" => {
            let query = field("query");
            if query.is_empty() {
                String::new()
            } else {
                format!(
                    "**Query** `{}`",
                    truncate(query, TOOL_INPUT_FIELD_MAX_CHARS)
                )
            }
        }
        _ => String::new(),
    }
}

/// Mask raw emails across every string in the card (TS `deepMaskEmails`).
fn mask_card_emails(value: &mut Value) {
    match value {
        Value::String(text) => {
            let masked = mask_emails(text);
            if masked != *text {
                *text = masked;
            }
        }
        Value::Array(items) => items.iter_mut().for_each(mask_card_emails),
        Value::Object(map) => map.values_mut().for_each(mask_card_emails),
        Value::Null | Value::Bool(_) | Value::Number(_) => {}
    }
}

fn json_len(value: &Value) -> usize {
    serde_json::to_vec(value)
        .map(|bytes| bytes.len())
        .unwrap_or(usize::MAX)
}

/// Shrink the answer markdown until the serialized card fits `cap`.
///
/// Only plain markdown elements (answer blocks) are candidates: panel bodies
/// and the notices/status lines carry `text_size: notation`, and the metadata
/// note uses the `note` tag, so they are never truncated here.
fn trim_answer_text(value: Value, cap: usize) -> Value {
    let mut value = value;
    for _ in 0..64 {
        let len = json_len(&value);
        if len <= cap {
            return value;
        }
        let Some(elements) = value["body"]["elements"].as_array_mut() else {
            return value;
        };
        let mut best: Option<usize> = None;
        let mut best_chars = 0;
        let mut fallback: Option<(usize, usize)> = None;
        for (index, element) in elements.iter().enumerate() {
            if element["tag"] != "markdown" {
                continue;
            }
            let chars = element["content"]
                .as_str()
                .map(|content| content.chars().count())
                .unwrap_or(0);
            if chars == 0 {
                continue;
            }
            if element.get("text_size").is_none() {
                if chars > best_chars {
                    best_chars = chars;
                    best = Some(index);
                }
            } else if fallback.is_none_or(|(_, fallback_chars)| chars > fallback_chars) {
                fallback = Some((index, chars));
            }
        }
        // Prefer answer blocks; a terminal notice is only shrunk when it is
        // the sole reason the card cannot fit.
        let (index, best_chars) = match best {
            Some(index) => (index, best_chars),
            None => match fallback {
                Some((index, chars)) => (index, chars),
                None => return value,
            },
        };
        if best_chars == 0 {
            return value;
        }
        let overflow = len - cap;
        let already_suffixed = elements[index]["content"]
            .as_str()
            .is_some_and(|content| content.ends_with(CARD_TRUNCATION_SUFFIX));
        let reserve = if already_suffixed {
            0
        } else {
            CARD_TRUNCATION_SUFFIX.len()
        };
        // Answer text is usually CJK (3 bytes per char); this over-shoots for
        // ASCII (1 byte) but only makes the shrink loop take a few more passes.
        let drop_chars = ((overflow + reserve) / 3).max(1) + 1;
        let content = elements[index]["content"].as_str().unwrap_or_default();
        let body = content
            .strip_suffix(CARD_TRUNCATION_SUFFIX)
            .unwrap_or(content);
        let keep = body.chars().count().saturating_sub(drop_chars);
        let mut next: String = body.chars().take(keep).collect();
        next.push_str(CARD_TRUNCATION_SUFFIX);
        elements[index]["content"] = Value::String(next);
    }
    value
}

fn truncate(value: &str, max: usize) -> String {
    if value.chars().count() <= max {
        value.to_string()
    } else {
        let mut out = take_chars(value, max);
        out.push(ELLIPSIS);
        out
    }
}

fn take_chars(value: &str, max: usize) -> String {
    value.chars().take(max).collect()
}

/// Everything needed to render one permission-approval card.
pub struct ApprovalCardInput<'a> {
    pub scope: &'a str,
    pub operator: &'a str,
    /// Unique id of the run that raised this request. Binding it into the
    /// signed action prevents an old card from being replayed against a later
    /// run that happens to reuse the same request id.
    pub run_id: u64,
    pub request_id: &'a str,
    pub tool_name: &'a str,
    /// Short run context (`Claude Code · /work/repo`).
    pub context: Option<&'a str>,
    pub description: Option<&'a str>,
    /// One-line command/path preview shown above the full input.
    pub input_preview: &'a str,
    /// Pretty-printed tool input, already bounded by the caller.
    pub input_json: &'a str,
    /// True when the caller had to cut the input for Feishu's card limit.
    pub input_truncated: bool,
    /// Signed action string for the allow button (also what the token binds).
    pub allow_action: &'a str,
    /// Signed callback token for the allow button.
    pub allow_token: &'a str,
    /// Signed action string for the deny button.
    pub deny_action: &'a str,
    /// Signed callback token for the deny button.
    pub deny_token: &'a str,
    /// Optional "allow for this session" button. Only offered when the
    /// provider gave a usable session suggestion (Claude) or supports
    /// `acceptForSession` (Codex).
    pub session_action: Option<&'a str>,
    pub session_token: Option<&'a str>,
    pub timeout_minutes: u64,
}

/// A tool-permission prompt as a standalone interactive card.
///
/// Kept separate from the progress card: an approval can outlive the 8.5-minute
/// streaming window, and a new message is what notifies the user that the agent
/// is waiting. The button payload is signed by the bridge's process-local
/// callback key and binds the requesting run's scope, the operator allowed to
/// answer and the request id.
pub fn approval_card(input: &ApprovalCardInput<'_>) -> Value {
    let mut elements = vec![json!({
        "tag": "markdown",
        "content": format!("🔐 **需要授权：{}**", input.tool_name),
    })];
    if let Some(context) = input.context.filter(|text| !text.trim().is_empty()) {
        elements.push(json!({
            "tag": "markdown",
            "content": mask_emails(context),
            "text_size": "notation",
        }));
    }
    if let Some(description) = input.description.filter(|text| !text.trim().is_empty()) {
        elements.push(json!({
            "tag": "markdown",
            "content": mask_emails(description),
            "text_size": "notation",
        }));
    }
    let preview = mask_emails(input.input_preview.trim());
    if !preview.is_empty() {
        elements.push(json!({
            "tag": "markdown",
            "content": format!("```\n{preview}\n```"),
        }));
    }
    let body = mask_emails(input.input_json.trim());
    if !body.is_empty() && body != preview {
        elements.push(json!({
            "tag": "collapsible_panel",
            "expanded": false,
            "header": {
                "title": { "tag": "markdown", "content": "完整参数" },
                "vertical_align": "center",
                "icon": {
                    "tag": "standard_icon",
                    "token": "down-small-ccm_outlined",
                    "size": "16px 16px"
                },
                "icon_position": "follow_text",
                "icon_expanded_angle": -180
            },
            "border": { "color": "grey", "corner_radius": "5px" },
            "vertical_spacing": "8px",
            "padding": "8px 8px 8px 8px",
            "elements": [{
                "tag": "markdown",
                "content": format!("```json\n{body}\n```"),
                "text_size": "notation"
            }]
        }));
    }
    elements.push(json!({
        "tag": "button",
        "text": { "tag": "plain_text", "content": "✅ 允许" },
        "type": "primary",
        "behaviors": [{ "type": "callback", "value": {
            "__bridge_cb": true,
            "action": input.allow_action,
            "scope": input.scope,
            "operator": input.operator,
            "run_id": input.run_id,
            "request_id": input.request_id,
            "decision": "allow",
            "token": input.allow_token,
        } }]
    }));
    if let (Some(action), Some(token)) = (input.session_action, input.session_token) {
        elements.push(json!({
            "tag": "button",
            "text": { "tag": "plain_text", "content": "✅ 本次会话都允许" },
            "type": "default",
            "behaviors": [{ "type": "callback", "value": {
                "__bridge_cb": true,
                "action": action,
                "scope": input.scope,
                "operator": input.operator,
                "run_id": input.run_id,
                "request_id": input.request_id,
                "decision": "allow_session",
                "token": token,
            } }]
        }));
    }
    elements.push(json!({
        "tag": "button",
        "text": { "tag": "plain_text", "content": "⛔ 拒绝" },
        "type": "danger",
        "behaviors": [{ "type": "callback", "value": {
            "__bridge_cb": true,
            "action": input.deny_action,
            "scope": input.scope,
            "operator": input.operator,
            "run_id": input.run_id,
            "request_id": input.request_id,
            "decision": "deny",
            "token": input.deny_token,
        } }]
    }));
    let mut note = format!(
        "仅发起本次任务的用户可以操作；{} 分钟内未处理将自动拒绝。",
        input.timeout_minutes
    );
    if input.input_truncated {
        note.push_str("\n参数超过飞书卡片上限，已截断显示。");
    }
    elements.push(json!({
        "tag": "markdown",
        "content": note,
        "text_size": "notation"
    }));
    json!({
        "schema": "2.0",
        "config": {
            "update_multi": true,
            "streaming_mode": false,
            "summary": { "content": format!("等待授权：{}", input.tool_name) }
        },
        "body": { "elements": elements }
    })
}

/// One option button on a user-question card.
#[derive(Debug, Clone)]
pub struct QuestionOptionButton {
    pub label: String,
    pub description: String,
    /// Signed action string; the bridge rebuilds and verifies it on click.
    pub action: String,
    pub token: String,
}

/// One question rendered on a user-question card.
#[derive(Debug, Clone)]
pub struct QuestionCardQuestion {
    pub header: String,
    pub question: String,
    pub options: Vec<QuestionOptionButton>,
    /// Indices of options currently selected.
    pub selected: Vec<usize>,
    pub multi_select: bool,
    /// True once a single-select question has been answered (hide its buttons).
    pub locked: bool,
}

/// A card that collects answers to one or more `AskUserQuestion` /
/// `request_user_input` questions.
pub struct QuestionCardInput<'a> {
    pub scope: &'a str,
    pub operator: &'a str,
    pub run_id: u64,
    pub request_id: &'a str,
    pub questions: &'a [QuestionCardQuestion],
    /// Submit button for multi-select questions, when every question has at
    /// least one selection. The action/token are signed by the caller.
    pub submit_action: Option<(&'a str, &'a str)>,
    pub timeout_minutes: u64,
}

pub fn question_card(input: &QuestionCardInput<'_>) -> Value {
    let mut elements = vec![json!({
        "tag": "markdown",
        "content": "🙋 **需要你的选择**",
    })];
    for (question_index, question) in input.questions.iter().enumerate() {
        if !question.header.trim().is_empty() {
            elements.push(json!({
                "tag": "markdown",
                "content": format!("**{}**", question.header),
                "text_size": "notation",
            }));
        }
        elements.push(json!({
            "tag": "markdown",
            "content": format!("{}. {}", question_index + 1, question.question),
        }));
        if question.locked {
            let labels = question
                .selected
                .iter()
                .filter_map(|index| question.options.get(*index))
                .map(|option| option.label.clone())
                .collect::<Vec<_>>()
                .join("、");
            elements.push(json!({
                "tag": "markdown",
                "content": format!("✅ 已选择：{labels}"),
                "text_size": "notation",
            }));
            continue;
        }
        for (option_index, option) in question.options.iter().enumerate() {
            let selected = question.selected.contains(&option_index);
            let prefix = if selected {
                "✅"
            } else if question.multi_select {
                "◻️"
            } else {
                ""
            };
            elements.push(json!({
                "tag": "button",
                "text": { "tag": "plain_text", "content": format!("{prefix} {}", option.label) },
                "type": if selected { "primary" } else { "default" },
                "behaviors": [{ "type": "callback", "value": {
                    "__bridge_cb": true,
                    "action": option.action,
                    "scope": input.scope,
                    "operator": input.operator,
                    "run_id": input.run_id,
                    "request_id": input.request_id,
                    "question_index": question_index,
                    "option_index": option_index,
                    "submit": false,
                    "token": option.token,
                } }]
            }));
            if !option.description.trim().is_empty() {
                elements.push(json!({
                    "tag": "markdown",
                    "content": mask_emails(&option.description),
                    "text_size": "notation",
                }));
            }
        }
    }
    if let Some((action, token)) = input.submit_action {
        elements.push(json!({
            "tag": "button",
            "text": { "tag": "plain_text", "content": "提交选择" },
            "type": "primary",
            "behaviors": [{ "type": "callback", "value": {
                "__bridge_cb": true,
                "action": action,
                "scope": input.scope,
                "operator": input.operator,
                "run_id": input.run_id,
                "request_id": input.request_id,
                "question_index": 0,
                "option_index": 0,
                "submit": true,
                "token": token,
            } }]
        }));
    }
    elements.push(json!({
        "tag": "markdown",
        "content": format!(
            "仅发起本次任务的用户可以操作；{} 分钟内未回答将自动拒绝。",
            input.timeout_minutes
        ),
        "text_size": "notation"
    }));
    json!({
        "schema": "2.0",
        "config": {
            "update_multi": true,
            "streaming_mode": false,
            "summary": { "content": "等待你的选择" }
        },
        "body": { "elements": elements }
    })
}

/// The question card after an answer/timeout: status plus a compact Q/A log.
pub fn question_decided_card(status: &str, answers: &[(String, String)]) -> Value {
    let mut elements = vec![json!({
        "tag": "markdown",
        "content": format!("🙋 **{status}**"),
    })];
    for (question, answer) in answers {
        elements.push(json!({
            "tag": "markdown",
            "content": format!("**{}**\\n{}", mask_emails(question), mask_emails(answer)),
            "text_size": "notation",
        }));
    }
    json!({
        "schema": "2.0",
        "config": {
            "update_multi": true,
            "streaming_mode": false,
            "summary": { "content": status }
        },
        "body": { "elements": elements }
    })
}

/// A form card for questions that need free-text answers (Codex
/// `request_user_input` without options, or any question with no options).
pub struct QuestionTextCardInput<'a> {
    pub scope: &'a str,
    pub operator: &'a str,
    pub run_id: u64,
    pub request_id: &'a str,
    pub questions: &'a [UserQuestion],
    pub submit_action: &'a str,
    pub submit_token: &'a str,
    pub timeout_minutes: u64,
}

pub fn question_text_card(input: &QuestionTextCardInput<'_>) -> Value {
    let mut intro = vec![json!({
        "tag": "markdown",
        "content": "🙋 **需要你的回答**",
    })];
    let mut form_elements = Vec::new();
    for (index, question) in input.questions.iter().enumerate() {
        if !question.header.trim().is_empty() {
            intro.push(json!({
                "tag": "markdown",
                "content": format!("**{}**", mask_emails(&question.header)),
                "text_size": "notation",
            }));
        }
        intro.push(json!({
            "tag": "markdown",
            "content": format!("{}. {}", index + 1, mask_emails(&question.question)),
        }));
        let label = if question.header.trim().is_empty() {
            format!("问题 {}", index + 1)
        } else {
            mask_emails(&question.header)
        };
        form_elements.push(json!({
            "tag": "input",
            "name": format!("q_{index}"),
            "label": { "tag": "plain_text", "content": label },
            "placeholder": { "tag": "plain_text", "content": "输入你的回答" },
            "max_length": 1_000,
            "required": true,
        }));
    }
    form_elements.push(json!({
        "tag": "button",
        "name": "submit",
        "text": { "tag": "plain_text", "content": "提交回答" },
        "type": "primary",
        "form_action_type": "submit",
        "behaviors": [{ "type": "callback", "value": {
            "__bridge_cb": true,
            "action": input.submit_action,
            "scope": input.scope,
            "operator": input.operator,
            "run_id": input.run_id,
            "request_id": input.request_id,
            "question_count": input.questions.len(),
            "question_index": 0,
            "option_index": 0,
            "submit": true,
            "token": input.submit_token,
        } }]
    }));
    intro.push(json!({
        "tag": "form",
        "name": "question_form",
        "elements": form_elements,
    }));
    intro.push(json!({
        "tag": "markdown",
        "content": format!(
            "仅发起本次任务的用户可以操作；{} 分钟内未回答将自动拒绝。",
            input.timeout_minutes
        ),
        "text_size": "notation"
    }));
    json!({
        "schema": "2.0",
        "config": {
            "update_multi": true,
            "streaming_mode": false,
            "summary": { "content": "等待你的回答" }
        },
        "body": { "elements": intro }
    })
}

/// Convert parsed user questions into the renderer's per-question shape.
///
/// The caller supplies the signed action/token for every option. Questions
/// without options are skipped by the card (the caller should fall back to a
/// text notice).
pub fn question_card_questions(
    questions: &[UserQuestion],
    actions: &[Vec<(String, String)>],
) -> Vec<QuestionCardQuestion> {
    questions
        .iter()
        .enumerate()
        .map(|(index, question)| QuestionCardQuestion {
            header: question.header.clone(),
            question: question.question.clone(),
            options: question
                .options
                .iter()
                .enumerate()
                .map(|(option_index, option)| {
                    let (action, token) = actions
                        .get(index)
                        .and_then(|row| row.get(option_index))
                        .cloned()
                        .unwrap_or_default();
                    QuestionOptionButton {
                        label: option.label.clone(),
                        description: option.description.clone(),
                        action,
                        token,
                    }
                })
                .collect(),
            selected: Vec::new(),
            multi_select: question.multi_select,
            locked: false,
        })
        .collect()
}

/// The approval card after a decision: status line, no buttons.
pub fn approval_decided_card(tool_name: &str, status: &str) -> Value {
    json!({
        "schema": "2.0",
        "config": {
            "update_multi": true,
            "streaming_mode": false,
            "summary": { "content": status }
        },
        "body": { "elements": [
            { "tag": "markdown", "content": format!("🔐 **{tool_name}**") },
            { "tag": "markdown", "content": status }
        ] }
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    // ---- fold -----------------------------------------------------------------

    fn applied(events: &[AgentEvent]) -> RunView {
        let mut view = RunView::default();
        for event in events {
            view.apply(event);
        }
        view
    }

    fn text(delta: &str) -> AgentEvent {
        AgentEvent::Text {
            delta: delta.into(),
        }
    }

    fn thinking(delta: &str) -> AgentEvent {
        AgentEvent::Thinking {
            delta: delta.into(),
        }
    }

    fn tool_use(id: &str, name: &str, input: serde_json::Value) -> AgentEvent {
        AgentEvent::ToolUse {
            id: id.into(),
            name: name.into(),
            input,
        }
    }

    fn tool_result(id: &str, output: &str, is_error: bool) -> AgentEvent {
        AgentEvent::ToolResult {
            id: id.into(),
            output: output.into(),
            is_error,
        }
    }

    fn done(reason: TerminationReason) -> AgentEvent {
        AgentEvent::Done {
            session_id: None,
            thread_id: None,
            termination_reason: reason,
        }
    }

    fn error(message: &str, reason: TerminationReason) -> AgentEvent {
        AgentEvent::Error {
            message: message.into(),
            termination_reason: reason,
        }
    }

    #[test]
    fn folds_text_reasoning_and_tools() {
        let mut view = RunView::default();
        view.apply(&text("hello "));
        view.apply(&thinking("thinking"));
        view.apply(&tool_use("t1", "Bash", json!({ "command": "ls -la" })));
        view.apply(&done(TerminationReason::Normal));

        assert_eq!(view.best_answer(), "hello ");
        assert_eq!(view.session_key(), None);
        assert_eq!(view.tools.len(), 1);
        assert!(view.finished);
        assert!(stats_contains(&view, "2.0s"));

        // The blocks interleave text and tools in event order.
        assert_eq!(
            view.blocks,
            vec![
                Block::Text {
                    content: "hello ".into(),
                    streaming: false,
                },
                Block::Tool(0),
            ]
        );
        assert_eq!(view.tool_calls[0].name, "Bash");
        assert_eq!(view.tool_calls[0].status, ToolStatus::Running);
        assert_eq!(view.terminal(), Terminal::Done);
    }

    #[test]
    fn final_text_replaces_streamed_commentary() {
        let mut view = RunView::default();
        view.apply(&text("working"));
        view.apply(&AgentEvent::FinalText {
            content: "answer".into(),
        });
        assert_eq!(view.best_answer(), "answer");
        assert_eq!(
            view.blocks,
            vec![Block::Text {
                content: "answer".into(),
                streaming: false,
            }]
        );
    }

    #[test]
    fn final_text_after_a_tool_gets_its_own_trailing_block() {
        let view = applied(&[
            text("working"),
            tool_use("t1", "Bash", json!({ "command": "pwd" })),
            tool_result("t1", "/repo", false),
            AgentEvent::FinalText {
                content: "answer".into(),
            },
        ]);

        assert_eq!(
            view.blocks,
            vec![
                Block::Text {
                    content: "working".into(),
                    streaming: false,
                },
                Block::Tool(0),
                Block::Text {
                    content: "answer".into(),
                    streaming: false,
                },
            ]
        );
    }

    #[test]
    fn final_text_already_streamed_before_a_tool_is_not_duplicated() {
        let view = applied(&[
            AgentEvent::FinalText {
                content: "answer".into(),
            },
            tool_use("t1", "Bash", json!({ "command": "pwd" })),
            tool_result("t1", "/repo", false),
            AgentEvent::FinalText {
                content: "answer".into(),
            },
        ]);

        let text_blocks: Vec<&str> = view
            .blocks
            .iter()
            .filter_map(|block| match block {
                Block::Text { content, .. } => Some(content.as_str()),
                Block::Tool(_) => None,
            })
            .collect();
        assert_eq!(text_blocks, vec!["answer"], "{:?}", view.blocks);
        assert_eq!(view.best_answer(), "answer");
    }

    #[test]
    fn error_becomes_the_answer() {
        let mut view = RunView::default();
        view.apply(&error("boom", TerminationReason::Failed));
        assert_eq!(view.best_answer(), "boom");
        assert!(view.finished);
        assert_eq!(view.terminal(), Terminal::Error);
    }

    #[test]
    fn initial_view_is_running_and_empty() {
        let view = RunView::default();
        assert_eq!(view.best_answer(), "");
        assert!(view.reasoning.is_empty());
        assert!(view.tools.is_empty());
        assert!(view.tool_calls.is_empty());
        assert!(view.blocks.is_empty());
        assert_eq!(view.tool_errors, 0);
        assert!(view.session_key().is_none());
        assert!(view.error.is_none());
        assert_eq!(view.termination, None);
        assert!(!view.finished);
        assert!(!view.reasoning_active);
        assert_eq!(view.footer_status, Some(FooterStatus::Thinking));
        assert_eq!(view.idle_timeout_minutes, None);
        assert_eq!(view.terminal(), Terminal::Running);
        assert_eq!(view.footer(Duration::ZERO), "0.0s");
    }

    #[test]
    fn folds_thinking_before_and_after_the_answer() {
        let mut view = applied(&[thinking("checking options")]);
        assert_eq!(view.reasoning, "checking options");
        assert!(view.reasoning_active);
        assert_eq!(view.footer_status, Some(FooterStatus::Thinking));
        assert!(!view.finished);

        view.apply(&text("final answer"));
        assert!(!view.reasoning_active);
        assert_eq!(view.footer_status, Some(FooterStatus::Streaming));
        view.apply(&done(TerminationReason::Normal));
        assert_eq!(view.reasoning, "checking options");
        assert_eq!(view.best_answer(), "final answer");
        assert_eq!(view.termination, Some(TerminationReason::Normal));
        assert_eq!(view.footer_status, None);
        assert!(view.finished);
    }

    #[test]
    fn folds_tool_running_done_and_error_states() {
        let running = applied(&[tool_use("tool-1", "Bash", json!({ "command": "pwd" }))]);
        assert_eq!(running.tools, vec!["Bash: pwd"]);
        assert_eq!(running.tool_errors, 0);
        assert_eq!(running.tool_calls[0].status, ToolStatus::Running);
        assert_eq!(running.footer_status, Some(FooterStatus::ToolRunning));
        assert!(!running.finished);

        let completed = applied(&[
            tool_use("tool-1", "Bash", json!({ "command": "pwd" })),
            tool_result("tool-1", "/repo", false),
            done(TerminationReason::Normal),
        ]);
        assert_eq!(completed.tool_errors, 0);
        assert_eq!(completed.tool_calls[0].status, ToolStatus::Done);
        assert_eq!(completed.tool_calls[0].output.as_deref(), Some("/repo"));
        assert_eq!(completed.termination, Some(TerminationReason::Normal));
        assert!(completed.finished);

        let failed = applied(&[
            tool_use("tool-2", "Read", json!({ "file_path": "/missing.ts" })),
            tool_result("tool-2", "ENOENT", true),
            done(TerminationReason::Normal),
        ]);
        assert_eq!(failed.tool_errors, 1);
        assert_eq!(failed.tool_calls[0].status, ToolStatus::Error);
        assert!(stats_contains(&failed, "1 failed"));
        assert!(failed.finished);
    }

    #[test]
    fn keeps_consecutive_tool_calls_in_order() {
        let events = [
            tool_use("tool-1", "Bash", json!({ "command": "pwd" })),
            tool_result("tool-1", "/repo", false),
            tool_use("tool-2", "Read", json!({ "file_path": "/repo/a.ts" })),
            tool_result("tool-2", "a", false),
            tool_use("tool-3", "Edit", json!({ "file_path": "/repo/a.ts" })),
        ];
        let running = applied(&events);
        assert_eq!(
            running.tools,
            vec!["Bash: pwd", "Read: /repo/a.ts", "Edit: /repo/a.ts"]
        );
        assert_eq!(running.tool_errors, 0);
        assert_eq!(
            running.blocks,
            vec![Block::Tool(0), Block::Tool(1), Block::Tool(2)]
        );
        assert!(!running.finished);

        let mut with_result = events.to_vec();
        with_result.push(tool_result("tool-3", "ok", false));
        with_result.push(done(TerminationReason::Normal));
        let completed = applied(&with_result);
        assert_eq!(completed.tools.len(), 3);
        assert_eq!(completed.tool_errors, 0);
        assert!(completed.finished);
    }

    #[test]
    fn records_done_error_interrupted_and_idle_timeout_terminals() {
        let finished = applied(&[done(TerminationReason::Normal)]);
        assert_eq!(finished.termination, Some(TerminationReason::Normal));
        assert!(finished.finished);
        assert_eq!(finished.terminal(), Terminal::Done);

        let failed = applied(&[AgentEvent::Error {
            message: "process failed".into(),
            termination_reason: TerminationReason::Failed,
        }]);
        assert_eq!(failed.best_answer(), "process failed");
        assert_eq!(failed.error.as_deref(), Some("process failed"));
        assert_eq!(failed.termination, Some(TerminationReason::Failed));
        assert!(failed.finished);
        assert_eq!(failed.terminal(), Terminal::Error);

        let interrupted = applied(&[text("partial"), done(TerminationReason::Interrupted)]);
        assert_eq!(interrupted.best_answer(), "partial");
        assert_eq!(
            interrupted.termination,
            Some(TerminationReason::Interrupted)
        );
        assert!(interrupted.finished);
        assert_eq!(interrupted.terminal(), Terminal::Interrupted);

        let done_timeout = applied(&[done(TerminationReason::Timeout)]);
        assert_eq!(done_timeout.termination, Some(TerminationReason::Timeout));
        assert!(done_timeout.finished);
        assert_eq!(done_timeout.terminal(), Terminal::IdleTimeout);

        let timed_out = applied(&[AgentEvent::Error {
            message: "run timed out".into(),
            termination_reason: TerminationReason::Timeout,
        }]);
        assert_eq!(timed_out.termination, Some(TerminationReason::Timeout));
        assert!(timed_out.finished);
        assert_eq!(timed_out.terminal(), Terminal::IdleTimeout);

        let mut marked = applied(&[text("partial")]);
        marked.mark_idle_timeout(15);
        assert_eq!(marked.terminal(), Terminal::IdleTimeout);
        assert_eq!(marked.idle_timeout_minutes, Some(15));
        assert_eq!(marked.best_answer(), "partial");
        assert_eq!(marked.footer_status, None);
    }

    #[test]
    fn keeps_local_paths_in_answers_and_tool_lines() {
        let path = "/Users/example/private/customer/repo/secret.txt";
        let view = applied(&[
            text(&format!("I read {path}")),
            tool_use("tool-1", "Read", json!({ "file_path": path })),
            tool_result("tool-1", &format!("content from {path}"), false),
            done(TerminationReason::Normal),
        ]);

        assert!(view.best_answer().contains(path));
        assert_eq!(view.tools, vec![format!("Read: {path}")]);
    }

    #[test]
    fn footer_reports_elapsed_tokens_tools_and_failures_in_order() {
        let view = applied(&[
            AgentEvent::Usage {
                input_tokens: Some(10),
                output_tokens: Some(20),
                cached_input_tokens: None,
                reasoning_output_tokens: None,
                cost_usd: None,
            },
            tool_use("tool-1", "Bash", json!({ "command": "pwd" })),
            tool_result("tool-1", "/repo", false),
            tool_use("tool-2", "Read", json!({ "file_path": "/missing.ts" })),
            tool_result("tool-2", "ENOENT", true),
        ]);

        assert_eq!(
            view.footer(Duration::from_millis(1_500)),
            "1.5s · 10 in / 20 out · 2 tool call(s) · 1 failed"
        );
        assert_eq!(
            applied(&[done(TerminationReason::Normal)]).footer(Duration::from_secs(2)),
            "2.0s"
        );
    }

    #[test]
    fn tool_lines_truncate_long_details_with_a_single_ellipsis() {
        let long = "a".repeat(100);
        let view = applied(&[tool_use("tool-1", "Bash", json!({ "command": long }))]);

        assert_eq!(view.tools, vec![format!("Bash: {}…", "a".repeat(80))]);
    }

    #[test]
    fn tool_lines_prefer_command_then_path_then_file_path_then_the_name() {
        let command = applied(&[tool_use(
            "tool-1",
            "Bash",
            json!({ "command": "pwd", "path": "/p", "file_path": "/f" }),
        )]);
        assert_eq!(command.tools, vec!["Bash: pwd"]);

        let path = applied(&[tool_use(
            "tool-1",
            "Read",
            json!({ "path": "/p", "file_path": "/f" }),
        )]);
        assert_eq!(path.tools, vec!["Read: /p"]);

        let file_path = applied(&[tool_use("tool-1", "Read", json!({ "file_path": "/f" }))]);
        assert_eq!(file_path.tools, vec!["Read: /f"]);

        let name_only = applied(&[tool_use("tool-1", "TodoWrite", json!({ "todos": [] }))]);
        assert_eq!(name_only.tools, vec!["TodoWrite"]);
    }

    #[test]
    fn system_and_done_events_keep_both_id_flavours() {
        let system = applied(&[AgentEvent::System {
            session_id: Some("sess-1".into()),
            thread_id: Some("thread-1".into()),
            cwd: None,
            model: None,
        }]);
        assert_eq!(system.session_id.as_deref(), Some("sess-1"));
        assert_eq!(system.thread_id.as_deref(), Some("thread-1"));
        assert!(!system.finished);
        assert_eq!(system.session_key(), Some("sess-1"));

        let mut with_ids = applied(&[done(TerminationReason::Normal)]);
        with_ids.apply(&AgentEvent::System {
            session_id: None,
            thread_id: Some("thread-only".into()),
            cwd: None,
            model: None,
        });
        assert_eq!(with_ids.session_key(), Some("thread-only"));
    }

    #[test]
    fn session_key_prefers_the_session_id_over_the_thread_id() {
        let view = applied(&[AgentEvent::System {
            session_id: Some("sess".into()),
            thread_id: Some("thread".into()),
            cwd: None,
            model: None,
        }]);
        assert_eq!(view.session_key(), Some("sess"));

        let thread_only = applied(&[AgentEvent::System {
            session_id: None,
            thread_id: Some("thread".into()),
            cwd: None,
            model: None,
        }]);
        assert_eq!(thread_only.session_key(), Some("thread"));
    }

    #[test]
    fn an_error_overrides_a_final_answer_for_the_visible_answer() {
        let view = applied(&[
            AgentEvent::FinalText {
                content: "answer".into(),
            },
            AgentEvent::Error {
                message: "boom".into(),
                termination_reason: TerminationReason::Failed,
            },
        ]);

        assert_eq!(view.final_answer.as_deref(), Some("answer"));
        assert_eq!(view.best_answer(), "boom");
        assert_eq!(view.error.as_deref(), Some("boom"));
        assert_eq!(view.termination, Some(TerminationReason::Failed));
        assert!(view.finished);
    }

    #[test]
    fn mark_interrupted_keeps_the_partial_answer_and_a_real_terminal_wins() {
        let mut partial = applied(&[text("partial answer")]);
        partial.mark_interrupted();
        assert_eq!(partial.best_answer(), "partial answer");
        assert_eq!(partial.termination, Some(TerminationReason::Interrupted));
        assert!(partial.is_interrupted());
        assert!(partial.finished);
        assert_eq!(partial.footer_status, None);

        let mut done = applied(&[text("answer"), done(TerminationReason::Normal)]);
        done.mark_interrupted();
        assert_eq!(done.termination, Some(TerminationReason::Normal));
        assert!(!done.is_interrupted());
        assert_eq!(done.best_answer(), "answer");

        let mut failed = applied(&[error("boom", TerminationReason::Failed)]);
        failed.mark_interrupted();
        assert_eq!(failed.termination, Some(TerminationReason::Failed));
        assert_eq!(failed.best_answer(), "boom");
    }

    #[test]
    fn usage_with_missing_numbers_is_zero() {
        let view = applied(&[AgentEvent::Usage {
            input_tokens: None,
            output_tokens: None,
            cached_input_tokens: None,
            reasoning_output_tokens: None,
            cost_usd: None,
        }]);
        assert_eq!(view.input_tokens, 0);
        assert_eq!(view.output_tokens, 0);
        assert_eq!(view.footer(Duration::ZERO), "0.0s");
    }

    // ---- card rendering helpers ----------------------------------------------

    fn render(view: &RunView) -> Value {
        render_card(view, &CardRenderOptions::default())
    }

    fn render_with_stop(view: &RunView) -> Value {
        render_card(
            view,
            &CardRenderOptions {
                stop_value: Some(json!({ "cmd": "stop", "token": "tk" })),
                answer_element_id: Some("stream_md".into()),
                ..CardRenderOptions::default()
            },
        )
    }

    fn elements(card: &Value) -> &Vec<Value> {
        card["body"]["elements"]
            .as_array()
            .expect("card body elements")
    }

    fn panel_titles(card: &Value) -> Vec<String> {
        elements(card)
            .iter()
            .filter(|element| element["tag"] == "collapsible_panel")
            .filter_map(|element| {
                element["header"]["title"]["content"]
                    .as_str()
                    .map(str::to_string)
            })
            .collect()
    }

    fn panel_bodies(card: &Value) -> Vec<String> {
        elements(card)
            .iter()
            .filter(|element| element["tag"] == "collapsible_panel")
            .filter_map(|element| {
                element["elements"][0]["content"]
                    .as_str()
                    .map(str::to_string)
            })
            .collect()
    }

    fn notation_texts(card: &Value) -> Vec<String> {
        elements(card)
            .iter()
            .filter(|element| element["tag"] == "markdown" && element["text_size"] == "notation")
            .filter_map(|element| element["content"].as_str().map(str::to_string))
            .collect()
    }

    fn plain_markdown(card: &Value) -> Vec<String> {
        elements(card)
            .iter()
            .filter(|element| element["tag"] == "markdown" && element.get("text_size").is_none())
            .filter_map(|element| element["content"].as_str().map(str::to_string))
            .collect()
    }

    fn serialized(card: &Value) -> String {
        serde_json::to_string(card).expect("card serializes")
    }

    fn stats_contains(view: &RunView, needle: &str) -> bool {
        view.footer(Duration::from_secs(2)).contains(needle)
    }

    // ---- reasoning panel ------------------------------------------------------

    #[test]
    fn reasoning_panel_is_open_while_thinking_and_titled_done_afterwards() {
        let running = applied(&[thinking("checking options")]);
        let card = render(&running);
        assert_eq!(panel_titles(&card), vec!["🧠 **思考中**"]);
        assert_eq!(panel_bodies(&card), vec!["checking options"]);
        assert_eq!(elements(&card)[0]["expanded"], true);
        assert_eq!(elements(&card)[0]["border"]["color"], "grey");

        let mut finished = running.clone();
        finished.apply(&done(TerminationReason::Normal));
        let card = render(&finished);
        assert_eq!(panel_titles(&card), vec!["🧠 **思考完成，点击查看**"]);
        assert_eq!(elements(&card)[0]["expanded"], false);
    }

    #[test]
    fn reasoning_panel_truncates_to_the_ts_cap() {
        let reasoning = "思".repeat(REASONING_MAX_CHARS + 10);
        let view = applied(&[thinking(&reasoning)]);
        let card = render(&view);
        let body = &panel_bodies(&card)[0];
        assert_eq!(body.chars().count(), REASONING_MAX_CHARS + 1);
        assert!(body.ends_with('…'));
    }

    #[test]
    fn reasoning_panel_can_be_disabled() {
        let view = applied(&[thinking("hidden")]);
        let card = render_card(
            &view,
            &CardRenderOptions {
                reasoning: false,
                ..CardRenderOptions::default()
            },
        );
        assert!(panel_titles(&card).is_empty());
        assert!(!serialized(&card).contains("hidden"));
    }

    // ---- tool panels ----------------------------------------------------------

    #[test]
    fn tool_panels_carry_status_icons_borders_and_bodies() {
        let running = applied(&[tool_use("t1", "Bash", json!({ "command": "pwd" }))]);
        let card = render(&running);
        assert_eq!(panel_titles(&card), vec!["⏳ **Bash** — pwd"]);
        assert_eq!(elements(&card)[0]["border"]["color"], "grey");
        assert_eq!(
            panel_bodies(&card),
            vec!["**Command**\n```bash\npwd\n```\n\n_运行中…_"]
        );

        let completed = applied(&[
            tool_use("t1", "Bash", json!({ "command": "pwd" })),
            tool_result("t1", "/repo", false),
            done(TerminationReason::Normal),
        ]);
        let card = render(&completed);
        assert_eq!(panel_titles(&card), vec!["✅ **Bash** — pwd"]);
        assert_eq!(elements(&card)[0]["border"]["color"], "grey");
        assert_eq!(
            panel_bodies(&card),
            vec!["**Command**\n```bash\npwd\n```\n\n**Output**\n```\n/repo\n```"]
        );

        let failed = applied(&[
            tool_use("t2", "Read", json!({ "file_path": "/missing.ts" })),
            tool_result("t2", "ENOENT", true),
            done(TerminationReason::Normal),
        ]);
        let card = render(&failed);
        assert_eq!(panel_titles(&card), vec!["❌ **Read** — /missing.ts"]);
        assert_eq!(elements(&card)[0]["border"]["color"], "red");
        assert_eq!(
            panel_bodies(&card),
            vec!["**File** `/missing.ts`\n\n**Error**\n```\nENOENT\n```"]
        );
    }

    #[test]
    fn tool_panel_without_body_uses_the_ts_placeholder() {
        let view = applied(&[
            tool_use("t1", "TodoWrite", json!({ "todos": [] })),
            tool_result("t1", "", false),
            done(TerminationReason::Normal),
        ]);
        let card = render(&view);
        assert_eq!(panel_bodies(&card), vec!["_无输出_"]);
    }

    #[test]
    fn tool_header_summaries_follow_the_ts_tool_table() {
        for (name, input, expected) in [
            ("Bash", json!({ "command": "git  status" }), "git status"),
            ("Read", json!({ "file_path": "/a/b.ts" }), "/a/b.ts"),
            (
                "Grep",
                json!({ "pattern": "foo", "path": "/src" }),
                "foo in /src",
            ),
            ("Glob", json!({ "pattern": "**/*.rs" }), "**/*.rs"),
            (
                "WebFetch",
                json!({ "url": "https://x.test" }),
                "https://x.test",
            ),
            ("WebSearch", json!({ "query": "rust card" }), "rust card"),
            (
                "Agent",
                json!({ "description": "audit", "subagent_type": "x" }),
                "audit",
            ),
            ("Task", json!({ "subagent_type": "explore" }), "explore"),
            ("Custom", json!({ "query": "q" }), "q"),
        ] {
            let view = applied(&[tool_use("t1", name, input)]);
            let card = render(&view);
            assert_eq!(
                panel_titles(&card),
                vec![format!("⏳ **{name}** — {expected}")],
                "tool {name}"
            );
        }
    }

    #[test]
    fn tool_body_caps_match_the_ts_renderer() {
        // Header cap: 80 chars + ellipsis.
        let long = "x".repeat(200);
        let card = render(&applied(&[tool_use(
            "t1",
            "Bash",
            json!({ "command": long }),
        )]));
        assert_eq!(
            panel_titles(&card),
            vec![format!("⏳ **Bash** — {}…", "x".repeat(80))]
        );

        // Input field cap: 600 chars + ellipsis.
        let card = render(&applied(&[tool_use(
            "t1",
            "Bash",
            json!({ "command": "y".repeat(1_000) }),
        )]));
        let body = &panel_bodies(&card)[0];
        assert!(body.contains(&format!("{}…", "y".repeat(600))));

        // Output cap: 1200 chars + ellipsis.
        let view = applied(&[
            tool_use("t1", "Bash", json!({ "command": "pwd" })),
            tool_result("t1", &"z".repeat(3_000), false),
            done(TerminationReason::Normal),
        ]);
        let body = &panel_bodies(&render(&view))[0];
        assert!(body.contains(&format!("{}…", "z".repeat(1_200))));

        // Cumulative body cap: 2500 chars + the TS note (Grep fields are not
        // field-capped, so they can push the body past the total).
        let view = applied(&[
            tool_use(
                "t1",
                "Grep",
                json!({ "pattern": "p".repeat(2_000), "path": "/p".repeat(2_000) }),
            ),
            tool_result("t1", &"o".repeat(1_200), false),
            done(TerminationReason::Normal),
        ]);
        let body = &panel_bodies(&render(&view))[0];
        assert!(body.contains(TOOL_BODY_TRUNCATION_SUFFIX));
        assert!(
            body.chars().count() <= TOOL_BODY_TOTAL_MAX_CHARS + 40,
            "body cap exceeded: {} chars",
            body.chars().count()
        );
    }

    // ---- collapse rules -------------------------------------------------------

    #[test]
    fn fewer_than_three_tools_render_one_panel_each() {
        // The TS snapshot keeps individual panels collapsed in the UI
        // (`expanded: false`); "expanded" here means "not summarized".
        let view = applied(&[
            tool_use("t1", "Bash", json!({ "command": "pwd" })),
            tool_result("t1", "/repo", false),
            tool_use("t2", "Read", json!({ "file_path": "/a.ts" })),
            tool_result("t2", "a", false),
        ]);
        let card = render(&view);
        assert_eq!(
            panel_titles(&card),
            vec!["✅ **Bash** — pwd", "✅ **Read** — /a.ts"]
        );
        assert!(
            elements(&card)
                .iter()
                .filter(|element| element["tag"] == "collapsible_panel")
                .all(|element| element["expanded"] == false)
        );
        assert!(!serialized(&card).contains("个工具调用"));
    }

    #[test]
    fn three_running_tools_collapse_prior_calls_and_expand_the_latest() {
        let view = applied(&[
            tool_use("t1", "Bash", json!({ "command": "pwd" })),
            tool_result("t1", "/repo", false),
            tool_use("t2", "Read", json!({ "file_path": "/a.ts" })),
            tool_result("t2", "a", false),
            tool_use("t3", "Edit", json!({ "file_path": "/a.ts" })),
        ]);
        let card = render(&view);
        assert_eq!(
            panel_titles(&card),
            vec!["☕ **2 个工具调用**", "⏳ **Edit** — /a.ts"]
        );
        let panels = elements(&card);
        assert_eq!(panels[0]["expanded"], false);
        assert_eq!(panels[0]["border"]["color"], "blue");
        assert_eq!(
            panel_bodies(&card)[0],
            "- ✅ **Bash** — pwd\n- ✅ **Read** — /a.ts"
        );
        assert_eq!(panels[1]["expanded"], true);
        // The summary drops bodies; the latest tool keeps its body.
        assert!(!panel_bodies(&card)[0].contains("**Command**"));
        assert!(panel_bodies(&card)[1].contains("**File** `/a.ts`"));
    }

    #[test]
    fn finished_runs_collapse_every_tool_into_one_summary() {
        let view = applied(&[
            tool_use("t1", "Bash", json!({ "command": "pwd" })),
            tool_result("t1", "/repo", false),
            tool_use("t2", "Read", json!({ "file_path": "/a.ts" })),
            tool_result("t2", "a", false),
            tool_use("t3", "Edit", json!({ "file_path": "/a.ts" })),
            tool_result("t3", "ok", false),
            done(TerminationReason::Normal),
        ]);
        let card = render(&view);
        assert_eq!(panel_titles(&card), vec!["☕ **3 个工具调用（已结束）**"]);
        assert_eq!(
            panel_bodies(&card),
            vec!["- ✅ **Bash** — pwd\n- ✅ **Read** — /a.ts\n- ✅ **Edit** — /a.ts"]
        );
        assert_eq!(elements(&card)[0]["expanded"], false);
    }

    #[test]
    fn tool_panels_can_be_hidden_without_hiding_the_answer() {
        let view = applied(&[
            text("answer"),
            tool_use("t1", "Bash", json!({ "command": "pwd" })),
            done(TerminationReason::Normal),
        ]);
        let card = render_card(
            &view,
            &CardRenderOptions {
                tool_panels: false,
                ..CardRenderOptions::default()
            },
        );
        assert!(panel_titles(&card).is_empty());
        assert!(!serialized(&card).contains("Bash"));
        assert_eq!(plain_markdown(&card), vec!["answer"]);
    }

    // ---- blocks / terminal ----------------------------------------------------

    #[test]
    fn text_and_tools_interleave_in_block_order() {
        let view = applied(&[
            text("first"),
            tool_use("t1", "Bash", json!({ "command": "pwd" })),
            tool_result("t1", "/repo", false),
            text("second"),
        ]);
        let card = render(&view);
        let payload = serialized(&card);
        let first = payload.find("first").expect("first text");
        let tool = payload.find("**Bash**").expect("tool panel");
        let second = payload.find("second").expect("second text");
        assert!(first < tool, "{payload}");
        assert!(tool < second, "{payload}");
        assert_eq!(plain_markdown(&card), vec!["first", "second"]);
    }

    #[test]
    fn terminal_notices_match_the_ts_strings() {
        let empty_done = render(&applied(&[done(TerminationReason::Normal)]));
        assert_eq!(notation_texts(&empty_done), vec!["_（未返回内容）_"]);
        assert_eq!(
            empty_done["config"]["summary"]["content"],
            Value::String("已完成".into())
        );

        let interrupted = render(&applied(&[
            text("partial"),
            done(TerminationReason::Interrupted),
        ]));
        assert_eq!(
            notation_texts(&interrupted),
            vec!["_⏹ 已被中断_"],
            "{}",
            serialized(&interrupted)
        );

        let mut timed_out = applied(&[text("partial")]);
        timed_out.mark_idle_timeout(15);
        let timed_out = render(&timed_out);
        assert_eq!(
            notation_texts(&timed_out),
            vec!["_⏱ 15 分钟无响应,已自动终止_"]
        );

        let failed = render(&applied(&[error(
            "process failed",
            TerminationReason::Failed,
        )]));
        assert_eq!(
            notation_texts(&failed),
            vec!["⚠️ agent 失败：process failed"]
        );
    }

    #[test]
    fn long_terminal_notices_fit_without_policy_truncation() {
        let message = "e".repeat(10_000);
        let view = applied(&[error(&message, TerminationReason::Failed)]);
        let card = render(&view);
        let payload = serialized(&card);
        assert!(
            payload.contains(&format!("agent 失败：{message}")),
            "{payload}"
        );
        assert!(payload.len() <= CARD_MAX_BYTES);
    }

    // ---- footer + stop button -------------------------------------------------

    #[test]
    fn live_frames_carry_the_status_phrase_then_meta_then_stop_button() {
        let mut view = applied(&[text("answer")]);
        let card = render_card(
            &view,
            &CardRenderOptions {
                metadata_note: Some("Claude Code · 运行中 · 0.1s".into()),
                stop_value: Some(json!({ "cmd": "stop" })),
                ..CardRenderOptions::default()
            },
        );
        let tags: Vec<&str> = elements(&card)
            .iter()
            .filter_map(|element| element["tag"].as_str())
            .collect();
        assert_eq!(tags, vec!["markdown", "markdown", "note", "button"]);
        assert_eq!(notation_texts(&card), vec!["✍️ 正在输出"]);
        assert_eq!(
            elements(&card)[3]["text"]["content"],
            Value::String("⏹ 终止".into())
        );
        assert_eq!(elements(&card)[3]["type"], "danger");
        assert_eq!(
            elements(&card)[3]["behaviors"][0]["value"],
            json!({ "cmd": "stop" })
        );

        // Footer phrases follow the last event.
        assert_eq!(
            notation_texts(&render(&RunView::default())),
            vec!["🧠 正在思考"]
        );
        let tool = applied(&[tool_use("t1", "Bash", json!({ "command": "pwd" }))]);
        assert_eq!(notation_texts(&render(&tool)), vec!["🧰 正在调用工具"]);

        view.apply(&done(TerminationReason::Normal));
        let terminal = render_card(
            &view,
            &CardRenderOptions {
                stop_value: Some(json!({ "cmd": "stop" })),
                ..CardRenderOptions::default()
            },
        );
        assert!(notation_texts(&terminal).is_empty());
        assert!(
            elements(&terminal)
                .iter()
                .all(|element| element["tag"] != "button"),
            "terminal frame kept a stop button"
        );
    }

    #[test]
    fn paused_frames_can_suppress_the_running_footer() {
        let view = applied(&[text("answer")]);
        let card = render_card(
            &view,
            &CardRenderOptions {
                running_footer: false,
                ..CardRenderOptions::default()
            },
        );
        assert!(notation_texts(&card).is_empty());
    }

    // ---- summary labels -------------------------------------------------------

    #[test]
    fn summary_labels_follow_terminal_and_footer() {
        assert_eq!(card_summary(&RunView::default()), "思考中");
        assert_eq!(card_summary(&applied(&[text("x")])), "正在输出");
        assert_eq!(
            card_summary(&applied(&[tool_use(
                "t1",
                "Bash",
                json!({ "command": "x" })
            )])),
            "正在调用工具"
        );
        assert_eq!(
            card_summary(&applied(&[done(TerminationReason::Normal)])),
            "已完成"
        );
        assert_eq!(
            card_summary(&applied(&[done(TerminationReason::Interrupted)])),
            "已中断"
        );
        assert_eq!(
            card_summary(&applied(&[done(TerminationReason::Timeout)])),
            "已超时"
        );
        assert_eq!(
            card_summary(&applied(&[error("x", TerminationReason::Failed)])),
            "出错"
        );

        assert_eq!(card_status(&RunView::default()), "运行中");
        assert_eq!(
            card_status(&applied(&[done(TerminationReason::Normal)])),
            "已完成"
        );
        assert_eq!(
            card_status(&applied(&[error("x", TerminationReason::Failed)])),
            "出错"
        );
    }

    // ---- email masking + size policy -----------------------------------------

    #[test]
    fn masks_emails_in_every_panel_before_rendering() {
        let view = applied(&[
            thinking("reason a@example.org"),
            text("answer b@example.com"),
            tool_use(
                "t1",
                "Bash",
                json!({ "command": "git log --author=c@example.net" }),
            ),
            tool_result("t1", "mail d@example.io", false),
            error("failed for e@example.dev", TerminationReason::Failed),
        ]);
        let card = render(&view);
        let payload = serialized(&card);
        assert!(!payload.contains('@'), "{payload}");
        for masked in [
            "a[at]example.org",
            "b[at]example.com",
            "c[at]example.net",
            "d[at]example.io",
            "e[at]example.dev",
        ] {
            assert!(payload.contains(masked), "{masked} missing: {payload}");
        }
    }

    #[test]
    fn pathological_terminal_notice_is_capped_as_a_last_resort() {
        let message = "e".repeat(CARD_MAX_BYTES + 5_000);
        let view = applied(&[error(&message, TerminationReason::Failed)]);
        let card = render(&view);
        let payload = serialized(&card);
        assert!(payload.len() <= CARD_MAX_BYTES, "{} bytes", payload.len());
        assert!(payload.contains("agent 失败："), "{payload}");
        assert!(payload.contains("卡片已截断"), "{payload}");
    }

    #[test]
    fn representative_run_stays_under_the_30kb_card_cap() {
        let mut view = RunView::default();
        view.apply(&thinking(&"想".repeat(5_000)));
        view.apply(&text(&"答".repeat(20_000)));
        for index in 0..8 {
            view.apply(&tool_use(
                &format!("t{index}"),
                "Bash",
                json!({ "command": format!("{index}{}", "x".repeat(4_000)) }),
            ));
            view.apply(&tool_result(
                &format!("t{index}"),
                &"y".repeat(4_000),
                index % 2 == 0,
            ));
        }
        view.apply(&text("收尾"));

        let card = render_with_stop(&view);
        let payload = serialized(&card);
        assert!(
            payload.len() <= CARD_MAX_BYTES,
            "card is {} bytes: {payload}",
            payload.len()
        );
        // The answer is the highest-value content: its beginning survives and
        // the truncation is announced.
        assert!(payload.contains("答答答"), "{payload}");
        assert!(payload.contains("内容过长，卡片已截断"), "{payload}");
        // The stop button and status phrase are never dropped.
        assert!(payload.contains("⏹ 终止"), "{payload}");
        assert!(payload.contains("正在输出"), "{payload}");
        // Low-value panels are what the policy sacrifices first.
        assert!(!payload.contains("☕"), "{payload}");
        assert!(!payload.contains("🧠"), "{payload}");
    }

    #[test]
    fn card_renders_the_streaming_element_id_once() {
        let view = applied(&[
            text("first"),
            tool_use("t1", "Bash", json!({ "command": "pwd" })),
            tool_result("t1", "/repo", false),
            text("second"),
        ]);
        let card = render_with_stop(&view);
        assert_eq!(
            serialized(&card).matches("\"element_id\"").count(),
            1,
            "{card}"
        );
        let tagged = elements(&card)
            .iter()
            .find(|element| element.get("element_id").is_some())
            .expect("answer element");
        assert_eq!(tagged["element_id"], "stream_md");
        assert_eq!(tagged["content"], "first");
    }

    #[test]
    fn small_cards_are_not_truncated() {
        let view = applied(&[text("short answer"), done(TerminationReason::Normal)]);
        let card = render(&view);
        assert_eq!(plain_markdown(&card), vec!["short answer"]);
        assert!(!serialized(&card).contains("卡片已截断"));
        assert_eq!(card["schema"], "2.0");
        assert_eq!(card["config"]["update_multi"], true);
        assert_eq!(card["config"]["streaming_mode"], false);
    }
    #[test]
    fn approval_card_binds_both_buttons_and_masks_emails() {
        let card = approval_card(&ApprovalCardInput {
            scope: "oc_1",
            operator: "ou_operator",
            run_id: 7,
            request_id: "req-1",
            tool_name: "Bash",
            context: Some("Claude Code · /work"),
            description: Some("run a command"),
            input_preview: "echo me@example.com",
            input_json: r#"{"command":"echo me@example.com"}"#,
            input_truncated: false,
            allow_action: "approval:allow:req-1:ou_operator:7",
            allow_token: "allow-token",
            deny_action: "approval:deny:req-1:ou_operator:7",
            deny_token: "deny-token",
            session_action: Some("approval:allow_session:req-1:ou_operator:7"),
            session_token: Some("session-token"),
            timeout_minutes: 10,
        });
        let text = serde_json::to_string(&card).unwrap();
        let buttons: Vec<&Value> = card["body"]["elements"]
            .as_array()
            .expect("elements")
            .iter()
            .filter(|element| element["tag"] == "button")
            .collect();
        assert_eq!(buttons.len(), 3, "{text}");
        assert_eq!(
            buttons[0]["behaviors"][0]["value"]["action"], "approval:allow:req-1:ou_operator:7",
            "{text}"
        );
        assert_eq!(
            buttons[1]["behaviors"][0]["value"]["action"],
            "approval:allow_session:req-1:ou_operator:7",
            "{text}"
        );
        assert_eq!(
            buttons[2]["behaviors"][0]["value"]["action"], "approval:deny:req-1:ou_operator:7",
            "{text}"
        );
        assert!(text.contains("allow-token"), "{text}");
        assert!(text.contains("deny-token"), "{text}");
        assert!(text.contains("\"decision\":\"allow\""), "{text}");
        assert!(text.contains("\"decision\":\"deny\""), "{text}");
        assert!(text.contains("ou_operator"), "{text}");
        assert!(!text.contains("me@example.com"), "{text}");
        assert!(text.contains("me[at]example.com"), "{text}");
        assert_eq!(
            card["body"]["elements"]
                .as_array()
                .unwrap()
                .iter()
                .filter(|element| element["tag"] == "button")
                .count(),
            3
        );

        let decided = approval_decided_card("Bash", "✅ 已允许");
        let decided_text = serde_json::to_string(&decided).unwrap();
        assert!(!decided_text.contains("__bridge_cb"), "{decided_text}");
        assert!(decided_text.contains("已允许"), "{decided_text}");
    }

    #[test]
    fn a_pending_approval_is_visible_in_the_progress_card_summary() {
        let mut view = RunView::default();
        view.apply(&AgentEvent::ToolUse {
            id: "t1".into(),
            name: "Bash".into(),
            input: json!({ "command": "pwd" }),
        });
        view.pending_approval = Some("Bash".into());
        let card = render_card(
            &view,
            &CardRenderOptions {
                stop_value: Some(json!({ "cmd": "stop" })),
                ..CardRenderOptions::default()
            },
        );
        let text = serde_json::to_string(&card).unwrap();
        assert!(text.contains("等待授权"), "{text}");
        assert_eq!(card["config"]["summary"]["content"], "等待授权");
    }
}
