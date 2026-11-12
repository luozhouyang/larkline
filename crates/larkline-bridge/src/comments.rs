//! Cloud-doc file-comment mentions (`drive.notice.comment_add_v1`).
//!
//! Port of `src/bot/comments.ts` plus the platform-event normalization the
//! TypeScript bridge got for free from `@larksuite/channel`.
//!
//! ## How the event reaches the Rust bridge
//!
//! The TypeScript SDK had a built-in handler for `drive.notice.comment_add_v1`
//! that normalized the platform payload into a `CommentEvent` and fanned it
//! out to the `comment` handler. The Rust SDK (`lark-channel` 0.6) models only
//! `Message`, `CardAction` and `Unknown`, so a comment event arrives as
//!
//! ```text
//! ChannelEvent::Unknown { context: Some(EventContext), raw: Value }
//! ```
//!
//! `raw` is the **whole** decrypted event envelope, not a stripped payload:
//!
//! ```json
//! {
//!   "schema": "2.0",
//!   "header": {
//!     "event_id": "…",
//!     "event_type": "drive.notice.comment_add_v1",
//!     "create_time": "169…",
//!     "tenant_key": "…"
//!   },
//!   "event": {
//!     "file_token": "doccn…",          // or notice_meta.file_token
//!     "file_type": "docx",            // or notice_meta.file_type
//!     "comment_id": "6…",
//!     "reply_id": "6…",
//!     "is_mentioned": true,           // or notice_meta.is_mentioned / is_mention
//!     "notice_meta": {
//!       "file_token": "doccn…",
//!       "file_type": "docx",
//!       "from_user_id": { "open_id": "ou_…", "user_id": "…", "union_id": "…" },
//!       "timestamp": "169…",
//!       "is_mentioned": true
//!     }
//!   }
//! }
//! ```
//!
//! Unlike the TypeScript SDK's dispatcher, `lark-channel` does not filter by
//! event type: every payload the server pushes is parsed and handed to the
//! handler, and the header survives. So the information the TypeScript bridge
//! used is all there — it is merely untyped. [`parse_comment_event`]
//! re-implements the SDK's `normalizeComment` on top of `raw`;
//! [`CommentEventParse`] describes the three outcomes. Nothing is faked: an
//! event that does not match the comment event type is reported as
//! [`CommentEventParse::NotAComment`], and a comment event missing a required
//! field as [`CommentEventParse::Incomplete`].
//!
//! The one operational prerequisite is the same as for the TypeScript bridge:
//! the Feishu app must subscribe to the 云文档评论 event
//! (`drive.notice.comment_add_v1`) in the developer console. The long
//! connection only pushes subscribed event types; `EventLoopOptions` and the
//! WebSocket endpoint request carry no event-type list of their own.
//!
//! ## Flow
//!
//! 1. [`parse_comment_event`] → [`RawCommentEvent`] (file token/type, comment
//!    id, reply id, `is_mentioned`, operator).
//! 2. [`comment_skip`] applies the same gates as the IM path: an @-mention is
//!    required, unsupported file types are skipped, the bot's own replies are
//!    ignored, and `access.allowed_users` (when non-empty) must contain the
//!    operator.
//! 3. `comments::fetch` pulls the thread and [`CommentMention`] keeps the
//!    question (the reply that @-ed the bot), the quoted selection, whether it
//!    is a whole-document comment, and the replies before it.
//! 4. The run takes a slot from the bridge's global run limiter (bounded wait)
//!    and a `<comment_context>` prompt section plus the plain-text comment
//!    prompt are handed to the shared agent runner.
//! 5. The answer is replied into the same comment thread through
//!    `larkline_lark::comments` (in-thread, or a fresh top-level comment for
//!    whole-document comments) — never into IM.
//!
//! ## Gaps vs. the TypeScript bridge
//!
//! * **Wiki nodes**: TS's `CommentSurface.resolveTarget` resolved a wiki token
//!   to the underlying `obj_token` before any comment call. `larkline-lark`
//!   has no wiki accessor, so `file_type: "wiki"` events are logged and
//!   skipped instead of being resolved (see [`SUPPORTED_FILE_TYPES`]).
//! * **`/stop`**: comment runs are detached from the event loop and are not
//!   registered in `Bridge::active`, so `/stop comment:<hash>` cannot abort
//!   one yet. The run is bounded by `RUN_TIMEOUT`.
//! * **Acknowledgement**: the CLI acknowledges the WebSocket frame immediately
//!   and runs the comment in a spawned task (a run may take up to
//!   `RUN_TIMEOUT`; holding the event loop for that long would stall
//!   heartbeats and every other event). Feishu can therefore not redeliver a
//!   comment while its run is in flight; `CommentRunGuard` still keeps a
//!   redelivery from starting a second concurrent run in the same thread.

use std::collections::HashSet;
use std::sync::{LazyLock, Mutex as StdMutex};
use std::time::Duration;

use larkline_agent::{AgentEvent, PromptContext, SandboxMode, TerminationReason};
use larkline_core::ProfileConfig;
use larkline_lark::comments::{self, Comment, CommentRef, CommentReply};
use larkline_lark::reactions::TYPING;
use larkline_lark::rest::RestClient;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};

use crate::agent_runner::{AgentProcess, AgentSpec};
use crate::render::RunView;
use crate::run::{Bridge, RUN_SLOT_TIMEOUT_REPLY, RUN_SLOT_WAIT, RUN_TIMEOUT};

/// Platform event type of a cloud-doc comment @-mention.
///
/// The TypeScript SDK registered this exact string as a built-in handler and
/// the Rust SDK delivers it as [`lark_channel::event::ChannelEvent::Unknown`].
pub const COMMENT_EVENT_TYPE: &str = "drive.notice.comment_add_v1";

/// File types `drive/v1/files/{file_token}/comments` supports (the same list
/// as the TypeScript bridge's `SUPPORTED_FILE_TYPES`). Slides, bitable,
/// mindnote and wiki are out of scope.
pub const SUPPORTED_FILE_TYPES: [&str; 4] = ["doc", "docx", "sheet", "file"];

/// Comment replies are plain text; cap them like the TypeScript bridge.
const REPLY_MAX_CHARS: usize = 2_000;

/// Comment runs in flight, keyed by comment thread scope.
///
/// The TypeScript bridge got this serialization from the SDK's per-scope
/// safety queue; the Rust SDK has no such queue for `Unknown` events, so a
/// redelivered comment would otherwise start a second agent on the same
/// thread.
static ACTIVE_COMMENT_RUNS: LazyLock<StdMutex<HashSet<String>>> =
    LazyLock::new(|| StdMutex::new(HashSet::new()));

// ---------------------------------------------------------------------------
// Event parsing
// ---------------------------------------------------------------------------

/// A `drive.notice.comment_add_v1` event after normalization.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RawCommentEvent {
    pub file_token: String,
    pub file_type: String,
    pub comment_id: String,
    /// The reply that @-mentioned the bot (absent for a top-level comment).
    pub reply_id: Option<String>,
    /// Feishu's authoritative "@-mentioned" flag.
    pub is_mentioned: bool,
    /// Open id of the user who wrote the comment / reply.
    pub operator_open_id: String,
    /// Event timestamp in milliseconds, when the payload carried one.
    pub timestamp: Option<i64>,
}

/// Outcome of looking at an untyped `ChannelEvent::Unknown` payload.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CommentEventParse {
    /// `header.event_type` is not [`COMMENT_EVENT_TYPE`].
    NotAComment,
    /// It is a comment event, but a required field is absent. `missing` names
    /// the field so the caller can log an actionable message.
    Incomplete { missing: &'static str },
    /// Fully parsed.
    Parsed(RawCommentEvent),
}

impl CommentEventParse {
    /// True for [`CommentEventParse::Parsed`] and
    /// [`CommentEventParse::Incomplete`] — i.e. the payload *is* a comment
    /// event of some shape.
    pub fn is_comment_event(&self) -> bool {
        !matches!(self, Self::NotAComment)
    }
}

/// Parse an untyped platform event into a comment event.
///
/// Mirrors the TypeScript SDK's `normalizeComment`: the flat `event.*` fields
/// win, `event.notice_meta.*` is the fallback, and the operator open id comes
/// from `notice_meta.from_user_id` (newer payloads) or `event.user_id`
/// (older ones).
pub fn parse_comment_event(raw: &Value) -> CommentEventParse {
    if raw.pointer("/header/event_type").and_then(Value::as_str) != Some(COMMENT_EVENT_TYPE) {
        return CommentEventParse::NotAComment;
    }
    let Some(event) = raw.get("event").filter(|event| event.is_object()) else {
        return CommentEventParse::Incomplete { missing: "event" };
    };
    let notice = event.get("notice_meta").unwrap_or(&Value::Null);

    let Some(file_token) = field(event, notice, "file_token") else {
        return CommentEventParse::Incomplete {
            missing: "file_token",
        };
    };
    let Some(file_type) = field(event, notice, "file_type") else {
        return CommentEventParse::Incomplete {
            missing: "file_type",
        };
    };
    let Some(comment_id) = field(event, notice, "comment_id") else {
        return CommentEventParse::Incomplete {
            missing: "comment_id",
        };
    };
    let Some(operator_open_id) = operator_open_id(event, notice) else {
        return CommentEventParse::Incomplete {
            missing: "operator_open_id",
        };
    };

    CommentEventParse::Parsed(RawCommentEvent {
        file_token,
        file_type,
        comment_id,
        reply_id: field(event, notice, "reply_id"),
        is_mentioned: bool_field(event, notice, "is_mentioned")
            || bool_field(event, notice, "is_mention"),
        operator_open_id,
        timestamp: timestamp(event, notice),
    })
}

/// Cheap check the CLI uses to route an `Unknown` event to the comment
/// handler. Accepts malformed comment events too, so they reach the handler
/// and are logged instead of silently disappearing.
pub fn is_file_comment_event(raw: &Value) -> bool {
    raw.pointer("/header/event_type").and_then(Value::as_str) == Some(COMMENT_EVENT_TYPE)
}

/// First non-empty string at `event[name]` or `notice_meta[name]`.
fn field(event: &Value, notice: &Value, name: &str) -> Option<String> {
    [event.get(name), notice.get(name)]
        .into_iter()
        .flatten()
        .filter_map(Value::as_str)
        .map(str::trim)
        .find(|value| !value.is_empty())
        .map(str::to_string)
}

fn bool_field(event: &Value, notice: &Value, name: &str) -> bool {
    [event.get(name), notice.get(name)]
        .into_iter()
        .flatten()
        .find_map(|value| {
            value
                .as_bool()
                .or_else(|| value.as_str().map(|s| s == "true"))
        })
        .unwrap_or(false)
}

fn operator_open_id(event: &Value, notice: &Value) -> Option<String> {
    let from_notice = notice
        .get("from_user_id")
        .and_then(|user| user.get("open_id"))
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|id| !id.is_empty())
        .map(str::to_string);
    from_notice.or_else(|| {
        event
            .get("user_id")
            .and_then(|user| user.get("open_id"))
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|id| !id.is_empty())
            .map(str::to_string)
    })
}

fn timestamp(event: &Value, notice: &Value) -> Option<i64> {
    ["create_time", "timestamp", "action_time"]
        .into_iter()
        .flat_map(|name| [event.get(name), notice.get(name)])
        .flatten()
        .find_map(|value| {
            value
                .as_i64()
                .or_else(|| value.as_str().and_then(|text| text.trim().parse().ok()))
        })
}

// ---------------------------------------------------------------------------
// Scope ids
// ---------------------------------------------------------------------------

/// First 16 hex chars of `sha256(token)` — the TypeScript bridge's
/// `commentTokenDigest`.
pub fn comment_token_digest(token: &str) -> String {
    Sha256::digest(token.as_bytes())
        .iter()
        .take(8)
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

/// Document scope used to look up the document cwd and resumable session.
///
/// Deliberately `doc:<digest>`, the key the TypeScript bridge wrote, so a
/// Rust bridge can resume sessions created by the TS bridge.
pub fn comment_document_scope_id(file_token: &str) -> String {
    format!("doc:{}", comment_token_digest(file_token))
}

/// Pre-digest document scope the TypeScript bridge also looked up.
pub fn legacy_comment_document_scope_id(file_token: &str) -> String {
    format!("doc:{file_token}")
}

/// One comment thread: `comment:<digest(file_token:comment_id)>`.
pub fn comment_scope_id(file_token: &str, comment_id: &str) -> String {
    format!(
        "comment:{}",
        comment_token_digest(&format!("{file_token}:{comment_id}"))
    )
}

// ---------------------------------------------------------------------------
// Gating
// ---------------------------------------------------------------------------

/// Why a comment event is ignored. The names mirror the TypeScript bridge's
/// `comment.skip` reasons.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CommentSkip {
    NotMentioned,
    UnsupportedFileType,
    SelfReply,
    NotAllowlisted,
}

impl CommentSkip {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::NotMentioned => "not-mentioned",
            Self::UnsupportedFileType => "unsupported-file-type",
            Self::SelfReply => "bridge-self-reply",
            Self::NotAllowlisted => "not-allowlisted",
        }
    }
}

/// Apply the same access rules the IM path applies in
/// [`crate::policy::allowed`]:
///
/// * a comment only triggers a run when the bot was @-mentioned;
/// * unsupported file types are ignored (no comment API access);
/// * the bot's own replies never trigger (a reply notification can arrive with
///   `is_mentioned` set on threaded replies);
/// * `access.allowed_users`, when configured, must contain the operator — the
///   empty default keeps comments document-scoped, exactly like chat access.
pub fn comment_skip(
    profile: &ProfileConfig,
    event: &RawCommentEvent,
    bot_open_id: Option<&str>,
) -> Option<CommentSkip> {
    if !event.is_mentioned {
        return Some(CommentSkip::NotMentioned);
    }
    if !SUPPORTED_FILE_TYPES.contains(&event.file_type.as_str()) {
        return Some(CommentSkip::UnsupportedFileType);
    }
    if bot_open_id == Some(event.operator_open_id.as_str()) {
        return Some(CommentSkip::SelfReply);
    }
    if let Some(access) = profile.access.as_ref() {
        if !access.allowed_users.is_empty()
            && !access
                .allowed_users
                .iter()
                .any(|id| id == &event.operator_open_id)
        {
            return Some(CommentSkip::NotAllowlisted);
        }
    }
    None
}

// ---------------------------------------------------------------------------
// The fetched thread
// ---------------------------------------------------------------------------

/// A comment mention enriched with the thread content: what the agent is
/// asked, what it is asked about, and whether the answer belongs in-thread or
/// as a top-level comment.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommentMention {
    pub file_token: String,
    pub file_type: String,
    pub comment_id: String,
    /// Whole-document comment (no thread to reply into).
    pub is_whole_document: bool,
    /// Text of the reply that @-mentioned the bot.
    pub question: String,
    /// Text the comment was anchored to, when it was an inline comment.
    pub quote: Option<String>,
    /// The reply the mention came from — the reaction anchor.
    pub target_reply_id: Option<String>,
    /// Replies before the mention, chronological.
    pub prior_replies: Vec<String>,
}

impl CommentMention {
    /// Fold a raw event and its fetched thread into a mention.
    pub fn from_thread(event: &RawCommentEvent, comment: &Comment) -> Self {
        let (question, target_reply_id) =
            extract_question(&comment.replies, event.reply_id.as_deref());
        let target_index = target_reply_id
            .as_deref()
            .and_then(|id| {
                comment
                    .replies
                    .iter()
                    .position(|reply| reply.reply_id.as_deref() == Some(id))
            })
            .unwrap_or_else(|| comment.replies.len().saturating_sub(1));
        let prior_replies = comment
            .replies
            .iter()
            .take(target_index)
            .map(CommentReply::text)
            .filter(|text| !text.is_empty())
            .collect();

        Self {
            file_token: event.file_token.clone(),
            file_type: event.file_type.clone(),
            comment_id: event.comment_id.clone(),
            is_whole_document: comment.is_whole,
            question,
            quote: comment.quote.clone().filter(|quote| !quote.is_empty()),
            target_reply_id,
            prior_replies,
        }
    }

    /// `drive/v1` comment reference for the fetch/reply helpers.
    pub fn reference(&self) -> CommentRef {
        CommentRef::new(
            self.file_token.clone(),
            self.file_type.clone(),
            self.comment_id.clone(),
        )
    }

    /// Document scope the session and cwd are keyed by.
    pub fn document_scope_id(&self) -> String {
        comment_document_scope_id(&self.file_token)
    }
}

/// Question extraction: the reply matching `reply_id`, else the last reply.
pub fn extract_question(
    replies: &[CommentReply],
    reply_id: Option<&str>,
) -> (String, Option<String>) {
    let target = reply_id
        .and_then(|id| {
            replies
                .iter()
                .find(|reply| reply.reply_id.as_deref() == Some(id))
        })
        .or_else(|| replies.last());
    match target {
        Some(reply) => (reply.text(), reply.reply_id.clone()),
        None => (String::new(), None),
    }
}

// ---------------------------------------------------------------------------
// Prompt
// ---------------------------------------------------------------------------

/// The `<comment_context>` prompt section (TypeScript `BridgePromptComment`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CommentPromptSection {
    pub comment_scope_id: String,
    pub is_whole_document: bool,
    pub docs_link: String,
    pub question: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub quote: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub prior_replies: Vec<String>,
}

impl CommentPromptSection {
    pub fn new(mention: &CommentMention, comment_scope_id: impl Into<String>) -> Self {
        Self {
            comment_scope_id: comment_scope_id.into(),
            is_whole_document: mention.is_whole_document,
            docs_link: docs_link(&mention.file_type, &mention.file_token),
            question: mention.question.clone(),
            quote: mention.quote.clone(),
            prior_replies: mention.prior_replies.clone(),
        }
    }
}

fn docs_link(file_type: &str, file_token: &str) -> String {
    format!("https://feishu.cn/{file_type}/{file_token}")
}

/// Plain-text prompt body: the TypeScript `buildCommentPrompt` port.
///
/// Feishu comment boxes render markdown literally, so the agent is told to
/// answer in plain text and to leave the comment reply to the bridge.
pub fn build_comment_prompt(mention: &CommentMention) -> String {
    let mut parts: Vec<String> = Vec::new();
    parts.push("我在飞书云文档里被 @了。文档信息：".to_string());
    parts.push(format!(
        "- 链接：{}",
        docs_link(&mention.file_type, &mention.file_token)
    ));
    parts.push(format!("- file_token：{}", mention.file_token));
    parts.push(format!("- 类型：{}", mention.file_type));
    parts.push(format!(
        "- 评论范围：{}",
        if mention.is_whole_document {
            "全文评论（针对整篇）"
        } else {
            "行内评论（针对选中文字）"
        }
    ));
    if let Some(quote) = mention.quote.as_deref() {
        parts.push(String::new());
        parts.push(format!(
            "用户选中的原文：\n> {}",
            quote.replace('\n', "\n> ")
        ));
    }
    if !mention.prior_replies.is_empty() {
        parts.push(String::new());
        parts.push("这条评论 thread 里此前的讨论（按时间顺序，@你的那条不在其中）：".to_string());
        for (index, text) in mention.prior_replies.iter().enumerate() {
            parts.push(format!("{}. {text}", index + 1));
        }
    }
    parts.push(String::new());
    parts.push(format!("用户的问题：{}", mention.question));
    parts.push(String::new());
    parts.push(comment_read_instruction(
        &mention.file_type,
        &mention.file_token,
    ));
    parts.push(String::new());
    parts.push(
        "评论回复由 bridge 负责：不要调用云文档评论或回复接口，也不要给评论添加或删除 reaction；最终答案直接用纯文本交给 bridge。"
            .to_string(),
    );
    parts.push(String::new());
    parts.push(
        "回复要求：直接用纯文本，不要 markdown（不要 ** __ # - * > ` 之类的标记），不要代码块；不要输出内部思考、内部分析、读取步骤、工具调用过程或工具日志。若用户要求解释依据，只说明用户可见的依据和结论。云文档评论框不渲染 markdown，会原样显示这些符号。"
            .to_string(),
    );
    parts.join("\n")
}

fn comment_read_instruction(file_type: &str, file_token: &str) -> String {
    match file_type {
        "doc" | "docx" => format!(
            "读取文档内容：优先使用当前 docs v2 读取命令：\n  `lark-cli docs +fetch --api-version v2 --doc {file_token} --doc-format markdown`\n如果本机 lark-cli 不支持上述参数，不要在同一错误上反复重试；使用当前可用的等价读取命令读取同一 file_token。"
        ),
        "sheet" => "读取表格内容：这是 sheet 类型，不要使用 docs +fetch。请按当前可用的表格读取工具或本机 lark-cli 支持的表格读取命令读取同一 file_token；如果命令参数不兼容，不要在同一错误上反复重试。".to_string(),
        _ => "读取文件内容：这是 file 类型，不要使用 docs +fetch。请按当前可用的云空间文件工具或本机 lark-cli 支持的文件读取/下载命令处理同一 file_token；如果命令参数不兼容，不要在同一错误上反复重试。".to_string(),
    }
}

/// Assemble `<bridge_context>`, `<comment_context>` and `<user_input>`.
///
/// The section layout matches `buildAgentPrompt` in `src/agent/prompt.ts`,
/// which already had a `comment_context` slot; the `<user_input>` body is the
/// port of `buildCommentPrompt`, so an agent sees the same wording on both
/// bridges.
pub fn build_comment_agent_prompt(
    context: &PromptContext,
    section: &CommentPromptSection,
    user_input: &str,
) -> String {
    let user = json!({ "text": user_input });
    [
        prompt_section("bridge_context", context),
        prompt_section("comment_context", section),
        prompt_section("user_input", &user),
    ]
    .join("\n\n")
}

fn prompt_section<T: Serialize>(tag: &str, value: &T) -> String {
    format!("<{tag}>\n{}\n</{tag}>", safe_json(value))
}

/// JSON with `<`/`>`/`&` escaped so injected text cannot close a section tag.
fn safe_json<T: Serialize>(value: &T) -> String {
    serde_json::to_string(value)
        .unwrap_or_else(|_| "null".to_string())
        .replace('<', "\\u003c")
        .replace('>', "\\u003e")
        .replace('&', "\\u0026")
        .replace('\u{2028}', "\\u2028")
        .replace('\u{2029}', "\\u2029")
}

/// Prompt context for a comment run: `source: "comment"`, the thread scope as
/// the resource id, and the commenting user as the sender.
pub fn comment_prompt_context(
    operator_open_id: &str,
    thread_scope_id: &str,
    bot_open_id: Option<&str>,
) -> PromptContext {
    PromptContext {
        chat_id: thread_scope_id.to_string(),
        // Comments have no chat type; `comment` makes the source obvious to
        // the agent instead of pretending it is a p2p chat.
        chat_type: "comment".to_string(),
        sender_id: operator_open_id.to_string(),
        sender_name: None,
        sender_type: Some("user".to_string()),
        bot_open_id: bot_open_id.map(str::to_string),
        mentions: None,
        thread_id: None,
        message_ids: None,
        source: "comment".to_string(),
    }
}

// ---------------------------------------------------------------------------
// Reply formatting
// ---------------------------------------------------------------------------

/// Turn the run view into the comment reply body (TypeScript parity:
/// `stripMarkdown`, error prefix, empty fallback, 2000-char cap).
pub fn comment_reply_text(view: &RunView, agent_label: &str) -> String {
    // The TypeScript bridge answers a timeout with a retry hint instead of the
    // raw "run timed out" error, which is more useful in the comment thread.
    if view.termination == Some(TerminationReason::Timeout) {
        return "本次评论任务已超时，请重新 @ 我。".to_string();
    }
    let mut text = match view.error.as_deref() {
        Some(error) => format!("⚠️ {agent_label} 报错：{error}"),
        None => strip_markdown(view.best_answer().trim()),
    };
    if text.trim().is_empty() {
        text = "（无回复内容）".to_string();
    }
    if text.chars().count() > REPLY_MAX_CHARS {
        let mut capped: String = text.chars().take(REPLY_MAX_CHARS - 1).collect();
        capped.push('…');
        return capped;
    }
    text
}

/// Drop the markdown markers a comment box would show literally.
///
/// Port of the TypeScript `stripMarkdown`, without a regex dependency:
/// headings, list bullets, blockquotes and fences are handled per line, then
/// inline code/emphasis markers are removed when they come in pairs. Files and
/// identifiers (`file_token`, `comment_id`) survive because an `_`/`*`
/// surrounded by word characters is not a pair boundary.
pub fn strip_markdown(text: &str) -> String {
    let mut lines: Vec<String> = Vec::new();
    for line in text.lines() {
        if line.trim_start().starts_with("```") {
            // Fence lines carry no content of their own.
            continue;
        }
        lines.push(strip_line_prefix(line).to_string());
    }
    let joined = lines.join("\n");
    let without_code = joined.replace('`', "");
    let without_bold = strip_emphasis(&without_code, '*', 2);
    let without_bold = strip_emphasis(&without_bold, '_', 2);
    let without_italic = strip_emphasis(&without_bold, '*', 1);
    strip_emphasis(&without_italic, '_', 1)
}

/// `# `, `- ` / `* ` and `> ` prefixes, exactly like the TypeScript regexes
/// (which only match at the start of a line).
fn strip_line_prefix(line: &str) -> &str {
    let hashes = line.chars().take_while(|c| *c == '#').count();
    if (1..=6).contains(&hashes) {
        let rest = &line[hashes..];
        if rest.starts_with(char::is_whitespace) {
            return rest.trim_start();
        }
    }
    if let Some(rest) = line.strip_prefix("- ").or_else(|| line.strip_prefix("* ")) {
        return rest;
    }
    if let Some(rest) = line.strip_prefix('>') {
        return rest.strip_prefix(' ').unwrap_or(rest);
    }
    line
}

/// Remove `marker`-delimited runs of at least `len` characters.
///
/// A run is only an opening marker when it is not preceded by a word
/// character, and the closing run must not be followed by one, which keeps
/// `file_token` and `2 * 3` intact.
fn strip_emphasis(text: &str, marker: char, len: usize) -> String {
    let chars: Vec<char> = text.chars().collect();
    let mut out = String::with_capacity(text.len());
    let mut index = 0;
    while index < chars.len() {
        if chars[index] != marker {
            out.push(chars[index]);
            index += 1;
            continue;
        }
        let run = run_length(&chars, index, marker);
        let preceded_by_word = index > 0 && is_word_char(chars[index - 1]);
        let mut close = index + run;
        let mut matched = None;
        while !preceded_by_word && close < chars.len() {
            if chars[close] != marker {
                close += 1;
                continue;
            }
            let close_run = run_length(&chars, close, marker);
            let followed_by_word =
                close + close_run < chars.len() && is_word_char(chars[close + close_run]);
            // The content must be non-empty and must not start or end with
            // whitespace: `2 * 3 * 4` is arithmetic, not emphasis.
            if close_run >= len
                && !followed_by_word
                && close > index + run
                && !chars[index + run].is_whitespace()
                && !chars[close - 1].is_whitespace()
            {
                matched = Some((close, close_run));
                break;
            }
            close += close_run;
        }
        match matched {
            Some((close, close_run)) => {
                out.extend(chars[index + run..close].iter());
                index = close + close_run;
            }
            None => {
                out.extend(std::iter::repeat_n(marker, run));
                index += run;
            }
        }
    }
    out
}

fn run_length(chars: &[char], start: usize, marker: char) -> usize {
    chars[start..].iter().take_while(|c| **c == marker).count()
}

fn is_word_char(character: char) -> bool {
    character.is_ascii_alphanumeric() || character == '_'
}

// ---------------------------------------------------------------------------
// Run flow
// ---------------------------------------------------------------------------

/// One-run-per-thread guard; see [`ACTIVE_COMMENT_RUNS`].
struct CommentRunGuard(String);

impl CommentRunGuard {
    fn acquire(thread_scope: &str) -> Option<Self> {
        let mut active = ACTIVE_COMMENT_RUNS
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        active
            .insert(thread_scope.to_string())
            .then(|| Self(thread_scope.to_string()))
    }
}

impl Drop for CommentRunGuard {
    fn drop(&mut self) {
        let mut active = ACTIVE_COMMENT_RUNS
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        active.remove(&self.0);
    }
}

/// Best-effort "Typing" reaction on the reply that @-mentioned the bot.
struct CommentTypingReaction {
    rest: RestClient,
    file_token: String,
    file_type: String,
    reply_id: Option<String>,
    added: bool,
}

impl CommentTypingReaction {
    async fn start(rest: RestClient, mention: &CommentMention) -> Self {
        let reply_id = mention.target_reply_id.clone();
        let added = match reply_id.as_deref() {
            Some(reply_id) => match comments::add_reaction(
                &rest,
                &mention.file_token,
                &mention.file_type,
                reply_id,
                TYPING,
            )
            .await
            {
                Ok(()) => true,
                Err(error) => {
                    tracing::debug!(error = %error, "cannot add comment reaction");
                    false
                }
            },
            None => false,
        };
        Self {
            rest,
            file_token: mention.file_token.clone(),
            file_type: mention.file_type.clone(),
            reply_id,
            added,
        }
    }

    async fn finish(self) {
        let (Some(reply_id), true) = (self.reply_id.as_deref(), self.added) else {
            return;
        };
        if let Err(error) = comments::remove_reaction(
            &self.rest,
            &self.file_token,
            &self.file_type,
            reply_id,
            TYPING,
        )
        .await
        {
            tracing::debug!(error = %error, "cannot remove comment reaction");
        }
    }
}

/// Handle one untyped event: parse, gate, fetch the thread, run the agent and
/// reply into the comment thread. Every failure is logged and swallowed — a
/// bad comment event must never take the bridge down.
pub async fn handle_comment_mention(bridge: &std::sync::Arc<Bridge>, raw: &Value) {
    let event = match parse_comment_event(raw) {
        CommentEventParse::Parsed(event) => event,
        CommentEventParse::NotAComment => return,
        CommentEventParse::Incomplete { missing } => {
            tracing::warn!(
                missing,
                "ignoring incomplete file-comment event (payload shape changed?)"
            );
            return;
        }
    };
    let thread_scope = comment_scope_id(&event.file_token, &event.comment_id);
    tracing::info!(
        document = %comment_document_scope_id(&event.file_token),
        thread = %thread_scope,
        file_type = %event.file_type,
        reply_id = event.reply_id.as_deref().unwrap_or_default(),
        mentioned = event.is_mentioned,
        operator = %event.operator_open_id,
        "inbound file-comment event"
    );

    if let Some(reason) = comment_skip(&bridge.profile, &event, bridge.bot_open_id.as_deref()) {
        tracing::info!(
            reason = reason.as_str(),
            thread = %thread_scope,
            "skipping file-comment event"
        );
        return;
    }

    let Some(_run_guard) = CommentRunGuard::acquire(&thread_scope) else {
        tracing::info!(
            thread = %thread_scope,
            "comment thread already has a live run; skipping redelivery"
        );
        return;
    };

    let Some(mention) = fetch_mention(bridge, &event, &thread_scope).await else {
        return;
    };
    tracing::info!(
        thread = %thread_scope,
        whole_document = mention.is_whole_document,
        has_quote = mention.quote.is_some(),
        prior_replies = mention.prior_replies.len(),
        question = %preview(&mention.question),
        "file-comment mention parsed"
    );

    let reaction = CommentTypingReaction::start(bridge.channel.rest().clone(), &mention).await;
    let view = run_comment_agent(bridge, &event, &mention, &thread_scope).await;
    let text = comment_reply_text(&view, bridge.profile.agent_kind.display_name());
    post_comment_reply(bridge, &mention, &text).await;
    reaction.finish().await;
}

/// Fetch the comment thread and turn it into a [`CommentMention`].
async fn fetch_mention(
    bridge: &Bridge,
    event: &RawCommentEvent,
    thread_scope: &str,
) -> Option<CommentMention> {
    let reference = CommentRef::new(
        event.file_token.clone(),
        event.file_type.clone(),
        event.comment_id.clone(),
    );
    let comment = match comments::fetch(bridge.channel.rest(), &reference).await {
        Ok(Some(comment)) => comment,
        Ok(None) => {
            tracing::info!(
                thread = %thread_scope,
                "comment not found through get or list; skipping"
            );
            return None;
        }
        Err(error) => {
            // 1069307 = no read access to the document.
            tracing::warn!(
                thread = %thread_scope,
                error = %error,
                "cannot fetch file comment; skipping"
            );
            return None;
        }
    };
    let mention = CommentMention::from_thread(event, &comment);
    if mention.question.is_empty() {
        tracing::info!(thread = %thread_scope, "comment has no question text; skipping");
        return None;
    }
    Some(mention)
}

/// Run the agent for one comment. Mirrors the IM `run_batch` lifetime, minus
/// the card: a comment has no streaming surface, so only the final answer is
/// used.
async fn run_comment_agent(
    bridge: &Bridge,
    event: &RawCommentEvent,
    mention: &CommentMention,
    thread_scope: &str,
) -> RunView {
    run_comment_agent_with_wait(bridge, event, mention, thread_scope, RUN_SLOT_WAIT).await
}

/// [`run_comment_agent`] with an injectable slot-wait budget (tests use a
/// short one to exercise the timeout without sleeping for 30s).
async fn run_comment_agent_with_wait(
    bridge: &Bridge,
    event: &RawCommentEvent,
    mention: &CommentMention,
    thread_scope: &str,
    slot_wait: Duration,
) -> RunView {
    // Comment runs have no pending queue to park in, so they wait a bounded
    // time for a global run slot instead of bypassing the cap. The permit is
    // held until this function returns (normal completion, spawn error, or a
    // dropped/aborted run); dropping it releases the slot for the queue
    // driver and other bounded waiters.
    let _permit = match bridge.run_limiter().acquire_within(slot_wait).await {
        Some(permit) => permit,
        None => {
            tracing::warn!(
                thread = %thread_scope,
                timeout_ms = slot_wait.as_millis(),
                "comment run timed out waiting for a global run slot"
            );
            let mut view = RunView::default();
            view.apply(&AgentEvent::Error {
                message: RUN_SLOT_TIMEOUT_REPLY.to_string(),
                termination_reason: TerminationReason::Failed,
            });
            return view;
        }
    };
    let document_scope = comment_document_scope_id(&mention.file_token);
    let legacy_scope = legacy_comment_document_scope_id(&mention.file_token);
    // Same cwd rules as the IM path, keyed by the document scope so a
    // document's runs stay in one directory.
    let cwd = bridge.workspace_cwd(&document_scope).await;
    let cwd_str = cwd.to_string_lossy().into_owned();
    let resume = {
        let sessions = bridge.sessions.lock().await;
        sessions
            .resume_for(&document_scope, &cwd_str)
            .or_else(|| sessions.resume_for(&legacy_scope, &cwd_str))
            .map(str::to_string)
    };

    let context = comment_prompt_context(
        &event.operator_open_id,
        thread_scope,
        bridge.bot_open_id.as_deref(),
    );
    let section = CommentPromptSection::new(mention, thread_scope);
    let prompt = build_comment_agent_prompt(&context, &section, &build_comment_prompt(mention));

    let spec = AgentSpec {
        kind: bridge.profile.agent_kind,
        binary: bridge.agent_binary.clone(),
        cwd,
        prompt,
        session_id: resume.clone(),
        model: None,
        sandbox: SandboxMode::DangerFullAccess,
        bot_open_id: bridge.bot_open_id.clone(),
        bot_name: bridge.bot_name.clone(),
    };
    tracing::info!(
        thread = %thread_scope,
        document = %document_scope,
        resume = resume.is_some(),
        cwd = %cwd_str,
        "starting comment run"
    );

    let mut view = RunView::default();
    let mut process = match AgentProcess::spawn(spec).await {
        Ok(process) => process,
        Err(error) => {
            tracing::warn!(error = %error, "cannot spawn agent for comment run");
            view.apply(&AgentEvent::Error {
                message: error.to_string(),
                termination_reason: TerminationReason::Failed,
            });
            return view;
        }
    };

    let deadline = tokio::time::Instant::now() + RUN_TIMEOUT;
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
            }
        }
    }

    if let Some(key) = view.session_key() {
        let key = key.to_string();
        if let Err(error) = bridge
            .sessions
            .lock()
            .await
            .set(&document_scope, &key, &cwd_str)
        {
            tracing::warn!(error = %error, "cannot persist comment session");
        }
    }
    view
}

/// Reply into the comment thread, never into IM.
///
/// Whole-document comments have no thread (`fileCommentReply.create` answers
/// 1069302), so they go straight to a fresh top-level comment; that is the
/// TypeScript bridge's `topLevel: true` fast path. In-thread replies still use
/// `comments::reply`, which falls back to top-level on 1069302.
async fn post_comment_reply(bridge: &Bridge, mention: &CommentMention, text: &str) {
    let rest = bridge.channel.rest();
    let result = if mention.is_whole_document {
        comments::reply_top_level(rest, &mention.file_token, &mention.file_type, text).await
    } else {
        comments::reply(rest, &mention.reference(), text).await
    };
    match result {
        Ok(()) => tracing::info!(
            comment = %mention.comment_id,
            whole_document = mention.is_whole_document,
            chars = text.chars().count(),
            "replied to file comment"
        ),
        Err(error) => tracing::warn!(
            comment = %mention.comment_id,
            whole_document = mention.is_whole_document,
            error = %error,
            "cannot reply to file comment"
        ),
    }
}

fn preview(text: &str) -> String {
    if text.chars().count() > 80 {
        let head: String = text.chars().take(80).collect();
        format!("{head}…")
    } else {
        text.to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::{Path, PathBuf};
    use std::sync::Arc;

    use larkline_core::config::{Access, AgentKind};
    use larkline_core::{AppPaths, TenantBrand};

    fn profile() -> ProfileConfig {
        ProfileConfig {
            agent_kind: AgentKind::Claude,
            ..ProfileConfig::default()
        }
    }

    fn event(value: Value) -> CommentEventParse {
        parse_comment_event(&value)
    }

    fn mention(value: Value) -> CommentMention {
        let CommentEventParse::Parsed(event) = event(value) else {
            panic!("expected a parsed comment event");
        };
        let comment: Comment = serde_json::from_value(json!({
            "comment_id": "comment_1",
            "quote": "selected text",
            "is_whole": false,
            "replies": [
                { "reply_id": "reply_1", "content": { "elements": [
                    { "type": "text_run", "text_run": { "text": "context" } }
                ] } },
                { "reply_id": "reply_2", "content": { "elements": [
                    { "type": "text_run", "text_run": { "text": "explain this" } }
                ] } }
            ]
        }))
        .expect("comment fixture");
        CommentMention::from_thread(&event, &comment)
    }

    fn flat_event_body() -> Value {
        json!({
            "header": {
                "event_id": "event_1",
                "event_type": COMMENT_EVENT_TYPE,
                "create_time": "1690000000000",
                "tenant_key": "tenant_1"
            },
            "event": {
                "file_token": "doccn_token",
                "file_type": "docx",
                "comment_id": "comment_1",
                "reply_id": "reply_2",
                "is_mentioned": true,
                "user_id": { "open_id": "ou_author" }
            }
        })
    }

    #[test]
    fn parses_the_flat_event_shape() {
        let CommentEventParse::Parsed(event) = event(flat_event_body()) else {
            panic!("expected a parsed comment event");
        };
        assert_eq!(event.file_token, "doccn_token");
        assert_eq!(event.file_type, "docx");
        assert_eq!(event.comment_id, "comment_1");
        assert_eq!(event.reply_id.as_deref(), Some("reply_2"));
        assert!(event.is_mentioned);
        assert_eq!(event.operator_open_id, "ou_author");
        // `create_time` lives on the *header*, which the TypeScript normalizer
        // also ignored; only a body `create_time`/`notice_meta.timestamp`
        // counts.
        assert_eq!(event.timestamp, None);
    }

    #[test]
    fn reads_the_timestamp_from_the_event_body() {
        let mut value = flat_event_body();
        value["event"]
            .as_object_mut()
            .unwrap()
            .insert("create_time".into(), json!("1690000000001"));
        let CommentEventParse::Parsed(event) = event(value) else {
            panic!("expected a parsed comment event");
        };
        assert_eq!(event.timestamp, Some(1_690_000_000_001));
    }

    #[test]
    fn parses_the_notice_meta_event_shape() {
        // Newer payloads put everything under `notice_meta` and omit the flat
        // fields; the TypeScript SDK accepted both.
        let CommentEventParse::Parsed(event) = event(json!({
            "header": { "event_id": "e", "event_type": COMMENT_EVENT_TYPE },
            "event": {
                "notice_meta": {
                    "file_token": "doccn_meta",
                    "file_type": "sheet",
                    "from_user_id": { "open_id": "ou_meta", "user_id": "u_1" },
                    "timestamp": "1690000000123",
                    "is_mentioned": true
                },
                "comment_id": "comment_meta",
                "reply_id": "reply_meta"
            }
        })) else {
            panic!("expected a parsed comment event");
        };
        assert_eq!(event.file_token, "doccn_meta");
        assert_eq!(event.file_type, "sheet");
        assert_eq!(event.operator_open_id, "ou_meta");
        assert_eq!(event.timestamp, Some(1_690_000_000_123));
        assert!(event.is_mentioned);
    }

    #[test]
    fn ignores_events_that_are_not_comments() {
        let parsed = event(json!({
            "header": { "event_id": "e", "event_type": "im.message.receive_v1" },
            "event": {}
        }));
        assert_eq!(parsed, CommentEventParse::NotAComment);
        assert!(!parsed.is_comment_event());
        assert_eq!(
            event(json!({ "event": {} })),
            CommentEventParse::NotAComment,
            "an event without a header cannot be a comment event"
        );
        assert!(!is_file_comment_event(&json!({})));
        assert!(is_file_comment_event(&flat_event_body()));
    }

    #[test]
    fn reports_the_missing_required_field() {
        let mut value = flat_event_body();
        value["event"].as_object_mut().unwrap().remove("comment_id");
        assert_eq!(
            event(value),
            CommentEventParse::Incomplete {
                missing: "comment_id"
            }
        );

        let mut value = flat_event_body();
        value["event"].as_object_mut().unwrap().remove("user_id");
        assert_eq!(
            event(value),
            CommentEventParse::Incomplete {
                missing: "operator_open_id"
            }
        );

        assert_eq!(
            event(json!({
                "header": { "event_id": "e", "event_type": COMMENT_EVENT_TYPE }
            })),
            CommentEventParse::Incomplete { missing: "event" }
        );
    }

    #[test]
    fn mention_without_the_flag_is_not_mentioned() {
        let mut value = flat_event_body();
        value["event"]
            .as_object_mut()
            .unwrap()
            .remove("is_mentioned");
        let CommentEventParse::Parsed(event) = event(value) else {
            panic!("expected a parsed comment event");
        };
        assert!(!event.is_mentioned);
    }

    #[test]
    fn parses_the_legacy_is_mention_alias() {
        let mut value = flat_event_body();
        let body = value["event"].as_object_mut().unwrap();
        body.remove("is_mentioned");
        body.insert("is_mention".into(), json!("true"));
        let CommentEventParse::Parsed(event) = event(value) else {
            panic!("expected a parsed comment event");
        };
        assert!(event.is_mentioned);
    }

    #[test]
    fn comment_scope_ids_match_the_typescript_scheme() {
        // The TypeScript bridge took the first 16 hex chars of sha256(scope).
        assert_eq!(comment_token_digest("doccn_token"), "f48b2d1aa2f2683a");
        assert_eq!(
            comment_token_digest("doccn_token:comment_1"),
            "65e7ae180aaf7336"
        );
        assert_eq!(
            comment_document_scope_id("doccn_token"),
            "doc:f48b2d1aa2f2683a"
        );
        assert_eq!(
            comment_scope_id("doccn_token", "comment_1"),
            "comment:65e7ae180aaf7336"
        );
        assert_eq!(
            legacy_comment_document_scope_id("doccn_token"),
            "doc:doccn_token"
        );
        // Thread digests differ per comment id and per document.
        assert_ne!(
            comment_scope_id("doccn_token", "comment_1"),
            comment_scope_id("doccn_token", "comment_2")
        );
        assert_ne!(
            comment_scope_id("a", "comment_1"),
            comment_scope_id("b", "comment_1")
        );
    }

    #[test]
    fn gates_follow_the_message_access_rules() {
        let CommentEventParse::Parsed(event) = event(flat_event_body()) else {
            panic!("expected a parsed comment event");
        };
        assert_eq!(comment_skip(&profile(), &event, Some("ou_bot")), None);

        let mut not_mentioned = event.clone();
        not_mentioned.is_mentioned = false;
        assert_eq!(
            comment_skip(&profile(), &not_mentioned, Some("ou_bot")),
            Some(CommentSkip::NotMentioned)
        );

        let mut wiki = event.clone();
        wiki.file_type = "wiki".to_string();
        assert_eq!(
            comment_skip(&profile(), &wiki, Some("ou_bot")),
            Some(CommentSkip::UnsupportedFileType)
        );

        let mut self_reply = event.clone();
        self_reply.operator_open_id = "ou_bot".to_string();
        assert_eq!(
            comment_skip(&profile(), &self_reply, Some("ou_bot")),
            Some(CommentSkip::SelfReply)
        );

        let mut allowlisted = profile();
        allowlisted.access = Some(Access {
            allowed_users: vec!["ou_owner".into()],
            ..Access::default()
        });
        assert_eq!(
            comment_skip(&allowlisted, &event, Some("ou_bot")),
            Some(CommentSkip::NotAllowlisted)
        );
        let mut allowed = event.clone();
        allowed.operator_open_id = "ou_owner".to_string();
        assert_eq!(comment_skip(&allowlisted, &allowed, Some("ou_bot")), None);
    }

    #[test]
    fn mention_keeps_question_quote_and_prior_replies() {
        let mention = mention(flat_event_body());
        assert_eq!(mention.file_token, "doccn_token");
        assert_eq!(mention.file_type, "docx");
        assert_eq!(mention.comment_id, "comment_1");
        assert!(!mention.is_whole_document);
        assert_eq!(mention.question, "explain this");
        assert_eq!(mention.quote.as_deref(), Some("selected text"));
        assert_eq!(mention.target_reply_id.as_deref(), Some("reply_2"));
        assert_eq!(mention.prior_replies, vec!["context".to_string()]);
        assert_eq!(mention.document_scope_id(), "doc:f48b2d1aa2f2683a");
        let _ = mention.reference();
    }

    #[test]
    fn falls_back_to_the_last_reply_without_a_reply_id() {
        let empty_reply = CommentReply::default();
        let replies = std::slice::from_ref(&empty_reply);
        assert_eq!(extract_question(replies, None), (String::new(), None));

        let comment: Comment = serde_json::from_value(json!({
            "is_whole": true,
            "replies": [
                { "reply_id": "a", "content": { "elements": [
                    { "type": "text_run", "text_run": { "text": "first" } } ] } },
                { "reply_id": "b", "content": { "elements": [
                    { "type": "text_run", "text_run": { "text": "last" } } ] } }
            ]
        }))
        .unwrap();
        let CommentEventParse::Parsed(event) = event(flat_event_body()) else {
            panic!("expected a parsed comment event");
        };
        let mut event = event;
        event.reply_id = None;
        let mention = CommentMention::from_thread(&event, &comment);
        assert_eq!(mention.question, "last");
        assert_eq!(mention.target_reply_id.as_deref(), Some("b"));
        assert!(mention.is_whole_document);
    }

    #[test]
    fn builds_the_plain_text_comment_prompt() {
        let prompt = build_comment_prompt(&mention(flat_event_body()));
        assert!(prompt.contains("https://feishu.cn/docx/doccn_token"));
        assert!(prompt.contains("- file_token：doccn_token"));
        assert!(prompt.contains("- 类型：docx"));
        assert!(prompt.contains("行内评论（针对选中文字）"));
        assert!(prompt.contains("> selected text"));
        assert!(prompt.contains("1. context"));
        assert!(prompt.contains("用户的问题：explain this"));
        assert!(
            prompt.contains("lark-cli docs +fetch --api-version v2 --doc doccn_token"),
            "the read instruction must carry the real file token: {prompt}"
        );
        assert!(prompt.contains("直接用纯文本"));

        let whole = CommentMention {
            is_whole_document: true,
            ..mention(flat_event_body())
        };
        assert!(build_comment_prompt(&whole).contains("全文评论（针对整篇）"));
    }

    #[test]
    fn comment_section_is_camel_case_and_escapes_the_question() {
        let mut mention = mention(flat_event_body());
        mention.question = "a <b> & c".to_string();
        let section = CommentPromptSection::new(&mention, "comment:abc");
        let value = serde_json::to_value(&section).unwrap();
        assert_eq!(value["commentScopeId"], "comment:abc");
        assert_eq!(value["isWholeDocument"], false);
        assert_eq!(value["docsLink"], "https://feishu.cn/docx/doccn_token");
        assert_eq!(value["question"], "a <b> & c");
        assert_eq!(value["quote"], "selected text");
        assert_eq!(value["priorReplies"], json!(["context"]));

        let prompt = build_comment_agent_prompt(
            &comment_prompt_context("ou_author", "comment:abc", Some("ou_bot")),
            &section,
            "body",
        );
        assert!(!prompt.contains("a <b> & c"));
        assert!(prompt.contains("\\u003cb\\u003e"));
        assert!(prompt.contains("\\u0026"));
    }

    #[test]
    fn prompt_has_three_sections_and_comment_source() {
        let mention = mention(flat_event_body());
        let context = comment_prompt_context("ou_author", "comment:abc", Some("ou_bot"));
        let prompt = build_comment_agent_prompt(
            &context,
            &CommentPromptSection::new(&mention, "comment:abc"),
            &build_comment_prompt(&mention),
        );
        let bridge = prompt.find("<bridge_context>").unwrap();
        let comment = prompt.find("<comment_context>").unwrap();
        let input = prompt.find("<user_input>").unwrap();
        assert!(bridge < comment && comment < input);
        assert!(prompt.contains("\"source\":\"comment\""));
        assert!(prompt.contains("\"chatType\":\"comment\""));
        assert!(prompt.contains("\"senderId\":\"ou_author\""));
        assert!(prompt.contains("\"botOpenId\":\"ou_bot\""));
        assert_eq!(prompt.matches("<comment_context>").count(), 1);
    }

    #[test]
    fn reply_text_strips_markdown_and_caps_at_2000_chars() {
        let mut view = RunView::default();
        view.apply(&AgentEvent::FinalText {
            content: "## 标题\n**粗体** 和 _斜体_ 以及 `代码`\n- 列表\n> 引用\n```rust\nfn main() {}\n```"
                .into(),
        });
        assert_eq!(
            comment_reply_text(&view, "Claude Code"),
            "标题\n粗体 和 斜体 以及 代码\n列表\n引用\nfn main() {}"
        );

        let mut view = RunView::default();
        view.apply(&AgentEvent::FinalText {
            content: "x".repeat(2_500),
        });
        let text = comment_reply_text(&view, "Claude Code");
        assert_eq!(text.chars().count(), 2_000);
        assert!(text.ends_with('…'));
    }

    #[test]
    fn reply_text_handles_errors_and_empty_answers() {
        let mut view = RunView::default();
        view.apply(&AgentEvent::Error {
            message: "boom".into(),
            termination_reason: TerminationReason::Failed,
        });
        assert_eq!(
            comment_reply_text(&view, "Claude Code"),
            "⚠️ Claude Code 报错：boom"
        );

        assert_eq!(
            comment_reply_text(&RunView::default(), "Codex"),
            "（无回复内容）"
        );

        let mut timed_out = RunView::default();
        timed_out.apply(&AgentEvent::Error {
            message: "运行超时（30 分钟），已终止。".into(),
            termination_reason: TerminationReason::Timeout,
        });
        assert_eq!(
            comment_reply_text(&timed_out, "Claude Code"),
            "本次评论任务已超时，请重新 @ 我。"
        );
    }

    #[test]
    fn strip_markdown_keeps_identifiers_and_lone_markers() {
        assert_eq!(
            strip_markdown("file_token 和 comment_id 都没变"),
            "file_token 和 comment_id 都没变"
        );
        assert_eq!(strip_markdown("2 * 3 * 4"), "2 * 3 * 4");
        assert_eq!(strip_markdown("**bold** __bold__"), "bold bold");
        assert_eq!(strip_markdown("# 一级"), "一级");
        assert_eq!(strip_markdown("  # 不是标题"), "  # 不是标题");
    }

    #[test]
    fn one_run_per_comment_thread_is_enforced() {
        // Unique scope so the process-global guard cannot collide with other
        // tests running in parallel.
        let scope = "comment:test-one-run-per-thread";
        let first = CommentRunGuard::acquire(scope).expect("first acquire");
        assert!(
            CommentRunGuard::acquire(scope).is_none(),
            "a second event for the same thread must not start another run"
        );
        drop(first);
        assert!(CommentRunGuard::acquire(scope).is_some());
    }

    // ---- global run limiter ---------------------------------------------

    fn bridge_for(root: &Path, binary: PathBuf, cap: u32) -> Arc<Bridge> {
        let mut profile = profile();
        profile
            .preferences
            .get_or_insert_with(Default::default)
            .max_concurrent_runs = Some(cap);
        let paths = AppPaths::resolve(root, "test").expect("app paths");
        let channel = larkline_lark::Channel::new("cli_test_app", "secret", TenantBrand::Feishu);
        Arc::new(
            Bridge::new(
                channel,
                profile,
                binary,
                root.join("workspace"),
                None,
                None,
                paths,
            )
            .expect("bridge"),
        )
    }

    /// Parse the standard fixture into the event + enriched mention a run
    /// needs, without touching the network (the fetch step is skipped).
    fn run_fixture() -> (RawCommentEvent, CommentMention) {
        let CommentEventParse::Parsed(event) = event(flat_event_body()) else {
            panic!("expected a parsed comment event");
        };
        let mention = mention(flat_event_body());
        (event, mention)
    }

    /// Serialize fake-script write + spawn across the test binary; see
    /// [`crate::TEST_FAKE_AGENT_LOCK`].
    #[cfg(unix)]
    async fn fake_agent_lock() -> tokio::sync::MutexGuard<'static, ()> {
        crate::TEST_FAKE_AGENT_LOCK.lock().await
    }

    #[cfg(unix)]
    fn fake_agent(dir: &Path, sleep_ms: u64) -> PathBuf {
        use std::os::unix::fs::PermissionsExt;

        let path = dir.join("fake-comment-agent");
        let script = format!(
            "#!/bin/sh\ncat > /dev/null\nsleep {}\nprintf '%s\\n' '{{\"type\":\"system\",\"subtype\":\"init\",\"session_id\":\"sess-comment\",\"cwd\":\"/work\",\"model\":\"fake\"}}'\nprintf '%s\\n' '{{\"type\":\"assistant\",\"message\":{{\"id\":\"m1\",\"content\":[{{\"type\":\"text\",\"text\":\"comment done\"}}]}}}}'\nprintf '%s\\n' '{{\"type\":\"result\",\"session_id\":\"sess-comment\",\"usage\":{{\"input_tokens\":1,\"output_tokens\":1}}}}'\n",
            sleep_ms as f64 / 1000.0
        );
        std::fs::write(&path, script).unwrap();
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o755)).unwrap();
        path
    }

    /// Fake agent that blocks until the test creates `release`, after writing
    /// `started`. Makes "the permit is held for the whole run" deterministic
    /// instead of relying on a sleep.
    #[cfg(unix)]
    fn blocking_fake_agent(dir: &Path, started: &Path, release: &Path) -> PathBuf {
        use std::os::unix::fs::PermissionsExt;

        let path = dir.join("fake-comment-agent-blocking");
        let script = format!(
            "#!/bin/sh\ncat > /dev/null\ntouch '{}'\nwhile [ ! -f '{}' ]; do sleep 0.01; done\nprintf '%s\\n' '{{\"type\":\"system\",\"subtype\":\"init\",\"session_id\":\"sess-comment\",\"cwd\":\"/work\",\"model\":\"fake\"}}'\nprintf '%s\\n' '{{\"type\":\"assistant\",\"message\":{{\"id\":\"m1\",\"content\":[{{\"type\":\"text\",\"text\":\"comment done\"}}]}}}}'\nprintf '%s\\n' '{{\"type\":\"result\",\"session_id\":\"sess-comment\",\"usage\":{{\"input_tokens\":1,\"output_tokens\":1}}}}'\n",
            started.display(),
            release.display()
        );
        std::fs::write(&path, script).unwrap();
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o755)).unwrap();
        path
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn comment_run_holds_a_global_slot_for_the_whole_fake_run() {
        let _agent_lock = fake_agent_lock().await;
        let root = tempfile::tempdir().unwrap();
        let started = root.path().join("comment-started");
        let release = root.path().join("comment-release");
        let bridge = bridge_for(
            root.path(),
            blocking_fake_agent(root.path(), &started, &release),
            1,
        );
        let (event, mention) = run_fixture();
        let thread_scope = comment_scope_id(&event.file_token, &event.comment_id);

        let task = {
            let bridge = Arc::clone(&bridge);
            tokio::spawn(async move {
                run_comment_agent(&bridge, &event, &mention, &thread_scope).await
            })
        };

        // The agent is blocked inside the fake binary: the permit is taken
        // before the spawn and held until the run ends.
        for _ in 0..400 {
            if started.exists() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        assert!(started.exists(), "the fake agent must start");
        assert_eq!(bridge.run_limiter().active(), 1);
        assert!(
            bridge.run_limiter().try_acquire().is_none(),
            "a live comment run must consume the global cap"
        );

        std::fs::write(&release, "go").unwrap();
        let view = tokio::time::timeout(Duration::from_secs(3), task)
            .await
            .expect("fake comment run finished")
            .expect("comment task");
        assert_eq!(view.best_answer(), "comment done");
        assert_eq!(bridge.run_limiter().active(), 0);
        assert!(bridge.run_limiter().try_acquire().is_some());
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn comment_run_releases_its_slot_when_the_agent_cannot_start() {
        let root = tempfile::tempdir().unwrap();
        let bridge = bridge_for(root.path(), PathBuf::from("/nonexistent/agent"), 1);
        let (event, mention) = run_fixture();
        let thread_scope = comment_scope_id(&event.file_token, &event.comment_id);

        let view = run_comment_agent_with_wait(
            &bridge,
            &event,
            &mention,
            &thread_scope,
            Duration::from_millis(100),
        )
        .await;
        assert!(
            comment_reply_text(&view, "Claude Code").contains("无法启动")
                || comment_reply_text(&view, "Claude Code").contains("报错"),
            "spawn failure should surface: {}",
            comment_reply_text(&view, "Claude Code")
        );
        assert_eq!(bridge.run_limiter().active(), 0);
        assert!(bridge.run_limiter().try_acquire().is_some());
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn comment_run_times_out_with_a_user_visible_slot_message() {
        let _agent_lock = fake_agent_lock().await;
        let root = tempfile::tempdir().unwrap();
        let bridge = bridge_for(root.path(), fake_agent(root.path(), 0), 1);
        let held = bridge.run_limiter().try_acquire().expect("only slot");
        let (event, mention) = run_fixture();
        let thread_scope = comment_scope_id(&event.file_token, &event.comment_id);

        let view = run_comment_agent_with_wait(
            &bridge,
            &event,
            &mention,
            &thread_scope,
            Duration::from_millis(25),
        )
        .await;
        let reply = comment_reply_text(&view, "Claude Code");
        assert!(reply.contains("并发任务"), "{reply}");
        assert!(reply.contains("超时"), "{reply}");
        assert_eq!(
            bridge.run_limiter().active(),
            1,
            "the held slot is untouched"
        );

        drop(held);
        assert_eq!(bridge.run_limiter().active(), 0);
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn comment_run_waits_for_the_slot_an_im_run_holds() {
        let _agent_lock = fake_agent_lock().await;
        let root = tempfile::tempdir().unwrap();
        let started = root.path().join("comment-wait-started");
        let release = root.path().join("comment-wait-release");
        let bridge = bridge_for(
            root.path(),
            blocking_fake_agent(root.path(), &started, &release),
            1,
        );
        let (event, mention) = run_fixture();
        let thread_scope = comment_scope_id(&event.file_token, &event.comment_id);

        // Stand in for a live IM run: the queue driver takes its permit with
        // `try_acquire`, and this test holds exactly that.
        let im_permit = bridge.run_limiter().try_acquire().expect("IM run slot");
        let task = {
            let bridge = Arc::clone(&bridge);
            tokio::spawn(async move {
                run_comment_agent_with_wait(
                    &bridge,
                    &event,
                    &mention,
                    &thread_scope,
                    Duration::from_secs(2),
                )
                .await
            })
        };

        tokio::time::sleep(Duration::from_millis(100)).await;
        assert!(
            !started.exists(),
            "the comment agent must not spawn while the IM run holds the cap"
        );
        assert_eq!(bridge.run_limiter().active(), 1);
        assert!(!task.is_finished());

        // Releasing the IM run's slot lets the waiting comment run start.
        drop(im_permit);
        for _ in 0..400 {
            if started.exists() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        assert!(started.exists(), "the comment run starts after the release");
        std::fs::write(&release, "go").unwrap();
        let view = tokio::time::timeout(Duration::from_secs(3), task)
            .await
            .expect("comment run finishes after the slot is released")
            .expect("comment task");
        assert_eq!(view.best_answer(), "comment done");
        assert_eq!(bridge.run_limiter().active(), 0);
    }
}
