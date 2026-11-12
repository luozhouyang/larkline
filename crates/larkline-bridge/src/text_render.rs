//! Plain-text / markdown rendering for non-card replies.
//!
//! This is the Rust port of `src/card/text-renderer.ts` and
//! `src/card/mask-email.ts`. It is intentionally pure: it takes the folded
//! [`RunView`] and options, and returns a string. No network, no clock, no
//! profile lookup — callers translate the profile into [`TextRenderOptions`].
//!
//! The TypeScript renderer works on an ordered `RunState.blocks` array (text
//! and tool blocks interleaved). [`RunView`] keeps the answer and the tool
//! summaries separately and flattened, so this port can only render the
//! answer followed by the tool lines. That is the one structural difference;
//! formatting, reasoning truncation, terminal notices and email masking match
//! the TS renderer.

use larkline_agent::TerminationReason;

use crate::render::RunView;

/// TS `DEFAULT_REASONING_MAX_CHARS` in `text-renderer.ts`.
pub const DEFAULT_REASONING_MAX_CHARS: usize = 3_000;

/// Default cap for one IM text message. Feishu rejects very large text
/// payloads; the previous Rust text fallback also used 4000 characters.
///
/// The TS text renderer has no overall cap (it relies on the caller/SDK), so
/// this is a Rust-side guard. The suffix matches the bridge's existing
/// [`crate::run`] truncation notice.
pub const DEFAULT_TEXT_MAX_CHARS: usize = 4_000;

const TRUNCATION_SUFFIX: &str = "\n\n…（内容过长已截断）";

/// How the reply will be parsed by the IM client.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum TextFormat {
    /// Markdown message: the TS `renderText` output (`**`, `>`, `_`).
    #[default]
    Markdown,
    /// Plain text message (`send_text`): no renderer-added markdown markers.
    ///
    /// The agent's answer is left untouched so code blocks and inline
    /// literals are not mangled; only the markers this renderer adds are
    /// omitted.
    Plain,
}

/// Options for [`render_text`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TextRenderOptions {
    /// Markdown vs plain text output.
    pub format: TextFormat,
    /// Include one line per tool call. Mirrors `preferences.showToolCalls`.
    pub show_tool_calls: bool,
    /// Include aggregated reasoning.
    ///
    /// The TS text renderer only includes reasoning for `reasoningDisplay:
    /// inline`; `auto` (card-only), `off` and `bubble` (COT stream) all map to
    /// `false`. Callers coming from a profile should map the display first.
    pub reasoning: bool,
    /// Max characters of reasoning to include.
    pub reasoning_max_chars: usize,
    /// Max characters of the complete rendered message.
    pub max_chars: usize,
    /// Minutes reported by the idle-timeout notice (`0` when unknown, matching
    /// `state.idleTimeoutMinutes ?? 0` in TS).
    pub idle_timeout_minutes: u64,
}

impl Default for TextRenderOptions {
    fn default() -> Self {
        Self {
            format: TextFormat::Markdown,
            show_tool_calls: true,
            reasoning: false,
            reasoning_max_chars: DEFAULT_REASONING_MAX_CHARS,
            max_chars: DEFAULT_TEXT_MAX_CHARS,
            idle_timeout_minutes: 0,
        }
    }
}

impl TextRenderOptions {
    /// Plain-text output (for `MessageReplyMode::Text`).
    pub fn plain() -> Self {
        Self {
            format: TextFormat::Plain,
            ..Self::default()
        }
    }

    /// Builder helper for optionally including reasoning.
    pub fn with_reasoning(mut self, reasoning: bool) -> Self {
        self.reasoning = reasoning;
        self
    }
}

/// Render a folded run as one markdown or plain-text message.
///
/// Mirrors `renderText` from `src/card/text-renderer.ts`:
///
/// 1. reasoning (when enabled), as a title + quoted block;
/// 2. the final answer;
/// 3. one short line per tool call (when enabled);
/// 4. the terminal notice while finished, or the run footer while running;
/// 5. email masking over the whole message; then
/// 6. truncation to the IM text limit.
///
/// The final answer is `RunView::final_answer` when the protocol provided one,
/// falling back to the streamed `answer` text. An error message is *not* used
/// as the answer; like TS, it is rendered as a terminal notice after the
/// partial answer.
pub fn render_text(view: &RunView, opts: TextRenderOptions) -> String {
    let markdown = opts.format == TextFormat::Markdown;
    let mut parts: Vec<String> = Vec::new();

    if opts.reasoning && !view.reasoning.trim().is_empty() {
        parts.push(reasoning_section(
            &view.reasoning,
            reasoning_is_active(view),
            opts.reasoning_max_chars,
            markdown,
        ));
    }

    let answer = selected_answer(view).trim();
    if !answer.is_empty() {
        parts.push(answer.to_string());
    }

    if opts.show_tool_calls {
        for tool in &view.tools {
            if let Some(line) = tool_summary_line(tool, markdown) {
                parts.push(line);
            }
        }
    }

    if let Some(notice) = terminal_notice(view, opts.idle_timeout_minutes, markdown) {
        parts.push(notice);
    } else if let Some(footer) = footer_line(view, markdown) {
        parts.push(footer);
    }

    // Masking happens before truncation so a cut can never expose a partial
    // (but still raw-looking) address from the retained prefix.
    truncate_output(mask_emails(&parts.join("\n\n")), opts.max_chars)
}

/// Rewrite every email in `text` so the Feishu tenant audit does not reject
/// the message with `EMAIL_ADDRESS`.
///
/// Exact port of `maskEmails` in `src/card/mask-email.ts`: the `@` of a
/// `local@domain.tld` match becomes `[at]`. A raw address must have a dotted
/// domain ending in a 2+ ASCII-letter TLD. Values like `@scope/pkg`,
/// `pkg@1.2.3` and `user@localhost` are left alone; SSH remotes such as
/// `git@example.com` are intentionally masked too.
pub fn mask_emails(text: &str) -> String {
    let bytes = text.as_bytes();
    let mut out = String::with_capacity(text.len());
    let mut i = 0;

    while i < bytes.len() {
        if is_local_byte(bytes[i]) {
            let run_start = i;
            while i < bytes.len() && is_local_byte(bytes[i]) {
                i += 1;
            }
            // The only possible match starting in this run ends at its first
            // non-local byte. If that byte is `@` and the domain does not
            // match, no later start inside the run can match either, so copy
            // the run and continue (avoids quadratic rescanning).
            if i < bytes.len() && bytes[i] == b'@' {
                if let Some(end) = match_domain(bytes, i + 1) {
                    out.push_str(&text[run_start..i]);
                    out.push_str("[at]");
                    out.push_str(&text[i + 1..end]);
                    i = end;
                    continue;
                }
            }
            out.push_str(&text[run_start..i]);
            continue;
        }

        let ch = text[i..]
            .chars()
            .next()
            .expect("index is at a character boundary");
        out.push(ch);
        i += ch.len_utf8();
    }

    out
}

fn is_local_byte(byte: u8) -> bool {
    byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'%' | b'+' | b'-')
}

fn is_domain_label_byte(byte: u8) -> bool {
    byte.is_ascii_alphanumeric() || byte == b'-'
}

/// Match `(?:[A-Za-z0-9-]+\.)+[A-Za-z]{2,}` starting at `start`.
///
/// Returns the exclusive end of the domain. Candidate TLD positions are tried
/// from the greediest (most labels) to the shortest, mirroring the backtracking
/// of the JS regex. The TLD is a maximal run of ASCII letters, so `a@b.com1`
/// masks `a@b.com` and leaves the trailing `1` untouched, exactly like TS.
fn match_domain(bytes: &[u8], start: usize) -> Option<usize> {
    let mut candidates = Vec::new();
    let mut cursor = start;

    loop {
        let label_start = cursor;
        while cursor < bytes.len() && is_domain_label_byte(bytes[cursor]) {
            cursor += 1;
        }
        if cursor == label_start || cursor >= bytes.len() || bytes[cursor] != b'.' {
            break;
        }
        cursor += 1;
        candidates.push(cursor);
    }

    for &tld_start in candidates.iter().rev() {
        let mut end = tld_start;
        while end < bytes.len() && bytes[end].is_ascii_alphabetic() {
            end += 1;
        }
        if end - tld_start >= 2 {
            return Some(end);
        }
    }
    None
}

fn selected_answer(view: &RunView) -> &str {
    match view.final_answer.as_deref() {
        Some(final_answer) if !final_answer.trim().is_empty() => final_answer,
        _ => &view.answer,
    }
}

/// The TS renderer titles reasoning "思考中" while the model is still thinking
/// and "思考过程" once it has moved on. The folded view has no event order, so
/// this approximates it: still running, no answer yet, and no tool activity.
fn reasoning_is_active(view: &RunView) -> bool {
    !view.finished
        && view.error.is_none()
        && selected_answer(view).trim().is_empty()
        && view.tools.is_empty()
}

fn reasoning_section(content: &str, active: bool, max_chars: usize, markdown: bool) -> String {
    let body = content.trim();
    let body_len = body.chars().count();
    let truncated = body_len > max_chars;
    let clipped: String = if truncated {
        body.chars().take(max_chars).collect()
    } else {
        body.to_string()
    };
    let clipped = clipped.trim_end();

    let title = match (active, markdown) {
        (true, true) => "🧠 **思考中**",
        (false, true) => "🧠 **思考过程**",
        (true, false) => "🧠 思考中",
        (false, false) => "🧠 思考过程",
    };
    let mut out = String::from(title);

    if !clipped.is_empty() {
        out.push('\n');
        if markdown {
            // Reasoning is model-authored: escape `<` so HTML tags do not
            // become live markup (same as the TS renderer).
            let escaped = clipped.replace('<', "&lt;");
            let quoted = escaped
                .split('\n')
                .map(|line| {
                    if line.is_empty() {
                        ">".to_string()
                    } else {
                        format!("> {line}")
                    }
                })
                .collect::<Vec<_>>()
                .join("\n");
            out.push_str(&quoted);
        } else {
            out.push_str(clipped);
        }
    }

    if truncated {
        out.push('\n');
        out.push_str(if markdown {
            "> …（思考内容已截断）"
        } else {
            "…（思考内容已截断）"
        });
    }

    out
}

/// Render one tool summary (`Bash: pwd`) as a TS-style header line.
///
/// The status icon (`✅`/`❌`/`⏳`) is unavailable: `RunView::tools` stores
/// flattened one-line summaries without the per-tool status. The name/detail
/// split still matches `toolHeaderText`'s layout.
fn tool_summary_line(summary: &str, markdown: bool) -> Option<String> {
    let summary = summary.trim();
    if summary.is_empty() {
        return None;
    }
    let (name, detail) = match summary.split_once(": ") {
        Some((name, detail)) => (name.trim(), collapse_whitespace(detail)),
        None => (summary, String::new()),
    };
    if name.is_empty() {
        return None;
    }

    let line = if detail.is_empty() {
        if markdown {
            format!("> **{name}**")
        } else {
            format!("• {name}")
        }
    } else if markdown {
        format!("> **{name}** — {detail}")
    } else {
        format!("• {name} — {detail}")
    };
    Some(line)
}

fn collapse_whitespace(value: &str) -> String {
    value.split_whitespace().collect::<Vec<_>>().join(" ")
}

fn footer_line(view: &RunView, markdown: bool) -> Option<String> {
    if view.finished || view.error.is_some() {
        return None;
    }
    let status = if !selected_answer(view).trim().is_empty() {
        "✍️ 正在输出…"
    } else if !view.tools.is_empty() {
        "🧰 正在调用工具…"
    } else {
        "🧠 正在思考…"
    };
    Some(emphasize(status, markdown))
}

fn terminal_notice(view: &RunView, idle_minutes: u64, markdown: bool) -> Option<String> {
    if !view.finished {
        return None;
    }
    match view.termination {
        Some(TerminationReason::Interrupted) => Some(emphasize("⏹ 已被中断", markdown)),
        Some(TerminationReason::Timeout) => Some(emphasize(
            &format!("⏱ {idle_minutes} 分钟无响应,已自动终止"),
            markdown,
        )),
        _ => view
            .error
            .as_deref()
            .filter(|message| !message.trim().is_empty())
            .map(|message| format!("⚠️ agent 失败:{message}")),
    }
}

fn emphasize(text: &str, markdown: bool) -> String {
    if markdown {
        format!("_{text}_")
    } else {
        text.to_string()
    }
}

fn truncate_output(text: String, max_chars: usize) -> String {
    if text.chars().count() <= max_chars {
        return text;
    }
    let kept: String = text.chars().take(max_chars).collect();
    let mut out = kept.trim_end().to_string();
    out.push_str(TRUNCATION_SUFFIX);
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use larkline_agent::AgentEvent;
    use serde_json::json;

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

    fn final_text(content: &str) -> AgentEvent {
        AgentEvent::FinalText {
            content: content.into(),
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

    // ---------------------------------------------------------------------
    // maskEmails — ported from tests/unit/card/mask-email.test.ts
    // ---------------------------------------------------------------------

    #[test]
    fn mask_emails_rewrites_a_plain_email() {
        assert_eq!(mask_emails("noreply@example.com"), "noreply[at]example.com");
    }

    #[test]
    fn mask_emails_masks_emails_inside_a_commit_trailer() {
        assert_eq!(
            mask_emails("Co-Authored-By: Some One <noreply@example.com>"),
            "Co-Authored-By: Some One <noreply[at]example.com>"
        );
    }

    #[test]
    fn mask_emails_masks_every_email_including_subdomains_and_plus_tags() {
        assert_eq!(
            mask_emails("a.b+c@sub.example.co.uk and x@example.org"),
            "a.b+c[at]sub.example.co.uk and x[at]example.org"
        );
    }

    #[test]
    fn mask_emails_masks_an_ssh_remote() {
        assert_eq!(
            mask_emails("git@example.com:org/repo.git"),
            "git[at]example.com:org/repo.git"
        );
    }

    #[test]
    fn mask_emails_leaves_non_email_at_usages_untouched() {
        assert_eq!(mask_emails("@scope/pkg"), "@scope/pkg");
        assert_eq!(mask_emails("pkg@1.2.3"), "pkg@1.2.3");
        assert_eq!(mask_emails("user@localhost"), "user@localhost");
        assert_eq!(
            mask_emails("see @someone in the thread"),
            "see @someone in the thread"
        );
    }

    #[test]
    fn mask_emails_leaves_no_raw_email() {
        let masked = mask_emails("reach me at foo.bar@example.org please");
        assert_eq!(masked, "reach me at foo.bar[at]example.org please");
        assert!(!masked.contains('@'));
    }

    #[test]
    fn mask_emails_matches_js_regex_edge_cases() {
        // One-letter TLDs and non-letter tails are not a match.
        assert_eq!(mask_emails("a@b.c"), "a@b.c");
        assert_eq!(mask_emails("a@b"), "a@b");
        assert_eq!(mask_emails("a@b.1c"), "a@b.1c");
        assert_eq!(mask_emails("a@b.c1"), "a@b.c1");
        assert_eq!(mask_emails("a@b.c.d"), "a@b.c.d");
        assert_eq!(mask_emails("a@b..com"), "a@b..com");
        // The TLD is a maximal letters-only run; the JS regex can stop in the
        // middle of a label and leaves the rest untouched.
        assert_eq!(mask_emails("a@b.com1"), "a[at]b.com1");
        assert_eq!(mask_emails("a@b.co.1"), "a[at]b.co.1");
        assert_eq!(mask_emails("a@b.co1.d"), "a[at]b.co1.d");
        assert_eq!(mask_emails("a@b.cc.d"), "a[at]b.cc.d");
        assert_eq!(mask_emails("a@b.co0m"), "a[at]b.co0m");
        // Labels accept digits and leading/trailing hyphens, like the JS class.
        assert_eq!(mask_emails("a@b.1.com"), "a[at]b.1.com");
        assert_eq!(mask_emails("a@-b.com"), "a[at]-b.com");
        assert_eq!(mask_emails("a@b.-c.com"), "a[at]b.-c.com");
        // Trailing punctuation stays outside the match.
        assert_eq!(mask_emails("a@b.co.uk."), "a[at]b.co.uk.");
        assert_eq!(mask_emails("x@y.zz@z.yy"), "x[at]y.zz@z.yy");
    }

    #[test]
    fn mask_emails_preserves_unicode_around_the_address() {
        assert_eq!(
            mask_emails("联系我：user@example.com，谢谢"),
            "联系我：user[at]example.com，谢谢"
        );
        assert_eq!(mask_emails("emoji 🦀 @scope/pkg"), "emoji 🦀 @scope/pkg");
    }

    // ---------------------------------------------------------------------
    // renderText — ported from run-renderer.snapshot.test.ts / mask-email.test.ts
    // ---------------------------------------------------------------------

    #[test]
    fn renders_markdown_text_mode_without_card_controls() {
        // TS snapshot: thinking, "Answer", Bash tool, done. RunView flattens
        // the text blocks, so only the content assertions carry over.
        let view = applied(&[
            thinking("hidden reasoning"),
            text("Answer"),
            tool_use("tool-1", "Bash", json!({ "command": "pwd" })),
            tool_result("tool-1", "/repo", false),
            done(TerminationReason::Normal),
        ]);
        let rendered = render_text(&view, TextRenderOptions::default());

        assert_eq!(rendered, "Answer\n\n> **Bash** — pwd");
        assert!(!rendered.contains("hidden reasoning"));
        assert!(!rendered.contains("停止"));
        assert!(!rendered.contains("工具调用"));
    }

    #[test]
    fn hides_reasoning_by_default() {
        let view = applied(&[thinking("hidden reasoning"), text("Answer")]);

        assert!(!render_text(&view, TextRenderOptions::default()).contains("hidden reasoning"));
        assert_eq!(
            render_text(&view, TextRenderOptions::default()),
            render_text(&view, TextRenderOptions::default().with_reasoning(false))
        );
    }

    #[test]
    fn prepends_active_reasoning_as_a_markdown_blockquote_when_enabled() {
        let view = applied(&[thinking("line 1\n\nline 2")]);

        assert_eq!(
            render_text(&view, TextRenderOptions::default().with_reasoning(true)),
            "🧠 **思考中**\n> line 1\n>\n> line 2\n\n_🧠 正在思考…_"
        );
    }

    #[test]
    fn uses_the_completed_reasoning_title_before_the_answer() {
        let view = applied(&[
            thinking("done thinking"),
            text("Answer"),
            done(TerminationReason::Normal),
        ]);
        let rendered = render_text(&view, TextRenderOptions::default().with_reasoning(true));

        assert_eq!(rendered, "🧠 **思考过程**\n> done thinking\n\nAnswer");
        assert!(rendered.find("🧠 **思考过程**").unwrap() < rendered.find("Answer").unwrap());
    }

    #[test]
    fn truncates_reasoning_to_reasoning_max_chars_and_appends_a_note() {
        let view = applied(&[thinking("0123456789EXTRA")]);
        let rendered = render_text(
            &view,
            TextRenderOptions {
                reasoning: true,
                reasoning_max_chars: 10,
                ..TextRenderOptions::default()
            },
        );

        assert!(rendered.contains("> 0123456789\n> …（思考内容已截断）"));
        assert!(!rendered.contains("EXTRA"));
    }

    #[test]
    fn defaults_the_reasoning_limit_to_3000_characters() {
        let head = "a".repeat(3_000);
        let view = applied(&[thinking(&format!("{head}TAIL"))]);
        let rendered = render_text(&view, TextRenderOptions::default().with_reasoning(true));

        assert!(rendered.contains(&format!("> {head}")));
        assert!(!rendered.contains("TAIL"));
        assert!(rendered.contains("> …（思考内容已截断）"));
    }

    #[test]
    fn masks_emails_and_neutralizes_html_in_reasoning() {
        let view = applied(&[thinking("mail me@example.com <b>hi</b>")]);
        let rendered = render_text(&view, TextRenderOptions::default().with_reasoning(true));

        assert!(rendered.contains("me[at]example.com"));
        assert!(!rendered.contains("me@example.com"));
        assert!(!rendered.contains("<b>"));
        assert!(rendered.contains("&lt;b>hi&lt;/b>"));
    }

    #[test]
    fn prefers_the_final_answer_over_streamed_text() {
        let view = applied(&[
            text("working"),
            final_text("answer"),
            done(TerminationReason::Normal),
        ]);

        assert_eq!(render_text(&view, TextRenderOptions::default()), "answer");
    }

    #[test]
    fn renders_one_line_tool_summaries() {
        let view = applied(&[
            text("done"),
            tool_use("tool-1", "Bash", json!({ "command": "pwd" })),
            tool_result("tool-1", "/repo", false),
            done(TerminationReason::Normal),
        ]);

        assert_eq!(
            render_text(&view, TextRenderOptions::default()),
            "done\n\n> **Bash** — pwd"
        );
        assert_eq!(
            render_text(
                &view,
                TextRenderOptions {
                    show_tool_calls: false,
                    ..TextRenderOptions::default()
                }
            ),
            "done"
        );
    }

    #[test]
    fn renders_terminal_notices() {
        let interrupted = applied(&[text("partial"), done(TerminationReason::Interrupted)]);
        assert_eq!(
            render_text(&interrupted, TextRenderOptions::default()),
            "partial\n\n_⏹ 已被中断_"
        );

        let timed_out = applied(&[
            text("partial"),
            error("run timed out", TerminationReason::Timeout),
        ]);
        assert_eq!(
            render_text(&timed_out, TextRenderOptions::default()),
            "partial\n\n_⏱ 0 分钟无响应,已自动终止_"
        );

        let failed = applied(&[error("process failed", TerminationReason::Failed)]);
        assert_eq!(
            render_text(&failed, TextRenderOptions::default()),
            "⚠️ agent 失败:process failed"
        );
    }

    #[test]
    fn renders_running_footer_statuses() {
        assert_eq!(
            render_text(&RunView::default(), TextRenderOptions::default()),
            "_🧠 正在思考…_"
        );
        assert_eq!(
            render_text(
                &applied(&[tool_use("tool-1", "Bash", json!({ "command": "pwd" }))]),
                TextRenderOptions::default()
            ),
            "> **Bash** — pwd\n\n_🧰 正在调用工具…_"
        );
        assert_eq!(
            render_text(&applied(&[text("Answer")]), TextRenderOptions::default()),
            "Answer\n\n_✍️ 正在输出…_"
        );
    }

    #[test]
    fn truncates_to_the_text_message_limit() {
        let view = applied(&[
            text(&format!("{}TAIL", "a".repeat(10))),
            done(TerminationReason::Normal),
        ]);
        let rendered = render_text(
            &view,
            TextRenderOptions {
                max_chars: 10,
                ..TextRenderOptions::default()
            },
        );

        assert_eq!(rendered, "aaaaaaaaaa\n\n…（内容过长已截断）");
        assert!(!rendered.contains("TAIL"));
    }

    #[test]
    fn plain_mode_omits_markdown_markup() {
        let reasoning = applied(&[thinking("hmm")]);
        let rendered = render_text(&reasoning, TextRenderOptions::plain().with_reasoning(true));
        assert_eq!(rendered, "🧠 思考中\nhmm\n\n🧠 正在思考…");
        assert!(!rendered.contains("**"));
        assert!(!rendered.contains('>'));
        assert!(!rendered.contains('_'));

        let with_tool = applied(&[
            text("Answer"),
            tool_use("tool-1", "Bash", json!({ "command": "pwd" })),
            done(TerminationReason::Normal),
        ]);
        assert_eq!(
            render_text(&with_tool, TextRenderOptions::plain()),
            "Answer\n\n• Bash — pwd"
        );
    }

    #[test]
    fn renders_no_body_for_an_empty_normal_finish() {
        let view = applied(&[done(TerminationReason::Normal)]);
        assert_eq!(render_text(&view, TextRenderOptions::default()), "");
    }

    #[test]
    fn keeps_local_paths_in_text_replies() {
        let sensitive_path = "/Users/example/private/customer/repo/secret.txt";
        let view = applied(&[
            text(&format!("I read {sensitive_path}")),
            tool_use("tool-1", "Read", json!({ "file_path": sensitive_path })),
            tool_result("tool-1", &format!("content from {sensitive_path}"), false),
            done(TerminationReason::Normal),
        ]);
        let rendered = render_text(&view, TextRenderOptions::default());

        assert!(rendered.contains(sensitive_path));
    }

    #[test]
    fn text_output_masks_emails_end_to_end() {
        let view = applied(&[
            tool_use(
                "tool-1",
                "Bash",
                json!({ "command": "git commit -m \"x\"" }),
            ),
            tool_result(
                "tool-1",
                "Co-Authored-By: Some One <noreply@example.com>",
                false,
            ),
            text("Committed. Trailer: authored by team@example.org."),
            done(TerminationReason::Normal),
        ]);
        let rendered = render_text(&view, TextRenderOptions::default());

        assert!(!rendered.contains('@'));
        assert!(rendered.contains("team[at]example.org"));
    }
}
