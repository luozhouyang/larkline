//! Run flow: policy -> card -> agent -> card updates -> session.
//!
//! Deliberately sequential per chat. A second message while a run is live gets
//! a short "still working" reply instead of spawning a competing agent;
//! `/stop` aborts the task, which drops the child (kill-on-drop).

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::Context as _;
use lark_channel::event::CardActionEvent;
use lark_channel::message::{MessageChatType, MessageSenderType, NormalizedMessage};
use larkline_agent::{
    AgentEvent, PromptContext, PromptMention, SandboxMode, TerminationReason, build_agent_prompt,
};
use larkline_core::{AppPaths, MessageReplyMode, ProfileConfig, SessionStore};
use larkline_lark::Channel;
use serde_json::json;
use tokio::sync::Mutex;
use tokio::task::JoinHandle;

use crate::agent_runner::{AgentProcess, AgentSpec};
use crate::callback::CallbackTokens;
use crate::policy;
use crate::render::RunView;

/// Card updates are throttled; Feishu rate-limits card writes and a token
/// stream would otherwise produce dozens of updates per second.
const CARD_UPDATE_INTERVAL: Duration = Duration::from_millis(900);
/// Hard cap for one agent run.
const RUN_TIMEOUT: Duration = Duration::from_secs(30 * 60);
/// Cards over this many characters are truncated (CardKit caps JSON size).
const MAX_CARD_CHARS: usize = 8_000;

pub struct Bridge {
    pub channel: Channel,
    pub profile: ProfileConfig,
    pub agent_binary: PathBuf,
    pub cwd: PathBuf,
    pub bot_open_id: Option<String>,
    pub bot_name: Option<String>,
    paths: AppPaths,
    tokens: CallbackTokens,
    sessions: Mutex<SessionStore>,
    active: Mutex<HashMap<String, JoinHandle<()>>>,
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
        Ok(Self {
            channel,
            profile,
            agent_binary,
            cwd,
            bot_open_id,
            bot_name,
            paths,
            tokens: CallbackTokens::new(),
            sessions: Mutex::new(sessions),
            active: Mutex::new(HashMap::new()),
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
            let _ = self.channel.send_text(&message.chat_id, reply).await;
            return;
        }

        let chat_id = message.chat_id.clone();
        let mut active = self.active.lock().await;
        active.retain(|_, handle| !handle.is_finished());
        if active.contains_key(&chat_id) {
            drop(active);
            let _ = self
                .channel
                .send_text(&chat_id, "上一条消息还在处理中，发送 /stop 可以中断。")
                .await;
            return;
        }

        let bridge = Arc::clone(self);
        let handle = tokio::spawn(async move {
            if let Err(err) = bridge.run_agent(message).await {
                tracing::error!(error = %err, "agent run failed");
            }
        });
        active.insert(chat_id, handle);
    }

    async fn handle_command(&self, message: &NormalizedMessage, text: &str) -> Option<String> {
        let command = text.split_whitespace().next().unwrap_or_default();
        match command {
            "/stop" => {
                let mut active = self.active.lock().await;
                match active.remove(&message.chat_id) {
                    Some(handle) => {
                        handle.abort();
                        Some("已停止当前任务。".to_string())
                    }
                    None => Some("当前没有正在运行的任务。".to_string()),
                }
            }
            "/new" | "/reset" => match self.sessions.lock().await.clear(&message.chat_id) {
                Ok(()) => Some("已开启新会话。".to_string()),
                Err(err) => Some(format!("重置失败：{err}")),
            },
            "/help" => {
                Some("可用命令：\n/new 开启新会话\n/stop 中断当前任务\n/help 显示帮助".to_string())
            }
            _ => None,
        }
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
                let mut active = self.active.lock().await;
                match active.remove(scope) {
                    Some(handle) => {
                        handle.abort();
                        Some("已停止当前任务。".to_string())
                    }
                    None => Some("当前没有正在运行的任务。".to_string()),
                }
            }
            _ => None,
        }
    }

    async fn run_agent(self: Arc<Self>, message: NormalizedMessage) -> anyhow::Result<()> {
        let started = Instant::now();
        let chat_id = message.chat_id.clone();
        let cwd = self.cwd.clone();
        let cwd_str = cwd.to_string_lossy().into_owned();
        let resume = {
            let sessions = self.sessions.lock().await;
            sessions.resume_for(&chat_id, &cwd_str).map(str::to_string)
        };

        let context = self.prompt_context(&message);
        let prompt = build_agent_prompt(&context, message.text.trim());
        let use_card = self.profile.message_reply() != MessageReplyMode::Text;

        let mut view = RunView::default();
        let mut message_id: Option<String> = None;
        if use_card {
            let card = build_card(&self.profile, &view, "启动中", started, None);
            match self.channel.send_card(&chat_id, &card).await {
                Ok(id) => message_id = Some(id.0),
                Err(err) => {
                    tracing::warn!(error = %err, "cannot send run card; falling back to text");
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
                self.finalize(&chat_id, message_id.as_deref(), &view, use_card, started)
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
                    if use_card
                        && message_id.is_some()
                        && last_update.elapsed() >= CARD_UPDATE_INTERVAL
                    {
                        self.update_card(&chat_id, message_id.as_deref(), &view, started)
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

        self.finalize(&chat_id, message_id.as_deref(), &view, use_card, started)
            .await;
        Ok(())
    }

    async fn finalize(
        &self,
        chat_id: &str,
        message_id: Option<&str>,
        view: &RunView,
        use_card: bool,
        started: Instant,
    ) {
        if use_card {
            if let Some(id) = message_id {
                self.update_card(chat_id, Some(id), view, started).await;
                return;
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
        let status = if view.error.is_some() {
            "失败"
        } else if view.finished {
            "完成"
        } else {
            "运行中"
        };
        let token = if view.finished {
            None
        } else {
            Some(self.tokens.sign(chat_id, "stop"))
        };
        let card = build_card(
            &self.profile,
            view,
            status,
            started,
            token.as_deref().map(|token| (chat_id, token)),
        );
        if let Err(err) = self.channel.update_card_message(message_id, &card).await {
            tracing::warn!(error = %err, "card update failed");
        }
    }

    fn prompt_context(&self, message: &NormalizedMessage) -> PromptContext {
        let chat_type = match message.chat_type {
            MessageChatType::P2p => "p2p",
            MessageChatType::Group => "group",
            MessageChatType::Unknown => "unknown",
        };
        let sender_type = match message.sender.sender_type {
            MessageSenderType::User => "user",
            MessageSenderType::Bot => "bot",
            MessageSenderType::Unknown => "unknown",
        };
        let mentions: Vec<PromptMention> = message
            .mentions
            .iter()
            .map(|mention| PromptMention {
                open_id: Some(mention.open_id.clone()).filter(|id| !id.is_empty()),
                name: mention.name.clone(),
                is_bot: Some(mention.mentioned_type == MessageSenderType::Bot),
            })
            .collect();

        PromptContext {
            chat_id: message.chat_id.clone(),
            chat_type: chat_type.to_string(),
            sender_id: message.sender_id.clone(),
            sender_name: None,
            sender_type: Some(sender_type.to_string()),
            bot_open_id: self.bot_open_id.clone(),
            mentions: if mentions.is_empty() {
                None
            } else {
                Some(mentions)
            },
            thread_id: message.thread_id.clone(),
            message_ids: Some(vec![message.message_id.clone()]),
            source: "im".to_string(),
        }
    }
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

    let mut elements = vec![json!({ "tag": "markdown", "content": body })];
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

fn truncate_chars(value: &str, max: usize) -> String {
    if value.chars().count() <= max {
        value.to_string()
    } else {
        let mut out: String = value.chars().take(max).collect();
        out.push_str("\n\n…（内容过长已截断）");
        out
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
}
