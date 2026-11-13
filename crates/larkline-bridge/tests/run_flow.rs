//! End-to-end run-flow tests with a fake outbound transport and a fake agent.
//!
//! These tests drive [`Bridge::handle_message`] exactly like the CLI event
//! loop does: an inbound message enters the debounced queue, the run flow
//! renders a running card, starts the agent, folds its events into the card,
//! and reaches a terminal card. Nothing here touches the network:
//!
//! * [`FakeTransport`] implements [`BridgeTransport`] and records every send,
//!   streaming frame and update;
//! * the agent is a shell script speaking Claude's `stream-json` dialect;
//! * attachments use a failing downloader, and reactions are a no-op.
//!
//! Determinism: the idle-timeout test shortens one configured minute through
//! [`Bridge::with_idle_timeout_unit`], and the `/stop` test waits for the
//! partial-answer frame (the production refresh interval is 900 ms) before
//! sending the second message. The only real sleep inside a fake agent is one
//! second; every wait has a hard bound.

#![cfg(unix)]

use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use lark_channel::event::{
    CardActionContext, CardActionEvent, CardActionOperator, CardActionPayload, EventContext,
};
use lark_channel::media::{DownloadedResource, ResourceDescriptor, ResourceType};
use lark_channel::message::MessageId;
use lark_channel::{Card, CardId};
use larkline_bridge::{
    Bridge, BridgeStream, BridgeTransport, MediaCache, MediaError, ReactionTransport,
    ResourceDownloader, StreamTarget,
};
use larkline_core::{
    AgentKind, AppPaths, MessageReplyMode, Preferences, ProfileConfig, TenantBrand,
};
use larkline_lark::{LarkResult, RestClient, RestResult};
use serde_json::{Value, json};

// ---------------------------------------------------------------------------
// Fake transport
// ---------------------------------------------------------------------------

#[allow(dead_code)]
#[derive(Debug, Clone)]
enum Frame {
    Text {
        chat_id: String,
        text: String,
    },
    Dm {
        open_id: String,
        text: String,
    },
    Markdown {
        chat_id: String,
        markdown: String,
    },
    Card {
        chat_id: String,
        message_id: String,
        card: Value,
    },
    ReplyText {
        parent: String,
        text: String,
    },
    ReplyMarkdown {
        parent: String,
        markdown: String,
    },
    ReplyCard {
        parent: String,
        message_id: String,
        card: Value,
    },
    UpdateCard {
        message_id: String,
        card: Value,
    },
    CardEntity {
        card_id: String,
        card: Value,
    },
    CardReference {
        chat_id: String,
        card_id: String,
        message_id: String,
    },
    CardReferenceReply {
        parent: String,
        card_id: String,
        message_id: String,
    },
    StreamStart {
        message_id: String,
        card: Value,
    },
    StreamCard {
        message_id: String,
        card: Value,
    },
    StreamContent {
        message_id: String,
        content: String,
    },
    StreamFinish {
        message_id: String,
        content: String,
    },
}

#[derive(Default)]
struct FakeState {
    frames: Vec<Frame>,
    next_id: u64,
}

/// Records every outbound operation; the run flow treats it like a channel.
#[derive(Clone, Default)]
struct FakeTransport {
    state: Arc<Mutex<FakeState>>,
}

impl FakeTransport {
    fn frames(&self) -> Vec<Frame> {
        self.state
            .lock()
            .expect("fake transport state")
            .frames
            .clone()
    }

    fn next(&self, prefix: &str) -> String {
        let mut state = self.state.lock().expect("fake transport state");
        state.next_id += 1;
        format!("{prefix}_{}", state.next_id)
    }

    fn push(&self, frame: Frame) {
        self.state
            .lock()
            .expect("fake transport state")
            .frames
            .push(frame);
    }
}

#[async_trait]
impl BridgeTransport for FakeTransport {
    async fn send_text(&self, chat_id: &str, text: String) -> LarkResult<MessageId> {
        let id = self.next("om_text");
        self.push(Frame::Text {
            chat_id: chat_id.to_string(),
            text,
        });
        Ok(MessageId(id))
    }

    async fn send_dm_text(&self, open_id: &str, text: String) -> LarkResult<MessageId> {
        let id = self.next("om_dm");
        self.push(Frame::Dm {
            open_id: open_id.to_string(),
            text,
        });
        Ok(MessageId(id))
    }

    async fn send_markdown(&self, chat_id: &str, markdown: String) -> LarkResult<MessageId> {
        let id = self.next("om_md");
        self.push(Frame::Markdown {
            chat_id: chat_id.to_string(),
            markdown,
        });
        Ok(MessageId(id))
    }

    async fn send_card(&self, chat_id: &str, card: &Card) -> LarkResult<MessageId> {
        let id = self.next("om_card");
        self.push(Frame::Card {
            chat_id: chat_id.to_string(),
            message_id: id.clone(),
            card: card.as_value().clone(),
        });
        Ok(MessageId(id))
    }

    async fn reply_text(&self, parent: &MessageId, text: String) -> LarkResult<MessageId> {
        let id = self.next("om_reply_text");
        self.push(Frame::ReplyText {
            parent: parent.0.clone(),
            text,
        });
        Ok(MessageId(id))
    }

    async fn reply_markdown(&self, parent: &MessageId, markdown: String) -> LarkResult<MessageId> {
        let id = self.next("om_reply_md");
        self.push(Frame::ReplyMarkdown {
            parent: parent.0.clone(),
            markdown,
        });
        Ok(MessageId(id))
    }

    async fn reply_card(&self, parent: &MessageId, card: &Card) -> LarkResult<MessageId> {
        let id = self.next("om_reply_card");
        self.push(Frame::ReplyCard {
            parent: parent.0.clone(),
            message_id: id.clone(),
            card: card.as_value().clone(),
        });
        Ok(MessageId(id))
    }

    async fn send_card_reference(&self, chat_id: &str, card_id: &CardId) -> LarkResult<MessageId> {
        let id = self.next("om_ref");
        self.push(Frame::CardReference {
            chat_id: chat_id.to_string(),
            card_id: card_id.0.clone(),
            message_id: id.clone(),
        });
        Ok(MessageId(id))
    }

    async fn reply_card_reference(
        &self,
        parent: &MessageId,
        card_id: &CardId,
    ) -> LarkResult<MessageId> {
        let id = self.next("om_ref_reply");
        self.push(Frame::CardReferenceReply {
            parent: parent.0.clone(),
            card_id: card_id.0.clone(),
            message_id: id.clone(),
        });
        Ok(MessageId(id))
    }

    async fn update_card_message(&self, message_id: &str, card: &Card) -> LarkResult<()> {
        self.push(Frame::UpdateCard {
            message_id: message_id.to_string(),
            card: card.as_value().clone(),
        });
        Ok(())
    }

    async fn create_card_entity(&self, card: &Card) -> LarkResult<CardId> {
        let id = self.next("card");
        self.push(Frame::CardEntity {
            card_id: id.clone(),
            card: card.as_value().clone(),
        });
        Ok(CardId(id))
    }

    async fn start_stream(
        &self,
        _target: StreamTarget<'_>,
        card: Card,
        _element_id: &str,
    ) -> LarkResult<Box<dyn BridgeStream>> {
        let id = self.next("om_stream");
        self.push(Frame::StreamStart {
            message_id: id.clone(),
            card: card.as_value().clone(),
        });
        Ok(Box::new(FakeStream {
            message_id: id,
            transport: self.clone(),
            live: true,
        }))
    }
}

/// A streaming card handle that records frames instead of calling CardKit.
struct FakeStream {
    message_id: String,
    transport: FakeTransport,
    live: bool,
}

#[async_trait]
impl BridgeStream for FakeStream {
    fn message_id(&self) -> &str {
        &self.message_id
    }

    fn is_live(&self) -> bool {
        self.live
    }

    async fn set_content(&mut self, content: &str) -> LarkResult<bool> {
        if !self.live {
            return Ok(false);
        }
        self.transport.push(Frame::StreamContent {
            message_id: self.message_id.clone(),
            content: content.to_string(),
        });
        Ok(true)
    }

    async fn set_card(&mut self, card: &Card) -> LarkResult<bool> {
        if !self.live {
            return Ok(false);
        }
        self.transport.push(Frame::StreamCard {
            message_id: self.message_id.clone(),
            card: card.as_value().clone(),
        });
        Ok(true)
    }

    async fn finish(&mut self, content: &str) -> LarkResult<bool> {
        self.transport.push(Frame::StreamFinish {
            message_id: self.message_id.clone(),
            content: content.to_string(),
        });
        self.live = false;
        Ok(true)
    }
}

// ---------------------------------------------------------------------------
// Fakes for reactions and attachments
// ---------------------------------------------------------------------------

struct NoopReaction;

#[async_trait]
impl ReactionTransport for NoopReaction {
    async fn add(&self, _message_id: &str, _emoji_type: &str) -> RestResult<String> {
        Ok("re_fake".to_string())
    }

    async fn remove(&self, _message_id: &str, _reaction_id: &str) -> RestResult<()> {
        Ok(())
    }
}

struct FailingDownloader {
    calls: Arc<AtomicUsize>,
}

#[async_trait]
impl ResourceDownloader for FailingDownloader {
    async fn download(
        &self,
        _resource: &ResourceDescriptor,
    ) -> Result<DownloadedResource, MediaError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Err(MediaError::Download(
            "attachment download failed in test".into(),
        ))
    }
}

// ---------------------------------------------------------------------------
// Bridge / agent fixtures
// ---------------------------------------------------------------------------

fn card_profile() -> ProfileConfig {
    let mut profile = ProfileConfig::default();
    let preferences = profile.preferences.get_or_insert_with(Preferences::default);
    preferences.message_reply = Some(MessageReplyMode::Card);
    profile
}

fn markdown_profile() -> ProfileConfig {
    let mut profile = ProfileConfig::default();
    let preferences = profile.preferences.get_or_insert_with(Preferences::default);
    preferences.message_reply = Some(MessageReplyMode::Markdown);
    // The markdown progress card follows `showToolCalls`; turn it off to prove
    // the frames carry no tool content, footer element or button.
    preferences.show_tool_calls = Some(false);
    profile
}

fn test_bridge(
    root: &Path,
    profile: ProfileConfig,
    agent: PathBuf,
    downloader: Arc<dyn ResourceDownloader>,
    idle_unit: Option<Duration>,
) -> (Arc<Bridge>, FakeTransport) {
    test_bridge_with_safe_age(root, profile, agent, downloader, idle_unit, None, None)
}

fn test_bridge_with_safe_age(
    root: &Path,
    profile: ProfileConfig,
    agent: PathBuf,
    downloader: Arc<dyn ResourceDownloader>,
    idle_unit: Option<Duration>,
    card_safe_age: Option<Duration>,
    approval_timeout: Option<Duration>,
) -> (Arc<Bridge>, FakeTransport) {
    let paths = AppPaths::resolve(root, "test").expect("app paths");
    let media = MediaCache::new(paths.media_dir.clone(), downloader);
    let fake = FakeTransport::default();
    let transport: Arc<dyn BridgeTransport> = Arc::new(fake.clone());
    let mut bridge = Bridge::new_with_transport(
        transport,
        RestClient::new(TenantBrand::Feishu, "cli_test_app", "secret"),
        media,
        profile,
        agent,
        root.join("workspace"),
        None,
        None,
        paths,
        None,
    )
    .expect("bridge")
    .with_reaction_transport(Arc::new(NoopReaction));
    if let Some(unit) = idle_unit {
        bridge = bridge.with_idle_timeout_unit(unit);
    }
    if let Some(safe_age) = card_safe_age {
        bridge = bridge.with_card_safe_age(safe_age);
    }
    if let Some(timeout) = approval_timeout {
        bridge = bridge.with_approval_timeout(timeout);
    }
    (Arc::new(bridge), fake)
}

fn fake_agent(dir: &Path, body: &str) -> PathBuf {
    let path = dir.join("fake-claude");
    std::fs::write(&path, body).expect("fake agent script");
    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o755))
        .expect("fake agent executable");
    path
}

fn message(id: &str, chat_id: &str, text: &str) -> lark_channel::message::NormalizedMessage {
    serde_json::from_value(json!({
        "message_id": id,
        "chat_id": chat_id,
        "chat_type": "p2p",
        "sender_id": "ou_user",
        "sender": { "open_id": "ou_user", "sender_type": "user" },
        "text": text,
    }))
    .expect("normalized message fixture")
}

fn image_resource(message_id: &str) -> ResourceDescriptor {
    ResourceDescriptor {
        message_id: message_id.to_string(),
        resource_type: ResourceType::Image,
        file_key: None,
        image_key: Some("img_fake".to_string()),
        file_name: None,
        duration_ms: None,
    }
}

/// One Claude `stream-json` line for an assistant text block.
fn assistant_text(message_id: &str, text: &str) -> String {
    json!({
        "type": "assistant",
        "message": {
            "id": message_id,
            "content": [{ "type": "text", "text": text }],
        }
    })
    .to_string()
}

fn claude_init(session: &str) -> String {
    json!({
        "type": "system",
        "subtype": "init",
        "session_id": session,
        "cwd": "/work",
        "model": "fake",
    })
    .to_string()
}

// ---------------------------------------------------------------------------
// Waiting and frame helpers
// ---------------------------------------------------------------------------

/// Poll `predicate` until it holds, panicking with the observed frames after
/// `timeout`. Polling is cheaper and more debuggable than a sleep for a fixed
/// duration, and keeps the tests deterministic on slow CI.
async fn wait_for(
    transport: &FakeTransport,
    timeout: Duration,
    mut predicate: impl FnMut(&[Frame]) -> bool,
) {
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        let frames = transport.frames();
        if predicate(&frames) {
            return;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "timed out waiting for run-flow frame; observed {frames:#?}"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

fn stream_cards(frames: &[Frame]) -> Vec<Value> {
    frames
        .iter()
        .filter_map(|frame| match frame {
            Frame::StreamStart { card, .. } | Frame::StreamCard { card, .. } => Some(card.clone()),
            _ => None,
        })
        .collect()
}

fn stream_card_text(frames: &[Frame]) -> Vec<String> {
    stream_cards(frames)
        .iter()
        .map(|card| serde_json::to_string(card).expect("serialize card"))
        .collect()
}

fn last_stream_card(frames: &[Frame]) -> Value {
    stream_cards(frames)
        .last()
        .cloned()
        .expect("run produced a streaming card")
}

fn card_json(card: &Value) -> String {
    serde_json::to_string(card).expect("serialize card")
}

fn has_button(card: &Value) -> bool {
    let elements = card
        .pointer("/body/elements")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    elements.iter().any(|element| element["tag"] == "button")
        || card_json(card).contains("__bridge_cb")
}

fn element_tags(card: &Value) -> Vec<String> {
    card.pointer("/body/elements")
        .and_then(Value::as_array)
        .map(|elements| {
            elements
                .iter()
                .filter_map(|element| element["tag"].as_str().map(str::to_string))
                .collect()
        })
        .unwrap_or_default()
}

fn text_frames(frames: &[Frame]) -> Vec<String> {
    frames
        .iter()
        .filter_map(|frame| match frame {
            Frame::Text { text, .. } => Some(text.clone()),
            _ => None,
        })
        .collect()
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn happy_path_renders_running_card_tool_events_and_terminal_card() {
    let dir = tempfile::tempdir().unwrap();
    let body = format!(
        "#!/bin/sh\n\
         cat > /dev/null\n\
         printf '%s\\n' '{}'\n\
         printf '%s\\n' '{}'\n\
         printf '%s\\n' '{}'\n\
         printf '%s\\n' '{}'\n\
         printf '%s\\n' '{}'\n",
        claude_init("sess-happy"),
        assistant_text("m1", "working on it"),
        json!({
            "type": "assistant",
            "message": {
                "id": "m2",
                "content": [{
                    "type": "tool_use",
                    "id": "tool-1",
                    "name": "Bash",
                    "input": { "command": "pwd" }
                }]
            }
        }),
        json!({
            "type": "user",
            "message": {
                "content": [{
                    "type": "tool_result",
                    "tool_use_id": "tool-1",
                    "content": "/work"
                }]
            }
        }),
        assistant_text("m3", "hello from fake"),
    ) + &format!(
        "printf '%s\\n' '{}'\n",
        json!({
            "type": "result",
            "session_id": "sess-happy",
            "usage": { "input_tokens": 3, "output_tokens": 4 }
        })
    );
    let agent = fake_agent(dir.path(), &body);
    let calls = Arc::new(AtomicUsize::new(0));
    let (bridge, transport) = test_bridge(
        dir.path(),
        card_profile(),
        agent,
        Arc::new(FailingDownloader {
            calls: Arc::clone(&calls),
        }),
        None,
    );

    bridge
        .handle_message(message("om_run", "oc_flow", "hello"))
        .await;

    wait_for(&transport, Duration::from_secs(2), |frames| {
        stream_cards(frames)
            .last()
            .is_some_and(|card| card_json(card).contains("已完成"))
    })
    .await;

    let frames = transport.frames();
    let cards = stream_cards(&frames);
    assert!(cards.len() >= 2, "running + terminal frames: {frames:#?}");

    let running = &cards[0];
    assert!(
        card_json(running).contains("启动中"),
        "first frame is the running card: {running}"
    );
    assert!(
        has_button(running),
        "a live rich card carries the stop button: {running}"
    );

    let terminal = last_stream_card(&frames);
    assert!(
        card_json(&terminal).contains("hello from fake"),
        "the terminal card carries the final answer: {terminal}"
    );
    assert!(
        card_json(&terminal).contains("已完成"),
        "the terminal card reports the completion status: {terminal}"
    );
    assert!(
        card_json(&terminal).contains("Bash"),
        "the tool call is folded into the rich card: {terminal}"
    );
    assert!(
        !has_button(&terminal),
        "a finished card must not keep a live stop button: {terminal}"
    );
    assert_eq!(calls.load(Ordering::SeqCst), 0, "no attachment to download");
    assert!(
        transport.frames().iter().all(|frame| !matches!(
            frame,
            Frame::Text { text, .. } if text.contains("hello from fake")
        )),
        "the final answer stays in the card, not a separate text message"
    );
}

#[tokio::test]
async fn stop_finalizes_the_partial_answer_without_a_live_button() {
    let dir = tempfile::tempdir().unwrap();
    // The second event lands just after the 900 ms card-update throttle, so
    // the test can observe a frame containing the partial answer before it
    // sends `/stop`. The agent then stays silent until the run is interrupted.
    let body = format!(
        "#!/bin/sh\n\
         cat > /dev/null\n\
         printf '%s\\n' '{}'\n\
         printf '%s\\n' '{}'\n\
         sleep 1\n\
         printf '%s\\n' '{}'\n\
         sleep 30\n",
        claude_init("sess-stop"),
        assistant_text("m1", "partial answer"),
        assistant_text("m2", "still working"),
    );
    let agent = fake_agent(dir.path(), &body);
    let (bridge, transport) = test_bridge(
        dir.path(),
        card_profile(),
        agent,
        Arc::new(FailingDownloader {
            calls: Arc::new(AtomicUsize::new(0)),
        }),
        None,
    );

    bridge
        .handle_message(message("om_run", "oc_flow", "do some work"))
        .await;

    // Wait until the run loop has folded the partial text into a card frame.
    // That proves the partial answer lives in the view before the interrupt.
    wait_for(&transport, Duration::from_secs(3), |frames| {
        stream_cards(frames)
            .iter()
            .any(|card| card_json(card).contains("partial answer"))
    })
    .await;

    // `/stop` arrives as a second message in the same scope.
    bridge
        .handle_message(message("om_stop", "oc_flow", "/stop"))
        .await;

    wait_for(&transport, Duration::from_secs(2), |frames| {
        stream_cards(frames)
            .last()
            .is_some_and(|card| card_json(card).contains("已被中断"))
    })
    .await;

    let frames = transport.frames();
    let terminal = last_stream_card(&frames);
    assert!(
        card_json(&terminal).contains("partial answer"),
        "the interrupted terminal card keeps the partial answer: {terminal}"
    );
    assert!(
        card_json(&terminal).contains("已被中断"),
        "the terminal card renders the stop notice: {terminal}"
    );
    assert!(
        !has_button(&terminal),
        "an interrupted card has no live stop button: {terminal}"
    );
    assert_eq!(
        terminal
            .pointer("/config/summary/content")
            .and_then(Value::as_str),
        Some("已中断"),
        "the terminal summary says the run was interrupted"
    );
    assert!(
        text_frames(&frames)
            .iter()
            .any(|text| text.contains("已停止当前任务")),
        "the `/stop` command itself replies: {:?}",
        text_frames(&frames)
    );
}

#[tokio::test]
async fn idle_timeout_stops_a_silent_agent_with_the_timeout_notice() {
    let dir = tempfile::tempdir().unwrap();
    let agent = fake_agent(dir.path(), "#!/bin/sh\ncat > /dev/null\nsleep 30\n");
    let (bridge, transport) = test_bridge(
        dir.path(),
        card_profile(),
        agent,
        Arc::new(FailingDownloader {
            calls: Arc::new(AtomicUsize::new(0)),
        }),
        // One configured minute is 40 ms in this test, so the `/timeout 1`
        // override below expires deterministically without a real sleep.
        Some(Duration::from_millis(40)),
    );

    // Set the per-scope override through the real command path, then run.
    bridge
        .handle_message(message("om_timeout", "oc_flow", "/timeout 1"))
        .await;
    bridge
        .handle_message(message("om_run", "oc_flow", "stay silent"))
        .await;

    wait_for(&transport, Duration::from_secs(2), |frames| {
        stream_cards(frames)
            .last()
            .is_some_and(|card| card_json(card).contains("无响应"))
    })
    .await;

    let frames = transport.frames();
    let terminal = last_stream_card(&frames);
    let terminal_json = card_json(&terminal);
    assert!(
        terminal_json.contains("1 分钟无响应"),
        "the notice reports the configured scope override: {terminal}"
    );
    assert!(
        terminal_json.contains("已自动终止"),
        "the notice says the run was stopped automatically: {terminal}"
    );
    assert!(
        !has_button(&terminal),
        "a timed-out card has no live stop button: {terminal}"
    );
    assert_eq!(
        terminal
            .pointer("/config/summary/content")
            .and_then(Value::as_str),
        Some("已超时"),
        "the terminal summary says the run timed out"
    );
}

#[tokio::test]
async fn markdown_mode_frames_have_no_tools_footer_or_button() {
    let dir = tempfile::tempdir().unwrap();
    let body = format!(
        "#!/bin/sh\n\
         cat > /dev/null\n\
         printf '%s\\n' '{}'\n\
         printf '%s\\n' '{}'\n\
         printf '%s\\n' '{}'\n",
        claude_init("sess-markdown"),
        assistant_text("m1", "markdown answer"),
        json!({
            "type": "assistant",
            "message": {
                "id": "m2",
                "content": [{
                    "type": "tool_use",
                    "id": "tool-1",
                    "name": "Bash",
                    "input": { "command": "pwd" }
                }]
            }
        }),
    ) + &format!(
        "printf '%s\\n' '{}'\n",
        json!({
            "type": "result",
            "session_id": "sess-markdown",
            "usage": { "input_tokens": 1, "output_tokens": 2 }
        })
    );
    let agent = fake_agent(dir.path(), &body);
    let (bridge, transport) = test_bridge(
        dir.path(),
        markdown_profile(),
        agent,
        Arc::new(FailingDownloader {
            calls: Arc::new(AtomicUsize::new(0)),
        }),
        None,
    );

    bridge
        .handle_message(message("om_md", "oc_flow", "markdown please"))
        .await;

    wait_for(&transport, Duration::from_secs(2), |frames| {
        frames
            .iter()
            .any(|frame| matches!(frame, Frame::StreamFinish { .. }))
    })
    .await;

    let frames = transport.frames();
    for card in stream_cards(&frames) {
        assert_eq!(
            element_tags(&card),
            vec!["markdown".to_string()],
            "a markdown card is one markdown element: {card}"
        );
        assert!(
            !has_button(&card),
            "markdown frames never carry a stop button: {card}"
        );
        assert!(
            !card_json(&card).contains("Bash"),
            "showToolCalls=false keeps tool summaries out of the frame: {card}"
        );
    }

    let finish = frames
        .iter()
        .find_map(|frame| match frame {
            Frame::StreamFinish { content, .. } => Some(content.clone()),
            _ => None,
        })
        .expect("markdown run finishes the streaming card");
    assert!(
        finish.contains("markdown answer"),
        "the final markdown body carries the answer: {finish}"
    );
    assert!(
        !finish.contains("Bash"),
        "the final markdown body has no tool summary: {finish}"
    );
    // No rich-card footer element, metadata note or tool panel survives in
    // markdown mode. The single markdown element may still carry the text
    // renderer's running status phrase; that is body text, not a footer
    // element (matches the TS renderer).
    for text in stream_card_text(&frames)
        .iter()
        .chain(std::iter::once(&finish))
    {
        assert!(
            !text.contains("\"tag\":\"note\"")
                && !text.contains("Claude Code ·")
                && !text.contains("collapsible_panel"),
            "markdown frames carry no footer element or metadata note: {text}"
        );
    }
    assert!(
        !frames.iter().any(|frame| matches!(
            frame,
            Frame::Text { .. } | Frame::Markdown { .. } | Frame::Card { .. }
        )),
        "markdown mode delivers the answer through the streaming card only: {frames:#?}"
    );
}

#[tokio::test]
async fn attachment_download_failure_does_not_fail_the_run() {
    let dir = tempfile::tempdir().unwrap();
    let body = format!(
        "#!/bin/sh\n\
         cat > /dev/null\n\
         printf '%s\\n' '{}'\n\
         printf '%s\\n' '{}'\n",
        claude_init("sess-attachment"),
        assistant_text("m1", "answer despite the failed attachment"),
    ) + &format!(
        "printf '%s\\n' '{}'\n",
        json!({
            "type": "result",
            "session_id": "sess-attachment",
            "usage": { "input_tokens": 1, "output_tokens": 2 }
        })
    );
    let agent = fake_agent(dir.path(), &body);
    let calls = Arc::new(AtomicUsize::new(0));
    let (bridge, transport) = test_bridge(
        dir.path(),
        card_profile(),
        agent,
        Arc::new(FailingDownloader {
            calls: Arc::clone(&calls),
        }),
        None,
    );

    let mut inbound = message("om_image", "oc_flow", "look at this");
    inbound.resources = vec![image_resource("om_image")];
    bridge.handle_message(inbound).await;

    wait_for(&transport, Duration::from_secs(2), |frames| {
        stream_cards(frames)
            .last()
            .is_some_and(|card| card_json(card).contains("已完成"))
    })
    .await;

    assert_eq!(
        calls.load(Ordering::SeqCst),
        1,
        "the bridge attempted the attachment download"
    );
    let terminal = last_stream_card(&transport.frames());
    assert!(
        card_json(&terminal).contains("answer despite the failed attachment"),
        "the failed download does not fail the run: {terminal}"
    );
    assert!(
        !card_json(&terminal).contains("失败"),
        "the failed optional attachment is not reported as a run failure: {terminal}"
    );
    assert!(
        !has_button(&terminal),
        "the run finalized normally: {terminal}"
    );
}

/// A run that outlives the progress card's safe age gets a second card; the
/// old card is finalized with a pointer and the answer lands on the new one.
#[tokio::test]
async fn long_runs_roll_over_to_a_second_card_and_finish_there() {
    let dir = tempfile::tempdir().unwrap();
    let body = format!(
        "#!/bin/sh\n\
         cat > /dev/null\n\
         printf '%s\\n' '{}'\n\
         printf '%s\\n' '{}'\n\
         sleep 0.5\n\
         printf '%s\\n' '{}'\n\
         printf '%s\\n' '{}'\n",
        claude_init("sess-roll"),
        assistant_text("m1", "before rollover"),
        assistant_text("m2", "after rollover"),
        json!({
            "type": "result",
            "session_id": "sess-roll",
            "usage": { "input_tokens": 1, "output_tokens": 2 }
        })
    );
    let agent = fake_agent(dir.path(), &body);
    let calls = Arc::new(AtomicUsize::new(0));
    let (bridge, transport) = test_bridge_with_safe_age(
        dir.path(),
        card_profile(),
        agent,
        Arc::new(FailingDownloader {
            calls: Arc::clone(&calls),
        }),
        None,
        Some(Duration::from_millis(120)),
        None,
    );

    bridge
        .handle_message(message("om_roll", "oc_flow", "go"))
        .await;

    wait_for(&transport, Duration::from_secs(4), |frames| {
        let starts = frames
            .iter()
            .filter(|frame| matches!(frame, Frame::StreamStart { .. }))
            .count();
        starts >= 2
            && frames.iter().any(|frame| {
                matches!(
                    frame,
                    Frame::StreamCard { card, .. } if card_json(card).contains("已完成")
                )
            })
    })
    .await;

    let frames = transport.frames();
    let starts: Vec<String> = frames
        .iter()
        .filter_map(|frame| match frame {
            Frame::StreamStart { message_id, .. } => Some(message_id.clone()),
            _ => None,
        })
        .collect();
    assert!(
        starts.len() >= 2,
        "expected a continuation card, got {starts:?}"
    );
    let head_id = &starts[0];
    let tail_id = starts.last().expect("tail card");

    let head_last = frames
        .iter()
        .filter_map(|frame| match frame {
            Frame::StreamCard { message_id, card } if message_id == head_id => Some(card),
            _ => None,
        })
        .next_back()
        .expect("head card got a final frame");
    let head_json = card_json(head_last);
    assert!(
        head_json.contains("接续到下一张卡片"),
        "head card points at the continuation: {head_json}"
    );
    assert!(
        !has_button(head_last),
        "the rolled-over head card has no live stop button: {head_json}"
    );

    let tail_text = frames
        .iter()
        .filter_map(|frame| match frame {
            Frame::StreamStart { message_id, card } | Frame::StreamCard { message_id, card }
                if message_id == tail_id =>
            {
                Some(card_json(card))
            }
            _ => None,
        })
        .collect::<Vec<_>>()
        .join("\n");
    assert!(
        tail_text.contains("接续上一张卡片"),
        "continuation card says what it is: {tail_text}"
    );
    assert!(
        tail_text.contains("after rollover"),
        "continuation card streams the tail: {tail_text}"
    );
    assert!(
        tail_text.contains("before rollover") && tail_text.contains("after rollover"),
        "the final card carries the complete answer: {tail_text}"
    );

    assert!(
        !frames
            .iter()
            .any(|frame| matches!(frame, Frame::Card { .. } | Frame::ReplyCard { .. })),
        "a rolled-over run finishes on its last card instead of sending a separate one"
    );
}

fn card_click(value: Value, message_id: &str, chat_id: &str, operator: &str) -> CardActionEvent {
    CardActionEvent {
        context: EventContext {
            event_id: "evt_appr".to_string(),
            tenant_key: None,
            create_time: None,
        },
        operator: CardActionOperator {
            tenant_key: None,
            user_id: None,
            open_id: Some(operator.to_string()),
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
            open_message_id: Some(message_id.to_string()),
            open_chat_id: Some(chat_id.to_string()),
            raw: Value::Null,
        }),
        raw: Value::Null,
    }
}

/// A workspace/`default` profile routes Claude's tool prompt to an approval
/// card; allowing it resumes the run and records the control response.
#[tokio::test]
async fn approval_card_allows_a_blocked_tool_and_resumes_the_run() {
    let dir = tempfile::tempdir().unwrap();
    let body = r#"#!/bin/sh
read prompt
printf '%s\n' '{"type":"system","subtype":"init","session_id":"sess-appr"}'
printf '%s\n' '{"type":"assistant","message":{"id":"m1","content":[{"type":"tool_use","id":"call-1","name":"Bash","input":{"command":"pwd"}}]}}'
printf '%s\n' '{"type":"control_request","request_id":"req-1","request":{"subtype":"can_use_tool","tool_name":"Bash","display_name":"Bash","input":{"command":"pwd"},"description":"print cwd","tool_use_id":"call-1"}}'
read response
printf '%s\n' "$response" > "$(dirname "$0")/decision.json"
printf '%s\n' '{"type":"assistant","message":{"id":"m2","content":[{"type":"text","text":"done after approval"}]}}'
printf '%s\n' '{"type":"result","session_id":"sess-appr"}'
"#;
    let agent = fake_agent(dir.path(), &body);
    let mut profile = card_profile();
    profile.extra.insert(
        "permissions".into(),
        json!({
            "defaultAccess": "workspace",
            "maxAccess": "workspace",
            "claude": { "permissionMode": "default" }
        }),
    );
    let (bridge, transport) = test_bridge(
        dir.path(),
        profile,
        agent,
        Arc::new(FailingDownloader {
            calls: Arc::new(AtomicUsize::new(0)),
        }),
        None,
    );

    bridge
        .handle_message(message("om_appr", "oc_flow", "run a command"))
        .await;

    // The approval card is a standalone normal card message.
    wait_for(&transport, Duration::from_secs(4), |frames| {
        frames.iter().any(|frame| {
            matches!(
                frame,
                Frame::Card { card, .. } if card_json(card).contains("approval:allow:req-1")
            )
        })
    })
    .await;
    let (approval_message_id, approval_card) = transport
        .frames()
        .into_iter()
        .find_map(|frame| match frame {
            Frame::Card {
                message_id, card, ..
            } if card_json(&card).contains("approval:allow:req-1") => Some((message_id, card)),
            _ => None,
        })
        .expect("approval card");
    let allow_button = approval_card["body"]["elements"]
        .as_array()
        .expect("elements")
        .iter()
        .find(|element| {
            element["tag"] == "button" && element["behaviors"][0]["value"]["decision"] == "allow"
        })
        .expect("allow button");
    let button_value = allow_button["behaviors"][0]["value"].clone();
    assert_eq!(button_value["operator"], "ou_user");
    assert_eq!(button_value["request_id"], "req-1");
    assert!(
        button_value["token"]
            .as_str()
            .is_some_and(|token| !token.is_empty())
    );

    let other_event = card_click(
        button_value.clone(),
        &approval_message_id,
        "oc_flow",
        "ou_someone_else",
    );
    assert_eq!(
        bridge.handle_card_action(&other_event).await.as_deref(),
        Some("只有发起本次任务的人可以处理授权。")
    );

    let event = card_click(button_value, &approval_message_id, "oc_flow", "ou_user");
    assert_eq!(
        bridge.handle_card_action(&event).await.as_deref(),
        Some("已允许，继续执行。")
    );

    wait_for(&transport, Duration::from_secs(4), |frames| {
        frames.iter().any(|frame| {
            matches!(
                frame,
                Frame::UpdateCard { message_id, card }
                    if message_id == &approval_message_id
                        && card_json(card).contains("已允许")
            )
        })
    })
    .await;
    wait_for(&transport, Duration::from_secs(4), |frames| {
        stream_cards(frames)
            .last()
            .is_some_and(|card| card_json(card).contains("done after approval"))
    })
    .await;

    let decision = std::fs::read_to_string(dir.path().join("decision.json"))
        .expect("the fake agent recorded the control response");
    assert!(decision.contains("control_response"), "{decision}");
    assert!(decision.contains("\"behavior\":\"allow\""), "{decision}");
    assert!(decision.contains("updatedInput"), "{decision}");

    let updated = transport
        .frames()
        .into_iter()
        .filter_map(|frame| match frame {
            Frame::UpdateCard { message_id, card } if message_id == approval_message_id => {
                Some(card)
            }
            _ => None,
        })
        .next_back()
        .expect("the approval card was updated");
    assert!(
        !card_json(&updated).contains("__bridge_cb"),
        "the decided card has no live buttons: {updated}"
    );
}

#[tokio::test]
async fn approval_times_out_and_denies_the_tool() {
    let dir = tempfile::tempdir().unwrap();
    let body = r#"#!/bin/sh
read prompt
printf '%s\n' '{"type":"system","subtype":"init","session_id":"sess-timeout"}'
printf '%s\n' '{"type":"assistant","message":{"id":"m1","content":[{"type":"tool_use","id":"call-1","name":"Bash","input":{"command":"rm -rf /tmp/x"}}]}}'
printf '%s\n' '{"type":"control_request","request_id":"req-timeout","request":{"subtype":"can_use_tool","tool_name":"Bash","input":{"command":"rm -rf /tmp/x"}}}'
read response
printf '%s\n' "$response" > "$(dirname "$0")/timeout-decision.json"
printf '%s\n' '{"type":"assistant","message":{"id":"m2","content":[{"type":"text","text":"denied by timeout"}]}}'
printf '%s\n' '{"type":"result","session_id":"sess-timeout"}'
"#;
    let agent = fake_agent(dir.path(), body);
    let mut profile = card_profile();
    profile.extra.insert(
        "permissions".into(),
        json!({
            "defaultAccess": "workspace",
            "maxAccess": "workspace",
            "claude": { "permissionMode": "default" }
        }),
    );
    let (bridge, transport) = test_bridge_with_safe_age(
        dir.path(),
        profile,
        agent,
        Arc::new(FailingDownloader {
            calls: Arc::new(AtomicUsize::new(0)),
        }),
        None,
        None,
        Some(Duration::from_millis(150)),
    );

    bridge
        .handle_message(message("om_timeout", "oc_flow", "clean up"))
        .await;

    wait_for(&transport, Duration::from_secs(4), |frames| {
        frames.iter().any(|frame| {
            matches!(
                frame,
                Frame::Card { card, .. } if card_json(card).contains("approval:allow:req-timeout")
            )
        })
    })
    .await;
    let approval_message_id = transport
        .frames()
        .into_iter()
        .find_map(|frame| match frame {
            Frame::Card {
                message_id, card, ..
            } if card_json(&card).contains("approval:allow:req-timeout") => Some(message_id),
            _ => None,
        })
        .expect("approval card");

    wait_for(&transport, Duration::from_secs(4), |frames| {
        frames.iter().any(|frame| {
            matches!(
                frame,
                Frame::UpdateCard { message_id, card }
                    if message_id == &approval_message_id
                        && card_json(card).contains("超时自动拒绝")
            )
        })
    })
    .await;
    wait_for(&transport, Duration::from_secs(4), |frames| {
        stream_cards(frames)
            .last()
            .is_some_and(|card| card_json(card).contains("denied by timeout"))
    })
    .await;

    let decision = std::fs::read_to_string(dir.path().join("timeout-decision.json"))
        .expect("the fake agent recorded the timeout denial");
    assert!(decision.contains("\"behavior\":\"deny\""), "{decision}");
    assert!(decision.contains("审批超时"), "{decision}");
}

/// Codex under workspace-write uses `codex app-server`; its command approval
/// request becomes the same approval card and the decision resumes the turn.
#[tokio::test]
async fn codex_app_server_approval_card_resumes_the_turn() {
    let dir = tempfile::tempdir().unwrap();
    let body = r#"#!/usr/bin/env python3
import json
import os
import sys

here = os.path.dirname(os.path.abspath(__file__))
def send(obj):
    sys.stdout.write(json.dumps(obj, separators=(",", ":")) + "\n")
    sys.stdout.flush()

for line in sys.stdin:
    msg = json.loads(line)
    method = msg.get("method")
    req_id = msg.get("id")
    if method == "initialize":
        send({"id": req_id, "result": {"userAgent": "fake"}})
    elif method == "thread/start":
        send({"id": req_id, "result": {"thread": {"id": "thr_codex"}}})
    elif method == "turn/start":
        send({"id": req_id, "result": {"turn": {"id": "turn_1", "status": "inProgress", "items": []}}})
        send({"method": "item/started", "params": {"threadId": "thr_codex", "turnId": "turn_1", "item": {
            "id": "cmd_1", "type": "commandExecution", "command": "pwd", "cwd": "/work",
            "commandActions": [], "status": "inProgress"}}})
        send({"id": 77, "method": "item/commandExecution/requestApproval", "params": {
            "threadId": "thr_codex", "turnId": "turn_1", "itemId": "cmd_1",
            "command": "pwd", "cwd": "/work", "reason": "test approval"}})
    elif req_id == 77:
        with open(os.path.join(here, "codex-decision.json"), "w") as handle:
            json.dump(msg, handle)
        send({"method": "item/completed", "params": {"threadId": "thr_codex", "turnId": "turn_1", "item": {
            "id": "cmd_1", "type": "commandExecution", "command": "pwd", "cwd": "/work",
            "commandActions": [], "status": "completed", "aggregatedOutput": "/work\n", "exitCode": 0}}})
        send({"method": "item/agentMessage/delta", "params": {"threadId": "thr_codex", "turnId": "turn_1", "itemId": "m", "delta": "done after codex approval"}})
        send({"method": "item/completed", "params": {"threadId": "thr_codex", "turnId": "turn_1", "item": {
            "id": "m", "type": "agentMessage", "text": "done after codex approval"}}})
        send({"method": "turn/completed", "params": {"threadId": "thr_codex", "turn": {"id": "turn_1", "status": "completed", "items": []}}})
"#;
    let agent = fake_agent(dir.path(), body);
    let mut profile = card_profile();
    profile.agent_kind = AgentKind::Codex;
    profile.extra.insert(
        "permissions".into(),
        json!({
            "defaultAccess": "workspace",
            "maxAccess": "workspace"
        }),
    );
    let (bridge, transport) = test_bridge(
        dir.path(),
        profile,
        agent,
        Arc::new(FailingDownloader {
            calls: Arc::new(AtomicUsize::new(0)),
        }),
        None,
    );

    bridge
        .handle_message(message("om_codex", "oc_flow", "run pwd"))
        .await;

    wait_for(&transport, Duration::from_secs(4), |frames| {
        frames.iter().any(|frame| {
            matches!(
                frame,
                Frame::Card { card, .. } if card_json(card).contains("approval:allow:77")
            )
        })
    })
    .await;
    let (approval_message_id, approval_card) = transport
        .frames()
        .into_iter()
        .find_map(|frame| match frame {
            Frame::Card {
                message_id, card, ..
            } if card_json(&card).contains("approval:allow:77") => Some((message_id, card)),
            _ => None,
        })
        .expect("approval card");
    let button = approval_card["body"]["elements"]
        .as_array()
        .expect("elements")
        .iter()
        .find(|element| {
            element["tag"] == "button" && element["behaviors"][0]["value"]["decision"] == "allow"
        })
        .expect("allow button");
    let event = card_click(
        button["behaviors"][0]["value"].clone(),
        &approval_message_id,
        "oc_flow",
        "ou_user",
    );
    assert_eq!(
        bridge.handle_card_action(&event).await.as_deref(),
        Some("已允许，继续执行。")
    );

    wait_for(&transport, Duration::from_secs(4), |frames| {
        stream_cards(frames)
            .last()
            .is_some_and(|card| card_json(card).contains("done after codex approval"))
    })
    .await;
    let decision = std::fs::read_to_string(dir.path().join("codex-decision.json"))
        .expect("the fake app-server recorded the decision");
    assert!(
        decision.contains("\"decision\": \"accept\"")
            || decision.contains("\"decision\":\"accept\""),
        "{decision}"
    );
}
