//! In-meeting agent (VC bot) — port of `src/meeting/*` and the `/meeting`
//! command from `src/commands/index.ts`.
//!
//! The bot joins a Feishu video meeting as a real participant (path 2 / TAT),
//! receives the live transcript, in-meeting chat, participant and doc-share
//! activity, and can answer either inside the meeting or over IM. The transcript
//! is **context only**: a run needs an explicit ask (in-meeting chat prefixed
//! with the configured trigger, or `/meeting ask`/`notes` from IM), because
//! acting on an offhand remark would be expensive and unsafe.
//!
//! Agent runs (answers, `/meeting ask|notes`, end-of-meeting summaries) share
//! the bridge's global run limiter with IM and comment runs. A run waits up
//! to a bounded budget for a slot and reports a user-visible timeout instead
//! of spawning the agent over the cap.
//!
//! Every meeting run is registered under `meeting:<meeting_id>` in the
//! bridge's active-run map through [`MeetingHost::spawn_tracked_run`], and in
//! the manager's own `runs` map. `/meeting stop`, the in-meeting stop trigger
//! and an admin `/stop meeting:<meeting_id>` therefore abort the same task;
//! aborting drops the run future, which releases the global permit, kills the
//! agent child (`kill_on_drop`) and removes both map entries through their
//! guards. Meeting runs have no stop button of their own.
//!
//! # Configuration
//!
//! The Rust profile schema is deliberately closed and keeps unknown keys in
//! `profile.extra`, so the whole `meeting` object from the TypeScript schema is
//! read from `profile.extra["meeting"]` (camelCase, same keys as
//! `src/config/profile-schema.ts`). Defaults mirror `MEETING_DEFAULTS`:
//!
//! | key | default | meaning |
//! | --- | --- | --- |
//! | `enabled` | `false` | opt in to the (allowlisted-beta) capability |
//! | `autoJoinOnInvite` | `false` | join when a `meeting_invited` push arrives |
//! | `transcript.keep` | `200` | rolling transcript lines kept as context |
//! | `transcript.stabilizeMs` | `0` | debounce before a sentence is final |
//! | `respondIn` | `"meeting"` | `meeting` \| `im` \| `both` |
//! | `trigger` | `"@bot"` | chat prefix that asks the agent (plus `@<botName>`) |
//! | `pollIntervalMs` | `3000` | base `bots/events` poll interval |
//! | `summaryOnEnd` | `false` | summarize to IM when the meeting ends |
//! | `summaryTarget` | `"origin"` | `origin` (chat that ran `/meeting join`) \| `owner` |
//!
//! Rust-only keys (the TypeScript bridge resolves these from the SDK instead):
//!
//! * `botOpenId` / `botName` — overrides for the bot identity. `lark-channel`
//!   0.6 has no bot-identity accessor and the CLI constructs the bridge with
//!   `None`, so without these the transcript self-echo filter and the
//!   `@<botName>` trigger cannot work.
//! * `summaryOwnerOpenId` — the owner DM target for `summaryTarget: "owner"`.
//!   The runtime app owner resolved for `/meeting` command gating
//!   ([`crate::owner`]) is not yet handed to the meeting manager, so the
//!   summary target needs this explicit open id; when it is absent, `owner`
//!   falls back to `origin` exactly like the TypeScript
//!   `resolveSummaryTarget` fallback.
//!
//! The manager snapshots this config when it starts (`Bridge::start_meeting`).
//! The bridge `/config set` only covers run preferences, so a meeting-config
//! change takes effect after a restart; the TypeScript bridge re-reads the
//! profile on every use instead.
//!
//! # Event sources
//!
//! * **Poll**: `GET /open-apis/vc/v1/bots/events` on a per-meeting schedule
//!   with exponential backoff (idle rounds 2x, capped at 10s).
//! * **Push**: `vc.bot.meeting_{invited,activity,ended}_v1` frames arrive as
//!   `ChannelEvent::Unknown`; [`parse_meeting_push`] extracts them and the CLI
//!   hands them to `Bridge::handle_meeting_event`. Once push is observed for
//!   a meeting, polling for it stops (as in the TypeScript bridge) — the
//!   `event_id` dedupe covers the overlap window.
//!
//! Everything that decides *what* happens is pure and offline-testable: event
//! unpacking, de-duplication, transcript stabilization, backoff, trigger
//! matching, prompt building, target resolution and command parsing. The
//! network lives behind [`MeetingApi`] and the bridge integration behind
//! [`MeetingHost`], both faked in the tests below.

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet, VecDeque};
use std::future::Future;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex as StdMutex};
use std::time::Duration;

use async_trait::async_trait;
use larkline_agent::{AgentEvent, TerminationReason};
use larkline_core::{AgentKind, ProfileConfig};
use larkline_lark::{
    EventsPage, JoinResult, MeetingActivityItem, RestClient, RestError, RestResult, VcBotClient,
    is_not_in_gray,
};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use tokio::sync::{Notify, oneshot, watch};
use tokio::task::JoinHandle;

use crate::agent_runner::{AgentProcess, AgentSpec};
use crate::permissions::RunPermissions;
use crate::render::RunView;
use crate::run::{RUN_SLOT_TIMEOUT_REPLY, RUN_SLOT_WAIT, RUN_TIMEOUT};
use crate::run_limit::RunLimiter;

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/// Key under `profile.extra` that carries the meeting config object.
pub const MEETING_CONFIG_KEY: &str = "meeting";
/// Default trigger prefix (`MEETING_DEFAULTS.trigger`).
pub const DEFAULT_TRIGGER: &str = "@bot";
/// Default base poll interval in milliseconds.
pub const DEFAULT_POLL_INTERVAL_MS: u64 = 3_000;
/// Backoff ceiling for idle/error poll rounds.
pub const MAX_POLL_BACKOFF_MS: u64 = 10_000;
/// Page size for `bots/events`.
pub const EVENTS_PAGE_SIZE: u32 = 100;

/// Early-bird group to request the allowlisted beta (error 20017).
pub const MEETING_BETA_CHAT_URL: &str =
    "https://go.larkoffice.com/join-chat/2f4nb0e1-fe00-4f67-bed7-25beaf533fbd";

/// App-identity scopes the feature needs (`src/meeting/preflight.ts`).
pub const MEETING_REQUIRED_SCOPES: [(&str, &str); 3] = [
    ("vc:meeting.bot.join:write", "让 bot 入会 / 离会"),
    ("vc:meeting.message:write", "在会中发消息（回答会发不出去）"),
    (
        "vc:meeting.meetingevent:read",
        "读会中事件（字幕/弹幕/进退会）",
    ),
];

/// Events the app must subscribe to in the console for push to arrive.
pub const MEETING_REQUIRED_EVENTS: [&str; 3] = [
    "vc.bot.meeting_invited_v1",
    "vc.bot.meeting_activity_v1",
    "vc.bot.meeting_ended_v1",
];

/// Words that mean "stop the current run" when sent after the trigger.
const STOP_WORDS: [&str; 7] = ["stop", "停", "停止", "中断", "取消", "cancel", "abort"];

/// Reply used for a denied sensitive command (`canRunAdminCommand` in TS).
pub const MEETING_DENIED_REPLY: &str = "❌ 此命令仅管理员可用。";
/// Reply when the capability is switched off for the profile.
pub const MEETING_DISABLED_REPLY: &str =
    "会议智能体未启用。在 profile 的 extra.meeting.enabled 设为 true 后重启 bot 即可。";
/// Reply when enabled but the manager is not running.
pub const MEETING_NOT_READY_REPLY: &str = "会议能力未就绪（channel 未连接或启用后尚未重启）。";

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

/// Where the agent's answer goes when it responds to meeting content.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum RespondIn {
    #[default]
    Meeting,
    Im,
    Both,
}

impl RespondIn {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Meeting => "meeting",
            Self::Im => "im",
            Self::Both => "both",
        }
    }

    fn parse(value: Option<&str>) -> Self {
        match value {
            Some("im") => Self::Im,
            Some("both") => Self::Both,
            _ => Self::Meeting,
        }
    }
}

/// Where the end-of-meeting summary is delivered.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum SummaryTarget {
    #[default]
    Origin,
    Owner,
}

impl SummaryTarget {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Origin => "origin",
            Self::Owner => "owner",
        }
    }

    fn parse(value: Option<&str>) -> Self {
        match value {
            Some("owner") => Self::Owner,
            _ => Self::Origin,
        }
    }
}

/// `meeting.transcript` sub-object.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MeetingTranscriptConfig {
    /// Rolling transcript lines kept as agent context (10..=2000).
    pub keep: usize,
    /// Debounce window before a sentence counts as final; `0` = emit every
    /// update immediately.
    pub stabilize_ms: u64,
}

impl Default for MeetingTranscriptConfig {
    fn default() -> Self {
        Self {
            keep: 200,
            stabilize_ms: 0,
        }
    }
}

/// In-meeting agent settings, read from `profile.extra["meeting"]`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, rename_all = "camelCase")]
pub struct MeetingConfig {
    pub enabled: bool,
    pub auto_join_on_invite: bool,
    pub transcript: MeetingTranscriptConfig,
    pub respond_in: RespondIn,
    pub trigger: String,
    pub poll_interval_ms: u64,
    pub summary_on_end: bool,
    pub summary_target: SummaryTarget,
    /// Rust-only override for the bot open id (self-echo filtering).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub bot_open_id: Option<String>,
    /// Rust-only override for the bot display name (`@<botName>` trigger).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub bot_name: Option<String>,
    /// Rust-only owner DM target for `summaryTarget: "owner"`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub summary_owner_open_id: Option<String>,
}

impl Default for MeetingConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            auto_join_on_invite: false,
            transcript: MeetingTranscriptConfig::default(),
            respond_in: RespondIn::Meeting,
            trigger: DEFAULT_TRIGGER.to_string(),
            poll_interval_ms: DEFAULT_POLL_INTERVAL_MS,
            summary_on_end: false,
            summary_target: SummaryTarget::Origin,
            bot_open_id: None,
            bot_name: None,
            summary_owner_open_id: None,
        }
    }
}

impl MeetingConfig {
    /// Parse and normalize the `meeting` object. Mirrors `normalizeMeeting`:
    /// missing/mistyped fields fall back to defaults, numbers are clamped.
    pub fn from_value(value: Option<&Value>) -> Self {
        let empty = Map::new();
        let raw = value.and_then(Value::as_object).unwrap_or(&empty);
        let transcript = raw.get("transcript").and_then(Value::as_object);
        let defaults = Self::default();

        let transcript = MeetingTranscriptConfig {
            keep: clamp_number(
                transcript.and_then(|t| t.get("keep")),
                10,
                2_000,
                defaults.transcript.keep as u64,
            ) as usize,
            stabilize_ms: clamp_number(
                transcript.and_then(|t| t.get("stabilizeMs")),
                0,
                30_000,
                defaults.transcript.stabilize_ms,
            ),
        };

        Self {
            enabled: bool_field(raw, "enabled"),
            auto_join_on_invite: bool_field(raw, "autoJoinOnInvite"),
            transcript,
            respond_in: RespondIn::parse(raw.get("respondIn").and_then(Value::as_str)),
            trigger: string_field(raw, "trigger").unwrap_or(defaults.trigger),
            poll_interval_ms: clamp_number(
                raw.get("pollIntervalMs"),
                1_000,
                60_000,
                defaults.poll_interval_ms,
            ),
            summary_on_end: bool_field(raw, "summaryOnEnd"),
            summary_target: SummaryTarget::parse(raw.get("summaryTarget").and_then(Value::as_str)),
            bot_open_id: string_field(raw, "botOpenId"),
            bot_name: string_field(raw, "botName"),
            summary_owner_open_id: string_field(raw, "summaryOwnerOpenId"),
        }
    }
}

/// Read the effective meeting config from a profile's `extra` map.
pub fn config_from_profile(profile: &ProfileConfig) -> MeetingConfig {
    MeetingConfig::from_value(profile.extra.get(MEETING_CONFIG_KEY))
}

/// Whether `/meeting` is enabled for this profile.
pub fn meeting_enabled(profile: &ProfileConfig) -> bool {
    config_from_profile(profile).enabled
}

/// Static-list admin gate for `/meeting`, the TypeScript
/// `canRunAdminCommand` subset *without* the runtime owner.
///
/// The bridge's command handlers call
/// [`crate::owner::can_run_admin_command`] with the resolved owner, so they
/// also accept the app owner. This helper keeps the pre-owner behaviour for
/// callers/tests that only have a profile: a configured admin always passes;
/// with an empty admin list a sender must be in `access.allowedUsers`
/// (mirroring the personal-mode allowlist). With both lists empty the command
/// fails closed, which is the safe default for a capability that exposes the
/// bot to every participant.
pub fn can_run_meeting_command(profile: &ProfileConfig, sender_id: &str) -> bool {
    crate::owner::can_run_admin_command(profile, None, sender_id)
}

fn bool_field(object: &Map<String, Value>, key: &str) -> bool {
    object.get(key).and_then(Value::as_bool) == Some(true)
}

fn string_field(object: &Map<String, Value>, key: &str) -> Option<String> {
    object
        .get(key)
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string)
}

/// Like the TypeScript `clampNumber`: accepts numbers or numeric strings,
/// floors, clamps, and falls back for anything non-finite.
fn clamp_number(value: Option<&Value>, min: u64, max: u64, fallback: u64) -> u64 {
    let Some(value) = value else {
        return fallback;
    };
    let parsed = match value {
        Value::Number(number) => number.as_f64(),
        Value::String(text) => text.trim().parse::<f64>().ok(),
        _ => None,
    };
    match parsed {
        Some(number) if number.is_finite() && number >= 0.0 => (number as u64).clamp(min, max),
        _ => fallback,
    }
}

// ---------------------------------------------------------------------------
// Normalized in-meeting events (port of `src/meeting/types.ts`)
// ---------------------------------------------------------------------------

/// A meeting participant, as much as the payload exposes.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct MeetingActor {
    pub id: Option<String>,
    pub name: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TranscriptEvent {
    pub speaker: MeetingActor,
    pub text: String,
    /// Stable per sentence — later items with the same id supersede earlier ones.
    pub sentence_id: String,
    pub start_ms: Option<i64>,
    pub end_ms: Option<i64>,
    pub language: Option<String>,
    /// True when the speaker is our own bot (its speech is transcribed back).
    pub self_echo: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChatEvent {
    pub from: MeetingActor,
    pub content: String,
    /// Feishu's in-meeting message type; `3` is a reaction rather than text.
    pub message_type: Option<i64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ParticipantAction {
    Joined,
    Left,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParticipantEvent {
    pub action: ParticipantAction,
    pub user: MeetingActor,
    pub leave_reason: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ShareAction {
    Started,
    Ended,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShareEvent {
    pub action: ShareAction,
    pub url: Option<String>,
    pub title: Option<String>,
}

/// One normalized in-meeting event.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MeetingEvent {
    Transcript(TranscriptEvent),
    Chat(ChatEvent),
    Participant(ParticipantEvent),
    Share(ShareEvent),
}

/// `activity_event_type` → the array field carrying its items.
fn array_field(kind: &str) -> Option<&'static str> {
    match kind {
        "transcript_received" => Some("transcript_received_items"),
        "chat_received" => Some("chat_received_items"),
        "participant_joined" => Some("participant_joined_items"),
        "participant_left" => Some("participant_left_items"),
        "magic_share_started" => Some("magic_share_started_items"),
        "magic_share_ended" => Some("magic_share_ended_items"),
        _ => None,
    }
}

fn str_field(value: Option<&Value>) -> Option<String> {
    match value? {
        Value::String(text) if !text.trim().is_empty() => Some(text.clone()),
        _ => None,
    }
}

fn scalar_string(value: Option<&Value>) -> String {
    match value {
        Some(Value::String(text)) => text.clone(),
        Some(Value::Number(number)) => number.to_string(),
        Some(Value::Bool(value)) => value.to_string(),
        _ => String::new(),
    }
}

fn num_field(value: Option<&Value>) -> Option<i64> {
    match value? {
        Value::Number(number) => number
            .as_i64()
            .or_else(|| number.as_f64().map(|value| value as i64)),
        Value::String(text) => text.trim().parse::<i64>().ok(),
        _ => None,
    }
}

/// Pull the actor out of an item. The wire format is inconsistent about the
/// wrapper key (`speaker` / `user` / `operator` / `sender` / flat), so probe.
fn probe_actor(object: &Map<String, Value>, keys: &[&str]) -> MeetingActor {
    for key in keys {
        let Some(Value::Object(wrapped)) = object.get(*key) else {
            continue;
        };
        let id = str_field(wrapped.get("open_id"))
            .or_else(|| str_field(wrapped.get("user_id")))
            .or_else(|| str_field(wrapped.get("id")));
        let name = str_field(wrapped.get("name")).or_else(|| str_field(wrapped.get("user_name")));
        if id.is_some() || name.is_some() {
            return MeetingActor { id, name };
        }
    }
    MeetingActor {
        id: str_field(object.get("open_id")).or_else(|| str_field(object.get("user_id"))),
        name: str_field(object.get("name"))
            .or_else(|| str_field(object.get("user_name")))
            .or_else(|| str_field(object.get("speaker_name"))),
    }
}

fn activity_array<'a>(item: &'a MeetingActivityItem, field: &str) -> Option<&'a [Value]> {
    if let Some(Value::Array(values)) = item.extra.get(field) {
        return Some(values);
    }
    match item.payload.as_ref()?.get(field) {
        Some(Value::Array(values)) => Some(values),
        _ => None,
    }
}

/// Flatten one activity item into normalized events (the second unpack layer).
///
/// A single push can carry dozens of transcript lines — taking only the first
/// would silently drop data, which is why this returns an array.
pub fn unpack_activity(item: &MeetingActivityItem, bot_open_id: Option<&str>) -> Vec<MeetingEvent> {
    let Some(kind) = item.activity_kind() else {
        return Vec::new();
    };
    let Some(field) = array_field(kind) else {
        return Vec::new();
    };
    let Some(entries) = activity_array(item, field) else {
        return Vec::new();
    };

    let mut events = Vec::with_capacity(entries.len());
    for entry in entries {
        let Some(object) = entry.as_object() else {
            continue;
        };
        match kind {
            "transcript_received" => {
                let text = str_field(object.get("text"))
                    .or_else(|| str_field(object.get("content")))
                    .unwrap_or_default();
                let sentence_id = scalar_string(
                    object
                        .get("sentence_id")
                        .or_else(|| object.get("sentenceId")),
                );
                if text.is_empty() || sentence_id.is_empty() {
                    continue;
                }
                let speaker = probe_actor(object, &["speaker", "user", "sender"]);
                let self_echo = match (bot_open_id, speaker.id.as_deref()) {
                    (Some(bot), Some(id)) => bot == id,
                    _ => false,
                };
                events.push(MeetingEvent::Transcript(TranscriptEvent {
                    speaker,
                    text,
                    sentence_id,
                    start_ms: num_field(
                        object
                            .get("start_time_ms")
                            .or_else(|| object.get("start_ms")),
                    ),
                    end_ms: num_field(object.get("end_time_ms").or_else(|| object.get("end_ms"))),
                    language: str_field(object.get("language")),
                    self_echo,
                }));
            }
            "chat_received" => {
                let content = str_field(object.get("content"))
                    .or_else(|| str_field(object.get("text")))
                    .unwrap_or_default();
                if content.is_empty() {
                    continue;
                }
                events.push(MeetingEvent::Chat(ChatEvent {
                    from: probe_actor(object, &["sender", "user", "from"]),
                    content,
                    message_type: num_field(object.get("message_type")),
                }));
            }
            "participant_joined" | "participant_left" => {
                events.push(MeetingEvent::Participant(ParticipantEvent {
                    action: if kind == "participant_joined" {
                        ParticipantAction::Joined
                    } else {
                        ParticipantAction::Left
                    },
                    user: probe_actor(object, &["user", "participant"]),
                    leave_reason: str_field(object.get("leave_reason")),
                }));
            }
            "magic_share_started" | "magic_share_ended" => {
                events.push(MeetingEvent::Share(ShareEvent {
                    action: if kind == "magic_share_started" {
                        ShareAction::Started
                    } else {
                        ShareAction::Ended
                    },
                    url: str_field(object.get("url")),
                    title: str_field(object.get("title")),
                }));
            }
            _ => {}
        }
    }
    events
}

// ---------------------------------------------------------------------------
// Transcript, status and poll schedule
// ---------------------------------------------------------------------------

/// One committed transcript line in the rolling buffer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TranscriptLine {
    pub sentence_id: String,
    pub speaker: String,
    pub text: String,
}

impl TranscriptLine {
    pub fn render(&self) -> String {
        format!("{}: {}", self.speaker, self.text)
    }
}

/// Where in-meeting content is currently coming from.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum MeetingEventSource {
    #[default]
    Poll,
    Push,
}

impl MeetingEventSource {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Poll => "poll",
            Self::Push => "push",
        }
    }
}

/// Point-in-time view of one session (what `/meeting` prints and what an agent
/// run is built from).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MeetingSessionStatus {
    pub meeting_id: String,
    pub meeting_no: String,
    pub topic: Option<String>,
    pub started_at_ms: i64,
    pub source: MeetingEventSource,
    pub transcript_lines: usize,
    pub participants: usize,
    /// Pushes/polls ingested — a quick health signal.
    pub ingested: u64,
    /// Raw activity items per `activity_event_type`, including types we do not
    /// handle (prefixed `?`). This distinguishes "Feishu never sent
    /// transcripts" from "it sent them and we failed to parse".
    pub event_counts: BTreeMap<String, u64>,
    pub ended: bool,
}

/// An immutable copy of a session, safe to use while the manager keeps running.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SessionSnapshot {
    pub meeting_id: String,
    pub meeting_no: String,
    pub topic: Option<String>,
    pub origin_chat_id: Option<String>,
    pub started_at_ms: i64,
    pub source: MeetingEventSource,
    pub participants: usize,
    pub ended: bool,
    pub transcript: Vec<TranscriptLine>,
}

impl SessionSnapshot {
    /// Latest transcript as `说话人: 内容` lines, oldest first — agent context.
    pub fn recent_transcript(&self, limit: Option<usize>) -> Vec<String> {
        let start = match limit {
            Some(limit) if limit < self.transcript.len() => self.transcript.len() - limit,
            _ => 0,
        };
        self.transcript[start..]
            .iter()
            .map(TranscriptLine::render)
            .collect()
    }

    /// `topic`, else the meeting number — used in confirmations and summaries.
    pub fn title(&self) -> String {
        self.topic
            .clone()
            .unwrap_or_else(|| self.meeting_no.clone())
    }
}

/// Pure backoff state for the `bots/events` poller.
///
/// The TypeScript session keeps `idleRounds`: an empty page increments it
/// (capped at 3), any item resets it to 0, and `hasMore` forces `-1`
/// ("come back immediately"). The delay is `base * 2^idle`, capped at 10s.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct PollSchedule {
    idle_rounds: i32,
}

impl PollSchedule {
    pub fn new() -> Self {
        Self::default()
    }

    /// Account for one fetched page.
    pub fn on_page(&mut self, items: usize, has_more: bool) {
        self.idle_rounds = if items > 0 {
            0
        } else {
            (self.idle_rounds + 1).min(3)
        };
        if has_more {
            self.idle_rounds = -1;
        }
    }

    /// Account for a failed poll (network or API error): back off like an idle
    /// round.
    pub fn on_error(&mut self) {
        self.idle_rounds = (self.idle_rounds + 1).min(3);
    }

    pub fn idle_rounds(&self) -> i32 {
        self.idle_rounds
    }

    pub fn delay_ms(&self, base_ms: u64) -> u64 {
        if self.idle_rounds < 0 {
            return 0;
        }
        let factor = 1u64 << (self.idle_rounds.min(16) as u32);
        base_ms.saturating_mul(factor).min(MAX_POLL_BACKOFF_MS)
    }
}

/// What one [`MeetingSession::ingest`] produced.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct IngestReport {
    /// The item was already seen (`event_id` or content key) and was dropped.
    pub duplicate: bool,
    /// Normalized events emitted now (transcripts under debounce are not
    /// included until [`MeetingSession::flush_due_transcripts`]).
    pub events: Vec<MeetingEvent>,
    /// `activity_event_type` that produced no normalized event, if any.
    pub unparsed_kind: Option<String>,
}

/// One live meeting: owns the transcript buffer, de-duplication, transcript
/// stabilization and the poll schedule. Routing across meetings lives in
/// [`MeetingManager`], so this type is cheap to unit-test with no network.
#[derive(Debug)]
pub struct MeetingSession {
    meeting_id: String,
    meeting_no: String,
    topic: Option<String>,
    origin_chat_id: Option<String>,
    started_at_ms: i64,
    config: MeetingConfig,
    bot_open_id: Option<String>,

    transcript: Vec<TranscriptLine>,
    /// `sentence_id` → latest text, so a re-sent sentence overwrites in place.
    sentences: HashMap<String, String>,
    /// `event_id` of already-ingested activity items (push replay / poll overlap).
    seen_events: HashSet<String>,
    seen_order: VecDeque<String>,
    /// Pending debounce timers when `stabilize_ms > 0`.
    pending: HashMap<String, PendingTranscript>,
    participants: BTreeSet<String>,
    event_counts: BTreeMap<String, u64>,
    ingested: u64,

    cursor: Option<String>,
    source: MeetingEventSource,
    schedule: PollSchedule,
    next_poll_at_ms: i64,
    ended: bool,
}

#[derive(Debug, Clone)]
struct PendingTranscript {
    event: TranscriptEvent,
    deadline_ms: i64,
}

/// Construction input for [`MeetingSession`] (kept as a struct because the
/// list of fields is long and mostly optional).
#[derive(Debug, Clone)]
pub struct MeetingSessionConfig {
    pub meeting_id: String,
    pub meeting_no: String,
    pub topic: Option<String>,
    pub origin_chat_id: Option<String>,
    pub config: MeetingConfig,
    pub bot_open_id: Option<String>,
    pub started_at_ms: i64,
}

impl MeetingSession {
    pub fn new(spec: MeetingSessionConfig) -> Self {
        let MeetingSessionConfig {
            meeting_id,
            meeting_no,
            topic,
            origin_chat_id,
            config,
            bot_open_id,
            started_at_ms,
        } = spec;
        Self {
            meeting_id,
            meeting_no,
            topic,
            origin_chat_id,
            started_at_ms,
            config,
            bot_open_id,
            transcript: Vec::new(),
            sentences: HashMap::new(),
            seen_events: HashSet::new(),
            seen_order: VecDeque::new(),
            pending: HashMap::new(),
            participants: BTreeSet::new(),
            event_counts: BTreeMap::new(),
            ingested: 0,
            cursor: None,
            source: MeetingEventSource::Poll,
            schedule: PollSchedule::new(),
            next_poll_at_ms: started_at_ms,
            ended: false,
        }
    }

    pub fn meeting_id(&self) -> &str {
        &self.meeting_id
    }

    pub fn meeting_no(&self) -> &str {
        &self.meeting_no
    }

    pub fn topic(&self) -> Option<&str> {
        self.topic.as_deref()
    }

    pub fn origin_chat_id(&self) -> Option<&str> {
        self.origin_chat_id.as_deref()
    }

    pub fn is_ended(&self) -> bool {
        self.ended
    }

    pub fn source(&self) -> MeetingEventSource {
        self.source
    }

    pub fn status(&self) -> MeetingSessionStatus {
        MeetingSessionStatus {
            meeting_id: self.meeting_id.clone(),
            meeting_no: self.meeting_no.clone(),
            topic: self.topic.clone(),
            started_at_ms: self.started_at_ms,
            source: self.source,
            transcript_lines: self.transcript.len(),
            participants: self.participants.len(),
            ingested: self.ingested,
            event_counts: self.event_counts.clone(),
            ended: self.ended,
        }
    }

    pub fn snapshot(&self) -> SessionSnapshot {
        SessionSnapshot {
            meeting_id: self.meeting_id.clone(),
            meeting_no: self.meeting_no.clone(),
            topic: self.topic.clone(),
            origin_chat_id: self.origin_chat_id.clone(),
            started_at_ms: self.started_at_ms,
            source: self.source,
            participants: self.participants.len(),
            ended: self.ended,
            transcript: self.transcript.clone(),
        }
    }

    /// Latest transcript as `说话人: 内容` lines, oldest first.
    pub fn recent_transcript(&self, limit: Option<usize>) -> Vec<String> {
        self.snapshot().recent_transcript(limit)
    }

    pub fn transcript_len(&self) -> usize {
        self.transcript.len()
    }

    /// Ingest one raw activity item. Idempotent per `event_id`, so replayed
    /// pushes and poll/push overlap collapse to one delivery.
    pub fn ingest(&mut self, item: &MeetingActivityItem, now_ms: i64) -> IngestReport {
        if self.ended {
            return IngestReport::default();
        }

        let dedup_key = item
            .event_id
            .as_deref()
            .map(str::trim)
            .filter(|id| !id.is_empty())
            .map(str::to_string)
            .or_else(|| content_dedup_key(item));
        if let Some(key) = dedup_key {
            if !self.seen_events.insert(key.clone()) {
                return IngestReport {
                    duplicate: true,
                    ..IngestReport::default()
                };
            }
            self.seen_order.push_back(key);
            // Bound the dedup set; meetings are long-lived and ids never repeat
            // once they have scrolled far enough out of the window.
            while self.seen_events.len() > 5_000 && self.seen_order.len() > 4_000 {
                match self.seen_order.pop_front() {
                    Some(old) => {
                        self.seen_events.remove(&old);
                    }
                    None => break,
                }
            }
        }

        self.ingested += 1;
        let raw_kind = item
            .activity_kind()
            .filter(|kind| !kind.is_empty())
            .unwrap_or("unknown")
            .to_string();
        let events = unpack_activity(item, self.bot_open_id.as_deref());
        let key = if events.is_empty() {
            format!("?{raw_kind}")
        } else {
            raw_kind.clone()
        };
        *self.event_counts.entry(key).or_insert(0) += 1;

        let mut report = IngestReport {
            unparsed_kind: events.is_empty().then_some(raw_kind),
            ..IngestReport::default()
        };
        for event in events {
            match event {
                MeetingEvent::Transcript(transcript) => {
                    if let Some(committed) = self.handle_transcript(transcript, now_ms) {
                        report.events.push(MeetingEvent::Transcript(committed));
                    }
                }
                MeetingEvent::Participant(participant) => {
                    if let Some(id) = participant.user.id.as_deref() {
                        match participant.action {
                            ParticipantAction::Joined => {
                                self.participants.insert(id.to_string());
                            }
                            ParticipantAction::Left => {
                                self.participants.remove(id);
                            }
                        }
                    }
                    report.events.push(MeetingEvent::Participant(participant));
                }
                other => report.events.push(other),
            }
        }
        report
    }

    /// Filter self-echo, de-duplicate exact repeats and either commit now
    /// (`stabilize_ms == 0`) or schedule the debounce.
    fn handle_transcript(
        &mut self,
        event: TranscriptEvent,
        now_ms: i64,
    ) -> Option<TranscriptEvent> {
        // The bot's own speech comes back through the meeting's transcription;
        // not filtering it here would let "answer whatever you hear" trigger.
        if event.self_echo {
            return None;
        }
        if self.sentences.get(&event.sentence_id) == Some(&event.text) {
            return None; // exact repeat
        }
        self.sentences
            .insert(event.sentence_id.clone(), event.text.clone());

        if self.config.transcript.stabilize_ms == 0 {
            self.commit_transcript(&event);
            return Some(event);
        }

        // Debounce: a sentence is "final" once it stops growing for stabilizeMs.
        self.pending.insert(
            event.sentence_id.clone(),
            PendingTranscript {
                event: event.clone(),
                deadline_ms: now_ms.saturating_add(self.config.transcript.stabilize_ms as i64),
            },
        );
        None
    }

    /// Append (or replace) the sentence in the rolling buffer.
    fn commit_transcript(&mut self, event: &TranscriptEvent) {
        let line = TranscriptLine {
            sentence_id: event.sentence_id.clone(),
            speaker: event
                .speaker
                .name
                .clone()
                .or_else(|| event.speaker.id.clone())
                .unwrap_or_else(|| "?".to_string()),
            text: event.text.clone(),
        };
        // Same sentence growing -> replace in place, so the buffer holds one
        // entry per sentence instead of every intermediate prefix.
        match self
            .transcript
            .iter()
            .position(|line| line.sentence_id == event.sentence_id)
        {
            Some(index) => self.transcript[index] = line,
            None => self.transcript.push(line),
        }
        let keep = self.config.transcript.keep.max(1);
        if self.transcript.len() > keep {
            let excess = self.transcript.len() - keep;
            self.transcript.drain(..excess);
        }
    }

    /// Commit every debounced sentence whose quiet window has elapsed.
    pub fn flush_due_transcripts(&mut self, now_ms: i64) -> Vec<TranscriptEvent> {
        let mut due: Vec<(i64, String)> = self
            .pending
            .iter()
            .filter(|(_, pending)| pending.deadline_ms <= now_ms)
            .map(|(id, pending)| (pending.deadline_ms, id.clone()))
            .collect();
        due.sort();
        let mut committed = Vec::with_capacity(due.len());
        for (_, sentence_id) in due {
            if let Some(pending) = self.pending.remove(&sentence_id) {
                self.commit_transcript(&pending.event);
                committed.push(pending.event);
            }
        }
        committed
    }

    /// Earliest debounce deadline, if any sentence is pending.
    pub fn next_transcript_deadline_ms(&self) -> Option<i64> {
        self.pending
            .values()
            .map(|pending| pending.deadline_ms)
            .min()
    }

    pub fn pending_transcripts(&self) -> usize {
        self.pending.len()
    }

    /// Polling was the primary lane; once push is observed for this meeting,
    /// stop polling it (both lanes would deliver every event twice).
    pub fn mark_push_active(&mut self) -> bool {
        if self.source == MeetingEventSource::Push {
            return false;
        }
        self.source = MeetingEventSource::Push;
        true
    }

    /// Called when the meeting ends server-side: stop work, keep the transcript.
    pub fn mark_ended(&mut self) {
        self.ended = true;
        self.pending.clear();
    }

    pub fn cursor(&self) -> Option<&str> {
        self.cursor.as_deref()
    }

    pub fn poll_due(&self, now_ms: i64) -> bool {
        !self.ended && self.source == MeetingEventSource::Poll && self.next_poll_at_ms <= now_ms
    }

    /// Next poll instant, if this session still polls.
    pub fn next_poll_at_ms(&self) -> Option<i64> {
        (!self.ended && self.source == MeetingEventSource::Poll).then_some(self.next_poll_at_ms)
    }

    /// Record a successful page: advance the cursor and schedule the next poll.
    pub fn note_page(
        &mut self,
        now_ms: i64,
        page_token: Option<String>,
        items: usize,
        has_more: bool,
    ) {
        // A `has_more` page that neither produced items nor moved the cursor
        // would otherwise spin the tick loop; treat it as an idle round.
        let next_cursor = page_token.or_else(|| self.cursor.clone());
        let moved = next_cursor != self.cursor;
        self.cursor = next_cursor;
        let repeated_cursor = has_more && items == 0 && !moved;
        self.schedule.on_page(items, has_more && !repeated_cursor);
        self.next_poll_at_ms =
            now_ms.saturating_add(self.schedule.delay_ms(self.poll_interval_ms()) as i64);
    }

    /// Record a failed poll and back off.
    pub fn note_error(&mut self, now_ms: i64) {
        self.schedule.on_error();
        self.next_poll_at_ms =
            now_ms.saturating_add(self.schedule.delay_ms(self.poll_interval_ms()) as i64);
    }

    pub fn schedule(&self) -> PollSchedule {
        self.schedule
    }

    pub fn poll_interval_ms(&self) -> u64 {
        self.config.poll_interval_ms
    }
}

/// Stable key for an activity item that carries no `event_id`. Built from the
/// type plus the identity-bearing fields of its items, so the same event
/// arriving over both lanes collapses to one.
fn content_dedup_key(item: &MeetingActivityItem) -> Option<String> {
    let kind = item.activity_kind()?;
    let arrays = item
        .extra
        .iter()
        .map(|(key, value)| (key.as_str(), value))
        .chain(
            item.payload
                .as_ref()
                .and_then(Value::as_object)
                .map(|payload| payload.iter().map(|(key, value)| (key.as_str(), value)))
                .into_iter()
                .flatten(),
        );
    let array = arrays
        .filter(|(key, _)| key.ends_with("_items"))
        .find_map(|(_, value)| value.as_array())?;
    if array.is_empty() {
        return None;
    }
    let parts: Vec<String> = array
        .iter()
        .map(|entry| {
            let Some(object) = entry.as_object() else {
                return String::new();
            };
            let sender_open_id = match object.get("sender") {
                Some(Value::Object(sender)) => str_field(sender.get("open_id")).unwrap_or_default(),
                _ => String::new(),
            };
            [
                scalar_string(
                    object
                        .get("sentence_id")
                        .or_else(|| object.get("message_id")),
                ),
                scalar_string(object.get("content").or_else(|| object.get("text"))),
                scalar_string(
                    object
                        .get("start_time_ms")
                        .or_else(|| object.get("create_time"))
                        .or_else(|| object.get("time")),
                ),
                str_field(object.get("open_id")).unwrap_or(sender_open_id),
            ]
            .join("|")
        })
        .collect();
    Some(format!("{kind}#{}", parts.join("~")))
}

// ---------------------------------------------------------------------------
// Push payloads (`vc.bot.*` long-connection events)
// ---------------------------------------------------------------------------

/// One parsed `vc.bot.*` push frame.
#[derive(Debug, Clone, PartialEq)]
pub enum MeetingPush {
    Invited {
        meeting_no: String,
        inviter_id: Option<String>,
    },
    Activity(Vec<MeetingActivityItem>),
    Ended {
        meeting_id: String,
    },
}

/// The `header.event_type` of a long-connection frame, if present.
pub fn push_event_type(raw: &Value) -> Option<&str> {
    raw.get("header")
        .and_then(|header| header.get("event_type"))
        .and_then(Value::as_str)
        .or_else(|| raw.get("event_type").and_then(Value::as_str))
}

fn push_body(raw: &Value) -> &Value {
    raw.get("event").unwrap_or(raw)
}

/// Parse a `vc.bot.*` frame; `None` for unrelated events.
pub fn parse_meeting_push(raw: &Value) -> Option<MeetingPush> {
    match push_event_type(raw)? {
        "vc.bot.meeting_invited_v1" => {
            let body = push_body(raw);
            let meeting_no = body
                .get("meeting")
                .and_then(|meeting| meeting.get("meeting_no"))
                .and_then(Value::as_str)?
                .trim()
                .to_string();
            if meeting_no.is_empty() {
                return None;
            }
            let inviter = body.get("operator").or_else(|| body.get("inviter"));
            let inviter_id = inviter
                .and_then(|inviter| inviter.get("open_id"))
                .and_then(Value::as_str)
                .map(str::to_string);
            Some(MeetingPush::Invited {
                meeting_no,
                inviter_id,
            })
        }
        "vc.bot.meeting_activity_v1" => {
            let body = push_body(raw);
            let items = body
                .get("meeting_activity_items")
                .and_then(Value::as_array)
                .or_else(|| body.get("events").and_then(Value::as_array))?;
            let parsed = items
                .iter()
                .filter_map(|item| serde_json::from_value::<MeetingActivityItem>(item.clone()).ok())
                .collect();
            Some(MeetingPush::Activity(parsed))
        }
        "vc.bot.meeting_ended_v1" => {
            let meeting_id = push_body(raw)
                .get("meeting")
                .and_then(|meeting| meeting.get("id"))
                .and_then(Value::as_str)?
                .trim()
                .to_string();
            if meeting_id.is_empty() {
                return None;
            }
            Some(MeetingPush::Ended { meeting_id })
        }
        _ => None,
    }
}

// ---------------------------------------------------------------------------
// Error hints (port of `describeMeetingError` + `preflight.ts` classification)
// ---------------------------------------------------------------------------

/// Human-readable hint for the allowlisted-beta failure, else the raw message.
pub fn describe_meeting_error(err: &RestError) -> String {
    if is_not_in_gray(err) {
        return format!(
            "智能体入会能力尚未对本应用开通（错误码 20017）。需要先申请加入内测，再重试：{MEETING_BETA_CHAT_URL}"
        );
    }
    if let RestError::Api { code, message } = err {
        let looks_like_scope = *code == 99_991_672
            || message.to_ascii_lowercase().contains("scope")
            || message.to_ascii_lowercase().contains("permission");
        if looks_like_scope {
            return format!(
                "应用身份缺少 VC 相关权限（需要 {}）：{message}",
                MEETING_REQUIRED_SCOPES
                    .iter()
                    .map(|(scope, _)| *scope)
                    .collect::<Vec<_>>()
                    .join(" / ")
            );
        }
    }
    err.to_string()
}

/// Port of `MeetingPreflightStatus`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MeetingPreflightStatus {
    Ok,
    ScopeMissing,
    NotInBeta,
    Unknown,
}

impl MeetingPreflightStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Ok => "ok",
            Self::ScopeMissing => "scope-missing",
            Self::NotInBeta => "not-in-beta",
            Self::Unknown => "unknown",
        }
    }
}

/// Port of `MeetingPreflight` (the `lark-cli` probe itself is not run here).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MeetingPreflight {
    pub status: MeetingPreflightStatus,
    pub message: String,
    pub missing_scopes: Vec<String>,
    pub console_url: Option<String>,
    pub beta_chat_url: Option<String>,
}

/// Classify an already-parsed `lark-cli vc +meeting-list-active` response.
pub fn classify_preflight(payload: &Value) -> MeetingPreflight {
    let base = |status, message: String| MeetingPreflight {
        status,
        message,
        missing_scopes: Vec::new(),
        console_url: None,
        beta_chat_url: None,
    };
    let Some(object) = payload.as_object() else {
        return base(
            MeetingPreflightStatus::Unknown,
            "无法解析 lark-cli 输出".to_string(),
        );
    };
    if object.get("ok").and_then(Value::as_bool) == Some(true) {
        return base(MeetingPreflightStatus::Ok, "应用身份权限已就绪".to_string());
    }

    let error = object.get("error").and_then(Value::as_object);
    let code = error
        .and_then(|error| error.get("code"))
        .and_then(Value::as_i64);
    let subtype = error
        .and_then(|error| error.get("subtype"))
        .and_then(Value::as_str)
        .unwrap_or_default();
    let message = error
        .and_then(|error| error.get("message"))
        .and_then(Value::as_str)
        .unwrap_or("权限检查失败")
        .to_string();
    let console_url = error
        .and_then(|error| error.get("console_url"))
        .and_then(Value::as_str)
        .map(str::to_string);
    let missing_scopes: Vec<String> = error
        .and_then(|error| error.get("missing_scopes"))
        .and_then(Value::as_array)
        .map(|scopes| {
            scopes
                .iter()
                .filter_map(Value::as_str)
                .map(str::to_string)
                .collect()
        })
        .unwrap_or_default();

    if code == Some(20_017)
        || message.to_ascii_lowercase().contains("errnotingray")
        || subtype.to_ascii_lowercase().contains("notingray")
    {
        return MeetingPreflight {
            status: MeetingPreflightStatus::NotInBeta,
            message: "智能体入会能力尚未对本应用开通（内测灰度）".to_string(),
            missing_scopes: Vec::new(),
            console_url,
            beta_chat_url: Some(MEETING_BETA_CHAT_URL.to_string()),
        };
    }
    if subtype == "app_scope_not_applied" || code == Some(99_991_672) || !missing_scopes.is_empty()
    {
        return MeetingPreflight {
            status: MeetingPreflightStatus::ScopeMissing,
            message,
            missing_scopes: if missing_scopes.is_empty() {
                vec![MEETING_REQUIRED_SCOPES[0].0.to_string()]
            } else {
                missing_scopes
            },
            console_url,
            beta_chat_url: None,
        };
    }
    MeetingPreflight {
        status: MeetingPreflightStatus::Unknown,
        message,
        missing_scopes: Vec::new(),
        console_url,
        beta_chat_url: None,
    }
}

// ---------------------------------------------------------------------------
// Trigger matching and prompt building (port of `src/meeting/orchestrator.ts`)
// ---------------------------------------------------------------------------

/// Scope id for a meeting's agent session — one conversation per meeting.
pub fn meeting_scope_id(meeting_id: &str) -> String {
    format!("meeting:{meeting_id}")
}

/// Match an in-meeting chat line against the accepted trigger prefixes and
/// return what follows.
///
/// `None` means "not for me"; `Some("")` means "addressed but empty". Leading
/// punctuation after the prefix is dropped, so `@bot, hi` behaves like
/// `@bot hi`.
pub fn match_trigger(text: &str, prefixes: &[String]) -> Option<String> {
    let trimmed = text.trim();
    let lower = trimmed.to_lowercase();
    for prefix in prefixes {
        let prefix = prefix.trim();
        if prefix.is_empty() || !lower.starts_with(&prefix.to_lowercase()) {
            continue;
        }
        let rest = trimmed.get(prefix.len()..).unwrap_or_default();
        let rest = rest.trim_start_matches(|character: char| {
            character.is_whitespace() || ",，、:：。.!！?？~～-".contains(character)
        });
        return Some(rest.trim().to_string());
    }
    None
}

/// Configured prefix plus `@<botName>`, de-duplicated.
pub fn trigger_prefixes(configured: &str, bot_name: Option<&str>) -> Vec<String> {
    let mut prefixes: Vec<String> = Vec::new();
    for prefix in [
        configured,
        bot_name
            .map(|name| format!("@{name}"))
            .as_deref()
            .unwrap_or(""),
    ] {
        let prefix = prefix.trim();
        if !prefix.is_empty() && !prefixes.iter().any(|existing| existing == prefix) {
            prefixes.push(prefix.to_string());
        }
    }
    prefixes
}

fn is_stop_word(question: &str) -> bool {
    let lowered = question.to_lowercase();
    STOP_WORDS.iter().any(|word| *word == lowered)
}

/// Compose the agent prompt: meeting context first, then the actual ask.
pub fn build_meeting_prompt(
    question: &str,
    transcript: &[String],
    topic: Option<&str>,
    asked_by: Option<&str>,
) -> String {
    let mut parts: Vec<String> = Vec::new();
    parts.push("你正在参加一场飞书视频会议，以下是会议的实时字幕上下文。".to_string());
    if let Some(topic) = topic.filter(|topic| !topic.trim().is_empty()) {
        parts.push(format!("会议主题：{topic}"));
    }
    parts.push(String::new());
    parts.push("=== 会议字幕（按时间顺序，可能不完整） ===".to_string());
    parts.push(if transcript.is_empty() {
        "（暂无字幕）".to_string()
    } else {
        transcript.join("\n")
    });
    parts.push("=== 字幕结束 ===".to_string());
    parts.push(String::new());
    parts.push(match asked_by.filter(|name| !name.trim().is_empty()) {
        Some(name) => format!("参会人「{name}」在会中向你提问："),
        None => "收到的请求：".to_string(),
    });
    parts.push(question.to_string());
    parts.push(String::new());
    parts.push(
        "请基于上面的会议上下文作答。回答会被发到会议里，请简洁（尽量 200 字内），不要用 markdown 标题。"
            .to_string(),
    );
    parts.join("\n")
}

/// Summary question used by `summaryOnEnd`.
pub const SUMMARY_QUESTION: &str = "会议已结束。请基于以上会议字幕输出一份纪要：讨论了什么、达成的结论、待办（含负责人，如字幕里有）。";
/// Notes question used by `/meeting notes`.
pub const NOTES_QUESTION: &str =
    "请基于以上会议字幕做一份简洁纪要：讨论了什么、结论、待办（如有）。";

/// One resolved summary destination.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolvedSummaryTarget {
    pub to: String,
    pub kind: SummaryTarget,
    /// True when the preferred target was unavailable and the other was used.
    pub fell_back: bool,
}

/// Pick where the summary goes: the configured preference first, the other one
/// as a fallback, so a summary is never thrown away just because the preferred
/// lane is missing.
pub fn resolve_summary_target(
    preference: SummaryTarget,
    origin_chat_id: Option<&str>,
    owner_open_id: Option<&str>,
) -> Option<ResolvedSummaryTarget> {
    let order = match preference {
        SummaryTarget::Owner => [SummaryTarget::Owner, SummaryTarget::Origin],
        SummaryTarget::Origin => [SummaryTarget::Origin, SummaryTarget::Owner],
    };
    for (index, kind) in order.into_iter().enumerate() {
        let to = match kind {
            SummaryTarget::Origin => origin_chat_id,
            SummaryTarget::Owner => owner_open_id,
        };
        if let Some(to) = to.filter(|to| !to.trim().is_empty()) {
            return Some(ResolvedSummaryTarget {
                to: to.to_string(),
                kind,
                fell_back: index > 0,
            });
        }
    }
    None
}

/// `**会议纪要 · <title>**\n\n<answer>` — the IM summary body.
pub fn format_summary(title: &str, answer: &str) -> String {
    format!("**会议纪要 · {title}**\n\n{answer}")
}

// ---------------------------------------------------------------------------
// Network seam
// ---------------------------------------------------------------------------

/// The four VC bot endpoints the manager needs, behind a trait so polling and
/// de-duplication can be exercised offline with a scripted fake.
#[async_trait]
pub trait MeetingApi: Send + Sync {
    async fn join(&self, meeting_no: &str) -> RestResult<JoinResult>;
    async fn leave(&self, meeting_id: &str) -> RestResult<()>;
    async fn send_text(&self, meeting_id: &str, text: &str) -> RestResult<()>;
    async fn events(
        &self,
        meeting_id: &str,
        page_size: u32,
        page_token: Option<&str>,
    ) -> RestResult<EventsPage>;
}

/// [`MeetingApi`] backed by the real [`VcBotClient`].
pub struct RestMeetingApi {
    rest: RestClient,
}

impl RestMeetingApi {
    pub fn new(rest: RestClient) -> Self {
        Self { rest }
    }
}

#[async_trait]
impl MeetingApi for RestMeetingApi {
    async fn join(&self, meeting_no: &str) -> RestResult<JoinResult> {
        VcBotClient::new(&self.rest).join(meeting_no).await
    }

    async fn leave(&self, meeting_id: &str) -> RestResult<()> {
        VcBotClient::new(&self.rest).leave(meeting_id).await
    }

    async fn send_text(&self, meeting_id: &str, text: &str) -> RestResult<()> {
        VcBotClient::new(&self.rest)
            .send_message(meeting_id, "text", text)
            .await
    }

    async fn events(
        &self,
        meeting_id: &str,
        page_size: u32,
        page_token: Option<&str>,
    ) -> RestResult<EventsPage> {
        VcBotClient::new(&self.rest)
            .events(meeting_id, page_size, page_token)
            .await
    }
}

/// Where an IM-side meeting reply should go.
///
/// `scope` is the message scope that issued the command (`chatId` or
/// `chatId:threadId`) and `parent_message_id` is the triggering message.
/// A topic scope replies inside its thread; everything else stays top-level.
#[derive(Debug, Clone, Copy, Default)]
pub struct ImReplyTarget<'a> {
    pub scope: Option<&'a str>,
    pub parent_message_id: Option<&'a str>,
}

/// What a meeting run needs from the bridge: workspace, session persistence and
/// IM delivery. Implemented by the bridge; faked in the tests below.
#[async_trait]
pub trait MeetingHost: Send + Sync {
    /// Working directory for a meeting-scoped agent run.
    async fn workspace_cwd(&self, scope: &str) -> PathBuf;
    /// Resumable agent session for the scope, if any.
    async fn resume_session(&self, scope: &str, cwd: &str) -> Option<String>;
    /// Persist the agent session id for the scope.
    async fn save_session(&self, scope: &str, session_key: &str, cwd: &str);
    /// Send a text message over IM. Accepts `oc_*` chat ids and `ou_*` open ids.
    async fn send_im_text(&self, to: &str, text: &str);
    /// Send an IM text reply through `target` (thread-aware when the host can).
    ///
    /// The default is top-level [`MeetingHost::send_im_text`]; the bridge
    /// overrides it to honour a topic command's `reply_in_thread`.
    async fn send_im_text_reply(&self, to: &str, target: ImReplyTarget<'_>, text: &str) {
        let _ = target;
        self.send_im_text(to, text).await;
    }
    /// Spawn one meeting agent task under `scope`.
    ///
    /// The bridge implementation also registers the task in its active-run map
    /// (`meeting:<meeting_id>`) so `/stop meeting:<id>` can abort it; the
    /// default is a plain spawn for standalone managers and tests.
    fn spawn_tracked_run(
        &self,
        scope: &str,
        future: Pin<Box<dyn Future<Output = ()> + Send>>,
    ) -> JoinHandle<()> {
        let _ = scope;
        tokio::spawn(future)
    }
}

// ---------------------------------------------------------------------------
// Manager
// ---------------------------------------------------------------------------

/// Identity + binary needed to spawn a meeting run.
#[derive(Debug, Clone)]
pub struct AgentSettings {
    pub kind: AgentKind,
    pub binary: PathBuf,
    /// Profile-resolved access policy applied to every meeting run.
    pub permissions: RunPermissions,
    pub bot_open_id: Option<String>,
    pub bot_name: Option<String>,
}

/// Push-lane health (`/meeting` status). `received` proves the console
/// subscription works; `hooked` only says the long-connection route exists.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct MeetingPushHealth {
    pub hooked: bool,
    pub reason: Option<String>,
    pub received: u64,
    pub last_at_ms: Option<i64>,
}

/// Result of `join`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JoinOutcome {
    pub session: SessionSnapshot,
    /// The bot was already in this meeting (no new join call).
    pub already_joined: bool,
}

struct ActiveMeetingRun {
    id: u64,
    handle: JoinHandle<()>,
}

struct MeetingRunGuard {
    manager: Arc<MeetingManager>,
    scope: String,
    id: u64,
}

impl Drop for MeetingRunGuard {
    fn drop(&mut self) {
        let mut runs = lock(&self.manager.runs);
        if runs.get(&self.scope).map(|run| run.id) == Some(self.id) {
            runs.remove(&self.scope);
        }
    }
}

enum AnswerDelivery {
    /// Deliver per `respondIn` (in-meeting chat trigger).
    Broadcast,
    /// Return the answer to the caller (`/meeting ask|notes`), deliver nothing.
    Caller(oneshot::Sender<String>),
}

struct AnswerPlan {
    meeting_id: String,
    question: String,
    asked_by: Option<String>,
    used_prefix: Option<String>,
    deliver: AnswerDelivery,
}

/// Owns every live meeting plus the push health and the agent runs.
///
/// Started by `Bridge::start_meeting` as a Tokio task; [`MeetingHandle`] is
/// the shutdown handle. All network calls go through [`MeetingApi`], so the
/// whole poller can run against a scripted fake in tests.
pub struct MeetingManager {
    config: MeetingConfig,
    agent: AgentSettings,
    api: Arc<dyn MeetingApi>,
    host: Arc<dyn MeetingHost>,
    /// Global bridge run limiter. `None` in standalone/test managers, where
    /// meeting runs are not capped.
    run_limit: Option<Arc<RunLimiter>>,
    sessions: StdMutex<BTreeMap<String, MeetingSession>>,
    runs: StdMutex<HashMap<String, ActiveMeetingRun>>,
    next_run_id: AtomicU64,
    push: StdMutex<MeetingPushHealth>,
    shutdown: StdMutex<Option<watch::Sender<bool>>>,
    wake: Notify,
}

impl MeetingManager {
    pub fn new(
        config: MeetingConfig,
        agent: AgentSettings,
        api: Arc<dyn MeetingApi>,
        host: Arc<dyn MeetingHost>,
    ) -> Self {
        Self {
            config,
            agent,
            api,
            host,
            run_limit: None,
            sessions: StdMutex::new(BTreeMap::new()),
            runs: StdMutex::new(HashMap::new()),
            next_run_id: AtomicU64::new(1),
            push: StdMutex::new(MeetingPushHealth::default()),
            shutdown: StdMutex::new(None),
            wake: Notify::new(),
        }
    }

    /// Attach the bridge's global run limiter.
    ///
    /// `Bridge::start_meeting` calls this; tests that are not exercising
    /// concurrency leave it unset. The limiter is shared with IM, comment and
    /// every other meeting run.
    pub fn with_run_limit(mut self, limiter: Arc<RunLimiter>) -> Self {
        self.run_limit = Some(limiter);
        self
    }

    pub fn config(&self) -> &MeetingConfig {
        &self.config
    }

    pub fn push_health(&self) -> MeetingPushHealth {
        lock(&self.push).clone()
    }

    /// Record whether the `vc.bot.*` long-connection route exists.
    pub fn set_push_hooked(&self, hooked: bool, reason: Option<String>) {
        let mut push = lock(&self.push);
        push.hooked = hooked;
        push.reason = reason;
        if !hooked {
            push.received = 0;
            push.last_at_ms = None;
        }
    }

    fn note_push(&self) {
        let mut push = lock(&self.push);
        push.received += 1;
        push.last_at_ms = Some(now_ms());
    }

    /// Start the poll/dispatch task.
    pub fn start(self: &Arc<Self>) -> MeetingHandle {
        let (shutdown, receiver) = watch::channel(false);
        *lock(&self.shutdown) = Some(shutdown);
        let manager = Arc::clone(self);
        let task = tokio::spawn(async move { run_loop(manager, receiver).await });
        MeetingHandle {
            manager: Arc::clone(self),
            task,
        }
    }

    fn request_shutdown(&self) {
        if let Some(shutdown) = lock(&self.shutdown).as_ref() {
            let _ = shutdown.send(true);
        }
        self.wake.notify_waiters();
    }

    pub fn statuses(&self) -> Vec<MeetingSessionStatus> {
        lock(&self.sessions)
            .values()
            .map(MeetingSession::status)
            .collect()
    }

    pub fn snapshot(&self, meeting_id: &str) -> Option<SessionSnapshot> {
        lock(&self.sessions)
            .get(meeting_id)
            .map(MeetingSession::snapshot)
    }

    pub fn find_by_meeting_no(&self, meeting_no: &str) -> Option<SessionSnapshot> {
        lock(&self.sessions)
            .values()
            .find(|session| session.meeting_no() == meeting_no.trim())
            .map(MeetingSession::snapshot)
    }

    pub fn all_snapshots(&self) -> Vec<SessionSnapshot> {
        lock(&self.sessions)
            .values()
            .map(MeetingSession::snapshot)
            .collect()
    }

    /// Join a meeting by its 9-digit number and start a session.
    ///
    /// Returns the existing session when the bot is already in this meeting
    /// (the TypeScript `join` short-circuit).
    pub async fn join(
        self: &Arc<Self>,
        meeting_no: &str,
        origin_chat_id: Option<&str>,
    ) -> Result<JoinOutcome, String> {
        let meeting_no = meeting_no.trim();
        if !larkline_lark::meeting::is_meeting_no(meeting_no) {
            return Err(format!("会议号必须是 9 位数字：{meeting_no}"));
        }
        if let Some(session) = self.find_by_meeting_no(meeting_no) {
            if !session.ended {
                return Ok(JoinOutcome {
                    session,
                    already_joined: true,
                });
            }
        }

        let joined = self
            .api
            .join(meeting_no)
            .await
            .map_err(|err| describe_meeting_error(&err))?;
        let started_at_ms = now_ms();
        let bot_open_id = self
            .agent
            .bot_open_id
            .clone()
            .or_else(|| self.config.bot_open_id.clone());
        let session = MeetingSession::new(MeetingSessionConfig {
            meeting_id: joined.meeting_id.clone(),
            meeting_no: joined.meeting_no.clone(),
            topic: joined.topic.clone(),
            origin_chat_id: origin_chat_id.map(str::to_string),
            config: self.config.clone(),
            bot_open_id,
            started_at_ms,
        });
        let snapshot = session.snapshot();
        lock(&self.sessions).insert(joined.meeting_id.clone(), session);
        self.wake.notify_one();
        tracing::info!(
            meeting_id = %joined.meeting_id,
            meeting_no = %joined.meeting_no,
            "joined meeting"
        );
        Ok(JoinOutcome {
            session: snapshot,
            already_joined: false,
        })
    }

    /// Leave a meeting and drop its session. Idempotent.
    pub async fn leave(&self, meeting_id: &str) -> bool {
        let session = lock(&self.sessions).remove(meeting_id);
        let Some(mut session) = session else {
            return false;
        };
        session.mark_ended();
        // An explicit leave means the answer has nowhere to go; do not burn
        // tokens on a run the user just abandoned.
        self.interrupt(meeting_id);
        if let Err(err) = self.api.leave(meeting_id).await {
            tracing::warn!(
                meeting_id,
                error = %describe_meeting_error(&err),
                "cannot leave meeting"
            );
        }
        self.wake.notify_one();
        true
    }

    /// Leave every meeting (shutdown). Best-effort: failures are logged.
    pub async fn leave_all(&self) {
        let ids: Vec<String> = lock(&self.sessions).keys().cloned().collect();
        for meeting_id in ids {
            self.leave(&meeting_id).await;
        }
    }

    /// Mark a meeting ended from a push and summarize it when configured.
    pub fn end_meeting(self: &Arc<Self>, meeting_id: &str) -> Option<SessionSnapshot> {
        let snapshot = {
            let mut sessions = lock(&self.sessions);
            sessions.remove(meeting_id).map(|mut session| {
                session.mark_ended();
                session.snapshot()
            })
        };
        if let Some(snapshot) = snapshot.clone() {
            tracing::info!(meeting_id, "meeting ended");
            // The in-meeting lane is gone; abort a live answer so the summary
            // run does not race it for the same resumed agent session.
            self.interrupt(meeting_id);
            self.spawn_summary(snapshot);
        }
        self.wake.notify_one();
        snapshot
    }

    /// Abort the live agent run for a meeting. Returns whether one was running.
    ///
    /// This aborts the same task that is mirrored in the bridge's active-run
    /// map (`meeting:<id>`); the task's guards remove both entries, so
    /// `/meeting stop`, the in-meeting stop trigger and `/stop meeting:<id>`
    /// stay consistent.
    pub fn interrupt(&self, meeting_id: &str) -> bool {
        let scope = meeting_scope_id(meeting_id);
        let mut runs = lock(&self.runs);
        match runs.remove(&scope) {
            Some(run) => {
                run.handle.abort();
                true
            }
            None => false,
        }
    }

    /// Abort every live meeting run (shutdown).
    pub fn interrupt_all(&self) -> usize {
        let mut runs = lock(&self.runs);
        let count = runs.len();
        for (_, run) in runs.drain() {
            run.handle.abort();
        }
        count
    }

    /// Earliest instant the poll loop has work to do.
    pub fn next_wakeup(&self) -> Option<Duration> {
        let now = now_ms();
        let mut earliest: Option<i64> = None;
        for session in lock(&self.sessions).values() {
            for deadline in [
                session.next_poll_at_ms(),
                session.next_transcript_deadline_ms(),
            ]
            .into_iter()
            .flatten()
            {
                earliest = Some(match earliest {
                    Some(current) => current.min(deadline),
                    None => deadline,
                });
            }
        }
        earliest.map(|deadline| Duration::from_millis(deadline.saturating_sub(now).max(0) as u64))
    }

    /// Run one poll/dispatch round: commit due transcripts, poll every session
    /// whose interval elapsed, and answer triggered in-meeting chat.
    pub async fn tick(self: &Arc<Self>) {
        let now = now_ms();
        let due: Vec<String> = {
            let mut sessions = lock(&self.sessions);
            let mut due = Vec::new();
            for (meeting_id, session) in sessions.iter_mut() {
                session.flush_due_transcripts(now);
                if session.poll_due(now) {
                    due.push(meeting_id.clone());
                }
            }
            due
        };

        for meeting_id in due {
            let events = self.poll_session_once(&meeting_id).await;
            self.dispatch_events(&meeting_id, events);
        }
    }

    /// Poll one page for one session and ingest it. Exposed for offline tests;
    /// the poll loop calls it through [`Self::tick`].
    pub async fn poll_session_once(self: &Arc<Self>, meeting_id: &str) -> Vec<MeetingEvent> {
        let (cursor, should_poll) = {
            let sessions = lock(&self.sessions);
            match sessions.get(meeting_id) {
                Some(session) => (
                    session.cursor().map(str::to_string),
                    !session.is_ended() && session.source() == MeetingEventSource::Poll,
                ),
                None => return Vec::new(),
            }
        };
        if !should_poll {
            return Vec::new();
        }

        let page = self
            .api
            .events(meeting_id, EVENTS_PAGE_SIZE, cursor.as_deref())
            .await;
        let now = now_ms();
        let mut events = Vec::new();
        let mut sessions = lock(&self.sessions);
        // The session may have been left while the page was in flight.
        let Some(session) = sessions.get_mut(meeting_id) else {
            return events;
        };
        match page {
            Ok(page) => {
                session.note_page(
                    now,
                    page.page_token.clone(),
                    page.events.len(),
                    page.has_more,
                );
                for item in &page.events {
                    let report = session.ingest(item, now);
                    if report.duplicate {
                        tracing::debug!(meeting_id, "dropped duplicate meeting activity");
                    }
                    if let Some(kind) = report.unparsed_kind.as_deref() {
                        tracing::warn!(
                            meeting_id,
                            kind,
                            "meeting activity produced no event (payload shape changed?)"
                        );
                    }
                    events.extend(report.events);
                }
            }
            Err(err) => {
                session.note_error(now);
                tracing::warn!(
                    meeting_id,
                    error = %describe_meeting_error(&err),
                    "meeting poll failed"
                );
            }
        }
        events
    }

    /// Route one long-connection `vc.bot.*` frame. Returns whether it was ours.
    pub fn handle_push(self: &Arc<Self>, raw: &Value) -> bool {
        let Some(push) = parse_meeting_push(raw) else {
            return false;
        };
        self.note_push();
        match push {
            MeetingPush::Invited {
                meeting_no,
                inviter_id,
            } => {
                tracing::info!(
                    meeting_no = %meeting_no,
                    inviter = inviter_id.as_deref().unwrap_or_default(),
                    "invited to meeting"
                );
                if self.config.auto_join_on_invite {
                    let manager = Arc::clone(self);
                    tokio::spawn(async move {
                        if let Err(err) = manager.join(&meeting_no, None).await {
                            tracing::warn!(
                                meeting_no = %meeting_no,
                                error = %err,
                                "auto-join failed"
                            );
                        }
                    });
                }
            }
            MeetingPush::Activity(items) => {
                let now = now_ms();
                let mut routed: Vec<(String, Vec<MeetingEvent>)> = Vec::new();
                {
                    let mut sessions = lock(&self.sessions);
                    for item in &items {
                        let Some(meeting_id) = item
                            .meeting
                            .as_ref()
                            .and_then(|meeting| meeting.id.as_deref())
                        else {
                            continue;
                        };
                        let Some(session) = sessions.get_mut(meeting_id) else {
                            // Not a meeting this process manages; ignore.
                            continue;
                        };
                        session.mark_push_active();
                        let report = session.ingest(item, now);
                        if report.events.is_empty() {
                            continue;
                        }
                        match routed.iter_mut().find(|(id, _)| id.as_str() == meeting_id) {
                            Some((_, events)) => events.extend(report.events),
                            None => routed.push((meeting_id.to_string(), report.events)),
                        }
                    }
                }
                for (meeting_id, events) in routed {
                    self.dispatch_events(&meeting_id, events);
                }
            }
            MeetingPush::Ended { meeting_id } => {
                self.end_meeting(&meeting_id);
            }
        }
        // A push can enqueue a debounced transcript whose flush deadline is
        // the only reason the loop needs to wake; without this, a push-only
        // meeting with `stabilizeMs > 0` could sit on the line indefinitely.
        self.wake.notify_one();
        true
    }

    /// Turn in-meeting chat into agent runs.
    fn dispatch_events(self: &Arc<Self>, meeting_id: &str, events: Vec<MeetingEvent>) {
        for event in events {
            let MeetingEvent::Chat(chat) = event else {
                continue;
            };
            // message_type 3 is an in-meeting reaction, not text.
            if chat.message_type == Some(3) {
                continue;
            }
            let prefixes = trigger_prefixes(&self.config.trigger, self.agent.bot_name.as_deref());
            let Some(question) = match_trigger(&chat.content, &prefixes) else {
                continue; // not addressed to us
            };
            if question.is_empty() {
                continue; // addressed but empty
            }
            let used_prefix = prefixes
                .iter()
                .find(|prefix| {
                    chat.content
                        .trim()
                        .to_lowercase()
                        .starts_with(&prefix.to_lowercase())
                })
                .cloned();

            if is_stop_word(&question) {
                let stopped = self.interrupt(meeting_id);
                let text = if stopped {
                    "已中断当前任务。"
                } else {
                    "当前没有正在执行的任务。"
                };
                let manager = Arc::clone(self);
                let meeting_id = meeting_id.to_string();
                tokio::spawn(async move {
                    manager.send_meeting_text(&meeting_id, text).await;
                });
                continue;
            }

            let plan = AnswerPlan {
                meeting_id: meeting_id.to_string(),
                question,
                asked_by: chat.from.name.clone(),
                used_prefix,
                deliver: AnswerDelivery::Broadcast,
            };
            if let Err(busy) = self.spawn_answer(plan) {
                let manager = Arc::clone(self);
                let meeting_id = meeting_id.to_string();
                tokio::spawn(async move {
                    manager.send_meeting_text(&meeting_id, &busy).await;
                });
            }
        }
    }

    /// Spawn one agent run for a meeting, refusing when one is already live.
    ///
    /// The active-run entry is inserted while the map lock is held, so a
    /// concurrent `/meeting stop` either sees the run and aborts it or sees
    /// nothing at all.
    fn spawn_answer(self: &Arc<Self>, plan: AnswerPlan) -> Result<(), String> {
        let scope = meeting_scope_id(&plan.meeting_id);
        let mut runs = lock(&self.runs);
        if runs.contains_key(&scope) {
            return Err(self.busy_reply(plan.used_prefix.as_deref()));
        }
        let id = self.next_run_id.fetch_add(1, Ordering::Relaxed);
        let manager = Arc::clone(self);
        let task_scope = scope.clone();
        // The host registers the task in the bridge's active-run map
        // (`meeting:<id>`) before it can run, so `/stop meeting:<id>` and the
        // manager's own `interrupt` abort the same task. The run guard removes
        // the manager entry on normal completion and on abort.
        let future: Pin<Box<dyn Future<Output = ()> + Send>> = Box::pin(async move {
            let _guard = MeetingRunGuard {
                manager: Arc::clone(&manager),
                scope: task_scope,
                id,
            };
            manager.execute_answer(plan).await;
        });
        let handle = self.host.spawn_tracked_run(&scope, future);
        runs.insert(scope, ActiveMeetingRun { id, handle });
        Ok(())
    }

    /// Run the agent and deliver the answer.
    async fn execute_answer(self: Arc<Self>, plan: AnswerPlan) {
        let Some(snapshot) = self.snapshot(&plan.meeting_id) else {
            if let AnswerDelivery::Caller(sender) = plan.deliver {
                let _ = sender.send("会议已结束。".to_string());
            }
            return;
        };
        let transcript = snapshot.recent_transcript(Some(self.config.transcript.keep));
        let prompt = build_meeting_prompt(
            &plan.question,
            &transcript,
            snapshot.topic.as_deref(),
            plan.asked_by.as_deref(),
        );
        let scope = meeting_scope_id(&snapshot.meeting_id);
        let answer = self.run_agent(&scope, prompt).await;
        let answer = answer.trim().to_string();

        match plan.deliver {
            AnswerDelivery::Caller(sender) => {
                let answer = if answer.is_empty() {
                    "（没有产生回答）".to_string()
                } else {
                    answer
                };
                let _ = sender.send(answer);
            }
            AnswerDelivery::Broadcast => {
                if answer.is_empty() {
                    return;
                }
                if matches!(self.config.respond_in, RespondIn::Meeting | RespondIn::Both) {
                    self.send_meeting_text(&snapshot.meeting_id, &answer).await;
                }
                if matches!(self.config.respond_in, RespondIn::Im | RespondIn::Both) {
                    match snapshot.origin_chat_id.as_deref() {
                        Some(chat_id) => self.host.send_im_text(chat_id, &answer).await,
                        None => tracing::warn!(
                            meeting_id = %snapshot.meeting_id,
                            "respondIn asks for IM delivery but the meeting has no origin chat"
                        ),
                    }
                }
            }
        }
    }

    /// Submit one prompt to the profile's agent and drain it to a string.
    async fn run_agent(&self, scope: &str, prompt: String) -> String {
        self.run_agent_with_wait(scope, prompt, RUN_SLOT_WAIT).await
    }

    /// [`Self::run_agent`] with an injectable slot-wait budget (tests use a
    /// short one to exercise the timeout without sleeping for 30s).
    ///
    /// The global run permit is held for the whole agent run and dropped on
    /// completion, spawn error, timeout or task abort. When no permit is free
    /// within the budget the user gets [`RUN_SLOT_TIMEOUT_REPLY`] instead of
    /// an over-cap spawn.
    async fn run_agent_with_wait(
        &self,
        scope: &str,
        prompt: String,
        slot_wait: Duration,
    ) -> String {
        let _permit = match self.run_limit.as_ref() {
            Some(limiter) => match limiter.acquire_within(slot_wait).await {
                Some(permit) => Some(permit),
                None => {
                    tracing::warn!(
                        scope,
                        timeout_ms = slot_wait.as_millis(),
                        "meeting run timed out waiting for a global run slot"
                    );
                    return RUN_SLOT_TIMEOUT_REPLY.to_string();
                }
            },
            None => None,
        };
        let cwd = self.host.workspace_cwd(scope).await;
        let cwd_str = cwd.to_string_lossy().into_owned();
        let resume = self.host.resume_session(scope, &cwd_str).await;
        let spec = AgentSpec {
            kind: self.agent.kind,
            binary: self.agent.binary.clone(),
            cwd,
            prompt,
            session_id: resume.clone(),
            model: None,
            permissions: self.agent.permissions,
            bot_open_id: self.agent.bot_open_id.clone(),
            bot_name: self.agent.bot_name.clone(),
        };
        tracing::info!(scope, resume = resume.is_some(), cwd = %cwd_str, "starting meeting run");

        let mut view = RunView::default();
        let mut process = match AgentProcess::spawn(spec).await {
            Ok(process) => process,
            Err(err) => {
                tracing::warn!(scope, error = %err, "cannot spawn agent for meeting run");
                return format!("无法启动 agent：{err}");
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

        if let Some(key) = view.session_key().map(str::to_string) {
            self.host.save_session(scope, &key, &cwd_str).await;
        }
        view.best_answer().trim().to_string()
    }

    async fn send_meeting_text(&self, meeting_id: &str, text: &str) {
        if let Err(err) = self.api.send_text(meeting_id, text).await {
            tracing::warn!(
                meeting_id,
                error = %describe_meeting_error(&err),
                "cannot send in-meeting message"
            );
        }
    }

    /// Summarize a finished meeting to IM (port of `summarizeEndedMeeting`).
    fn spawn_summary(self: &Arc<Self>, snapshot: SessionSnapshot) {
        if !self.config.summary_on_end {
            return;
        }
        if snapshot.transcript.is_empty() {
            tracing::info!(
                meeting_id = %snapshot.meeting_id,
                "summary skipped: empty transcript"
            );
            return;
        }
        let Some(target) = resolve_summary_target(
            self.config.summary_target,
            snapshot.origin_chat_id.as_deref(),
            self.config.summary_owner_open_id.as_deref(),
        ) else {
            tracing::warn!(
                meeting_id = %snapshot.meeting_id,
                "summary skipped: no origin chat and no owner DM"
            );
            return;
        };
        let title = snapshot.title();
        let manager = Arc::clone(self);
        let summary_target = target.clone();
        let scope = meeting_scope_id(&snapshot.meeting_id);
        let id = self.next_run_id.fetch_add(1, Ordering::Relaxed);
        let task_scope = scope.clone();
        // Summaries are meeting runs too: register them under the same scope
        // so `/stop meeting:<id>` can abort a long summary, and so shutdown's
        // `interrupt_all` cancels it.
        let future: Pin<Box<dyn Future<Output = ()> + Send>> = Box::pin(async move {
            let _guard = MeetingRunGuard {
                manager: Arc::clone(&manager),
                scope: task_scope.clone(),
                id,
            };
            let transcript = snapshot.recent_transcript(Some(manager.config.transcript.keep));
            let prompt = build_meeting_prompt(
                SUMMARY_QUESTION,
                &transcript,
                snapshot.topic.as_deref(),
                None,
            );
            let answer = manager.run_agent(&task_scope, prompt).await;
            let answer = answer.trim().to_string();
            if answer.is_empty() {
                return;
            }
            manager
                .host
                .send_im_text(&summary_target.to, &format_summary(&title, &answer))
                .await;
            tracing::info!(
                meeting_id = %snapshot.meeting_id,
                target = summary_target.kind.as_str(),
                fell_back = summary_target.fell_back,
                "meeting summary sent"
            );
        });
        // Hold the manager map across spawn + insert: the task's guard takes
        // the same lock, so it cannot remove an entry that was never inserted.
        let mut runs = lock(&self.runs);
        // `end_meeting` already interrupted the answer run; this closes the
        // small race where a new answer was spawned between that interrupt
        // and the summary start.
        if let Some(previous) = runs.remove(&scope) {
            previous.handle.abort();
        }
        let handle = self.host.spawn_tracked_run(&scope, future);
        runs.insert(scope, ActiveMeetingRun { id, handle });
    }

    fn busy_reply(&self, used_prefix: Option<&str>) -> String {
        let prefix = used_prefix
            .map(str::to_string)
            .or_else(|| {
                trigger_prefixes(&self.config.trigger, self.agent.bot_name.as_deref())
                    .into_iter()
                    .next()
            })
            .unwrap_or_else(|| self.config.trigger.clone());
        format!("上一个任务还在执行。发「{prefix} stop」可以中断它，然后再问我。")
    }

    // -- commands -----------------------------------------------------------

    /// `/meeting …` from IM. Returns the reply text.
    pub async fn handle_command(
        self: &Arc<Self>,
        args: &str,
        sender_id: &str,
        chat_id: &str,
    ) -> String {
        self.handle_command_with_reply(args, sender_id, chat_id, ImReplyTarget::default())
            .await
    }

    /// [`Self::handle_command`] with an explicit IM reply target.
    ///
    /// `/meeting ask|notes` acknowledges immediately; the acknowledgement is a
    /// command reply and must follow the sender's topic, so the bridge passes
    /// the message scope and parent id here.
    pub async fn handle_command_with_reply(
        self: &Arc<Self>,
        args: &str,
        _sender_id: &str,
        chat_id: &str,
        reply_target: ImReplyTarget<'_>,
    ) -> String {
        let (sub, rest) = split_command(args);
        match sub {
            "" | "status" => self.status_text(),
            "join" => {
                let meeting_no = strip_whitespace(rest);
                if !larkline_lark::meeting::is_meeting_no(&meeting_no) {
                    return "用法：`/meeting join <9位会议号>`（只接受 9 位纯数字，不是会议链接）"
                        .to_string();
                }
                match self.join(&meeting_no, Some(chat_id)).await {
                    Ok(outcome) => {
                        if outcome.already_joined {
                            return format!(
                                "已经在会议 **{}** 里了（会议号 {}）。\n`/meeting notes` 总结 · `/meeting leave` 离会",
                                outcome.session.title(),
                                outcome.session.meeting_no
                            );
                        }
                        format!(
                            "✅ 已入会 **{}**\n会议号 {} · 会中发 `{} 你的问题` 可以问我\n`/meeting notes` 总结 · `/meeting leave` 离会",
                            outcome.session.title(),
                            outcome.session.meeting_no,
                            self.config.trigger
                        )
                    }
                    Err(err) => format!("入会失败：{err}"),
                }
            }
            "leave" => match self.pick(chat_id, rest) {
                Ok(session) => {
                    self.leave(&session.meeting_id).await;
                    format!("✅ 已离会（会议号 {}）", session.meeting_no)
                }
                Err(message) => message,
            },
            "transcript" => match self.pick(chat_id, rest) {
                Ok(session) => self.transcript_text(&session),
                Err(message) => message,
            },
            "stop" => match self.pick(chat_id, rest) {
                Ok(session) => {
                    if self.interrupt(&session.meeting_id) {
                        "✅ 已中断该会议的当前任务。".to_string()
                    } else {
                        "该会议当前没有正在执行的任务。".to_string()
                    }
                }
                Err(message) => message,
            },
            "notes" | "ask" => {
                let question = if sub == "notes" {
                    NOTES_QUESTION.to_string()
                } else {
                    rest.trim().to_string()
                };
                if question.is_empty() {
                    return "用法：`/meeting ask <问题>`".to_string();
                }
                let session = match self.pick(chat_id, if sub == "notes" { rest } else { "" }) {
                    Ok(session) => session,
                    Err(message) => return message,
                };
                // Immediate acknowledgement: the run itself can take minutes,
                // and this command is detached from the event loop.
                self.host
                    .send_im_text_reply(
                        chat_id,
                        reply_target,
                        if sub == "notes" {
                            "正在总结会议…"
                        } else {
                            "正在思考…"
                        },
                    )
                    .await;
                let (sender, receiver) = oneshot::channel();
                let plan = AnswerPlan {
                    meeting_id: session.meeting_id.clone(),
                    question,
                    asked_by: None,
                    used_prefix: None,
                    deliver: AnswerDelivery::Caller(sender),
                };
                if let Err(busy) = self.spawn_answer(plan) {
                    return busy;
                }
                match receiver.await {
                    Ok(answer) => answer,
                    Err(_) => "任务已被中断。".to_string(),
                }
            }
            _ => "用法：\n\
                 `/meeting` — 状态\n\
                 `/meeting join <9位会议号>` — 让 bot 入会\n\
                 `/meeting leave [会议号]` — 离会\n\
                 `/meeting notes [会议号]` — 基于字幕做纪要（只发给你）\n\
                 `/meeting stop [会议号]` — 中断该会议卡住的任务\n\
                 `/meeting transcript [会议号]` — 看 agent 实际拿到的字幕上下文\n\
                 `/meeting ask <问题>` — 带会议上下文提问"
                .to_string(),
        }
    }

    fn status_text(&self) -> String {
        let sessions = self.statuses();
        let push = self.push_health();
        let push_line = if push.hooked {
            format!(
                "推送：已挂载，累计收到 {} 条{}",
                push.received,
                if push.received == 0 {
                    "（尚未收到，可能还没在后台订阅 vc.bot.* 事件）"
                } else {
                    ""
                }
            )
        } else {
            format!(
                "推送：未挂载（{}），仅靠轮询",
                push.reason.as_deref().unwrap_or("未知原因")
            )
        };

        if sessions.is_empty() {
            return [
                "当前没有在跟的会议。".to_string(),
                push_line,
                String::new(),
                "`/meeting join <9位会议号>` 开始。".to_string(),
            ]
            .join("\n");
        }
        let mut lines = vec![format!("正在跟 {} 场会议：", sessions.len())];
        for session in &sessions {
            lines.push(format!(
                "- **{}**（{}）· 来源 {} · 字幕 {} 条 · 参会 {} 人",
                session.topic.as_deref().unwrap_or(&session.meeting_no),
                session.meeting_no,
                if session.source == MeetingEventSource::Push {
                    "推送"
                } else {
                    "轮询"
                },
                session.transcript_lines,
                session.participants,
            ));
        }
        lines.push(String::new());
        lines.push(push_line);
        lines.join("\n")
    }

    fn transcript_text(&self, session: &SessionSnapshot) -> String {
        let lines: Vec<String> = session
            .transcript
            .iter()
            .map(TranscriptLine::render)
            .collect();
        if lines.is_empty() {
            return "字幕缓冲为空 —— agent 这次拿到的会议上下文是「（暂无字幕）」。\n\
                    注意：同一场会里 agent 复用一个会话，**早先轮次**的字幕仍留在它自己的对话历史里，\
                    所以它可能引用缓冲里已经没有的内容。`/new` 可以清掉该会话记忆。"
                .to_string();
        }
        let tail = lines.len().saturating_sub(30);
        let mut reply = vec![format!(
            "字幕缓冲共 {} 条{}：",
            lines.len(),
            if tail > 0 {
                "，以下是最近 30 条".to_string()
            } else {
                String::new()
            }
        )];
        reply.push("```".to_string());
        reply.extend(lines[tail..].iter().cloned());
        reply.push("```".to_string());
        reply.join("\n")
    }

    /// Resolve which meeting a command targets when the bot may be in several.
    ///
    /// Order: explicit 9-digit number → the only meeting → the only meeting
    /// joined from *this* chat → otherwise ask, listing the candidates.
    fn pick(&self, chat_id: &str, explicit: &str) -> Result<SessionSnapshot, String> {
        let wanted = strip_whitespace(explicit);
        if !wanted.is_empty() {
            return self.find_by_meeting_no(&wanted).ok_or_else(|| {
                format!("没找到会议号 {wanted} 对应的会议。用 `/meeting` 看当前在跟哪几场。")
            });
        }
        let all = self.all_snapshots();
        match all.len() {
            0 => Err("当前没有在跟的会议。先 `/meeting join <9位会议号>`。".to_string()),
            1 => Ok(all.into_iter().next().expect("length checked")),
            _ => {
                let from_here: Vec<&SessionSnapshot> = all
                    .iter()
                    .filter(|session| session.origin_chat_id.as_deref() == Some(chat_id))
                    .collect();
                if from_here.len() == 1 {
                    return Ok(from_here[0].clone());
                }
                let list = all
                    .iter()
                    .map(|session| {
                        format!(
                            "- {}{}",
                            session.meeting_no,
                            session
                                .topic
                                .as_deref()
                                .map(|topic| format!("（{topic}）"))
                                .unwrap_or_default()
                        )
                    })
                    .collect::<Vec<_>>()
                    .join("\n");
                Err(format!(
                    "当前在跟 {} 场会议，请指定会议号：\n{list}\n\n例如 `/meeting notes {}`",
                    all.len(),
                    all[0].meeting_no
                ))
            }
        }
    }
}

/// Shutdown handle for the meeting poll task. Dropping it stops the loop.
pub struct MeetingHandle {
    manager: Arc<MeetingManager>,
    task: JoinHandle<()>,
}

impl MeetingHandle {
    /// Stop the poll/dispatch task. Safe to call more than once.
    pub fn stop(&self) {
        self.manager.request_shutdown();
        self.task.abort();
    }
}

impl Drop for MeetingHandle {
    fn drop(&mut self) {
        self.stop();
    }
}

async fn run_loop(manager: Arc<MeetingManager>, mut shutdown: watch::Receiver<bool>) {
    loop {
        let wait = manager.next_wakeup();
        tokio::select! {
            changed = shutdown.changed() => {
                if changed.is_err() || *shutdown.borrow() {
                    return;
                }
            }
            _ = manager.wake.notified() => {}
            _ = async {
                match wait {
                    Some(wait) => tokio::time::sleep(wait).await,
                    None => std::future::pending::<()>().await,
                }
            } => {}
        }
        manager.tick().await;
    }
}

fn split_command(args: &str) -> (&str, &str) {
    let args = args.trim();
    match args.find(char::is_whitespace) {
        Some(index) => (&args[..index], args[index..].trim()),
        None => (args, ""),
    }
}

fn strip_whitespace(value: &str) -> String {
    value
        .chars()
        .filter(|character| !character.is_whitespace())
        .collect()
}

fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_millis() as i64)
        .unwrap_or(0)
}

fn lock<T>(mutex: &StdMutex<T>) -> std::sync::MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::collections::VecDeque;
    use std::sync::Mutex as StdMutex;

    use larkline_core::config::Access;
    use larkline_core::{AppPaths, TenantBrand};
    use larkline_lark::Channel;
    use serde_json::json;

    use crate::run::BridgeMeetingHost;

    // -----------------------------------------------------------------------
    // Fakes: network + bridge seam
    // -----------------------------------------------------------------------

    #[derive(Default)]
    struct FakeApiState {
        joins: VecDeque<RestResult<JoinResult>>,
        pages: HashMap<String, VecDeque<RestResult<EventsPage>>>,
        calls: Vec<(String, Option<String>)>,
        sent: Vec<(String, String)>,
        leaves: Vec<String>,
    }

    #[derive(Clone, Default)]
    struct FakeApi {
        state: Arc<StdMutex<FakeApiState>>,
    }

    impl FakeApi {
        fn new() -> Self {
            Self::default()
        }

        fn push_join(&self, result: RestResult<JoinResult>) {
            lock(&self.state).joins.push_back(result);
        }

        fn push_page(&self, meeting_id: &str, page: EventsPage) {
            lock(&self.state)
                .pages
                .entry(meeting_id.to_string())
                .or_default()
                .push_back(Ok(page));
        }

        fn push_error(&self, meeting_id: &str, message: &str) {
            lock(&self.state)
                .pages
                .entry(meeting_id.to_string())
                .or_default()
                .push_back(Err(RestError::Message(message.to_string())));
        }

        fn calls(&self) -> Vec<(String, Option<String>)> {
            lock(&self.state).calls.clone()
        }

        fn sent(&self) -> Vec<(String, String)> {
            lock(&self.state).sent.clone()
        }

        fn leaves(&self) -> Vec<String> {
            lock(&self.state).leaves.clone()
        }
    }

    #[async_trait]
    impl MeetingApi for FakeApi {
        async fn join(&self, _meeting_no: &str) -> RestResult<JoinResult> {
            lock(&self.state)
                .joins
                .pop_front()
                .unwrap_or_else(|| Err(RestError::Message("no scripted join".to_string())))
        }

        async fn leave(&self, meeting_id: &str) -> RestResult<()> {
            lock(&self.state).leaves.push(meeting_id.to_string());
            Ok(())
        }

        async fn send_text(&self, meeting_id: &str, text: &str) -> RestResult<()> {
            lock(&self.state)
                .sent
                .push((meeting_id.to_string(), text.to_string()));
            Ok(())
        }

        async fn events(
            &self,
            meeting_id: &str,
            page_size: u32,
            page_token: Option<&str>,
        ) -> RestResult<EventsPage> {
            let mut state = lock(&self.state);
            state
                .calls
                .push((meeting_id.to_string(), page_token.map(str::to_string)));
            assert_eq!(page_size, EVENTS_PAGE_SIZE);
            state
                .pages
                .get_mut(meeting_id)
                .and_then(VecDeque::pop_front)
                .unwrap_or_else(|| Ok(EventsPage::default()))
        }
    }

    #[derive(Default)]
    struct FakeHost {
        im: StdMutex<Vec<(String, String)>>,
        replies: StdMutex<Vec<(String, Option<String>, Option<String>, String)>>,
    }

    impl FakeHost {
        fn im(&self) -> Vec<(String, String)> {
            lock(&self.im).clone()
        }

        fn replies(&self) -> Vec<(String, Option<String>, Option<String>, String)> {
            lock(&self.replies).clone()
        }
    }

    #[async_trait]
    impl MeetingHost for FakeHost {
        async fn workspace_cwd(&self, _scope: &str) -> PathBuf {
            std::env::temp_dir()
        }

        async fn resume_session(&self, _scope: &str, _cwd: &str) -> Option<String> {
            None
        }

        async fn save_session(&self, _scope: &str, _session_key: &str, _cwd: &str) {
            // Every fake run fails at spawn, so there is no session id to keep.
        }

        async fn send_im_text(&self, to: &str, text: &str) {
            lock(&self.im).push((to.to_string(), text.to_string()));
        }

        async fn send_im_text_reply(&self, to: &str, target: ImReplyTarget<'_>, text: &str) {
            lock(&self.replies).push((
                to.to_string(),
                target.scope.map(str::to_string),
                target.parent_message_id.map(str::to_string),
                text.to_string(),
            ));
            self.send_im_text(to, text).await;
        }
    }

    // -----------------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------------

    fn join_result(meeting_id: &str, meeting_no: &str) -> JoinResult {
        JoinResult {
            meeting_id: meeting_id.to_string(),
            meeting_no: meeting_no.to_string(),
            topic: Some("周会".to_string()),
        }
    }

    fn session(config: MeetingConfig) -> MeetingSession {
        MeetingSession::new(MeetingSessionConfig {
            meeting_id: "70001".to_string(),
            meeting_no: "123456789".to_string(),
            topic: Some("周会".to_string()),
            origin_chat_id: None,
            config,
            bot_open_id: None,
            started_at_ms: 0,
        })
    }

    fn manager(
        config: MeetingConfig,
        api: Arc<FakeApi>,
        host: Arc<FakeHost>,
    ) -> Arc<MeetingManager> {
        manager_with_binary(
            config,
            api,
            host,
            // A missing binary makes the run fail fast, which is exactly
            // what the delivery-path tests need offline.
            PathBuf::from("/nonexistent/larkline-test-agent"),
        )
    }

    fn manager_with_binary(
        config: MeetingConfig,
        api: Arc<FakeApi>,
        host: Arc<FakeHost>,
        binary: PathBuf,
    ) -> Arc<MeetingManager> {
        Arc::new(MeetingManager::new(
            config,
            AgentSettings {
                kind: AgentKind::Claude,
                binary,
                permissions: RunPermissions::default(),
                bot_open_id: None,
                bot_name: None,
            },
            api,
            host,
        ))
    }

    /// Manager with a built-in limiter; returns both so the test can hold or
    /// inspect slots.
    fn manager_with_limiter(
        config: MeetingConfig,
        api: Arc<FakeApi>,
        host: Arc<FakeHost>,
        binary: PathBuf,
        cap: u32,
    ) -> (Arc<MeetingManager>, Arc<RunLimiter>) {
        let limiter = Arc::new(RunLimiter::new(cap));
        let manager = Arc::new(
            MeetingManager::new(
                config,
                AgentSettings {
                    kind: AgentKind::Claude,
                    binary,
                    permissions: RunPermissions::default(),
                    bot_open_id: None,
                    bot_name: None,
                },
                api,
                host,
            )
            .with_run_limit(Arc::clone(&limiter)),
        );
        (manager, limiter)
    }

    /// Minimal real bridge, used by the `/stop meeting:<id>` integration test.
    fn bridge_with_agent(root: &std::path::Path, binary: PathBuf) -> Arc<crate::run::Bridge> {
        let paths = AppPaths::resolve(root, "test").expect("app paths");
        let profile = ProfileConfig {
            agent_kind: AgentKind::Claude,
            ..ProfileConfig::default()
        };
        Arc::new(
            crate::run::Bridge::new(
                Channel::new("cli_test_app", "secret", TenantBrand::Feishu),
                profile,
                binary,
                root.join("workspace"),
                None,
                None,
                paths,
                None,
            )
            .expect("bridge"),
        )
    }

    /// Serialize fake-script write + spawn across the test binary; see
    /// [`crate::TEST_FAKE_AGENT_LOCK`].
    #[cfg(unix)]
    async fn fake_agent_lock() -> tokio::sync::MutexGuard<'static, ()> {
        crate::TEST_FAKE_AGENT_LOCK.lock().await
    }

    #[cfg(unix)]
    fn fake_agent(dir: &std::path::Path, sleep_ms: u64) -> PathBuf {
        use std::os::unix::fs::PermissionsExt;

        let path = dir.join("fake-meeting-agent");
        let script = format!(
            "#!/bin/sh\ncat > /dev/null\nsleep {}\nprintf '%s\\n' '{{\"type\":\"system\",\"subtype\":\"init\",\"session_id\":\"sess-meeting\",\"cwd\":\"/work\",\"model\":\"fake\"}}'\nprintf '%s\\n' '{{\"type\":\"assistant\",\"message\":{{\"id\":\"m1\",\"content\":[{{\"type\":\"text\",\"text\":\"meeting done\"}}]}}}}'\nprintf '%s\\n' '{{\"type\":\"result\",\"session_id\":\"sess-meeting\",\"usage\":{{\"input_tokens\":1,\"output_tokens\":1}}}}'\n",
            sleep_ms as f64 / 1000.0
        );
        std::fs::write(&path, script).unwrap();
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o755)).unwrap();
        path
    }

    /// Fake agent that blocks until the test creates `release`, after writing
    /// `started`; makes the permit-held assertion deterministic.
    #[cfg(unix)]
    fn blocking_fake_agent(
        dir: &std::path::Path,
        started: &std::path::Path,
        release: &std::path::Path,
    ) -> PathBuf {
        use std::os::unix::fs::PermissionsExt;

        let path = dir.join("fake-meeting-agent-blocking");
        let script = format!(
            "#!/bin/sh\ncat > /dev/null\ntouch '{}'\nwhile [ ! -f '{}' ]; do sleep 0.01; done\nprintf '%s\\n' '{{\"type\":\"system\",\"subtype\":\"init\",\"session_id\":\"sess-meeting\",\"cwd\":\"/work\",\"model\":\"fake\"}}'\nprintf '%s\\n' '{{\"type\":\"assistant\",\"message\":{{\"id\":\"m1\",\"content\":[{{\"type\":\"text\",\"text\":\"meeting done\"}}]}}}}'\nprintf '%s\\n' '{{\"type\":\"result\",\"session_id\":\"sess-meeting\",\"usage\":{{\"input_tokens\":1,\"output_tokens\":1}}}}'\n",
            started.display(),
            release.display()
        );
        std::fs::write(&path, script).unwrap();
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o755)).unwrap();
        path
    }

    fn activity(value: Value) -> MeetingActivityItem {
        serde_json::from_value(value).expect("valid activity item")
    }

    fn transcript_item(
        event_id: Option<&str>,
        sentence_id: &str,
        text: &str,
        speaker: &str,
    ) -> MeetingActivityItem {
        activity(json!({
            "event_id": event_id,
            "meeting": {"id": "70001"},
            "activity_event_type": "transcript_received",
            "transcript_received_items": [{
                "sentence_id": sentence_id,
                "text": text,
                "speaker": {"open_id": format!("ou_{speaker}"), "name": speaker},
                "start_time_ms": 1000
            }]
        }))
    }

    fn chat_item(content: &str, message_type: Option<i64>, from: &str) -> MeetingActivityItem {
        activity(json!({
            "meeting": {"id": "70001"},
            "activity_event_type": "chat_received",
            "chat_received_items": [{
                "content": content,
                "sender": {"open_id": format!("ou_{from}"), "name": from},
                "message_type": message_type
            }]
        }))
    }

    fn page(items: Vec<MeetingActivityItem>, token: Option<&str>, has_more: bool) -> EventsPage {
        EventsPage {
            events: items,
            page_token: token.map(str::to_string),
            has_more,
        }
    }

    fn push(event_type: &str, body: Value) -> Value {
        json!({
            "header": {"event_type": event_type, "event_id": "evt_1"},
            "event": body,
        })
    }

    fn activity_frame(items: Vec<MeetingActivityItem>) -> Value {
        push(
            "vc.bot.meeting_activity_v1",
            json!({ "meeting_activity_items": items }),
        )
    }

    async fn wait_until(mut condition: impl FnMut() -> bool) {
        for _ in 0..300 {
            if condition() {
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        panic!("condition was not met in time");
    }

    // -----------------------------------------------------------------------
    // Config
    // -----------------------------------------------------------------------

    #[test]
    fn meeting_config_defaults_match_typescript() {
        let config = MeetingConfig::from_value(None);
        assert_eq!(config, MeetingConfig::default());
        assert!(!config.enabled);
        assert!(!config.auto_join_on_invite);
        assert_eq!(config.transcript.keep, 200);
        assert_eq!(config.transcript.stabilize_ms, 0);
        assert_eq!(config.respond_in, RespondIn::Meeting);
        assert_eq!(config.trigger, "@bot");
        assert_eq!(config.poll_interval_ms, 3_000);
        assert!(!config.summary_on_end);
        assert_eq!(config.summary_target, SummaryTarget::Origin);
        assert_eq!(config.bot_open_id, None);
        assert_eq!(config.summary_owner_open_id, None);
    }

    #[test]
    fn meeting_config_reads_extra_keys_and_clamps_numbers() {
        let config = MeetingConfig::from_value(Some(&json!({
            "enabled": true,
            "autoJoinOnInvite": true,
            "transcript": {"keep": 1, "stabilizeMs": 99_999},
            "respondIn": "both",
            "trigger": "  @小助手  ",
            "pollIntervalMs": 10,
            "summaryOnEnd": true,
            "summaryTarget": "owner",
            "botOpenId": "ou_bot",
            "botName": "小助手",
            "summaryOwnerOpenId": "ou_owner"
        })));
        assert!(config.enabled);
        assert!(config.auto_join_on_invite);
        assert_eq!(config.transcript.keep, 10);
        assert_eq!(config.transcript.stabilize_ms, 30_000);
        assert_eq!(config.respond_in, RespondIn::Both);
        assert_eq!(config.trigger, "@小助手");
        assert_eq!(config.poll_interval_ms, 1_000);
        assert!(config.summary_on_end);
        assert_eq!(config.summary_target, SummaryTarget::Owner);
        assert_eq!(config.bot_open_id.as_deref(), Some("ou_bot"));
        assert_eq!(config.bot_name.as_deref(), Some("小助手"));
        assert_eq!(config.summary_owner_open_id.as_deref(), Some("ou_owner"));

        let clamped = MeetingConfig::from_value(Some(&json!({
            "transcript": {"keep": 99_999, "stabilizeMs": -1},
            "pollIntervalMs": 999_999,
            "respondIn": "telepathy",
            "trigger": "   "
        })));
        assert_eq!(clamped.transcript.keep, 2_000);
        assert_eq!(clamped.transcript.stabilize_ms, 0);
        assert_eq!(clamped.poll_interval_ms, 60_000);
        assert_eq!(clamped.respond_in, RespondIn::Meeting);
        assert_eq!(clamped.trigger, "@bot");
    }

    #[test]
    fn meeting_config_is_read_from_profile_extra() {
        let mut profile = ProfileConfig::default();
        assert!(!meeting_enabled(&profile));

        profile.extra.insert(
            MEETING_CONFIG_KEY.to_string(),
            json!({"enabled": true, "trigger": "@小助手"}),
        );
        let config = config_from_profile(&profile);
        assert!(config.enabled);
        assert_eq!(config.trigger, "@小助手");
        assert!(meeting_enabled(&profile));

        // A non-object `meeting` value falls back to the defaults.
        profile
            .extra
            .insert(MEETING_CONFIG_KEY.to_string(), json!("yes"));
        assert!(!meeting_enabled(&profile));
    }

    #[test]
    fn admin_gate_matches_the_typescript_command_policy() {
        let mut profile = ProfileConfig::default();
        // Default profile: empty admin and allowlist lists -> fail closed.
        assert!(!can_run_meeting_command(&profile, "ou_1"));

        profile.access = Some(Access {
            admins: vec!["ou_admin".to_string()],
            allowed_users: vec!["ou_user".to_string()],
            ..Access::default()
        });
        assert!(can_run_meeting_command(&profile, "ou_admin"));
        assert!(!can_run_meeting_command(&profile, "ou_user"));

        profile.access = Some(Access {
            allowed_users: vec!["ou_user".to_string()],
            ..Access::default()
        });
        assert!(can_run_meeting_command(&profile, "ou_user"));
        assert!(!can_run_meeting_command(&profile, "ou_other"));
    }

    // -----------------------------------------------------------------------
    // Event unpacking
    // -----------------------------------------------------------------------

    #[test]
    fn unpack_activity_normalizes_every_supported_event() {
        let transcript = activity(json!({
            "activity_event_type": "transcript_received",
            "transcript_received_items": [
                {
                    "sentence_id": "s1",
                    "text": "大家好",
                    "speaker": {"open_id": "ou_bot", "name": "小助手"},
                    "start_time_ms": "1000",
                    "end_time_ms": 2000,
                    "language": "zh"
                },
                {
                    "sentence_id": 7,
                    "content": "第二句",
                    "user": {"user_id": "u2", "user_name": "张三"}
                }
            ]
        }));
        let events = unpack_activity(&transcript, Some("ou_bot"));
        assert_eq!(events.len(), 2);
        let MeetingEvent::Transcript(first) = &events[0] else {
            panic!("expected a transcript event");
        };
        assert!(first.self_echo);
        assert_eq!(first.text, "大家好");
        assert_eq!(first.start_ms, Some(1000));
        assert_eq!(first.end_ms, Some(2000));
        assert_eq!(first.language.as_deref(), Some("zh"));
        let MeetingEvent::Transcript(second) = &events[1] else {
            panic!("expected a transcript event");
        };
        assert_eq!(second.sentence_id, "7");
        assert!(!second.self_echo);
        assert_eq!(second.speaker.name.as_deref(), Some("张三"));

        // The payload container is accepted for every type.
        let chat = activity(json!({
            "event_type": "chat_received",
            "payload": {"chat_received_items": [{
                "content": "hi",
                "sender": {"open_id": "ou_u", "name": "李四"},
                "message_type": "3"
            }]}
        }));
        let events = unpack_activity(&chat, None);
        let MeetingEvent::Chat(chat) = &events[0] else {
            panic!("expected a chat event");
        };
        assert_eq!(chat.content, "hi");
        assert_eq!(chat.message_type, Some(3));
        assert_eq!(chat.from.name.as_deref(), Some("李四"));

        let participant = activity(json!({
            "activity_event_type": "participant_left",
            "participant_left_items": [{
                "user": {"open_id": "ou_x", "name": "王五"},
                "leave_reason": "left"
            }]
        }));
        let MeetingEvent::Participant(participant) = &unpack_activity(&participant, None)[0] else {
            panic!("expected a participant event");
        };
        assert_eq!(participant.action, ParticipantAction::Left);
        assert_eq!(participant.user.name.as_deref(), Some("王五"));
        assert_eq!(participant.leave_reason.as_deref(), Some("left"));

        let share = activity(json!({
            "activity_event_type": "magic_share_started",
            "magic_share_started_items": [{"url": "https://example.test/doc", "title": "设计稿"}]
        }));
        let MeetingEvent::Share(share) = &unpack_activity(&share, None)[0] else {
            panic!("expected a share event");
        };
        assert_eq!(share.action, ShareAction::Started);
        assert_eq!(share.title.as_deref(), Some("设计稿"));

        let unknown = activity(json!({
            "activity_event_type": "not_a_meeting_event",
            "not_a_meeting_event_items": [{"x": 1}]
        }));
        assert!(unpack_activity(&unknown, None).is_empty());
    }

    // -----------------------------------------------------------------------
    // Session state: dedupe, transcript stabilization, poll schedule
    // -----------------------------------------------------------------------

    #[test]
    fn session_dedupes_event_ids_and_content_keys() {
        let mut session = session(MeetingConfig::default());
        let item = transcript_item(Some("e1"), "s1", "你好", "Alice");

        let first = session.ingest(&item, 0);
        assert!(!first.duplicate);
        assert_eq!(first.events.len(), 1);
        let again = session.ingest(&item, 0);
        assert!(again.duplicate);
        assert!(again.events.is_empty());

        // No event_id: the content-derived key still collapses a replay.
        let no_id = chat_item("晚上好", None, "Bob");
        assert!(!session.ingest(&no_id, 0).duplicate);
        assert!(session.ingest(&no_id, 0).duplicate);

        let status = session.status();
        assert_eq!(status.ingested, 2);
        assert_eq!(status.transcript_lines, 1);
        assert_eq!(status.event_counts.get("transcript_received"), Some(&1));
        assert_eq!(status.event_counts.get("chat_received"), Some(&1));

        // An item that yields no event is still counted, marked with `?`.
        let unparsed = activity(json!({"activity_event_type": "magic_share_started"}));
        let report = session.ingest(&unparsed, 0);
        assert_eq!(report.unparsed_kind.as_deref(), Some("magic_share_started"));
        assert_eq!(
            session.status().event_counts.get("?magic_share_started"),
            Some(&1)
        );
    }

    #[test]
    fn transcript_debounce_replaces_the_sentence_in_place() {
        let mut config = MeetingConfig::default();
        config.transcript.stabilize_ms = 500;
        config.transcript.keep = 2;
        let mut session = session(config);

        let report = session.ingest(&transcript_item(Some("e1"), "s1", "你", "Alice"), 0);
        assert!(
            report.events.is_empty(),
            "debounced lines are not emitted yet"
        );
        assert!(session.flush_due_transcripts(499).is_empty());
        let committed = session.flush_due_transcripts(500);
        assert_eq!(committed.len(), 1);
        assert_eq!(committed[0].text, "你");
        assert_eq!(session.recent_transcript(None), vec!["Alice: 你"]);

        let report = session.ingest(&transcript_item(Some("e2"), "s1", "你好", "Alice"), 600);
        assert!(report.events.is_empty());
        assert_eq!(session.next_transcript_deadline_ms(), Some(1100));
        session.flush_due_transcripts(1100);
        assert_eq!(session.recent_transcript(None), vec!["Alice: 你好"]);

        // Same text again (new event id): an exact repeat is not rescheduled.
        let report = session.ingest(&transcript_item(Some("e3"), "s1", "你好", "Alice"), 1200);
        assert!(report.events.is_empty());
        assert_eq!(session.pending_transcripts(), 0);

        // The keep window drops the oldest committed sentence.
        session.ingest(&transcript_item(Some("e4"), "s2", "第二句", "Bob"), 2000);
        session.ingest(&transcript_item(Some("e5"), "s3", "第三句", "Carol"), 2000);
        session.flush_due_transcripts(2500);
        assert_eq!(session.transcript_len(), 2);
        assert_eq!(
            session.recent_transcript(None),
            vec!["Bob: 第二句", "Carol: 第三句"]
        );
    }

    #[test]
    fn self_echo_transcripts_are_dropped() {
        let mut session = MeetingSession::new(MeetingSessionConfig {
            meeting_id: "70001".to_string(),
            meeting_no: "123456789".to_string(),
            topic: None,
            origin_chat_id: None,
            config: MeetingConfig::default(),
            bot_open_id: Some("ou_bot".to_string()),
            started_at_ms: 0,
        });
        let echo = transcript_item(Some("e1"), "s1", "这是我自己说的", "bot");
        let report = session.ingest(&echo, 0);
        assert!(report.events.is_empty());
        assert_eq!(session.transcript_len(), 0);

        let other = transcript_item(Some("e2"), "s2", "别人说的", "Alice");
        assert_eq!(session.ingest(&other, 0).events.len(), 1);
        assert_eq!(session.transcript_len(), 1);
    }

    #[test]
    fn poll_schedule_backs_off_and_caps_like_the_typescript_timer() {
        let mut schedule = PollSchedule::new();
        assert_eq!(schedule.delay_ms(3_000), 3_000);
        schedule.on_page(0, false);
        assert_eq!(schedule.idle_rounds(), 1);
        assert_eq!(schedule.delay_ms(3_000), 6_000);
        schedule.on_page(0, false);
        assert_eq!(schedule.delay_ms(3_000), 10_000);
        schedule.on_page(0, false);
        assert_eq!(schedule.delay_ms(3_000), 10_000);
        schedule.on_page(2, false);
        assert_eq!(schedule.idle_rounds(), 0);
        assert_eq!(schedule.delay_ms(3_000), 3_000);
        schedule.on_page(0, true);
        assert_eq!(schedule.idle_rounds(), -1);
        assert_eq!(schedule.delay_ms(3_000), 0);

        let mut errors = PollSchedule::new();
        errors.on_error();
        assert_eq!(errors.delay_ms(3_000), 6_000);
    }

    #[test]
    fn session_cursor_schedule_and_push_handoff() {
        let mut session = session(MeetingConfig::default());
        assert!(session.poll_due(0));
        session.note_page(1_000, Some("tok2".to_string()), 0, false);
        assert_eq!(session.cursor(), Some("tok2"));
        assert!(!session.poll_due(6_999));
        assert!(session.poll_due(7_000));

        session.note_page(8_000, None, 2, false);
        assert_eq!(session.cursor(), Some("tok2"));
        assert_eq!(session.next_poll_at_ms(), Some(11_000));

        assert!(session.mark_push_active());
        assert!(!session.mark_push_active());
        assert!(!session.poll_due(20_000));
        assert_eq!(session.next_poll_at_ms(), None);

        session.mark_ended();
        assert!(session.is_ended());
    }

    // -----------------------------------------------------------------------
    // Manager: polling, dedupe and push routing (offline)
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn poller_pages_with_a_cursor_and_collapses_push_overlap() {
        let api = Arc::new(FakeApi::new());
        api.push_join(Ok(join_result("70001", "123456789")));
        api.push_page(
            "70001",
            page(
                vec![
                    transcript_item(Some("e1"), "s1", "第一句", "Alice"),
                    chat_item("随便聊聊", None, "Bob"),
                ],
                Some("tok2"),
                false,
            ),
        );
        api.push_page(
            "70001",
            page(
                vec![transcript_item(Some("e3"), "s2", "第二句", "Alice")],
                Some("tok3"),
                false,
            ),
        );
        let host = Arc::new(FakeHost::default());
        let manager = manager(MeetingConfig::default(), Arc::clone(&api), host);

        manager
            .join("123456789", Some("oc_origin"))
            .await
            .expect("scripted join");
        let events = manager.poll_session_once("70001").await;
        assert_eq!(events.len(), 2);
        assert_eq!(
            api.calls(),
            vec![("70001".to_string(), None)],
            "first poll has no cursor"
        );
        assert_eq!(
            manager
                .snapshot("70001")
                .expect("session")
                .recent_transcript(None),
            vec!["Alice: 第一句"]
        );
        assert_eq!(manager.statuses()[0].ingested, 2);

        let events = manager.poll_session_once("70001").await;
        assert_eq!(events.len(), 1);
        assert_eq!(
            api.calls(),
            vec![
                ("70001".to_string(), None),
                ("70001".to_string(), Some("tok2".to_string()))
            ],
            "the cursor is threaded into the next page request"
        );
        assert_eq!(manager.statuses()[0].ingested, 3);
        assert_eq!(
            manager
                .snapshot("70001")
                .expect("session")
                .recent_transcript(None),
            vec!["Alice: 第一句", "Alice: 第二句"]
        );

        // The same three items arrive again over push: every one is a
        // duplicate, so nothing is ingested twice and no run sees them.
        let frame = activity_frame(vec![
            transcript_item(Some("e1"), "s1", "第一句", "Alice"),
            chat_item("随便聊聊", None, "Bob"),
            transcript_item(Some("e3"), "s2", "第二句", "Alice"),
        ]);
        assert!(manager.handle_push(&frame));
        assert_eq!(manager.statuses()[0].ingested, 3);
        assert_eq!(manager.statuses()[0].transcript_lines, 2);
        assert_eq!(manager.push_health().received, 1);
        assert_eq!(
            manager.snapshot("70001").expect("session").source,
            MeetingEventSource::Push
        );

        // Push is now the primary lane, so polling stops for this meeting.
        assert!(manager.poll_session_once("70001").await.is_empty());
        assert_eq!(api.calls().len(), 2);
    }

    #[tokio::test]
    async fn push_only_meetings_wake_for_debounced_transcripts() {
        let mut config = MeetingConfig::default();
        config.transcript.stabilize_ms = 1_000;
        let api = Arc::new(FakeApi::new());
        api.push_join(Ok(join_result("70001", "123456789")));
        let manager = manager(config, Arc::clone(&api), Arc::new(FakeHost::default()));
        manager
            .join("123456789", None)
            .await
            .expect("scripted join");

        let wake = manager.wake.notified();
        tokio::pin!(wake);
        assert!(manager.handle_push(&activity_frame(vec![transcript_item(
            Some("e1"),
            "s1",
            "hello",
            "Alice"
        )])));
        assert_eq!(
            manager.snapshot("70001").expect("session").source,
            MeetingEventSource::Push
        );
        let wait = manager.next_wakeup().expect("debounce deadline");
        assert!(wait <= Duration::from_millis(1_000));
        assert!(
            tokio::time::timeout(Duration::from_millis(50), wake)
                .await
                .is_ok(),
            "a push must wake the loop so the debounced line is committed"
        );
    }

    #[tokio::test]
    async fn poll_errors_back_off_without_dropping_the_session() {
        let api = Arc::new(FakeApi::new());
        api.push_join(Ok(join_result("70001", "123456789")));
        api.push_error("70001", "network down");
        let manager = manager(
            MeetingConfig::default(),
            Arc::clone(&api),
            Arc::new(FakeHost::default()),
        );

        manager
            .join("123456789", None)
            .await
            .expect("scripted join");
        assert!(manager.poll_session_once("70001").await.is_empty());
        assert_eq!(manager.statuses().len(), 1);
        assert_eq!(manager.statuses()[0].ingested, 0);
        assert!(manager.next_wakeup().is_some(), "the poller keeps retrying");
    }

    #[tokio::test]
    async fn chat_trigger_runs_the_agent_with_the_transcript_context() {
        let api = Arc::new(FakeApi::new());
        api.push_join(Ok(join_result("70001", "123456789")));
        let host = Arc::new(FakeHost::default());
        let manager = manager(
            MeetingConfig::default(),
            Arc::clone(&api),
            Arc::clone(&host),
        );
        manager
            .join("123456789", Some("oc_origin"))
            .await
            .expect("scripted join");

        // A message that is not addressed to the bot never starts a run.
        assert!(manager.handle_push(&activity_frame(vec![chat_item("随便聊聊", None, "Alice")])));
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(api.sent().is_empty());
        assert!(manager.snapshot("70001").expect("session").ended.eq(&false));

        // A stop word with no live run answers in-meeting.
        assert!(manager.handle_push(&activity_frame(vec![chat_item("@bot stop", None, "Alice")])));
        wait_until(|| !api.sent().is_empty()).await;
        assert!(api.sent()[0].1.contains("当前没有正在执行的任务"));

        // A trigger with a question spawns a run; the missing agent binary
        // fails fast, and the failure is delivered where the ask happened.
        let before = api.sent().len();
        assert!(manager.handle_push(&activity_frame(vec![chat_item(
            "@bot, 我们决定用哪个方案？",
            None,
            "Alice"
        )])));
        wait_until(|| api.sent().len() > before).await;
        let (meeting_id, text) = api.sent().last().cloned().expect("reply");
        assert_eq!(meeting_id, "70001");
        assert!(
            text.contains("无法启动 agent"),
            "expected a spawn failure, got {text:?}"
        );
    }

    #[tokio::test]
    async fn end_of_meeting_summary_falls_back_to_the_origin_chat() {
        let config = MeetingConfig {
            summary_on_end: true,
            summary_target: SummaryTarget::Owner,
            ..MeetingConfig::default()
        };
        let api = Arc::new(FakeApi::new());
        api.push_join(Ok(join_result("70001", "123456789")));
        let host = Arc::new(FakeHost::default());
        let manager = manager(config, Arc::clone(&api), Arc::clone(&host));
        manager
            .join("123456789", Some("oc_origin"))
            .await
            .expect("scripted join");

        assert!(manager.handle_push(&activity_frame(vec![transcript_item(
            Some("e1"),
            "s1",
            "我们决定用方案 A",
            "Alice"
        )])));
        assert!(manager.end_meeting("70001").is_some());
        assert!(manager.snapshot("70001").is_none());

        wait_until(|| !host.im().is_empty()).await;
        let (to, text) = &host.im()[0];
        assert_eq!(to, "oc_origin");
        assert!(text.starts_with("**会议纪要 · 周会**"), "got {text:?}");
        assert!(text.contains("无法启动 agent"));
    }

    #[tokio::test]
    async fn summary_is_skipped_when_disabled_or_without_a_target() {
        let api = Arc::new(FakeApi::new());
        api.push_join(Ok(join_result("70001", "123456789")));
        let host = Arc::new(FakeHost::default());
        let disabled = manager(
            MeetingConfig::default(),
            Arc::clone(&api),
            Arc::clone(&host),
        );
        disabled
            .join("123456789", None)
            .await
            .expect("scripted join");
        assert!(disabled.handle_push(&activity_frame(vec![transcript_item(
            Some("e1"),
            "s1",
            "内容",
            "Alice"
        )])));
        disabled.end_meeting("70001");
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(host.im().is_empty());

        // Enabled, but neither an origin chat (joined without one) nor an
        // owner DM is configured, so there is nowhere to send the summary.
        let config = MeetingConfig {
            summary_on_end: true,
            summary_target: SummaryTarget::Owner,
            ..MeetingConfig::default()
        };
        let api = Arc::new(FakeApi::new());
        api.push_join(Ok(join_result("70001", "123456789")));
        let host = Arc::new(FakeHost::default());
        let targetless = manager(config, Arc::clone(&api), Arc::clone(&host));
        targetless
            .join("123456789", None)
            .await
            .expect("scripted join");
        assert!(targetless.handle_push(&activity_frame(vec![transcript_item(
            Some("e1"),
            "s1",
            "内容",
            "Alice"
        )])));
        targetless.end_meeting("70001");
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(host.im().is_empty());
    }

    // -----------------------------------------------------------------------
    // Trigger matching, prompt building, targets
    // -----------------------------------------------------------------------

    #[test]
    fn trigger_matching_accepts_the_configured_prefix_and_bot_name() {
        let prefixes = trigger_prefixes("@bot", Some("小助手"));
        assert_eq!(prefixes, vec!["@bot".to_string(), "@小助手".to_string()]);
        assert_eq!(
            match_trigger("@bot, 你好", &prefixes).as_deref(),
            Some("你好")
        );
        assert_eq!(
            match_trigger("  @小助手：总结一下  ", &prefixes).as_deref(),
            Some("总结一下")
        );
        assert_eq!(match_trigger("@BOT  hi", &prefixes).as_deref(), Some("hi"));
        assert_eq!(match_trigger("@bot", &prefixes).as_deref(), Some(""));
        assert_eq!(match_trigger("hello", &prefixes), None);
        assert_eq!(trigger_prefixes("", None), Vec::<String>::new());
        assert_eq!(
            trigger_prefixes("@bot", Some("bot")),
            vec!["@bot".to_string()],
            "the `@<botName>` form is de-duplicated against the configured prefix"
        );
    }

    #[test]
    fn meeting_prompt_orders_context_then_question() {
        let prompt = build_meeting_prompt(
            "方案 A 还是 B？",
            &["Alice: 用 A".to_string()],
            Some("周会"),
            Some("Alice"),
        );
        assert!(prompt.starts_with("你正在参加一场飞书视频会议"));
        assert!(prompt.contains("会议主题：周会"));
        assert!(prompt.contains("Alice: 用 A"));
        assert!(prompt.contains("参会人「Alice」在会中向你提问："));
        assert!(prompt.contains("方案 A 还是 B？"));
        assert!(!prompt.contains("（暂无字幕）"));

        let empty = build_meeting_prompt("hi", &[], None, None);
        assert!(empty.contains("（暂无字幕）"));
        assert!(empty.contains("收到的请求："));
    }

    #[test]
    fn summary_targets_prefer_the_configured_lane_and_fall_back() {
        let origin = Some("oc_origin");
        let owner = Some("ou_owner");

        let target = resolve_summary_target(SummaryTarget::Origin, origin, owner).expect("target");
        assert_eq!(target.to, "oc_origin");
        assert_eq!(target.kind, SummaryTarget::Origin);
        assert!(!target.fell_back);

        let target = resolve_summary_target(SummaryTarget::Owner, origin, owner).expect("target");
        assert_eq!(target.to, "ou_owner");
        assert_eq!(target.kind, SummaryTarget::Owner);
        assert!(!target.fell_back);

        let target = resolve_summary_target(SummaryTarget::Owner, origin, None).expect("target");
        assert_eq!(target.to, "oc_origin");
        assert_eq!(target.kind, SummaryTarget::Origin);
        assert!(target.fell_back);

        assert!(resolve_summary_target(SummaryTarget::Origin, None, None).is_none());
    }

    // -----------------------------------------------------------------------
    // Push parsing and preflight hints
    // -----------------------------------------------------------------------

    #[test]
    fn meeting_push_frames_are_parsed() {
        let invited = push(
            "vc.bot.meeting_invited_v1",
            json!({"meeting": {"meeting_no": "123456789"}, "operator": {"open_id": "ou_inv"}}),
        );
        assert_eq!(
            parse_meeting_push(&invited),
            Some(MeetingPush::Invited {
                meeting_no: "123456789".to_string(),
                inviter_id: Some("ou_inv".to_string()),
            })
        );

        let ended = push(
            "vc.bot.meeting_ended_v1",
            json!({"meeting": {"id": "70001"}}),
        );
        assert_eq!(
            parse_meeting_push(&ended),
            Some(MeetingPush::Ended {
                meeting_id: "70001".to_string()
            })
        );

        let items = vec![transcript_item(Some("e1"), "s1", "hi", "Alice")];
        match parse_meeting_push(&activity_frame(items.clone())) {
            Some(MeetingPush::Activity(parsed)) => assert_eq!(parsed, items),
            other => panic!("unexpected push: {other:?}"),
        }

        assert!(parse_meeting_push(&push("im.message.receive_v1", json!({}))).is_none());
        assert_eq!(
            push_event_type(&json!({"event_type": "vc.bot.meeting_ended_v1"})),
            Some("vc.bot.meeting_ended_v1")
        );
    }

    #[test]
    fn preflight_and_error_hints_are_actionable() {
        assert!(
            describe_meeting_error(&RestError::Api {
                code: 20_017,
                message: "ErrNotInGray".to_string()
            })
            .contains("内测")
        );
        assert!(
            describe_meeting_error(&RestError::Api {
                code: 99_991_672,
                message: "no permission: vc:meeting.message:write".to_string()
            })
            .contains("vc:meeting.bot.join:write")
        );
        assert_eq!(
            describe_meeting_error(&RestError::Message("boom".to_string())),
            "boom"
        );

        assert_eq!(
            classify_preflight(&json!({"ok": true})).status,
            MeetingPreflightStatus::Ok
        );
        let beta = classify_preflight(&json!({
            "error": {"code": 20017, "message": "ErrNotInGray"}
        }));
        assert_eq!(beta.status, MeetingPreflightStatus::NotInBeta);
        assert_eq!(beta.beta_chat_url.as_deref(), Some(MEETING_BETA_CHAT_URL));

        let scopes = classify_preflight(&json!({
            "error": {
                "code": 99991672,
                "console_url": "https://open.feishu.cn/scope",
                "missing_scopes": ["vc:meeting.bot.join:write"]
            }
        }));
        assert_eq!(scopes.status, MeetingPreflightStatus::ScopeMissing);
        assert_eq!(
            scopes.missing_scopes,
            vec!["vc:meeting.bot.join:write".to_string()]
        );
        assert_eq!(
            scopes.console_url.as_deref(),
            Some("https://open.feishu.cn/scope")
        );

        assert_eq!(
            classify_preflight(&json!({"error": {"code": 1, "message": "boom"}})).status,
            MeetingPreflightStatus::Unknown
        );
        assert_eq!(MEETING_REQUIRED_EVENTS.len(), 3);
        assert_eq!(MEETING_REQUIRED_SCOPES.len(), 3);
    }

    // -----------------------------------------------------------------------
    // Command surface
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn meeting_commands_join_status_ask_and_leave() {
        let api = Arc::new(FakeApi::new());
        api.push_join(Ok(join_result("70001", "123456789")));
        api.push_join(Ok(join_result("70002", "123456788")));
        let host = Arc::new(FakeHost::default());
        let manager = manager(
            MeetingConfig::default(),
            Arc::clone(&api),
            Arc::clone(&host),
        );

        let usage = manager
            .handle_command("join 123", "ou_admin", "oc_origin")
            .await;
        assert!(usage.contains("用法"));

        let joined = manager
            .handle_command("join 123456789", "ou_admin", "oc_origin")
            .await;
        assert!(joined.contains("已入会"), "got {joined:?}");
        assert!(joined.contains("123456789"));

        let second = manager
            .join("123456789", Some("oc_origin"))
            .await
            .expect("already joined");
        assert!(second.already_joined);

        let status = manager.handle_command("", "ou_admin", "oc_origin").await;
        assert!(status.contains("正在跟 1 场会议"), "got {status:?}");
        assert!(status.contains("来源 轮询"));

        let transcript = manager
            .handle_command("transcript", "ou_admin", "oc_origin")
            .await;
        assert!(transcript.contains("字幕缓冲为空"));

        assert!(manager.handle_push(&activity_frame(vec![transcript_item(
            Some("e1"),
            "s1",
            "我们决定用方案 A",
            "Alice"
        )])));
        let transcript = manager
            .handle_command("transcript", "ou_admin", "oc_origin")
            .await;
        assert!(
            transcript.contains("Alice: 我们决定用方案 A"),
            "got {transcript:?}"
        );
        assert!(transcript.contains("```"));

        let answer = manager
            .handle_command("ask 现在开会了吗", "ou_admin", "oc_origin")
            .await;
        assert!(answer.contains("无法启动 agent"), "got {answer:?}");
        assert!(
            host.im()
                .iter()
                .any(|(to, text)| to == "oc_origin" && text == "正在思考…"),
            "the command acknowledges before the slow run"
        );

        let joined = manager
            .handle_command("join 123 456 788", "ou_admin", "oc_origin")
            .await;
        assert!(joined.contains("已入会"), "got {joined:?}");

        let ambiguous = manager
            .handle_command("leave", "ou_admin", "oc_origin")
            .await;
        assert!(ambiguous.contains("请指定会议号"), "got {ambiguous:?}");

        let left = manager
            .handle_command("leave 123456789", "ou_admin", "oc_origin")
            .await;
        assert!(left.contains("已离会"));
        assert_eq!(api.leaves(), vec!["70001".to_string()]);

        let left = manager
            .handle_command("leave", "ou_admin", "oc_origin")
            .await;
        assert!(left.contains("已离会"));
        assert_eq!(api.leaves(), vec!["70001".to_string(), "70002".to_string()]);

        let empty = manager.handle_command("", "ou_admin", "oc_origin").await;
        assert!(empty.contains("当前没有在跟的会议"));

        let unknown = manager
            .handle_command("nope", "ou_admin", "oc_origin")
            .await;
        assert!(unknown.contains("/meeting ask"));
    }

    #[tokio::test]
    async fn stop_interrupts_a_live_run_and_busy_asks_are_refused() {
        let api = Arc::new(FakeApi::new());
        api.push_join(Ok(join_result("70001", "123456789")));
        let manager = manager(
            MeetingConfig::default(),
            Arc::clone(&api),
            Arc::new(FakeHost::default()),
        );
        manager
            .join("123456789", Some("oc_origin"))
            .await
            .expect("scripted join");

        // Simulate an in-flight answer run that never finishes.
        let handle = tokio::spawn(std::future::pending::<()>());
        lock(&manager.runs).insert(
            meeting_scope_id("70001"),
            ActiveMeetingRun { id: 1, handle },
        );

        let busy = manager
            .handle_command("ask 还有多久", "ou_admin", "oc_origin")
            .await;
        assert!(busy.contains("上一个任务还在执行"), "got {busy:?}");
        assert!(busy.contains("@bot stop"), "the hint quotes the trigger");

        let stopped = manager
            .handle_command("stop", "ou_admin", "oc_origin")
            .await;
        assert!(stopped.contains("已中断该会议的当前任务"));
        let again = manager
            .handle_command("stop", "ou_admin", "oc_origin")
            .await;
        assert!(again.contains("当前没有正在执行的任务"));
    }

    #[tokio::test]
    async fn im_only_answers_go_to_the_origin_chat() {
        let config = MeetingConfig {
            respond_in: RespondIn::Im,
            ..MeetingConfig::default()
        };
        let api = Arc::new(FakeApi::new());
        api.push_join(Ok(join_result("70001", "123456789")));
        let host = Arc::new(FakeHost::default());
        let manager = manager(config, Arc::clone(&api), Arc::clone(&host));
        manager
            .join("123456789", Some("oc_origin"))
            .await
            .expect("scripted join");

        assert!(manager.handle_push(&activity_frame(vec![chat_item(
            "@bot 方案 A 的风险是什么？",
            None,
            "Alice"
        )])));
        wait_until(|| !host.im().is_empty()).await;
        let (to, text) = &host.im()[0];
        assert_eq!(to, "oc_origin");
        assert!(text.contains("无法启动 agent"));
        assert!(
            api.sent().is_empty(),
            "respondIn=im must not post into the meeting"
        );
    }

    #[tokio::test]
    async fn invitation_push_auto_joins_when_configured() {
        let config = MeetingConfig {
            auto_join_on_invite: true,
            ..MeetingConfig::default()
        };
        let api = Arc::new(FakeApi::new());
        api.push_join(Ok(join_result("70001", "123456789")));
        let manager = manager(config, Arc::clone(&api), Arc::new(FakeHost::default()));

        assert!(manager.handle_push(&push(
            "vc.bot.meeting_invited_v1",
            json!({"meeting": {"meeting_no": "123456789"}, "operator": {"open_id": "ou_inv"}})
        )));
        wait_until(|| manager.statuses().len() == 1).await;
        assert_eq!(manager.push_health().received, 1);
        let session = manager.snapshot("70001").expect("auto-joined session");
        assert_eq!(session.meeting_no, "123456789");
        assert_eq!(
            session.origin_chat_id, None,
            "a push join has no origin chat"
        );
    }

    #[tokio::test]
    async fn join_failure_surfaces_the_not_in_gray_hint() {
        let api = Arc::new(FakeApi::new());
        api.push_join(Err(RestError::Api {
            code: 20_017,
            message: "ErrNotInGray".to_string(),
        }));
        let manager = manager(
            MeetingConfig::default(),
            Arc::clone(&api),
            Arc::new(FakeHost::default()),
        );
        let reply = manager
            .handle_command("join 123456789", "ou_admin", "oc_origin")
            .await;
        assert!(reply.starts_with("入会失败："), "got {reply:?}");
        assert!(reply.contains("内测"));
    }

    #[test]
    fn scope_ids_are_meeting_scoped() {
        assert_eq!(meeting_scope_id("70001"), "meeting:70001");
        assert_eq!(
            format_summary("周会", "纪要"),
            "**会议纪要 · 周会**\n\n纪要"
        );
    }

    // ---- global run limiter ---------------------------------------------

    #[cfg(unix)]
    #[tokio::test]
    async fn meeting_run_holds_a_global_slot_for_the_whole_fake_run() {
        let _agent_lock = fake_agent_lock().await;
        let root = tempfile::tempdir().unwrap();
        let started = root.path().join("meeting-started");
        let release = root.path().join("meeting-release");
        let api = Arc::new(FakeApi::new());
        let host = Arc::new(FakeHost::default());
        let (manager, limiter) = manager_with_limiter(
            MeetingConfig::default(),
            api,
            host,
            blocking_fake_agent(root.path(), &started, &release),
            1,
        );
        let scope = meeting_scope_id("70001");

        let task = {
            let manager = Arc::clone(&manager);
            let scope = scope.clone();
            tokio::spawn(async move { manager.run_agent(&scope, "hi".to_string()).await })
        };

        for _ in 0..400 {
            if started.exists() || task.is_finished() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        if task.is_finished() && !started.exists() {
            let answer = task.await.expect("meeting task");
            panic!("the meeting run ended before the fake agent started: {answer}");
        }
        assert!(started.exists(), "the fake agent must start");
        assert_eq!(limiter.active(), 1);
        assert!(
            limiter.try_acquire().is_none(),
            "a live meeting run must consume the global cap"
        );

        std::fs::write(&release, "go").unwrap();
        let answer = tokio::time::timeout(Duration::from_secs(3), task)
            .await
            .expect("fake meeting run finished")
            .expect("meeting task");
        assert_eq!(answer, "meeting done");
        assert_eq!(limiter.active(), 0);
        assert!(limiter.try_acquire().is_some());
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn meeting_run_releases_its_slot_when_the_agent_cannot_start() {
        let api = Arc::new(FakeApi::new());
        let host = Arc::new(FakeHost::default());
        let (manager, limiter) = manager_with_limiter(
            MeetingConfig::default(),
            api,
            host,
            PathBuf::from("/nonexistent/meeting-agent"),
            1,
        );
        let answer = manager
            .run_agent_with_wait(
                &meeting_scope_id("70001"),
                "hi".to_string(),
                Duration::from_millis(100),
            )
            .await;
        assert!(answer.contains("无法启动 agent"), "{answer}");
        assert_eq!(limiter.active(), 0);
        assert!(limiter.try_acquire().is_some());
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn meeting_run_times_out_with_a_user_visible_slot_message() {
        let _agent_lock = fake_agent_lock().await;
        let root = tempfile::tempdir().unwrap();
        let api = Arc::new(FakeApi::new());
        let host = Arc::new(FakeHost::default());
        let (manager, limiter) = manager_with_limiter(
            MeetingConfig::default(),
            api,
            host,
            fake_agent(root.path(), 0),
            1,
        );
        let held = limiter.try_acquire().expect("only slot");

        let answer = manager
            .run_agent_with_wait(
                &meeting_scope_id("70001"),
                "hi".to_string(),
                Duration::from_millis(25),
            )
            .await;
        assert_eq!(answer, RUN_SLOT_TIMEOUT_REPLY);
        assert_eq!(limiter.active(), 1, "the held slot is untouched");

        drop(held);
        assert_eq!(limiter.active(), 0);
    }
    #[cfg(unix)]
    #[tokio::test]
    async fn bridge_stop_aborts_a_meeting_run_and_releases_the_slot() {
        let _agent_lock = fake_agent_lock().await;
        let root = tempfile::tempdir().unwrap();
        let started = root.path().join("meeting-stop-started");
        let release = root.path().join("meeting-stop-release");
        let bridge = bridge_with_agent(
            root.path(),
            blocking_fake_agent(root.path(), &started, &release),
        );
        let host: Arc<dyn MeetingHost> = Arc::new(BridgeMeetingHost {
            bridge: Arc::downgrade(&bridge),
        });
        let api = Arc::new(FakeApi::new());
        api.push_join(Ok(join_result("70001", "123456789")));
        let manager = Arc::new(
            MeetingManager::new(
                MeetingConfig {
                    respond_in: RespondIn::Im,
                    ..MeetingConfig::default()
                },
                AgentSettings {
                    kind: AgentKind::Claude,
                    binary: bridge.agent_binary.clone(),
                    permissions: RunPermissions::default(),
                    bot_open_id: None,
                    bot_name: None,
                },
                api,
                host,
            )
            .with_run_limit(Arc::clone(bridge.run_limiter())),
        );
        manager
            .join("123456789", Some("oc_origin"))
            .await
            .expect("scripted join");
        assert!(manager.handle_push(&activity_frame(vec![chat_item(
            "@bot 方案 A 的风险是什么？",
            None,
            "Alice"
        )])));

        for _ in 0..400 {
            if started.exists() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        assert!(started.exists(), "the fake meeting agent must start");
        let scope = meeting_scope_id("70001");
        assert!(
            bridge.is_scope_active(&scope),
            "the meeting run must be registered in the bridge"
        );
        assert_eq!(bridge.run_limiter().active(), 1);

        let (stopped, dropped) = bridge.stop_scope(&scope);
        assert!(stopped, "the meeting run must be aborted");
        assert_eq!(dropped, 0, "a meeting scope has no pending queue");
        assert!(!bridge.is_scope_active(&scope));

        // The manager's own map and the global slot are released by the
        // aborted task's guards.
        for _ in 0..400 {
            if lock(&manager.runs).is_empty() && bridge.run_limiter().active() == 0 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        assert!(
            lock(&manager.runs).is_empty(),
            "the manager run entry must be removed on abort"
        );
        assert_eq!(
            bridge.run_limiter().active(),
            0,
            "the aborted meeting run must release its global slot"
        );
        assert!(bridge.run_limiter().try_acquire().is_some());
        assert!(
            !manager.interrupt("70001"),
            "nothing is left for a second interrupt"
        );
    }
    #[tokio::test]
    async fn ask_acknowledgement_uses_the_topic_reply_target() {
        let api = Arc::new(FakeApi::new());
        api.push_join(Ok(join_result("70001", "123456789")));
        let host = Arc::new(FakeHost::default());
        let manager = manager(
            MeetingConfig::default(),
            Arc::clone(&api),
            Arc::clone(&host),
        );
        manager
            .join("123456789", Some("oc_origin"))
            .await
            .expect("scripted join");

        let answer = manager
            .handle_command_with_reply(
                "ask 现在开会了吗",
                "ou_admin",
                "oc_origin",
                ImReplyTarget {
                    scope: Some("oc_origin:omt_1"),
                    parent_message_id: Some("om_1"),
                },
            )
            .await;
        assert!(answer.contains("无法启动 agent"), "got {answer:?}");

        let replies = host.replies();
        assert_eq!(replies.len(), 1, "one immediate acknowledgement");
        let (to, scope, parent, text) = &replies[0];
        assert_eq!(to, "oc_origin");
        assert_eq!(scope.as_deref(), Some("oc_origin:omt_1"));
        assert_eq!(parent.as_deref(), Some("om_1"));
        assert!(text.contains("正在思考"), "got {text:?}");
    }
}
