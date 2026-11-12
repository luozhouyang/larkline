//! CardKit 2.0 streaming cards.
//!
//! A *streaming card* is a CardKit entity (`card_id`) referenced by one sent
//! message. The bridge keeps rewriting one Markdown element of that entity
//! while an agent runs, which lets Lark/Feishu render the text with its
//! typewriter effect and avoids re-sending the whole card JSON on every token.
//!
//! Two server-side rules shape the API:
//!
//! * Every CardKit operation on one entity must use a strictly increasing
//!   `sequence`; [`StreamingCard`] owns that counter.
//! * Feishu force-finalizes a streaming card about ten minutes after the entity
//!   is created and silently drops later writes. Updates therefore stop at
//!   [`CARD_SAFE_AGE`] (8.5 minutes); [`StreamingCard::finish`] then reports
//!   `false` so the caller can deliver the final answer as a separate message.
//!
//! The decision logic lives in [`StreamingState`], which is pure and fully
//! unit-tested; [`StreamingCard`] is the thin asynchronous shell around it.

use std::time::{Duration, Instant};

use lark_channel::lark_openapi::CardUpdateOptions;
use lark_channel::message::{MessageId, Recipient};
use lark_channel::{Card, CardId, CardSettings, CardStreamingConfig, Error};
use serde_json::{Map, Value};

use crate::client::{Channel, LarkResult};

/// Feishu force-finalizes a streaming card about ten minutes after creation;
/// updates after that point are silently discarded. Stop well before the
/// cutoff so the final frame (or the caller's fallback message) still lands.
pub const CARD_SAFE_AGE: Duration = Duration::from_secs(8 * 60 + 30);

/// Conventional `element_id` of the Markdown element that is streamed.
///
/// CardKit only streams text into a component that carries an `element_id`;
/// [`StreamingCard::start`] takes the id explicitly, this constant is the
/// default the bridge should use when building its card.
pub const STREAMING_ELEMENT_ID: &str = "stream_md";

/// CardKit caps an element id at 20 characters, starting with a letter.
const MAX_ELEMENT_ID_CHARS: usize = 20;

/// Chat-preview summaries longer than this are truncated, like the SDK does.
const MAX_SUMMARY_CHARS: usize = 50;

/// Why an update was skipped.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SkipReason {
    /// The card is no longer live: it was finished, expired, or a write failed.
    NotLive,
    /// [`CARD_SAFE_AGE`] elapsed; the server would drop the write anyway.
    Expired,
    /// The requested content is empty, which CardKit rejects.
    Empty,
    /// The card already shows exactly this content.
    Unchanged,
}

/// What [`StreamingState`] decided for one mid-run content update.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[must_use]
pub enum UpdatePlan {
    /// Perform the write with this CardKit sequence.
    Send { sequence: u32 },
    /// Do not write.
    Skip { reason: SkipReason },
}

/// What [`StreamingState`] decided for the final update.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[must_use]
pub enum FinishPlan {
    /// Perform the final write with this CardKit sequence.
    Send { sequence: u32 },
    /// The card already shows the final content; nothing to write.
    AlreadyShown,
    /// The card cannot take the final content; send a separate message.
    Skip { reason: SkipReason },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Phase {
    Live,
    Finished,
    Expired,
    Failed,
}

/// Pure state of one streaming card.
///
/// The state owns the CardKit sequence counter, the last content that was
/// successfully written, and the 8.5-minute window. All decisions take an
/// explicit `now`, so the whole state is testable without a network or a clock.
#[derive(Debug, Clone)]
pub struct StreamingState {
    sequence: u32,
    last_content: Option<String>,
    created_at: Instant,
    safe_age: Duration,
    phase: Phase,
}

impl StreamingState {
    /// Creates a live state for a card whose entity was created at `created_at`.
    pub fn new(created_at: Instant, safe_age: Duration) -> Self {
        Self {
            sequence: 0,
            last_content: None,
            created_at,
            safe_age,
            phase: Phase::Live,
        }
    }

    /// When the CardKit entity was created (the server measures its lifetime
    /// from this point).
    pub fn created_at(&self) -> Instant {
        self.created_at
    }

    /// How long the card stays safe to update.
    pub fn safe_age(&self) -> Duration {
        self.safe_age
    }

    /// The sequence of the last CardKit operation; `0` before the first write.
    pub fn sequence(&self) -> u32 {
        self.sequence
    }

    /// The content of the last successful write, if any.
    pub fn last_content(&self) -> Option<&str> {
        self.last_content.as_deref()
    }

    /// True while this card may still be updated.
    pub fn is_live(&self) -> bool {
        self.phase == Phase::Live
    }

    /// True once [`StreamingState::plan_finish`] accepted the final frame.
    pub fn is_finished(&self) -> bool {
        self.phase == Phase::Finished
    }

    /// True while `now` is still inside the server-safe window.
    pub fn within_window(&self, now: Instant) -> bool {
        now.saturating_duration_since(self.created_at) < self.safe_age
    }

    /// Decides what a mid-run content update should do and advances the state.
    ///
    /// A write is only planned when the card is live, still inside the window,
    /// and the content differs from the last successful write. Skipping never
    /// consumes a sequence number, so CardKit never sees a duplicate.
    pub fn plan_content(&mut self, content: &str, now: Instant) -> UpdatePlan {
        if self.phase != Phase::Live {
            return UpdatePlan::Skip {
                reason: SkipReason::NotLive,
            };
        }
        if !self.within_window(now) {
            self.phase = Phase::Expired;
            return UpdatePlan::Skip {
                reason: SkipReason::Expired,
            };
        }
        if content.is_empty() {
            return UpdatePlan::Skip {
                reason: SkipReason::Empty,
            };
        }
        if self.last_content.as_deref() == Some(content) {
            return UpdatePlan::Skip {
                reason: SkipReason::Unchanged,
            };
        }
        UpdatePlan::Send {
            sequence: self.next_sequence(),
        }
    }

    /// Decides what the final update should do and closes the card.
    ///
    /// The card is finished even when nothing is written: after `finish` no
    /// further update is allowed. `AlreadyShown` means the card already holds
    /// the final content (the caller does not need a separate message), while
    /// `Skip` means it does not (the caller must send one).
    pub fn plan_finish(&mut self, content: &str, now: Instant) -> FinishPlan {
        if self.phase == Phase::Finished {
            // Idempotent second call: report the card state without writing.
            return if self.last_content.as_deref() == Some(content) {
                FinishPlan::AlreadyShown
            } else {
                FinishPlan::Skip {
                    reason: SkipReason::NotLive,
                }
            };
        }
        if self.phase != Phase::Live {
            return FinishPlan::Skip {
                reason: SkipReason::NotLive,
            };
        }
        if !self.within_window(now) {
            self.phase = Phase::Expired;
            return FinishPlan::Skip {
                reason: SkipReason::Expired,
            };
        }
        self.phase = Phase::Finished;
        if content.is_empty() {
            return FinishPlan::Skip {
                reason: SkipReason::Empty,
            };
        }
        if self.last_content.as_deref() == Some(content) {
            return FinishPlan::AlreadyShown;
        }
        FinishPlan::Send {
            sequence: self.next_sequence(),
        }
    }

    /// Records a content write that the server accepted.
    ///
    /// [`StreamingCard`] calls this after a successful API write. It is public
    /// so the pure state stays usable with another transport.
    pub fn record_written(&mut self, content: &str) {
        self.last_content = Some(content.to_owned());
    }

    /// Marks the card unusable after an ambiguous or failed write.
    ///
    /// The card never becomes live again: the caller owns the message from now
    /// on and can fall back to message-level updates.
    pub fn mark_failed(&mut self) {
        self.phase = Phase::Failed;
    }

    /// Allocates the next CardKit sequence number.
    pub(crate) fn next_sequence(&mut self) -> u32 {
        self.sequence = self.sequence.saturating_add(1);
        self.sequence
    }
}

/// A live CardKit 2.0 streaming card.
///
/// Created by [`StreamingCard::start`], which creates the entity and sends the
/// message that references it. Call [`StreamingCard::set_content`] while the
/// agent runs and [`StreamingCard::finish`] once it is done.
///
/// If the entity cannot be created, `start` returns the SDK error before any
/// message exists: fall back to [`Channel::send_card`] plus
/// [`Channel::update_card_message`]. If a later write fails, `is_live` turns
/// `false` but [`StreamingCard::message_id`] stays usable, so the caller can
/// fall back to message-level updates for the same chat message.
#[derive(Debug, Clone)]
pub struct StreamingCard {
    channel: Channel,
    card_id: CardId,
    message_id: MessageId,
    element_id: String,
    state: StreamingState,
}

impl StreamingCard {
    /// Creates a CardKit entity for `card` and sends a reference message.
    ///
    /// The card JSON is passed through [`enable_streaming`] first, so streaming
    /// mode is enabled even when the caller built a plain CardKit 2.0 card.
    /// The clock starts before the first API call because Feishu measures the
    /// card lifetime from entity creation.
    pub async fn start(
        channel: &Channel,
        chat_id: &str,
        card: Card,
        element_id: &str,
    ) -> LarkResult<Self> {
        Self::start_with_safe_age(channel, chat_id, card, element_id, CARD_SAFE_AGE).await
    }

    /// Like [`StreamingCard::start`] with an explicit server-safe window.
    ///
    /// Useful for tenants that enforce a shorter (or longer, never past the
    /// documented ten minutes) card lifetime.
    pub async fn start_with_safe_age(
        channel: &Channel,
        chat_id: &str,
        card: Card,
        element_id: &str,
        safe_age: Duration,
    ) -> LarkResult<Self> {
        validate_element_id(element_id)?;
        // Deliberately before entity creation: starting a touch early only
        // makes the guard more conservative.
        let created_at = Instant::now();
        let card = enable_streaming(card)?;
        let card_id = channel.create_card_entity(&card).await?;
        let message_id = channel
            .sender()
            .card_reference_message(Recipient::Chat(chat_id.to_owned()), card_id.clone())
            .send()
            .await?;
        Ok(Self {
            channel: channel.clone(),
            card_id,
            message_id,
            element_id: element_id.to_owned(),
            state: StreamingState::new(created_at, safe_age),
        })
    }

    /// The CardKit entity behind the message.
    pub fn card_id(&self) -> &CardId {
        &self.card_id
    }

    /// The message that references the entity.
    pub fn message_id(&self) -> &MessageId {
        &self.message_id
    }

    /// The streamed element id.
    pub fn element_id(&self) -> &str {
        &self.element_id
    }

    /// The CardKit sequence of the last write; `0` before the first one.
    pub fn sequence(&self) -> u32 {
        self.state.sequence()
    }

    /// True while the card may still be updated.
    ///
    /// Turns `false` after [`StreamingCard::finish`], once [`CARD_SAFE_AGE`]
    /// elapsed, or after a failed write.
    pub fn is_live(&self) -> bool {
        self.state.is_live()
    }

    /// True once [`StreamingCard::finish`] accepted the final frame.
    pub fn is_finished(&self) -> bool {
        self.state.is_finished()
    }

    /// Push a full content snapshot into the streaming element.
    ///
    /// Returns `Ok(true)` when a write happened, `Ok(false)` when it was
    /// skipped (unchanged content, empty content, finished card, or the
    /// 8.5-minute guard). A server error is returned as `Err` and also turns
    /// `is_live` false; from then on the caller owns the card through
    /// message-level updates.
    pub async fn set_content(&mut self, content: &str) -> LarkResult<bool> {
        let now = Instant::now();
        match self.state.plan_content(content, now) {
            UpdatePlan::Send { sequence } => {
                let result = self
                    .channel
                    .openapi()
                    .update_card_element_content(
                        &self.card_id,
                        &self.element_id,
                        content,
                        CardUpdateOptions::new(sequence),
                    )
                    .await;
                match result {
                    Ok(()) => {
                        self.state.record_written(content);
                        Ok(true)
                    }
                    Err(error) => {
                        self.state.mark_failed();
                        tracing::warn!(
                            error = %error,
                            card_id = self.card_id.as_str(),
                            message_id = self.message_id.0.as_str(),
                            sequence,
                            "streaming card update failed; falling back to message updates"
                        );
                        Err(error)
                    }
                }
            }
            UpdatePlan::Skip { reason } => {
                tracing::debug!(
                    card_id = self.card_id.as_str(),
                    message_id = self.message_id.0.as_str(),
                    ?reason,
                    "streaming card update skipped"
                );
                Ok(false)
            }
        }
    }

    /// Write the final content, disable streaming mode, and close the card.
    ///
    /// Returns `Ok(true)` when the card holds the final content (it was either
    /// written now or already shown by the previous update). Returns `Ok(false)`
    /// when the card must be left alone — the 8.5-minute window elapsed, the
    /// card is already dead, or `content` is empty — and the caller should
    /// deliver the final answer as a separate message.
    ///
    /// Stopping the typewriter (`streaming_mode=false`) plus the preview
    /// summary is best-effort: the final content is what matters, so a failed
    /// settings update is logged, not returned.
    ///
    /// A server error while writing the content (for example oversized text)
    /// is returned as `Err`; treat it like `Ok(false)` and deliver the answer
    /// in a separate message.
    pub async fn finish(&mut self, content: &str) -> LarkResult<bool> {
        let now = Instant::now();
        match self.state.plan_finish(content, now) {
            FinishPlan::Skip { reason } => {
                tracing::debug!(
                    card_id = self.card_id.as_str(),
                    message_id = self.message_id.0.as_str(),
                    ?reason,
                    "streaming card finish skipped"
                );
                Ok(false)
            }
            FinishPlan::AlreadyShown => {
                self.disable_streaming(content).await;
                Ok(true)
            }
            FinishPlan::Send { sequence } => {
                let result = self
                    .channel
                    .openapi()
                    .update_card_element_content(
                        &self.card_id,
                        &self.element_id,
                        content,
                        CardUpdateOptions::new(sequence),
                    )
                    .await;
                match result {
                    Ok(()) => {
                        self.state.record_written(content);
                        self.disable_streaming(content).await;
                        Ok(true)
                    }
                    Err(error) => {
                        self.state.mark_failed();
                        tracing::warn!(
                            error = %error,
                            card_id = self.card_id.as_str(),
                            message_id = self.message_id.0.as_str(),
                            sequence,
                            "streaming card final update failed; caller must send the answer"
                        );
                        Err(error)
                    }
                }
            }
        }
    }

    /// Best-effort finalization: stop the typewriter and freeze the preview.
    async fn disable_streaming(&mut self, content: &str) {
        let sequence = self.state.next_sequence();
        let settings = CardSettings::new()
            .streaming_mode(false)
            .summary(summarize(content));
        if let Err(error) = self
            .channel
            .openapi()
            .update_card_settings(&self.card_id, &settings, CardUpdateOptions::new(sequence))
            .await
        {
            tracing::warn!(
                error = %error,
                card_id = self.card_id.as_str(),
                sequence,
                "cannot disable streaming mode; the final card content is already in place"
            );
        }
    }
}

/// Enables CardKit streaming on a complete schema 2.0 card.
///
/// The exact shape the card server expects (confirmed against
/// `lark-channel`'s `CardBuilder::streaming`) is:
///
/// ```json
/// "config": {
///   "streaming_mode": true,
///   "streaming_config": {
///     "print_frequency_ms": { "default": 70 },
///     "print_step": { "default": 1 },
///     "print_strategy": "fast"
///   }
/// }
/// ```
///
/// `config.update_multi` is also forced to `true` so the same card stays valid
/// for the message-level (`update_message_card`) fallback. An existing
/// `streaming_config` is preserved, so callers can tune the typewriter.
pub fn enable_streaming(card: Card) -> LarkResult<Card> {
    enable_streaming_with_config(card, CardStreamingConfig::default())
}

/// Like [`enable_streaming`] with a custom typewriter configuration.
pub fn enable_streaming_with_config(
    card: Card,
    streaming_config: CardStreamingConfig,
) -> LarkResult<Card> {
    let mut value = card.into_value();
    let config = value
        .as_object_mut()
        .ok_or_else(|| Error::Validation("card must be a JSON object".to_owned()))?
        .entry("config".to_owned())
        .or_insert_with(|| Value::Object(Map::new()))
        .as_object_mut()
        .ok_or_else(|| Error::Validation("card config must be a JSON object".to_owned()))?;
    config.insert("update_multi".to_owned(), Value::Bool(true));
    config.insert("streaming_mode".to_owned(), Value::Bool(true));
    config
        .entry("streaming_config".to_owned())
        .or_insert_with(|| Value::from(streaming_config));
    Card::from_value(value)
}

/// Short chat-preview summary for the final frame.
fn summarize(content: &str) -> String {
    let compact = content.split_whitespace().collect::<Vec<_>>().join(" ");
    if compact.chars().count() <= MAX_SUMMARY_CHARS {
        return compact;
    }
    compact
        .chars()
        .take(MAX_SUMMARY_CHARS - 3)
        .chain("...".chars())
        .collect()
}

/// Mirrors the SDK's element-id rules so `start` fails before creating an
/// entity that could never be updated.
fn validate_element_id(element_id: &str) -> LarkResult<()> {
    let mut chars = element_id.chars();
    let valid = chars
        .next()
        .is_some_and(|first| first.is_ascii_alphabetic())
        && chars.all(|character| character.is_ascii_alphanumeric() || character == '_')
        && element_id.chars().count() <= MAX_ELEMENT_ID_CHARS;
    if !valid {
        return Err(Error::Validation(format!(
            "card element_id must start with a letter, contain only ASCII letters, digits, or underscores, and be at most {MAX_ELEMENT_ID_CHARS} characters"
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cards::running_card;

    /// A state whose clock starts "now", paired with that clock.
    fn state() -> (StreamingState, Instant) {
        let created_at = Instant::now();
        (StreamingState::new(created_at, CARD_SAFE_AGE), created_at)
    }

    #[test]
    fn safe_age_matches_the_typescript_bridge() {
        assert_eq!(CARD_SAFE_AGE, Duration::from_secs(8 * 60 + 30));
        // Must stay below Feishu's ~10 minute server cutoff.
        assert!(CARD_SAFE_AGE < Duration::from_secs(10 * 60));
    }

    #[test]
    fn streaming_card_json_enables_streaming_and_keeps_metadata() {
        let card = enable_streaming(running_card("Running", "detail")).expect("streaming card");
        let value = card.as_value();

        assert_eq!(value["config"]["update_multi"], true);
        assert_eq!(value["config"]["streaming_mode"], true);
        assert_eq!(
            value["config"]["streaming_config"],
            serde_json::json!({
                "print_frequency_ms": { "default": 70 },
                "print_step": { "default": 1 },
                "print_strategy": "fast"
            })
        );
        // Existing config survives the merge.
        assert_eq!(value["config"]["summary"]["content"], "Running");
        assert_eq!(value["schema"], "2.0");
    }

    #[test]
    fn streaming_card_json_preserves_explicit_streaming_config() {
        let custom = serde_json::json!({
            "print_frequency_ms": { "default": 10 },
            "print_step": { "default": 2 },
            "print_strategy": "delay"
        });
        let mut value = running_card("t", "d").into_value();
        value["config"]["streaming_config"] = custom.clone();
        let card = Card::from_value(value).expect("valid card");

        let card = enable_streaming(card).expect("streaming card");
        assert_eq!(
            card.as_value()["config"]["streaming_config"],
            custom,
            "caller-provided typewriter settings must not be overwritten"
        );
    }

    #[test]
    fn sequence_increments_only_on_real_writes() {
        let (mut state, now) = state();
        assert_eq!(state.sequence(), 0);

        assert_eq!(
            state.plan_content("first", now),
            UpdatePlan::Send { sequence: 1 }
        );
        state.record_written("first");
        assert_eq!(state.sequence(), 1);

        // Same content again: no write, no sequence.
        assert_eq!(
            state.plan_content("first", now),
            UpdatePlan::Skip {
                reason: SkipReason::Unchanged
            }
        );
        assert_eq!(state.sequence(), 1);

        assert_eq!(
            state.plan_content("second", now),
            UpdatePlan::Send { sequence: 2 }
        );
        state.record_written("second");
        assert_eq!(state.sequence(), 2);
    }

    #[test]
    fn empty_content_is_never_written() {
        let (mut state, now) = state();
        assert_eq!(
            state.plan_content("", now),
            UpdatePlan::Skip {
                reason: SkipReason::Empty
            }
        );
        assert_eq!(state.sequence(), 0);
        assert!(state.is_live(), "an empty frame keeps the card live");
    }

    #[test]
    fn updates_stop_once_the_safe_age_elapses() {
        let (mut state, created_at) = state();
        let just_inside = created_at + CARD_SAFE_AGE - Duration::from_millis(1);
        let at_cutoff = created_at + CARD_SAFE_AGE;

        assert!(state.within_window(just_inside));
        assert!(!state.within_window(at_cutoff));

        assert_eq!(
            state.plan_content("too late", at_cutoff),
            UpdatePlan::Skip {
                reason: SkipReason::Expired
            }
        );
        assert!(!state.is_live());
        assert_eq!(state.sequence(), 0, "no sequence consumed by an expiry");

        // Once expired the card never comes back.
        assert_eq!(
            state.plan_content("later", created_at + 2 * CARD_SAFE_AGE),
            UpdatePlan::Skip {
                reason: SkipReason::NotLive
            }
        );
        assert_eq!(state.sequence(), 0);
    }

    #[test]
    fn finish_inside_the_window_writes_the_last_frame_and_closes() {
        let (mut state, now) = state();

        assert_eq!(
            state.plan_finish("final answer", now),
            FinishPlan::Send { sequence: 1 }
        );
        assert!(!state.is_live());
        assert!(state.is_finished());

        state.record_written("final answer");
        // A repeated finish reports the card is already up to date.
        assert_eq!(
            state.plan_finish("final answer", now),
            FinishPlan::AlreadyShown
        );
        // Different content cannot be written anymore.
        assert_eq!(
            state.plan_finish("other", now),
            FinishPlan::Skip {
                reason: SkipReason::NotLive
            }
        );
    }

    #[test]
    fn finish_with_unchanged_content_does_not_write() {
        let (mut state, now) = state();
        assert_eq!(
            state.plan_content("done", now),
            UpdatePlan::Send { sequence: 1 }
        );
        state.record_written("done");

        assert_eq!(state.plan_finish("done", now), FinishPlan::AlreadyShown);
        assert!(!state.is_live());
        assert_eq!(state.sequence(), 1, "already shown needs no new sequence");
    }

    #[test]
    fn finish_outside_the_window_leaves_the_card_for_a_separate_message() {
        let (mut state, created_at) = state();

        assert_eq!(
            state.plan_finish("final answer", created_at + CARD_SAFE_AGE),
            FinishPlan::Skip {
                reason: SkipReason::Expired
            }
        );
        assert!(!state.is_live());
        assert!(!state.is_finished(), "an expired card is not finalized");
        assert_eq!(state.sequence(), 0);

        // A later finish cannot revive the card.
        assert_eq!(
            state.plan_finish("final answer", created_at + 2 * CARD_SAFE_AGE),
            FinishPlan::Skip {
                reason: SkipReason::NotLive
            }
        );
    }

    #[test]
    fn finish_with_empty_content_closes_without_writing() {
        let (mut state, now) = state();
        assert_eq!(
            state.plan_finish("", now),
            FinishPlan::Skip {
                reason: SkipReason::Empty
            }
        );
        assert!(!state.is_live());
        assert!(state.is_finished());
        assert_eq!(state.sequence(), 0);
    }

    #[test]
    fn failed_write_kills_the_card_but_keeps_the_state_readable() {
        let (mut state, now) = state();
        assert_eq!(
            state.plan_content("partial", now),
            UpdatePlan::Send { sequence: 1 }
        );
        state.mark_failed();

        assert!(!state.is_live());
        assert_eq!(
            state.plan_content("more", now),
            UpdatePlan::Skip {
                reason: SkipReason::NotLive
            }
        );
        assert_eq!(
            state.plan_finish("more", now),
            FinishPlan::Skip {
                reason: SkipReason::NotLive
            }
        );
        assert_eq!(state.sequence(), 1);
    }

    #[test]
    fn summaries_are_compact_and_unicode_safe() {
        assert_eq!(summarize("  hello\n\nworld  "), "hello world");
        assert_eq!(summarize(""), "");

        let summary = summarize(&"界".repeat(60));
        assert_eq!(summary.chars().count(), MAX_SUMMARY_CHARS);
        assert!(summary.ends_with("..."));
    }

    #[test]
    fn element_id_rules_match_the_sdk() {
        for valid in ["stream_md", "a", "Markdown_1"] {
            assert!(
                validate_element_id(valid).is_ok(),
                "{valid} should be valid"
            );
        }
        for invalid in [
            "",
            "1stream",
            "stream-md",
            "stream md",
            "界",
            "a23456789012345678901", // 21 characters
        ] {
            assert!(
                validate_element_id(invalid).is_err(),
                "{invalid} should be invalid"
            );
        }
    }
}
