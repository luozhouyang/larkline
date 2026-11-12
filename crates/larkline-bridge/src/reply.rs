//! Thread-aware outbound replies.
//!
//! The bridge keys every message run by [`crate::scope_key`]: `chatId` for a
//! p2p / regular group and `chatId:threadId` for a topic group. A top-level
//! `im/v1/messages` send always lands in the chat, which for a topic group
//! means outside the thread the user wrote in. The Feishu reply API can place
//! a message in the parent message's thread with `reply_in_thread: true`, so
//! for a topic scope every user-visible reply should be a reply to the message
//! that triggered it.
//!
//! This module owns the routing decision ([`reply_route`], pure and unit
//! tested) and the two send paths the run flow needs:
//!
//! * [`send_text`] — plain text replies (command replies, `/meeting` replies,
//!   text-mode final answers, errors that fall back to text).
//! * [`send_card`] / [`start_streaming_card`] — the run card and its streaming
//!   CardKit variant, so a topic run's card is created inside the thread.
//!
//! A topic reply needs the parent message id. When the caller has none (for
//! example a meeting broadcast to its origin chat), the route falls back to a
//! top-level chat message: a reply cannot be threaded without a parent. The
//! fallback is deliberate — losing the message would be worse.
//!
//! Only *message* scopes are topics. Comment (`comment:<hash>`), document
//! (`doc:<hash>`) and meeting (`meeting:<id>`) scopes also contain a colon but
//! are not chat topics; [`is_topic_scope`] rejects them by requiring the chat
//! half to be a Feishu chat id (`oc_…`).

use lark_channel::Card;
use lark_channel::message::MessageId;
use larkline_lark::{Channel, StreamingCard};

/// Where one outbound reply should go.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ReplyRoute<'a> {
    /// A top-level message in the chat (the pre-existing behaviour).
    Chat,
    /// A reply to `parent_message_id`, placed in its thread.
    Thread { parent_message_id: &'a str },
}

/// Whether `scope` is a topic-group message scope (`chatId:threadId`).
///
/// The chat half must look like a Feishu chat id (`oc_…`) and the thread half
/// must be non-empty. That keeps `comment:…`, `doc:…` and `meeting:…` scopes
/// out of the topic path even though they contain a colon.
pub(crate) fn is_topic_scope(scope: &str) -> bool {
    let Some((chat_id, thread_id)) = scope.split_once(':') else {
        return false;
    };
    chat_id.starts_with("oc_") && !chat_id.is_empty() && !thread_id.is_empty()
}

/// Pick the delivery route for a reply in `scope`.
///
/// * topic scope + a known triggering message → threaded reply;
/// * topic scope without a message id → top-level chat message (cannot reply
///   without a parent);
/// * every other scope → top-level chat message.
pub(crate) fn reply_route<'a>(scope: &str, source_message_id: Option<&'a str>) -> ReplyRoute<'a> {
    match source_message_id.filter(|id| !id.is_empty()) {
        Some(parent_message_id) if is_topic_scope(scope) => {
            ReplyRoute::Thread { parent_message_id }
        }
        _ => ReplyRoute::Chat,
    }
}

/// Send a plain-text reply through the route selected by [`reply_route`].
///
/// A threaded reply that the API rejects falls back to a top-level chat
/// message so the user still gets the answer; the failure is logged.
pub(crate) async fn send_text(
    channel: &Channel,
    scope: &str,
    chat_id: &str,
    source_message_id: Option<&str>,
    text: impl Into<String>,
) -> larkline_lark::LarkResult<MessageId> {
    let text = text.into();
    match reply_route(scope, source_message_id) {
        ReplyRoute::Chat => channel.send_text(chat_id, text).await,
        ReplyRoute::Thread { parent_message_id } => {
            let parent = MessageId(parent_message_id.to_owned());
            match channel
                .sender()
                .text_reply(parent, text.clone())
                .reply_in_thread(true)
                .send()
                .await
            {
                Ok(id) => Ok(id),
                Err(error) => {
                    tracing::warn!(
                        error = %error,
                        chat = %chat_id,
                        parent = %parent_message_id,
                        "thread reply failed; falling back to a top-level message"
                    );
                    channel.send_text(chat_id, text).await
                }
            }
        }
    }
}

/// Send a non-streaming run card through the route selected by
/// [`reply_route`]. A threaded card that the API rejects falls back to a
/// top-level card, mirroring [`send_text`].
pub(crate) async fn send_card(
    channel: &Channel,
    scope: &str,
    chat_id: &str,
    source_message_id: Option<&str>,
    card: &Card,
) -> larkline_lark::LarkResult<MessageId> {
    match reply_route(scope, source_message_id) {
        ReplyRoute::Chat => channel.send_card(chat_id, card).await,
        ReplyRoute::Thread { parent_message_id } => {
            let parent = MessageId(parent_message_id.to_owned());
            match channel
                .sender()
                .card_reply(parent, card.clone())
                .reply_in_thread(true)
                .send()
                .await
            {
                Ok(id) => Ok(id),
                Err(error) => {
                    tracing::warn!(
                        error = %error,
                        chat = %chat_id,
                        parent = %parent_message_id,
                        "thread card reply failed; falling back to a top-level card"
                    );
                    channel.send_card(chat_id, card).await
                }
            }
        }
    }
}

/// Create and send the streaming run card through the route selected by
/// [`reply_route`].
///
/// CardKit entities are created the same way for both routes; only the
/// message that references the entity differs (`card_reference_message` vs a
/// threaded `card_reference_reply`). If the threaded reference send fails the
/// entity is left unused and the error is returned, exactly like the top-level
/// path: the caller falls back to [`send_card`].
pub(crate) async fn start_streaming_card(
    channel: &Channel,
    scope: &str,
    chat_id: &str,
    source_message_id: Option<&str>,
    card: Card,
    element_id: &str,
) -> larkline_lark::LarkResult<StreamingCard> {
    match reply_route(scope, source_message_id) {
        ReplyRoute::Chat => StreamingCard::start(channel, chat_id, card, element_id).await,
        ReplyRoute::Thread { parent_message_id } => {
            StreamingCard::start_reply(
                channel,
                MessageId(parent_message_id.to_owned()),
                card,
                element_id,
            )
            .await
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn topic_scopes_are_detected_only_for_chat_threads() {
        assert!(is_topic_scope("oc_1:omt_a"));
        assert!(is_topic_scope("oc_123:omt_456"));
        assert!(!is_topic_scope("oc_1"));
        assert!(!is_topic_scope("oc_1:"));
        assert!(!is_topic_scope(":omt_a"));
        // Other colon-separated bridge scopes are not chat topics.
        assert!(!is_topic_scope("comment:0123456789abcdef"));
        assert!(!is_topic_scope("doc:0123456789abcdef"));
        assert!(!is_topic_scope("meeting:70001"));
    }

    #[test]
    fn topic_replies_use_the_triggering_message_as_parent() {
        assert_eq!(
            reply_route("oc_1:omt_a", Some("om_trigger")),
            ReplyRoute::Thread {
                parent_message_id: "om_trigger"
            }
        );
    }

    #[test]
    fn non_topic_replies_stay_top_level() {
        assert_eq!(reply_route("oc_1", Some("om_trigger")), ReplyRoute::Chat);
        assert_eq!(
            reply_route("comment:abc", Some("om_trigger")),
            ReplyRoute::Chat
        );
        assert_eq!(
            reply_route("meeting:70001", Some("om_trigger")),
            ReplyRoute::Chat
        );
    }

    #[test]
    fn topic_scope_without_a_parent_falls_back_to_the_chat() {
        assert_eq!(reply_route("oc_1:omt_a", None), ReplyRoute::Chat);
        assert_eq!(reply_route("oc_1:omt_a", Some("")), ReplyRoute::Chat);
    }

    #[test]
    fn topic_thread_ids_may_contain_colons() {
        // `split_once(':')` keeps everything after the first colon as the
        // thread id, so a thread id that itself contains a colon still routes
        // as a threaded reply.
        assert!(is_topic_scope("oc_1:omt_a:b"));
        assert_eq!(
            reply_route("oc_1:omt_a:b", Some("om_trigger")),
            ReplyRoute::Thread {
                parent_message_id: "om_trigger"
            }
        );
        // The chat half must still look like a Feishu chat id.
        assert!(!is_topic_scope("chat_1:omt_a"));
    }
}
