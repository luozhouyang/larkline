//! Policy: should the bridge answer this message at all?
//!
//! A deliberately small subset of `src/policy/access.ts`. The full version
//! also resolves the runtime owner and admin list from Lark; this one handles
//! the two rules that decide almost every message:
//!
//! * direct messages honour the allowlist;
//! * group/topic messages require an @-mention unless disabled per profile.
//!
//! Bot senders are ignored unless explicitly mentioned, which is what keeps
//! two bots from talking to each other forever.

use lark_channel::message::MessageChatType;
use lark_channel::message::MessageSenderType;
use lark_channel::message::NormalizedMessage;
use larkline_core::ProfileConfig;

pub fn allowed(
    profile: &ProfileConfig,
    message: &NormalizedMessage,
    bot_open_id: Option<&str>,
) -> bool {
    let sender_type = message.sender.sender_type;
    let sender = if message.sender.open_id.is_empty() {
        message.sender_id.as_str()
    } else {
        message.sender.open_id.as_str()
    };

    if sender_type == MessageSenderType::Bot && !mentions_bot(message, bot_open_id) {
        return false;
    }

    let access = profile.access.as_ref();
    let allowed_users = access.map(|a| a.allowed_users.as_slice()).unwrap_or(&[]);
    if !allowed_users.is_empty() && !allowed_users.iter().any(|id| id == sender) {
        return false;
    }

    match message.chat_type {
        MessageChatType::P2p => true,
        MessageChatType::Group => {
            if require_mention(profile) {
                mentions_bot(message, bot_open_id)
            } else {
                true
            }
        }
        MessageChatType::Unknown => false,
    }
}

fn require_mention(profile: &ProfileConfig) -> bool {
    if let Some(access) = profile.access.as_ref() {
        if let Some(value) = access.require_mention_in_group {
            return value;
        }
    }
    profile
        .preferences
        .as_ref()
        .and_then(|p| p.require_mention_in_group)
        .unwrap_or(true)
}

/// True when the message @-mentions the bridge bot.
///
/// `bot_open_id` is the precise check. Before the handshake resolves it, fall
/// back to "mentions a bot" so the bridge still answers instead of going
/// silent in groups.
fn mentions_bot(message: &NormalizedMessage, bot_open_id: Option<&str>) -> bool {
    message.mentions.iter().any(|mention| match bot_open_id {
        Some(id) => mention.open_id == id,
        None => mention.mentioned_type == MessageSenderType::Bot,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use larkline_core::config::{Access, AgentKind};
    use serde_json::json;

    fn profile() -> ProfileConfig {
        ProfileConfig {
            agent_kind: AgentKind::Claude,
            access: Some(Access::default()),
            ..ProfileConfig::default()
        }
    }

    fn message(value: serde_json::Value) -> NormalizedMessage {
        serde_json::from_value(value).expect("valid normalized message")
    }

    #[test]
    fn dm_is_allowed() {
        let msg = message(json!({
            "message_id": "om_1",
            "chat_id": "oc_1",
            "chat_type": "p2p",
            "sender_id": "ou_1",
            "sender": { "open_id": "ou_1", "sender_type": "user" },
            "text": "hi"
        }));
        assert!(allowed(&profile(), &msg, Some("ou_bot")));
    }

    #[test]
    fn unknown_chat_type_is_ignored() {
        let msg = message(json!({
            "message_id": "om_1",
            "chat_id": "oc_1",
            "sender_id": "ou_1",
            "text": "hi"
        }));
        assert!(!allowed(&profile(), &msg, None));
    }

    #[test]
    fn group_requires_mention_by_default() {
        let msg = message(json!({
            "message_id": "om_1",
            "chat_id": "oc_g",
            "chat_type": "group",
            "sender_id": "ou_1",
            "sender": { "open_id": "ou_1", "sender_type": "user" },
            "text": "hi"
        }));
        assert!(!allowed(&profile(), &msg, Some("ou_bot")));

        let mentioned = message(json!({
            "message_id": "om_2",
            "chat_id": "oc_g",
            "chat_type": "group",
            "sender_id": "ou_1",
            "sender": { "open_id": "ou_1", "sender_type": "user" },
            "mentions": [{ "key": "@_user_1", "open_id": "ou_bot", "mentioned_type": "bot" }],
            "text": "hi @bot"
        }));
        assert!(allowed(&profile(), &mentioned, Some("ou_bot")));
    }

    #[test]
    fn allowlist_fails_closed() {
        let mut profile = profile();
        profile.access = Some(Access {
            allowed_users: vec!["ou_owner".into()],
            ..Access::default()
        });
        let msg = message(json!({
            "message_id": "om_1",
            "chat_id": "oc_1",
            "chat_type": "p2p",
            "sender_id": "ou_stranger",
            "sender": { "open_id": "ou_stranger", "sender_type": "user" },
            "text": "hi"
        }));
        assert!(!allowed(&profile, &msg, None));
    }

    #[test]
    fn bots_need_an_explicit_mention() {
        let msg = message(json!({
            "message_id": "om_1",
            "chat_id": "oc_g",
            "chat_type": "group",
            "sender_id": "ou_other_bot",
            "sender": { "open_id": "ou_other_bot", "sender_type": "bot" },
            "text": "hello"
        }));
        assert!(!allowed(&profile(), &msg, Some("ou_bot")));
    }
}
