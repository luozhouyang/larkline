//! Message emoji reactions (`im/v1/messageReaction`).
//!
//! The bridge decorates the triggering message with a "Typing" reaction as an
//! instant acknowledgement and removes it once the run settles. The SDK does
//! not model these endpoints, so they are called through [`RestClient`].

use reqwest::Method;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use crate::rest::{RestClient, RestError, RestResult};

/// Emoji type the bridge uses while a run is in flight.
pub const TYPING: &str = "Typing";

const LIST_PAGE_SIZE: &str = "50";
const BOT_OPERATOR_TYPE: &str = "app";

/// Add `emoji_type` to `message_id` and return the new `reaction_id`.
pub async fn add_reaction(
    rest: &RestClient,
    message_id: &str,
    emoji_type: &str,
) -> RestResult<String> {
    let data = rest
        .post(
            &message_reactions_path(message_id),
            &json!({ "reaction_type": { "emoji_type": emoji_type } }),
        )
        .await?;
    parse_reaction_id(data)
}

/// Delete one of the bot's reactions by `reaction_id`.
///
/// Only the bot's own reactions can be deleted; deleting a reaction added by
/// a user fails with a Feishu permission error.
pub async fn remove_reaction(
    rest: &RestClient,
    message_id: &str,
    reaction_id: &str,
) -> RestResult<()> {
    rest.request(
        Method::DELETE,
        &message_reaction_path(message_id, reaction_id),
        None,
        &[],
    )
    .await
    .map(|_| ())
}

/// Delete the bot's reaction matching `emoji_type`, if it exists.
///
/// Lists the message's reactions filtered by emoji, picks the one added by
/// this app (`operator_type == "app"`), and deletes it. Returns `true` when a
/// matching reaction was deleted and `false` when the bot never added it.
pub async fn remove_reaction_by_emoji(
    rest: &RestClient,
    message_id: &str,
    emoji_type: &str,
) -> RestResult<bool> {
    let data = rest
        .get(
            &message_reactions_path(message_id),
            &[("reaction_type", emoji_type), ("page_size", LIST_PAGE_SIZE)],
        )
        .await?;
    let list: ReactionList = serde_json::from_value(data)
        .map_err(|error| RestError::Message(format!("invalid reaction list payload: {error}")))?;
    let Some(reaction_id) = bot_reaction_id(list, emoji_type) else {
        return Ok(false);
    };
    remove_reaction(rest, message_id, &reaction_id).await?;
    Ok(true)
}

fn parse_reaction_id(data: Value) -> RestResult<String> {
    let created: ReactionCreated = serde_json::from_value(data)
        .map_err(|error| RestError::Message(format!("invalid reaction payload: {error}")))?;
    created.reaction_id.ok_or_else(|| {
        RestError::Message("message reaction create returned no reaction_id".to_string())
    })
}

/// Pick the reaction the bot itself added for `emoji_type`.
fn bot_reaction_id(list: ReactionList, emoji_type: &str) -> Option<String> {
    list.items
        .into_iter()
        .find(|item| {
            item.operator
                .as_ref()
                .and_then(|operator| operator.operator_type.as_deref())
                == Some(BOT_OPERATOR_TYPE)
                && item
                    .reaction_type
                    .as_ref()
                    .is_none_or(|kind| kind.emoji_type == emoji_type)
        })
        .map(|item| item.reaction_id)
}

/// `/open-apis/im/v1/messages/{message_id}/reactions`
fn message_reactions_path(message_id: &str) -> String {
    format!(
        "/open-apis/im/v1/messages/{}/reactions",
        encode_path_segment(message_id)
    )
}

/// `/open-apis/im/v1/messages/{message_id}/reactions/{reaction_id}`
fn message_reaction_path(message_id: &str, reaction_id: &str) -> String {
    format!(
        "{}/{}",
        message_reactions_path(message_id),
        encode_path_segment(reaction_id)
    )
}

/// Percent-encode one path segment, keeping only RFC 3986 unreserved bytes.
fn encode_path_segment(value: &str) -> String {
    let mut encoded = String::with_capacity(value.len());
    for byte in value.bytes() {
        if byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.' | b'~') {
            encoded.push(char::from(byte));
        } else {
            encoded.push('%');
            encoded.push(hex_digit(byte >> 4));
            encoded.push(hex_digit(byte & 0x0f));
        }
    }
    encoded
}

fn hex_digit(nibble: u8) -> char {
    match nibble {
        0..=9 => char::from(b'0' + nibble),
        _ => char::from(b'A' + nibble - 10),
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct ReactionCreated {
    #[serde(default)]
    reaction_id: Option<String>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
struct ReactionList {
    #[serde(default)]
    items: Vec<Reaction>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
struct Reaction {
    #[serde(default)]
    reaction_id: String,
    #[serde(default)]
    reaction_type: Option<ReactionType>,
    #[serde(default)]
    operator: Option<ReactionOperator>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
struct ReactionType {
    #[serde(default)]
    emoji_type: String,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
struct ReactionOperator {
    #[serde(default)]
    operator_type: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn reaction_paths_use_the_im_v1_endpoint() {
        assert_eq!(
            message_reactions_path("om_message"),
            "/open-apis/im/v1/messages/om_message/reactions"
        );
        assert_eq!(
            message_reaction_path("om_message", "re_action"),
            "/open-apis/im/v1/messages/om_message/reactions/re_action"
        );
    }

    #[test]
    fn reaction_paths_percent_encode_ids() {
        assert_eq!(
            message_reaction_path("a/b c", "re/1"),
            "/open-apis/im/v1/messages/a%2Fb%20c/reactions/re%2F1"
        );
        assert_eq!(
            message_reactions_path("表情"),
            "/open-apis/im/v1/messages/%E8%A1%A8%E6%83%85/reactions"
        );
    }

    #[test]
    fn parses_created_reaction_id() {
        assert_eq!(
            parse_reaction_id(json!({ "reaction_id": "re_1" })).unwrap(),
            "re_1"
        );
    }

    #[test]
    fn missing_reaction_id_is_an_error() {
        let error = parse_reaction_id(json!({})).unwrap_err();
        match error {
            RestError::Message(message) => assert!(message.contains("no reaction_id")),
            other => panic!("unexpected error: {other:?}"),
        }
    }

    #[test]
    fn malformed_reaction_payload_is_an_error() {
        let error = parse_reaction_id(json!({ "reaction_id": 7 })).unwrap_err();
        match error {
            RestError::Message(message) => assert!(message.contains("invalid reaction payload")),
            other => panic!("unexpected error: {other:?}"),
        }
    }

    #[test]
    fn picks_the_app_owned_reaction_with_the_requested_emoji() {
        let list: ReactionList = serde_json::from_value(json!({
            "items": [
                {
                    "reaction_id": "re_user",
                    "reaction_type": { "emoji_type": "Typing" },
                    "operator": { "operator_type": "user" }
                },
                {
                    "reaction_id": "re_other_emoji",
                    "reaction_type": { "emoji_type": "DONE" },
                    "operator": { "operator_type": "app" }
                },
                {
                    "reaction_id": "re_app",
                    "reaction_type": { "emoji_type": "Typing" },
                    "operator": { "operator_type": "app" }
                }
            ]
        }))
        .unwrap();

        assert_eq!(bot_reaction_id(list, "Typing").as_deref(), Some("re_app"));
    }

    #[test]
    fn returns_none_without_an_app_reaction() {
        let list: ReactionList = serde_json::from_value(json!({
            "items": [{
                "reaction_id": "re_user",
                "reaction_type": { "emoji_type": "Typing" },
                "operator": { "operator_type": "user" }
            }]
        }))
        .unwrap();

        assert_eq!(bot_reaction_id(list, "Typing"), None);
    }

    #[test]
    fn reaction_payloads_round_trip() {
        let created = ReactionCreated {
            reaction_id: Some("re_1".into()),
        };
        let value = serde_json::to_value(&created).unwrap();
        assert_eq!(
            serde_json::from_value::<ReactionCreated>(value).unwrap(),
            created
        );

        let list: ReactionList = serde_json::from_value(json!({
            "items": [{
                "reaction_id": "re_1",
                "reaction_type": { "emoji_type": "Typing" },
                "operator": { "operator_type": "app" }
            }]
        }))
        .unwrap();
        let value = serde_json::to_value(&list).unwrap();
        assert_eq!(serde_json::from_value::<ReactionList>(value).unwrap(), list);
    }
}
