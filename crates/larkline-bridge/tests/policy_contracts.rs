//! Behaviour contracts for the implemented subset of the TS access policy.
//!
//! `src/policy/access.ts` has a wider surface (runtime owner, team mode,
//! `allowedChats`, per-chat mention overrides) than the Rust port. These tests
//! cover the rules the Rust bridge actually implements: the DM allowlist, the
//! group mention requirement and its profile-wide overrides, and bot senders.
//! Each case is a direct port of the corresponding `access.test.ts` /
//! `mention-policy.test.ts` contract.

use lark_channel::message::NormalizedMessage;
use larkline_bridge::policy;
use larkline_core::ProfileConfig;
use larkline_core::config::{Access, AgentKind};
use serde_json::{Value, json};

const BOT: &str = "ou_bot";

fn profile() -> ProfileConfig {
    ProfileConfig {
        agent_kind: AgentKind::Claude,
        access: Some(Access::default()),
        ..ProfileConfig::default()
    }
}

fn message(value: Value) -> NormalizedMessage {
    serde_json::from_value(value).expect("normalized message fixture")
}

fn group_with(mentions: Value) -> NormalizedMessage {
    message(json!({
        "message_id": "om_group",
        "chat_id": "oc_group",
        "chat_type": "group",
        "sender_id": "ou_user",
        "sender": { "open_id": "ou_user", "sender_type": "user" },
        "mentions": mentions,
        "text": "hello"
    }))
}

fn mention_bot() -> Value {
    json!([{ "key": "@_user_1", "open_id": BOT, "mentioned_type": "bot" }])
}

#[test]
fn group_messages_require_a_bot_mention_by_default() {
    let profile = profile();
    assert!(!policy::allowed(
        &profile,
        &group_with(json!([])),
        Some(BOT)
    ));
    assert!(policy::allowed(
        &profile,
        &group_with(mention_bot()),
        Some(BOT)
    ));
}

#[test]
fn access_mention_override_wins_over_preferences_in_both_directions() {
    // Explicit `access.requireMentionInGroup: false` opens the group even
    // though preferences still default to requiring the mention.
    let mut open = profile();
    open.preferences
        .as_mut()
        .expect("default preferences")
        .require_mention_in_group = Some(true);
    open.access = Some(Access {
        require_mention_in_group: Some(false),
        ..Access::default()
    });
    assert!(policy::allowed(&open, &group_with(json!([])), Some(BOT)));

    // Explicit true keeps the requirement even when preferences say false.
    let mut strict = profile();
    strict
        .preferences
        .as_mut()
        .expect("default preferences")
        .require_mention_in_group = Some(false);
    strict.access = Some(Access {
        require_mention_in_group: Some(true),
        ..Access::default()
    });
    assert!(!policy::allowed(&strict, &group_with(json!([])), Some(BOT)));
}

#[test]
fn preferences_mention_override_applies_when_access_has_none() {
    let mut open = profile();
    open.access = Some(Access::default());
    open.preferences
        .as_mut()
        .expect("default preferences")
        .require_mention_in_group = Some(false);
    assert!(policy::allowed(&open, &group_with(json!([])), Some(BOT)));

    // With neither override the safe default is still "require @".
    let mut defaulted = profile();
    defaulted.access = None;
    defaulted.preferences = None;
    assert!(!policy::allowed(
        &defaulted,
        &group_with(json!([])),
        Some(BOT)
    ));
}

#[test]
fn bot_senders_still_need_an_explicit_mention_when_the_gate_is_open() {
    // The mention requirement can be disabled for humans, but two bots must
    // not answer each other forever: a bot sender without a mention stays
    // ignored.
    let mut profile = profile();
    profile.access = Some(Access {
        require_mention_in_group: Some(false),
        ..Access::default()
    });

    let from_bot = |mentions: Value| {
        message(json!({
            "message_id": "om_bot",
            "chat_id": "oc_group",
            "chat_type": "group",
            "sender_id": "ou_other_bot",
            "sender": { "open_id": "ou_other_bot", "sender_type": "bot" },
            "mentions": mentions,
            "text": "hello"
        }))
    };
    assert!(!policy::allowed(&profile, &from_bot(json!([])), Some(BOT)));
    assert!(policy::allowed(
        &profile,
        &from_bot(mention_bot()),
        Some(BOT)
    ));
}

#[test]
fn allowlist_uses_the_open_id_with_a_sender_id_fallback() {
    // `sender.open_id` is the precise identity when present.
    let mut profile = profile();
    profile.access = Some(Access {
        allowed_users: vec!["ou_fallback".into()],
        ..Access::default()
    });
    let fallback = message(json!({
        "message_id": "om_dm",
        "chat_id": "oc_dm",
        "chat_type": "p2p",
        "sender_id": "ou_fallback",
        "sender": { "open_id": "", "sender_type": "user" },
        "text": "hi"
    }));
    assert!(policy::allowed(&profile, &fallback, None));

    // A listed user is admitted, an unlisted one fails closed.
    let listed = message(json!({
        "message_id": "om_listed",
        "chat_id": "oc_dm",
        "chat_type": "p2p",
        "sender_id": "ou_listed",
        "sender": { "open_id": "ou_listed", "sender_type": "user" },
        "text": "hi"
    }));
    profile.access = Some(Access {
        allowed_users: vec!["ou_listed".into()],
        ..Access::default()
    });
    assert!(policy::allowed(&profile, &listed, None));
    let stranger = message(json!({
        "message_id": "om_stranger",
        "chat_id": "oc_dm",
        "chat_type": "p2p",
        "sender_id": "ou_stranger",
        "sender": { "open_id": "ou_stranger", "sender_type": "user" },
        "text": "hi"
    }));
    assert!(!policy::allowed(&profile, &stranger, None));
}
