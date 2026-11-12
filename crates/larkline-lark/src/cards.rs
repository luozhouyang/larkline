//! Card builders.
//!
//! Cards are plain JSON, so the builders stay small and data-driven. The
//! shape follows CardKit 2.0 (`schema: "2.0"`), matching what the TypeScript
//! bridge sends so existing chats render identically.

use lark_channel::Card;
use serde_json::{Value, json};

/// One-line status card used while a run is in progress.
pub fn running_card(title: &str, detail: &str) -> Card {
    card(json!({
        "schema": "2.0",
        "config": { "update_multi": true, "summary": { "content": title } },
        "body": {
            "elements": [
                { "tag": "markdown", "content": format!("**{title}**\n{detail}") }
            ]
        }
    }))
}

/// Final answer card.
pub fn final_card(title: &str, body_markdown: &str) -> Card {
    card(json!({
        "schema": "2.0",
        "config": { "update_multi": true, "summary": { "content": title } },
        "body": {
            "elements": [
                { "tag": "markdown", "content": body_markdown }
            ]
        }
    }))
}

/// Error card with the same visual language as the running card.
pub fn error_card(title: &str, message: &str) -> Card {
    card(json!({
        "schema": "2.0",
        "config": { "update_multi": true, "summary": { "content": title } },
        "body": {
            "elements": [
                { "tag": "markdown", "content": format!("❌ **{title}**\n{message}") }
            ]
        }
    }))
}

fn card(value: Value) -> Card {
    // `Card::raw` wraps a *body*; these builders already produce a complete
    // schema 2.0 document, so validate it as-is.
    Card::from_value(value).expect("builder-produced card is valid")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cards_are_schema_2() {
        for card in [
            running_card("t", "d"),
            final_card("t", "body"),
            error_card("t", "boom"),
        ] {
            assert_eq!(card.as_value()["schema"], "2.0");
        }
    }
}
