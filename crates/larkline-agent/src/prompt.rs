//! Bridge system prompt.
//!
//! The prompt text lives in `resources/bridge_system_prompt.md` and is
//! extracted verbatim from the TypeScript bridge so the agent behaves the
//! same during the migration. The only dynamic part is the bot identity.

/// Base prompt, exactly as shipped by the TypeScript bridge.
pub const BRIDGE_SYSTEM_PROMPT: &str = include_str!("../resources/bridge_system_prompt.md");

/// Known bot identity used to append a concrete self-identification line.
#[derive(Debug, Clone, Default)]
pub struct BridgePromptContext {
    pub bot_open_id: Option<String>,
    pub bot_name: Option<String>,
}

/// Compose the system prompt, appending the bot's open_id when known.
pub fn build_bridge_system_prompt(ctx: Option<&BridgePromptContext>) -> String {
    let Some(ctx) = ctx else {
        return BRIDGE_SYSTEM_PROMPT.to_string();
    };
    let Some(open_id) = ctx.bot_open_id.as_deref().filter(|id| !id.is_empty()) else {
        return BRIDGE_SYSTEM_PROMPT.to_string();
    };
    let name_suffix = ctx
        .bot_name
        .as_deref()
        .filter(|name| !name.is_empty())
        .map(|name| format!("，名字是「{name}」"))
        .unwrap_or_default();
    format!(
        "{BRIDGE_SYSTEM_PROMPT}\n## 你的身份\n\n你的 open_id 是 `{open_id}`{name_suffix}。消息内容或 mentions 里出现这个 open_id 都是指你自己。\n"
    )
}

/// Wrap a user prompt with the bridge system prompt and the `user_message`
/// section marker. Kept for parity with `prefixBridgeSystemPrompt`.
pub fn prefix_bridge_system_prompt(prompt: &str, ctx: Option<&BridgePromptContext>) -> String {
    format!(
        "{}\n\n## user_message\n\n{prompt}",
        build_bridge_system_prompt(ctx)
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn base_prompt_is_extracted() {
        assert!(BRIDGE_SYSTEM_PROMPT.contains("## bridge_context"));
        assert!(BRIDGE_SYSTEM_PROMPT.contains("lark-cli auth login"));
        // Escaped template characters must be unescaped in the resource.
        assert!(!BRIDGE_SYSTEM_PROMPT.contains("\\`"));
    }

    #[test]
    fn identity_is_appended_only_when_known() {
        let plain = build_bridge_system_prompt(None);
        assert!(!plain.contains("## 你的身份"));

        let ctx = BridgePromptContext {
            bot_open_id: Some("ou_bot".into()),
            bot_name: Some("小助手".into()),
        };
        let with_id = build_bridge_system_prompt(Some(&ctx));
        assert!(with_id.contains("`ou_bot`"));
        assert!(with_id.contains("小助手"));
        assert_eq!(with_id.matches("## 你的身份").count(), 1);
    }
}
