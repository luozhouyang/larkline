//! Configuration schema.
//!
//! The TypeScript bridge owns a fairly large schema and several normalizers.
//! The Rust port types only the fields it actively reads and keeps everything
//! else in a `flatten`ed `extra` map, so:
//!
//! 1. reading an existing `config.json` never loses fields;
//! 2. writing it back preserves settings the Rust binary does not use yet;
//! 3. both implementations can be run side by side during the migration.
//!
//! Anything that later needs first-class handling graduates from `extra` into
//! a typed field.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

/// Current root config schema version (`config.json.schemaVersion`).
pub const SCHEMA_VERSION: u32 = 2;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum TenantBrand {
    #[default]
    Feishu,
    Lark,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum AgentKind {
    #[default]
    Claude,
    Codex,
}

impl AgentKind {
    pub fn binary_name(self) -> &'static str {
        match self {
            AgentKind::Claude => "claude",
            AgentKind::Codex => "codex",
        }
    }

    pub fn display_name(self) -> &'static str {
        match self {
            AgentKind::Claude => "Claude Code",
            AgentKind::Codex => "Codex",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum ProfileMode {
    #[default]
    Personal,
    Team,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum MessageReplyMode {
    Card,
    Markdown,
    Text,
}

impl Default for MessageReplyMode {
    fn default() -> Self {
        Self::Markdown
    }
}

/// Whether a run publishes a chain-of-thought (COT) process message.
///
/// Mirrors `CotMessagesMode` in `src/config/schema.ts`. The legacy values
/// (`on`, `simple`) are accepted by [`deserialize_cot_messages`] and collapse
/// onto [`CotMessagesMode::Detailed`] / [`CotMessagesMode::Brief`], exactly
/// like `getCotMessages` in TypeScript.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum CotMessagesMode {
    Off,
    Brief,
    Detailed,
}

impl Default for CotMessagesMode {
    fn default() -> Self {
        Self::Off
    }
}

impl CotMessagesMode {
    /// Whether the mode publishes anything at all (TS `cotMessages !== 'off'`).
    pub fn is_enabled(self) -> bool {
        self != Self::Off
    }
}

/// Deserialize `preferences.cotMessages`, accepting the legacy TypeScript
/// values `on` (detailed) and `simple` (brief).
///
/// A typed enum alone would reject those strings and fail the *whole* profile
/// load, so the coercion happens here rather than surfacing as a parse error.
fn deserialize_cot_messages<'de, D>(
    deserializer: D,
) -> std::result::Result<Option<CotMessagesMode>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    use serde::de::Error as DeError;

    let raw = Option::<String>::deserialize(deserializer)?;
    raw.map(|value| match value.as_str() {
        "off" => Ok(CotMessagesMode::Off),
        "brief" | "simple" => Ok(CotMessagesMode::Brief),
        "detailed" | "on" => Ok(CotMessagesMode::Detailed),
        other => Err(DeError::unknown_variant(
            other,
            &["off", "brief", "simple", "detailed", "on"],
        )),
    })
    .transpose()
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum ReasoningDisplay {
    #[default]
    Auto,
    Off,
    Inline,
    Bubble,
}

/// A pointer to a secret stored outside `config.json`.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SecretRef {
    pub source: SecretSource,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub provider: Option<String>,
    pub id: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum SecretSource {
    Env,
    File,
    Exec,
}

/// A secret field is either a literal string (possibly a `${VAR}` template)
/// or a reference. The untagged representation matches the JSON both ways.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum SecretInput {
    Plain(String),
    Reference(SecretRef),
}

impl SecretInput {
    pub fn is_set(&self) -> bool {
        match self {
            SecretInput::Plain(value) => !value.is_empty(),
            SecretInput::Reference(reference) => !reference.id.is_empty(),
        }
    }

    pub fn as_reference(&self) -> Option<&SecretRef> {
        match self {
            SecretInput::Reference(reference) => Some(reference),
            SecretInput::Plain(_) => None,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AppCredentials {
    pub id: String,
    pub secret: SecretInput,
    #[serde(default)]
    pub tenant: TenantBrand,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct Accounts {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub app: Option<AppCredentials>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Preferences {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub message_reply: Option<MessageReplyMode>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub message_reply_migrated: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub show_tool_calls: Option<bool>,
    #[serde(
        default,
        deserialize_with = "deserialize_cot_messages",
        skip_serializing_if = "Option::is_none"
    )]
    pub cot_messages: Option<CotMessagesMode>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reasoning_display: Option<ReasoningDisplay>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_concurrent_runs: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub require_mention_in_group: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub agent_stop_grace_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub run_idle_timeout_minutes: Option<u64>,
    #[serde(flatten)]
    pub extra: Map<String, Value>,
}

impl Default for Preferences {
    fn default() -> Self {
        Self {
            message_reply: Some(MessageReplyMode::Markdown),
            message_reply_migrated: None,
            show_tool_calls: Some(true),
            cot_messages: None,
            reasoning_display: None,
            max_concurrent_runs: Some(10),
            require_mention_in_group: None,
            agent_stop_grace_ms: None,
            run_idle_timeout_minutes: None,
            extra: Map::new(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct Access {
    #[serde(default)]
    pub allowed_users: Vec<String>,
    #[serde(default)]
    pub allowed_chats: Vec<String>,
    #[serde(default)]
    pub admins: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub require_mention_in_group: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub chat_require_mention: Option<BTreeMap<String, bool>>,
    #[serde(flatten)]
    pub extra: Map<String, Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ProfileConfig {
    #[serde(default = "default_schema_version")]
    pub schema_version: u32,
    #[serde(default)]
    pub agent_kind: AgentKind,
    #[serde(default)]
    pub mode: ProfileMode,
    #[serde(default)]
    pub accounts: Accounts,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub preferences: Option<Preferences>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub access: Option<Access>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub secrets: Option<Value>,
    #[serde(flatten)]
    pub extra: Map<String, Value>,
}

impl Default for ProfileConfig {
    fn default() -> Self {
        Self {
            schema_version: SCHEMA_VERSION,
            agent_kind: AgentKind::Claude,
            mode: ProfileMode::Personal,
            accounts: Accounts::default(),
            preferences: Some(Preferences::default()),
            access: Some(Access::default()),
            secrets: None,
            extra: Map::new(),
        }
    }
}

impl ProfileConfig {
    pub fn new(agent: AgentKind, app_id: &str, secret: SecretInput, tenant: TenantBrand) -> Self {
        Self {
            agent_kind: agent,
            accounts: Accounts {
                app: Some(AppCredentials {
                    id: app_id.to_string(),
                    secret,
                    tenant,
                }),
            },
            ..Self::default()
        }
    }

    pub fn app(&self) -> Option<&AppCredentials> {
        self.accounts.app.as_ref()
    }

    pub fn is_complete(&self) -> bool {
        self.app()
            .map(|app| !app.id.is_empty() && app.secret.is_set())
            .unwrap_or(false)
    }

    /// Message reply mode with the pre-0.1.27 legacy coercion applied
    /// (`text` + not migrated -> `markdown`).
    pub fn message_reply(&self) -> MessageReplyMode {
        let prefs = self.preferences.as_ref();
        if let Some(prefs) = prefs {
            if prefs.message_reply == Some(MessageReplyMode::Text)
                && prefs.message_reply_migrated != Some(true)
            {
                return MessageReplyMode::Markdown;
            }
            if let Some(mode) = prefs.message_reply {
                return mode;
            }
        }
        MessageReplyMode::Markdown
    }

    pub fn show_tool_calls(&self) -> bool {
        self.preferences
            .as_ref()
            .and_then(|p| p.show_tool_calls)
            .unwrap_or(true)
    }

    /// COT process-message mode, defaulting to off (TS `getCotMessages`).
    pub fn cot_messages(&self) -> CotMessagesMode {
        self.preferences
            .as_ref()
            .and_then(|p| p.cot_messages)
            .unwrap_or_default()
    }

    /// Where reasoning is rendered, defaulting to the legacy `auto`
    /// (TS `getReasoningDisplay`).
    pub fn reasoning_display(&self) -> ReasoningDisplay {
        self.preferences
            .as_ref()
            .and_then(|p| p.reasoning_display)
            .unwrap_or_default()
    }

    pub fn max_concurrent_runs(&self) -> u32 {
        self.preferences
            .as_ref()
            .and_then(|p| p.max_concurrent_runs)
            .map(|n| n.clamp(1, 50))
            .unwrap_or(10)
    }

    pub fn agent_stop_grace_ms(&self) -> u64 {
        self.preferences
            .as_ref()
            .and_then(|p| p.agent_stop_grace_ms)
            .map(|n| n.clamp(100, 30_000))
            .unwrap_or(5_000)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RootConfig {
    #[serde(default = "default_schema_version")]
    pub schema_version: u32,
    #[serde(default)]
    pub active_profile: String,
    #[serde(default)]
    pub profiles: BTreeMap<String, ProfileConfig>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub secrets: Option<Value>,
    #[serde(flatten)]
    pub extra: Map<String, Value>,
}

impl Default for RootConfig {
    fn default() -> Self {
        Self {
            schema_version: SCHEMA_VERSION,
            active_profile: String::new(),
            profiles: BTreeMap::new(),
            secrets: None,
            extra: Map::new(),
        }
    }
}

impl RootConfig {
    pub fn profile(&self, name: &str) -> Option<&ProfileConfig> {
        self.profiles.get(name)
    }

    pub fn profile_mut(&mut self, name: &str) -> Option<&mut ProfileConfig> {
        self.profiles.get_mut(name)
    }

    /// The profile named by `LARKLINE_PROFILE`, then `activeProfile`.
    pub fn resolved_profile(&self) -> Option<&str> {
        crate::env::profile_override()
            .map(|name| self.profiles.get_key_value(&name).map(|(k, _)| k.as_str()))
            .unwrap_or_else(|| {
                if self.profiles.contains_key(&self.active_profile) {
                    Some(self.active_profile.as_str())
                } else {
                    None
                }
            })
    }
}

fn default_schema_version() -> u32 {
    SCHEMA_VERSION
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn unknown_fields_roundtrip() {
        let raw = json!({
            "schemaVersion": 2,
            "activeProfile": "bot",
            "preferences": {},
            "profiles": {
                "bot": {
                    "schemaVersion": 2,
                    "agentKind": "codex",
                    "mode": "personal",
                    "accounts": {
                        "app": {
                            "id": "cli_x",
                            "secret": { "source": "exec", "provider": "bridge", "id": "app-cli_x" },
                            "tenant": "feishu"
                        }
                    },
                    "meeting": { "enabled": true, "futureKnob": 42 },
                    "workspaces": { "default": "/tmp/ws" }
                }
            }
        });

        let root: RootConfig = serde_json::from_value(raw.clone()).unwrap();
        let profile = root.profile("bot").unwrap();
        assert_eq!(profile.agent_kind, AgentKind::Codex);
        assert_eq!(profile.app().unwrap().id, "cli_x");
        assert!(profile.extra.contains_key("meeting"));
        assert!(profile.extra.contains_key("workspaces"));

        let back = serde_json::to_value(&root).unwrap();
        assert_eq!(back["profiles"]["bot"]["meeting"]["futureKnob"], 42);
        assert_eq!(back["profiles"]["bot"]["workspaces"]["default"], "/tmp/ws");
    }

    #[test]
    fn legacy_text_reply_is_coerced() {
        let profile: ProfileConfig = serde_json::from_value(json!({
            "preferences": { "messageReply": "text" }
        }))
        .unwrap();
        assert_eq!(profile.message_reply(), MessageReplyMode::Markdown);

        let migrated: ProfileConfig = serde_json::from_value(json!({
            "preferences": { "messageReply": "text", "messageReplyMigrated": true }
        }))
        .unwrap();
        assert_eq!(migrated.message_reply(), MessageReplyMode::Text);
    }

    #[test]
    fn plain_string_secret_is_accepted() {
        let app: AppCredentials = serde_json::from_value(json!({
            "id": "cli_x", "secret": "plain-secret", "tenant": "lark"
        }))
        .unwrap();
        assert!(matches!(app.secret, SecretInput::Plain(_)));
        assert_eq!(app.tenant, TenantBrand::Lark);
    }

    #[test]
    fn cot_messages_accepts_the_legacy_typescript_values() {
        // TS `getCotMessages` coerces `simple` -> brief and `on` -> detailed.
        for (raw, expected) in [
            ("off", CotMessagesMode::Off),
            ("brief", CotMessagesMode::Brief),
            ("simple", CotMessagesMode::Brief),
            ("detailed", CotMessagesMode::Detailed),
            ("on", CotMessagesMode::Detailed),
        ] {
            let profile: ProfileConfig = serde_json::from_value(json!({
                "preferences": { "cotMessages": raw }
            }))
            .unwrap();
            assert_eq!(profile.cot_messages(), expected, "cotMessages={raw}");
        }

        // A typo still surfaces instead of silently disabling the feature.
        let error = serde_json::from_value::<ProfileConfig>(json!({
            "preferences": { "cotMessages": "loud" }
        }))
        .unwrap_err();
        assert!(
            error.to_string().contains("unknown variant"),
            "unexpected error: {error}"
        );

        // Absent preferences keep the COT feature off (TS default).
        let empty: ProfileConfig = serde_json::from_value(json!({})).unwrap();
        assert_eq!(empty.cot_messages(), CotMessagesMode::Off);
        assert!(!empty.cot_messages().is_enabled());
        assert_eq!(empty.reasoning_display(), ReasoningDisplay::Auto);
    }

    #[test]
    fn reasoning_display_resolves_every_typed_value_and_defaults_to_auto() {
        for (raw, expected) in [
            ("auto", ReasoningDisplay::Auto),
            ("off", ReasoningDisplay::Off),
            ("inline", ReasoningDisplay::Inline),
            ("bubble", ReasoningDisplay::Bubble),
        ] {
            let profile: ProfileConfig = serde_json::from_value(json!({
                "preferences": { "reasoningDisplay": raw }
            }))
            .unwrap();
            assert_eq!(profile.reasoning_display(), expected, "display={raw}");
        }
        let missing: ProfileConfig = serde_json::from_value(json!({ "preferences": {} })).unwrap();
        assert_eq!(missing.reasoning_display(), ReasoningDisplay::Auto);
    }
}
