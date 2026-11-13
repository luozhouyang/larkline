//! Agent run permissions.
//!
//! Port of the parts of `src/config/permissions.ts` and
//! `src/policy/run-policy.ts` that decide how the agent is spawned:
//!
//! 1. read the profile's `permissions` block (canonical `defaultAccess` /
//!    `maxAccess` / `claude.permissionMode`), falling back to the legacy
//!    `sandbox` block and then to the TypeScript defaults (`full` / `full`);
//! 2. clamp the requested default access to `maxAccess` (the agent capability
//!    cap is the profile cap for both adapters, exactly like
//!    `claudeCapability` / `codexCapability`);
//! 3. map the effective access to Codex's `--sandbox` value and Claude's
//!    `--permission-mode` value.
//!
//! The Rust [`ProfileConfig`] keeps unknown fields in `extra`, so both blocks
//! live in `profile.extra["permissions"]` and `profile.extra["sandbox"]`.
//!
//! TypeScript mapping (pinned by the unit tests):
//!
//! | access        | Codex `--sandbox`     | Claude `--permission-mode` |
//! | ------------- | --------------------- | -------------------------- |
//! | `read-only`   | `read-only`           | `plan`                     |
//! | `workspace`   | `workspace-write`     | `acceptEdits`              |
//! | `full`        | `danger-full-access`  | `bypassPermissions`        |
//!
//! A `permissions.claude.permissionMode` override is honoured only when its
//! access rank is no higher than the effective access (the same guard
//! `accessToClaudePermissionMode` applies). With no configuration the
//! effective access is `full`, preserving the previous Rust (and TypeScript)
//! default of `danger-full-access` + `bypassPermissions`.

use larkline_agent::SandboxMode;
use larkline_core::ProfileConfig;
use serde_json::{Map, Value};

/// Effective access level (`AccessMode` in TypeScript).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AccessMode {
    ReadOnly,
    Workspace,
    Full,
}

impl AccessMode {
    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "read-only" => Some(Self::ReadOnly),
            "workspace" => Some(Self::Workspace),
            "full" => Some(Self::Full),
            _ => None,
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::ReadOnly => "read-only",
            Self::Workspace => "workspace",
            Self::Full => "full",
        }
    }

    /// `ACCESS_ORDER` from the TypeScript implementation.
    pub fn rank(self) -> u8 {
        match self {
            Self::ReadOnly => 0,
            Self::Workspace => 1,
            Self::Full => 2,
        }
    }

    /// Codex sandbox mode (`accessToCodexSandbox`).
    pub fn sandbox(self) -> SandboxMode {
        match self {
            Self::ReadOnly => SandboxMode::ReadOnly,
            Self::Workspace => SandboxMode::WorkspaceWrite,
            Self::Full => SandboxMode::DangerFullAccess,
        }
    }

    /// Claude permission mode (`accessToDefaultClaudePermissionMode`).
    pub fn default_claude_permission_mode(self) -> ClaudePermissionMode {
        match self {
            Self::ReadOnly => ClaudePermissionMode::Plan,
            Self::Workspace => ClaudePermissionMode::AcceptEdits,
            Self::Full => ClaudePermissionMode::BypassPermissions,
        }
    }
}

/// Claude Code `--permission-mode` values.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ClaudePermissionMode {
    Default,
    AcceptEdits,
    BypassPermissions,
    Plan,
}

impl ClaudePermissionMode {
    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "default" => Some(Self::Default),
            "acceptEdits" => Some(Self::AcceptEdits),
            "bypassPermissions" => Some(Self::BypassPermissions),
            "plan" => Some(Self::Plan),
            _ => None,
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Default => "default",
            Self::AcceptEdits => "acceptEdits",
            Self::BypassPermissions => "bypassPermissions",
            Self::Plan => "plan",
        }
    }

    /// `CLAUDE_PERMISSION_ACCESS` from the TypeScript implementation.
    pub fn access_rank(self) -> u8 {
        match self {
            Self::Plan => AccessMode::ReadOnly.rank(),
            Self::Default | Self::AcceptEdits => AccessMode::Workspace.rank(),
            Self::BypassPermissions => AccessMode::Full.rank(),
        }
    }
}

/// Resolved run permissions for one profile.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RunPermissions {
    pub default_access: AccessMode,
    pub max_access: AccessMode,
    /// Explicit `permissions.claude.permissionMode`, already validated against
    /// `maxAccess` the way profile normalization does in TypeScript.
    pub claude_permission_mode: Option<ClaudePermissionMode>,
}

impl Default for RunPermissions {
    /// TypeScript `defaultPermissions()`: `{ defaultAccess: 'full', maxAccess:
    /// 'full' }`.
    fn default() -> Self {
        Self {
            default_access: AccessMode::Full,
            max_access: AccessMode::Full,
            claude_permission_mode: None,
        }
    }
}

impl RunPermissions {
    /// Resolve the profile's permission blocks.
    ///
    /// `permissions` is authoritative when present; missing fields inherit
    /// from the legacy `sandbox` base, matching `normalizePermissions`. Invalid
    /// values fall back to the safe default instead of failing the bridge: the
    /// TypeScript config loader rejects the profile, while an already running
    /// Rust bridge must not crash on a hand-edited config.
    pub fn from_profile(profile: &ProfileConfig) -> Self {
        let base = legacy_sandbox(profile.extra.get("sandbox")).unwrap_or_default();
        match profile.extra.get("permissions") {
            Some(value) => Self::from_canonical(value, base),
            None => base,
        }
    }

    fn from_canonical(value: &Value, base: Self) -> Self {
        let Some(object) = value.as_object() else {
            tracing::warn!("ignoring non-object profile permissions block");
            return base;
        };

        let max_access = read_access(object.get("maxAccess")).unwrap_or(base.max_access);
        let default_access = read_access(object.get("defaultAccess")).unwrap_or_else(|| {
            if base.default_access.rank() <= max_access.rank() {
                base.default_access
            } else {
                max_access
            }
        });
        // TypeScript throws when `defaultAccess > maxAccess`; clamping is the
        // safe equivalent and keeps the bridge running.
        let default_access = clamp_access(default_access, max_access, max_access);

        let claude_permission_mode = object
            .get("claude")
            .and_then(Value::as_object)
            .and_then(|claude| claude.get("permissionMode"))
            .and_then(Value::as_str)
            .and_then(ClaudePermissionMode::parse)
            .filter(|mode| mode.access_rank() <= max_access.rank());

        Self {
            default_access,
            max_access,
            claude_permission_mode,
        }
    }

    /// `clampAccess(defaultAccess, profileMax, capabilityMax)` with the
    /// capability cap equal to `maxAccess` for both agents.
    pub fn effective_access(self) -> AccessMode {
        clamp_access(self.default_access, self.max_access, self.max_access)
    }

    /// Codex `--sandbox` value for the effective access.
    pub fn sandbox(self) -> SandboxMode {
        self.effective_access().sandbox()
    }

    /// Claude `--permission-mode` value, honouring `claude.permissionMode`
    /// only when `accessToClaudePermissionMode` would.
    pub fn claude_permission_mode(self) -> ClaudePermissionMode {
        let access = self.effective_access();
        match self.claude_permission_mode {
            Some(mode) if mode.access_rank() <= access.rank() => mode,
            _ => access.default_claude_permission_mode(),
        }
    }

    /// Whether Claude should route permission prompts to the bridge.
    ///
    /// `default` asks for anything that would need approval and `acceptEdits`
    /// still asks for commands/network/MCP tools; both become card approvals.
    /// `plan` cannot execute tools and `bypassPermissions` never asks, so they
    /// keep the plain one-shot stdin path.
    pub fn claude_approvals_enabled(self) -> bool {
        self.claude_permission_mode().asks_for_approval()
    }
}

impl ClaudePermissionMode {
    /// Modes whose prompts should reach the bridge's approval card.
    pub fn asks_for_approval(self) -> bool {
        matches!(self, Self::Default | Self::AcceptEdits)
    }
}

/// `clampAccess` from `src/config/permissions.ts`.
pub fn clamp_access(
    default_access: AccessMode,
    profile_max: AccessMode,
    capability_max: AccessMode,
) -> AccessMode {
    let max_allowed = if profile_max.rank() < capability_max.rank() {
        profile_max
    } else {
        capability_max
    };
    if default_access.rank() <= max_allowed.rank() {
        default_access
    } else {
        max_allowed
    }
}

fn read_access(value: Option<&Value>) -> Option<AccessMode> {
    value.and_then(Value::as_str).and_then(AccessMode::parse)
}

/// Parse the legacy `sandbox` block: `{default,max}` or
/// `{defaultMode,maxMode}`, in Codex naming. Returns `None` when the block is
/// absent or has none of the recognized fields.
fn legacy_sandbox(value: Option<&Value>) -> Option<RunPermissions> {
    let object = value?.as_object()?;
    if !has_legacy_fields(object) {
        return None;
    }
    let max_access = read_sandbox(object.get("max").or_else(|| object.get("maxMode")))
        .unwrap_or(AccessMode::Full);
    let default_access = read_sandbox(object.get("default").or_else(|| object.get("defaultMode")))
        .unwrap_or(max_access);
    Some(RunPermissions {
        default_access: clamp_access(default_access, max_access, max_access),
        max_access,
        claude_permission_mode: None,
    })
}

fn has_legacy_fields(object: &Map<String, Value>) -> bool {
    ["default", "max", "defaultMode", "maxMode"]
        .iter()
        .any(|key| object.get(*key).is_some())
}

fn read_sandbox(value: Option<&Value>) -> Option<AccessMode> {
    match value.and_then(Value::as_str) {
        Some("read-only") => Some(AccessMode::ReadOnly),
        Some("workspace-write") => Some(AccessMode::Workspace),
        Some("danger-full-access") => Some(AccessMode::Full),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use larkline_core::config::AgentKind;
    use serde_json::json;

    fn profile() -> ProfileConfig {
        ProfileConfig {
            agent_kind: AgentKind::Claude,
            ..ProfileConfig::default()
        }
    }

    fn with_permissions(value: Value) -> ProfileConfig {
        let mut profile = profile();
        profile.extra.insert("permissions".into(), value);
        profile
    }

    #[test]
    fn absent_config_keeps_the_typescript_full_default() {
        let permissions = RunPermissions::from_profile(&profile());
        assert_eq!(permissions.effective_access(), AccessMode::Full);
        assert_eq!(permissions.sandbox(), SandboxMode::DangerFullAccess);
        assert_eq!(
            permissions.claude_permission_mode(),
            ClaudePermissionMode::BypassPermissions
        );
        // The previous Rust hardcoding is exactly the absent-config behavior.
        assert_eq!(permissions, RunPermissions::default());
    }

    #[test]
    fn default_access_is_clamped_to_max_access() {
        let permissions = RunPermissions::from_profile(&with_permissions(json!({
            "defaultAccess": "full",
            "maxAccess": "read-only"
        })));
        assert_eq!(permissions.max_access, AccessMode::ReadOnly);
        assert_eq!(permissions.effective_access(), AccessMode::ReadOnly);
        assert_eq!(permissions.sandbox(), SandboxMode::ReadOnly);
        assert_eq!(
            permissions.claude_permission_mode(),
            ClaudePermissionMode::Plan
        );

        // `workspace` is the middle rung in both directions.
        let permissions = RunPermissions::from_profile(&with_permissions(json!({
            "defaultAccess": "workspace",
            "maxAccess": "workspace"
        })));
        assert_eq!(permissions.effective_access(), AccessMode::Workspace);
        assert_eq!(permissions.sandbox(), SandboxMode::WorkspaceWrite);
        assert_eq!(
            permissions.claude_permission_mode(),
            ClaudePermissionMode::AcceptEdits
        );

        // A missing default inherits read-only from the explicit max clamp.
        let permissions = RunPermissions::from_profile(&with_permissions(json!({
            "maxAccess": "read-only"
        })));
        assert_eq!(permissions.effective_access(), AccessMode::ReadOnly);
    }

    #[test]
    fn claude_override_is_honoured_only_within_the_effective_access() {
        let permissions = RunPermissions::from_profile(&with_permissions(json!({
            "defaultAccess": "full",
            "maxAccess": "full",
            "claude": { "permissionMode": "default" }
        })));
        assert_eq!(
            permissions.claude_permission_mode(),
            ClaudePermissionMode::Default
        );

        // `acceptEdits` needs workspace access; read-only falls back to plan.
        let permissions = RunPermissions::from_profile(&with_permissions(json!({
            "defaultAccess": "read-only",
            "maxAccess": "full",
            "claude": { "permissionMode": "acceptEdits" }
        })));
        assert_eq!(
            permissions.claude_permission_mode(),
            ClaudePermissionMode::Plan
        );

        // An override above maxAccess is invalid and ignored.
        let permissions = RunPermissions::from_profile(&with_permissions(json!({
            "defaultAccess": "workspace",
            "maxAccess": "workspace",
            "claude": { "permissionMode": "bypassPermissions" }
        })));
        assert_eq!(permissions.claude_permission_mode, None);
        assert_eq!(
            permissions.claude_permission_mode(),
            ClaudePermissionMode::AcceptEdits
        );
    }

    #[test]
    fn legacy_sandbox_block_is_read_when_permissions_are_absent() {
        let mut profile = profile();
        profile.extra.insert(
            "sandbox".into(),
            json!({ "defaultMode": "workspace-write", "maxMode": "workspace-write" }),
        );
        let permissions = RunPermissions::from_profile(&profile);
        assert_eq!(permissions.effective_access(), AccessMode::Workspace);
        assert_eq!(permissions.sandbox(), SandboxMode::WorkspaceWrite);
        assert_eq!(
            permissions.claude_permission_mode(),
            ClaudePermissionMode::AcceptEdits
        );

        // Canonical permissions win, but inherit missing fields from the
        // legacy base just like `normalizeCanonicalPermissions`.
        profile.extra.insert(
            "permissions".into(),
            json!({ "defaultAccess": "read-only" }),
        );
        let permissions = RunPermissions::from_profile(&profile);
        assert_eq!(permissions.max_access, AccessMode::Workspace);
        assert_eq!(permissions.effective_access(), AccessMode::ReadOnly);
        assert_eq!(permissions.sandbox(), SandboxMode::ReadOnly);
    }

    #[test]
    fn invalid_values_fall_back_instead_of_panicking() {
        let permissions = RunPermissions::from_profile(&with_permissions(json!("nope")));
        assert_eq!(permissions, RunPermissions::default());

        let permissions = RunPermissions::from_profile(&with_permissions(json!({
            "defaultAccess": "root",
            "maxAccess": 42
        })));
        assert_eq!(permissions, RunPermissions::default());

        // Legacy sandbox names are not canonical access names.
        let mut profile = profile();
        profile.extra.insert(
            "sandbox".into(),
            json!({ "default": "full", "max": "danger-full-access" }),
        );
        let permissions = RunPermissions::from_profile(&profile);
        assert_eq!(permissions.effective_access(), AccessMode::Full);
    }

    #[test]
    fn clamp_access_matches_the_typescript_helper() {
        assert_eq!(
            clamp_access(AccessMode::Full, AccessMode::Workspace, AccessMode::Full),
            AccessMode::Workspace
        );
        assert_eq!(
            clamp_access(AccessMode::Full, AccessMode::Full, AccessMode::Workspace),
            AccessMode::Workspace
        );
        assert_eq!(
            clamp_access(AccessMode::ReadOnly, AccessMode::Full, AccessMode::Full),
            AccessMode::ReadOnly
        );
    }
}
