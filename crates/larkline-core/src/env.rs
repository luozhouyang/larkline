//! Bridge-specific environment variables.
//!
//! These mirror `src/config/env-compat.ts` in the TypeScript bridge so an
//! existing deployment keeps working after the Rust switch.

use std::env;

/// Read `LARKLINE_HOME`, falling back to the pre-rename `LARK_CHANNEL_HOME`.
pub fn home_override() -> Option<String> {
    non_empty(env::var("LARKLINE_HOME").ok())
        .or_else(|| non_empty(env::var("LARK_CHANNEL_HOME").ok()))
}

/// Read `LARKLINE_PROFILE`, falling back to `LARK_CHANNEL_PROFILE`.
pub fn profile_override() -> Option<String> {
    non_empty(env::var("LARKLINE_PROFILE").ok())
        .or_else(|| non_empty(env::var("LARK_CHANNEL_PROFILE").ok()))
}

/// True when the process was started by a bridge (`LARKLINE=1`).
pub fn in_bridge_context() -> bool {
    env::var("LARKLINE").ok().as_deref() == Some("1")
}

/// Read an optional agent binary override (`CLAUDE_BIN` / `CODEX_BIN`).
pub fn agent_bin_override(kind: &str) -> Option<String> {
    let upper = kind.to_ascii_uppercase();
    non_empty(env::var(format!("{upper}_BIN")).ok())
}

fn non_empty(value: Option<String>) -> Option<String> {
    value.filter(|v| !v.is_empty())
}
