//! In-chat commands that the Rust bridge previously did not implement.
//!
//! This module is a port of the handlers in `src/commands/index.ts` that need
//! the command dispatcher (admin gating / session resets), plus the commands
//! that were missing from the original port. `/pwd`, `/status`, `/config`,
//! `/stop`, `/meeting` and `/help` stay in [`crate::run`].
//!
//! | command | TypeScript handler | Rust behaviour |
//! | --- | --- | --- |
//! | `/account` | `handleAccount` | current app/profile; credential changes are guidance only (no CardKit form) |
//! | `/cd` | `handleCd` | admin-only; expands `~`, interrupts the live run, resets the scope session |
//! | `/doc` | `handleDoc` | exact TS reply |
//! | `/doctor` | `handleDoctor` | in-process self-check summary; no agent echo probe |
//! | `/exit` | `handleExit` | CLI guidance (`larkline kill` / `larkline stop`); never fakes a shutdown |
//! | `/invite` | `handleInvite` | access-list mutation under the config lock; `all group` is guidance only |
//! | `/ps` | `handlePs` | live run scopes and queued batches from bridge state |
//! | `/new`, `/reset` | `handleNew` | interrupts the live run, bumps the scope session generation, clears the session |
//! | `/reconnect` | `handleReconnect` | guidance (the SDK exposes no reconnect handle) |
//! | `/remove` | `handleRemove` | access-list mutation under the config lock |
//! | `/resume` | `handleResume` | `/resume use <id>` (and `/resume <id>`) persist a session for the scope |
//! | `/timeout` | `handleTimeout` | per-scope idle timeout in `sessions.json` |
//! | `/ws`, `/work` | `handleWs` | named workspace aliases through [`larkline_core::WorkspaceStore`] |
//!
//! Everything user-visible is a text reply, routed by `run.rs` through the
//! topic-aware [`crate::reply`] helpers. Commands never call the network:
//! `all group` needs the chat-list API the port does not have, `/doctor`'s
//! agent echo probe is not ported, and process control (`/exit`) belongs to
//! the CLI. Those cases return explicit guidance instead of pretending to
//! succeed.
//!
//! # Gating
//!
//! The TypeScript dispatcher marks `/account`, `/config`, `/ps`, `/exit`,
//! `/reconnect`, `/doctor`, `/cd`, `/ws`, `/invite`, `/remove` and `/meeting`
//! as admin commands. The handlers that stayed in `run.rs` keep their own
//! gates; this module gates its admin-only commands (including `/cd`) with the
//! same
//! [`crate::owner::can_run_admin_command`] check used by `/meeting` and
//! `/config set` (configured admins, the legacy allowed-users fallback while
//! the admin list is empty, and the runtime app owner). `/timeout` is open to
//! everyone for the current scope and admin-only when it targets an explicit
//! `comment:<hash>` scope, exactly like the TS handler.

use std::collections::BTreeMap;
use std::path::Path;
use std::sync::Arc;

use lark_channel::message::{MessageChatType, MessageSenderType, NormalizedMessage};
use larkline_core::config::{Access, AgentKind, ProfileConfig, SecretInput, TenantBrand};
use larkline_core::{Keystore, store};

use crate::run::{
    Bridge, message_sender_id, profile_default_workspace, scope_key, validate_cd_target,
};

/// Reply used for a denied admin command (same text as
/// `src/commands/index.ts` and [`crate::meeting::MEETING_DENIED_REPLY`]).
pub const ADMIN_DENIED_REPLY: &str = "❌ 此命令仅管理员可用。";

/// `/doc` reply, copied verbatim from the TypeScript handler.
pub const DOC_REPLY: &str =
    "云文档评论现在不需要绑定工作区；在支持的文档评论里 @bot 即可触发回复。";

/// Reply after `/resume use <id>` applied a session (TS `RESUME_APPLIED_REPLY`).
pub const RESUME_APPLIED_REPLY: &str = "已完成，请继续发送下一条消息。";

/// Reply for `/timeout` display mode, copied verbatim from the TS handler
/// (the note about `/new` is stale there too: `/new` keeps the override).
const TIMEOUT_USAGE: &str = "\n\n用法:\n\
- `/timeout 15` 当前 session 设 15 分钟\n\
- `/timeout off` 当前 session 关闭探活\n\
- `/timeout default` 清除 session 覆盖,回退全局\n\
- `/timeout comment:<scopeHash> 15` 管理员设置 comment scope\n\
\n\
_注:`/new` 会清掉当前 session 的覆盖,回到全局_";

const INVITE_USAGE: &str = "用法：\n\
• `/invite user @某人` — 加入允许私聊\n\
• `/invite admin @某人` — 加入管理员\n\
• `/invite group` — 把当前群加入响应群名单\n\
• `/invite all group` — 把 bot 所在的所有群一键加入";

const REMOVE_USAGE: &str = "用法：\n\
• `/remove user @某人` — 移出用户白名单\n\
• `/remove admin @某人` — 移出管理员\n\
• `/remove group` — 把当前群移出响应群名单";

/// Reply for `/invite all group`: the port has no chat-list API.
const INVITE_ALL_GROUPS_UNAVAILABLE: &str = "⚠️ 本构建没有移植「列出 bot 所在群」的接口（`im/v1/chats`），无法一键加入所有群。\n\
请在每个群里发送 `/invite group`，或在工作机上把这些群 id 加到 `config.json` 的 \
`access.allowedChats`。";

/// Reply for `/account change` and the form submits the port cannot serve.
const ACCOUNT_CHANGE_GUIDANCE: &str = "更换凭据需要走扫码/onboard 流程，本构建不支持在会话内修改凭据。\n\
请在工作机上执行：\n\
- `larkline onboard --name <新 profile>`（扫码创建新应用）\n\
- `larkline profile use <新 profile>`（切换并重启 bot）\n\
当前 profile 的凭据不会被聊天消息修改。";

/// `/exit` cannot stop this process from inside a command handler.
const EXIT_UNAVAILABLE: &str = "当前构建不能从聊天内关闭进程；请在工作机上执行 `larkline kill <id|#>`\
（先用 `larkline ps` 查看），或 `larkline stop` 停止托管服务。";

/// `/reconnect`: the SDK has no force-reconnect handle (see
/// [`crate::keepalive::KEEPALIVE_GAPS`]).
const RECONNECT_UNAVAILABLE: &str = "⚠️ 当前构建无法在进程内重连：`lark-channel` 0.6 没有暴露 WebSocket 重连接口，\
保活看门狗也未启动。\n\
请在工作机上执行 `larkline restart`（托管服务），或停止前台进程后重新运行 `larkline run`。";

/// Guard against a copied separator in a user-supplied alias.
const WORKSPACE_NAME_SEPARATOR: char = '\u{1f}';

/// A user-visible reply plus the bridge state mutation it caused.
///
/// `effect` is `None` for pure reads and for commands that can only return
/// guidance. The caller (`run.rs`) logs non-trivial effects; tests use them to
/// assert that a command really mutated live state instead of only replying.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommandOutcome {
    pub reply: String,
    pub effect: CommandEffect,
}

impl CommandOutcome {
    /// A reply with no state change.
    pub fn reply(reply: impl Into<String>) -> Self {
        Self {
            reply: reply.into(),
            effect: CommandEffect::None,
        }
    }

    /// A reply plus the state change it applied.
    pub fn applied(reply: impl Into<String>, effect: CommandEffect) -> Self {
        Self {
            reply: reply.into(),
            effect,
        }
    }

    /// The text to send back to the chat.
    pub fn into_reply(self) -> String {
        self.reply
    }
}

/// Live state changed by a command.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub enum CommandEffect {
    /// Read-only reply or deliberate guidance; nothing changed.
    #[default]
    None,
    /// `/resume use <id>` pinned a session for this scope.
    SessionResumed {
        scope: String,
        session_id: String,
        cwd: String,
    },
    /// `/new` / `/reset` interrupted the live run and cleared the session.
    SessionReset { scope: String },
    /// `/timeout …` wrote (`Some`) or cleared (`None`) an override.
    TimeoutOverride { scope: String, minutes: Option<u64> },
    /// `/ws use` moved a scope to another cwd and reset its session.
    WorkspaceCwd { scope: String, cwd: String },
    /// `/ws save` created/replaced a named alias.
    WorkspaceAliasSaved { name: String, cwd: String },
    /// `/ws remove` deleted a named alias.
    WorkspaceAliasRemoved { name: String },
    /// `/invite` or `/remove` rewrote the profile access lists.
    AccessLists {
        allowed_users: Vec<String>,
        allowed_chats: Vec<String>,
        admins: Vec<String>,
    },
}

/// Dispatch one command owned by this module.
///
/// Returns `None` for a command `run.rs` still handles inline, so unknown
/// slash text keeps falling through to the agent.
pub async fn handle(
    bridge: &Arc<Bridge>,
    message: &NormalizedMessage,
    command: &str,
    args: &str,
) -> Option<CommandOutcome> {
    // `/work` is an alias for `/ws`; the usage text follows the spelling the
    // user actually sent.
    let invoked = command;
    let canonical = match command {
        "/work" => "/ws",
        other => other,
    };
    if !is_owned_command(canonical) {
        return None;
    }

    // Admin gate first, exactly like the TS dispatcher: a denied command gets
    // the generic reply and never reaches its handler. `/timeout` is not in
    // this set because only its explicit-scope form is admin-only.
    if is_admin_command(canonical)
        && !bridge
            .can_run_admin_command(message_sender_id(message))
            .await
    {
        tracing::info!(command = canonical, "admin-deny");
        return Some(CommandOutcome::reply(ADMIN_DENIED_REPLY));
    }

    let outcome = match canonical {
        "/account" => handle_account(bridge, args).await,
        "/cd" => handle_cd(bridge, message, args).await,
        "/doc" => CommandOutcome::reply(DOC_REPLY),
        "/doctor" => handle_doctor(bridge, message).await,
        "/exit" => handle_exit(args),
        "/invite" => handle_invite(bridge, message, args).await,
        "/new" | "/reset" => handle_new(bridge, message).await,
        "/ps" => handle_ps(bridge),
        "/reconnect" => handle_reconnect(args),
        "/remove" => handle_remove(bridge, message, args).await,
        "/resume" => handle_resume(bridge, message, args).await,
        "/timeout" => handle_timeout(bridge, message, args).await,
        "/ws" => handle_ws(bridge, message, invoked, args).await,
        _ => return None,
    };
    Some(outcome)
}

/// Commands this module dispatches.
fn is_owned_command(command: &str) -> bool {
    matches!(
        command,
        "/account"
            | "/cd"
            | "/doc"
            | "/doctor"
            | "/exit"
            | "/invite"
            | "/new"
            | "/ps"
            | "/reconnect"
            | "/remove"
            | "/reset"
            | "/resume"
            | "/timeout"
            | "/ws"
    )
}

/// Whether the TypeScript dispatcher gates `command` as an admin command.
fn is_admin_command(command: &str) -> bool {
    matches!(
        command,
        "/account"
            | "/cd"
            | "/doctor"
            | "/exit"
            | "/invite"
            | "/ps"
            | "/reconnect"
            | "/remove"
            | "/ws"
    )
}

// ---------------------------------------------------------------------------
// /cd and /new
// ---------------------------------------------------------------------------

/// `/cd <absolute|~/subpath>`: admin-gated cwd change that resets the session.
///
/// Port of the TS `handleCd`: validate first (never interrupt on a bad path),
/// interrupt the live run, persist the scope cwd, clear the session, then reply
/// with the TS text including "session 已重置".
async fn handle_cd(
    bridge: &Arc<Bridge>,
    message: &NormalizedMessage,
    args: &str,
) -> CommandOutcome {
    let scope = scope_key(message);
    let path = match validate_cd_target(args) {
        Ok(path) => path,
        Err(reply) => return CommandOutcome::reply(reply),
    };
    bridge.interrupt_scope(&scope);
    if let Err(err) = bridge.workspace_store().lock().await.set(&scope, &path) {
        return CommandOutcome::reply(format!("保存工作目录失败：{err}"));
    }
    let mut reply = format!("✓ 已切换 cwd 到 `{}`\n（session 已重置）", path.display());
    if let Err(err) = bridge.reset_scope_session(&scope).await {
        reply.push_str(&format!("\n⚠️ session 重置失败：{err}"));
    }
    CommandOutcome::applied(
        reply,
        CommandEffect::WorkspaceCwd {
            scope,
            cwd: path.display().to_string(),
        },
    )
}

/// `/new` / `/reset`: interrupt the live run before clearing the session.
///
/// The run generation is bumped before the clear, so the finishing run cannot
/// save the session it just lost (`save_scope_session_if_current`).
async fn handle_new(bridge: &Arc<Bridge>, message: &NormalizedMessage) -> CommandOutcome {
    let scope = scope_key(message);
    let was_running = bridge.interrupt_scope(&scope);
    match bridge.reset_scope_session(&scope).await {
        Ok(()) => CommandOutcome::applied(
            if was_running {
                "已中断当前任务并开始新会话。"
            } else {
                "已开始新会话。"
            },
            CommandEffect::SessionReset { scope },
        ),
        Err(err) => CommandOutcome::reply(format!("重置失败：{err}")),
    }
}

// ---------------------------------------------------------------------------
// /account
// ---------------------------------------------------------------------------

/// Parsed `/account` form.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AccountCommand {
    Show,
    Change,
    Usage,
}

/// Pure `/account` parser. The TS handler is case-sensitive.
pub fn parse_account(args: &str) -> AccountCommand {
    match args.split_whitespace().next().unwrap_or_default() {
        "" => AccountCommand::Show,
        // `submit`/`cancel` only exist for the CardKit form, which this build
        // does not render; treat them as "change" so the caller sees honest
        // onboarding guidance rather than silence.
        "change" | "submit" | "cancel" => AccountCommand::Change,
        _ => AccountCommand::Usage,
    }
}

async fn handle_account(bridge: &Arc<Bridge>, args: &str) -> CommandOutcome {
    match parse_account(args) {
        AccountCommand::Show => {
            let profile = bridge.live_profile();
            let app = profile.app();
            let app_id = app.map(|app| app.id.as_str()).unwrap_or("(未配置)");
            let app_id = if app_id == "(未配置)" {
                app_id.to_string()
            } else {
                format!("`{}`", mask_app_id(app_id))
            };
            let tenant = app
                .map(|app| tenant_label(app.tenant))
                .unwrap_or("(未配置)");
            CommandOutcome::reply(format!(
                "📋 当前应用\n\
                 App ID: {app_id}\n\
                 Bot 名: {}\n\
                 Tenant: {tenant}\n\
                 profile: {}",
                bridge.bot_name.as_deref().unwrap_or("(未知)"),
                bridge.app_paths().profile,
            ))
        }
        AccountCommand::Change => CommandOutcome::reply(ACCOUNT_CHANGE_GUIDANCE),
        AccountCommand::Usage => CommandOutcome::reply("用法：`/account` 或 `/account change`"),
    }
}

/// Mask an app id the way `src/card/account-cards.ts` does.
pub fn mask_app_id(id: &str) -> String {
    let chars: Vec<char> = id.chars().collect();
    if chars.len() < 12 {
        return id.to_string();
    }
    let prefix: String = chars.iter().take(13).collect();
    let suffix: String = chars.iter().skip(chars.len() - 2).collect();
    format!("{prefix}****{suffix}")
}

// ---------------------------------------------------------------------------
// /doctor
// ---------------------------------------------------------------------------

async fn handle_doctor(bridge: &Arc<Bridge>, message: &NormalizedMessage) -> CommandOutcome {
    let profile = bridge.live_profile();
    let paths = bridge.app_paths();
    let scope = scope_key(message);

    let requested_cwd = {
        bridge
            .workspace_store()
            .lock()
            .await
            .workspace_for(&scope)
            .map(str::to_string)
    };
    let workspace = match requested_cwd {
        Some(path) => format!(
            "workspace: {path} ({})",
            if Path::new(&path).is_dir() {
                "ok"
            } else {
                "missing"
            }
        ),
        None => "workspace: (未设置)".to_string(),
    };
    let workspace_default = if profile_default_workspace(&profile).is_some() {
        "set"
    } else {
        "missing"
    };
    let app = match profile.app() {
        Some(app) => format!("app id: {} (tenant: {})", app.id, tenant_label(app.tenant)),
        None => "app id: (未配置)".to_string(),
    };
    let keystore = Keystore::new(paths.secrets_file.clone(), paths.keystore_salt_file.clone());
    let owner = bridge.owner_state().await;
    let limiter = bridge.run_limiter();
    let binary = bridge.agent_binary.display();
    let binary_state = if bridge.agent_binary.is_file() {
        "found"
    } else {
        "missing"
    };

    CommandOutcome::reply(
        [
            "self-check: ok".to_string(),
            format!("profile: {}", paths.profile),
            format!(
                "agent: {} ({})",
                profile.agent_kind.display_name(),
                agent_kind_label(profile.agent_kind)
            ),
            app,
            secret_status(&profile).to_string(),
            format!(
                "keystore: {} ({})",
                if keystore.exists() {
                    "present"
                } else {
                    "missing"
                },
                keystore.secrets_file().display()
            ),
            format!("agent binary: {binary} ({binary_state})"),
            workspace,
            format!("workspace default: {workspace_default}"),
            "admin: yes".to_string(),
            format!(
                "owner API: {} owner={}",
                owner.state.as_str(),
                if owner.owner_id.is_some() {
                    "present"
                } else {
                    "missing"
                }
            ),
            format!("run slots: {}/{} active", limiter.active(), limiter.cap()),
            "connection: unknown (lark-channel 0.6 exposes no connection status)".to_string(),
            "agent echo check: skipped (not implemented in this build)".to_string(),
        ]
        .join("\n"),
    )
}

fn secret_status(profile: &ProfileConfig) -> &'static str {
    let Some(app) = profile.app() else {
        return "app secret: missing";
    };
    if !app.secret.is_set() {
        return "app secret: missing";
    }
    match &app.secret {
        SecretInput::Plain(_) => "app secret: configured (plaintext)",
        SecretInput::Reference(_) => "app secret: configured (keystore reference)",
    }
}

fn agent_kind_label(kind: AgentKind) -> &'static str {
    match kind {
        AgentKind::Claude => "claude",
        AgentKind::Codex => "codex",
    }
}

fn tenant_label(tenant: TenantBrand) -> &'static str {
    match tenant {
        TenantBrand::Feishu => "feishu",
        TenantBrand::Lark => "lark",
    }
}

// ---------------------------------------------------------------------------
// /exit and /reconnect — process control guidance
// ---------------------------------------------------------------------------

/// Pure `/exit` parser: the target, or `None` for the usage reply.
pub fn parse_exit_target(args: &str) -> Option<String> {
    let target = args.trim();
    (!target.is_empty()).then(|| target.to_string())
}

fn handle_exit(args: &str) -> CommandOutcome {
    match parse_exit_target(args) {
        None => CommandOutcome::reply(format!("用法：`/exit <id|#>`\n\n{EXIT_UNAVAILABLE}")),
        Some(target) => CommandOutcome::reply(format!(
            "⚠️ 当前构建不能从聊天内关闭进程 `{target}`。\n\
             请在工作机上执行 `larkline kill {target}`（先用 `larkline ps` 查看），\
             或 `larkline stop` 停止托管服务。"
        )),
    }
}

/// Pure `/reconnect` parser. `--wait` exists in the TS command; this build
/// cannot reconnect at all, so the flag only changes the wording.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct ReconnectRequest {
    pub wait: bool,
}

pub fn parse_reconnect(args: &str) -> ReconnectRequest {
    ReconnectRequest {
        wait: args.split_whitespace().any(|token| token == "--wait"),
    }
}

fn handle_reconnect(args: &str) -> CommandOutcome {
    let request = parse_reconnect(args);
    let mut reply = String::from(RECONNECT_UNAVAILABLE);
    if request.wait {
        reply.push_str("\n（`--wait` 在当前构建中不生效：没有可挂起的重连流程。）");
    }
    CommandOutcome::reply(reply)
}

// ---------------------------------------------------------------------------
// /ps
// ---------------------------------------------------------------------------

fn handle_ps(bridge: &Arc<Bridge>) -> CommandOutcome {
    let active = bridge.active_scope_list();
    let queued = bridge.queued_scope_list();
    let queue: BTreeMap<&str, usize> = queued
        .iter()
        .map(|(scope, count)| (scope.as_str(), *count))
        .collect();
    let limiter = bridge.run_limiter();
    let profile = bridge.app_paths().profile.clone();

    let mut lines = vec![
        format!("🧭 Bridge 运行状态（profile: {profile}）"),
        String::new(),
        format!("运行槽：{}/{} active", limiter.active(), limiter.cap()),
    ];

    if active.is_empty() && queued.is_empty() {
        lines.push("当前没有正在运行或排队的任务。".to_string());
    } else if !active.is_empty() {
        lines.push(String::new());
        lines.push("活动运行：".to_string());
        for scope in &active {
            lines.push(format!(
                "- `{scope}`（排队 {} 条）",
                queue.get(scope.as_str()).copied().unwrap_or(0)
            ));
        }
    }

    let idle: Vec<&(String, usize)> = queued
        .iter()
        .filter(|(scope, _)| !active.iter().any(|active_scope| active_scope == scope))
        .collect();
    if !idle.is_empty() {
        lines.push(String::new());
        lines.push("排队中（尚未运行）：".to_string());
        for (scope, count) in idle {
            lines.push(format!("- `{scope}`：{count} 条"));
        }
    }

    lines.push(String::new());
    lines.push("用 `/stop <scope>` 中断指定运行（需管理员）。".to_string());
    lines.push(
        "本构建不支持从聊天内关闭进程；请在工作机执行 `larkline ps` / `larkline kill <id|#>`。"
            .to_string(),
    );
    CommandOutcome::reply(lines.join("\n"))
}

// ---------------------------------------------------------------------------
// /invite and /remove — access lists
// ---------------------------------------------------------------------------

/// One access-list target extracted from a message mention.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommandTarget {
    pub open_id: String,
    pub name: Option<String>,
}

impl CommandTarget {
    fn label(&self) -> String {
        self.name
            .clone()
            .filter(|name| !name.trim().is_empty())
            .unwrap_or_else(|| self.open_id.clone())
    }
}

/// Access-list kind addressed by `/invite` and `/remove`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AccessKind {
    User,
    Admin,
    Group,
}

impl AccessKind {
    fn label(self) -> &'static str {
        match self {
            AccessKind::User => "用户白名单",
            AccessKind::Admin => "管理员",
            AccessKind::Group => "响应群名单",
        }
    }
}

/// Parsed `/invite` form.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum InviteCommand {
    /// `/invite all group`
    AllGroups,
    /// `/invite group`
    Group,
    /// `/invite user|admin @…`
    Targets {
        kind: AccessKind,
        targets: Vec<CommandTarget>,
    },
    /// Missing or unknown kind.
    Usage,
}

/// Parsed `/remove` form.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RemoveCommand {
    Group,
    Targets {
        kind: AccessKind,
        targets: Vec<CommandTarget>,
    },
    Usage,
}

fn lower_tokens(args: &str) -> Vec<String> {
    args.split_whitespace()
        .map(|token| token.to_ascii_lowercase())
        .collect()
}

fn kind_from_tokens(tokens: &[String]) -> Option<AccessKind> {
    tokens.iter().find_map(|token| match token.as_str() {
        "user" => Some(AccessKind::User),
        "admin" => Some(AccessKind::Admin),
        "group" => Some(AccessKind::Group),
        _ => None,
    })
}

/// Pure `/invite` parser. Mentions are resolved by the caller because only a
/// [`NormalizedMessage`] knows them.
pub fn parse_invite(args: &str, targets: Vec<CommandTarget>) -> InviteCommand {
    let tokens = lower_tokens(args);
    if tokens.iter().any(|token| token == "all") && tokens.iter().any(|token| token == "group") {
        return InviteCommand::AllGroups;
    }
    match kind_from_tokens(&tokens) {
        None => InviteCommand::Usage,
        Some(AccessKind::Group) => InviteCommand::Group,
        Some(kind) => InviteCommand::Targets { kind, targets },
    }
}

/// Pure `/remove` parser.
pub fn parse_remove(args: &str, targets: Vec<CommandTarget>) -> RemoveCommand {
    let tokens = lower_tokens(args);
    match kind_from_tokens(&tokens) {
        None => RemoveCommand::Usage,
        Some(AccessKind::Group) => RemoveCommand::Group,
        Some(kind) => RemoveCommand::Targets { kind, targets },
    }
}

/// Human-visible mentions on a message, excluding the bot itself.
fn mention_targets(message: &NormalizedMessage) -> Vec<CommandTarget> {
    message
        .mentions
        .iter()
        .filter(|mention| mention.mentioned_type != MessageSenderType::Bot)
        .filter_map(|mention| {
            normalize_target_id(&mention.open_id).map(|open_id| CommandTarget {
                open_id,
                name: mention.name.clone().filter(|name| !name.trim().is_empty()),
            })
        })
        .collect()
}

/// Validate a target id before it enters a persisted access list.
fn normalize_target_id(raw: &str) -> Option<String> {
    let id = raw.trim();
    if id.is_empty()
        || id.chars().count() > 128
        || id.chars().any(|ch| ch.is_whitespace() || ch.is_control())
    {
        return None;
    }
    Some(id.to_string())
}

async fn handle_invite(
    bridge: &Arc<Bridge>,
    message: &NormalizedMessage,
    args: &str,
) -> CommandOutcome {
    match parse_invite(args, mention_targets(message)) {
        InviteCommand::AllGroups => CommandOutcome::reply(INVITE_ALL_GROUPS_UNAVAILABLE),
        InviteCommand::Usage => CommandOutcome::reply(INVITE_USAGE),
        InviteCommand::Group => invite_current_group(bridge, message).await,
        InviteCommand::Targets { kind, targets } => invite_targets(bridge, kind, targets).await,
    }
}

async fn invite_current_group(bridge: &Arc<Bridge>, message: &NormalizedMessage) -> CommandOutcome {
    if message.chat_type == MessageChatType::P2p {
        return CommandOutcome::reply(
            "❌ `/invite group` 只能在群里发，在私聊里没有 chat_id 可以加。",
        );
    }
    let chat_id = message.chat_id.clone();
    let mut already = false;
    let result = mutate_access(bridge, |access| {
        let mut chats = dedupe(&access.allowed_chats);
        already = chats.iter().any(|id| id == &chat_id);
        if !already {
            chats.push(chat_id.clone());
        }
        access.allowed_chats = chats;
    });
    match result {
        Ok(effect) => {
            let reply = if already {
                "✅ 当前群已在白名单里，无需重复添加。".to_string()
            } else {
                format!("✅ 已把当前群（`{chat_id}`）加入响应群名单。")
            };
            CommandOutcome::applied(reply, effect)
        }
        Err(err) => CommandOutcome::reply(err),
    }
}

async fn invite_targets(
    bridge: &Arc<Bridge>,
    kind: AccessKind,
    targets: Vec<CommandTarget>,
) -> CommandOutcome {
    if targets.is_empty() {
        return CommandOutcome::reply(format!(
            "❌ 没检测到 @ 的用户。请像这样发：`/invite {} @某人`（注意 @ 用户不是 @ bot）。",
            kind_name(kind)
        ));
    }

    let mut added: Vec<String> = Vec::new();
    let mut already: Vec<String> = Vec::new();
    let result = mutate_access(bridge, |access| {
        let list = match kind {
            AccessKind::User => &mut access.allowed_users,
            AccessKind::Admin => &mut access.admins,
            AccessKind::Group => unreachable!("group invites are handled separately"),
        };
        let mut current = dedupe(list);
        for target in &targets {
            if current.iter().any(|id| id == &target.open_id) {
                already.push(target.label());
            } else {
                current.push(target.open_id.clone());
                added.push(target.label());
            }
        }
        *list = current;
    });

    match result {
        Ok(effect) => {
            CommandOutcome::applied(access_targets_reply(kind.label(), &added, &already), effect)
        }
        Err(err) => CommandOutcome::reply(err),
    }
}

async fn handle_remove(
    bridge: &Arc<Bridge>,
    message: &NormalizedMessage,
    args: &str,
) -> CommandOutcome {
    match parse_remove(args, mention_targets(message)) {
        RemoveCommand::Usage => CommandOutcome::reply(REMOVE_USAGE),
        RemoveCommand::Group => remove_current_group(bridge, message).await,
        RemoveCommand::Targets { kind, targets } => remove_targets(bridge, kind, targets).await,
    }
}

async fn remove_current_group(bridge: &Arc<Bridge>, message: &NormalizedMessage) -> CommandOutcome {
    if message.chat_type == MessageChatType::P2p {
        return CommandOutcome::reply("`/remove group` 请在要移除的群里发，私聊里没有可移除的群。");
    }
    let chat_id = message.chat_id.clone();
    let mut missing = false;
    let result = mutate_access(bridge, |access| {
        let mut chats = dedupe(&access.allowed_chats);
        missing = !chats.iter().any(|id| id == &chat_id);
        chats.retain(|id| id != &chat_id);
        access.allowed_chats = chats;
    });
    match result {
        Ok(effect) => {
            let reply = if missing {
                "✅ 当前群本来就不在响应名单里，无需移除。".to_string()
            } else {
                "✅ 已把当前群移出响应群名单。".to_string()
            };
            CommandOutcome::applied(reply, effect)
        }
        Err(err) => CommandOutcome::reply(err),
    }
}

async fn remove_targets(
    bridge: &Arc<Bridge>,
    kind: AccessKind,
    targets: Vec<CommandTarget>,
) -> CommandOutcome {
    if targets.is_empty() {
        return CommandOutcome::reply(format!(
            "请 @ 上要移除的人，例如：`/remove {} @某人`。",
            kind_name(kind)
        ));
    }

    let mut removed: Vec<String> = Vec::new();
    let mut not_there: Vec<String> = Vec::new();
    let result = mutate_access(bridge, |access| {
        let list = match kind {
            AccessKind::User => &mut access.allowed_users,
            AccessKind::Admin => &mut access.admins,
            AccessKind::Group => unreachable!("group removals are handled separately"),
        };
        let mut current = dedupe(list);
        for target in &targets {
            if current.iter().any(|id| id == &target.open_id) {
                current.retain(|id| id != &target.open_id);
                removed.push(target.label());
            } else {
                not_there.push(target.label());
            }
        }
        *list = current;
    });

    match result {
        Ok(effect) => CommandOutcome::applied(
            access_remove_reply(kind.label(), &removed, &not_there),
            effect,
        ),
        Err(err) => CommandOutcome::reply(err),
    }
}

fn access_targets_reply(label: &str, added: &[String], already: &[String]) -> String {
    let mut parts: Vec<String> = Vec::new();
    if !added.is_empty() {
        parts.push(format!("✅ 已把 {} 加入{label}。", added.join("、")));
    }
    if !already.is_empty() {
        parts.push(format!("_{} 已经在{label}里，跳过。_", already.join("、")));
    }
    if parts.is_empty() {
        "未做任何修改。".to_string()
    } else {
        parts.join("\n")
    }
}

fn access_remove_reply(label: &str, removed: &[String], not_there: &[String]) -> String {
    let mut parts: Vec<String> = Vec::new();
    if !removed.is_empty() {
        parts.push(format!("✅ 已把 {} 移出{label}。", removed.join("、")));
    }
    if !not_there.is_empty() {
        parts.push(format!(
            "{} 本来就不在{label}里，无需移除。",
            not_there.join("、")
        ));
    }
    if parts.is_empty() {
        "未做任何修改。".to_string()
    } else {
        parts.join("\n")
    }
}

fn kind_name(kind: AccessKind) -> &'static str {
    match kind {
        AccessKind::User => "user",
        AccessKind::Admin => "admin",
        AccessKind::Group => "group",
    }
}

fn dedupe(list: &[String]) -> Vec<String> {
    let mut unique: Vec<String> = Vec::with_capacity(list.len());
    for id in list {
        if !unique.iter().any(|existing| existing == id) {
            unique.push(id.clone());
        }
    }
    unique
}

/// Mutate the profile access lists under the config lock, then reload the live
/// profile. Mirrors `saveAccessConfig` in `src/config/config-ops.ts`.
fn mutate_access(
    bridge: &Arc<Bridge>,
    mutate: impl FnOnce(&mut Access),
) -> Result<CommandEffect, String> {
    let paths = bridge.app_paths();
    let config_file = paths.config_file.clone();
    let profile_name = paths.profile.clone();
    store::with_config_lock(&config_file, |root| {
        let profile = root.profiles.get_mut(&profile_name).ok_or_else(|| {
            larkline_core::CoreError::Message(format!(
                "profile {profile_name} not found in {}",
                config_file.display()
            ))
        })?;
        let access = profile.access.get_or_insert_with(Access::default);
        mutate(access);
        Ok(())
    })
    .map_err(|err| format!("保存访问名单失败：{err}"))?;
    if let Err(err) = bridge.reload_profile() {
        return Err(format!(
            "访问名单已写入，但重新加载 profile 失败：{err}（重启 bot 后生效）"
        ));
    }
    Ok(access_effect(bridge))
}

fn access_effect(bridge: &Bridge) -> CommandEffect {
    let profile = bridge.live_profile();
    let access = profile.access.unwrap_or_default();
    CommandEffect::AccessLists {
        allowed_users: access.allowed_users,
        allowed_chats: access.allowed_chats,
        admins: access.admins,
    }
}

// ---------------------------------------------------------------------------
// /resume
// ---------------------------------------------------------------------------

/// Parsed `/resume` form.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ResumeCommand {
    /// List recent sessions. The port cannot list history, but the limit is
    /// still parsed so `/resume 3` behaves like the TS list form.
    List { limit: usize },
    /// Apply a known session id.
    Use { session_id: String },
}

/// Pure `/resume` parser.
///
/// TS only applies a session for `/resume use <nonce>`; the Rust port also
/// accepts `/resume <session-id>` because that is the documented recovery path
/// here. Numeric arguments keep the TS list semantics (1-20, else 5).
pub fn parse_resume(args: &str) -> ResumeCommand {
    let mut parts = args.split_whitespace();
    let sub = parts.next().unwrap_or_default();
    let rest = parts.collect::<Vec<_>>().join(" ");
    if sub == "use" {
        if !rest.trim().is_empty() {
            return ResumeCommand::Use {
                session_id: rest.trim().to_string(),
            };
        }
        return ResumeCommand::List { limit: 5 };
    }
    match sub.parse::<u32>() {
        Ok(n) if (1..=20).contains(&n) => ResumeCommand::List { limit: n as usize },
        Ok(_) => ResumeCommand::List { limit: 5 },
        Err(_) if !sub.is_empty() => ResumeCommand::Use {
            session_id: sub.to_string(),
        },
        Err(_) => ResumeCommand::List { limit: 5 },
    }
}

async fn handle_resume(
    bridge: &Arc<Bridge>,
    message: &NormalizedMessage,
    args: &str,
) -> CommandOutcome {
    match parse_resume(args) {
        ResumeCommand::Use { session_id } => apply_resume(bridge, message, &session_id).await,
        ResumeCommand::List { limit } => list_resume(bridge, message, limit).await,
    }
}

async fn apply_resume(
    bridge: &Arc<Bridge>,
    message: &NormalizedMessage,
    raw_session_id: &str,
) -> CommandOutcome {
    let profile = bridge.live_profile();
    if profile.agent_kind == AgentKind::Codex {
        return CommandOutcome::reply(
            "⚠️ 本构建未实现 Codex thread 历史与恢复（会话目录未移植）。\
请直接发送消息开始新的 Codex 会话。",
        );
    }

    let session_id = match normalize_session_id(raw_session_id) {
        Ok(id) => id,
        Err(message) => return CommandOutcome::reply(message),
    };
    let scope = scope_key(message);
    let Some(cwd) = bridge
        .workspace_store()
        .lock()
        .await
        .workspace_for(&scope)
        .map(str::to_string)
    else {
        return CommandOutcome::reply("请先使用 /cd <path> 选择工作目录，再查看或恢复会话。");
    };
    if let Err(err) = validate_cd_target(&cwd) {
        return CommandOutcome::reply(format!("工作目录不可用：{err}"));
    }

    // Persist the store's exact cwd string, not the canonicalized path: the
    // run flow compares `resume_for()` against `workspace_for()` byte for byte,
    // so storing a different spelling would make the session unresumable.
    // A live run owns the old session; abort it so the next message starts in
    // the resumed one. Queued messages stay queued (TS interrupts the run
    // without cancelling its pending batch).
    bridge.interrupt_scope(&scope);
    // Any finishing run from before this resume must not overwrite the
    // explicitly chosen session.
    bridge.bump_scope_generation(&scope);
    if let Err(err) = bridge.sessions.lock().await.set(&scope, &session_id, &cwd) {
        return CommandOutcome::reply(format!("保存会话失败：{err}"));
    }
    CommandOutcome::applied(
        RESUME_APPLIED_REPLY,
        CommandEffect::SessionResumed {
            scope,
            session_id,
            cwd,
        },
    )
}

async fn list_resume(
    bridge: &Arc<Bridge>,
    message: &NormalizedMessage,
    limit: usize,
) -> CommandOutcome {
    let scope = scope_key(message);
    let Some(cwd) = bridge
        .workspace_store()
        .lock()
        .await
        .workspace_for(&scope)
        .map(str::to_string)
    else {
        return CommandOutcome::reply("请先使用 /cd <path> 选择工作目录，再查看或恢复会话。");
    };
    if message.chat_type != MessageChatType::P2p {
        return CommandOutcome::reply(
            "群聊中不展示历史会话详情。请私聊 bot 使用 `/resume` 查看和选择历史会话。",
        );
    }

    let profile = bridge.live_profile();
    if profile.agent_kind == AgentKind::Codex {
        return CommandOutcome::reply(
            "⚠️ 本构建未实现 Codex thread 历史与恢复（会话目录未移植）。\
请直接发送消息开始新的 Codex 会话。",
        );
    }

    let current = bridge
        .sessions
        .lock()
        .await
        .raw(&scope)
        .and_then(|entry| entry.session_id.clone());
    let current = match current {
        Some(id) => format!("`{}…`", id.chars().take(8).collect::<String>()),
        None => "(无)".to_string(),
    };
    CommandOutcome::reply(format!(
        "📚 历史会话\n\
         当前 cwd：`{cwd}`\n\
         当前 session：{current}\n\
         本构建没有移植历史会话读取，无法列出最近 {limit} 个会话。\n\
         用 `/resume use <session-id>` 恢复已知会话。"
    ))
}

fn normalize_session_id(raw: &str) -> Result<String, String> {
    let id = raw.trim();
    if id.is_empty() {
        return Err("用法：`/resume use <session-id>`".to_string());
    }
    if id.chars().count() > 256
        || id.starts_with('-')
        || id.chars().any(|ch| ch.is_whitespace() || ch.is_control())
    {
        return Err(format!("无效的 session id：`{id}`"));
    }
    Ok(id.to_string())
}

// ---------------------------------------------------------------------------
// /timeout
// ---------------------------------------------------------------------------

/// Parsed `/timeout` request, including the scope it targets.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TimeoutRequest {
    pub scope: String,
    /// Empty means "show the effective value"; otherwise `off`, `default` or
    /// a minute count.
    pub value: String,
    /// True when the request named an explicit `comment:<hash>` scope.
    pub targeted: bool,
}

/// Pure `/timeout` parser, a direct port of `parseTimeoutTarget`.
pub fn parse_timeout(args: &str, current_scope: &str) -> TimeoutRequest {
    let input = args.trim().to_ascii_lowercase();
    let parts: Vec<&str> = input.split_whitespace().collect();
    let first = parts.first().copied().unwrap_or_default();
    if first.starts_with("comment:") {
        return TimeoutRequest {
            scope: first.to_string(),
            value: parts[1..].join(" "),
            targeted: true,
        };
    }
    TimeoutRequest {
        scope: current_scope.to_string(),
        value: input,
        targeted: false,
    }
}

async fn handle_timeout(
    bridge: &Arc<Bridge>,
    message: &NormalizedMessage,
    args: &str,
) -> CommandOutcome {
    let current_scope = scope_key(message);
    let parsed = parse_timeout(args, &current_scope);
    if parsed.targeted
        && !bridge
            .can_run_admin_command(message_sender_id(message))
            .await
    {
        tracing::info!(command = "/timeout", target = %parsed.scope, "admin-deny");
        return CommandOutcome::reply("❌ 指定 scope 设置 timeout 仅管理员可用。");
    }

    let profile = bridge.live_profile();
    let global_minutes = global_idle_timeout_minutes(&profile);
    let global_text = if global_minutes > 0 {
        format!("{global_minutes} 分钟")
    } else {
        "未启用".to_string()
    };

    // Display mode: effective value + source.
    if parsed.value.is_empty() {
        let scope_minutes = bridge
            .sessions
            .lock()
            .await
            .idle_timeout_minutes(&parsed.scope);
        let label = if parsed.targeted {
            format!(" ({})", parsed.scope)
        } else {
            String::new()
        };
        return CommandOutcome::reply(match scope_minutes {
            Some(minutes) => {
                let effective = if minutes > 0 {
                    format!("{minutes} 分钟")
                } else {
                    "已关闭（当前 session）".to_string()
                };
                format!(
                    "⏱ 当前 session{label} 探活:{effective}\n全局默认:{global_text}{TIMEOUT_USAGE}"
                )
            }
            None => format!("⏱ 当前 session{label} 探活:跟随全局({global_text}){TIMEOUT_USAGE}"),
        });
    }

    if parsed.value == "default" {
        let cleared = match bridge
            .sessions
            .lock()
            .await
            .clear_idle_timeout_override(&parsed.scope)
        {
            Ok(cleared) => cleared,
            Err(err) => return CommandOutcome::reply(format!("保存 timeout 失败：{err}")),
        };
        let reply = if cleared {
            format!("✅ 已清除 session 覆盖,回退到全局({global_text})。")
        } else {
            format!("当前 session 本来就没设过覆盖,跟随全局({global_text})。")
        };
        return if cleared {
            CommandOutcome::applied(
                reply,
                CommandEffect::TimeoutOverride {
                    scope: parsed.scope,
                    minutes: None,
                },
            )
        } else {
            CommandOutcome::reply(reply)
        };
    }

    if parsed.value == "off" || parsed.value == "0" {
        if let Err(err) = bridge
            .sessions
            .lock()
            .await
            .set_idle_timeout_minutes(&parsed.scope, 0)
        {
            return CommandOutcome::reply(format!("保存 timeout 失败：{err}"));
        }
        return CommandOutcome::applied(
            "✅ 已关闭当前 session 的探活。",
            CommandEffect::TimeoutOverride {
                scope: parsed.scope,
                minutes: Some(0),
            },
        );
    }

    let minutes = parsed.value.parse::<u64>().ok();
    match minutes {
        Some(n) if (1..=120).contains(&n) => {
            if let Err(err) = bridge
                .sessions
                .lock()
                .await
                .set_idle_timeout_minutes(&parsed.scope, n)
            {
                return CommandOutcome::reply(format!("保存 timeout 失败：{err}"));
            }
            CommandOutcome::applied(
                format!("✅ 当前 session 探活已设为 {n} 分钟。"),
                CommandEffect::TimeoutOverride {
                    scope: parsed.scope,
                    minutes: Some(n),
                },
            )
        }
        _ => CommandOutcome::reply(
            "❌ 用法:`/timeout <1-120>` / `/timeout off` / `/timeout default`",
        ),
    }
}

fn global_idle_timeout_minutes(profile: &ProfileConfig) -> u64 {
    profile
        .preferences
        .as_ref()
        .and_then(|preferences| preferences.run_idle_timeout_minutes)
        .filter(|minutes| *minutes > 0)
        .map(|minutes| minutes.clamp(1, 120))
        .unwrap_or(0)
}

// ---------------------------------------------------------------------------
// /ws and /work — named workspaces
// ---------------------------------------------------------------------------

/// Parsed `/ws` form.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WsCommand {
    List,
    Save(String),
    Use(String),
    Remove(String),
    Usage,
}

/// Pure `/ws` parser, port of `handleWs`.
pub fn parse_ws(args: &str) -> WsCommand {
    let mut parts = args.split_whitespace();
    let sub = parts.next().unwrap_or_default();
    let name = parts.collect::<Vec<_>>().join(" ");
    match sub {
        "" | "list" => WsCommand::List,
        "save" => WsCommand::Save(name),
        "use" => WsCommand::Use(name),
        "remove" | "rm" => WsCommand::Remove(name),
        _ => WsCommand::Usage,
    }
}

async fn handle_ws(
    bridge: &Arc<Bridge>,
    message: &NormalizedMessage,
    invoked: &str,
    args: &str,
) -> CommandOutcome {
    let scope = scope_key(message);
    match parse_ws(args) {
        WsCommand::List => list_workspaces(bridge, &scope, invoked).await,
        WsCommand::Save(name) => save_workspace(bridge, message, invoked, &name).await,
        WsCommand::Use(name) => use_workspace(bridge, message, invoked, &name).await,
        WsCommand::Remove(name) => remove_workspace(bridge, &scope, invoked, &name).await,
        WsCommand::Usage => CommandOutcome::reply(format!(
            "用法：`{invoked} [list|save <name>|use <name>|remove <name>]`"
        )),
    }
}

async fn list_workspaces(bridge: &Arc<Bridge>, scope: &str, invoked: &str) -> CommandOutcome {
    let profile = bridge.app_paths().profile.clone();
    let (current, named) = {
        let store = bridge.workspace_store().lock().await;
        let current = store.workspace_for(scope).map(str::to_string);
        let mut scoped: BTreeMap<String, String> = BTreeMap::new();
        // Aliases are scoped by profile + scope. The owner segment is kept for
        // file compatibility with the TypeScript store, but is not part of the
        // Rust lookup: `botOwnerId` is resolved lazily here (and not at all
        // for configured admins), so requiring one exact owner segment would
        // hide aliases after a refresh.
        for (key, cwd) in store.list_named() {
            if let Some(name) = scoped_alias_name(&profile, scope, &key) {
                scoped.insert(name.to_string(), cwd);
            }
        }
        // Legacy aliases (saved before per-profile/scope scoping, or by the
        // TypeScript bridge) have no separator; they stay visible to everyone.
        for (key, cwd) in store.list_named() {
            if !key.contains(WORKSPACE_NAME_SEPARATOR) && !scoped.contains_key(&key) {
                scoped.insert(key, cwd);
            }
        }
        (current, scoped)
    };

    let mut lines = vec![
        "📂 工作目录".to_string(),
        format!("当前 cwd：`{}`", current.as_deref().unwrap_or("(未设置)")),
        String::new(),
    ];
    if named.is_empty() {
        lines.push("暂无命名工作目录。".to_string());
    } else {
        for (name, cwd) in &named {
            let marker = if current.as_deref() == Some(cwd.as_str()) {
                "  ← 当前"
            } else {
                ""
            };
            lines.push(format!("**{name}** → `{cwd}`{marker}"));
        }
    }
    lines.push(String::new());
    lines.push(format!(
        "💡 发送 `{invoked} save <name>` 把当前 cwd 存为命名工作目录"
    ));
    CommandOutcome::reply(lines.join("\n"))
}

async fn save_workspace(
    bridge: &Arc<Bridge>,
    message: &NormalizedMessage,
    invoked: &str,
    name: &str,
) -> CommandOutcome {
    if name.trim().is_empty() {
        return CommandOutcome::reply(format!("用法：`{invoked} save <name>`"));
    }
    if name.contains(WORKSPACE_NAME_SEPARATOR) {
        return CommandOutcome::reply("工作目录别名不能包含控制字符。");
    }
    let scope = scope_key(message);
    let Some(cwd) = bridge
        .workspace_store()
        .lock()
        .await
        .workspace_for(&scope)
        .map(str::to_string)
    else {
        return CommandOutcome::reply("当前 chat 未设置 cwd，先用 `/cd` 设置再保存。");
    };
    let owner = bridge.owner_state().await.owner_id;
    let profile = bridge.app_paths().profile.clone();
    let key = scoped_workspace_key(&profile, owner.as_deref(), &scope, name);
    if let Err(err) = bridge.workspace_store().lock().await.save_named(&key, &cwd) {
        return CommandOutcome::reply(format!("保存工作目录别名失败：{err}"));
    }
    CommandOutcome::applied(
        format!("✓ 工作目录别名已保存：`{name}` → {cwd}"),
        CommandEffect::WorkspaceAliasSaved {
            name: name.to_string(),
            cwd,
        },
    )
}

async fn use_workspace(
    bridge: &Arc<Bridge>,
    message: &NormalizedMessage,
    invoked: &str,
    name: &str,
) -> CommandOutcome {
    if name.trim().is_empty() {
        return CommandOutcome::reply(format!("用法：`{invoked} use <name>`"));
    }
    let scope = scope_key(message);
    let owner = bridge.owner_state().await.owner_id;
    let profile = bridge.app_paths().profile.clone();
    let cwd = {
        let store = bridge.workspace_store().lock().await;
        scoped_alias_keys(&store, &profile, owner.as_deref(), &scope, name)
            .iter()
            .find_map(|key| store.get_named(key))
            .or_else(|| store.get_named(name))
            .map(str::to_string)
    };
    let Some(cwd) = cwd else {
        return CommandOutcome::reply(format!("未找到工作目录别名：`{name}`"));
    };
    let canonical = match validate_cd_target(&cwd) {
        Ok(path) => path,
        Err(reply) => return CommandOutcome::reply(reply),
    };
    bridge.interrupt_scope(&scope);
    if let Err(err) = bridge
        .workspace_store()
        .lock()
        .await
        .set(&scope, &canonical)
    {
        return CommandOutcome::reply(format!("保存工作目录失败：{err}"));
    }
    let reply = format!(
        "✓ 已切换到 `{name}` ({})\n（session 已重置）",
        canonical.display()
    );
    if let Err(err) = bridge.reset_scope_session(&scope).await {
        return CommandOutcome::applied(
            format!("{reply}\n⚠️ session 重置失败：{err}"),
            CommandEffect::WorkspaceCwd {
                scope,
                cwd: canonical.display().to_string(),
            },
        );
    }
    CommandOutcome::applied(
        reply,
        CommandEffect::WorkspaceCwd {
            scope,
            cwd: canonical.display().to_string(),
        },
    )
}

async fn remove_workspace(
    bridge: &Arc<Bridge>,
    scope: &str,
    invoked: &str,
    name: &str,
) -> CommandOutcome {
    if name.trim().is_empty() {
        return CommandOutcome::reply(format!("用法：`{invoked} remove <name>`"));
    }
    let owner = bridge.owner_state().await.owner_id;
    let profile = bridge.app_paths().profile.clone();
    let removed = {
        let mut store = bridge.workspace_store().lock().await;
        let keys = scoped_alias_keys(&store, &profile, owner.as_deref(), scope, name);
        let mut removed = false;
        for key in keys {
            match store.remove_named(&key) {
                Ok(existed) => removed |= existed,
                Err(err) => {
                    return CommandOutcome::reply(format!("删除工作目录别名失败：{err}"));
                }
            }
        }
        if !removed {
            match store.remove_named(name) {
                Ok(existed) => removed = existed,
                Err(err) => {
                    return CommandOutcome::reply(format!("删除工作目录别名失败：{err}"));
                }
            }
        }
        removed
    };
    if !removed {
        return CommandOutcome::reply(format!("未找到工作目录别名：`{name}`"));
    }
    CommandOutcome::applied(
        format!("✓ 已删除工作目录别名：`{name}`"),
        CommandEffect::WorkspaceAliasRemoved {
            name: name.to_string(),
        },
    )
}

/// The name part of a scoped alias key (`profile\x1fowner\x1fscope\x1fname`).
fn scoped_alias_name<'a>(profile: &str, scope: &str, key: &'a str) -> Option<&'a str> {
    let rest = key.strip_prefix(profile)?;
    let rest = rest.strip_prefix(WORKSPACE_NAME_SEPARATOR)?;
    let (_owner, rest) = rest.split_once(WORKSPACE_NAME_SEPARATOR)?;
    let (key_scope, name) = rest.split_once(WORKSPACE_NAME_SEPARATOR)?;
    (key_scope == scope && !name.is_empty()).then_some(name)
}

/// Candidate keys for one scoped alias.
///
/// The exact owner-scoped key is preferred, then the `owner-unknown` key, then
/// any other owner segment for the same profile/scope/name. The last case keeps
/// aliases saved by the TypeScript bridge (which always knows `botOwnerId`)
/// resolvable here, where the owner is resolved lazily.
fn scoped_alias_keys(
    store: &larkline_core::WorkspaceStore,
    profile: &str,
    owner: Option<&str>,
    scope: &str,
    name: &str,
) -> Vec<String> {
    let mut keys = vec![
        scoped_workspace_key(profile, owner, scope, name),
        scoped_workspace_key(profile, None, scope, name),
    ];
    keys.dedup();
    for key in store.list_named().keys() {
        if scoped_alias_name(profile, scope, key) == Some(name) && !keys.contains(key) {
            keys.push(key.clone());
        }
    }
    keys
}

fn scoped_workspace_prefix(profile: &str, owner: Option<&str>, scope: &str) -> String {
    format!(
        "{profile}{WORKSPACE_NAME_SEPARATOR}{}{WORKSPACE_NAME_SEPARATOR}{scope}{WORKSPACE_NAME_SEPARATOR}",
        owner.unwrap_or("owner-unknown")
    )
}

fn scoped_workspace_key(profile: &str, owner: Option<&str>, scope: &str, name: &str) -> String {
    format!("{}{name}", scoped_workspace_prefix(profile, owner, scope))
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::path::Path;
    use std::time::Duration;

    use larkline_core::config::{Access, Preferences};
    use larkline_core::{AppPaths, ProfileConfig, RootConfig, SecretInput, TenantBrand};
    use larkline_lark::Channel;
    use serde_json::json;

    use super::*;

    fn admin_profile() -> ProfileConfig {
        ProfileConfig {
            access: Some(Access {
                admins: vec!["ou_admin".into()],
                ..Access::default()
            }),
            ..ProfileConfig::default()
        }
    }

    fn app_admin_profile(app_id: &str) -> ProfileConfig {
        let mut profile = ProfileConfig::new(
            AgentKind::Claude,
            app_id,
            SecretInput::Plain("secret".into()),
            TenantBrand::Feishu,
        );
        profile.access = Some(Access {
            admins: vec!["ou_admin".into()],
            ..Access::default()
        });
        profile
    }

    /// No admins, no app id: the admin gate fails closed without any network.
    fn plain_profile() -> ProfileConfig {
        ProfileConfig {
            access: Some(Access::default()),
            ..ProfileConfig::default()
        }
    }

    fn test_bridge(root: &Path, profile: ProfileConfig) -> Arc<Bridge> {
        let paths = AppPaths::resolve(root, "test").expect("app paths");
        let channel = Channel::new("cli_test_app", "secret", TenantBrand::Feishu);
        Arc::new(
            Bridge::new(
                channel,
                profile,
                root.join("nonexistent-agent"),
                root.join("workspace"),
                None,
                None,
                paths,
            )
            .expect("bridge"),
        )
    }

    /// Write a root config so access-list edits have a profile to land in.
    fn seed_config(root: &Path, profile: &ProfileConfig) {
        let paths = AppPaths::resolve(root, "test").expect("app paths");
        let mut config = RootConfig {
            active_profile: "test".into(),
            ..RootConfig::default()
        };
        config.profiles.insert("test".into(), profile.clone());
        larkline_core::store::save_root(&paths.config_file, &config).expect("seed config");
    }

    fn persisted_access(root: &Path) -> Access {
        let paths = AppPaths::resolve(root, "test").expect("app paths");
        larkline_core::store::load_root(&paths.config_file)
            .expect("load config")
            .expect("config exists")
            .profile("test")
            .expect("profile")
            .access
            .clone()
            .expect("access")
    }

    fn message(chat_type: &str, chat_id: &str, sender: &str, text: &str) -> NormalizedMessage {
        serde_json::from_value(json!({
            "message_id": "om_test",
            "chat_id": chat_id,
            "chat_type": chat_type,
            "sender_id": sender,
            "sender": { "open_id": sender, "sender_type": "user" },
            "text": text,
        }))
        .expect("normalized message fixture")
    }

    fn group(text: &str) -> NormalizedMessage {
        message("group", "oc_1", "ou_admin", text)
    }

    fn p2p(text: &str) -> NormalizedMessage {
        message("p2p", "oc_1", "ou_admin", text)
    }

    fn with_mentions(
        mut message: NormalizedMessage,
        mentions: &[(&str, &str, bool)],
    ) -> NormalizedMessage {
        message.mentions = mentions
            .iter()
            .enumerate()
            .map(|(index, (id, name, is_bot))| {
                serde_json::from_value(json!({
                    "key": format!("@_user_{index}"),
                    "open_id": id,
                    "name": name,
                    "mentioned_type": if *is_bot { "bot" } else { "user" },
                }))
                .expect("mention fixture")
            })
            .collect();
        message
    }

    async fn dispatch(
        bridge: &Arc<Bridge>,
        message: &NormalizedMessage,
        text: &str,
    ) -> CommandOutcome {
        let command = text.split_whitespace().next().expect("command");
        let args = text.get(command.len()..).unwrap_or_default().trim();
        handle(bridge, message, command, args)
            .await
            .expect("command belongs to this module")
    }

    /// Every admin-only command answers with the shared denial reply.
    async fn assert_admin_denied(root: &Path, text: &str) {
        let bridge = test_bridge(root, plain_profile());
        let message = message("group", "oc_1", "ou_visitor", text);
        let outcome = dispatch(&bridge, &message, text).await;
        assert_eq!(outcome.reply, ADMIN_DENIED_REPLY);
        assert_eq!(outcome.effect, CommandEffect::None);
    }

    #[test]
    fn parsers_match_the_typescript_shapes() {
        assert_eq!(parse_account(""), AccountCommand::Show);
        assert_eq!(parse_account("change"), AccountCommand::Change);
        assert_eq!(parse_account("submit"), AccountCommand::Change);
        assert_eq!(parse_account("nope"), AccountCommand::Usage);

        assert_eq!(mask_app_id("short"), "short");
        assert_eq!(mask_app_id("cli_1234567890abcdef"), "cli_123456789****ef");

        assert_eq!(parse_ws(""), WsCommand::List);
        assert_eq!(parse_ws("list"), WsCommand::List);
        assert_eq!(
            parse_ws("save main work"),
            WsCommand::Save("main work".into())
        );
        assert_eq!(parse_ws("use main"), WsCommand::Use("main".into()));
        assert_eq!(parse_ws("rm main"), WsCommand::Remove("main".into()));
        assert_eq!(parse_ws("nope"), WsCommand::Usage);

        assert_eq!(parse_resume(""), ResumeCommand::List { limit: 5 });
        assert_eq!(parse_resume("3"), ResumeCommand::List { limit: 3 });
        assert_eq!(parse_resume("99"), ResumeCommand::List { limit: 5 });
        assert_eq!(
            parse_resume("use sess-1"),
            ResumeCommand::Use {
                session_id: "sess-1".into()
            }
        );
        assert_eq!(parse_resume("use"), ResumeCommand::List { limit: 5 });
        assert_eq!(
            parse_resume("sess-direct"),
            ResumeCommand::Use {
                session_id: "sess-direct".into()
            }
        );

        let current = parse_timeout("", "oc_1");
        assert_eq!(current.scope, "oc_1");
        assert_eq!(current.value, "");
        assert!(!current.targeted);
        let targeted = parse_timeout("COMMENT:AbC 15", "oc_1");
        assert_eq!(targeted.scope, "comment:abc");
        assert_eq!(targeted.value, "15");
        assert!(targeted.targeted);

        assert_eq!(
            parse_invite("all group", Vec::new()),
            InviteCommand::AllGroups
        );
        assert_eq!(parse_invite("group", Vec::new()), InviteCommand::Group);
        assert_eq!(parse_invite("", Vec::new()), InviteCommand::Usage);
        assert_eq!(
            parse_remove("remove user @x", Vec::new()),
            RemoveCommand::Targets {
                kind: AccessKind::User,
                targets: Vec::new()
            }
        );

        assert_eq!(parse_exit_target("  #2 ").as_deref(), Some("#2"));
        assert!(!parse_reconnect("").wait);
        assert!(parse_reconnect("--wait").wait);
    }

    #[tokio::test]
    async fn account_shows_the_app_and_guides_credential_changes() {
        let root = tempfile::tempdir().unwrap();
        let bridge = test_bridge(root.path(), app_admin_profile("cli_1234567890abcdef"));
        let message = group("/account");

        let outcome = dispatch(&bridge, &message, "/account").await;
        assert_eq!(outcome.effect, CommandEffect::None);
        assert!(outcome.reply.contains("当前应用"), "{}", outcome.reply);
        assert!(
            outcome.reply.contains("cli_123456789****ef"),
            "{}",
            outcome.reply
        );
        assert!(outcome.reply.contains("profile: test"), "{}", outcome.reply);

        let change = dispatch(&bridge, &message, "/account change").await;
        assert!(change.reply.contains("onboard"), "{}", change.reply);
        assert!(
            change.reply.contains("不会被聊天消息修改"),
            "{}",
            change.reply
        );

        let cancel = dispatch(&bridge, &message, "/account cancel").await;
        assert_eq!(cancel.reply, ACCOUNT_CHANGE_GUIDANCE);

        let usage = dispatch(&bridge, &message, "/account nope").await;
        assert_eq!(usage.reply, "用法：`/account` 或 `/account change`");

        assert_admin_denied(root.path(), "/account").await;
    }

    #[tokio::test]
    async fn doc_returns_the_verbatim_typescript_reply_without_a_gate() {
        let root = tempfile::tempdir().unwrap();
        let bridge = test_bridge(root.path(), plain_profile());
        let message = message("group", "oc_1", "ou_visitor", "/doc anything");

        let outcome = dispatch(&bridge, &message, "/doc anything").await;
        assert_eq!(outcome.reply, DOC_REPLY);
        assert_eq!(
            outcome.reply,
            "云文档评论现在不需要绑定工作区；在支持的文档评论里 @bot 即可触发回复。"
        );
        assert_eq!(outcome.effect, CommandEffect::None);
    }

    #[tokio::test]
    async fn doctor_summarizes_in_process_facts() {
        let root = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(root.path().join("workspace")).unwrap();
        let profile = app_admin_profile("cli_doctor");
        let bridge = test_bridge(root.path(), profile);
        let message = group("/doctor");

        let outcome = dispatch(&bridge, &message, "/doctor").await;
        let reply = &outcome.reply;
        assert!(reply.contains("self-check: ok"), "{reply}");
        assert!(reply.contains("profile: test"), "{reply}");
        assert!(reply.contains("agent: Claude Code (claude)"), "{reply}");
        assert!(reply.contains("app id: cli_doctor"), "{reply}");
        assert!(
            reply.contains("app secret: configured (plaintext)"),
            "{reply}"
        );
        assert!(reply.contains("keystore: missing"), "{reply}");
        assert!(reply.contains("(missing)"), "{reply}");
        assert!(
            reply.contains("workspace: ") && reply.contains("(ok)"),
            "{reply}"
        );
        assert!(reply.contains("workspace default: missing"), "{reply}");
        assert!(reply.contains("admin: yes"), "{reply}");
        assert!(
            reply.contains("owner API: unknown owner=missing"),
            "{reply}"
        );
        assert!(reply.contains("run slots: 0/10 active"), "{reply}");
        assert!(
            reply.contains("connection: unknown (lark-channel 0.6"),
            "{reply}"
        );
        assert!(reply.contains("agent echo check: skipped"), "{reply}");
        assert_eq!(outcome.effect, CommandEffect::None);

        assert_admin_denied(root.path(), "/doctor").await;
    }

    #[tokio::test]
    async fn exit_returns_cli_guidance_and_never_claims_a_shutdown() {
        let root = tempfile::tempdir().unwrap();
        let bridge = test_bridge(root.path(), admin_profile());
        let message = group("/exit");

        let usage = dispatch(&bridge, &message, "/exit").await;
        assert!(
            usage.reply.contains("用法：`/exit <id|#>`"),
            "{}",
            usage.reply
        );
        assert!(
            usage.reply.contains("larkline kill <id|#>"),
            "{}",
            usage.reply
        );
        assert!(usage.reply.contains("larkline stop"), "{}", usage.reply);

        let targeted = dispatch(&bridge, &message, "/exit #2").await;
        assert!(
            targeted.reply.contains("关闭进程 `#2`"),
            "{}",
            targeted.reply
        );
        assert!(
            targeted.reply.contains("larkline kill #2"),
            "{}",
            targeted.reply
        );
        assert!(!targeted.reply.contains("已关闭 bot"), "{}", targeted.reply);
        assert_eq!(targeted.effect, CommandEffect::None);

        assert_admin_denied(root.path(), "/exit").await;
    }

    #[tokio::test]
    async fn invite_mutates_access_lists_under_the_config_lock() {
        let root = tempfile::tempdir().unwrap();
        let mut profile = admin_profile();
        profile.access = Some(Access {
            admins: vec!["ou_admin".into()],
            allowed_users: vec!["ou_user".into()],
            ..Access::default()
        });
        seed_config(root.path(), &profile);
        let bridge = test_bridge(root.path(), profile);

        // Existing user + a bot mention: the bot is ignored, the user is
        // reported as already present.
        let message = with_mentions(
            group("/invite user @User"),
            &[("ou_user", "User", false), ("ou_bot", "Bot", true)],
        );
        let already = dispatch(&bridge, &message, "/invite user @User").await;
        assert!(
            already.reply.contains("已经在用户白名单里，跳过"),
            "{}",
            already.reply
        );
        assert!(matches!(already.effect, CommandEffect::AccessLists { .. }));
        assert_eq!(
            persisted_access(root.path()).allowed_users,
            vec!["ou_user".to_string()]
        );

        // New user: added, config and live profile both updated.
        let message = with_mentions(group("/invite user @New"), &[("ou_new", "New", false)]);
        let added = dispatch(&bridge, &message, "/invite user @New").await;
        assert!(
            added.reply.contains("✅ 已把 New 加入用户白名单。"),
            "{}",
            added.reply
        );
        assert_eq!(
            persisted_access(root.path()).allowed_users,
            vec!["ou_user".to_string(), "ou_new".to_string()]
        );
        assert_eq!(
            bridge.live_profile().access.unwrap().allowed_users,
            vec!["ou_user".to_string(), "ou_new".to_string()]
        );

        // Admin target.
        let message = with_mentions(group("/invite admin @Boss"), &[("ou_boss", "Boss", false)]);
        let added = dispatch(&bridge, &message, "/invite admin @Boss").await;
        assert!(added.reply.contains("Boss 加入管理员"), "{}", added.reply);
        assert!(
            persisted_access(root.path())
                .admins
                .contains(&"ou_boss".to_string())
        );

        // A DM cannot add a group chat.
        let dm = with_mentions(p2p("/invite group"), &[]);
        let denied = dispatch(&bridge, &dm, "/invite group").await;
        assert_eq!(
            denied.reply,
            "❌ `/invite group` 只能在群里发，在私聊里没有 chat_id 可以加。"
        );

        // Group add, persisted, idempotent.
        let group_message = group("/invite group");
        let added = dispatch(&bridge, &group_message, "/invite group").await;
        assert!(
            added.reply.contains("已把当前群（`oc_1`）加入响应群名单"),
            "{}",
            added.reply
        );
        assert!(
            persisted_access(root.path())
                .allowed_chats
                .contains(&"oc_1".to_string())
        );
        let again = dispatch(&bridge, &group_message, "/invite group").await;
        assert!(
            again.reply.contains("当前群已在白名单里"),
            "{}",
            again.reply
        );

        // `all group` needs the chat-list API; the reply says so.
        let all = dispatch(&bridge, &group("/invite all group"), "/invite all group").await;
        assert!(
            all.reply.contains("没有移植「列出 bot 所在群」的接口"),
            "{}",
            all.reply
        );

        // Missing mentions / unknown kind.
        let none = dispatch(&bridge, &group("/invite user"), "/invite user").await;
        assert!(none.reply.contains("没检测到 @ 的用户"), "{}", none.reply);
        let usage = dispatch(&bridge, &group("/invite nope"), "/invite nope").await;
        assert_eq!(usage.reply, INVITE_USAGE);

        assert_admin_denied(root.path(), "/invite user @x").await;
    }

    #[tokio::test]
    async fn ps_lists_active_scopes_and_queued_batches() {
        let root = tempfile::tempdir().unwrap();
        let bridge = test_bridge(root.path(), admin_profile());

        // `spawn_tracked` registers a run but does not take a global slot;
        // hold one so `/ps` reports a realistic in-use slot.
        let _permit = bridge.run_limiter().try_acquire().expect("run slot");
        let active = bridge.spawn_tracked("oc_live", std::future::pending::<()>());
        bridge.queue_blocked_for_test(message("group", "oc_queued", "ou_admin", "queued message"));

        let outcome = dispatch(&bridge, &group("/ps"), "/ps").await;
        let reply = &outcome.reply;
        assert!(reply.contains("Bridge 运行状态"), "{reply}");
        assert!(reply.contains("运行槽：1/10 active"), "{reply}");
        assert!(reply.contains("活动运行"), "{reply}");
        assert!(reply.contains("`oc_live`"), "{reply}");
        assert!(reply.contains("排队中（尚未运行）"), "{reply}");
        assert!(reply.contains("`oc_queued`：1 条"), "{reply}");
        assert!(reply.contains("larkline ps"), "{reply}");
        assert_eq!(outcome.effect, CommandEffect::None);

        assert!(bridge.stop_scope("oc_live").0);
        let error = tokio::time::timeout(Duration::from_secs(2), active)
            .await
            .expect("aborted task finishes")
            .expect_err("abort yields JoinError");
        assert!(error.is_cancelled());

        assert_admin_denied(root.path(), "/ps").await;
    }

    #[tokio::test]
    async fn ps_reports_an_idle_bridge() {
        let root = tempfile::tempdir().unwrap();
        let bridge = test_bridge(root.path(), admin_profile());
        let outcome = dispatch(&bridge, &group("/ps"), "/ps").await;
        assert!(
            outcome.reply.contains("当前没有正在运行或排队的任务"),
            "{}",
            outcome.reply
        );
    }

    #[tokio::test]
    async fn reconnect_returns_guidance_instead_of_faking_a_restart() {
        let root = tempfile::tempdir().unwrap();
        let bridge = test_bridge(root.path(), admin_profile());

        let outcome = dispatch(&bridge, &group("/reconnect"), "/reconnect").await;
        assert!(
            outcome.reply.contains("无法在进程内重连"),
            "{}",
            outcome.reply
        );
        assert!(
            outcome.reply.contains("larkline restart"),
            "{}",
            outcome.reply
        );
        assert!(!outcome.reply.contains("已重连"), "{}", outcome.reply);
        assert_eq!(outcome.effect, CommandEffect::None);

        let wait = dispatch(&bridge, &group("/reconnect --wait"), "/reconnect --wait").await;
        assert!(wait.reply.contains("`--wait`"), "{}", wait.reply);
        assert!(parse_reconnect("--wait").wait);

        assert_admin_denied(root.path(), "/reconnect").await;
    }

    #[tokio::test]
    async fn remove_mutates_access_lists_under_the_config_lock() {
        let root = tempfile::tempdir().unwrap();
        let mut profile = admin_profile();
        profile.access = Some(Access {
            admins: vec!["ou_admin".into()],
            allowed_users: vec!["ou_old".into(), "ou_keep".into()],
            allowed_chats: vec!["oc_1".into(), "oc_2".into()],
            ..Access::default()
        });
        seed_config(root.path(), &profile);
        let bridge = test_bridge(root.path(), profile);

        let message = with_mentions(group("/remove user @Old"), &[("ou_old", "Old", false)]);
        let removed = dispatch(&bridge, &message, "/remove user @Old").await;
        assert!(
            removed.reply.contains("✅ 已把 Old 移出用户白名单。"),
            "{}",
            removed.reply
        );
        assert_eq!(
            persisted_access(root.path()).allowed_users,
            vec!["ou_keep".to_string()]
        );
        assert!(matches!(removed.effect, CommandEffect::AccessLists { .. }));

        let missing = with_mentions(group("/remove user @Old"), &[("ou_old", "Old", false)]);
        let not_there = dispatch(&bridge, &missing, "/remove user @Old").await;
        assert!(
            not_there.reply.contains("Old 本来就不在用户白名单里"),
            "{}",
            not_there.reply
        );

        // Group removal only works from a group.
        let dm = p2p("/remove group");
        let denied = dispatch(&bridge, &dm, "/remove group").await;
        assert_eq!(
            denied.reply,
            "`/remove group` 请在要移除的群里发，私聊里没有可移除的群。"
        );

        let group_message = group("/remove group");
        let removed = dispatch(&bridge, &group_message, "/remove group").await;
        assert_eq!(removed.reply, "✅ 已把当前群移出响应群名单。");
        assert_eq!(
            persisted_access(root.path()).allowed_chats,
            vec!["oc_2".to_string()]
        );
        let again = dispatch(&bridge, &group_message, "/remove group").await;
        assert!(
            again.reply.contains("本来就不在响应名单里"),
            "{}",
            again.reply
        );

        let none = dispatch(&bridge, &group("/remove user"), "/remove user").await;
        assert!(none.reply.contains("请 @ 上要移除的人"), "{}", none.reply);
        let usage = dispatch(&bridge, &group("/remove nope"), "/remove nope").await;
        assert_eq!(usage.reply, REMOVE_USAGE);

        assert_admin_denied(root.path(), "/remove user @x").await;
    }

    #[tokio::test]
    async fn resume_use_persists_the_session_and_interrupts_the_live_run() {
        let root = tempfile::tempdir().unwrap();
        let bridge = test_bridge(root.path(), admin_profile());
        let cwd = root.path().join("resume-workspace");
        std::fs::create_dir_all(&cwd).unwrap();
        bridge
            .workspace_store()
            .lock()
            .await
            .set("oc_1", &cwd)
            .unwrap();
        let live = bridge.spawn_tracked("oc_1", std::future::pending::<()>());

        let outcome = dispatch(
            &bridge,
            &group("/resume use sess-abc"),
            "/resume use sess-abc",
        )
        .await;
        assert_eq!(outcome.reply, RESUME_APPLIED_REPLY);
        assert_eq!(
            bridge
                .sessions
                .lock()
                .await
                .resume_for("oc_1", &cwd.to_string_lossy()),
            Some("sess-abc")
        );
        match outcome.effect {
            CommandEffect::SessionResumed {
                ref scope,
                ref session_id,
                cwd: ref effect_cwd,
            } => {
                assert_eq!(scope, "oc_1");
                assert_eq!(session_id, "sess-abc");
                assert_eq!(effect_cwd, &cwd.to_string_lossy());
            }
            other => panic!("unexpected effect: {other:?}"),
        }
        let error = tokio::time::timeout(Duration::from_secs(2), live)
            .await
            .expect("aborted live run finishes")
            .expect_err("abort yields JoinError");
        assert!(error.is_cancelled());
        assert!(!bridge.is_scope_active("oc_1"));

        // Direct `<session-id>` form documented by the Rust port.
        let direct = dispatch(
            &bridge,
            &group("/resume sess-direct"),
            "/resume sess-direct",
        )
        .await;
        assert_eq!(direct.reply, RESUME_APPLIED_REPLY);
        assert_eq!(
            bridge
                .sessions
                .lock()
                .await
                .resume_for("oc_1", &cwd.to_string_lossy()),
            Some("sess-direct")
        );

        // List mode is p2p-only and cannot read history in this build.
        let group_list = dispatch(&bridge, &group("/resume"), "/resume").await;
        assert!(
            group_list.reply.contains("群聊中不展示历史会话详情"),
            "{}",
            group_list.reply
        );
        let p2p_list = dispatch(&bridge, &p2p("/resume 3"), "/resume 3").await;
        assert!(p2p_list.reply.contains("当前 cwd"), "{}", p2p_list.reply);
        assert!(
            p2p_list.reply.contains("无法列出最近 3 个会话"),
            "{}",
            p2p_list.reply
        );
        assert!(p2p_list.reply.contains("sess-dir"), "{}", p2p_list.reply);

        // A missing cwd is rejected before a session is written.
        bridge
            .workspace_store()
            .lock()
            .await
            .set("oc_1", root.path().join("missing-dir"))
            .unwrap();
        let bad = dispatch(&bridge, &group("/resume use sess-x"), "/resume use sess-x").await;
        assert!(bad.reply.contains("工作目录不可用"), "{}", bad.reply);
        assert_eq!(bad.effect, CommandEffect::None);

        // Invalid ids never reach the session store.
        let invalid = dispatch(&bridge, &group("/resume use --evil"), "/resume use --evil").await;
        assert!(
            invalid.reply.contains("无效的 session id"),
            "{}",
            invalid.reply
        );
        assert_eq!(invalid.effect, CommandEffect::None);

        // No admin gate: an ordinary profile can resume.
        let ordinary = test_bridge(root.path(), plain_profile());
        ordinary
            .workspace_store()
            .lock()
            .await
            .set("oc_1", &cwd)
            .unwrap();
        let outcome = dispatch(
            &ordinary,
            &message("group", "oc_1", "ou_visitor", "/resume use sess-user"),
            "/resume use sess-user",
        )
        .await;
        assert_eq!(outcome.reply, RESUME_APPLIED_REPLY);
    }

    #[tokio::test]
    async fn cd_is_admin_gated_and_resets_the_scope_session() {
        let root = tempfile::tempdir().unwrap();
        let bridge = test_bridge(root.path(), admin_profile());
        let cwd = root.path().join("old-cwd");
        fs::create_dir_all(&cwd).unwrap();
        let target = root.path().join("new-cwd");
        fs::create_dir_all(&target).unwrap();
        let old_cwd = cwd.to_string_lossy().into_owned();
        bridge
            .sessions
            .lock()
            .await
            .set("oc_1", "sess-old", &old_cwd)
            .unwrap();

        // A live run is interrupted before the cwd/session swap.
        let live = bridge.spawn_tracked("oc_1", std::future::pending::<()>());
        let text = format!("/cd {}", target.display());
        let outcome = dispatch(&bridge, &group(&text), &text).await;

        let canonical = target.canonicalize().unwrap();
        assert!(
            outcome.reply.contains("session 已重置"),
            "{}",
            outcome.reply
        );
        assert!(
            outcome
                .reply
                .contains(&format!("`{}`", canonical.display())),
            "{}",
            outcome.reply
        );
        match outcome.effect {
            CommandEffect::WorkspaceCwd { ref scope, ref cwd } => {
                assert_eq!(scope, "oc_1");
                assert_eq!(cwd, &canonical.to_string_lossy());
            }
            other => panic!("unexpected effect: {other:?}"),
        }
        assert_eq!(
            bridge
                .workspace_store()
                .lock()
                .await
                .cwd_for("oc_1")
                .map(str::to_string),
            Some(canonical.to_string_lossy().into_owned())
        );
        assert_eq!(
            bridge
                .sessions
                .lock()
                .await
                .resume_for("oc_1", &canonical.to_string_lossy()),
            None,
            "the cwd swap must reset the session"
        );
        let error = tokio::time::timeout(Duration::from_secs(2), live)
            .await
            .expect("interrupted run finishes")
            .expect_err("interrupt aborts a tracked run");
        assert!(error.is_cancelled());
    }

    #[tokio::test]
    async fn cd_denies_non_admins_before_touching_state() {
        let root = tempfile::tempdir().unwrap();
        let target = root.path().join("admin-only");
        fs::create_dir_all(&target).unwrap();
        assert_admin_denied(root.path(), &format!("/cd {}", target.display())).await;
    }

    #[tokio::test]
    async fn new_interrupts_the_run_and_a_late_save_cannot_resurrect_the_session() {
        let root = tempfile::tempdir().unwrap();
        let bridge = test_bridge(root.path(), plain_profile());
        let cwd = root.path().join("new-cwd");
        fs::create_dir_all(&cwd).unwrap();
        let cwd = cwd.to_string_lossy().into_owned();
        bridge
            .sessions
            .lock()
            .await
            .set("oc_1", "sess-live", &cwd)
            .unwrap();
        let generation = bridge.scope_generation("oc_1");

        let live = bridge.spawn_tracked("oc_1", std::future::pending::<()>());
        let outcome = dispatch(&bridge, &group("/new"), "/new").await;
        assert_eq!(outcome.reply, "已中断当前任务并开始新会话。");
        assert!(matches!(
            outcome.effect,
            CommandEffect::SessionReset { ref scope } if scope == "oc_1"
        ));
        let error = tokio::time::timeout(Duration::from_secs(2), live)
            .await
            .expect("interrupted run finishes")
            .expect_err("interrupt aborts a tracked run");
        assert!(error.is_cancelled());
        assert_eq!(bridge.sessions.lock().await.resume_for("oc_1", &cwd), None);

        // A run that started before `/new` finishes later: its save is
        // rejected because the scope generation moved on.
        assert!(
            !bridge
                .save_scope_session_if_current("oc_1", "sess-late", &cwd, generation)
                .await
        );
        assert_eq!(
            bridge.sessions.lock().await.resume_for("oc_1", &cwd),
            None,
            "a finished run must not resurrect the cleared session"
        );

        // No live run: the plain TS reply.
        let idle = dispatch(&bridge, &group("/new"), "/new").await;
        assert_eq!(idle.reply, "已开始新会话。");
    }

    #[tokio::test]
    async fn resume_refuses_codex_threads_with_guidance() {
        let root = tempfile::tempdir().unwrap();
        let profile = ProfileConfig {
            agent_kind: AgentKind::Codex,
            access: Some(Access {
                admins: vec!["ou_admin".into()],
                ..Access::default()
            }),
            ..ProfileConfig::default()
        };
        let bridge = test_bridge(root.path(), profile);
        let cwd = root.path().join("workspace");
        std::fs::create_dir_all(&cwd).unwrap();
        bridge
            .workspace_store()
            .lock()
            .await
            .set("oc_1", &cwd)
            .unwrap();

        let outcome = dispatch(
            &bridge,
            &group("/resume use thread-1"),
            "/resume use thread-1",
        )
        .await;
        assert!(
            outcome.reply.contains("未实现 Codex thread"),
            "{}",
            outcome.reply
        );
        assert_eq!(outcome.effect, CommandEffect::None);
        assert_eq!(
            bridge
                .sessions
                .lock()
                .await
                .resume_for("oc_1", &cwd.to_string_lossy()),
            None
        );
    }

    #[tokio::test]
    async fn timeout_display_set_off_default_and_targeted_scope() {
        let root = tempfile::tempdir().unwrap();
        let mut profile = admin_profile();
        profile
            .preferences
            .get_or_insert_with(Preferences::default)
            .run_idle_timeout_minutes = Some(30);
        let bridge = test_bridge(root.path(), profile);

        let shown = dispatch(&bridge, &group("/timeout"), "/timeout").await;
        assert!(
            shown.reply.contains("探活:跟随全局(30 分钟)"),
            "{}",
            shown.reply
        );
        assert!(shown.reply.contains("用法:"), "{}", shown.reply);

        let set = dispatch(&bridge, &group("/timeout 15"), "/timeout 15").await;
        assert!(set.reply.contains("已设为 15 分钟"), "{}", set.reply);
        assert_eq!(
            bridge.sessions.lock().await.idle_timeout_minutes("oc_1"),
            Some(15)
        );
        assert_eq!(
            set.effect,
            CommandEffect::TimeoutOverride {
                scope: "oc_1".into(),
                minutes: Some(15)
            }
        );

        let shown = dispatch(&bridge, &group("/timeout"), "/timeout").await;
        assert!(shown.reply.contains("探活:15 分钟"), "{}", shown.reply);

        let off = dispatch(&bridge, &group("/timeout off"), "/timeout off").await;
        assert!(
            off.reply.contains("已关闭当前 session 的探活"),
            "{}",
            off.reply
        );
        assert_eq!(
            bridge.sessions.lock().await.idle_timeout_minutes("oc_1"),
            Some(0)
        );

        let shown = dispatch(&bridge, &group("/timeout"), "/timeout").await;
        assert!(
            shown.reply.contains("已关闭（当前 session）"),
            "{}",
            shown.reply
        );

        let cleared = dispatch(&bridge, &group("/timeout default"), "/timeout default").await;
        assert!(
            cleared
                .reply
                .contains("已清除 session 覆盖,回退到全局(30 分钟)"),
            "{}",
            cleared.reply
        );
        assert_eq!(
            bridge.sessions.lock().await.idle_timeout_minutes("oc_1"),
            None
        );
        let nothing = dispatch(&bridge, &group("/timeout default"), "/timeout default").await;
        assert!(
            nothing.reply.contains("本来就没设过覆盖"),
            "{}",
            nothing.reply
        );

        let invalid = dispatch(&bridge, &group("/timeout 999"), "/timeout 999").await;
        assert_eq!(
            invalid.reply,
            "❌ 用法:`/timeout <1-120>` / `/timeout off` / `/timeout default`"
        );
        assert_eq!(
            bridge.sessions.lock().await.idle_timeout_minutes("oc_1"),
            None
        );

        // Explicit comment scopes are admin-only.
        let comment = dispatch(
            &bridge,
            &group("/timeout comment:abc 2"),
            "/timeout comment:abc 2",
        )
        .await;
        assert!(comment.reply.contains("已设为 2 分钟"), "{}", comment.reply);
        assert_eq!(
            bridge
                .sessions
                .lock()
                .await
                .idle_timeout_minutes("comment:abc"),
            Some(2)
        );

        let outsider_root = tempfile::tempdir().unwrap();
        let outsider = test_bridge(outsider_root.path(), plain_profile());
        let denied = dispatch(
            &outsider,
            &message("group", "oc_1", "ou_visitor", "/timeout comment:abc 2"),
            "/timeout comment:abc 2",
        )
        .await;
        assert_eq!(denied.reply, "❌ 指定 scope 设置 timeout 仅管理员可用。");
        assert_eq!(
            outsider
                .sessions
                .lock()
                .await
                .idle_timeout_minutes("comment:abc"),
            None
        );
    }

    #[tokio::test]
    async fn ws_manages_named_aliases_for_the_scope_and_work_is_an_alias() {
        let root = tempfile::tempdir().unwrap();
        let bridge = test_bridge(root.path(), admin_profile());
        let cwd = root.path().join("workspace");
        std::fs::create_dir_all(&cwd).unwrap();
        bridge
            .workspace_store()
            .lock()
            .await
            .set("oc_1", &cwd)
            .unwrap();

        let saved = dispatch(&bridge, &group("/ws save main"), "/ws save main").await;
        assert!(
            saved.reply.contains("工作目录别名已保存：`main`"),
            "{}",
            saved.reply
        );
        match saved.effect {
            CommandEffect::WorkspaceAliasSaved {
                ref name,
                cwd: ref saved_cwd,
            } => {
                assert_eq!(name, "main");
                assert_eq!(saved_cwd, &cwd.to_string_lossy());
            }
            other => panic!("unexpected effect: {other:?}"),
        }
        let keys = bridge.workspace_store().lock().await.list_named();
        let key = keys.keys().next().expect("scoped alias");
        assert!(key.contains(WORKSPACE_NAME_SEPARATOR), "{key}");
        assert!(key.contains("test"), "{key}");
        assert!(key.contains("oc_1"), "{key}");

        let listed = dispatch(&bridge, &group("/ws list"), "/ws list").await;
        assert!(listed.reply.contains("当前 cwd："), "{}", listed.reply);
        assert!(listed.reply.contains("**main**"), "{}", listed.reply);
        assert!(listed.reply.contains("← 当前"), "{}", listed.reply);
        assert_eq!(listed.effect, CommandEffect::None);

        // `/ws use` validates and canonicalizes the target, resets state.
        bridge
            .workspace_store()
            .lock()
            .await
            .set("oc_1", root.path().join("elsewhere"))
            .unwrap();
        bridge
            .sessions
            .lock()
            .await
            .set("oc_1", "old-session", &cwd.to_string_lossy())
            .unwrap();
        let used = dispatch(&bridge, &group("/ws use main"), "/ws use main").await;
        assert!(used.reply.contains("已切换到 `main`"), "{}", used.reply);
        assert_eq!(
            bridge.workspace_store().lock().await.cwd_for("oc_1"),
            Some(cwd.to_string_lossy().as_ref())
        );
        assert_eq!(
            bridge
                .sessions
                .lock()
                .await
                .resume_for("oc_1", &cwd.to_string_lossy()),
            None
        );

        // Aliases saved under another owner segment (the TypeScript bridge
        // always knows the real bot owner) stay resolvable.
        let foreign_key = scoped_workspace_key("test", Some("ou_owner"), "oc_1", "shared");
        bridge
            .workspace_store()
            .lock()
            .await
            .save_named(&foreign_key, &cwd)
            .unwrap();
        let shared_use = dispatch(&bridge, &group("/ws use shared"), "/ws use shared").await;
        assert!(
            shared_use.reply.contains("已切换到 `shared`"),
            "{}",
            shared_use.reply
        );
        let shared_list = dispatch(&bridge, &group("/ws"), "/ws").await;
        assert!(
            shared_list.reply.contains("**shared**"),
            "{}",
            shared_list.reply
        );

        // Legacy (unscoped) aliases remain resolvable.
        bridge
            .workspace_store()
            .lock()
            .await
            .save_named("legacy", &cwd)
            .unwrap();
        let legacy_use = dispatch(&bridge, &group("/ws use legacy"), "/ws use legacy").await;
        assert!(
            legacy_use.reply.contains("已切换到 `legacy`"),
            "{}",
            legacy_use.reply
        );
        let legacy_list = dispatch(&bridge, &group("/ws"), "/ws").await;
        assert!(
            legacy_list.reply.contains("**legacy**"),
            "{}",
            legacy_list.reply
        );

        // `/work` is the same command with its own usage spelling.
        let work_list = dispatch(&bridge, &group("/work list"), "/work list").await;
        assert!(
            work_list.reply.contains("📂 工作目录"),
            "{}",
            work_list.reply
        );
        let work_usage = dispatch(&bridge, &group("/work nope"), "/work nope").await;
        assert!(
            work_usage.reply.contains("`/work ["),
            "{}",
            work_usage.reply
        );
        let work_save = dispatch(&bridge, &group("/work save second"), "/work save second").await;
        assert!(work_save.reply.contains("`second`"), "{}", work_save.reply);

        let removed = dispatch(&bridge, &group("/ws remove main"), "/ws remove main").await;
        assert!(
            removed.reply.contains("已删除工作目录别名"),
            "{}",
            removed.reply
        );
        assert!(
            !bridge
                .workspace_store()
                .lock()
                .await
                .list_named()
                .contains_key(key)
        );
        let missing = dispatch(&bridge, &group("/ws use missing"), "/ws use missing").await;
        assert!(
            missing.reply.contains("未找到工作目录别名"),
            "{}",
            missing.reply
        );
        let usage = dispatch(&bridge, &group("/ws save"), "/ws save").await;
        assert_eq!(usage.reply, "用法：`/ws save <name>`");

        assert_admin_denied(root.path(), "/ws").await;
    }
}
