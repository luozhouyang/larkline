# larkline

larkline is a single, self-contained static binary that bridges Feishu / Lark messenger with your local Claude Code or Codex CLI. **No Node.js. No npm. One binary, at install time and at runtime.** Start the bridge, scan a QR code to bind a PersonalAgent app, and talk to your local coding agent from chat.

larkline is a fork of [zarazhangrui/lark-coding-agent-bridge](https://github.com/zarazhangrui/lark-coding-agent-bridge), renamed from `lark-channel-bridge`. The Rust implementation keeps the same `~/.larkline` state directory, config schema and encrypted keystore as the TypeScript bridge, so an existing install can switch over without re-onboarding. See [Rust port notes](./docs/RUST-PORT.md).

[中文 README](./README.zh.md) · [Rust port notes](./docs/RUST-PORT.md) · [Product walkthrough](https://larkcommunity.feishu.cn/docx/OaRIdFIRFoLM3xxTmKwcetHqn5e)

## What it does

- Forwards Feishu / Lark messages to a local Claude Code or Codex CLI. DM the bot directly, or `@`-mention it in a group.
- **Streaming card**: the answer and a tool summary update on one Lark card while the agent works. Running cards carry a stop button. After about 8.5 minutes the run rolls over to a fresh card instead of going silent.
- **Session continuity**: each chat or topic thread resumes its own agent session. A session is only resumed from the working directory it was created in.
- **Queueing and batching**: messages sent in quick succession run as one turn; messages that arrive during a run wait for the next one. The queue is written through to disk and restored after a restart; the chat gets a short restored-count notice. `/stop` aborts the current run and drops its queued batch.
- **Global concurrency cap**: at most `preferences.maxConcurrentRuns` agent runs (default 10) are live at once; excess messages stay queued.
- **Per-chat/topic working directory**: use `/cd` to switch the directory for the current chat or topic, and `/pwd` to inspect it.
- **Cloud-doc comments**: `@`-mention the bot in a supported document comment (`doc`, `docx`, `sheet` or `file`) and it replies in the same thread. Comment runs are abortable by an admin with `/stop comment:<hash>`. The app must subscribe to `drive.notice.comment_add_v1`.
- **In-meeting agent (opt-in)**: join a Feishu meeting, answer in-meeting or over IM, and produce notes from the live transcript. Meeting runs are abortable by an admin with `/stop meeting:<id>`.
- **Approval cards**: when the agent works with a non-skip permission mode, tool prompts become a card with Allow/Deny buttons. Only the person who started the run can answer; unanswered prompts are denied after ten minutes. See [Permissions and approvals](#permissions-and-approvals).
- **Long-running tasks**: Feishu stops accepting updates for a streaming card after about 10 minutes. larkline rolls the progress over to a new card at 8.5 minutes, so a long run keeps showing progress and the final answer lands on the latest card.
- **OS-managed service**: run in the foreground for debugging, then install a per-profile launchd, systemd or Task Scheduler service.
- **Multiple profiles**: keep separate PersonalAgent apps, agents, sessions and workspaces on one machine.
- **Encrypted App Secret**: the App Secret is stored in a local encrypted keystore, never in plaintext in `config.json`.

## Requirements

- macOS, Linux or Windows. larkline itself needs no Node.js, npm or any language runtime.
- One local coding agent installed and logged in:
  - Claude Code: `claude`, see https://docs.anthropic.com/en/docs/claude-code/quickstart
  - Codex CLI: `codex`, see https://developers.openai.com/codex/cli
- A Feishu / Lark **PersonalAgent** app. `larkline onboard` can create and bind one through the QR flow.
- Network access to Feishu / Lark from the machine running the bridge.

The agent CLI is a separate tool and keeps its own requirements (including Node.js, if that is how you installed it). larkline only spawns the local binary.

## Install

Download the installer first, then run it:

```bash
curl -fsSL https://raw.githubusercontent.com/luozhouyang/larkline/main/install.sh -o /tmp/larkline-install.sh
sh /tmp/larkline-install.sh
```

The two steps are deliberate: the script is saved to a file so you can inspect it before it runs. It is never piped straight from the network into a shell.

The default install directory is `~/.local/bin`, with no `sudo`. If that directory is not on `PATH`, the installer prints the exact `export` line.

### Pin a version

```bash
sh /tmp/larkline-install.sh --version <version>
```

A leading `v` is accepted too. Use this when you want reproducible installs or need to roll back.

### Choose the install directory

```bash
LARKLINE_INSTALL_DIR="$HOME/bin" sh /tmp/larkline-install.sh
# or
sh /tmp/larkline-install.sh --dir "$HOME/bin"
```

The installer writes the new binary beside the destination and renames it into place. A running daemon keeps its old inode and finishes its work; the next start picks up the new binary.

### Checksums and platform assets

The installer downloads the matching asset and verifies it against the release's `SHA256SUMS` when that file is present. If a release has no checksums, it prints a warning instead of pretending to verify.

| Platform | Release asset | Notes |
|---|---|---|
| macOS (Apple Silicon + Intel) | `larkline-darwin-universal.zip` | Universal binary; the release job signs and notarizes it when the Apple secrets are configured (currently pending) |
| Linux x86_64 | `larkline-linux-x86_64.tar.gz` | Statically linked (musl), no glibc dependency |
| Linux aarch64 | `larkline-linux-aarch64.tar.gz` | Statically linked (musl), built on Ubuntu 24.04 ARM |
| Windows x86_64 | `larkline-windows-x86_64.zip` | Unsigned; SmartScreen may warn on first run |

On Windows, extract `larkline.exe` from the release zip and put it on `PATH`. The installer is a POSIX `sh` script, so the zip is the simplest option there.

### Upgrading

```bash
larkline upgrade --check   # report the latest release
larkline upgrade           # print the exact installer command for it
```

Both forms query the GitHub releases endpoint, so they need network access to GitHub. `--target-version <v>` still performs that lookup before printing the pinned installer command; it is not an offline mode.

`larkline upgrade` never replaces the binary underneath a running daemon. It reports the available version and the installer command; you decide when to run it and restart the service. On Windows, replace `larkline.exe` from the release zip instead.

## Quick start

### 1. Create a profile

The easiest path creates a PersonalAgent app through the QR device flow:

```bash
larkline onboard
```

A QR code renders in the terminal. Scan it with Feishu / Lark, confirm in the app, and the profile is written to `~/.larkline/config.json`. The default profile name is `bot` (then `bot-2`, `bot-3`, …); use `--name` to choose one, `--agent codex` for Codex, and `--tenant lark` for a Lark global app.

If you already have a PersonalAgent app, create the profile directly:

```bash
larkline profile create claude --app-id cli_xxx
```

The command prompts for the App Secret without echoing it. Pass `--agent codex` for Codex, `--tenant lark` for a Lark global app, and `--app-secret` when a script needs to supply the secret non-interactively. `--no-use` creates the profile without making it active.

You do not need to choose a project directory up front. The bridge creates a profile-managed default workspace; use `/cd <absolute path>` in Feishu / Lark to switch to a real project later.

### 2. Check the setup

```bash
larkline doctor
```

`doctor` checks the state directory, config, profiles, active profile, app id, keystore decryption, the agent binary, and (as a warning) `lark-cli`. It exits non-zero when a required check fails.

### 3. Run in the foreground

```bash
larkline run
```

`run` prints the profile, app, tenant, agent and workspace, then connects to the Feishu / Lark long connection. Send a direct message or `@`-mention the bot in a group. Press `Ctrl-C` to stop.

Use `larkline run --dry-run` to validate config, credentials and the agent binary without connecting. `--cwd` selects a working directory for this run when the profile does not define `workspaces.default`.

### 4. Run as a service

Stop the foreground process first, then install and start the per-profile daemon:

```bash
larkline start
larkline status
```

`start` installs or updates the OS service definition if needed, stops a live instance, and starts the new one. The service records the binary path, the profile and the invoking shell's `PATH`, so `claude` and `codex` remain discoverable after logout. Do not run `larkline run` and the daemon against the same app at the same time.

Service commands are per profile:

```bash
larkline start [--profile <name>]
larkline stop [--profile <name>]        # stop and disable autostart
larkline restart [--profile <name>]
larkline status [--profile <name>]
larkline unregister [--profile <name>]  # remove the OS registration
```

Platform mapping:

- **macOS**: launchd user agent `ai.larkline.bot.<profile>`
- **Linux**: systemd user unit `larkline.bot.<profile>.service`
- **Windows**: Task Scheduler task `Larkline.Bot.<profile>`, launched through a generated `.cmd` wrapper

Daemon stdout/stderr logs are under `~/.larkline/profiles/<profile>/logs/daemon/`.

## Command reference

| Command | What it does |
|---|---|
| `larkline run [--cwd <path>] [--dry-run]` | Run the bridge in the foreground. `--dry-run` validates and prints what would run without connecting. |
| `larkline doctor` | Check config, credentials, keystore, profiles and agent binaries. Non-zero exit on a failed required check. |
| `larkline onboard [--name <name>] [--agent claude\|codex] [--tenant feishu\|lark] [--no-use]` | Create a profile by scanning a QR code (app registration device flow). |
| `larkline profile list\|create\|use\|remove\|show` | Manage profiles. `create` needs `--app-id`; it also accepts `--app-secret`, `--agent`, `--tenant` and `--no-use`. `remove` takes `--yes` and `--purge-secret`. `show` prints one profile as JSON. |
| `larkline secrets get\|set\|list\|remove` | Manage the encrypted keystore. `get` speaks the exec-provider JSON protocol on stdin/stdout; `list` prints ids, never values. |
| `larkline upgrade [--check] [--target-version <v>]` | Report the latest release and print the installer command. It never replaces the binary automatically; it needs network access. |
| `larkline start` | Install (if needed) and start the OS-managed daemon. |
| `larkline stop` | Stop the daemon and disable autostart. |
| `larkline restart` | Restart an installed daemon. |
| `larkline status` | Show the service definition, whether it is installed and running, pid and last exit. |
| `larkline unregister` | Remove the OS service registration. Local state is kept. |
| `larkline ps` | List bridge processes recorded in the shared process registry, marking stale entries. |
| `larkline kill <id\|#>` | Stop a registered bridge process by short id or 1-based list index. |

Global options: `--home <path>` (defaults to `$LARKLINE_HOME` or `~/.larkline`), `--profile <name>` (defaults to `$LARKLINE_PROFILE` or the active profile), and `-v`/`-vv` for more logging.

`ps` and `kill` read `~/.larkline/registry/processes.json`. `larkline run` registers its process at startup and removes it on normal exit and on ctrl-c/SIGTERM, so Rust and TypeScript processes share one list. Rust entries carry no `botName` (`lark-channel` 0.6 has no bot-identity accessor); `larkline ps` shows the profile, agent, pid and start time.

## Commands inside Feishu / Lark

| Command | Effect |
|---|---|
| `/new`, `/reset` | Clear the current session for this chat/topic. |
| `/cd <absolute path>` | Switch this chat/topic's working directory. The path must exist and be absolute; it is canonicalized. |
| `/pwd` | Show the effective working directory. |
| `/status` | Show profile, agent, app id, tenant, cwd, reply mode, whether a session is resumable and the live run scopes. |
| `/config` | Show the current reply preferences and concurrency cap. |
| `/config set messageReply <card\|markdown\|text>` | Set the reply mode (admin only). |
| `/config set showToolCalls <true\|false>` | Show or hide tool summaries (admin only). |
| `/config set maxConcurrentRuns <1-50>` | Set the global agent concurrency cap (admin only). |
| `/meeting` | Show the in-meeting agent status. Requires `meeting.enabled`. |
| `/meeting join <9-digit number>` | Join a meeting by its 9-digit meeting number. |
| `/meeting leave [meeting number]` | Leave the selected meeting. |
| `/meeting notes [meeting number]` | Summarize the transcript for the selected meeting. |
| `/meeting ask <question>` | Ask a question with the meeting transcript as context. |
| `/meeting transcript [meeting number]` | Show the transcript context the agent actually receives. |
| `/meeting stop [meeting number]` | Interrupt the meeting's current agent run. |
| `/stop` | Abort the run in this chat/topic, including the card's stop button. |
| `/stop <scope>` | Abort a specific live run by scope, such as `comment:<hash>` or `meeting:<id>` (admin only). |
| `/help` | Show the list of available commands. |

Notes:

- DMs do not need an `@` mention. Groups require one by default; set `access.requireMentionInGroup` to `false` to relax that.
- `/config set`, `/meeting` and `/stop <scope>` are admin commands. They accept a sender in `access.admins`; while the admin list is empty, a sender in `access.allowedUsers` is accepted instead. The runtime app owner is also accepted. If the owner lookup fails, non-admins are denied.
- `/config` reads are open to any sender the message policy already admitted.
- Commands bypass the debounce queue and drop anything already waiting for that scope (chat or topic).
- `/stop <scope>` aborts a run that has no chat scope of its own. `/status` lists the live scopes.
- `maxConcurrentRuns` caps concurrent agent runs across IM, comment and meeting runs. When the cap is reached, IM batches stay queued; comment and meeting runs wait briefly for a slot and then reply with a timeout notice.
- `/meeting` additionally requires the profile's `meeting.enabled` to be `true`, and the app must have the VC bot scopes.
- Commands are replied to as text, not as interactive cards.

## Reply modes

`/config set messageReply` controls how the final answer is delivered:

- **`card`** — a CardKit streaming card is created and updated while the agent runs. The card shows the answer and a compact tool summary, plus a stop button while the run is live. A run longer than about 8.5 minutes continues on a fresh card.
- **`markdown`** — the default. The current Rust build also uses the card renderer for this mode, and falls back to a plain message when the card can no longer be updated.
- **`text`** — no card. The bridge sends one plain-text message when the run finishes.

`showToolCalls` controls whether the tool summary appears in the final card or text reply. It does not change what the agent does.

Feishu force-finalizes a streaming card about 10 minutes after creation and drops later updates. larkline keeps updating until 8.5 minutes, then starts a new card, marks the old one `⏭ 过程已接续到下一张卡片`, and continues with `↪ 接续上一张卡片` on the new card. The final answer is written on the latest card. If the new card cannot be created, the bridge falls back to stopping updates and sending the final answer as a separate message. This is a server-side limit, so it applies to desktop and mobile clients alike.

### Concurrency

`preferences.maxConcurrentRuns` (default 10, clamped to 1–50) caps how many agent runs are live at once across IM, comment and meeting scopes. `/config set maxConcurrentRuns <1-50>` writes it to the active profile and applies it immediately. Lowering the cap below the number of live runs lets those runs finish and holds new starts until a slot frees up.

## Configuration

### Profiles

A profile is one bot identity: its Feishu / Lark app credentials, agent kind, preferences, access lists, sessions and working directory. `config.json` uses schema v2 with an `activeProfile` and a `profiles` map. `--profile` (or `LARKLINE_PROFILE`) selects a profile for one command; otherwise the active profile is used.

A profile fragment looks like this. Do not replace the whole `config.json` with it; edit the matching profile.

```json
{
  "agentKind": "claude",
  "accounts": {
    "app": {
      "id": "cli_xxx",
      "tenant": "feishu",
      "secret": {
        "source": "exec",
        "provider": "bridge",
        "id": "app-cli_xxx"
      }
    }
  },
  "preferences": {
    "messageReply": "card",
    "showToolCalls": true,
    "maxConcurrentRuns": 10
  },
  "access": {
    "allowedUsers": ["ou_xxx"],
    "requireMentionInGroup": true,
    "admins": ["ou_xxx"]
  },
  "workspaces": {
    "default": "/Users/me/projects/app"
  }
}
```

The Rust binary preserves config fields it does not read yet, so a round-trip never drops settings written by the TypeScript bridge.

### App Secret and the keystore

`profile create` and `onboard` encrypt the App Secret into the profile's `secrets.enc` using AES-256-GCM. The config stores only a reference (`source: "exec"`, `provider: "bridge"`, `id: "app-<app_id>"`); the plaintext never appears in `config.json`.

```bash
larkline secrets list                       # ids only
larkline secrets set app-cli_xxx            # prompts without echo
larkline secrets remove app-cli_xxx
```

The encryption key is derived from the machine's hostname and your user name, plus a per-profile salt in `.keystore.salt`. Copying only `secrets.enc` to a different machine or user account will generally not decrypt; re-enter the App Secret there.

### Environment variables

| Variable | Purpose |
|---|---|
| `LARKLINE_HOME` | State root. Defaults to `~/.larkline`; `--home` overrides it. |
| `LARKLINE_PROFILE` | Profile for this process. `--profile` overrides it. |
| `LARKLINE_CALLBACK_SECRET` | Optional override for the HMAC key used to verify card buttons rendered by the agent. When absent, the profile App Secret is used as key version 1 (same as TypeScript). |

The pre-rename fallbacks `LARK_CHANNEL_HOME`, `LARK_CHANNEL_PROFILE` and `LARK_CHANNEL_CALLBACK_SECRET` are still read when the new names are absent.

For callbacks, the key defaults to the profile App Secret, so tokens minted by a bridge-aware `lark-cli` verify with no extra configuration. An explicit `callback.secret` (or `callback.keys` for key rotation) or `LARKLINE_CALLBACK_SECRET` still takes precedence. The bridge and the card-signing client must share the same value.

### Working directories

The profile may define a default working directory at `workspaces.default`. If it does not, the bridge uses the profile-managed default workspace under `<state-root>-workspaces/<profile>/default` (`~/.larkline-workspaces/<profile>/default` with the default home) and creates it on demand.

The working directory is the current directory for an agent run, not a filesystem sandbox. Actual file access still depends on the agent process and its permissions.

### Permissions and approvals

The profile's `permissions` block selects the agent's access and whether tool prompts become cards:

```json
{
  "permissions": {
    "defaultAccess": "workspace",
    "maxAccess": "workspace",
    "claude": { "permissionMode": "default" }
  }
}
```

- **Claude Code** with `default` or `acceptEdits` uses its stdio permission-prompt protocol. A tool prompt arrives as a separate card showing the agent and working directory, the command/path, and a collapsible parameters panel. `允许本次` echoes the original input; `本次会话都允许` sends the CLI's session suggestion when one is available; `拒绝` tells the model the user refused. `plan` and `bypassPermissions` never ask and keep the one-shot path. Parameters are shown in full up to a 12KB card budget; oversized inputs are cut with a visible notice.
- **Codex** with `read-only` or `workspace-write` runs through `codex app-server` (the plain `codex exec` protocol has no way to ask for approval). Command, file-change and additional-permission requests use the same card. `danger-full-access` keeps `codex exec` and never prompts.
- **User questions** (`AskUserQuestion` and Codex `request_user_input`) use the same card channel: options become buttons, multiple questions are answered one by one, multi-select questions get a submit button, and free-text questions render a Feishu form with input boxes. Answers go back to the agent in its native shape.
- Only the sender of the run's first message can answer; a click from anyone else is rejected. Ten minutes without a decision denies the tool/question automatically, and a card that cannot be sent is denied immediately. The approval/question card is a normal (non-streaming) message, so it is not affected by the 8.5-minute progress-card limit.
- The card is posted in the chat, so other chat members can see the command and tool parameters (only the initiator can click). Do not use a group chat for approvals if the tool input is sensitive.
- Comment and meeting runs do not have an approval-card flow; they keep the agent's non-interactive behavior. Run those scopes with full access if the agent needs to execute tools there.
- This needs a recent Claude Code with the stdio permission-prompt protocol, or a recent Codex with `codex app-server`. `larkline doctor` does not probe the approval protocol; if the installed CLI is too old, the run reports the CLI's error instead of silently auto-approving.

### Access control

The Rust bridge implements a subset of the TypeScript access model:

- `access.allowedUsers` restricts who can trigger the bot in DMs, groups and comment threads. When the list is empty, no user allowlist is applied.
- Group messages require an `@` mention by default. `access.requireMentionInGroup` can turn that off for a profile.
- `access.admins` gates `/config set`, `/meeting` and `/stop <scope>`; while `admins` is empty, `access.allowedUsers` acts as the admin set. The runtime app owner is also accepted, resolved from the Applications API and cached for 30 minutes.
- `access.allowedChats` is not applied to ordinary messages yet. It is only consulted when verifying card-button clicks.

`/invite` and `/remove` maintain the user, admin and group lists. The TypeScript owner bypass for ordinary messages, team-mode usage for ordinary messages and per-chat `chatRequireMention` overrides are not applied yet; `allowedChats` is consulted only for card-click verification. In particular, an empty `allowedUsers` list means anyone who can reach the app can DM it. Set `allowedUsers` explicitly before sharing the bot.

### Meetings

The in-meeting agent is opt-in. Add the meeting block to the profile and restart:

```json
{
  "meeting": {
    "enabled": true
  }
}
```

The app needs the VC bot scopes, and the tenant must support the VC bot capability. Once enabled, `/meeting join <9-digit number>` joins a meeting; in-meeting chat prefixed with the configured trigger can ask questions, and `/meeting notes` summarizes the transcript. See [Rust port notes](./docs/RUST-PORT.md) for the exact limitations.

## Data directory and compatibility

| Path | Content |
|---|---|
| `~/.larkline/config.json` | Root config: `activeProfile` and `profiles` |
| `~/.larkline/profiles/<profile>/sessions.json` | Resumable session per chat/topic scope |
| `~/.larkline/profiles/<profile>/workspaces.json` | Per-scope working directory overrides |
| `~/.larkline/profiles/<profile>/pending.json` | Durable per-scope message queue (24-hour per-message TTL, mode 0600) |
| `~/.larkline/profiles/<profile>/secrets.enc` | Encrypted App Secret and other secrets |
| `~/.larkline/profiles/<profile>/.keystore.salt` | Per-profile keystore salt |
| `~/.larkline/profiles/<profile>/logs/daemon/` | Daemon stdout/stderr logs |
| `~/.larkline/registry/processes.json` | Process registry read by `ps` / `kill` |
| `~/.larkline/secrets-getter` | Exec-provider wrapper used by lark-cli |

### Same config, same keystore

- The state layout, config schema v2 and encrypted keystore are shared with the TypeScript bridge.
- The keystore uses the same AES-256-GCM / PBKDF2-HMAC-SHA256 construction, so the Rust binary can decrypt an App Secret written by the TypeScript bridge (and the other way round).
- `~/.lark-channel` is renamed to `~/.larkline` automatically on first launch with the default home when `~/.larkline` does not exist. `~/.lark-channel-workspaces` is moved the same way when possible.
- Either implementation can read the same config. Stop one before starting the other; do not run both against the same app and profile at once.

### Migrating from the TypeScript bridge

1. Stop and unregister the old service with the old CLI: `larkline stop` and `larkline unregister`.
2. Install the Rust binary with `install.sh` and make sure `command -v larkline` resolves to it.
3. Run `larkline doctor`. The same profile, app id, keystore and sessions are found.
4. Start the service with `larkline start`.

If both `~/.lark-channel` and `~/.larkline` exist, nothing is renamed; the existing `~/.larkline` wins. No re-onboarding and no App Secret re-entry are needed.

### Remaining gaps

The bridge has an injectable outbound transport (`BridgeTransport`) plus
end-to-end run-flow tests (`crates/larkline-bridge/tests/run_flow.rs`) covering
the happy path, `/stop`, the idle timeout, markdown mode, attachment failures,
8.5-minute card rollover, and Claude/Codex approval click-throughs. Only these
items remain:

- **Apple release secrets**: the macOS release job needs Developer ID signing
  and notarization credentials that are still pending; Windows Authenticode
  signing is not configured either.
- **Wiki comments**: `wiki` comment targets are logged and skipped; `doc`,
  `docx`, `sheet` and `file` are supported.
- **Meeting summary threading**: end-of-meeting summaries go to the configured
  origin chat or owner DM as top-level messages, not into the originating topic.
- **Session catalog**: `sessions.json.catalog.json` is not written or read;
  resume is keyed by scope and working directory.
- **Approval scope**: approval and question cards are wired into the IM run
  loop only; comment and meeting runs keep the old non-interactive behavior.
  Tool input is not editable, mixed option/free-text questions fall back to a
  notice, and in a group only the run's first sender can approve.
- **Non-TS extras and remaining parity edges**: access-control edges (the
  TypeScript owner bypass for ordinary messages, team-mode usage for ordinary
  messages, per-chat `chatRequireMention` overrides and `allowedChats` on
  ordinary messages), Windows Authenticode signing, bot-name enrichment for
  Rust-owned `ps` entries, and the live keepalive watchdog (`lark-channel` 0.6
  exposes no connection-status or force-reconnect hook).

See [Rust port notes](./docs/RUST-PORT.md) for the current status and the reasons behind each gap.

## FAQ

**The bot is silent, or the local agent never replies.** Run `larkline doctor` first. Then check that the agent CLI is installed and logged in, and send `/status` to see the current cwd and whether a session is resumable. `/new` often fixes a stale session.

**The card stops updating and no answer arrives.** If the run passed about 8.5 minutes, look for a newer card in the same chat: the bridge rolls the progress over to a fresh card and puts the final answer there. Only if that new card could not be created does it fall back to sending the final answer as a separate message.

**Can I use Claude Code for one bot and Codex for another?** Yes. Create two profiles with different `--agent` values and run one service per profile. The agent kind is set at profile creation; create a new profile if you need to switch it.

**How do I change the App Secret?** Run `larkline secrets set app-<app_id>`, then restart the bridge or service. The config reference stays the same.

**Windows shows a SmartScreen warning.** The Windows binary is not code-signed yet. macOS release artifacts are Developer ID-signed and notarized when the Apple secrets are configured (currently pending); the Linux builds are unsigned but checksummed.

**`larkline ps` shows stale entries.** A process that was killed with SIGKILL (or crashed) cannot unregister itself. The next `larkline run` prunes dead pids; until then `ps` marks those entries `(stale)` and `kill` refuses to signal them.

**How do I uninstall?** Run `larkline stop`, then `larkline unregister`, remove the `larkline` binary, and delete `~/.larkline` if you also want to remove local state.

## Development and release

Build and test from the repository root:

```bash
cargo build --release
cargo fmt --all -- --check
cargo clippy --workspace --all-targets
cargo test --workspace
./target/release/larkline doctor
```

Rust 1.85 or newer is required. CI runs the test suite on macOS, Ubuntu and Windows; formatting and clippy run on Linux. Releases are tag-driven: pushing a `v*` tag runs the release workflow, which formats and tests the workspace, builds statically linked (musl) Linux x86_64/aarch64, a macOS universal binary (Developer ID signed/notarized; the job fails without the Apple secrets, which are still pending), and Windows x86_64, then publishes the assets with `SHA256SUMS` and a GitHub Release.

## License

[MIT](./LICENSE)

<img src="./assets/feedback-group-qr.png" alt="Feedback group QR code" width="360">
