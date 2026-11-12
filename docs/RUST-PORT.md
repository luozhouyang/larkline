# Rust port

The goal is a single, self-contained `larkline` binary with **no Node runtime
and no npm at install time or at runtime**. The TypeScript sources stay in the
tree until the port reaches parity; both implementations read and write the
same state directory, so an existing install can switch over without
re-onboarding.

This file tracks the port itself. The user-facing install and command docs live
in [`README.md`](../README.md) and [`README.zh.md`](../README.zh.md).

## Layout

| Crate | Responsibility |
|---|---|
| `larkline-core` | paths, config schema v2, keystore, atomic writes, file locks, session/workspace stores |
| `larkline-lark` | `lark-channel` SDK wrapper, CardKit cards and streaming, REST client, comments, meeting APIs, QR registration |
| `larkline-agent` | Claude Code / Codex argv builders, stream translators, bridge prompt, executable discovery |
| `larkline-bridge` | run flow, policy, queueing, sessions, workspaces, comments, meetings, callbacks |
| `larkline-cli` | `larkline` binary: clap commands, doctor, onboarding, daemon lifecycle, process registry |

## Compatibility contracts (do not break)

1. **State directory** — same as `src/config/app-paths.ts`:
   `config.json`, `profiles/<name>/{sessions.json,workspaces.json,
   callback-nonces.json,secrets.enc,.keystore.salt,logs}`,
   `registry/{processes.json,locks}` and the `secrets-getter` wrapper. `~/.lark-channel` is renamed to `~/.larkline` on
   first start with the default home when the new directory is absent.
   `LARKLINE_HOME` / `LARK_CHANNEL_HOME` and `LARKLINE_PROFILE` /
   `LARK_CHANNEL_PROFILE` both work.
2. **Keystore** — AES-256-GCM, key = PBKDF2-HMAC-SHA256(`hostname|username`,
   salt, 100_000, 32), 12-byte IV, 16-byte tag, standard padded base64. The
   envelope matches `src/config/keystore.ts`; Rust tests cover round-trip, a
   fresh IV per write, wrong-identity failure and single salt creation.
3. **Config** — schema v2, `activeProfile` + `profiles{}`. Fields the Rust
   binary does not use yet are kept in a `flatten`ed map, so a round-trip never
   drops settings written by the TypeScript bridge.
4. **Secrets protocol** — `larkline secrets get` reads
   `{protocolVersion, provider, ids}` on stdin and writes
   `{protocolVersion, values}` on stdout, plus an `errors` map when any id is
   missing; it exits 0 even for missing ids (lark-cli's exec-provider
   contract).
5. **Agent prompts** — `crates/larkline-agent/resources/bridge_system_prompt.md`
   is extracted verbatim from `src/agent/bridge-system-prompt.ts`; keep it in
   sync while both implementations exist.
6. **Service identity** — launchd label `ai.larkline.bot.<id>`, systemd unit
   `larkline.bot.<id>.service`, Windows task `Larkline.Bot.<id>`. The service id
   sanitizer matches the TypeScript one, so `larkline unregister` can remove a
   service installed by the old CLI.

## Status

Ported:

- paths/layout, profile CRUD, config load/save with unknown-field preservation
- encrypted keystore (Node-compatible), atomic writes, advisory locks
- `secrets get|set|list|remove` including the exec-provider protocol
- `profile list|create|use|remove|show`, `doctor`, `onboard`
- Claude argv builder + `stream-json` translator; Codex argv builder + JSONL
  translator
- bridge system prompt extraction, executable discovery
- `lark-channel` wiring: send text/card, update card, tenant-token REST client
- run flow: inbound message -> policy -> debounced queue -> CardKit streaming
  progress message (`card` = rich card, `markdown` = one-element streaming
  markdown card, `text` = final message only) -> agent process -> event
  translation -> throttled updates -> session persistence; at the 8.5-minute
  card safe age the progress frame is paused once (streaming off, no button,
  pause note) and the full answer is sent separately when the run finishes
- rich card content model (port of `src/card/run-renderer.ts` and
  `src/card/tool-render.ts`): reasoning panel (open while thinking, 3000-char
  body), one collapsible panel per tool call (✅/❌/⏳ header, red error border,
  input/output markdown with the 80/600/1200/2500-char caps), header-only
  "☕ N 个工具调用" summary once a group reaches three calls (running keeps the
  latest expanded; finished runs collapse all), terminal notices
  (`⏹ 已被中断`, `⏱ N 分钟无响应`, `⚠️ agent 失败：…`, `（未返回内容）`), the
  live footer phrase (`🧠 正在思考` / `🧰 正在调用工具` / `✍️ 正在输出`) and the
  signed stop button (`⏹ 终止`) after it
- rich card size policy: the serialized card is measured after email masking
  and kept under Feishu's 30KB limit by degrading in value order — tool bodies
  collapse to headers, the reasoning body shrinks then disappears, tool groups
  disappear, the bridge metadata note and live footer phrase drop, and the
  answer markdown is truncated last with a visible suffix; terminal notices,
  the summary label and the stop button are never dropped
- session store (resume only from the same cwd), stable per-scope sessions
- topic scoping: sessions, workspace overrides, pending queues and live runs
  use `chatId:threadId` for topic-group messages (port of `src/bot/scope.ts`);
  replies go back inside the triggering topic
- agent process runner: stdin prompt, stderr drain, kill-on-drop, 30-minute
  wall-clock safety net when the idle watchdog is disabled
- `/stop` cooperatively interrupts the run and drops its queued batch, via
  command or card button: the agent child is killed, the partial answer is
  kept and the card is finalized with `⏹ 已被中断` (no live button); a 5-second
  abort watchdog covers a hung finalize. `/stop <scope>` (admin) also aborts
  comment and meeting runs by their ids
- idle watchdog for IM runs (port of `runIdleTimeoutMs` / the
  `armOrPauseIdle` rule in `src/bot/channel.ts`): a run with no agent activity
  for N minutes is stopped cooperatively, keeping the partial answer and
  rendering `⏱ N 分钟无响应,已自动终止`. N comes from the per-scope `/timeout`
  override (`0` = off) else `preferences.runIdleTimeoutMinutes` (clamped
  1-120); the timer re-arms on every agent event and pauses while a `tool_use`
  has no matching `tool_result`. The 30-minute wall clock remains only as a
  safety net when the watchdog is disabled; `/timeout` persists to
  `sessions.json` and is now consumed by the run flow
- workspace store per scope (`/cd`, `/pwd`), working reaction, pending queue
  with the 600 ms merge window
- in-chat commands: `/new`, `/reset`, `/cd`, `/pwd`, `/status`, `/config`,
  `/meeting`, `/stop`, `/stop <scope>`, `/timeout`, `/help`; `/cd` (admin) expands `~`,
  interrupts the live run and resets the session; `/new` interrupts before
  clearing, and a per-scope run generation stops a finishing run from
  re-saving the cleared session
- global agent concurrency cap: port of `src/bot/process-pool.ts` backed by
  `preferences.maxConcurrentRuns` (default 10, clamped 1-50), enforced across
  IM, comment and meeting runs and resizable at runtime with
  `/config set maxConcurrentRuns <n>`
- runtime app-owner resolution for admin-gated commands
  (`application/v6/applications/<app_id>`, 30-minute cache, fail closed)
- signed card callbacks: bridge-rendered stop buttons use a process-local key;
  agent-rendered `bridge_token` callbacks are verified, access-checked and
  forwarded as `[card-click]` messages; accepted nonces are written through to
  `profiles/<name>/callback-nonces.json` (atomic, mode 0600) so a replay is
  rejected after a restart too
- file-comment mentions (`drive.notice.comment_add_v1`) for `doc`, `docx`,
  `sheet` and `file`, with in-thread replies; one run per comment thread,
  registered in `Bridge::active` as `comment:<hash>` and abortable
- in-meeting VC bot: poll/push sessions, transcript stabilization, `/meeting`
  commands, IM/meeting replies and end-of-meeting summaries; runs are
  registered as `meeting:<id>` and abortable
- QR onboarding (`larkline onboard`) with the registration device flow
- OS daemon lifecycle (`start`/`stop`/`restart`/`status`/`unregister`) with
  launchd, systemd --user and schtasks renderers
- process registry writer: `run` registers and unregisters its own process with
  an advisory lock, atomic replace, dead-pid pruning and a 64-entry cap;
  `ps` / `kill` read the shared registry

## Still to port and known gaps

Ordered roughly by user impact. Each item names the reason or the missing
primitive so it can be picked up without re-deriving the design.

1. **Access-control parity** — `crates/larkline-bridge/src/policy.rs` implements
   the `allowedUsers` allowlist and the group `@`-mention rule only. Runtime
   app-owner discovery now gates admin commands, but not ordinary messages;
   the TypeScript owner bypass, team mode for usage, per-chat
   `chatRequireMention` overrides, `allowedChats` for ordinary messages and
   `/invite` / `/remove` are still missing. Consequence: with `allowedUsers`
   empty, DMs are open to anyone who can reach the app. `allowedChats` is
   consulted only by the card-click check.
2. **In-chat command parity** — missing `/ws` (named workspaces), `/resume`,
   in-chat `/ps`, `/exit`, `/reconnect` and `/doctor`. `/config` is a text
   subset that can read and set `messageReply`, `showToolCalls` and
   `maxConcurrentRuns`; the CardKit form, COT/reasoning/access/lark-cli
   settings are not implemented.
3. **Idle timeout / watchdog for comment and meeting runs** — permission modes
   are now read from `permissions.defaultAccess` / `maxAccess` (with legacy
   `sandbox` fallback) and passed to Codex as `--sandbox` and Claude as
   `--permission-mode`; see `crates/larkline-bridge/src/permissions.rs`. The IM
   run flow now applies the idle watchdog and `/timeout` override; comment and
   meeting runs have their own loops and still use only the 30-minute
   `RUN_TIMEOUT` cap. `agent_stop_grace_ms` is parsed but unused.
4. **Presentation parity** — no COT process messages and no reasoning bubble.
   The rich card now matches the TypeScript content model (reasoning panel,
   per-tool panels, collapse rules, terminal notices, footer status and stop
   button), but three presentation details are deliberate Rust simplifications:
   tool inputs/outputs are stored bounded (4k chars) instead of raw; a
   `final_text` event is folded into the trailing text block so a final-only
   agent still renders (TS keeps it out of `blocks`); and individual tool panels
   below the three-call threshold start collapsed (`expanded: false`) exactly as
   in the TS snapshot. The rich-card terminal timeout notice reports the
   minutes configured by the idle watchdog (or 30 when the wall-clock safety
   net fired with the watchdog disabled); an agent-reported timeout without
   minutes renders `⏱ 0 分钟`. The text renderer
   honors `reasoningDisplay: inline` only; `auto`, `off` and `bubble` have no
   COT client behind them. `markdown` mode is a one-element streaming card with
   tool summaries from the text renderer but no tool panel, footer element or
   stop button.
5. **Attachments** — inbound images and files are not downloaded. `AgentSpec`
   always carries an empty image list, so the agent receives text prompts only.
6. **Topic-scope edge cases** — topic scoping is implemented from the event's
   `thread_id`. A topic-group event that drops `thread_id` falls back to the
   chat scope because the Rust bridge does no message-lookup backfill. Agent-
   rendered card callbacks bind the plain chat id: the click event carries no
   thread id, so a topic run cannot be verified for those callbacks.
7. **Registry bot names and stale detection** — the writer now takes the
   registry lock, prunes dead pids and writes atomically. The entry has no
   `botName`: `lark-channel` 0.6 exposes no bot-identity accessor, so the name
   is not known after the WebSocket handshake. Rust `ps` output shows the
   profile, agent, pid and start time; it does not display a bot name or app
   id. Stale cleanup probes pid liveness instead of
   the TypeScript runtime locks, because the Rust `run` does not acquire those
   locks yet. The TypeScript lock is a `proper-lockfile` directory while the
   Rust one is a regular file; both avoid interleaving but are not
   interchangeable representations.
8. **Keepalive watchdog** — not started. `lark-channel` 0.6 exposes neither a
   WebSocket connection-status hook nor an in-process force-reconnect handle;
   see `crates/larkline-bridge/src/keepalive.rs` and its `KEEPALIVE_GAPS`.
9. **Comments** — `wiki` file types are logged and skipped (no wiki token
   resolution). Comment runs are registered and abortable with
   `/stop comment:<hash>`; the event frame is acknowledged before the run
   starts, so an acknowledged comment event cannot be redelivered while its
   run is in flight.
10. **Meetings** — opt-in through `profile.extra.meeting.enabled`. The manager
    consumes push frames and polls `bots/events`; no `lark-cli` preflight probe
    is run (only the response classifier is ported). End-of-meeting summaries
    are sent as top-level IM messages to the configured origin/owner target;
    they are not threaded into the originating topic. The app needs the VC bot
    scopes and a tenant that supports the capability.
11. **Callbacks** — the profile App Secret is now callback key version 1 by
    default, matching TypeScript (`lark-cli` tokens verify without extra
    configuration); explicit `extra.callback.secret`, `extra.callback.keys` or
    `LARKLINE_CALLBACK_SECRET` still take precedence. Replay protection is
    durable: accepted nonces are written through to
    `profiles/<name>/callback-nonces.json` and loaded by the next bridge
    instance, so a restart cannot replay a token. The file keeps the TS flat
    `nonce -> "used" | "revoked"` shape plus a reserved expiry sidecar, so a
    TypeScript bridge can still read it after a downgrade. `run_id` and
    `policy_fingerprint` are not bound because the Rust bridge keeps neither.
12. **Session catalog** — `sessions.json.catalog.json` is not written or read;
    resume is keyed by scope and cwd only.
13. **Test parity** — the Rust workspace has unit and integration tests for the
    ported modules, but the TypeScript sources and vitest suite remain the
    behavioural reference and have not all been ported as Rust specs.
14. **Release signing** — the macOS job requires Apple Developer ID and
    notarization secrets that are still pending in the release environment;
    without them it fails rather than publishing an unsigned binary. Windows
    Authenticode signing is not configured.

## Upgrade network dependency

`larkline upgrade` (with or without `--check`) requests
`https://github.com/luozhouyang/larkline/releases/latest` and reads the tag
from the redirect target before doing anything else, so it needs network
access to GitHub. `--target-version <v>` still performs that latest-release
lookup before printing the pinned installer command; it is not an offline
mode. An unreachable GitHub or a missing release fails the command:

```bash
sh /tmp/larkline-install.sh --version <version>
```

Pin installs with the installer directly when the API is unavailable.

## Build & test

```bash
cargo fmt --all -- --check
cargo clippy --workspace --all-targets
cargo test --workspace
cargo build --release
./target/release/larkline doctor
```

CI (`.github/workflows/ci.yml`) runs the test suite on macOS, Ubuntu and
Windows; formatting and clippy run on Linux. Rust 1.85 or newer is required
(edition 2024).

## Release

Releases are tag-driven. Pushing a `v*` tag runs
`.github/workflows/release.yml`:

1. `cargo fmt --all -- --check` and `cargo test --workspace` on Linux.
2. Build statically linked Linux `x86_64` and `aarch64` binaries with the musl
   targets (`x86_64-unknown-linux-musl` / `aarch64-unknown-linux-musl`), a
   macOS universal binary (Developer ID signed and notarized when the Apple
   secrets exist), and Windows `x86_64`. The Linux jobs fail unless `file`
   reports `statically linked`, so the release never depends on the runner's
   glibc.
3. Generate `SHA256SUMS` over every asset and create the GitHub Release.

The macOS job fails loudly when the Apple signing/notarization secrets are
missing. Windows Authenticode signing is not configured; SmartScreen warnings
are tolerated for now.

`install.sh` downloads the asset for the current OS/arch, verifies it against
`SHA256SUMS` when present, and installs it into `~/.local/bin` (or
`LARKLINE_INSTALL_DIR` / `--dir`) without sudo. It supports `--version` to pin
a release.

## Upgrade policy

`larkline upgrade` is explicit. It reports the latest release and prints the
exact installer command; it never replaces the binary underneath a running
daemon on its own. `install.sh` writes `larkline.new` and renames it over the
existing binary, so a running process keeps its old inode and the next start
picks up the new one.
