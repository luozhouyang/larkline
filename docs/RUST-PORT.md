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
   pending.json,callback-nonces.json,secrets.enc,.keystore.salt,logs}`,
   `registry/{processes.json,locks}` and the `secrets-getter` wrapper.
   `pending.json` is a write-through copy of the per-scope debounce queue
   (mode 0600, 24-hour per-message TTL, raw platform payload stripped, corrupt files
   treated as empty); a restart re-checks the access policy and resumes the
   queue. Delivery is at-most-once: a claimed batch leaves the file before the
   agent starts. `~/.lark-channel` is renamed to `~/.larkline` on
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
  card safe age a fresh streaming card is created and the run continues there
  (the old card is finalized with a `⏭ 过程已接续到下一张卡片` note, the new
  one starts with `↪ 接续上一张卡片`, and the final answer lands on the new
  card). If the new card cannot be created the old pause behavior remains:
  streaming off, no button, a pause note, and the full answer sent separately
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
- COT process messages (port of `src/bot/cot.ts`): a typed
  `im/v1/message_cot` client plus a per-run publisher. `cotMessages`
  (`off`/`brief`/`detailed`, with the legacy `on`/`simple` values) and
  `reasoningDisplay` resolve one bubble per run: `brief` publishes summarized
  tool steps, `detailed` adds tool args and 1200-char output, reasoning deltas
  are capped at 4000 chars; events are batched at most once per 600 ms window.
  A process bubble replaces the progress-card stream (no duplicate progress,
  final answer sent without reasoning/tool panels), while `reasoningDisplay:
  bubble` with COT off creates a lazily-started reasoning-only bubble. The
  bubble is completed on finish/interrupt/timeout and every COT failure
  (create, update, complete, timeout) is logged and never fails the run; a
  mid-run update failure also sends the TS degraded notice
  (`COT 过程消息更新失败…`)
- workspace store per scope (`/cd`, `/pwd`), working reaction, pending queue
  with the 600 ms merge window, persisted through `pending.json` and restored
  on the next start (`Bridge::resume_pending` starts its driver after the
  runtime is live)
- IM approval cards for Claude Code `default`/`acceptEdits`: the adapter keeps
  stdin open and passes `--permission-prompt-tool stdio`; `can_use_tool`
  control requests become a standalone non-streaming card with signed
  Allow/Allow-for-session/Deny buttons. Only the run's initiator can answer, a
  click writes the `control_response` and resumes the CLI, ten minutes deny
  automatically, and a failed card send denies immediately. Allow-for-session
  sends the CLI's `permission_suggestions` as `updatedPermissions`.
  `plan`/`bypassPermissions` keep the one-shot path; comment/meeting runs set
  `AgentSpec.approvals = false` and keep their old one-shot behavior
- user questions: Claude `AskUserQuestion` and Codex `item/tool/requestUserInput`
  become option cards (one question at a time, multi-select with a submit
  button). Claude answers travel in `updatedInput.answers`; Codex answers use
  the `answers[questionId].answers[]` payload. Free-text questions without
  options are skipped with a visible notice for now. Approval parameters are
  shown in full up to the Feishu card budget (20KB of raw JSON); larger inputs
  are truncated with a notice
- Codex app-server transport for `read-only`/`workspace-write`: `codex
  app-server` replaces `codex exec` so command, file-change and permission
  requests can reach the same approval card. The bridge initializes, starts or
  resumes the thread (`thread/start`/`thread/resume`), starts the turn and
  translates notifications into the shared event vocabulary; the thread id is
  saved as the session. A failed resume falls back to a new thread, and
  unsupported server requests are answered immediately. `danger-full-access`
  keeps `codex exec --json` and `approval_policy=never`
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
- injectable outbound transport: `BridgeTransport` / `BridgeStream` (implemented
  by `larkline_lark::Channel`) covers text/card sends, threaded replies,
  message-card updates, CardKit entity creation and streaming cards; the
  working reaction has its own `ReactionTransport` injection point.
  `Bridge::new_with_transport` builds a bridge around a recording fake, so the
  real run flow can be tested without a `Channel` or a network
- end-to-end run-flow tests (`crates/larkline-bridge/tests/run_flow.rs`) with a
  fake agent binary speaking Claude `stream-json` plus the fake transport:
  happy path (running card -> text/tool events -> terminal card with the final
  answer), cooperative `/stop` (terminal `⏹ 已被中断` card with the partial
  answer and no live button), idle-watchdog timeout (terminal notice, no
  button), markdown mode (one markdown element, no tool panel/footer element or
  stop button), a failing attachment download that does not fail the run, the
  8.5-minute card rollover, a Claude approval click-through plus a
  non-initiator rejection, the ten-minute auto-deny, and a Codex app-server
  command approval click-through
- agent-runner integration tests cover the Claude control frames and the Codex
  app-server JSON-RPC round trip, including resume fallback when a thread is
  gone

## Remaining gaps

Only the items below are still open. The injectable outbound transport and the
end-to-end run flow are covered by unit and integration tests; the remaining
items are listed here so they can be picked up without re-deriving the design.

1. **Apple release secrets** — the macOS release job requires Apple Developer
   ID signing and notarization credentials that are not configured in the
   release environment. Without them the job fails loudly instead of
   publishing an unsigned binary. Windows Authenticode signing is also not
   configured (see the non-TS extras below).
2. **Wiki comments** — cloud-doc comment events for `doc`, `docx`, `sheet` and
   `file` are supported; `wiki` file types are logged and skipped because no
   wiki-token resolution is implemented.
3. **Meeting summary threading** — end-of-meeting summaries are sent as
   top-level IM messages to the configured origin chat or owner DM; they are
   not threaded into the topic the meeting started from.
4. **Session catalog** — `sessions.json.catalog.json` is neither written nor
   read; resume is keyed by scope and working directory only.
5. **Non-TS extras and remaining parity edges** — access-control edges that the
   message path still does not apply (the TypeScript owner bypass for ordinary
   messages, team-mode usage for ordinary messages, per-chat
   `chatRequireMention` overrides, and `allowedChats` on ordinary messages;
   `allowedChats` is consulted only for card-click verification), plus the
   non-TypeScript extras: Windows Authenticode signing, bot-name enrichment for
   Rust-owned `ps` registry entries (`lark-channel` 0.6 exposes no bot-identity
   accessor) and the live keepalive watchdog (`lark-channel` 0.6 exposes
   neither a WebSocket status hook nor a force-reconnect handle; the typed gap
   lives in `crates/larkline-bridge/src/keepalive.rs`).

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
