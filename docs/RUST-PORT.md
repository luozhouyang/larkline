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
   `config.json`, `profiles/<name>/{sessions.json,workspaces.json,secrets.enc,
   .keystore.salt,logs}`, `registry/{processes.json,locks}` and the
   `secrets-getter` wrapper. `~/.lark-channel` is renamed to `~/.larkline` on
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
   `{protocolVersion, values, errors}` on stdout, exit 0 even for missing ids
   (lark-cli's exec-provider contract).
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
  card -> agent process -> event translation -> throttled updates -> session
  persistence
- session store (resume only from the same cwd), stable per-chat sessions
- agent process runner: stdin prompt, stderr drain, kill-on-drop, 30 minute cap
- `/stop` aborts the run and drops its queued batch, via command or card button
- workspace store per chat (`/cd`, `/pwd`), working reaction, pending queue
  with the 600 ms merge window
- in-chat commands: `/new`, `/reset`, `/cd`, `/pwd`, `/status`, `/config`,
  `/meeting`, `/stop`, `/help`
- signed card callbacks: bridge-rendered stop buttons use a process-local key;
  agent-rendered `bridge_token` callbacks are verified, access-checked and
  forwarded as `[card-click]` messages
- file-comment mentions (`drive.notice.comment_add_v1`) for `doc`, `docx`,
  `sheet` and `file`, with in-thread replies
- in-meeting VC bot: poll/push sessions, transcript stabilization, `/meeting`
  commands, IM/meeting replies and end-of-meeting summaries
- QR onboarding (`larkline onboard`) with the registration device flow
- OS daemon lifecycle (`start`/`stop`/`restart`/`status`/`unregister`) with
  launchd, systemd --user and schtasks renderers
- `ps` / `kill` over the shared process registry, with `run` registering and
  unregistering its own process

## Still to port and known gaps

Ordered roughly by user impact. Each item names the reason or the missing
primitive so it can be picked up without re-deriving the design.

1. **Access-control parity** — `crates/larkline-bridge/src/policy.rs` implements
   the `allowedUsers` allowlist and the group `@`-mention rule only. Runtime
   app-owner discovery, admin bypass, `allowedChats` for ordinary messages and
   `/invite` / `/remove` are missing. Consequence: with `allowedUsers` empty,
   DMs are open to anyone who can reach the app. `allowedChats` is consulted
   only by the card-click check.
2. **In-chat command parity** — missing `/ws` (named workspaces), `/resume`,
   `/timeout`, in-chat `/ps`, `/exit`, `/reconnect` and `/doctor`. `/config`
   is a text subset that can read and set `messageReply` and `showToolCalls`;
   the CardKit form, COT/reasoning/access/lark-cli settings and
   `maxConcurrentRuns` writes are not implemented (`maxConcurrentRuns` is
   displayed read-only and is not enforced).
3. **Permission modes** — `permissions.defaultAccess` / `maxAccess` and the
   legacy `sandbox` field are not read. Runs, comment runs and meeting runs
   hardcode `SandboxMode::DangerFullAccess`. The idle watchdog and `/timeout`
   are also not wired; `agent_stop_grace_ms` and session idle timeouts are
   parsed but unused.
4. **Presentation parity** — no COT process messages, no card thinking panel
   and no reasoning bubble. The text renderer honors `reasoningDisplay: inline`
   only; `auto`, `off` and `bubble` have no COT client behind them.
5. **Attachments** — inbound images and files are not downloaded. `AgentSpec`
   always carries an empty image list, so the agent receives text prompts only.
6. **Topic/thread scopes** — sessions and the pending queue are keyed by
   `chat_id`; topic groups share one session. The TypeScript bridge uses
   `chatId:threadId`.
7. **Registry bot names and stale detection** — `run` registers its process
   on startup and removes it on normal exit and on ctrl-c/SIGTERM, so both
   implementations share one `ps` list. The entry has no `botName`:
   `lark-channel` 0.6 exposes no bot-identity accessor, so the name is not
   known after the WebSocket handshake and `ps`/`status` fall back to the app
   id for Rust entries. Stale cleanup probes pid liveness instead of the
   TypeScript runtime locks, because the Rust `run` does not acquire those
   locks yet.
8. **Keepalive watchdog** — not started. `lark-channel` 0.6 exposes neither a
   WebSocket connection-status hook nor an in-process force-reconnect handle;
   see `crates/larkline-bridge/src/keepalive.rs` and its `KEEPALIVE_GAPS`.
9. **Comments** — `wiki` file types are logged and skipped (no wiki token
   resolution). Comment runs are detached from the event loop and are not
   registered in `Bridge::active`, so `/stop` cannot abort one; they are
   bounded by the 30 minute run timeout. The frame is acknowledged before the
   run starts, so an acknowledged comment event cannot be redelivered while
   its run is in flight.
10. **Meetings** — opt-in through `profile.extra.meeting.enabled`. The manager
    consumes push frames and polls `bots/events`; no `lark-cli` preflight probe
    is run (only the response classifier is ported). The app needs the VC bot
    scopes and a tenant that supports the capability.
11. **Callbacks** — agent-rendered callbacks need an explicit key
    (`extra.callback.secret`, `extra.callback.keys` or
    `LARKLINE_CALLBACK_SECRET`); the TypeScript bridge could sign with the App
    Secret, which is not handed to `Bridge`. Replay protection is in-memory,
    so a token replayed after a restart is accepted until it expires. `run_id`
    and `policy_fingerprint` are not bound because the Rust bridge keeps
    neither.
12. **Session catalog** — `sessions.json.catalog.json` is not written or read;
    resume is keyed by chat id and cwd only.
13. **Test parity** — the Rust workspace has unit and integration tests for the
    ported modules, but the remaining TypeScript test files have not all been
    ported as behaviour specs.

## Known CLI issue

`UpgradeArgs::version` and clap's propagated `--version` flag share the name
`version`. Release builds tolerate it, but a debug build panics on any
`larkline upgrade` invocation (including `--help` and `--check`). Release
`upgrade --version <v>` also performs the GitHub latest-release lookup before
using the pinned value. Until the argument is renamed, pin versions with the
installer and use `larkline upgrade --check` only from a release build:

```bash
sh /tmp/larkline-install.sh --version <version>
```

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
   macOS universal binary (Developer ID signed and notarized), and Windows
   `x86_64`. The Linux jobs fail unless `file` reports `statically linked`, so
   the release never depends on the runner's glibc.
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
