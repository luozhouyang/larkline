# Rust port

The goal is a single, statically linked `larkline` binary with **no Node
runtime and no npm at install time**. The TypeScript sources stay in the tree
until the port reaches parity; both implementations read and write the same
state directory, so they can run side by side during the migration.

## Layout

| Crate | Responsibility |
|---|---|
| `larkline-core` | paths, config schema v2, keystore, atomic writes, file locks |
| `larkline-lark` | `lark-channel` SDK wrapper, CardKit cards, raw OpenAPI gaps |
| `larkline-agent` | Claude Code / Codex argv builders and stream translators |
| `larkline-bridge` | run flow, policy, session persistence (in progress) |
| `larkline-cli` | `larkline` binary: clap commands, doctor, upgrade |

## Compatibility contracts (do not break)

1. **State directory** — same as `src/config/app-paths.ts`:
   `~/.larkline/config.json`, `profiles/<name>/{sessions.json,workspaces.json,
   secrets.enc,.keystore.salt,media,logs}`, `registry/{processes.json,locks}`.
   `LARKLINE_HOME` / `LARK_CHANNEL_HOME` / `LARKLINE_PROFILE` keep working and
   `~/.lark-channel` is migrated on first start.
2. **Keystore** — AES-256-GCM, key = PBKDF2-HMAC-SHA256(`hostname|username`,
   salt, 100_000, 32), 12-byte IV, 16-byte tag, base64. Verified both ways:
   `crates/larkline-core/src/keystore.rs` has a fixture test, and a Node
   script using the original algorithm decrypts a Rust-written store.
3. **Config** — schema v2, `activeProfile` + `profiles{}`. Fields the Rust
   binary does not use yet are kept in a `flatten`ed map, so a round-trip
   never drops settings.
4. **Secrets protocol** — `larkline secrets get` reads
   `{protocolVersion, provider, ids}` on stdin and writes
   `{protocolVersion, values, errors}` on stdout, exit 0 even for missing ids
   (lark-cli's exec-provider contract).
5. **Agent prompts** — `resources/bridge_system_prompt.md` is extracted
   verbatim from `src/agent/bridge-system-prompt.ts`; keep it in sync while
   both implementations exist.

## Status

Ported and tested:

- paths/layout, profile CRUD, config load/save with unknown-field preservation
- encrypted keystore (Node-compatible), atomic writes, advisory locks
- `secrets get|set|list|remove` incl. the exec-provider protocol
- `profile list|create|use|remove|show`, `doctor`
- Codex argv builder + JSONL translator (port of `codex/jsonl.ts`)
- Claude argv builder + `stream-json` translator (port of `claude/*`)
- bridge system prompt extraction, executable discovery
- `lark-channel` wiring: send text/card, update card, tenant-token REST client
- `run` connects the real WebSocket event stream and logs inbound events

Still to port (rough order):

1. run flow: policy -> card -> spawn agent -> stream into card -> session
2. session/workspace stores, pending queue, active runs, keepalive
3. commands (`/config`, `/stop`, …) and card callback dispatch
4. comments, meetings, reactions (raw REST endpoints)
5. QR app registration (device flow: `begin` + poll)
6. daemon install (launchd/systemd/schtasks) and `ps`/`kill`
7. port the remaining 113 TypeScript test files as behaviour specs

## Build & test

```bash
cargo test --workspace
cargo build --release
./target/release/larkline doctor
```

The release pipeline builds Linux (x86_64, aarch64), a signed + notarized
macOS universal binary, and Windows x86_64. `install.sh` downloads one of
those assets, verifies `SHA256SUMS` and installs into `~/.local/bin` without
sudo.

## Upgrade policy

`larkline upgrade` is explicit. It reports the latest release and prints the
exact installer command; it never replaces the binary underneath a running
daemon on its own. `install.sh` writes `larkline.new` and renames it over the
existing binary, so a running process keeps its old inode and the next start
picks up the new one.
