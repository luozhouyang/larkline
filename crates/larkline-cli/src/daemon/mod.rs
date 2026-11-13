//! OS daemon management.
//!
//! Ported from `src/daemon/{launchd,systemd,schtasks,paths,service-adapter}.ts`
//! and the lifecycle semantics of `src/cli/commands/service.ts`.
//!
//! Every platform module has two halves:
//!
//! * **Pure renderers** (`render_plist`, `render_unit`, `render_launcher_cmd`,
//!   `*_args`, `parse_status`) that turn a [`ServiceSpec`] into the exact text
//!   or argv a service manager consumes. These touch neither the filesystem nor
//!   the network, and are unit-tested.
//! * **Runtime helpers** (`install`, `uninstall`, `start`, `stop`, `status`)
//!   that write the definition file and shell out to `launchctl`,
//!   `systemctl --user` or `schtasks` through [`std::process::Command`].
//!
//! All three managers are driven in **per-user** mode: the launchd `gui/<uid>`
//! domain, systemd `--user` units and Task Scheduler `/RL LIMITED`. No command
//! here needs root, and none asks for elevation.
//!
//! The sibling [`crate::ps`] module owns the process registry that daemons
//! write to (`~/.larkline/registry/processes.json`).

pub mod launchd;
pub mod schtasks;
pub mod systemd;

use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;

use anyhow::{Context, Result, anyhow, bail};

/// Base name shared by every service definition: launchd label
/// (`ai.<SERVICE_NAME>.<id>`), systemd unit (`<SERVICE_NAME>.<id>.service`) and
/// Task Scheduler task (`Larkline.Bot.<id>`).
///
/// Kept in sync with `SERVICE_NAME` in `src/daemon/paths.ts`.
pub const SERVICE_NAME: &str = "larkline.bot";

/// Reserved service id for the machine-wide supervisor/console daemon.
///
/// It is keyed distinctly from any real profile so the supervisor has one
/// fixed label/log path per machine and never flaps against a per-profile
/// service. It is a valid service id, so it flows through the same helpers as
/// a profile name.
pub const SUPERVISOR_SERVICE_ID: &str = "supervisor";

// ---------------------------------------------------------------------------
// Service ids
// ---------------------------------------------------------------------------

/// Normalize a profile name / service id into an OS-label-safe id.
///
/// ASCII-safe names pass through unchanged so existing labels and paths stay
/// stable. Names with non-ASCII characters (or other label-unsafe characters)
/// become `<sanitized-base>-<first 8 hex chars of sha1(raw)>`, exactly like the
/// TypeScript implementation. The hash keeps two different non-ASCII names
/// from collapsing onto the same service.
pub fn service_profile_id(raw: &str) -> Result<String> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        bail!("profile name is required for service id");
    }
    if trimmed == "." || trimmed == ".." {
        bail!("invalid profile name: {raw}");
    }
    if trimmed
        .bytes()
        .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'.' | b'_' | b'-'))
    {
        return Ok(trimmed.to_string());
    }

    // Collapse every run of unsafe characters into a single '-' (matching
    // `s.replace(/[^A-Za-z0-9._-]+/g, '-')`), then trim [-.] from both ends.
    let mut base = String::with_capacity(trimmed.len());
    let mut separator_pending = false;
    for ch in trimmed.chars() {
        if ch.is_ascii_alphanumeric() || matches!(ch, '.' | '_' | '-') {
            if separator_pending && !base.is_empty() {
                base.push('-');
            }
            separator_pending = false;
            base.push(ch);
        } else {
            separator_pending = true;
        }
    }
    let base = base.trim_matches(|ch| ch == '-' || ch == '.');
    let base: String = base.chars().take(24).collect();
    let base = if base.is_empty() {
        "profile".to_string()
    } else {
        base
    };
    let digest = sha1_hex(trimmed.as_bytes());
    Ok(format!("{base}-{}", &digest[..8]))
}

/// SHA-1, used only for the service-id suffix.
///
/// Kept local instead of pulling a hashing crate into the CLI dependency
/// tree; it must match `createHash('sha1')` from the TypeScript bridge so a
/// service installed by the old implementation can still be found (and
/// unregistered) by the Rust one.
fn sha1_hex(data: &[u8]) -> String {
    let mut state: [u32; 5] = [
        0x6745_2301,
        0xEFCD_AB89,
        0x98BA_DCFE,
        0x1032_5476,
        0xC3D2_E1F0,
    ];
    let bit_len = (data.len() as u64).wrapping_mul(8);

    let mut message = Vec::with_capacity(data.len() + 72);
    message.extend_from_slice(data);
    message.push(0x80);
    while message.len() % 64 != 56 {
        message.push(0);
    }
    message.extend_from_slice(&bit_len.to_be_bytes());

    for chunk in message.chunks_exact(64) {
        let mut words = [0u32; 80];
        for (index, word) in words.iter_mut().take(16).enumerate() {
            let offset = index * 4;
            *word = u32::from_be_bytes([
                chunk[offset],
                chunk[offset + 1],
                chunk[offset + 2],
                chunk[offset + 3],
            ]);
        }
        for index in 16..80 {
            words[index] =
                (words[index - 3] ^ words[index - 8] ^ words[index - 14] ^ words[index - 16])
                    .rotate_left(1);
        }

        let [mut a, mut b, mut c, mut d, mut e] = state;
        for (index, word) in words.iter().enumerate() {
            let (f, k) = match index {
                0..=19 => ((b & c) | ((!b) & d), 0x5A82_7999),
                20..=39 => (b ^ c ^ d, 0x6ED9_EBA1),
                40..=59 => ((b & c) | (b & d) | (c & d), 0x8F1B_BCDC),
                _ => (b ^ c ^ d, 0xCA62_C1D6),
            };
            let temp = a
                .rotate_left(5)
                .wrapping_add(f)
                .wrapping_add(e)
                .wrapping_add(k)
                .wrapping_add(*word);
            e = d;
            d = c;
            c = b.rotate_left(30);
            b = a;
            a = temp;
        }

        state[0] = state[0].wrapping_add(a);
        state[1] = state[1].wrapping_add(b);
        state[2] = state[2].wrapping_add(c);
        state[3] = state[3].wrapping_add(d);
        state[4] = state[4].wrapping_add(e);
    }

    state
        .iter()
        .map(|word| format!("{word:08x}"))
        .collect::<String>()
}

// ---------------------------------------------------------------------------
// Paths (mirrors src/daemon/paths.ts)
// ---------------------------------------------------------------------------

fn env_nonempty(key: &str) -> Option<String> {
    std::env::var(key).ok().filter(|value| !value.is_empty())
}

/// The OS user's home directory.
///
/// Returns `None` when `HOME` (Unix) / `USERPROFILE` (Windows) is unset;
/// callers that must write a per-user service definition turn that into a
/// descriptive error instead of guessing a path.
pub fn home_dir() -> Option<PathBuf> {
    #[cfg(windows)]
    {
        if let Some(profile) = env_nonempty("USERPROFILE") {
            return Some(PathBuf::from(profile));
        }
        if let (Some(drive), Some(tail)) = (env_nonempty("HOMEDRIVE"), env_nonempty("HOMEPATH")) {
            return Some(PathBuf::from(format!("{drive}{tail}")));
        }
        None
    }
    #[cfg(not(windows))]
    {
        env_nonempty("HOME").map(PathBuf::from)
    }
}

/// `$XDG_CONFIG_HOME`, defaulting to `~/.config`.
pub fn xdg_config_home() -> Option<PathBuf> {
    env_nonempty("XDG_CONFIG_HOME")
        .map(PathBuf::from)
        .or_else(|| home_dir().map(|home| home.join(".config")))
}

/// launchd label: `ai.larkline.bot.<id>`.
pub fn launch_agent_label(service_id: &str) -> String {
    format!("ai.{SERVICE_NAME}.{service_id}")
}

/// `~/Library/LaunchAgents/ai.larkline.bot.<id>.plist`.
pub fn launch_agent_plist_path(home: &Path, service_id: &str) -> PathBuf {
    home.join("Library")
        .join("LaunchAgents")
        .join(format!("{}.plist", launch_agent_label(service_id)))
}

/// systemd unit name: `larkline.bot.<id>.service`.
pub fn systemd_unit_name(service_id: &str) -> String {
    format!("{SERVICE_NAME}.{service_id}.service")
}

/// `$XDG_CONFIG_HOME/systemd/user/larkline.bot.<id>.service`.
pub fn systemd_unit_path(config_home: &Path, service_id: &str) -> PathBuf {
    config_home
        .join("systemd")
        .join("user")
        .join(systemd_unit_name(service_id))
}

/// Task Scheduler task name: `Larkline.Bot.<id>`.
pub fn windows_task_name(service_id: &str) -> String {
    format!("Larkline.Bot.{service_id}")
}

/// `<root>/daemon/<id>/launcher.cmd` — the wrapper the scheduled task runs.
pub fn windows_launcher_cmd_path(root_dir: &Path, service_id: &str) -> PathBuf {
    root_dir
        .join("daemon")
        .join(service_id)
        .join("launcher.cmd")
}

/// `<root>/profiles/<id>/logs/daemon` (same layout as `AppPaths::logs_dir`).
pub fn daemon_log_dir(root_dir: &Path, service_id: &str) -> PathBuf {
    root_dir
        .join("profiles")
        .join(service_id)
        .join("logs")
        .join("daemon")
}

/// Daemon stdout log: `<root>/profiles/<id>/logs/daemon/daemon-stdout.log`.
pub fn daemon_stdout_path(root_dir: &Path, service_id: &str) -> PathBuf {
    daemon_log_dir(root_dir, service_id).join("daemon-stdout.log")
}

/// Daemon stderr log: `<root>/profiles/<id>/logs/daemon/daemon-stderr.log`.
pub fn daemon_stderr_path(root_dir: &Path, service_id: &str) -> PathBuf {
    daemon_log_dir(root_dir, service_id).join("daemon-stderr.log")
}

// ---------------------------------------------------------------------------
// Service spec
// ---------------------------------------------------------------------------

/// Everything a platform renderer needs, fully resolved.
///
/// The spec intentionally carries log paths and the state root instead of
/// reading the environment at render time, so rendering stays pure.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ServiceSpec {
    /// Raw service id: a profile name, or [`SUPERVISOR_SERVICE_ID`].
    pub id: String,
    /// ASCII-safe id used in every OS label/path (`serviceProfileId`).
    pub service_id: String,
    /// launchd label: `ai.larkline.bot.<service_id>`.
    pub label: String,
    /// Base name: `larkline.bot.<service_id>` (systemd adds `.service`).
    pub name: String,
    /// Human description used by systemd and Task Scheduler.
    pub description: String,
    /// Absolute path to the larkline binary.
    pub program: PathBuf,
    /// Arguments passed after the program path, e.g. `["run", "--profile", "claude"]`.
    pub args: Vec<String>,
    /// Environment baked into the definition, e.g. `LARKLINE_HOME`,
    /// `LARKLINE_PROFILE`, and the invoking shell's `PATH`.
    pub env: Vec<(String, String)>,
    /// Daemon stdout log (append mode in every service manager).
    pub stdout_log: PathBuf,
    /// Daemon stderr log (append mode in every service manager).
    pub stderr_log: PathBuf,
    /// Working directory, when the caller wants one. The TypeScript bridge did
    /// not set one, so it defaults to `None`.
    pub working_dir: Option<PathBuf>,
    /// Larkline state root (`LARKLINE_HOME`). Also hosts the Windows launcher.
    pub root_dir: PathBuf,
}

impl ServiceSpec {
    /// Low-level constructor. Prefer [`Self::for_profile`] to get the pinned
    /// `run --profile <name>` arguments.
    ///
    /// `program` and `root_dir` are made absolute (without touching the
    /// filesystem) because every service manager requires absolute paths.
    /// `PATH` is captured from the invoking shell: launchd and systemd user
    /// units otherwise start with a minimal environment and fail to find
    /// `claude` / `codex` / `lark-cli`.
    pub fn new(
        id: &str,
        program: impl Into<PathBuf>,
        args: Vec<String>,
        root_dir: impl Into<PathBuf>,
    ) -> Result<Self> {
        let id = id.trim().to_string();
        if id.is_empty() {
            bail!("service id is required");
        }
        let service_id = service_profile_id(&id)?;
        let program = absolute_path(program.into(), "program")?;
        let root_dir = absolute_path(root_dir.into(), "state")?;
        let log_dir = daemon_log_dir(&root_dir, &service_id);

        let mut env = vec![(
            "LARKLINE_HOME".to_string(),
            root_dir.to_string_lossy().into_owned(),
        )];
        if let Some(path) = env_nonempty("PATH") {
            env.push(("PATH".to_string(), path));
        }

        Ok(Self {
            id,
            label: launch_agent_label(&service_id),
            name: format!("{SERVICE_NAME}.{service_id}"),
            description: format!("larkline bot ({service_id})"),
            service_id,
            program,
            args,
            env,
            stdout_log: log_dir.join("daemon-stdout.log"),
            stderr_log: log_dir.join("daemon-stderr.log"),
            working_dir: None,
            root_dir,
        })
    }

    /// Build the spec for a classic per-profile daemon.
    ///
    /// The run arguments pin `--profile <name>` so the service stays tied to
    /// its profile even if the active profile changes later. `LARKLINE_PROFILE`
    /// is set for the same reason.
    pub fn for_profile(
        profile: &str,
        program: impl Into<PathBuf>,
        root_dir: impl Into<PathBuf>,
    ) -> Result<Self> {
        let profile = profile.trim().to_string();
        let args = vec!["run".to_string(), "--profile".to_string(), profile.clone()];
        let spec = Self::new(&profile, program, args, root_dir)?;
        Ok(spec.with_env("LARKLINE_PROFILE", profile))
    }

    /// Build the spec for the machine-wide supervisor.
    ///
    /// The caller passes the run arguments (the TypeScript bridge used
    /// `["run", "--web-ui"]`) because the Rust CLI decides when the supervisor
    /// console becomes available. No `LARKLINE_PROFILE` is set: one supervisor
    /// hosts every profile in-process.
    pub fn for_supervisor(
        program: impl Into<PathBuf>,
        run_args: Vec<String>,
        root_dir: impl Into<PathBuf>,
    ) -> Result<Self> {
        let mut spec = Self::new(SUPERVISOR_SERVICE_ID, program, run_args, root_dir)?;
        spec.description = "larkline supervisor".to_string();
        Ok(spec)
    }

    /// Set or replace an environment variable (insertion order is preserved).
    pub fn with_env(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        let key = key.into();
        let value = value.into();
        if let Some((_, slot)) = self.env.iter_mut().find(|(existing, _)| *existing == key) {
            *slot = value;
        } else {
            self.env.push((key, value));
        }
        self
    }

    /// Set the daemon's working directory (`WorkingDirectory=` / plist key).
    pub fn with_working_dir(mut self, dir: impl Into<PathBuf>) -> Self {
        self.working_dir = Some(dir.into());
        self
    }

    /// Override the human description (systemd / Task Scheduler XML).
    pub fn with_description(mut self, description: impl Into<String>) -> Self {
        self.description = description.into();
        self
    }

    /// Look up an environment value baked into the spec.
    pub fn env_value(&self, key: &str) -> Option<&str> {
        self.env
            .iter()
            .find(|(existing, _)| existing == key)
            .map(|(_, value)| value.as_str())
    }
}

fn absolute_path(path: PathBuf, what: &str) -> Result<PathBuf> {
    std::path::absolute(&path)
        .with_context(|| format!("cannot resolve absolute {what} path: {}", path.display()))
}

// ---------------------------------------------------------------------------
// Platform selection and dispatch
// ---------------------------------------------------------------------------

/// Which OS service manager backs daemon mode.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Platform {
    /// macOS `launchd` user agents.
    Launchd,
    /// Linux `systemd --user` units.
    Systemd,
    /// Windows Task Scheduler (`schtasks`).
    Schtasks,
}

impl Platform {
    /// Map a `std::env::consts::OS` value to a platform.
    ///
    /// Pure so the dispatch table is testable on any host. `darwin` is accepted
    /// as an alias for callers that use Node's `process.platform` spelling.
    pub fn for_os(os: &str) -> Option<Self> {
        match os.trim().to_ascii_lowercase().as_str() {
            "macos" | "darwin" => Some(Self::Launchd),
            "linux" => Some(Self::Systemd),
            "windows" => Some(Self::Schtasks),
            _ => None,
        }
    }

    /// Platform for the host this binary was compiled for.
    pub fn current() -> Option<Self> {
        Self::for_os(std::env::consts::OS)
    }

    /// Display name used in status / error messages.
    pub fn name(self) -> &'static str {
        match self {
            Self::Launchd => "launchd (macOS)",
            Self::Systemd => "systemd (Linux user)",
            Self::Schtasks => "Task Scheduler (Windows)",
        }
    }
}

/// Resolve the platform for this host or fail with an actionable message.
pub fn current_platform() -> Result<Platform> {
    Platform::current().ok_or_else(|| {
        anyhow!(
            "background services are not supported on {} (supported: macOS/launchd, Linux/systemd, Windows/Task Scheduler)",
            std::env::consts::OS
        )
    })
}

/// Result of a lifecycle command. `ok == false` means the service manager
/// rejected the command; `stderr` carries its diagnostic text.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ServiceAction {
    pub platform: Platform,
    pub ok: bool,
    /// Exit code of the service-manager command, when it ran.
    pub code: Option<i32>,
    /// What the runtime helper did (useful for logs and CLI output).
    pub detail: String,
    pub stdout: String,
    pub stderr: String,
}

impl ServiceAction {
    pub(crate) fn ok(platform: Platform, detail: impl Into<String>) -> Self {
        Self {
            platform,
            ok: true,
            code: None,
            detail: detail.into(),
            stdout: String::new(),
            stderr: String::new(),
        }
    }

    pub(crate) fn from_output(
        platform: Platform,
        detail: impl Into<String>,
        output: Output,
    ) -> Self {
        Self {
            platform,
            ok: output.ok,
            code: output.code,
            detail: detail.into(),
            stdout: output.stdout,
            stderr: output.stderr,
        }
    }
}

/// Structured `status` result.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ServiceStatus {
    pub platform: Platform,
    /// Service definition file on disk. On Windows this is the launcher
    /// `.cmd`; the task itself is identified by `service_name`.
    pub definition_path: PathBuf,
    /// launchd label / systemd unit / Task Scheduler task name.
    pub service_name: String,
    /// Whether the definition exists on disk / is registered with the OS.
    pub installed: bool,
    /// Whether the service manager reports it running/loaded/active.
    pub running: bool,
    /// Main pid, when the manager exposes one.
    pub pid: Option<u32>,
    /// Last exit code, when the manager recorded one.
    pub last_exit: Option<i32>,
    /// Raw status output (`launchctl print` / `systemctl status` /
    /// `schtasks /Query /V /FO LIST`), for callers that want more detail.
    pub detail: String,
}

/// Fields extracted from a platform's raw status output.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ParsedStatus {
    pub pid: Option<u32>,
    pub last_exit: Option<i32>,
}

/// Write (or overwrite) the service definition and return its path.
pub fn install(spec: &ServiceSpec) -> Result<ServiceAction> {
    match current_platform()? {
        Platform::Launchd => launchd::install(spec),
        Platform::Systemd => systemd::install(spec),
        Platform::Schtasks => schtasks::install(spec),
    }
}

/// Stop the service (best effort), disable autostart and delete the definition.
///
/// Idempotent: unregistering an already-absent service succeeds.
pub fn uninstall(spec: &ServiceSpec) -> Result<ServiceAction> {
    match current_platform()? {
        Platform::Launchd => launchd::uninstall(spec),
        Platform::Systemd => systemd::uninstall(spec),
        Platform::Schtasks => schtasks::uninstall(spec),
    }
}

/// Start the service and enable autostart.
pub fn start(spec: &ServiceSpec) -> Result<ServiceAction> {
    match current_platform()? {
        Platform::Launchd => launchd::start(spec),
        Platform::Systemd => systemd::start(spec),
        Platform::Schtasks => schtasks::start(spec),
    }
}

/// Stop the service **and** turn off autostart.
///
/// This matches `runServiceStop` in the TypeScript CLI: `bootout` / `stop`
/// alone is session-scoped, and a definition with `RunAtLoad=true` (`WantedBy`
/// for systemd) would bring the daemon back at the next login.
pub fn stop(spec: &ServiceSpec) -> Result<ServiceAction> {
    match current_platform()? {
        Platform::Launchd => launchd::stop(spec),
        Platform::Systemd => systemd::stop(spec),
        Platform::Schtasks => schtasks::stop(spec),
    }
}

/// Bounce a service in place, keeping autostart enabled.
pub fn restart(spec: &ServiceSpec) -> Result<ServiceAction> {
    match current_platform()? {
        Platform::Launchd => launchd::restart(spec),
        Platform::Systemd => systemd::restart(spec),
        Platform::Schtasks => schtasks::restart(spec),
    }
}

/// Query the service manager for install/running state.
pub fn status(spec: &ServiceSpec) -> Result<ServiceStatus> {
    match current_platform()? {
        Platform::Launchd => launchd::status(spec),
        Platform::Systemd => systemd::status(spec),
        Platform::Schtasks => schtasks::status(spec),
    }
}

/// Whether a service definition exists on disk / is registered.
pub fn is_installed(spec: &ServiceSpec) -> Result<bool> {
    match current_platform()? {
        Platform::Launchd => launchd::is_installed(spec),
        Platform::Systemd => systemd::is_installed(spec),
        Platform::Schtasks => schtasks::is_installed(spec),
    }
}

/// Where [`install`] writes the definition / [`uninstall`] removes it from.
///
/// Windows has no single definition file: this returns the launcher `.cmd`,
/// while the task registration itself is named by [`windows_task_name`].
pub fn install_path(spec: &ServiceSpec) -> Result<PathBuf> {
    match current_platform()? {
        Platform::Launchd => launchd::default_plist_path(spec),
        Platform::Systemd => systemd::default_unit_path(spec),
        Platform::Schtasks => Ok(schtasks::launcher_cmd_path(spec)),
    }
}

// ---------------------------------------------------------------------------
// Process execution
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct Output {
    pub(crate) ok: bool,
    pub(crate) code: Option<i32>,
    pub(crate) stdout: String,
    pub(crate) stderr: String,
}

/// Run an external command without a shell and capture its output.
pub(crate) fn run(program: &str, args: &[String]) -> Result<Output> {
    let rendered = render_command(program, args);
    let output = Command::new(program)
        .args(args)
        .output()
        .with_context(|| format!("failed to execute `{rendered}`"))?;
    Ok(Output {
        ok: output.status.success(),
        code: output.status.code(),
        stdout: String::from_utf8_lossy(&output.stdout).into_owned(),
        stderr: String::from_utf8_lossy(&output.stderr).into_owned(),
    })
}

fn render_command(program: &str, args: &[String]) -> String {
    let mut rendered = program.to_string();
    for arg in args {
        rendered.push(' ');
        rendered.push_str(arg);
    }
    rendered
}

// ---------------------------------------------------------------------------
// Small filesystem helpers shared by the platform modules
// ---------------------------------------------------------------------------

pub(crate) fn write_service_file(path: &Path, contents: &str) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("cannot create {}", parent.display()))?;
    }
    fs::write(path, contents).with_context(|| format!("cannot write {}", path.display()))
}

/// Remove a definition file. Returns `false` when it was already absent.
pub(crate) fn remove_service_file(path: &Path) -> Result<bool> {
    match fs::remove_file(path) {
        Ok(()) => Ok(true),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(err) => Err(err).with_context(|| format!("cannot remove {}", path.display())),
    }
}

/// Create the daemon log directory before the service manager opens the files.
pub(crate) fn prepare_log_dirs(spec: &ServiceSpec) -> Result<()> {
    for log in [&spec.stdout_log, &spec.stderr_log] {
        if let Some(parent) = log.parent() {
            fs::create_dir_all(parent)
                .with_context(|| format!("cannot create {}", parent.display()))?;
        }
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Misc helpers
// ---------------------------------------------------------------------------

/// Current user id for launchd's `gui/<uid>` domain.
pub fn current_uid() -> u32 {
    #[cfg(unix)]
    {
        // SAFETY: getuid(2) has no preconditions and cannot fail.
        unsafe { libc::getuid() }
    }
    #[cfg(not(unix))]
    {
        0
    }
}

pub(crate) fn display_path(path: &Path) -> String {
    path.to_string_lossy().into_owned()
}

pub(crate) fn xml_escape(value: &str) -> String {
    let mut escaped = String::with_capacity(value.len());
    for ch in value.chars() {
        match ch {
            '&' => escaped.push_str("&amp;"),
            '<' => escaped.push_str("&lt;"),
            '>' => escaped.push_str("&gt;"),
            '"' => escaped.push_str("&quot;"),
            _ => escaped.push(ch),
        }
    }
    escaped
}

/// Value after the first occurrence of `marker` on a line (`pid = 123`).
pub(crate) fn value_after<'a>(line: &'a str, marker: &str) -> Option<&'a str> {
    let line = line.trim();
    let index = line.find(marker)?;
    Some(line[index + marker.len()..].trim())
}

/// Value after a `Field: value` line, case-insensitively (`Main PID: 123`).
pub(crate) fn field_after_colon<'a>(line: &'a str, field: &str) -> Option<&'a str> {
    let (key, value) = line.trim().split_once(':')?;
    key.trim()
        .eq_ignore_ascii_case(field)
        .then_some(value.trim())
}

pub(crate) fn parse_u32_prefix(value: &str) -> Option<u32> {
    let digits: String = value.chars().take_while(char::is_ascii_digit).collect();
    if digits.is_empty() {
        None
    } else {
        digits.parse().ok()
    }
}

pub(crate) fn parse_i32_prefix(value: &str) -> Option<i32> {
    let value = value.trim();
    let (sign, body) = match value.strip_prefix('-') {
        Some(body) => ("-", body),
        None => ("", value),
    };
    let digits: String = body.chars().take_while(char::is_ascii_digit).collect();
    if digits.is_empty() {
        None
    } else {
        format!("{sign}{digits}").parse().ok()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn spec() -> ServiceSpec {
        ServiceSpec::for_profile("claude", "/opt/larkline", "/home/u/.larkline")
            .unwrap()
            .with_env("PATH", "/usr/local/bin:/usr/bin")
    }

    #[test]
    fn platform_dispatch_mapping() {
        assert_eq!(Platform::for_os("macos"), Some(Platform::Launchd));
        assert_eq!(Platform::for_os("linux"), Some(Platform::Systemd));
        assert_eq!(Platform::for_os("windows"), Some(Platform::Schtasks));
        assert_eq!(Platform::for_os("darwin"), Some(Platform::Launchd));
        assert_eq!(Platform::for_os("freebsd"), None);
        assert_eq!(Platform::for_os(""), None);
        assert_eq!(Platform::for_os(" Linux "), Some(Platform::Systemd));
        assert_eq!(Platform::current(), Platform::for_os(std::env::consts::OS));
    }

    #[test]
    fn platform_names_are_stable() {
        assert_eq!(Platform::Launchd.name(), "launchd (macOS)");
        assert_eq!(Platform::Systemd.name(), "systemd (Linux user)");
        assert_eq!(Platform::Schtasks.name(), "Task Scheduler (Windows)");
    }

    #[test]
    fn ascii_service_ids_pass_through() {
        assert_eq!(service_profile_id("claude").unwrap(), "claude");
        assert_eq!(service_profile_id("bot-2").unwrap(), "bot-2");
        assert_eq!(service_profile_id("a.b_c").unwrap(), "a.b_c");
    }

    #[test]
    fn service_ids_reject_empty_and_dots() {
        assert!(service_profile_id("").is_err());
        assert!(service_profile_id("   ").is_err());
        assert!(service_profile_id(".").is_err());
        assert!(service_profile_id("..").is_err());
    }

    #[test]
    fn service_ids_sanitize_like_typescript() {
        // sha1("小助手") = d78a12b1e4db6e1481cee4429b52268c1039265d
        assert_eq!(service_profile_id("小助手").unwrap(), "profile-d78a12b1");
        // sha1("助手 bot") = 4759eec472bed07f7e826a75ac9df54fe579ae31
        assert_eq!(service_profile_id("助手 bot").unwrap(), "bot-4759eec4");
        let expected = format!("a-b-c-{}", &sha1_hex(b"a/b c")[..8]);
        assert_eq!(service_profile_id("a/b c").unwrap(), expected);
    }

    #[test]
    fn service_id_base_is_truncated_to_24_chars() {
        let raw = format!("{}小", "a".repeat(30));
        let id = service_profile_id(&raw).unwrap();
        assert!(id.starts_with(&"a".repeat(24)));
        assert!(!id.starts_with(&"a".repeat(25)));
        assert_eq!(id.len(), 24 + 1 + 8);
    }

    #[test]
    fn sha1_matches_known_vectors() {
        assert_eq!(sha1_hex(b""), "da39a3ee5e6b4b0d3255bfef95601890afd80709");
        assert_eq!(sha1_hex(b"abc"), "a9993e364706816aba3e25717850c26c9cd0d89d");
    }

    #[test]
    fn profile_spec_shape() {
        let spec = spec();
        let root = std::path::absolute("/home/u/.larkline").unwrap();
        assert_eq!(spec.id, "claude");
        assert_eq!(spec.service_id, "claude");
        assert_eq!(spec.label, "ai.larkline.bot.claude");
        assert_eq!(spec.name, "larkline.bot.claude");
        assert_eq!(spec.args, ["run", "--profile", "claude"]);
        assert_eq!(spec.program, std::path::absolute("/opt/larkline").unwrap());
        assert_eq!(
            spec.env_value("LARKLINE_HOME"),
            Some(root.to_string_lossy().as_ref())
        );
        assert_eq!(spec.env_value("LARKLINE_PROFILE"), Some("claude"));
        assert_eq!(
            spec.stdout_log,
            root.join("profiles/claude/logs/daemon/daemon-stdout.log")
        );
        assert_eq!(
            spec.stderr_log,
            root.join("profiles/claude/logs/daemon/daemon-stderr.log")
        );
        assert!(spec.working_dir.is_none());
    }

    #[test]
    fn supervisor_spec_shape() {
        let root = std::path::absolute("/home/u/.larkline").unwrap();
        let spec = ServiceSpec::for_supervisor(
            "/opt/larkline",
            vec!["run".to_string(), "--web-ui".to_string()],
            root.clone(),
        )
        .unwrap();
        assert_eq!(spec.id, SUPERVISOR_SERVICE_ID);
        assert_eq!(spec.label, "ai.larkline.bot.supervisor");
        assert_eq!(spec.name, "larkline.bot.supervisor");
        assert_eq!(spec.args, ["run", "--web-ui"]);
        assert_eq!(spec.env_value("LARKLINE_PROFILE"), None);
        assert_eq!(spec.description, "larkline supervisor");
        assert_eq!(
            spec.stdout_log,
            root.join("profiles/supervisor/logs/daemon/daemon-stdout.log")
        );
    }

    #[test]
    fn with_env_replaces_in_place_and_appends() {
        let spec = spec();
        assert_eq!(spec.env_value("PATH"), Some("/usr/local/bin:/usr/bin"));
        let spec = spec.with_env("PATH", "/bin").with_env("EXTRA", "1");
        assert_eq!(spec.env_value("PATH"), Some("/bin"));
        assert_eq!(spec.env_value("EXTRA"), Some("1"));
        let path_positions = spec.env.iter().filter(|(key, _)| key == "PATH").count();
        assert_eq!(path_positions, 1);
    }

    #[test]
    fn log_paths_use_the_sanitized_service_id() {
        let root = std::path::absolute("/home/u/.larkline").unwrap();
        let spec = ServiceSpec::for_profile("小助手", "/opt/larkline", root.clone()).unwrap();
        assert_eq!(spec.service_id, "profile-d78a12b1");
        assert_eq!(
            spec.stdout_log,
            root.join("profiles/profile-d78a12b1/logs/daemon/daemon-stdout.log")
        );
    }

    #[test]
    fn unknown_os_has_no_platform() {
        assert!(Platform::for_os("aix").is_none());
        assert_eq!(
            launch_agent_label("claude"),
            "ai.larkline.bot.claude".to_string()
        );
        assert_eq!(
            systemd_unit_name("claude"),
            "larkline.bot.claude.service".to_string()
        );
        assert_eq!(
            windows_task_name("claude"),
            "Larkline.Bot.claude".to_string()
        );
    }
}
