//! macOS `launchd` user agents.
//!
//! One plist per service under `~/Library/LaunchAgents/`. The job is loaded
//! into the per-user `gui/<uid>` domain, so no `sudo` is involved; root would
//! target a different, system-wide domain and would not even see this plist.
//!
//! Semantics ported from `src/daemon/launchd.ts` and the launchd adapter in
//! `src/daemon/service-adapter.ts`:
//!
//! * `install` writes the plist (and log dirs);
//! * `start` enables the job first (a previous `stop` leaves it disabled in
//!   launchd's override database, where it would stay dead through bootstrap),
//!   unloads a stale registration, then bootstraps the fresh definition;
//! * `stop` boots out **and** disables, because `bootout` alone is
//!   session-scoped: `RunAtLoad=true` brings the job back at the next login;
//! * `uninstall` boots out, disables and deletes the plist.

use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use anyhow::{Context, Result};

use super::{
    ParsedStatus, Platform, ServiceAction, ServiceSpec, ServiceStatus, current_uid, display_path,
    home_dir, launch_agent_plist_path, parse_i32_prefix, parse_u32_prefix, prepare_log_dirs,
    remove_service_file, run, value_after, write_service_file, xml_escape,
};

/// Pure path helper: the plist path under an explicit home directory.
pub fn plist_path(home: &Path, spec: &ServiceSpec) -> PathBuf {
    launch_agent_plist_path(home, &spec.service_id)
}

/// Resolve `~/Library/LaunchAgents/...` for the current user.
pub fn default_plist_path(spec: &ServiceSpec) -> Result<PathBuf> {
    let home = home_dir().context("cannot locate the user home directory (HOME is unset)")?;
    Ok(plist_path(&home, spec))
}

/// Render the launchd property list for `spec`.
///
/// Pure: no filesystem, environment or command access. The shape follows the
/// TypeScript `buildPlist`: `RunAtLoad` + `KeepAlive` for restart semantics,
/// explicit stdout/stderr paths, and the captured environment (launchd starts
/// jobs with a minimal PATH).
pub fn render_plist(spec: &ServiceSpec) -> String {
    let mut plist = String::new();
    plist.push_str("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n");
    plist.push_str(
        "<!DOCTYPE plist PUBLIC \"-//Apple//DTD PLIST 1.0//EN\" \
         \"http://www.apple.com/DTDs/PropertyList-1.0.dtd\">\n",
    );
    plist.push_str("<plist version=\"1.0\">\n<dict>\n");

    plist.push_str("    <key>Label</key>\n");
    plist.push_str(&format!(
        "    <string>{}</string>\n",
        xml_escape(&spec.label)
    ));

    plist.push_str("    <key>ProgramArguments</key>\n    <array>\n");
    plist.push_str(&format!(
        "        <string>{}</string>\n",
        xml_escape(&display_path(&spec.program))
    ));
    for arg in &spec.args {
        plist.push_str(&format!("        <string>{}</string>\n", xml_escape(arg)));
    }
    plist.push_str("    </array>\n");

    plist.push_str("    <key>RunAtLoad</key>\n    <true/>\n");
    plist.push_str("    <key>KeepAlive</key>\n    <true/>\n");

    plist.push_str("    <key>StandardOutPath</key>\n");
    plist.push_str(&format!(
        "    <string>{}</string>\n",
        xml_escape(&display_path(&spec.stdout_log))
    ));
    plist.push_str("    <key>StandardErrorPath</key>\n");
    plist.push_str(&format!(
        "    <string>{}</string>\n",
        xml_escape(&display_path(&spec.stderr_log))
    ));

    if let Some(working_dir) = &spec.working_dir {
        plist.push_str("    <key>WorkingDirectory</key>\n");
        plist.push_str(&format!(
            "    <string>{}</string>\n",
            xml_escape(&display_path(working_dir))
        ));
    }

    plist.push_str("    <key>EnvironmentVariables</key>\n    <dict>\n");
    for (key, value) in &spec.env {
        plist.push_str(&format!(
            "        <key>{}</key>\n        <string>{}</string>\n",
            xml_escape(key),
            xml_escape(value)
        ));
    }
    plist.push_str("    </dict>\n");

    plist.push_str("</dict>\n</plist>\n");
    plist
}

// ---------------------------------------------------------------------------
// launchctl argv builders (pure)
// ---------------------------------------------------------------------------

fn service_target(uid: u32, label: &str) -> String {
    format!("gui/{uid}/{label}")
}

/// `launchctl bootstrap gui/<uid> <plist>`.
pub fn bootstrap_args(uid: u32, plist: &Path) -> Vec<String> {
    vec![
        "bootstrap".to_string(),
        format!("gui/{uid}"),
        display_path(plist),
    ]
}

/// `launchctl bootout gui/<uid>/<label>`.
pub fn bootout_args(uid: u32, label: &str) -> Vec<String> {
    vec!["bootout".to_string(), service_target(uid, label)]
}

/// `launchctl enable gui/<uid>/<label>`.
pub fn enable_args(uid: u32, label: &str) -> Vec<String> {
    vec!["enable".to_string(), service_target(uid, label)]
}

/// `launchctl disable gui/<uid>/<label>`.
pub fn disable_args(uid: u32, label: &str) -> Vec<String> {
    vec!["disable".to_string(), service_target(uid, label)]
}

/// `launchctl kickstart -k gui/<uid>/<label>`.
pub fn kickstart_args(uid: u32, label: &str) -> Vec<String> {
    vec![
        "kickstart".to_string(),
        "-k".to_string(),
        service_target(uid, label),
    ]
}

/// `launchctl print gui/<uid>/<label>`.
pub fn print_args(uid: u32, label: &str) -> Vec<String> {
    vec!["print".to_string(), service_target(uid, label)]
}

// ---------------------------------------------------------------------------
// Status parsing (pure)
// ---------------------------------------------------------------------------

/// Extract the pid (`pid = 123`) and last exit code (`last exit code = 1`)
/// from `launchctl print` output.
pub fn parse_status(text: &str) -> ParsedStatus {
    let mut parsed = ParsedStatus::default();
    for line in text.lines() {
        if parsed.pid.is_none() {
            if let Some(value) = value_after(line, "pid = ") {
                parsed.pid = parse_u32_prefix(value);
            }
        }
        if parsed.last_exit.is_none() {
            if let Some(value) = value_after(line, "last exit code = ") {
                parsed.last_exit = parse_i32_prefix(value);
            }
        }
    }
    parsed
}

// ---------------------------------------------------------------------------
// Runtime
// ---------------------------------------------------------------------------

fn is_loaded(uid: u32, label: &str) -> bool {
    matches!(run("launchctl", &print_args(uid, label)), Ok(output) if output.ok)
}

/// Poll until launchd really unloads the job.
///
/// `launchctl bootout` returns synchronously but the unload is asynchronous:
/// bootstrapping during that window fails with `Bootstrap failed: 5:
/// Input/output error`.
pub fn wait_until_unloaded(uid: u32, label: &str, timeout: Duration) -> bool {
    let deadline = Instant::now() + timeout;
    loop {
        if !is_loaded(uid, label) {
            return true;
        }
        if Instant::now() >= deadline {
            return false;
        }
        std::thread::sleep(Duration::from_millis(200));
    }
}

/// Write the plist and ensure the log directory exists.
pub fn install(spec: &ServiceSpec) -> Result<ServiceAction> {
    let path = default_plist_path(spec)?;
    write_service_file(&path, &render_plist(spec))?;
    prepare_log_dirs(spec)?;
    Ok(ServiceAction::ok(
        Platform::Launchd,
        format!("wrote {}", path.display()),
    ))
}

/// Stop (if loaded), disable autostart, delete the plist. Idempotent.
pub fn uninstall(spec: &ServiceSpec) -> Result<ServiceAction> {
    let uid = current_uid();
    let path = default_plist_path(spec)?;
    let mut stdout = String::new();
    let mut stderr = String::new();

    if is_loaded(uid, &spec.label) {
        let output = run("launchctl", &bootout_args(uid, &spec.label))?;
        stdout.push_str(&output.stdout);
        stderr.push_str(&output.stderr);
    }
    let disabled = run("launchctl", &disable_args(uid, &spec.label))?;
    stdout.push_str(&disabled.stdout);
    stderr.push_str(&disabled.stderr);

    let removed = remove_service_file(&path)?;
    Ok(ServiceAction {
        platform: Platform::Launchd,
        // Unregistering is best effort before the file deletion; the user
        // visible effect is that the definition is gone.
        ok: true,
        code: None,
        detail: format!(
            "{} {}",
            if removed { "removed" } else { "already gone:" },
            path.display()
        ),
        stdout,
        stderr,
    })
}

/// Enable autostart and (re)load the current plist.
pub fn start(spec: &ServiceSpec) -> Result<ServiceAction> {
    let uid = current_uid();
    let path = default_plist_path(spec)?;

    // A previous `stop` disabled the job in launchd's override DB; bootstrap
    // alone would leave it dead.
    let _ = run("launchctl", &enable_args(uid, &spec.label));

    // `bootstrap` refuses to load over an existing registration, and launchd
    // does not re-read the plist on kickstart. Unload first so `start` always
    // runs the definition that `install` just wrote.
    if is_loaded(uid, &spec.label) {
        let _ = run("launchctl", &bootout_args(uid, &spec.label));
        let _ = wait_until_unloaded(uid, &spec.label, Duration::from_secs(5));
    }

    let output = run("launchctl", &bootstrap_args(uid, &path))?;
    Ok(ServiceAction::from_output(
        Platform::Launchd,
        format!("launchctl bootstrap gui/{uid} {}", path.display()),
        output,
    ))
}

/// Stop the job and disable autostart.
pub fn stop(spec: &ServiceSpec) -> Result<ServiceAction> {
    let uid = current_uid();
    let mut stdout = String::new();
    let mut stderr = String::new();
    let mut ok = true;

    // `bootout` on an already-unloaded job is an error; treat that as the
    // idempotent success the adapter contract promises.
    if is_loaded(uid, &spec.label) {
        let boot = run("launchctl", &bootout_args(uid, &spec.label))?;
        ok &= boot.ok;
        stdout.push_str(&boot.stdout);
        stderr.push_str(&boot.stderr);
    }

    let disabled = run("launchctl", &disable_args(uid, &spec.label))?;
    ok &= disabled.ok;
    stdout.push_str(&disabled.stdout);
    stderr.push_str(&disabled.stderr);

    Ok(ServiceAction {
        platform: Platform::Launchd,
        ok,
        code: None,
        detail: format!("launchctl bootout + disable {}", spec.label),
        stdout,
        stderr,
    })
}

/// Kill the running instance and start a new one (keeps autostart enabled).
pub fn restart(spec: &ServiceSpec) -> Result<ServiceAction> {
    let uid = current_uid();
    let _ = run("launchctl", &enable_args(uid, &spec.label));
    let args = if is_loaded(uid, &spec.label) {
        kickstart_args(uid, &spec.label)
    } else {
        bootstrap_args(uid, &default_plist_path(spec)?)
    };
    let output = run("launchctl", &args)?;
    Ok(ServiceAction::from_output(
        Platform::Launchd,
        format!("launchctl {}", args.join(" ")),
        output,
    ))
}

/// Query the launchd domain for this service.
pub fn status(spec: &ServiceSpec) -> Result<ServiceStatus> {
    let uid = current_uid();
    let path = default_plist_path(spec)?;
    let installed = path.exists();

    let output = run("launchctl", &print_args(uid, &spec.label))?;
    let running = output.ok;
    let parsed = parse_status(&output.stdout);
    let detail = if output.stdout.trim().is_empty() {
        output.stderr
    } else {
        output.stdout
    };

    Ok(ServiceStatus {
        platform: Platform::Launchd,
        definition_path: path,
        service_name: spec.label.clone(),
        installed,
        running,
        pid: if running { parsed.pid } else { None },
        last_exit: parsed.last_exit,
        detail,
    })
}

/// Whether the plist exists on disk.
pub fn is_installed(spec: &ServiceSpec) -> Result<bool> {
    Ok(default_plist_path(spec)?.exists())
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
    fn renders_key_plist_fields() {
        let spec = spec();
        let plist = render_plist(&spec);

        assert!(plist.starts_with("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"));
        assert!(plist.contains("<!DOCTYPE plist PUBLIC"));
        assert!(plist.contains("<plist version=\"1.0\">"));
        assert!(plist.contains("<key>Label</key>\n    <string>ai.larkline.bot.claude</string>"));
        assert!(plist.contains("<key>ProgramArguments</key>\n    <array>"));
        assert!(plist.contains("        <string>/opt/larkline</string>"));
        assert!(plist.contains("        <string>run</string>"));
        assert!(plist.contains("        <string>--profile</string>"));
        assert!(plist.contains("        <string>claude</string>"));
        assert!(plist.contains("<key>RunAtLoad</key>\n    <true/>"));
        assert!(plist.contains("<key>KeepAlive</key>\n    <true/>"));
        assert!(plist.contains(&format!(
            "<key>StandardOutPath</key>\n    <string>{}</string>",
            spec.stdout_log.display()
        )));
        assert!(plist.contains(&format!(
            "<key>StandardErrorPath</key>\n    <string>{}</string>",
            spec.stderr_log.display()
        )));
        assert!(plist.contains("<key>LARKLINE_HOME</key>"));
        assert!(plist.contains("<string>/home/u/.larkline</string>"));
        assert!(plist.contains("<key>LARKLINE_PROFILE</key>"));
        assert!(plist.contains("<key>PATH</key>"));
        assert!(!plist.contains("<key>WorkingDirectory</key>"));
        assert!(plist.ends_with("</plist>\n"));
    }

    #[test]
    fn renders_working_directory_when_set() {
        let spec = spec().with_working_dir("/home/u/work");
        let plist = render_plist(&spec);
        assert!(plist.contains("<key>WorkingDirectory</key>\n    <string>/home/u/work</string>"));
    }

    #[test]
    fn xml_escapes_program_and_environment() {
        let spec = ServiceSpec::for_profile("claude", "/opt/a&b<larkline>", "/home/u/a&b")
            .unwrap()
            .with_env("CUSTOM", "x\"y");
        let plist = render_plist(&spec);
        assert!(plist.contains("&amp;"));
        assert!(plist.contains("&lt;"));
        assert!(plist.contains("&quot;"));
        assert!(!plist.contains("/opt/a&b<larkline>"));
    }

    #[test]
    fn plist_path_is_a_user_launch_agent() {
        assert_eq!(
            plist_path(Path::new("/Users/me"), &spec()),
            PathBuf::from("/Users/me/Library/LaunchAgents/ai.larkline.bot.claude.plist")
        );
    }

    #[test]
    fn launchctl_args_use_the_gui_domain() {
        let plist = PathBuf::from("/Users/me/Library/LaunchAgents/ai.larkline.bot.claude.plist");
        let target = "gui/501/ai.larkline.bot.claude";

        assert_eq!(
            bootstrap_args(501, &plist),
            vec![
                "bootstrap",
                "gui/501",
                "/Users/me/Library/LaunchAgents/ai.larkline.bot.claude.plist"
            ]
        );
        assert_eq!(
            bootout_args(501, "ai.larkline.bot.claude"),
            vec!["bootout", target]
        );
        assert_eq!(
            enable_args(501, "ai.larkline.bot.claude"),
            vec!["enable", target]
        );
        assert_eq!(
            disable_args(501, "ai.larkline.bot.claude"),
            vec!["disable", target]
        );
        assert_eq!(
            kickstart_args(501, "ai.larkline.bot.claude"),
            vec!["kickstart", "-k", target]
        );
        assert_eq!(
            print_args(501, "ai.larkline.bot.claude"),
            vec!["print", target]
        );
    }

    #[test]
    fn parses_pid_and_last_exit_from_print_output() {
        let text = r#"gui/501/ai.larkline.bot.claude = {
	active count = 1
	path = /Users/me/Library/LaunchAgents/ai.larkline.bot.claude.plist
	state = running
	program = /opt/larkline
	pid = 4242
	last exit code = 1
}
"#;
        let parsed = parse_status(text);
        assert_eq!(parsed.pid, Some(4242));
        assert_eq!(parsed.last_exit, Some(1));
    }

    #[test]
    fn parse_status_tolerates_missing_fields() {
        let parsed = parse_status("state = exited\nlast exit code = -1\n");
        assert_eq!(parsed.pid, None);
        assert_eq!(parsed.last_exit, Some(-1));
    }
}
