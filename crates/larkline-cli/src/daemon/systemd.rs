//! Linux `systemd --user` units.
//!
//! One unit per service under `$XDG_CONFIG_HOME/systemd/user/` (default
//! `~/.config/systemd/user/`), managed with `systemctl --user`. No root.
//!
//! Semantics ported from `src/daemon/systemd.ts` and the systemd adapter in
//! `src/daemon/service-adapter.ts`:
//!
//! * `install` writes the unit and reloads the daemon;
//! * `start` is `enable --now` (autostart at login + start now);
//! * `stop` is `disable --now` (stop + remove autostart);
//! * `uninstall` disables, deletes the unit and reloads.
//!
//! `Restart=always` + `RestartSec=5` mirrors launchd's `KeepAlive=true` with a
//! back-off, so a crash loop does not pin the CPU. `WantedBy=default.target`
//! makes `enable` autostart the unit at login. Note that user units only
//! survive logout when `loginctl enable-linger <user>` is set; the CLI's
//! success message mentions it (see `runServiceStart` in the TypeScript
//! implementation).

use std::path::{Path, PathBuf};

use anyhow::{Context, Result};

use super::{
    ParsedStatus, Platform, ServiceAction, ServiceSpec, ServiceStatus, display_path,
    field_after_colon, parse_i32_prefix, parse_u32_prefix, prepare_log_dirs, remove_service_file,
    run, systemd_unit_name, systemd_unit_path, write_service_file, xdg_config_home,
};

/// Pure path helper: the unit path under an explicit config home.
pub fn unit_path(config_home: &Path, spec: &ServiceSpec) -> PathBuf {
    systemd_unit_path(config_home, &spec.service_id)
}

/// `larkline.bot.<id>.service`.
pub fn unit_name(spec: &ServiceSpec) -> String {
    systemd_unit_name(&spec.service_id)
}

/// Resolve `$XDG_CONFIG_HOME/systemd/user/...` for the current user.
pub fn default_unit_path(spec: &ServiceSpec) -> Result<PathBuf> {
    let config_home =
        xdg_config_home().context("cannot locate the XDG config directory (HOME is unset)")?;
    Ok(unit_path(&config_home, spec))
}

/// Quote one `ExecStart` / environment token for systemd.
///
/// Double quotes and backslashes are escaped; `%` is doubled because systemd
/// performs specifier expansion inside unit files.
fn quote(value: &str) -> String {
    let mut quoted = String::with_capacity(value.len() + 2);
    quoted.push('"');
    for ch in value.chars() {
        match ch {
            '\\' => quoted.push_str("\\\\"),
            '"' => quoted.push_str("\\\""),
            '%' => quoted.push_str("%%"),
            _ => quoted.push(ch),
        }
    }
    quoted.push('"');
    quoted
}

fn escape_environment(value: &str) -> String {
    let mut escaped = String::with_capacity(value.len());
    for ch in value.chars() {
        match ch {
            '\\' => escaped.push_str("\\\\"),
            '"' => escaped.push_str("\\\""),
            '%' => escaped.push_str("%%"),
            _ => escaped.push(ch),
        }
    }
    escaped
}

/// Render the systemd user unit for `spec`.
///
/// Pure: no filesystem, environment or command access.
pub fn render_unit(spec: &ServiceSpec) -> String {
    let mut unit = String::new();

    unit.push_str("[Unit]\n");
    unit.push_str(&format!(
        "Description={}\n",
        spec.description.replace('%', "%%")
    ));
    unit.push_str("After=network-online.target\n");
    unit.push_str("Wants=network-online.target\n");
    unit.push('\n');

    unit.push_str("[Service]\n");
    unit.push_str("Type=simple\n");
    unit.push_str("ExecStart=");
    unit.push_str(&quote(&display_path(&spec.program)));
    for arg in &spec.args {
        unit.push(' ');
        unit.push_str(&quote(arg));
    }
    unit.push('\n');
    unit.push_str("Restart=always\n");
    unit.push_str("RestartSec=5\n");
    if let Some(working_dir) = &spec.working_dir {
        unit.push_str(&format!(
            "WorkingDirectory={}\n",
            quote(&display_path(working_dir))
        ));
    }
    unit.push_str(&format!(
        "StandardOutput=append:{}\n",
        display_path(&spec.stdout_log)
    ));
    unit.push_str(&format!(
        "StandardError=append:{}\n",
        display_path(&spec.stderr_log)
    ));
    for (key, value) in &spec.env {
        unit.push_str(&format!(
            "Environment=\"{}={}\"\n",
            escape_environment(key),
            escape_environment(value)
        ));
    }
    unit.push('\n');

    unit.push_str("[Install]\n");
    unit.push_str("WantedBy=default.target\n");
    unit
}

// ---------------------------------------------------------------------------
// systemctl argv builders (pure)
// ---------------------------------------------------------------------------

fn user_args(args: &[&str]) -> Vec<String> {
    let mut argv = vec!["--user".to_string()];
    argv.extend(args.iter().map(|arg| (*arg).to_string()));
    argv
}

/// `systemctl --user daemon-reload`.
pub fn daemon_reload_args() -> Vec<String> {
    user_args(&["daemon-reload"])
}

/// `systemctl --user enable --now <unit>`.
pub fn enable_now_args(unit: &str) -> Vec<String> {
    user_args(&["enable", "--now", unit])
}

/// `systemctl --user disable --now <unit>`.
pub fn disable_now_args(unit: &str) -> Vec<String> {
    user_args(&["disable", "--now", unit])
}

/// `systemctl --user stop <unit>`.
pub fn stop_args(unit: &str) -> Vec<String> {
    user_args(&["stop", unit])
}

/// `systemctl --user restart <unit>`.
pub fn restart_args(unit: &str) -> Vec<String> {
    user_args(&["restart", unit])
}

/// `systemctl --user is-active <unit>`.
pub fn is_active_args(unit: &str) -> Vec<String> {
    user_args(&["is-active", unit])
}

/// `systemctl --user status <unit> --no-pager`.
pub fn status_args(unit: &str) -> Vec<String> {
    user_args(&["status", unit, "--no-pager"])
}

// ---------------------------------------------------------------------------
// Status parsing (pure)
// ---------------------------------------------------------------------------

/// Extract `Main PID:` and the `status=<n>` marker from `systemctl status`.
pub fn parse_status(text: &str) -> ParsedStatus {
    let mut parsed = ParsedStatus::default();
    for line in text.lines() {
        if parsed.pid.is_none() {
            if let Some(value) = field_after_colon(line, "Main PID") {
                parsed.pid = parse_u32_prefix(value);
            }
        }
        if parsed.last_exit.is_none() && line.contains("ExecStart=") && line.contains("status=") {
            if let Some((_, value)) = line.rsplit_once("status=") {
                parsed.last_exit = parse_i32_prefix(value);
            }
        }
    }
    parsed
}

// ---------------------------------------------------------------------------
// Runtime
// ---------------------------------------------------------------------------

fn is_active(spec: &ServiceSpec) -> Result<bool> {
    Ok(run("systemctl", &is_active_args(&unit_name(spec)))?.ok)
}

/// Write the unit and reload systemd.
pub fn install(spec: &ServiceSpec) -> Result<ServiceAction> {
    let path = default_unit_path(spec)?;
    write_service_file(&path, &render_unit(spec))?;
    prepare_log_dirs(spec)?;

    let mut action = ServiceAction::ok(Platform::Systemd, format!("wrote {}", path.display()));
    // The adapter ignores a failed daemon-reload: the unit file is on disk and
    // a later `systemctl --user` call (or login) picks it up. Keep that
    // behaviour but surface the diagnostic text.
    match run("systemctl", &daemon_reload_args()) {
        Ok(output) if output.ok => action.detail.push_str("; systemctl --user daemon-reload"),
        Ok(output) => {
            action
                .detail
                .push_str("; systemctl --user daemon-reload failed");
            action.stderr = output.stderr;
        }
        Err(error) => {
            action
                .detail
                .push_str(&format!("; systemctl --user daemon-reload failed: {error}"));
        }
    }
    Ok(action)
}

/// Disable autostart, delete the unit and reload systemd. Idempotent.
pub fn uninstall(spec: &ServiceSpec) -> Result<ServiceAction> {
    let path = default_unit_path(spec)?;
    let installed = path.exists();
    let mut stdout = String::new();
    let mut stderr = String::new();

    if installed {
        let disabled = run("systemctl", &disable_now_args(&unit_name(spec)))?;
        stdout.push_str(&disabled.stdout);
        stderr.push_str(&disabled.stderr);
    }
    let removed = remove_service_file(&path)?;
    let _ = run("systemctl", &daemon_reload_args());

    Ok(ServiceAction {
        platform: Platform::Systemd,
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

/// Enable autostart and start now.
pub fn start(spec: &ServiceSpec) -> Result<ServiceAction> {
    let unit = unit_name(spec);
    let output = run("systemctl", &enable_now_args(&unit))?;
    Ok(ServiceAction::from_output(
        Platform::Systemd,
        format!("systemctl --user enable --now {unit}"),
        output,
    ))
}

/// Disable autostart and stop now.
pub fn stop(spec: &ServiceSpec) -> Result<ServiceAction> {
    let unit = unit_name(spec);
    let output = run("systemctl", &disable_now_args(&unit))?;
    Ok(ServiceAction::from_output(
        Platform::Systemd,
        format!("systemctl --user disable --now {unit}"),
        output,
    ))
}

/// Restart the unit in place (autostart stays enabled).
pub fn restart(spec: &ServiceSpec) -> Result<ServiceAction> {
    let unit = unit_name(spec);
    let output = run("systemctl", &restart_args(&unit))?;
    Ok(ServiceAction::from_output(
        Platform::Systemd,
        format!("systemctl --user restart {unit}"),
        output,
    ))
}

/// Query `is-active` and, when active, `status` for the main pid.
pub fn status(spec: &ServiceSpec) -> Result<ServiceStatus> {
    let path = default_unit_path(spec)?;
    let installed = path.exists();
    let running = is_active(spec)?;

    let mut parsed = ParsedStatus::default();
    let detail = if running {
        let unit = unit_name(spec);
        let output = run("systemctl", &status_args(&unit))?;
        let text = if output.stdout.trim().is_empty() {
            output.stderr
        } else {
            output.stdout
        };
        parsed = parse_status(&text);
        text
    } else {
        String::new()
    };

    Ok(ServiceStatus {
        platform: Platform::Systemd,
        definition_path: path,
        service_name: unit_name(spec),
        installed,
        running,
        pid: parsed.pid,
        last_exit: parsed.last_exit,
        detail,
    })
}

/// Whether the unit file exists on disk.
pub fn is_installed(spec: &ServiceSpec) -> Result<bool> {
    Ok(default_unit_path(spec)?.exists())
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
    fn renders_key_unit_fields() {
        let spec = spec();
        let unit = render_unit(&spec);

        assert!(unit.contains("[Unit]\nDescription=larkline bot (claude)\n"));
        assert!(unit.contains("After=network-online.target"));
        assert!(unit.contains("Wants=network-online.target"));
        assert!(unit.contains("[Service]\nType=simple"));
        assert!(unit.contains("ExecStart=\"/opt/larkline\" \"run\" \"--profile\" \"claude\""));
        assert!(unit.contains("Restart=always"));
        assert!(unit.contains("RestartSec=5"));
        assert!(unit.contains(&format!(
            "StandardOutput=append:{}",
            spec.stdout_log.display()
        )));
        assert!(unit.contains(&format!(
            "StandardError=append:{}",
            spec.stderr_log.display()
        )));
        assert!(unit.contains("Environment=\"LARKLINE_HOME=/home/u/.larkline\""));
        assert!(unit.contains("Environment=\"LARKLINE_PROFILE=claude\""));
        assert!(unit.contains("Environment=\"PATH=/usr/local/bin:/usr/bin\""));
        assert!(unit.contains("[Install]\nWantedBy=default.target"));
        assert!(!unit.contains("WorkingDirectory="));
        assert!(unit.ends_with('\n'));
    }

    #[test]
    fn renders_working_directory_when_set() {
        let unit = render_unit(&spec().with_working_dir("/home/u/work dir"));
        assert!(unit.contains("WorkingDirectory=\"/home/u/work dir\""));
    }

    #[test]
    fn escapes_systemd_quotes_and_percent() {
        let spec = ServiceSpec::for_profile("claude", "/opt/my larkline", "/home/u/.larkline")
            .unwrap()
            .with_env("CUSTOM", "100%");
        let unit = render_unit(&spec);
        assert!(unit.contains("ExecStart=\"/opt/my larkline\" \"run\" \"--profile\" \"claude\""));
        assert!(unit.contains("Environment=\"CUSTOM=100%%\""));
    }

    #[test]
    fn unit_path_is_a_user_unit() {
        assert_eq!(
            unit_path(Path::new("/home/u/.config"), &spec()),
            PathBuf::from("/home/u/.config/systemd/user/larkline.bot.claude.service")
        );
        assert_eq!(unit_name(&spec()), "larkline.bot.claude.service");
    }

    #[test]
    fn systemctl_args_are_user_scoped() {
        let unit = "larkline.bot.claude.service";
        assert_eq!(daemon_reload_args(), vec!["--user", "daemon-reload"]);
        assert_eq!(
            enable_now_args(unit),
            vec!["--user", "enable", "--now", unit]
        );
        assert_eq!(
            disable_now_args(unit),
            vec!["--user", "disable", "--now", unit]
        );
        assert_eq!(stop_args(unit), vec!["--user", "stop", unit]);
        assert_eq!(restart_args(unit), vec!["--user", "restart", unit]);
        assert_eq!(is_active_args(unit), vec!["--user", "is-active", unit]);
        assert_eq!(
            status_args(unit),
            vec!["--user", "status", unit, "--no-pager"]
        );
    }

    #[test]
    fn parses_main_pid_and_exit_status() {
        let text = "\
● larkline.bot.claude.service - larkline bot (claude)
     Loaded: loaded (/home/u/.config/systemd/user/larkline.bot.claude.service; enabled)
     Active: active (running) since Wed 2026-01-01 00:00:00 UTC
   Main PID: 4321 (larkline)
      Tasks: 4
     CGroup: /user.slice/...
             └─4321 /opt/larkline run --profile claude
";
        let parsed = parse_status(text);
        assert_eq!(parsed.pid, Some(4321));
        assert_eq!(parsed.last_exit, None);
    }

    #[test]
    fn parses_last_exit_from_process_line() {
        let text = "\
     Active: failed (Result: exit-code)
 Process: 4321 ExecStart=/opt/larkline run --profile claude (code=exited, status=1/FAILURE)
   Main PID: 4321 (code=exited, status=1/FAILURE)
";
        let parsed = parse_status(text);
        assert_eq!(parsed.pid, Some(4321));
        assert_eq!(parsed.last_exit, Some(1));
    }
}
