//! Windows Task Scheduler (`schtasks`).
//!
//! Windows has no single service-definition file. Following the TypeScript
//! bridge, `install` writes a `.cmd` launcher (stdout/stderr redirection + a
//! `PATH` override) and registers a `ONLOGON` task that runs it with
//! `/RL LIMITED`, i.e. as the current user and without elevation.
//!
//! Semantics ported from `src/daemon/schtasks.ts` and the schtasks adapter in
//! `src/daemon/service-adapter.ts`:
//!
//! * `install` writes the launcher and `/Create /F` re-registers the task;
//! * `start` re-enables the task (a previous `stop` disabled it) and `/Run`s it;
//! * `stop` is `/End` + `/Change /Disable` so "stopped" survives a logon;
//! * `uninstall` ends, deletes and removes the launcher.
//!
//! [`render_task_xml`] renders the equivalent Task Scheduler XML definition
//! for callers that prefer `schtasks /Create /XML`; the runtime install path
//! keeps the TypeScript `/Create /SC ONLOGON /TR ...` argv.

use std::path::PathBuf;
use std::time::{Duration, Instant};

use anyhow::Result;

use super::{
    Platform, ServiceAction, ServiceSpec, ServiceStatus, display_path, field_after_colon,
    parse_i32_prefix, parse_u32_prefix, prepare_log_dirs, remove_service_file, run,
    windows_launcher_cmd_path, windows_task_name, write_service_file, xml_escape,
};

/// The scheduled task's name: `Larkline.Bot.<id>`.
pub fn task_name(spec: &ServiceSpec) -> String {
    windows_task_name(&spec.service_id)
}

/// The `.cmd` wrapper the task invokes.
pub fn launcher_cmd_path(spec: &ServiceSpec) -> PathBuf {
    windows_launcher_cmd_path(&spec.root_dir, &spec.service_id)
}

/// Render the batch wrapper that redirects output and sets the environment.
///
/// `set "VAR=value"` keeps values with spaces intact. `>>` / `2>>` append so
/// log history survives restarts. Lines end with CRLF, as a `.cmd` file
/// requires.
pub fn render_launcher_cmd(spec: &ServiceSpec) -> String {
    let mut lines = vec!["@echo off".to_string()];
    for (key, value) in &spec.env {
        lines.push(format!("set \"{key}={value}\""));
    }
    if let Some(working_dir) = &spec.working_dir {
        lines.push(format!("cd /d \"{}\"", display_path(working_dir)));
    }

    let mut command = format!("\"{}\"", display_path(&spec.program));
    for arg in &spec.args {
        command.push(' ');
        command.push('"');
        command.push_str(arg);
        command.push('"');
    }
    command.push_str(&format!(
        " >> \"{}\" 2>> \"{}\"",
        display_path(&spec.stdout_log),
        display_path(&spec.stderr_log)
    ));
    lines.push(command);

    let mut cmd = lines.join("\r\n");
    cmd.push_str("\r\n");
    cmd
}

/// Render the equivalent Task Scheduler XML definition.
///
/// Pure. Mirrors the task created by [`create_task_args`]: logon trigger,
/// interactive token, least privilege, no execution time limit. The action
/// goes through `cmd.exe /c` so the batch wrapper is executed by `cmd` even
/// when Task Scheduler launches it without a console.
pub fn render_task_xml(spec: &ServiceSpec) -> String {
    format!(
        r#"<?xml version="1.0" encoding="UTF-8"?>
<Task version="1.2" xmlns="http://schemas.microsoft.com/windows/2004/02/mit/task">
  <RegistrationInfo>
    <Description>{description}</Description>
  </RegistrationInfo>
  <Triggers>
    <LogonTrigger>
      <Enabled>true</Enabled>
    </LogonTrigger>
  </Triggers>
  <Principals>
    <Principal id="Author">
      <LogonType>InteractiveToken</LogonType>
      <RunLevel>LeastPrivilege</RunLevel>
    </Principal>
  </Principals>
  <Settings>
    <MultipleInstancesPolicy>IgnoreNew</MultipleInstancesPolicy>
    <DisallowStartIfOnBatteries>false</DisallowStartIfOnBatteries>
    <StopIfGoingOnBatteries>false</StopIfGoingOnBatteries>
    <StartWhenAvailable>true</StartWhenAvailable>
    <ExecutionTimeLimit>PT0S</ExecutionTimeLimit>
    <Enabled>true</Enabled>
  </Settings>
  <Actions Context="Author">
    <Exec>
      <Command>cmd.exe</Command>
      <Arguments>/c &quot;&quot;{launcher}&quot;&quot;</Arguments>
    </Exec>
  </Actions>
</Task>
"#,
        description = xml_escape(&spec.description),
        launcher = xml_escape(&display_path(&launcher_cmd_path(spec))),
    )
}

// ---------------------------------------------------------------------------
// schtasks argv builders (pure)
// ---------------------------------------------------------------------------

/// `schtasks /Create /F /SC ONLOGON /RL LIMITED /TN <name> /TR "<launcher>"`.
pub fn create_task_args(spec: &ServiceSpec) -> Vec<String> {
    vec![
        "/Create".to_string(),
        "/F".to_string(),
        "/SC".to_string(),
        "ONLOGON".to_string(),
        "/RL".to_string(),
        "LIMITED".to_string(),
        "/TN".to_string(),
        task_name(spec),
        "/TR".to_string(),
        format!("\"{}\"", display_path(&launcher_cmd_path(spec))),
    ]
}

/// `schtasks /Run /TN <name>`.
pub fn run_task_args(spec: &ServiceSpec) -> Vec<String> {
    vec!["/Run".to_string(), "/TN".to_string(), task_name(spec)]
}

/// `schtasks /End /TN <name>`.
pub fn end_task_args(spec: &ServiceSpec) -> Vec<String> {
    vec!["/End".to_string(), "/TN".to_string(), task_name(spec)]
}

/// `schtasks /Change /TN <name> /Disable`.
pub fn disable_task_args(spec: &ServiceSpec) -> Vec<String> {
    vec![
        "/Change".to_string(),
        "/TN".to_string(),
        task_name(spec),
        "/Disable".to_string(),
    ]
}

/// `schtasks /Change /TN <name> /Enable`.
pub fn enable_task_args(spec: &ServiceSpec) -> Vec<String> {
    vec![
        "/Change".to_string(),
        "/TN".to_string(),
        task_name(spec),
        "/Enable".to_string(),
    ]
}

/// `schtasks /Query /TN <name>`.
pub fn query_task_args(spec: &ServiceSpec) -> Vec<String> {
    vec!["/Query".to_string(), "/TN".to_string(), task_name(spec)]
}

/// `schtasks /Query /V /FO LIST /TN <name>`.
pub fn query_verbose_args(spec: &ServiceSpec) -> Vec<String> {
    vec![
        "/Query".to_string(),
        "/V".to_string(),
        "/FO".to_string(),
        "LIST".to_string(),
        "/TN".to_string(),
        task_name(spec),
    ]
}

/// `schtasks /Delete /F /TN <name>`.
pub fn delete_task_args(spec: &ServiceSpec) -> Vec<String> {
    vec![
        "/Delete".to_string(),
        "/F".to_string(),
        "/TN".to_string(),
        task_name(spec),
    ]
}

// ---------------------------------------------------------------------------
// Status parsing (pure)
// ---------------------------------------------------------------------------

/// Fields parsed from `schtasks /Query /V /FO LIST`.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct TaskState {
    pub running: bool,
    pub pid: Option<u32>,
    pub last_exit: Option<i32>,
}

/// Parse `Status:`, `Process ID:` and `Last Result:` from the verbose query.
///
/// Note: like the TypeScript implementation this matches the English field
/// names, so a localized Windows installation falls back to "not running".
pub fn parse_status(text: &str) -> TaskState {
    let mut state = TaskState::default();
    for line in text.lines() {
        if let Some(value) = field_after_colon(line, "Status") {
            if value.eq_ignore_ascii_case("running") {
                state.running = true;
            }
        }
        if state.pid.is_none() {
            if let Some(value) = field_after_colon(line, "Process ID") {
                state.pid = parse_u32_prefix(value);
            }
        }
        if state.last_exit.is_none() {
            if let Some(value) = field_after_colon(line, "Last Result") {
                state.last_exit = parse_i32_prefix(value);
            }
        }
    }
    state
}

// ---------------------------------------------------------------------------
// Runtime
// ---------------------------------------------------------------------------

/// Whether the task is registered (`/Query` exits 0).
pub fn is_registered(spec: &ServiceSpec) -> Result<bool> {
    Ok(run("schtasks", &query_task_args(spec))?.ok)
}

/// Whether the task is currently running.
pub fn is_running(spec: &ServiceSpec) -> Result<bool> {
    let output = run("schtasks", &query_verbose_args(spec))?;
    if !output.ok {
        return Ok(false);
    }
    Ok(parse_status(&output.stdout).running)
}

/// Poll until the task is no longer running (schtasks has no async stop).
pub fn wait_until_stopped(spec: &ServiceSpec, timeout: Duration) -> Result<bool> {
    let deadline = Instant::now() + timeout;
    loop {
        if !is_running(spec)? {
            return Ok(true);
        }
        if Instant::now() >= deadline {
            return Ok(false);
        }
        std::thread::sleep(Duration::from_millis(200));
    }
}

/// Write the launcher and create/replace the scheduled task.
pub fn install(spec: &ServiceSpec) -> Result<ServiceAction> {
    let path = launcher_cmd_path(spec);
    write_service_file(&path, &render_launcher_cmd(spec))?;
    prepare_log_dirs(spec)?;

    let name = task_name(spec);
    let output = run("schtasks", &create_task_args(spec))?;
    Ok(ServiceAction::from_output(
        Platform::Schtasks,
        format!("schtasks /Create /F /SC ONLOGON /TN {name}"),
        output,
    ))
}

/// End, delete and remove the launcher. Idempotent.
pub fn uninstall(spec: &ServiceSpec) -> Result<ServiceAction> {
    let mut stdout = String::new();
    let mut stderr = String::new();
    let mut ok = true;

    if is_registered(spec)? {
        // `/End` fails when the task is not running; that is not an error for
        // an unregister.
        if let Ok(output) = run("schtasks", &end_task_args(spec)) {
            stdout.push_str(&output.stdout);
            stderr.push_str(&output.stderr);
        }
        let deleted = run("schtasks", &delete_task_args(spec))?;
        ok = deleted.ok;
        stdout.push_str(&deleted.stdout);
        stderr.push_str(&deleted.stderr);
    }

    let path = launcher_cmd_path(spec);
    let removed = remove_service_file(&path)?;

    Ok(ServiceAction {
        platform: Platform::Schtasks,
        ok,
        code: None,
        detail: format!(
            "schtasks /Delete /F /TN {}; {} {}",
            task_name(spec),
            if removed { "removed" } else { "already gone:" },
            path.display()
        ),
        stdout,
        stderr,
    })
}

/// Re-enable and run the task now.
pub fn start(spec: &ServiceSpec) -> Result<ServiceAction> {
    let name = task_name(spec);
    // A disabled task refuses to run; a disabled flag is left behind by stop.
    let enabled = run("schtasks", &enable_task_args(spec))?;
    let output = run("schtasks", &run_task_args(spec))?;
    let mut action = ServiceAction::from_output(
        Platform::Schtasks,
        format!("schtasks /Change /Enable + /Run /TN {name}"),
        output,
    );
    if !enabled.ok && action.ok {
        // `/Enable` fails for a never-registered task; `/Run` would have
        // failed too, but keep the hint if it did not.
        action.detail.push_str(" (enable failed)");
    }
    Ok(action)
}

/// End the running instance and disable autostart.
///
/// Mirrors `endAndDisable` in the TypeScript module: the disable result wins
/// because preventing autostart is the user-visible effect; when both calls
/// fail the `/End` diagnostic is the more informative one.
pub fn stop(spec: &ServiceSpec) -> Result<ServiceAction> {
    let name = task_name(spec);
    let ended = run("schtasks", &end_task_args(spec))?;
    let disabled = run("schtasks", &disable_task_args(spec))?;

    // Same precedence as the TypeScript helper: a successful disable wins
    // (and a failed disable is still reported when the stop itself worked);
    // only when both fail is the `/End` diagnostic more informative.
    let primary = if disabled.ok || ended.ok {
        disabled
    } else {
        ended
    };
    Ok(ServiceAction::from_output(
        Platform::Schtasks,
        format!("schtasks /End + /Change /Disable /TN {name}"),
        primary,
    ))
}

/// End, wait, run (schtasks has no native restart).
pub fn restart(spec: &ServiceSpec) -> Result<ServiceAction> {
    let _ = run("schtasks", &end_task_args(spec));
    let _ = wait_until_stopped(spec, Duration::from_secs(5));
    let name = task_name(spec);
    let output = run("schtasks", &run_task_args(spec))?;
    Ok(ServiceAction::from_output(
        Platform::Schtasks,
        format!("schtasks /End + /Run /TN {name}"),
        output,
    ))
}

/// Query registration and run state.
pub fn status(spec: &ServiceSpec) -> Result<ServiceStatus> {
    let installed = is_registered(spec)?;
    let mut state = TaskState::default();
    let mut detail = String::new();

    if installed {
        let output = run("schtasks", &query_verbose_args(spec))?;
        if output.ok {
            state = parse_status(&output.stdout);
            detail = output.stdout;
        } else {
            detail = output.stderr;
        }
    }

    Ok(ServiceStatus {
        platform: Platform::Schtasks,
        definition_path: launcher_cmd_path(spec),
        service_name: task_name(spec),
        installed,
        running: state.running,
        pid: state.pid,
        last_exit: state.last_exit,
        detail,
    })
}

/// Whether the task is registered.
pub fn is_installed(spec: &ServiceSpec) -> Result<bool> {
    is_registered(spec)
}

#[cfg(test)]
mod tests {
    use super::*;

    // Rendering is path-agnostic: it prints whatever the spec carries, so the
    // tests use POSIX paths and run on any host.
    fn spec() -> ServiceSpec {
        ServiceSpec::for_profile("claude", "/opt/larkline", "/home/me/.larkline")
            .unwrap()
            .with_env("PATH", "/usr/local/bin:/usr/bin")
    }

    #[test]
    fn launcher_sets_env_cds_and_redirects() {
        let spec = spec().with_working_dir("/home/me/work");
        let cmd = render_launcher_cmd(&spec);

        assert!(cmd.starts_with("@echo off\r\n"));
        assert!(cmd.contains(&format!(
            "set \"LARKLINE_HOME={}\"\r\n",
            spec.env_value("LARKLINE_HOME").unwrap()
        )));
        assert!(cmd.contains("set \"PATH=/usr/local/bin:/usr/bin\"\r\n"));
        assert!(cmd.contains("set \"LARKLINE_PROFILE=claude\"\r\n"));
        assert!(cmd.contains(&format!(
            "cd /d \"{}\"\r\n",
            spec.working_dir.as_ref().unwrap().display()
        )));

        let expected_command = format!(
            "\"{}\" \"run\" \"--profile\" \"claude\" >> \"{}\" 2>> \"{}\"",
            spec.program.display(),
            spec.stdout_log.display(),
            spec.stderr_log.display()
        );
        assert!(cmd.contains(&expected_command));
        assert!(cmd.ends_with("\r\n"));
        assert!(!cmd.contains("\n\n"));
    }

    #[test]
    fn task_xml_describes_a_logon_task() {
        let xml = render_task_xml(&spec());
        assert!(xml.starts_with("<?xml version=\"1.0\" encoding=\"UTF-8\"?>"));
        assert!(xml.contains("<Task version=\"1.2\""));
        assert!(xml.contains("<LogonTrigger>"));
        assert!(xml.contains("<RunLevel>LeastPrivilege</RunLevel>"));
        assert!(xml.contains("<LogonType>InteractiveToken</LogonType>"));
        assert!(xml.contains("<MultipleInstancesPolicy>IgnoreNew</MultipleInstancesPolicy>"));
        assert!(xml.contains("<ExecutionTimeLimit>PT0S</ExecutionTimeLimit>"));
        assert!(xml.contains("<Command>cmd.exe</Command>"));
        assert!(xml.contains("launcher.cmd"));
        assert!(xml.contains("<Description>larkline bot (claude)</Description>"));
    }

    #[test]
    fn create_args_match_the_typescript_shape() {
        let spec = spec();
        let launcher = launcher_cmd_path(&spec);
        assert_eq!(
            create_task_args(&spec),
            vec![
                "/Create".to_string(),
                "/F".to_string(),
                "/SC".to_string(),
                "ONLOGON".to_string(),
                "/RL".to_string(),
                "LIMITED".to_string(),
                "/TN".to_string(),
                "Larkline.Bot.claude".to_string(),
                "/TR".to_string(),
                format!("\"{}\"", launcher.display()),
            ]
        );
    }

    #[test]
    fn task_name_and_launcher_path() {
        let spec = spec();
        assert_eq!(task_name(&spec), "Larkline.Bot.claude");
        assert_eq!(
            launcher_cmd_path(&spec),
            spec.root_dir
                .join("daemon")
                .join("claude")
                .join("launcher.cmd")
        );
    }

    #[test]
    fn parses_verbose_query_output() {
        let text = "\
Folder: \\
HostName:                             DESKTOP
TaskName:                             \\Larkline.Bot.claude
Status:                               Running
Last Run Time:                        1/1/2026 10:00:00 AM
Last Result:                          0
Task To Run:                          C:\\Users\\me\\.larkline\\daemon\\claude\\launcher.cmd
Process ID:                           9876
";
        let state = parse_status(text);
        assert!(state.running);
        assert_eq!(state.pid, Some(9876));
        assert_eq!(state.last_exit, Some(0));
    }

    #[test]
    fn parses_ready_task_without_pid() {
        let text = "Status: Ready\nLast Result: 267011\n";
        let state = parse_status(text);
        assert!(!state.running);
        assert_eq!(state.pid, None);
        assert_eq!(state.last_exit, Some(267011));
    }
}
