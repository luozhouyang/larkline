//! `larkline doctor` — pre-flight diagnosis.
//!
//! Every check is independent and reports a concrete next step. Exit status is
//! non-zero when any check fails, so CI and scripts can gate on it.

use std::path::Path;

use larkline_agent::resolve_executable;

use crate::context::AppContext;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Status {
    Ok,
    Warn,
    Fail,
}

#[derive(Debug)]
struct Check {
    name: &'static str,
    status: Status,
    detail: String,
}

impl Check {
    fn ok(name: &'static str, detail: impl Into<String>) -> Self {
        Self {
            name,
            status: Status::Ok,
            detail: detail.into(),
        }
    }

    fn warn(name: &'static str, detail: impl Into<String>) -> Self {
        Self {
            name,
            status: Status::Warn,
            detail: detail.into(),
        }
    }

    fn fail(name: &'static str, detail: impl Into<String>) -> Self {
        Self {
            name,
            status: Status::Fail,
            detail: detail.into(),
        }
    }
}

pub fn execute(ctx: &AppContext) -> anyhow::Result<()> {
    let mut checks = Vec::new();
    checks.push(check_home(&ctx.root_dir));

    let config_exists = ctx.config_file.exists();
    checks.push(if config_exists {
        Check::ok("config", ctx.config_file.display().to_string())
    } else {
        Check::warn(
            "config",
            format!(
                "{} not found; run `larkline profile create`",
                ctx.config_file.display()
            ),
        )
    });

    if ctx.root.profiles.is_empty() {
        checks.push(Check::fail("profiles", "no profiles configured"));
    } else {
        checks.push(Check::ok(
            "profiles",
            format!(
                "{} configured: {}",
                ctx.root.profiles.len(),
                ctx.root
                    .profiles
                    .keys()
                    .cloned()
                    .collect::<Vec<_>>()
                    .join(", ")
            ),
        ));
    }

    let active = ctx.profile_name();
    match ctx.profile() {
        None => checks.push(Check::fail(
            "active profile",
            format!("{active} does not exist"),
        )),
        Some(profile) => {
            checks.push(Check::ok("active profile", active));
            match profile.app() {
                Some(app) if !app.id.is_empty() => {
                    checks.push(Check::ok("app id", &app.id));
                }
                _ => checks.push(Check::fail("app id", "missing accounts.app.id")),
            }
            if !profile.is_complete() {
                checks.push(Check::fail(
                    "credentials",
                    "accounts.app.secret is missing or empty",
                ));
            } else {
                match ctx.app_secret() {
                    Ok(secret) if !secret.is_empty() => {
                        checks.push(Check::ok("keystore", "app secret decrypts"));
                    }
                    Ok(_) => checks.push(Check::fail("keystore", "app secret is empty")),
                    Err(err) => checks.push(Check::fail("keystore", err.to_string())),
                }
            }

            let binary_name = profile.agent_kind.binary_name();
            match resolve_executable(binary_name) {
                Ok(found) => checks.push(Check::ok(
                    "agent",
                    format!(
                        "{} -> {}",
                        profile.agent_kind.display_name(),
                        found.path.display()
                    ),
                )),
                Err(_) => checks.push(Check::fail(
                    "agent",
                    format!(
                        "{binary_name} not found on PATH; install {} or set {}_BIN",
                        profile.agent_kind.display_name(),
                        binary_name.to_ascii_uppercase()
                    ),
                )),
            }
        }
    }

    match resolve_executable("lark-cli") {
        Ok(found) => checks.push(Check::ok("lark-cli", found.path.display().to_string())),
        Err(_) => checks.push(Check::warn(
            "lark-cli",
            "not found on PATH; in-chat /commands and user-identity tools will be unavailable",
        )),
    }

    let failures = checks.iter().filter(|c| c.status == Status::Fail).count();
    let warnings = checks.iter().filter(|c| c.status == Status::Warn).count();

    println!("larkline doctor\n");
    for check in &checks {
        let mark = match check.status {
            Status::Ok => "✓",
            Status::Warn => "!",
            Status::Fail => "✗",
        };
        println!("  {mark} {:<15} {}", check.name, check.detail);
    }
    println!(
        "\n{} check(s): {} ok, {} warning, {} failure",
        checks.len(),
        checks.len() - failures - warnings,
        warnings,
        failures
    );

    if failures > 0 {
        anyhow::bail!("{failures} problem(s) found");
    }
    Ok(())
}

fn check_home(root: &Path) -> Check {
    let result = std::fs::create_dir_all(root)
        .and_then(|_| std::fs::metadata(root))
        .and_then(|meta| {
            if meta.is_dir() {
                Ok(())
            } else {
                Err(std::io::Error::other("not a directory"))
            }
        });
    match result {
        Ok(()) => Check::ok("home", root.display().to_string()),
        Err(err) => Check::fail("home", format!("{}: {err}", root.display())),
    }
}
