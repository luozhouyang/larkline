//! `larkline upgrade` — explicit upgrade reporting.
//!
//! There is deliberately no silent auto-update: the bridge runs as a daemon,
//! and replacing the binary underneath a live supervisor is how runs get
//! killed. `upgrade` reports the available version and prints the exact
//! installer command; the user decides when to restart.
//!
//! Once the release pipeline is live, `upgrade --apply` can download, verify
//! and atomically replace the binary. Until assets exist, this command never
//! pretends to have upgraded anything.

use anyhow::bail;
use reqwest::header::USER_AGENT;

use crate::cli::UpgradeArgs;

const REPO: &str = "luozhouyang/larkline";
const INSTALL_SCRIPT: &str =
    "https://raw.githubusercontent.com/luozhouyang/larkline/main/install.sh";

pub async fn execute(args: &UpgradeArgs) -> anyhow::Result<()> {
    let latest = latest_tag().await?;
    let current = larkline_core::VERSION;
    let newer = is_newer(&latest, current).unwrap_or(true);

    println!("current: v{current}");
    println!("latest:  {latest}");

    if args.check {
        if newer {
            println!("\nan update is available");
        } else {
            println!("\nalready up to date");
        }
        return Ok(());
    }

    if !newer && args.version.is_none() {
        println!("\nalready up to date");
        return Ok(());
    }

    let target = args
        .version
        .clone()
        .unwrap_or_else(|| latest.trim_start_matches('v').to_string());

    println!("\nupgrade explicitly with:\n");
    println!("  curl -fsSL {INSTALL_SCRIPT} -o /tmp/larkline-install.sh");
    println!("  sh /tmp/larkline-install.sh --version {target}");
    println!("  larkline restart");
    Ok(())
}

async fn latest_tag() -> anyhow::Result<String> {
    let response = reqwest::Client::new()
        .get(format!(
            "https://api.github.com/repos/{REPO}/releases/latest"
        ))
        .header(USER_AGENT, format!("larkline/{}", larkline_core::VERSION))
        .send()
        .await
        .map_err(|e| anyhow::anyhow!("cannot reach github: {e}"))?;
    if !response.status().is_success() {
        bail!("github api returned {}", response.status());
    }
    let body: serde_json::Value = response.json().await?;
    body.get("tag_name")
        .and_then(|value| value.as_str())
        .map(str::to_string)
        .ok_or_else(|| anyhow::anyhow!("release response has no tag_name"))
}

/// Compare `vA.B.C` strings. Returns `None` when either side is unparseable.
fn is_newer(latest: &str, current: &str) -> Option<bool> {
    let latest = parse_version(latest)?;
    let current = parse_version(current)?;
    Some(latest > current)
}

fn parse_version(value: &str) -> Option<(u64, u64, u64)> {
    let trimmed = value.trim().trim_start_matches('v');
    let mut parts = trimmed.split('.');
    let major = parts.next()?.parse().ok()?;
    let minor = parts.next().unwrap_or("0").parse().ok()?;
    let patch = parts
        .next()
        .unwrap_or("0")
        .split('-')
        .next()
        .unwrap_or("0")
        .parse()
        .ok()?;
    Some((major, minor, patch))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn versions_compare() {
        assert_eq!(is_newer("v0.2.0", "0.1.0"), Some(true));
        assert_eq!(is_newer("v0.1.0", "0.1.0"), Some(false));
        assert_eq!(is_newer("0.1.10", "0.1.9"), Some(true));
        assert_eq!(is_newer("v1.0.0-beta.1", "0.9.9"), Some(true));
        assert_eq!(parse_version("not-a-version"), None);
    }
}
