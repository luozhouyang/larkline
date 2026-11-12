//! `larkline onboard` — create a profile from a scanned QR code.
//!
//! Feishu/Lark expose an unauthenticated device flow for personal agent apps:
//! the CLI shows a verification URL as a QR code, the user scans it, and the
//! accounts host hands back the new app's id and secret. This command ties that
//! flow to the same profile/keystore machinery as `profile create`, so the
//! resulting setup is indistinguishable from a manually created one.

use anyhow::{Context as _, bail};
use larkline_core::TenantBrand;
use larkline_core::store::{
    create_profile, ensure_secrets_getter_wrapper, secret_key_for_app, set_active_profile,
    with_config_lock,
};
use larkline_lark::qr::{self, RegistrationOptions, RegistrationSession};
use qrcode::QrCode;
use qrcode::render::unicode;

use crate::cli::{Cli, OnboardArgs};
use crate::context::{AppContext, keystore_for};

/// The profile name used when `--name` is omitted, before de-duplication.
const DEFAULT_PROFILE_NAME: &str = "bot";

pub async fn execute(cli: &Cli, args: &OnboardArgs) -> anyhow::Result<()> {
    let ctx = AppContext::load(cli)?;
    let name = match args.name.as_deref() {
        Some(raw) => {
            let name = larkline_core::paths::normalize_profile_name(raw)?;
            if ctx.root.profiles.contains_key(&name) {
                bail!("profile {name} already exists");
            }
            name
        }
        None => suggest_profile_name(ctx.root.profiles.keys().map(String::as_str)),
    };

    println!("Creating a new Feishu/Lark app for profile {name}...");
    let session = qr::begin(&RegistrationOptions {
        domain: args.tenant.into(),
        source: "larkline".to_string(),
        app_id: None,
        create_only: true,
    })
    .await
    .context("failed to start app registration")?;

    print_qr(&session);

    println!("Waiting for you to scan the QR code and confirm in the app...");
    let result = qr::poll(&session, None)
        .await
        .context("app registration did not complete")?;
    println!("app registered: {}", result.app_id);

    // The poll result is authoritative for the tenant: polling switches to the
    // Lark accounts host when the scanned tenant is international.
    let tenant: TenantBrand = result.tenant;
    let app_id = result.app_id;
    let app_secret = result.app_secret;

    // The wrapper keeps lark-cli's exec-provider audit happy and is stable
    // across upgrades (it points at this binary, not at a temp path).
    let binary = std::env::current_exe().context("cannot resolve current executable")?;
    let wrapper = ensure_secrets_getter_wrapper(&ctx.root_dir, &binary)?;

    // Write the secret first: a config that points at a missing secret is
    // worse than an orphaned secret.
    let keystore = keystore_for(&ctx.root_dir, &name)?;
    keystore.set(&secret_key_for_app(&app_id), &app_secret)?;

    with_config_lock(&ctx.config_file, |root| {
        // The profile may have appeared while the user was scanning; refuse to
        // overwrite it rather than silently replacing someone else's config.
        if root.profiles.contains_key(&name) {
            return Err(larkline_core::CoreError::Message(format!(
                "profile {name} already exists"
            )));
        }
        create_profile(root, &name, args.agent.into(), &app_id, tenant, &wrapper)?;
        if !args.no_use {
            set_active_profile(root, &name)?;
        }
        Ok(())
    })?;

    std::fs::create_dir_all(ctx.root_dir.join("profiles").join(&name))?;

    println!("created profile {name} ({:?}, {app_id})", args.agent);
    if args.no_use {
        println!("activate it with `larkline profile use {name}`");
    } else {
        println!("active profile: {name}");
    }
    println!("next: run `larkline doctor` to verify the setup");
    Ok(())
}

/// Suggest a free default profile name: `bot`, then `bot-2`, `bot-3`, ...
///
/// Only names already present in the config are considered; a numeric suffix
/// is picked in increasing order so repeated onboarding stays predictable.
fn suggest_profile_name<'a>(existing: impl IntoIterator<Item = &'a str>) -> String {
    let existing: std::collections::BTreeSet<&str> = existing.into_iter().collect();
    if !existing.contains(DEFAULT_PROFILE_NAME) {
        return DEFAULT_PROFILE_NAME.to_string();
    }
    let mut suffix = 2u32;
    loop {
        let candidate = format!("{DEFAULT_PROFILE_NAME}-{suffix}");
        if !existing.contains(candidate.as_str()) {
            return candidate;
        }
        suffix += 1;
    }
}

/// Print the QR code, the clickable URL and expiry guidance.
fn print_qr(session: &RegistrationSession) {
    println!();
    match render_terminal_qr(&session.qr_url) {
        Some(qr) => print!("{qr}"),
        None => eprintln!("warning: could not render a terminal QR code; use the URL below"),
    }
    println!();
    println!("Scan the QR code with Feishu/Lark, or open this URL in a browser:");
    println!("  {}", session.qr_url);
    let minutes = session.expire_in.div_ceil(60).max(1);
    println!(
        "The QR code expires in about {minutes} minute(s); run `larkline onboard` again if it expires."
    );
    println!();
}

/// Render `url` as a terminal QR code, or `None` when the payload cannot be
/// encoded (for example an unexpectedly long verification URL).
fn render_terminal_qr(url: &str) -> Option<String> {
    let code = QrCode::new(url.as_bytes()).ok()?;
    // Dense1x2 packs two modules per character cell. The colour swap makes
    // the dark QR modules light so the code scans on a dark terminal.
    let rendered = code
        .render::<unicode::Dense1x2>()
        .dark_color(unicode::Dense1x2::Light)
        .light_color(unicode::Dense1x2::Dark)
        .quiet_zone(true)
        .build();
    Some(rendered)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn suggests_bot_when_no_profiles_exist() {
        assert_eq!(suggest_profile_name(std::iter::empty()), "bot");
    }

    #[test]
    fn ignores_unrelated_profile_names() {
        assert_eq!(
            suggest_profile_name(["default", "work", "alpha"]),
            "bot",
            "only an existing `bot` should force a suffix"
        );
    }

    #[test]
    fn dedupes_bot_with_numeric_suffix() {
        assert_eq!(suggest_profile_name(["bot"]), "bot-2");
        assert_eq!(suggest_profile_name(["bot", "bot-2"]), "bot-3");
        assert_eq!(
            suggest_profile_name(["bot", "bot-3", "bot-4"]),
            "bot-2",
            "the first free suffix wins"
        );
    }

    #[test]
    fn qr_renders_for_a_registration_url() {
        let url = "https://accounts.feishu.cn/oauth/v1/app/registration?user_code=abc";
        let rendered = render_terminal_qr(url).expect("registration URL must fit a QR code");
        assert!(rendered.lines().count() > 10);
        assert!(!rendered.trim().is_empty());
    }
}
