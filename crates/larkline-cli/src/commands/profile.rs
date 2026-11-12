//! `larkline profile` — profile lifecycle.

use std::io::Write as _;

use anyhow::{Context as _, bail};
use larkline_core::store::{
    create_profile, delete_profile, secret_key_for_app, set_active_profile, with_config_lock,
};

use crate::cli::{Cli, CreateArgs, ProfileArgs, ProfileCommand};
use crate::context::{AppContext, keystore_for, prompt_secret};

pub fn execute(cli: &Cli, args: &ProfileArgs) -> anyhow::Result<()> {
    let ctx = AppContext::load(cli)?;
    match &args.command {
        ProfileCommand::List => list(&ctx),
        ProfileCommand::Create(create_args) => create(&ctx, create_args),
        ProfileCommand::Use { name } => {
            with_config_lock(&ctx.config_file, |root| {
                set_active_profile(root, name)?;
                Ok(())
            })?;
            println!("active profile: {name}");
            Ok(())
        }
        ProfileCommand::Remove {
            name,
            yes,
            purge_secret,
        } => remove(&ctx, name, *yes, *purge_secret),
        ProfileCommand::Show { name } => show(&ctx, name.as_deref()),
    }
}

fn list(ctx: &AppContext) -> anyhow::Result<()> {
    if ctx.root.profiles.is_empty() {
        println!(
            "no profiles yet; create one with `larkline profile create <name> --app-id cli_xxx`"
        );
        return Ok(());
    }
    let active = ctx.profile_name();
    println!(
        "{:<3} {:<18} {:<8} {:<10} {}",
        "", "NAME", "AGENT", "TENANT", "APP ID"
    );
    for (name, profile) in &ctx.root.profiles {
        let marker = if name == active { "*" } else { " " };
        let app_id = profile.app().map(|a| a.id.as_str()).unwrap_or("-");
        let tenant = profile
            .app()
            .map(|a| match a.tenant {
                larkline_core::TenantBrand::Feishu => "feishu",
                larkline_core::TenantBrand::Lark => "lark",
            })
            .unwrap_or("-");
        println!(
            "{:<3} {:<18} {:<8} {:<10} {}",
            marker,
            name,
            profile.agent_kind.binary_name(),
            tenant,
            app_id
        );
    }
    Ok(())
}

fn create(ctx: &AppContext, args: &CreateArgs) -> anyhow::Result<()> {
    let name = args.name.trim();
    let app_id = args.app_id.trim();
    if app_id.is_empty() {
        bail!("--app-id must not be empty");
    }
    if ctx.root.profiles.contains_key(name) {
        bail!("profile {name} already exists");
    }

    let secret = match args.app_secret.as_ref().map(|s| s.trim()) {
        Some(value) if !value.is_empty() => value.to_string(),
        _ => prompt_secret(format!("App Secret for {app_id}: "))?,
    };

    // The wrapper keeps lark-cli's exec-provider audit happy and is stable
    // across upgrades (it points at this binary, not at a temp path).
    let binary = std::env::current_exe().context("cannot resolve current executable")?;
    let wrapper = larkline_core::store::ensure_secrets_getter_wrapper(&ctx.root_dir, &binary)?;

    // Write the secret first: a config that points at a missing secret is
    // worse than an orphaned secret.
    let keystore = keystore_for(&ctx.root_dir, name)?;
    keystore.set(&secret_key_for_app(app_id), &secret)?;

    with_config_lock(&ctx.config_file, |root| {
        create_profile(
            root,
            name,
            args.agent.into(),
            app_id,
            args.tenant.into(),
            &wrapper,
        )?;
        if !args.no_use {
            set_active_profile(root, name)?;
        }
        Ok(())
    })?;

    std::fs::create_dir_all(ctx.root_dir.join("profiles").join(name))?;
    println!("created profile {name} ({:?}, {app_id})", args.agent);
    if args.no_use {
        println!("activate it with `larkline profile use {name}`");
    } else {
        println!("active profile: {name}");
    }
    Ok(())
}

fn remove(ctx: &AppContext, name: &str, yes: bool, purge_secret: bool) -> anyhow::Result<()> {
    let profile = ctx
        .root
        .profile(name)
        .cloned()
        .ok_or_else(|| anyhow::anyhow!("unknown profile: {name}"))?;

    if !yes {
        print!("Delete profile {name}? type the name to confirm: ");
        std::io::stdout().flush()?;
        let mut input = String::new();
        std::io::stdin().read_line(&mut input)?;
        if input.trim() != name {
            bail!("aborted");
        }
    }

    with_config_lock(&ctx.config_file, |root| {
        delete_profile(root, name)?;
        Ok(())
    })?;

    if purge_secret {
        if let Some(app) = profile.app() {
            let keystore = keystore_for(&ctx.root_dir, name)?;
            keystore.remove(&secret_key_for_app(&app.id))?;
        }
    }

    println!("deleted profile {name}");
    Ok(())
}

fn show(ctx: &AppContext, name: Option<&str>) -> anyhow::Result<()> {
    let name = name.unwrap_or_else(|| ctx.profile_name());
    let profile = ctx
        .root
        .profile(name)
        .ok_or_else(|| anyhow::anyhow!("unknown profile: {name}"))?;
    println!("{}", serde_json::to_string_pretty(profile)?);
    Ok(())
}
