//! `larkline secrets` — encrypted keystore operations.
//!
//! `secrets get` is the exec-provider protocol lark-cli speaks: a JSON request
//! on stdin, a JSON response on stdout, exit 0 even when individual ids are
//! missing (they land in `errors`).

use std::io::Read as _;

use serde_json::{Map, Value, json};

use crate::cli::{Cli, SecretsArgs, SecretsCommand};
use crate::context::{AppContext, keystore_for, prompt_secret};

const PROTOCOL_VERSION: u32 = 1;

pub fn execute(cli: &Cli, args: &SecretsArgs) -> anyhow::Result<()> {
    let ctx = AppContext::load(cli)?;
    match &args.command {
        SecretsCommand::Get => get_protocol(&ctx),
        SecretsCommand::Set { id, value } => {
            let value = match value.as_ref().map(|v| v.trim()) {
                Some(v) if !v.is_empty() => v.to_string(),
                _ => prompt_secret(format!("Secret for {id}: "))?,
            };
            ctx.keystore().set(id, &value)?;
            println!("stored {id}");
            Ok(())
        }
        SecretsCommand::List => {
            let ids = ctx.keystore().list_ids()?;
            if ids.is_empty() {
                println!("no secrets stored in {}", ctx.paths.secrets_file.display());
            }
            for id in ids {
                println!("{id}");
            }
            Ok(())
        }
        SecretsCommand::Remove { id } => {
            if ctx.keystore().remove(id)? {
                println!("removed {id}");
            } else {
                println!("no such secret: {id}");
            }
            Ok(())
        }
    }
}

fn get_protocol(ctx: &AppContext) -> anyhow::Result<()> {
    let mut input = String::new();
    std::io::stdin().read_to_string(&mut input)?;
    let request: Value = if input.trim().is_empty() {
        json!({})
    } else {
        serde_json::from_str(&input).map_err(|e| anyhow::anyhow!("invalid stdin JSON: {e}"))?
    };

    let ids: Vec<String> = request
        .get("ids")
        .and_then(Value::as_array)
        .map(|ids| {
            ids.iter()
                .filter_map(|id| id.as_str().map(str::to_string))
                .collect()
        })
        .unwrap_or_default();

    let mut values = Map::new();
    let mut errors = Map::new();
    for id in ids {
        match lookup(&ctx, &id)? {
            Some(value) => {
                values.insert(id, Value::String(value));
            }
            None => {
                errors.insert(id, json!({ "message": "not found" }));
            }
        }
    }

    let mut response = json!({
        "protocolVersion": PROTOCOL_VERSION,
        "values": values,
    });
    if !errors.is_empty() {
        response["errors"] = Value::Object(errors);
    }
    println!("{response}");
    Ok(())
}

/// Search every profile's keystore, then fall back to declared providers.
fn lookup(ctx: &AppContext, id: &str) -> anyhow::Result<Option<String>> {
    for (name, _profile) in &ctx.root.profiles {
        let keystore = keystore_for(&ctx.root_dir, name)?;
        if let Some(value) = keystore.get(id)? {
            return Ok(Some(value));
        }
    }
    Ok(None)
}
