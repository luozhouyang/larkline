//! End-to-end test of the agent runner with a fake agent binary.
//!
//! The fake speaks Claude's `stream-json` dialect, so this covers argv
//! construction, stdin prompt delivery, line reading, translation and the
//! render fold without needing a real agent or network.

#![cfg(unix)]

use std::os::unix::fs::PermissionsExt;

use larkline_agent::{AgentEvent, TerminationReason};
use larkline_bridge::agent_runner::{AgentProcess, AgentSpec};
use larkline_bridge::permissions::{AccessMode, RunPermissions};
use larkline_bridge::render::RunView;
use larkline_core::AgentKind;

fn fake_agent(dir: &std::path::Path) -> std::path::PathBuf {
    let path = dir.join("fake-claude");
    let script = r#"#!/bin/sh
# Consume the prompt so the writer never blocks.
cat > /dev/null
printf '%s\n' '{"type":"system","subtype":"init","session_id":"sess-1","cwd":"/work","model":"fake"}'
printf '%s\n' '{"type":"assistant","message":{"id":"m1","content":[{"type":"text","text":"hello from fake"}]}}'
printf '%s\n' '{"type":"result","session_id":"sess-1","usage":{"input_tokens":3,"output_tokens":4}}'
"#;
    std::fs::write(&path, script).unwrap();
    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o755)).unwrap();
    path
}

#[tokio::test]
async fn streams_and_folds_a_fake_claude_run() {
    let dir = tempfile::tempdir().unwrap();
    let binary = fake_agent(dir.path());

    let mut process = AgentProcess::spawn(AgentSpec {
        kind: AgentKind::Claude,
        binary,
        cwd: dir.path().to_path_buf(),
        prompt: "hello".into(),
        session_id: None,
        model: None,
        permissions: RunPermissions {
            default_access: AccessMode::Workspace,
            max_access: AccessMode::Workspace,
            claude_permission_mode: None,
        },
        bot_open_id: Some("ou_bot".into()),
        bot_name: None,
    })
    .await
    .expect("fake agent should spawn");

    let mut view = RunView::default();
    while let Some(events) = process.next_events().await {
        for event in &events {
            view.apply(event);
        }
        if view.finished {
            break;
        }
    }
    for event in process.finish().await {
        view.apply(&event);
    }

    assert_eq!(view.best_answer(), "hello from fake");
    assert_eq!(view.session_key(), Some("sess-1"));
    assert_eq!(view.input_tokens, 3);
    assert_eq!(view.output_tokens, 4);
    assert_eq!(view.termination, Some(TerminationReason::Normal));
    assert!(view.finished);
}

#[tokio::test]
async fn missing_binary_is_reported() {
    let dir = tempfile::tempdir().unwrap();
    let result = AgentProcess::spawn(AgentSpec {
        kind: AgentKind::Codex,
        binary: dir.path().join("does-not-exist"),
        cwd: dir.path().to_path_buf(),
        prompt: "hello".into(),
        session_id: None,
        model: None,
        permissions: RunPermissions::default(),
        bot_open_id: None,
        bot_name: None,
    })
    .await;
    assert!(result.is_err());
}

#[allow(dead_code)]
fn _assert_event_is_send(_: AgentEvent) {}
