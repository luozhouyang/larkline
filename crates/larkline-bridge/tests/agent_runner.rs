//! End-to-end test of the agent runner with a fake agent binary.
//!
//! The fake speaks Claude's `stream-json` dialect, so this covers argv
//! construction, stdin prompt delivery, line reading, translation and the
//! render fold without needing a real agent or network.

#![cfg(unix)]

use std::os::unix::fs::PermissionsExt;

use larkline_agent::{AgentEvent, PermissionResponse, TerminationReason};
use larkline_bridge::agent_runner::{AgentProcess, AgentSpec};
use larkline_bridge::permissions::{AccessMode, ClaudePermissionMode, RunPermissions};
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
        // One-shot stream test: bypass permissions keeps the plain stdin
        // path. Workspace/acceptEdits now keeps stdin open for approval frames
        // and would deadlock this fake's `cat > /dev/null`.
        permissions: RunPermissions {
            default_access: AccessMode::Full,
            max_access: AccessMode::Full,
            claude_permission_mode: None,
        },
        images: Vec::new(),
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
        images: Vec::new(),
        bot_open_id: None,
        bot_name: None,
    })
    .await;
    assert!(result.is_err());
}

#[allow(dead_code)]
fn _assert_event_is_send(_: AgentEvent) {}

/// Fake Claude that asks for a Bash approval over the control protocol.
fn approval_agent(dir: &std::path::Path) -> std::path::PathBuf {
    let path = dir.join("fake-claude-approval");
    let script = r#"#!/bin/sh
# The bridge keeps stdin open for approval runs; read the stream-json prompt.
read prompt
printf '%s\n' '{"type":"system","subtype":"init","session_id":"sess-1"}'
printf '%s\n' '{"type":"assistant","message":{"id":"m1","content":[{"type":"tool_use","id":"call-1","name":"Bash","input":{"command":"pwd"}}]}}'
printf '%s\n' '{"type":"control_request","request_id":"req-1","request":{"subtype":"can_use_tool","tool_name":"Bash","display_name":"Bash","input":{"command":"pwd"},"description":"print cwd","tool_use_id":"call-1","permission_suggestions":[{"type":"setMode","mode":"acceptEdits","destination":"session"}]}}'
read response
printf '%s\n' "$response" > response.json
printf '%s\n' '{"type":"assistant","message":{"id":"m2","content":[{"type":"text","text":"done after approval"}]}}'
printf '%s\n' '{"type":"result","session_id":"sess-1"}'
"#;
    std::fs::write(&path, script).unwrap();
    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o755)).unwrap();
    path
}

#[tokio::test]
async fn answers_a_claude_permission_request_over_stdin() {
    let dir = tempfile::tempdir().unwrap();
    let binary = approval_agent(dir.path());
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
            claude_permission_mode: Some(ClaudePermissionMode::Default),
        },
        images: Vec::new(),
        bot_open_id: Some("ou_bot".into()),
        bot_name: None,
    })
    .await
    .expect("fake agent should spawn");

    let mut request_id = None;
    for _ in 0..20 {
        let events = tokio::time::timeout(std::time::Duration::from_secs(2), process.next_events())
            .await
            .expect("permission request did not arrive")
            .expect("stdout stayed open");
        for event in events {
            if let AgentEvent::PermissionRequest { request_id: id, .. } = event {
                request_id = Some(id);
            }
        }
        if request_id.is_some() {
            break;
        }
    }
    let request_id = request_id.expect("permission request");
    process
        .respond_to_permission(
            &request_id,
            PermissionResponse::Allow {
                updated_input: serde_json::json!({ "command": "pwd" }),
            },
        )
        .await
        .expect("permission response");

    let mut answer = String::new();
    loop {
        let Some(events) =
            tokio::time::timeout(std::time::Duration::from_secs(2), process.next_events())
                .await
                .expect("run did not finish")
        else {
            break;
        };
        for event in events {
            if let AgentEvent::Text { delta } = event {
                answer.push_str(&delta);
            }
        }
        if answer.contains("done after approval") {
            break;
        }
    }
    process.close_stdin().await;
    process.finish().await;

    let response = std::fs::read_to_string(dir.path().join("response.json"))
        .expect("the fake agent recorded the response");
    assert!(response.contains("control_response"), "{response}");
    assert!(response.contains("req-1"), "{response}");
    assert!(response.contains("allow"), "{response}");
    assert!(response.contains("updatedInput"), "{response}");
    assert_eq!(answer, "done after approval");
}
