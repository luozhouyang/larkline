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
        approvals: true,
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
        approvals: true,
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
        approvals: true,
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

/// Minimal `codex app-server` that asks for one command approval.
fn fake_codex_app_server(dir: &std::path::Path) -> std::path::PathBuf {
    let path = dir.join("fake-codex");
    let script = r#"#!/usr/bin/env python3
import json
import os
import sys

here = os.path.dirname(os.path.abspath(__file__))
methods = []

def send(obj):
    sys.stdout.write(json.dumps(obj, separators=(",", ":")) + "\n")
    sys.stdout.flush()

for line in sys.stdin:
    line = line.strip()
    if not line:
        continue
    msg = json.loads(line)
    method = msg.get("method")
    req_id = msg.get("id")
    if method:
        methods.append(method)
        with open(os.path.join(here, "methods.json"), "w") as handle:
            json.dump(methods, handle)
    if method == "initialize":
        send({"id": req_id, "result": {"userAgent": "fake-codex"}})
    elif method in ("thread/start", "thread/resume"):
        send({"id": req_id, "result": {"thread": {"id": "thr_codex"}}})
    elif method == "turn/start":
        send({"id": req_id, "result": {"turn": {"id": "turn_1", "status": "inProgress", "items": []}}})
        send({"method": "turn/started", "params": {"threadId": "thr_codex", "turn": {"id": "turn_1", "status": "inProgress", "items": []}}})
        send({"method": "item/started", "params": {"threadId": "thr_codex", "turnId": "turn_1", "item": {
            "id": "cmd_1", "type": "commandExecution", "command": "pwd", "cwd": "/work",
            "commandActions": [], "status": "inProgress"}}})
        send({"id": 99, "method": "item/commandExecution/requestApproval", "params": {
            "threadId": "thr_codex", "turnId": "turn_1", "itemId": "cmd_1",
            "command": "pwd", "cwd": "/work", "reason": "test approval"}})
    elif req_id == 99:
        with open(os.path.join(here, "codex-decision.json"), "w") as handle:
            json.dump(msg, handle)
        send({"method": "item/completed", "params": {"threadId": "thr_codex", "turnId": "turn_1", "item": {
            "id": "cmd_1", "type": "commandExecution", "command": "pwd", "cwd": "/work",
            "commandActions": [], "status": "completed", "aggregatedOutput": "/work\n", "exitCode": 0}}})
        send({"method": "item/agentMessage/delta", "params": {"threadId": "thr_codex", "turnId": "turn_1", "itemId": "msg_1", "delta": "codex says hi"}})
        send({"method": "item/completed", "params": {"threadId": "thr_codex", "turnId": "turn_1", "item": {
            "id": "msg_1", "type": "agentMessage", "text": "codex says hi"}}})
        send({"method": "turn/completed", "params": {"threadId": "thr_codex", "turn": {"id": "turn_1", "status": "completed", "items": []}}})
"#;
    std::fs::write(&path, script).unwrap();
    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o755)).unwrap();
    path
}

#[tokio::test]
async fn codex_app_server_approval_round_trip() {
    let dir = tempfile::tempdir().unwrap();
    let binary = fake_codex_app_server(dir.path());
    let mut process = AgentProcess::spawn(AgentSpec {
        kind: AgentKind::Codex,
        approvals: true,
        binary,
        cwd: dir.path().to_path_buf(),
        prompt: "please run pwd".into(),
        session_id: None,
        model: Some("gpt-5".into()),
        permissions: RunPermissions {
            default_access: AccessMode::Workspace,
            max_access: AccessMode::Workspace,
            claude_permission_mode: None,
        },
        images: Vec::new(),
        bot_open_id: None,
        bot_name: None,
    })
    .await
    .expect("fake app-server should spawn");
    let mut view = RunView::default();
    let mut answered = false;
    loop {
        let Some(events) =
            tokio::time::timeout(std::time::Duration::from_secs(3), process.next_events())
                .await
                .expect("app-server event timed out")
        else {
            break;
        };
        for event in &events {
            if let AgentEvent::PermissionRequest {
                request_id,
                tool_name,
                ..
            } = event
            {
                assert_eq!(tool_name, "Bash");
                assert_eq!(request_id, "99");
                process
                    .respond_to_permission(
                        "99",
                        PermissionResponse::Allow {
                            updated_input: serde_json::json!({ "command": "pwd" }),
                        },
                    )
                    .await
                    .expect("approval response");
                answered = true;
            }
            view.apply(event);
        }
        if view.finished {
            break;
        }
    }
    assert!(answered, "the app-server asked for approval");
    assert_eq!(view.best_answer(), "codex says hi");
    assert_eq!(view.session_key(), Some("thr_codex"));
    assert_eq!(view.termination, Some(TerminationReason::Normal));

    process.close_stdin().await;
    process.finish().await;

    let decision = std::fs::read_to_string(dir.path().join("codex-decision.json"))
        .expect("the fake app-server recorded the decision");
    assert!(
        decision.contains("\"decision\": \"accept\"")
            || decision.contains("\"decision\":\"accept\""),
        "{decision}"
    );
    let methods = std::fs::read_to_string(dir.path().join("methods.json"))
        .expect("the fake app-server recorded methods");
    assert!(methods.contains("thread/start"), "{methods}");
    assert!(methods.contains("turn/start"), "{methods}");
}

#[tokio::test]
async fn codex_app_server_falls_back_when_resume_fails() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("fake-codex-resume");
    let script = r#"#!/usr/bin/env python3
import json
import os
import sys

here = os.path.dirname(os.path.abspath(__file__))
methods = []

def send(obj):
    sys.stdout.write(json.dumps(obj, separators=(",", ":")) + "\n")
    sys.stdout.flush()

for line in sys.stdin:
    msg = json.loads(line)
    method = msg.get("method")
    req_id = msg.get("id")
    if method:
        methods.append(method)
        with open(os.path.join(here, "resume-methods.json"), "w") as handle:
            json.dump(methods, handle)
    if method == "initialize":
        send({"id": req_id, "result": {"userAgent": "fake"}})
    elif method == "thread/resume":
        send({"id": req_id, "error": {"code": -32600, "message": "thread not found"}})
    elif method == "thread/start":
        send({"id": req_id, "result": {"thread": {"id": "thr_fallback"}}})
    elif method == "turn/start":
        send({"id": req_id, "result": {"turn": {"id": "turn_2", "status": "inProgress", "items": []}}})
        send({"method": "item/agentMessage/delta", "params": {"threadId": "thr_fallback", "turnId": "turn_2", "itemId": "m", "delta": "resumed"}})
        send({"method": "turn/completed", "params": {"threadId": "thr_fallback", "turn": {"id": "turn_2", "status": "completed", "items": []}}})
"#;
    std::fs::write(&path, script).unwrap();
    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o755)).unwrap();

    let mut process = AgentProcess::spawn(AgentSpec {
        kind: AgentKind::Codex,
        approvals: true,
        binary: path,
        cwd: dir.path().to_path_buf(),
        prompt: "hello again".into(),
        session_id: Some("missing-thread".into()),
        model: None,
        permissions: RunPermissions {
            default_access: AccessMode::Workspace,
            max_access: AccessMode::Workspace,
            claude_permission_mode: None,
        },
        images: Vec::new(),
        bot_open_id: None,
        bot_name: None,
    })
    .await
    .expect("fake app-server should spawn");

    let mut view = RunView::default();
    while let Some(events) =
        tokio::time::timeout(std::time::Duration::from_secs(3), process.next_events())
            .await
            .expect("event timed out")
    {
        for event in &events {
            view.apply(event);
        }
        if view.finished {
            break;
        }
    }
    assert_eq!(view.best_answer(), "resumed");
    assert_eq!(view.session_key(), Some("thr_fallback"));
    process.close_stdin().await;
    process.finish().await;

    let methods =
        std::fs::read_to_string(dir.path().join("resume-methods.json")).expect("recorded methods");
    assert!(methods.contains("thread/resume"), "{methods}");
    assert!(methods.contains("thread/start"), "{methods}");
}
