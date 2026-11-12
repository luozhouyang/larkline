//! Chain-of-thought (COT) process messages (`im/v1/message_cot`).
//!
//! Port of `CotClient` from `src/bot/cot.ts`. Feishu renders a live "过程消息"
//! bubble from a stream of structured events; the bubble is created once per
//! run, appended to with batched [`CotEvent`]s, and completed (or failed) when
//! the run settles.
//!
//! Three wire details matter and are carried over verbatim:
//!
//! * `message_cot` only accepts `receive_id_type=chat_id`. A thread id
//!   (`omt_…`) is **not** a valid receive type for this endpoint; topic
//!   placement comes from `origin_message_id`, whose thread the bubble
//!   inherits.
//! * Create returns `cot_id` + `message_id`; both are needed for every later
//!   call. A `code=0` response missing either id is treated as a failure (a
//!   retry could create a duplicate bubble).
//! * `complete` takes the reason as a query parameter. The TypeScript bridge
//!   only ever sends `done` or `error`; there is no separate cancel endpoint,
//!   so a cancelled/interrupted run completes through
//!   [`CotCompleteReason::Error`].
//!
//! The trait [`CotTransport`] exists so the bridge publisher can be tested
//! against an in-memory fake without a network or an HTTP server.

use async_trait::async_trait;
use reqwest::Method;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use crate::rest::{RestClient, RestError, RestResult};

/// Create endpoint. `receive_id_type=chat_id` is part of the path.
pub const COT_CREATE_PATH: &str = "/open-apis/im/v1/message_cot?receive_id_type=chat_id";
/// Append endpoint (one batch of events per call).
pub const COT_UPDATE_PATH: &str = "/open-apis/im/v1/message_cot";

/// One live COT bubble: the ids `create` returned.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct CotRef {
    /// Server id used by `update`/`complete`.
    pub cot_id: String,
    /// Message id of the rendered bubble.
    pub message_id: String,
}

impl CotRef {
    /// A bubble reference.
    pub fn new(cot_id: impl Into<String>, message_id: impl Into<String>) -> Self {
        Self {
            cot_id: cot_id.into(),
            message_id: message_id.into(),
        }
    }
}

/// Event types the COT bubble understands (TS string literals).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CotEventType {
    RunStarted,
    StepStarted,
    StepFinished,
    ReasoningStart,
    ReasoningMessageStart,
    ReasoningMessageContent,
    ReasoningMessageEnd,
    ReasoningEnd,
    ToolCallStart,
    ToolCallArgs,
    ToolCallEnd,
    ToolCallResult,
    TextMessageStart,
    TextMessageContent,
    TextMessageEnd,
    RunFinished,
    RunError,
}

impl CotEventType {
    /// The wire string (`event_type`).
    pub fn as_str(self) -> &'static str {
        match self {
            Self::RunStarted => "RUN_STARTED",
            Self::StepStarted => "STEP_STARTED",
            Self::StepFinished => "STEP_FINISHED",
            Self::ReasoningStart => "REASONING_START",
            Self::ReasoningMessageStart => "REASONING_MESSAGE_START",
            Self::ReasoningMessageContent => "REASONING_MESSAGE_CONTENT",
            Self::ReasoningMessageEnd => "REASONING_MESSAGE_END",
            Self::ReasoningEnd => "REASONING_END",
            Self::ToolCallStart => "TOOL_CALL_START",
            Self::ToolCallArgs => "TOOL_CALL_ARGS",
            Self::ToolCallEnd => "TOOL_CALL_END",
            Self::ToolCallResult => "TOOL_CALL_RESULT",
            Self::TextMessageStart => "TEXT_MESSAGE_START",
            Self::TextMessageContent => "TEXT_MESSAGE_CONTENT",
            Self::TextMessageEnd => "TEXT_MESSAGE_END",
            Self::RunFinished => "RUN_FINISHED",
            Self::RunError => "RUN_ERROR",
        }
    }
}

/// One COT event as the endpoint expects it.
///
/// TypeScript sends `content` as `JSON.stringify(value)`; the JSON text is the
/// wire shape, so it is kept as a string here instead of a `Value`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CotEvent {
    pub event_type: String,
    pub content: String,
    /// Milliseconds since the Unix epoch (TS `Date.now()`).
    pub timestamp: i64,
}

impl CotEvent {
    /// Build an event with an explicit JSON content string.
    pub fn new(event_type: CotEventType, content: impl Into<String>, timestamp: i64) -> Self {
        Self {
            event_type: event_type.as_str().to_string(),
            content: content.into(),
            timestamp,
        }
    }

    /// Build an event whose content is `content` serialized as JSON.
    ///
    /// Serialization cannot fail for the JSON values the publisher builds; a
    /// failure falls back to `null` so a COT event never panics a run.
    pub fn json(event_type: CotEventType, content: impl Serialize, timestamp: i64) -> Self {
        Self::new(
            event_type,
            serde_json::to_string(&content).unwrap_or_else(|_| "null".to_string()),
            timestamp,
        )
    }

    /// Like [`CotEvent::json`] with `Date.now()` as the timestamp.
    pub fn now(event_type: CotEventType, content: impl Serialize) -> Self {
        Self::json(event_type, content, now_ms())
    }

    /// The parsed `content` object (for tests and the fake transport).
    pub fn content_value(&self) -> Value {
        serde_json::from_str(&self.content).unwrap_or(Value::Null)
    }
}

/// How a COT bubble is completed.
///
/// The endpoint takes a free-form `reason` query parameter; the TypeScript
/// bridge only produces these two values (`finish('done' | 'error')`), so
/// cancellation and interruption are reported as [`CotCompleteReason::Error`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CotCompleteReason {
    /// The run finished normally.
    Done,
    /// The run failed, was stopped, or timed out.
    Error,
}

impl CotCompleteReason {
    /// The `reason` query value.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Done => "done",
            Self::Error => "error",
        }
    }
}

/// `true` -> [`CotCompleteReason::Done`], `false` -> [`CotCompleteReason::Error`].
impl From<bool> for CotCompleteReason {
    /// `true` -> done, `false` -> error.
    fn from(done: bool) -> Self {
        if done { Self::Done } else { Self::Error }
    }
}

/// The three COT operations the publisher needs.
///
/// Implemented for [`CotClient`] over the real REST client, and by the bridge
/// test suite with an in-memory fake.
#[async_trait]
pub trait CotTransport: Send + Sync {
    /// Create the bubble and return its reference.
    async fn create(&self, chat_id: &str, origin_message_id: Option<&str>) -> RestResult<CotRef>;

    /// Append one batch of events to the bubble.
    async fn update(&self, reference: &CotRef, events: &[CotEvent]) -> RestResult<()>;

    /// Complete the bubble with `reason`.
    async fn complete(&self, reference: &CotRef, reason: CotCompleteReason) -> RestResult<()>;
}

/// Typed client for the COT endpoints.
#[derive(Clone)]
pub struct CotClient {
    rest: RestClient,
}

impl CotClient {
    /// Wrap a REST client (the tenant token cache is shared).
    pub fn new(rest: RestClient) -> Self {
        Self { rest }
    }

    /// Underlying REST client (for callers that need the raw surface).
    pub fn rest(&self) -> &RestClient {
        &self.rest
    }

    /// Create the bubble for `chat_id`, originating from `origin_message_id`.
    ///
    /// `receive_id_type` is always `chat_id`; the origin message only decides
    /// which thread the bubble inherits (see the module docs).
    pub async fn create(
        &self,
        chat_id: &str,
        origin_message_id: Option<&str>,
    ) -> RestResult<CotRef> {
        let body = create_body(chat_id, origin_message_id);
        let data = self.rest.post(COT_CREATE_PATH, &body).await?;
        parse_created(&data)
    }

    /// Append `events` to `reference`. An empty batch is a no-op, like TS.
    pub async fn update(&self, reference: &CotRef, events: &[CotEvent]) -> RestResult<()> {
        if events.is_empty() {
            return Ok(());
        }
        let body = update_body(reference, events);
        self.rest
            .request(Method::PUT, COT_UPDATE_PATH, Some(&body), &[])
            .await
            .map(|_| ())
    }

    /// Cancel the bubble.
    ///
    /// There is no separate cancel endpoint: the TypeScript publisher reports
    /// a stopped/interrupted run by completing with `reason=error`
    /// (`finish('error')`), so `cancel` is that operation by name.
    pub async fn cancel(&self, reference: &CotRef) -> RestResult<()> {
        self.complete(reference, CotCompleteReason::Error).await
    }

    /// Complete (or fail) the bubble.
    pub async fn complete(&self, reference: &CotRef, reason: CotCompleteReason) -> RestResult<()> {
        let reason = reason.as_str();
        self.rest
            .request(
                Method::POST,
                &complete_path(&reference.cot_id),
                None,
                &[
                    ("message_id", reference.message_id.as_str()),
                    ("reason", reason),
                ],
            )
            .await
            .map(|_| ())
    }
}

#[async_trait]
impl CotTransport for CotClient {
    async fn create(&self, chat_id: &str, origin_message_id: Option<&str>) -> RestResult<CotRef> {
        CotClient::create(self, chat_id, origin_message_id).await
    }

    async fn update(&self, reference: &CotRef, events: &[CotEvent]) -> RestResult<()> {
        CotClient::update(self, reference, events).await
    }

    async fn complete(&self, reference: &CotRef, reason: CotCompleteReason) -> RestResult<()> {
        CotClient::complete(self, reference, reason).await
    }
}

/// Request body for `create`.
fn create_body(chat_id: &str, origin_message_id: Option<&str>) -> Value {
    match origin_message_id {
        Some(origin) => json!({ "receive_id": chat_id, "origin_message_id": origin }),
        None => json!({ "receive_id": chat_id }),
    }
}

/// Request body for `update`.
fn update_body(reference: &CotRef, events: &[CotEvent]) -> Value {
    json!({
        "cot_id": reference.cot_id,
        "message_id": reference.message_id,
        "events": events,
    })
}

/// Path (including the escaped id) for `complete`.
fn complete_path(cot_id: &str) -> String {
    format!(
        "/open-apis/im/v1/message_cot/complete/{}",
        encode_path_segment(cot_id)
    )
}

/// Parse the create response, tolerating camelCase ids.
fn parse_created(data: &Value) -> RestResult<CotRef> {
    #[derive(Deserialize)]
    struct Created {
        #[serde(alias = "cotId")]
        cot_id: Option<String>,
        #[serde(alias = "messageId")]
        message_id: Option<String>,
    }

    let parsed: Created = serde_json::from_value(data.clone()).unwrap_or(Created {
        cot_id: None,
        message_id: None,
    });
    match (parsed.cot_id, parsed.message_id) {
        (Some(cot_id), Some(message_id)) if !cot_id.is_empty() && !message_id.is_empty() => {
            Ok(CotRef { cot_id, message_id })
        }
        _ => Err(RestError::Message(format!(
            "create message_cot response is missing ids: {}",
            truncate(&data.to_string(), 200)
        ))),
    }
}

/// Percent-encode one path segment, keeping RFC 3986 unreserved characters.
///
/// The TypeScript client uses `encodeURIComponent`; a COT id is normally
/// `cot_…`, so this only matters for defensive hygiene.
fn encode_path_segment(value: &str) -> String {
    const UNRESERVED: &[u8] = b"-._~";
    let mut out = String::with_capacity(value.len());
    for byte in value.bytes() {
        if byte.is_ascii_alphanumeric() || UNRESERVED.contains(&byte) {
            out.push(char::from(byte));
        } else {
            out.push_str(&format!("%{byte:02X}"));
        }
    }
    out
}

/// Milliseconds since the Unix epoch, clamped at zero for a bad clock.
pub fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|elapsed| elapsed.as_millis().min(i64::MAX as u128) as i64)
        .unwrap_or(0)
}

fn truncate(value: &str, max: usize) -> String {
    if value.chars().count() <= max {
        value.to_string()
    } else {
        let mut out: String = value.chars().take(max).collect();
        out.push('…');
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn reference() -> CotRef {
        CotRef::new("cot_abc", "om_bubble")
    }

    fn event(event_type: CotEventType, content: Value, timestamp: i64) -> CotEvent {
        CotEvent::json(event_type, content, timestamp)
    }

    #[test]
    fn create_request_targets_the_chat_receive_type_and_keeps_the_origin() {
        // TS test: `receive_id_type=chat_id` is in the path and `thread_id`
        // never appears; the origin message id carries topic placement.
        assert!(COT_CREATE_PATH.contains("receive_id_type=chat_id"));
        assert!(!COT_CREATE_PATH.contains("thread_id"));

        assert_eq!(
            create_body("oc_chat", Some("om_origin")),
            json!({ "receive_id": "oc_chat", "origin_message_id": "om_origin" })
        );
        // Without an origin the key is omitted (not sent as null).
        assert_eq!(
            create_body("oc_chat", None),
            json!({ "receive_id": "oc_chat" })
        );
    }

    #[test]
    fn update_body_carries_the_ref_and_the_event_batch() {
        let events = vec![event(
            CotEventType::TextMessageContent,
            json!({ "messageId": "text-run-1", "delta": "hi" }),
            1234,
        )];
        assert_eq!(
            update_body(&reference(), &events),
            json!({
                "cot_id": "cot_abc",
                "message_id": "om_bubble",
                "events": [{
                    "event_type": "TEXT_MESSAGE_CONTENT",
                    "content": "{\"messageId\":\"text-run-1\",\"delta\":\"hi\"}",
                    "timestamp": 1234,
                }],
            })
        );
    }

    #[test]
    fn complete_path_escapes_the_cot_id_and_uses_the_ts_reasons() {
        assert_eq!(
            complete_path("cot_abc"),
            "/open-apis/im/v1/message_cot/complete/cot_abc"
        );
        // Ids with an unsafe byte are escaped like `encodeURIComponent`.
        assert_eq!(
            complete_path("cot a/b"),
            "/open-apis/im/v1/message_cot/complete/cot%20a%2Fb"
        );
        // The complete call keeps the message id and reason in the query.
        assert_eq!(CotCompleteReason::Done.as_str(), "done");
        assert_eq!(CotCompleteReason::Error.as_str(), "error");
        assert_eq!(CotCompleteReason::from(true), CotCompleteReason::Done);
        assert_eq!(CotCompleteReason::from(false), CotCompleteReason::Error);
    }

    #[test]
    fn event_types_match_the_typescript_strings() {
        assert_eq!(CotEventType::RunStarted.as_str(), "RUN_STARTED");
        assert_eq!(CotEventType::StepStarted.as_str(), "STEP_STARTED");
        assert_eq!(CotEventType::StepFinished.as_str(), "STEP_FINISHED");
        assert_eq!(
            CotEventType::ReasoningMessageStart.as_str(),
            "REASONING_MESSAGE_START"
        );
        assert_eq!(
            CotEventType::ReasoningMessageContent.as_str(),
            "REASONING_MESSAGE_CONTENT"
        );
        assert_eq!(
            CotEventType::ReasoningMessageEnd.as_str(),
            "REASONING_MESSAGE_END"
        );
        assert_eq!(CotEventType::ToolCallStart.as_str(), "TOOL_CALL_START");
        assert_eq!(CotEventType::ToolCallArgs.as_str(), "TOOL_CALL_ARGS");
        assert_eq!(CotEventType::ToolCallEnd.as_str(), "TOOL_CALL_END");
        assert_eq!(CotEventType::ToolCallResult.as_str(), "TOOL_CALL_RESULT");
        assert_eq!(
            CotEventType::TextMessageStart.as_str(),
            "TEXT_MESSAGE_START"
        );
        assert_eq!(CotEventType::TextMessageEnd.as_str(), "TEXT_MESSAGE_END");
        assert_eq!(CotEventType::RunFinished.as_str(), "RUN_FINISHED");
        assert_eq!(CotEventType::RunError.as_str(), "RUN_ERROR");
    }

    #[test]
    fn events_serialize_with_a_json_string_content() {
        let event = event(CotEventType::RunStarted, json!({ "runId": "run-1" }), 42);
        assert_eq!(event.content, "{\"runId\":\"run-1\"}");
        assert_eq!(event.content_value()["runId"], "run-1");
        assert_eq!(
            serde_json::to_value(&event).unwrap(),
            json!({
                "event_type": "RUN_STARTED",
                "content": "{\"runId\":\"run-1\"}",
                "timestamp": 42,
            })
        );
    }

    #[test]
    fn created_response_requires_both_ids() {
        let parsed = parse_created(&json!({ "cot_id": "cot_1", "message_id": "om_1" })).unwrap();
        assert_eq!(parsed, CotRef::new("cot_1", "om_1"));

        // camelCase is accepted, like the TS `created.cotId ?? created.cot_id`.
        let camel = parse_created(&json!({ "cotId": "cot_2", "messageId": "om_2" })).unwrap();
        assert_eq!(camel, CotRef::new("cot_2", "om_2"));

        // A response missing either id must not be used: a retry could leave
        // two bubbles spinning.
        for payload in [
            json!({ "cot_id": "cot_1" }),
            json!({ "message_id": "om_1" }),
            json!({ "unexpected": "shape" }),
            json!({ "cot_id": "", "message_id": "om_1" }),
        ] {
            let error = parse_created(&payload).unwrap_err();
            assert!(
                error.to_string().contains("missing ids"),
                "payload {payload} produced {error}"
            );
        }
    }

    #[test]
    fn path_escaping_keeps_unreserved_characters() {
        assert_eq!(encode_path_segment("cot_1-2.3~4"), "cot_1-2.3~4");
        assert_eq!(encode_path_segment("a b+c"), "a%20b%2Bc");
    }

    #[test]
    fn timestamps_are_milliseconds() {
        let before = now_ms();
        assert!(before > 1_600_000_000_000, "now_ms must be milliseconds");
    }
}
