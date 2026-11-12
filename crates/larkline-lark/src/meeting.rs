//! VC bot REST layer for the in-meeting agent.
//!
//! These four endpoints are not modelled by the `lark-channel` SDK; they
//! authenticate with the tenant token that [`RestClient`] already caches, so
//! everything runs in-process. Paths and payloads mirror the TypeScript bridge
//! (`src/meeting/api.ts`):
//!
//! ```text
//! POST /open-apis/vc/v1/bots/join     { join_type: 1, join_identify: { meeting_no } }
//! POST /open-apis/vc/v1/bots/leave    { meeting_id }
//! POST /open-apis/vc/v1/bots/message  { meeting_id, msg_type, content }
//! GET  /open-apis/vc/v1/bots/events   ?meeting_id&page_size&page_token
//! ```
//!
//! [`RestClient`] has already unwrapped the `{code,msg,data}` envelope, so the
//! typed parsers below start at the `data` object. Non-zero API codes are
//! passed through unchanged as [`RestError::Api`]; [`is_not_in_gray`] detects
//! the allowlisted-beta gate (code `20017`).

use serde::de::Error as _;
use serde::{Deserialize, Deserializer, Serialize};
use serde_json::{Map, Value, json};

use crate::rest::{RestClient, RestError, RestResult};

/// Path for joining a meeting by its 9-digit meeting number.
pub const JOIN_PATH: &str = "/open-apis/vc/v1/bots/join";
/// Path for leaving a meeting by its long meeting id.
pub const LEAVE_PATH: &str = "/open-apis/vc/v1/bots/leave";
/// Path for sending an in-meeting chat message or reaction.
pub const MESSAGE_PATH: &str = "/open-apis/vc/v1/bots/message";
/// Path for polling in-meeting activity events.
pub const EVENTS_PATH: &str = "/open-apis/vc/v1/bots/events";

/// `ErrNotInGray`: the VC bot capability is an allowlisted beta.
pub const NOT_IN_GRAY_CODE: i64 = 20017;

/// Path for polling in-meeting activity events.
pub fn events_path() -> &'static str {
    EVENTS_PATH
}

/// A meeting number is exactly 9 digits — not a URL and not a long meeting id.
pub fn is_meeting_no(value: &str) -> bool {
    let value = value.trim();
    value.len() == 9 && value.bytes().all(|byte| byte.is_ascii_digit())
}

/// `true` when an API error is the beta gate (`20017` / `ErrNotInGray`).
///
/// Callers surface a dedicated "capability not enabled for this app" hint for
/// this code instead of a raw API failure.
pub fn is_not_in_gray(err: &RestError) -> bool {
    matches!(err, RestError::Api { code, .. } if *code == NOT_IN_GRAY_CODE)
}

/// Client for the four `/open-apis/vc/v1/bots` endpoints.
///
/// Borrows the shared [`RestClient`], so the tenant token cache is reused.
#[derive(Clone, Copy)]
pub struct VcBotClient<'a> {
    rest: &'a RestClient,
}

impl<'a> VcBotClient<'a> {
    pub fn new(rest: &'a RestClient) -> Self {
        Self { rest }
    }

    /// Join a meeting by its 9-digit number. Visible to every participant.
    ///
    /// `join_type` is fixed to `1` by the protocol. The response's long
    /// `meeting.id` is required by every subsequent call and is returned as
    /// [`JoinResult::meeting_id`].
    pub async fn join(&self, meeting_no: &str) -> RestResult<JoinResult> {
        let meeting_no = meeting_no.trim();
        if !is_meeting_no(meeting_no) {
            return Err(RestError::Message(format!(
                "会议号必须是 9 位数字：{meeting_no}"
            )));
        }
        let data = self.rest.post(JOIN_PATH, &join_body(meeting_no)).await?;
        JoinResult::from_data(data, meeting_no)
    }

    /// Leave a meeting by its long meeting id (not the 9-digit number).
    pub async fn leave(&self, meeting_id: &str) -> RestResult<()> {
        self.rest.post(LEAVE_PATH, &leave_body(meeting_id)).await?;
        Ok(())
    }

    /// Send an in-meeting message as the bot.
    ///
    /// `msg_type` is `text` for plain text or `reaction` for an emoji; the
    /// meaning of `content` follows from it.
    pub async fn send_message(
        &self,
        meeting_id: &str,
        msg_type: &str,
        content: &str,
    ) -> RestResult<()> {
        self.rest
            .post(MESSAGE_PATH, &message_body(meeting_id, msg_type, content))
            .await?;
        Ok(())
    }

    /// Pull one page of in-meeting activity events.
    ///
    /// Requires the bot to still be in the meeting. Query parameters are
    /// passed through [`RestClient::get`] so reqwest URL-encodes them.
    pub async fn events(
        &self,
        meeting_id: &str,
        page_size: u32,
        page_token: Option<&str>,
    ) -> RestResult<EventsPage> {
        let query = events_query(meeting_id, page_size, page_token);
        let query: Vec<(&str, &str)> = query
            .iter()
            .map(|(key, value)| (*key, value.as_str()))
            .collect();
        let data = self.rest.get(events_path(), &query).await?;
        EventsPage::from_data(data)
    }
}

/// The long meeting id plus the number that was joined.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct JoinResult {
    /// Long meeting id — required by every subsequent call.
    pub meeting_id: String,
    /// The 9-digit meeting number echoed back (the join response omits it).
    pub meeting_no: String,
    /// Meeting topic, when the API returns one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub topic: Option<String>,
}

impl JoinResult {
    /// Parse the already-unwrapped `data` body of a join response.
    ///
    /// `meeting_no` is supplied by the caller because the API only returns the
    /// long meeting id. A missing `meeting.id` is an error even though `code`
    /// was `0`: without it, no later VC bot call can target this meeting.
    pub fn from_data(data: Value, meeting_no: &str) -> RestResult<Self> {
        let data: JoinData = serde_json::from_value(object_or_empty(data))
            .map_err(|err| RestError::Message(format!("invalid VC join response: {err}")))?;
        let meeting = data.meeting.unwrap_or_default();
        let meeting_id = meeting
            .id
            .filter(|id| !id.is_empty())
            .ok_or_else(|| RestError::Message("入会成功但未返回 meeting.id".to_string()))?;
        Ok(Self {
            meeting_id,
            meeting_no: meeting_no.trim().to_string(),
            topic: meeting.topic.filter(|topic| !topic.is_empty()),
        })
    }
}

/// Nested `meeting` object shared by join and event payloads.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Meeting {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub topic: Option<String>,
}

/// One raw in-meeting activity item.
///
/// Mirrors `RawActivityItem` in `src/meeting/api.ts` plus the `type` /
/// `happen_time` fields the events endpoint returns. The per-type arrays
/// (`transcript_received_items`, `chat_received_items`, …) and any future
/// fields are preserved in [`extra`](Self::extra); some payload revisions nest
/// them under [`payload`](Self::payload) instead.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct MeetingActivityItem {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub event_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub meeting: Option<Meeting>,
    /// Numeric activity type (wire key `type`).
    #[serde(
        rename = "type",
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "de_opt_i64"
    )]
    pub r#type: Option<i64>,
    /// When the activity happened, in epoch milliseconds.
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "de_opt_i64"
    )]
    pub happen_time: Option<i64>,
    /// `transcript_received`, `chat_received`, `participant_joined`, …
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub activity_event_type: Option<String>,
    /// Legacy alias for `activity_event_type` used by some payload revisions.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub event_type: Option<String>,
    /// Alternate container for the per-type `*_items` arrays.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub payload: Option<Value>,
    /// Any field not named above, including the per-type item arrays.
    #[serde(flatten)]
    pub extra: Map<String, Value>,
}

impl MeetingActivityItem {
    /// `activity_event_type`, falling back to the legacy `event_type` key.
    ///
    /// This is the `item.activity_event_type ?? item.event_type` probe from the
    /// TypeScript bridge; an empty value is treated as absent.
    pub fn activity_kind(&self) -> Option<&str> {
        self.activity_event_type
            .as_deref()
            .or(self.event_type.as_deref())
            .filter(|kind| !kind.is_empty())
    }
}

/// One page of in-meeting activity events.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct EventsPage {
    /// Activity items, from either `events` or `meeting_activity_items`.
    #[serde(default)]
    pub events: Vec<MeetingActivityItem>,
    /// Cursor for the next incremental pull; absent when caught up.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub page_token: Option<String>,
    /// Whether the server has more events; keep pulling while this is true.
    #[serde(default)]
    pub has_more: bool,
}

impl EventsPage {
    /// Parse the already-unwrapped `data` body of an events response.
    ///
    /// Field names differ across API revisions: `events` and
    /// `meeting_activity_items` are both accepted, with `events` winning when
    /// present. A blank cursor is normalized to `None`.
    pub fn from_data(data: Value) -> RestResult<Self> {
        let data: EventsData = serde_json::from_value(object_or_empty(data))
            .map_err(|err| RestError::Message(format!("invalid VC events response: {err}")))?;
        Ok(Self {
            events: data
                .events
                .or(data.meeting_activity_items)
                .unwrap_or_default(),
            page_token: data.page_token.filter(|token| !token.trim().is_empty()),
            has_more: data.has_more,
        })
    }
}

#[derive(Debug, Default, Deserialize)]
struct JoinData {
    #[serde(default)]
    meeting: Option<Meeting>,
}

#[derive(Debug, Default, Deserialize)]
struct EventsData {
    #[serde(default)]
    events: Option<Vec<MeetingActivityItem>>,
    #[serde(default)]
    meeting_activity_items: Option<Vec<MeetingActivityItem>>,
    #[serde(default)]
    page_token: Option<String>,
    #[serde(default)]
    has_more: bool,
}

fn join_body(meeting_no: &str) -> Value {
    json!({
        "join_type": 1,
        "join_identify": { "meeting_no": meeting_no },
    })
}

fn leave_body(meeting_id: &str) -> Value {
    json!({ "meeting_id": meeting_id })
}

fn message_body(meeting_id: &str, msg_type: &str, content: &str) -> Value {
    json!({
        "meeting_id": meeting_id,
        "msg_type": msg_type,
        "content": content,
    })
}

/// Query pairs for [`VcBotClient::events`], in wire order.
fn events_query(
    meeting_id: &str,
    page_size: u32,
    page_token: Option<&str>,
) -> Vec<(&'static str, String)> {
    let mut query = Vec::with_capacity(3);
    query.push(("meeting_id", meeting_id.to_string()));
    query.push(("page_size", page_size.to_string()));
    if let Some(page_token) = page_token.filter(|token| !token.trim().is_empty()) {
        query.push(("page_token", page_token.to_string()));
    }
    query
}

/// `RestClient` returns `data` when present and the whole body otherwise;
/// treating JSON `null` as `{}` matches the TypeScript `?? {}` fallback.
fn object_or_empty(data: Value) -> Value {
    if data.is_null() {
        Value::Object(Map::new())
    } else {
        data
    }
}

/// Accept both JSON numbers and numeric strings for int64 fields. Feishu is
/// inconsistent about encoding 64-bit timestamps; a single string-encoded
/// `happen_time` must not fail the whole page.
fn de_opt_i64<'de, D>(deserializer: D) -> Result<Option<i64>, D::Error>
where
    D: Deserializer<'de>,
{
    match Option::<Value>::deserialize(deserializer)? {
        None | Some(Value::Null) => Ok(None),
        Some(Value::Number(number)) => number
            .as_i64()
            .map(Some)
            .ok_or_else(|| D::Error::custom(format!("expected an integer, got {number}"))),
        Some(Value::String(text)) if text.trim().is_empty() => Ok(None),
        Some(Value::String(text)) => text
            .trim()
            .parse::<i64>()
            .map(Some)
            .map_err(|_| D::Error::custom(format!("expected an integer, got {text:?}"))),
        Some(other) => Err(D::Error::custom(format!(
            "expected an integer, got {other}"
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn join_result_round_trips() {
        let joined = JoinResult {
            meeting_id: "70001".to_string(),
            meeting_no: "123456789".to_string(),
            topic: Some("周会".to_string()),
        };
        let value = serde_json::to_value(&joined).unwrap();
        assert_eq!(
            value,
            json!({
                "meeting_id": "70001",
                "meeting_no": "123456789",
                "topic": "周会",
            })
        );
        let back: JoinResult = serde_json::from_value(value).unwrap();
        assert_eq!(back, joined);
    }

    #[test]
    fn events_page_round_trips() {
        let page = EventsPage {
            events: vec![MeetingActivityItem {
                event_id: Some("evt-1".to_string()),
                meeting: Some(Meeting {
                    id: Some("70001".to_string()),
                    topic: Some("周会".to_string()),
                }),
                r#type: Some(1),
                happen_time: Some(1_700_000_000_000),
                activity_event_type: Some("transcript_received".to_string()),
                event_type: None,
                payload: None,
                extra: Map::from_iter([(
                    "transcript_received_items".to_string(),
                    json!([{ "sentence_id": "s1", "text": "hello" }]),
                )]),
            }],
            page_token: Some("tok-2".to_string()),
            has_more: true,
        };

        let value = serde_json::to_value(&page).unwrap();
        assert_eq!(value["events"][0]["type"], json!(1));
        assert_eq!(
            value["events"][0]["happen_time"],
            json!(1_700_000_000_000_i64)
        );
        assert_eq!(
            value["events"][0]["transcript_received_items"][0]["text"],
            json!("hello")
        );

        let back: EventsPage = serde_json::from_value(value).unwrap();
        assert_eq!(back, page);
    }

    #[test]
    fn join_parses_the_nested_meeting_and_requires_an_id() {
        let joined = JoinResult::from_data(
            json!({ "meeting": { "id": "70001", "topic": "周会" } }),
            " 123456789 ",
        )
        .unwrap();
        assert_eq!(joined.meeting_id, "70001");
        assert_eq!(joined.meeting_no, "123456789");
        assert_eq!(joined.topic.as_deref(), Some("周会"));

        let err = JoinResult::from_data(json!({ "meeting": { "topic": "no id" } }), "123456789")
            .unwrap_err();
        assert!(matches!(err, RestError::Message(message) if message.contains("meeting.id")));
        assert!(JoinResult::from_data(Value::Null, "123456789").is_err());
    }

    #[test]
    fn events_data_accepts_both_item_fields_and_drops_blank_cursors() {
        let page = EventsPage::from_data(json!({
            "events": [{ "event_id": "e1" }],
            "page_token": "tok-2",
            "has_more": true,
        }))
        .unwrap();
        assert_eq!(page.events.len(), 1);
        assert_eq!(page.page_token.as_deref(), Some("tok-2"));
        assert!(page.has_more);

        let page = EventsPage::from_data(json!({
            "meeting_activity_items": [{ "event_id": "e1" }, { "event_id": "e2" }],
            "page_token": "",
        }))
        .unwrap();
        assert_eq!(page.events.len(), 2);
        assert_eq!(page.page_token, None);
        assert!(!page.has_more);

        assert!(
            EventsPage::from_data(Value::Null)
                .unwrap()
                .events
                .is_empty()
        );
    }

    #[test]
    fn activity_items_parse_numbers_and_legacy_event_type() {
        let item: MeetingActivityItem = serde_json::from_value(json!({
            "event_id": "e1",
            "meeting": { "id": "70001" },
            "type": "2",
            "happen_time": "1700000000000",
            "event_type": "chat_received",
            "chat_received_items": [{ "content": "hi" }],
            "payload": { "chat_received_items": [{ "content": "hi" }] },
        }))
        .unwrap();

        assert_eq!(item.r#type, Some(2));
        assert_eq!(item.happen_time, Some(1_700_000_000_000));
        assert_eq!(item.activity_kind(), Some("chat_received"));
        assert!(item.extra.contains_key("chat_received_items"));
        assert!(item.payload.is_some());
    }

    #[test]
    fn not_in_gray_is_detected_from_the_api_code() {
        assert!(is_not_in_gray(&RestError::Api {
            code: 20017,
            message: "ErrNotInGray".to_string(),
        }));
        assert!(!is_not_in_gray(&RestError::Api {
            code: 1_050_000,
            message: "boom".to_string(),
        }));
        assert!(!is_not_in_gray(&RestError::Message(
            "code 20017".to_string()
        )));
    }

    #[test]
    fn meeting_numbers_are_exactly_nine_digits() {
        assert!(is_meeting_no("123456789"));
        assert!(is_meeting_no(" 123456789 "));
        assert!(!is_meeting_no("12345678"));
        assert!(!is_meeting_no("1234567890"));
        assert!(!is_meeting_no("12345678a"));
        assert!(!is_meeting_no("https://vc.feishu.cn/j/123456789"));
    }

    #[test]
    fn events_path_and_query_are_stable() {
        assert_eq!(events_path(), "/open-apis/vc/v1/bots/events");
        assert_eq!(
            events_query("70001", 100, Some("tok-1")),
            vec![
                ("meeting_id", "70001".to_string()),
                ("page_size", "100".to_string()),
                ("page_token", "tok-1".to_string()),
            ]
        );
        assert_eq!(
            events_query("70001", 100, None),
            vec![
                ("meeting_id", "70001".to_string()),
                ("page_size", "100".to_string()),
            ]
        );
        assert_eq!(events_query("70001", 100, Some("   ")).len(), 2);
    }

    #[test]
    fn request_bodies_match_the_openapi_contract() {
        assert_eq!(
            join_body("123456789"),
            json!({
                "join_type": 1,
                "join_identify": { "meeting_no": "123456789" },
            })
        );
        assert_eq!(leave_body("70001"), json!({ "meeting_id": "70001" }));
        assert_eq!(
            message_body("70001", "text", "已记录"),
            json!({
                "meeting_id": "70001",
                "msg_type": "text",
                "content": "已记录",
            })
        );
    }
}
