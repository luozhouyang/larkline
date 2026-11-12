//! Cloud-doc file comments (`drive/v1/files/{file_token}/comments`).
//!
//! Mirrors the SDK's `CommentSurface`:
//!
//! * [`fetch`] GETs one comment and falls back to a paginated `list` when the
//!   get endpoint reports `1069307` (whole-document comments and some comment
//!   types are only visible through `list`).
//! * [`reply`] posts in-thread and falls back to [`reply_top_level`] when the
//!   thread rejects in-thread replies with `1069302` (whole-document comments
//!   have no thread).
//! * [`add_reaction`] / [`remove_reaction`] target the dedicated
//!   `drive/v2/.../comments/reaction` endpoint, where add and delete are both
//!   POSTs distinguished by an `action` field. That endpoint returns no
//!   reaction id.

use reqwest::Method;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use crate::rest::{RestClient, RestError, RestResult};

/// `fileComment.get` reports this when a comment is only reachable via list.
const COMMENT_GET_FALLBACK: i64 = 1069307;
/// `fileCommentReply.create` reports this for whole-document comments.
const COMMENT_HAS_NO_THREAD: i64 = 1069302;
const LIST_PAGE_SIZE: &str = "100";
const MAX_LIST_PAGES: usize = 10;

/// A comment on a cloud-doc file, as addressed by the drive comment API.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CommentRef {
    pub file_token: String,
    /// `doc`, `docx`, `sheet` or `file`: the types the comment API supports.
    pub file_type: String,
    pub comment_id: String,
}

impl CommentRef {
    pub fn new(
        file_token: impl Into<String>,
        file_type: impl Into<String>,
        comment_id: impl Into<String>,
    ) -> Self {
        Self {
            file_token: file_token.into(),
            file_type: file_type.into(),
            comment_id: comment_id.into(),
        }
    }
}

/// A comment thread: the quoted selection plus every reply in order.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Comment {
    #[serde(default)]
    pub comment_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub quote: Option<String>,
    #[serde(default)]
    pub is_whole: bool,
    #[serde(default)]
    pub replies: Vec<CommentReply>,
}

/// One reply inside a comment thread.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct CommentReply {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reply_id: Option<String>,
    #[serde(default)]
    pub content: ReplyContent,
}

impl CommentReply {
    /// Flatten text runs and doc links; @-mention elements carry no text and
    /// are dropped, matching `replyElementsToText` in the TypeScript bridge.
    pub fn text(&self) -> String {
        self.content
            .elements
            .iter()
            .filter_map(|element| match element {
                ReplyElement::TextRun { text_run } => Some(text_run.text.as_str()),
                ReplyElement::DocsLink { docs_link } => Some(docs_link.url.as_str()),
                ReplyElement::Person { .. } | ReplyElement::Other => None,
            })
            .collect::<String>()
            .trim()
            .to_string()
    }
}

/// The rich-text body of a comment reply.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReplyContent {
    #[serde(default)]
    pub elements: Vec<ReplyElement>,
}

/// One element of a comment reply body.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ReplyElement {
    TextRun {
        #[serde(default)]
        text_run: TextRun,
    },
    DocsLink {
        #[serde(default)]
        docs_link: DocsLink,
    },
    Person {
        #[serde(default)]
        person: CommentPerson,
    },
    /// Element kinds added to Feishu after this crate was written.
    #[serde(other)]
    Other,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct TextRun {
    #[serde(default)]
    pub text: String,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct DocsLink {
    #[serde(default)]
    pub url: String,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct CommentPerson {
    #[serde(default)]
    pub user_id: String,
}

/// Fetch one comment with its replies.
///
/// Returns `None` when neither `get` nor the paginated list fallback finds the
/// comment.
pub async fn fetch(rest: &RestClient, target: &CommentRef) -> RestResult<Option<Comment>> {
    let path = file_comment_path(&target.file_token, &target.comment_id);
    match rest
        .get(&path, &[("file_type", target.file_type.as_str())])
        .await
    {
        Ok(data) => parse_comment(data, &target.comment_id).map(Some),
        Err(error) if is_api_code(&error, COMMENT_GET_FALLBACK) => {
            find_via_list(rest, target).await
        }
        Err(error) => Err(error),
    }
}

/// Reply in-thread, falling back to a fresh top-level comment when the
/// comment has no thread (`1069302`).
pub async fn reply(rest: &RestClient, target: &CommentRef, text: &str) -> RestResult<()> {
    let path = comment_replies_path(&target.file_token, &target.comment_id);
    match rest
        .request(
            Method::POST,
            &path,
            Some(&reply_body(text)),
            &[("file_type", target.file_type.as_str())],
        )
        .await
    {
        Ok(_) => Ok(()),
        Err(error) if is_api_code(&error, COMMENT_HAS_NO_THREAD) => {
            reply_top_level(rest, &target.file_token, &target.file_type, text).await
        }
        Err(error) => Err(error),
    }
}

/// Post a fresh top-level comment on the file, without an in-thread probe.
pub async fn reply_top_level(
    rest: &RestClient,
    file_token: &str,
    file_type: &str,
    text: &str,
) -> RestResult<()> {
    rest.request(
        Method::POST,
        &file_comments_path(file_token),
        Some(&top_level_comment_body(text)),
        &[("file_type", file_type)],
    )
    .await
    .map(|_| ())
}

/// Add `emoji_type` to a comment reply.
pub async fn add_reaction(
    rest: &RestClient,
    file_token: &str,
    file_type: &str,
    reply_id: &str,
    emoji_type: &str,
) -> RestResult<()> {
    comment_reaction(rest, file_token, file_type, reply_id, emoji_type, "add").await
}

/// Remove `emoji_type` from a comment reply.
pub async fn remove_reaction(
    rest: &RestClient,
    file_token: &str,
    file_type: &str,
    reply_id: &str,
    emoji_type: &str,
) -> RestResult<()> {
    comment_reaction(rest, file_token, file_type, reply_id, emoji_type, "delete").await
}

async fn comment_reaction(
    rest: &RestClient,
    file_token: &str,
    file_type: &str,
    reply_id: &str,
    emoji_type: &str,
    action: &str,
) -> RestResult<()> {
    rest.request(
        Method::POST,
        &comment_reaction_path(file_token),
        Some(&json!({
            "action": action,
            "reply_id": reply_id,
            "reaction_type": emoji_type,
        })),
        &[("file_type", file_type)],
    )
    .await
    .map(|_| ())
}

async fn find_via_list(rest: &RestClient, target: &CommentRef) -> RestResult<Option<Comment>> {
    let path = file_comments_path(&target.file_token);
    let mut page_token: Option<String> = None;
    for _ in 0..MAX_LIST_PAGES {
        let data = {
            let mut query = vec![
                ("file_type", target.file_type.as_str()),
                ("page_size", LIST_PAGE_SIZE),
            ];
            if let Some(token) = page_token.as_deref() {
                query.push(("page_token", token));
            }
            rest.get(&path, &query).await?
        };
        let CommentListPayload {
            items,
            has_more,
            page_token: next_page_token,
        } = parse_comment_list(data)?;
        if let Some(found) = find_in_page(items, &target.comment_id) {
            return Ok(Some(found));
        }
        match next_page_token {
            Some(token) if has_more && !token.is_empty() => page_token = Some(token),
            _ => break,
        }
    }
    Ok(None)
}

fn find_in_page(items: Vec<CommentPayload>, comment_id: &str) -> Option<Comment> {
    items
        .into_iter()
        .find(|item| item.comment_id.as_deref() == Some(comment_id))
        .map(|item| item.into_comment(comment_id))
}

fn parse_comment(data: Value, fallback_id: &str) -> RestResult<Comment> {
    let raw: CommentPayload = serde_json::from_value(data)
        .map_err(|error| RestError::Message(format!("invalid comment payload: {error}")))?;
    Ok(raw.into_comment(fallback_id))
}

fn parse_comment_list(data: Value) -> RestResult<CommentListPayload> {
    serde_json::from_value(data)
        .map_err(|error| RestError::Message(format!("invalid comment list payload: {error}")))
}

fn is_api_code(error: &RestError, code: i64) -> bool {
    matches!(error, RestError::Api { code: actual, .. } if *actual == code)
}

fn reply_body(text: &str) -> Value {
    json!({ "content": { "elements": [text_element(text)] } })
}

fn top_level_comment_body(text: &str) -> Value {
    json!({
        "reply_list": {
            "replies": [
                { "content": { "elements": [text_element(text)] } }
            ]
        }
    })
}

fn text_element(text: &str) -> Value {
    json!({ "type": "text_run", "text_run": { "text": text } })
}

/// `/open-apis/drive/v1/files/{file_token}/comments`
fn file_comments_path(file_token: &str) -> String {
    format!(
        "/open-apis/drive/v1/files/{}/comments",
        encode_path_segment(file_token)
    )
}

/// `/open-apis/drive/v1/files/{file_token}/comments/{comment_id}`
fn file_comment_path(file_token: &str, comment_id: &str) -> String {
    format!(
        "{}/{}",
        file_comments_path(file_token),
        encode_path_segment(comment_id)
    )
}

/// `/open-apis/drive/v1/files/{file_token}/comments/{comment_id}/replies`
fn comment_replies_path(file_token: &str, comment_id: &str) -> String {
    format!("{}/replies", file_comment_path(file_token, comment_id))
}

/// `/open-apis/drive/v2/files/{file_token}/comments/reaction`
///
/// Comment reactions live on drive v2 (unlike reads and replies, which are
/// drive v1); this is the endpoint the SDK's `CommentSurface` calls.
fn comment_reaction_path(file_token: &str) -> String {
    format!(
        "/open-apis/drive/v2/files/{}/comments/reaction",
        encode_path_segment(file_token)
    )
}

/// Percent-encode one path segment, keeping only RFC 3986 unreserved bytes.
fn encode_path_segment(value: &str) -> String {
    let mut encoded = String::with_capacity(value.len());
    for byte in value.bytes() {
        if byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.' | b'~') {
            encoded.push(char::from(byte));
        } else {
            encoded.push('%');
            encoded.push(hex_digit(byte >> 4));
            encoded.push(hex_digit(byte & 0x0f));
        }
    }
    encoded
}

fn hex_digit(nibble: u8) -> char {
    match nibble {
        0..=9 => char::from(b'0' + nibble),
        _ => char::from(b'A' + nibble - 10),
    }
}

#[derive(Debug, Default, Deserialize)]
struct CommentPayload {
    #[serde(default)]
    comment_id: Option<String>,
    #[serde(default)]
    quote: Option<String>,
    #[serde(default)]
    is_whole: bool,
    #[serde(default)]
    reply_list: ReplyListPayload,
}

impl CommentPayload {
    fn into_comment(self, fallback_id: &str) -> Comment {
        Comment {
            comment_id: self.comment_id.unwrap_or_else(|| fallback_id.to_string()),
            quote: self.quote.filter(|quote| !quote.is_empty()),
            is_whole: self.is_whole,
            replies: self.reply_list.replies,
        }
    }
}

#[derive(Debug, Default, Deserialize)]
struct ReplyListPayload {
    #[serde(default)]
    replies: Vec<CommentReply>,
}

#[derive(Debug, Default, Deserialize)]
struct CommentListPayload {
    #[serde(default)]
    items: Vec<CommentPayload>,
    #[serde(default)]
    has_more: bool,
    #[serde(default)]
    page_token: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn comment_paths_use_drive_v1() {
        assert_eq!(
            file_comments_path("doc_token"),
            "/open-apis/drive/v1/files/doc_token/comments"
        );
        assert_eq!(
            file_comment_path("doc_token", "comment_1"),
            "/open-apis/drive/v1/files/doc_token/comments/comment_1"
        );
        assert_eq!(
            comment_replies_path("doc_token", "comment_1"),
            "/open-apis/drive/v1/files/doc_token/comments/comment_1/replies"
        );
    }

    #[test]
    fn comment_reaction_path_uses_drive_v2() {
        assert_eq!(
            comment_reaction_path("doc_token"),
            "/open-apis/drive/v2/files/doc_token/comments/reaction"
        );
    }

    #[test]
    fn comment_paths_percent_encode_tokens() {
        assert_eq!(
            file_comment_path("doc/token", "comment 1"),
            "/open-apis/drive/v1/files/doc%2Ftoken/comments/comment%201"
        );
        assert_eq!(
            comment_reaction_path("文档"),
            "/open-apis/drive/v2/files/%E6%96%87%E6%A1%A3/comments/reaction"
        );
    }

    #[test]
    fn parses_comment_with_quote_and_replies() {
        let comment = parse_comment(
            json!({
                "comment_id": "comment_1",
                "quote": "selected text",
                "is_whole": false,
                "reply_list": {
                    "replies": [
                        {
                            "reply_id": "reply_1",
                            "content": {
                                "elements": [
                                    { "type": "text_run", "text_run": { "text": "hello " } },
                                    { "type": "docs_link", "docs_link": { "url": "https://example.com" } },
                                    { "type": "person", "person": { "user_id": "ou_1" } },
                                    { "type": "future_element", "payload": 1 }
                                ]
                            }
                        }
                    ]
                }
            }),
            "fallback",
        )
        .unwrap();

        assert_eq!(comment.comment_id, "comment_1");
        assert_eq!(comment.quote.as_deref(), Some("selected text"));
        assert!(!comment.is_whole);
        assert_eq!(comment.replies.len(), 1);
        assert_eq!(comment.replies[0].reply_id.as_deref(), Some("reply_1"));
        assert_eq!(comment.replies[0].text(), "hello https://example.com");
    }

    #[test]
    fn empty_quote_is_normalized_to_none() {
        let comment = parse_comment(json!({ "quote": "" }), "comment_1").unwrap();
        assert_eq!(comment.quote, None);
        assert_eq!(comment.comment_id, "comment_1");
        assert!(comment.replies.is_empty());
    }

    #[test]
    fn missing_comment_id_falls_back_to_the_reference() {
        let comment = parse_comment(json!({}), "comment_1").unwrap();
        assert_eq!(comment.comment_id, "comment_1");
    }

    #[test]
    fn finds_a_comment_in_a_list_page() {
        let page = parse_comment_list(json!({
            "items": [
                { "comment_id": "other", "reply_list": { "replies": [] } },
                { "comment_id": "comment_1", "quote": "q", "reply_list": { "replies": [] } }
            ],
            "has_more": false
        }))
        .unwrap();

        let found = find_in_page(page.items, "comment_1").unwrap();
        assert_eq!(found.comment_id, "comment_1");
        assert_eq!(found.quote.as_deref(), Some("q"));
        assert_eq!(find_in_page(Vec::new(), "comment_1"), None);
    }

    #[test]
    fn comment_payloads_round_trip() {
        let target = CommentRef::new("doc_token", "docx", "comment_1");
        let value = serde_json::to_value(&target).unwrap();
        assert_eq!(serde_json::from_value::<CommentRef>(value).unwrap(), target);

        let comment = Comment {
            comment_id: "comment_1".into(),
            quote: Some("quoted".into()),
            is_whole: true,
            replies: vec![CommentReply {
                reply_id: Some("reply_1".into()),
                content: ReplyContent {
                    elements: vec![
                        ReplyElement::TextRun {
                            text_run: TextRun { text: "hi".into() },
                        },
                        ReplyElement::DocsLink {
                            docs_link: DocsLink {
                                url: "https://example.com".into(),
                            },
                        },
                        ReplyElement::Person {
                            person: CommentPerson {
                                user_id: "ou_1".into(),
                            },
                        },
                    ],
                },
            }],
        };
        let value = serde_json::to_value(&comment).unwrap();
        assert_eq!(serde_json::from_value::<Comment>(value).unwrap(), comment);
    }

    #[test]
    fn unknown_reply_elements_are_preserved_as_other() {
        let element: ReplyElement =
            serde_json::from_value(json!({ "type": "future_element", "x": 1 })).unwrap();
        assert_eq!(element, ReplyElement::Other);
    }

    #[test]
    fn reply_bodies_are_single_text_runs() {
        assert_eq!(
            reply_body("hello"),
            json!({
                "content": {
                    "elements": [
                        { "type": "text_run", "text_run": { "text": "hello" } }
                    ]
                }
            })
        );
        assert_eq!(
            top_level_comment_body("hello"),
            json!({
                "reply_list": {
                    "replies": [
                        {
                            "content": {
                                "elements": [
                                    { "type": "text_run", "text_run": { "text": "hello" } }
                                ]
                            }
                        }
                    ]
                }
            })
        );
    }

    #[test]
    fn api_codes_are_matched_for_fallbacks() {
        assert!(is_api_code(
            &RestError::Api {
                code: 1069307,
                message: "not found".into()
            },
            1069307
        ));
        assert!(is_api_code(
            &RestError::Api {
                code: 1069302,
                message: "no thread".into()
            },
            1069302
        ));
        assert!(!is_api_code(
            &RestError::Api {
                code: 1069302,
                message: "no thread".into()
            },
            1069307
        ));
        assert!(!is_api_code(&RestError::Message("boom".into()), 1069307));
    }

    #[test]
    fn malformed_comment_payload_is_an_error() {
        let error = parse_comment(json!({ "reply_list": "not-a-list" }), "comment_1").unwrap_err();
        match error {
            RestError::Message(message) => assert!(message.contains("invalid comment payload")),
            other => panic!("unexpected error: {other:?}"),
        }
    }

    #[test]
    fn malformed_comment_list_payload_is_an_error() {
        let error = parse_comment_list(json!({ "items": 7 })).unwrap_err();
        match error {
            RestError::Message(message) => {
                assert!(message.contains("invalid comment list payload"));
            }
            other => panic!("unexpected error: {other:?}"),
        }
    }
}
