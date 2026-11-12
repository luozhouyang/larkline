//! Feishu/Lark app-registration device flow.
//!
//! This is the Rust port of the npm SDK's `registerApp`: a two-step,
//! **unauthenticated** OAuth 2.0 Device Authorization Grant (RFC 8628) flow
//! that creates a personal agent app and yields its credentials once the user
//! scans a QR code.
//!
//! Both steps are form-encoded POSTs to the accounts host (never the OpenAPI
//! host, and never with a tenant token):
//!
//! | Tenant | Accounts host | Path |
//! | --- | --- | --- |
//! | Feishu | `https://accounts.feishu.cn` | `/oauth/v1/app/registration` |
//! | Lark | `https://accounts.larksuite.com` | `/oauth/v1/app/registration` |
//!
//! * [`begin`] posts `action=begin&archetype=PersonalAgent&auth_method=client_secret&request_user_info=open_id`
//!   and builds the QR URL from the returned `verification_uri_complete`.
//! * [`poll`] posts `action=poll&device_code=...` immediately and then at the
//!   server-provided `interval` until credentials arrive, the QR expires, or
//!   the caller cancels. If the scanned tenant is Lark, the SDK switches from
//!   the Feishu accounts host to the Lark accounts host once and re-polls.
//!
//! The original SDK also supports `addons` (gzip+base64 scope/event deltas)
//! and `appPreset` (prefilled name/avatar/description). The bridge does not
//! need either, so they are deliberately not modelled here.
//!
//! Errors returned by the endpoint use OAuth codes (`access_denied`,
//! `expired_token`, `authorization_pending`, `slow_down`) rather than Feishu's
//! usual `{code, msg}` envelope. `authorization_pending` and `slow_down` are
//! normal polling states; everything terminal becomes a [`QrError`].
//!
//! The registration endpoints are unauthenticated, so this module uses a plain
//! [`reqwest::Client`] instead of `crate::rest::RestClient`, which adds a
//! tenant access token to every request.

use std::time::{Duration, Instant};

use larkline_core::TenantBrand;
use reqwest::{Client, Url};
use serde_json::Value;
use tokio::sync::watch;

/// Accounts host used to create/update Feishu apps.
const FEISHU_ACCOUNTS_BASE: &str = "https://accounts.feishu.cn";
/// Accounts host used to create/update Lark apps.
const LARK_ACCOUNTS_BASE: &str = "https://accounts.larksuite.com";
/// Registration endpoint, relative to the accounts host.
const REGISTRATION_PATH: &str = "/oauth/v1/app/registration";

/// SDK name embedded in the QR URL's `source` parameter.
///
/// The npm SDK tags the value as `node-sdk/<source>`. Keep that convention so
/// the landing page sees the same value as the TypeScript bridge did.
const SDK_SOURCE: &str = "node-sdk";

/// Fallback QR lifetime when `expires_in` is absent from the begin response.
const DEFAULT_EXPIRE_IN: u64 = 600;
/// Fallback poll interval when `interval` is absent from the begin response.
const DEFAULT_INTERVAL: u64 = 5;
/// RFC 8628's recommended increment after the server says `slow_down`.
const SLOW_DOWN_STEP: Duration = Duration::from_secs(5);
/// Per-request timeout while talking to the accounts host.
const REQUEST_TIMEOUT: Duration = Duration::from_secs(30);

/// The begin request body, in the same order as the SDK sends it.
const BEGIN_PARAMS: &[(&str, &str)] = &[
    ("action", "begin"),
    ("archetype", "PersonalAgent"),
    ("auth_method", "client_secret"),
    ("request_user_info", "open_id"),
];

/// Options for starting an app-registration session.
#[derive(Debug, Clone)]
pub struct RegistrationOptions {
    /// Which accounts host to begin against. If a Lark tenant is detected
    /// during polling, the session switches hosts automatically.
    pub domain: TenantBrand,
    /// Analytics tag for the QR URL, e.g. `"larkline"`. The value is sent as
    /// `source=node-sdk/<tag>` to match the npm SDK's convention; empty means
    /// plain `node-sdk`.
    pub source: String,
    /// Existing app id (`cli_...`) to update. Sent as the QR URL's `clientID`
    /// parameter when present.
    pub app_id: Option<String>,
    /// When true, the landing page only permits creating a new app. Sent as
    /// `createOnly=true` on the QR URL. Takes precedence over `app_id` on the
    /// landing page, but both parameters are still passed through.
    pub create_only: bool,
}

impl Default for RegistrationOptions {
    fn default() -> Self {
        Self {
            domain: TenantBrand::Feishu,
            source: "larkline".to_string(),
            app_id: None,
            create_only: false,
        }
    }
}

/// A started registration flow: everything [`poll`] needs plus the QR URL the
/// caller must render.
#[derive(Clone)]
pub struct RegistrationSession {
    /// QR URL (or clickable link) to show to the user.
    pub qr_url: String,
    /// QR lifetime in seconds, from the begin response.
    pub expire_in: u64,
    /// Server-suggested poll interval in seconds (minimum 1).
    pub interval: u64,
    device_code: String,
    domain: TenantBrand,
    /// Monotonic deadline derived from `expire_in` at begin time.
    expires_at: Instant,
}

impl RegistrationSession {
    /// Whether the QR/device code has expired.
    pub fn is_expired(&self) -> bool {
        is_expired_at(self.expires_at, Instant::now())
    }

    /// Time left before the QR/device code expires (zero once expired).
    pub fn remaining(&self) -> Duration {
        remaining_until(self.expires_at, Instant::now())
    }
}

impl std::fmt::Debug for RegistrationSession {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Never print the device code, which is a short-lived credential.
        f.debug_struct("RegistrationSession")
            .field("qr_url", &self.qr_url)
            .field("expire_in", &self.expire_in)
            .field("interval", &self.interval)
            .field("domain", &self.domain)
            .field("remaining", &self.remaining())
            .finish()
    }
}

/// Fresh credentials for the app the user created.
#[derive(Clone)]
pub struct RegistrationResult {
    /// App id (`cli_...`).
    pub app_id: String,
    /// App secret; store it in the keystore, never in logs.
    pub app_secret: String,
    /// Tenant brand reported by the device-flow response, or the accounts
    /// domain the successful poll went to.
    pub tenant: TenantBrand,
    /// `open_id` of the user who scanned the QR code, when reported.
    pub open_id: Option<String>,
}

impl std::fmt::Debug for RegistrationResult {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RegistrationResult")
            .field("app_id", &self.app_id)
            .field("app_secret", &"<redacted>")
            .field("tenant", &self.tenant)
            .field("open_id", &self.open_id)
            .finish()
    }
}

/// Failure modes of the app-registration device flow.
#[derive(Debug, thiserror::Error)]
pub enum QrError {
    /// The accounts host could not be reached, or the HTTP exchange failed.
    #[error("network error: {0}")]
    Network(#[from] reqwest::Error),

    /// The endpoint returned a terminal OAuth error such as `access_denied`.
    #[error("lark registration error {code}: {msg}")]
    Api { code: String, msg: String },

    /// The device code or QR code lifetime elapsed (`expired_token`, or the
    /// local deadline passed).
    #[error("registration QR code expired")]
    Expired,

    /// The caller's cancellation channel fired.
    #[error("registration was aborted")]
    Aborted,

    /// The response was not valid JSON or lacked required fields.
    #[error("malformed registration response: {0}")]
    Malformed(String),
}

/// Start a registration: POST `action=begin` and build the QR URL.
///
/// This performs exactly one unauthenticated request, then returns while the
/// user is still scanning. Call [`poll`] to wait for the app credentials.
pub async fn begin(options: &RegistrationOptions) -> Result<RegistrationSession, QrError> {
    // Mirrors the SDK's up-front validation: an empty app id is a caller bug,
    // not something to silently send as `clientID=`.
    if options.app_id.as_deref().is_some_and(str::is_empty) {
        return Err(QrError::Malformed(
            "app_id must be a non-empty string".to_string(),
        ));
    }

    let client = http_client()?;
    let base = accounts_base(options.domain);
    let value = post_form(&client, base, BEGIN_PARAMS).await?;
    let response = parse_begin(&value)?;
    let qr_url = build_qr_url(
        &response.qr_url,
        &options.source,
        options.app_id.as_deref(),
        options.create_only,
    )?;

    let now = Instant::now();
    Ok(RegistrationSession {
        qr_url,
        expire_in: response.expire_in,
        interval: response.interval,
        device_code: response.device_code,
        domain: options.domain,
        expires_at: deadline_from(now, response.expire_in),
    })
}

/// Poll the registration endpoint until the app is created, the QR expires,
/// or the caller cancels.
///
/// `cancel` is optional. When `Some(receiver)` is supplied, pass a receiver
/// from `tokio::sync::watch::channel(false)` and send `true` to stop polling;
/// a dropped sender also counts as cancellation. The function returns
/// [`QrError::Aborted`] in both cases. Cancellation is observed both between
/// polls and while an HTTP request is in flight.
///
/// Polling starts immediately (the SDK does not wait one interval first) and
/// then sleeps for the server-provided interval. A `slow_down` response adds
/// five seconds to the interval; `authorization_pending` keeps polling. The
/// local deadline starts when [`begin`] returns, so time spent rendering or
/// displaying the QR counts against the QR's lifetime.
pub async fn poll(
    session: &RegistrationSession,
    cancel: Option<watch::Receiver<bool>>,
) -> Result<RegistrationResult, QrError> {
    let client = http_client()?;
    let mut cancel = cancel;
    let mut domain = session.domain;
    let mut base = accounts_base(domain);
    let mut interval = Duration::from_secs(session.interval.max(1));

    loop {
        if cancel_requested(&cancel) {
            return Err(QrError::Aborted);
        }
        if session.is_expired() {
            return Err(QrError::Expired);
        }

        let params = [
            ("action", "poll"),
            ("device_code", session.device_code.as_str()),
        ];
        let value = post_form_or_cancel(&client, base, &params, &mut cancel).await?;

        match classify_poll(&value, domain)? {
            PollOutcome::DomainSwitched => {
                // The SDK switches once, then re-polls immediately with the
                // same device code against the Lark accounts host.
                domain = TenantBrand::Lark;
                base = accounts_base(domain);
                tracing::debug!("registration poll switched to the Lark accounts domain");
                continue;
            }
            PollOutcome::Success {
                app_id,
                app_secret,
                tenant,
                open_id,
            } => {
                return Ok(RegistrationResult {
                    app_id,
                    app_secret,
                    tenant,
                    open_id,
                });
            }
            PollOutcome::Pending => {
                tracing::trace!("registration is still pending");
            }
            PollOutcome::SlowDown => {
                interval += SLOW_DOWN_STEP;
                tracing::debug!(?interval, "registration poll asked to slow down");
            }
        }

        let remaining = session.remaining();
        if remaining.is_zero() {
            return Err(QrError::Expired);
        }
        sleep_or_cancel(interval.min(remaining), &mut cancel).await?;
    }
}

/// A begin response after validation, before the QR URL has been decorated.
#[derive(Debug, PartialEq, Eq)]
struct BeginResponse {
    qr_url: String,
    device_code: String,
    expire_in: u64,
    interval: u64,
}

/// What one poll response means for the state machine.
#[derive(Debug, PartialEq, Eq)]
enum PollOutcome {
    /// The tenant is Lark but we began on the Feishu host: switch and re-poll.
    DomainSwitched,
    /// Credentials arrived.
    Success {
        app_id: String,
        app_secret: String,
        tenant: TenantBrand,
        open_id: Option<String>,
    },
    /// RFC 8628 `authorization_pending`.
    Pending,
    /// RFC 8628 `slow_down`.
    SlowDown,
}

fn http_client() -> Result<Client, QrError> {
    Client::builder()
        .timeout(REQUEST_TIMEOUT)
        .build()
        .map_err(QrError::from)
}

fn accounts_base(domain: TenantBrand) -> &'static str {
    match domain {
        TenantBrand::Feishu => FEISHU_ACCOUNTS_BASE,
        TenantBrand::Lark => LARK_ACCOUNTS_BASE,
    }
}

/// POST a form-encoded registration request and parse the JSON body.
///
/// RFC 8628 terminal/pending errors are returned with HTTP 400, so the body is
/// parsed regardless of status (the SDK does the same by reading
/// `AxiosError.response.data`). A non-JSON body becomes [`QrError::Malformed`].
async fn post_form(client: &Client, base: &str, params: &[(&str, &str)]) -> Result<Value, QrError> {
    let response = client
        .post(format!("{base}{REGISTRATION_PATH}"))
        .form(params)
        .send()
        .await?;
    let status = response.status();
    let body = response.text().await?;
    serde_json::from_str(&body).map_err(|error| {
        QrError::Malformed(format!(
            "http {status} from registration endpoint: {error}: {}",
            truncate(&body, 300)
        ))
    })
}

/// Like [`post_form`], but returns [`QrError::Aborted`] promptly if the
/// cancellation channel fires while the request is in flight.
async fn post_form_or_cancel(
    client: &Client,
    base: &str,
    params: &[(&str, &str)],
    cancel: &mut Option<watch::Receiver<bool>>,
) -> Result<Value, QrError> {
    let request = post_form(client, base, params);
    match cancel.as_mut() {
        Some(receiver) => tokio::select! {
            result = request => result,
            _ = wait_cancel(receiver) => Err(QrError::Aborted),
        },
        None => request.await,
    }
}

/// Resolve when the receiver's value becomes `true` or its sender is dropped.
async fn wait_cancel(receiver: &mut watch::Receiver<bool>) {
    loop {
        if *receiver.borrow() {
            return;
        }
        if receiver.changed().await.is_err() {
            return;
        }
    }
}

fn cancel_requested(cancel: &Option<watch::Receiver<bool>>) -> bool {
    match cancel {
        Some(receiver) => *receiver.borrow() || receiver.has_changed().is_err(),
        None => false,
    }
}

/// Sleep for `delay`, waking early with [`QrError::Aborted`] on cancellation.
async fn sleep_or_cancel(
    delay: Duration,
    cancel: &mut Option<watch::Receiver<bool>>,
) -> Result<(), QrError> {
    match cancel.as_mut() {
        None => {
            tokio::time::sleep(delay).await;
            Ok(())
        }
        Some(receiver) => {
            if *receiver.borrow() {
                return Err(QrError::Aborted);
            }
            tokio::select! {
                _ = tokio::time::sleep(delay) => Ok(()),
                _ = wait_cancel(receiver) => Err(QrError::Aborted),
            }
        }
    }
}

fn deadline_from(now: Instant, expire_in: u64) -> Instant {
    now.checked_add(Duration::from_secs(expire_in))
        .unwrap_or(now)
}

fn remaining_until(deadline: Instant, now: Instant) -> Duration {
    deadline.saturating_duration_since(now)
}

fn is_expired_at(deadline: Instant, now: Instant) -> bool {
    now >= deadline
}

/// Decorate `verification_uri_complete` with the parameters the landing page
/// expects, preserving unrelated query parameters and any fragment.
///
/// Values are added with URLSearchParams `set` semantics: an existing key is
/// updated in place (duplicates removed), otherwise it is appended. This is
/// what the SDK gets from `new URL(...).searchParams.set(...)`.
fn build_qr_url(
    verification_uri_complete: &str,
    source: &str,
    app_id: Option<&str>,
    create_only: bool,
) -> Result<String, QrError> {
    let mut params: Vec<(&str, String)> = vec![
        ("from", "sdk".to_string()),
        ("source", source_tag(source)),
        ("tp", "sdk".to_string()),
    ];
    // Order matches the SDK: createOnly first, then clientID, so the landing
    // page's documented createOnly precedence is preserved.
    if create_only {
        params.push(("createOnly", "true".to_string()));
    }
    if let Some(app_id) = app_id {
        params.push(("clientID", app_id.to_string()));
    }
    apply_query_params(verification_uri_complete, &params)
}

/// The npm SDK sends `node-sdk/<source>` and plain `node-sdk` when no source
/// was supplied. Preserve that for behavioural parity.
fn source_tag(source: &str) -> String {
    if source.is_empty() {
        SDK_SOURCE.to_string()
    } else {
        format!("{SDK_SOURCE}/{source}")
    }
}

fn apply_query_params(raw_url: &str, params: &[(&str, String)]) -> Result<String, QrError> {
    let mut url = Url::parse(raw_url).map_err(|error| {
        QrError::Malformed(format!("invalid verification_uri_complete: {error}"))
    })?;

    let mut pairs: Vec<(String, String)> = url
        .query_pairs()
        .map(|(key, value)| (key.into_owned(), value.into_owned()))
        .collect();
    for (key, value) in params {
        set_query_param(&mut pairs, key, value.clone());
    }

    {
        let mut query = url.query_pairs_mut();
        query.clear();
        query.extend_pairs(
            pairs
                .iter()
                .map(|(key, value)| (key.as_str(), value.as_str())),
        );
    }
    Ok(url.to_string())
}

/// `URLSearchParams.set`: replace the first matching value in place and drop
/// later duplicates, or append when the key is absent.
fn set_query_param(pairs: &mut Vec<(String, String)>, key: &str, value: String) {
    match pairs.iter().position(|(existing, _)| existing == key) {
        Some(first) => {
            pairs[first].1 = value;
            let mut index = first + 1;
            while index < pairs.len() {
                if pairs[index].0 == key {
                    pairs.remove(index);
                } else {
                    index += 1;
                }
            }
        }
        None => pairs.push((key.to_string(), value)),
    }
}

fn parse_begin(value: &Value) -> Result<BeginResponse, QrError> {
    if let Some(code) = non_empty_str(value, "error") {
        return Err(protocol_error(value, code));
    }

    let qr_url = required_str(value, "verification_uri_complete")?.to_string();
    let device_code = required_str(value, "device_code")?.to_string();
    let expire_in = value
        .get("expires_in")
        .and_then(Value::as_u64)
        .unwrap_or(DEFAULT_EXPIRE_IN);
    // A zero interval would busy-loop the endpoint; the platform sends 5.
    let interval = value
        .get("interval")
        .and_then(Value::as_u64)
        .unwrap_or(DEFAULT_INTERVAL)
        .max(1);

    Ok(BeginResponse {
        qr_url,
        device_code,
        expire_in,
        interval,
    })
}

/// Classify one poll response. Pure, so it can be tested without a network.
fn classify_poll(value: &Value, active_domain: TenantBrand) -> Result<PollOutcome, QrError> {
    let brand = value
        .get("user_info")
        .and_then(|info| info.get("tenant_brand"))
        .and_then(Value::as_str)
        .and_then(parse_tenant);

    // The SDK checks the domain switch before checking for credentials: a
    // Lark success response is handled by re-polling the Lark accounts host.
    if brand == Some(TenantBrand::Lark) && active_domain == TenantBrand::Feishu {
        return Ok(PollOutcome::DomainSwitched);
    }

    let app_id = non_empty_str(value, "client_id");
    let app_secret = non_empty_str(value, "client_secret");
    match (app_id, app_secret) {
        (Some(app_id), Some(app_secret)) => {
            let open_id = value
                .get("user_info")
                .and_then(|info| info.get("open_id"))
                .and_then(Value::as_str)
                .filter(|open_id| !open_id.is_empty())
                .map(str::to_string);
            Ok(PollOutcome::Success {
                app_id: app_id.to_string(),
                app_secret: app_secret.to_string(),
                tenant: brand.unwrap_or(active_domain),
                open_id,
            })
        }
        (None, None) => match non_empty_str(value, "error") {
            Some("authorization_pending") => Ok(PollOutcome::Pending),
            Some("slow_down") => Ok(PollOutcome::SlowDown),
            Some(code) => Err(protocol_error(value, code)),
            None => Err(QrError::Malformed(
                "registration response has neither credentials nor error".to_string(),
            )),
        },
        _ => Err(QrError::Malformed(
            "registration response has only one of client_id/client_secret".to_string(),
        )),
    }
}

fn parse_tenant(brand: &str) -> Option<TenantBrand> {
    match brand {
        "feishu" => Some(TenantBrand::Feishu),
        "lark" => Some(TenantBrand::Lark),
        _ => None,
    }
}

fn protocol_error(value: &Value, code: &str) -> QrError {
    if code == "expired_token" {
        QrError::Expired
    } else {
        QrError::Api {
            code: code.to_string(),
            msg: error_description(value).to_string(),
        }
    }
}

fn error_description(value: &Value) -> &str {
    value
        .get("error_description")
        .and_then(Value::as_str)
        .filter(|description| !description.is_empty())
        .unwrap_or("unknown error")
}

fn non_empty_str<'a>(value: &'a Value, field: &str) -> Option<&'a str> {
    value
        .get(field)
        .and_then(Value::as_str)
        .filter(|field| !field.is_empty())
}

fn required_str<'a>(value: &'a Value, field: &str) -> Result<&'a str, QrError> {
    non_empty_str(value, field)
        .ok_or_else(|| QrError::Malformed(format!("{field} missing from registration response")))
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
    use serde_json::json;

    fn query_values(url: &str, key: &str) -> Vec<String> {
        let parsed = Url::parse(url).expect("test URL");
        parsed
            .query_pairs()
            .filter(|(name, _)| name == key)
            .map(|(_, value)| value.into_owned())
            .collect()
    }

    fn query_value(url: &str, key: &str) -> Option<String> {
        query_values(url, key).into_iter().next()
    }

    fn test_session(expire_in: u64) -> RegistrationSession {
        let now = Instant::now();
        RegistrationSession {
            qr_url: "https://example.invalid/qr".to_string(),
            expire_in,
            interval: 5,
            device_code: "device-code".to_string(),
            domain: TenantBrand::Feishu,
            expires_at: deadline_from(now, expire_in),
        }
    }

    #[test]
    fn endpoint_uses_the_accounts_hosts_not_open_api() {
        assert_eq!(
            format!("{FEISHU_ACCOUNTS_BASE}{REGISTRATION_PATH}"),
            "https://accounts.feishu.cn/oauth/v1/app/registration"
        );
        assert_eq!(
            format!("{LARK_ACCOUNTS_BASE}{REGISTRATION_PATH}"),
            "https://accounts.larksuite.com/oauth/v1/app/registration"
        );
        assert_eq!(accounts_base(TenantBrand::Feishu), FEISHU_ACCOUNTS_BASE);
        assert_eq!(accounts_base(TenantBrand::Lark), LARK_ACCOUNTS_BASE);
    }

    #[test]
    fn begin_response_parses_fields_and_defaults() {
        let value = json!({
            "verification_uri_complete": "https://accounts.feishu.cn/page?code=abc",
            "device_code": "dev-1",
            "expires_in": 300,
            "interval": 2
        });
        assert_eq!(
            parse_begin(&value).unwrap(),
            BeginResponse {
                qr_url: "https://accounts.feishu.cn/page?code=abc".to_string(),
                device_code: "dev-1".to_string(),
                expire_in: 300,
                interval: 2,
            }
        );

        let minimal = json!({
            "verification_uri_complete": "https://accounts.feishu.cn/page",
            "device_code": "dev-2"
        });
        let parsed = parse_begin(&minimal).unwrap();
        assert_eq!(parsed.expire_in, DEFAULT_EXPIRE_IN);
        assert_eq!(parsed.interval, DEFAULT_INTERVAL);
    }

    #[test]
    fn begin_response_requires_qr_url_and_device_code() {
        let no_url = json!({ "device_code": "dev" });
        assert!(matches!(
            parse_begin(&no_url),
            Err(QrError::Malformed(message)) if message.contains("verification_uri_complete")
        ));

        let no_device_code =
            json!({ "verification_uri_complete": "https://accounts.feishu.cn/page" });
        assert!(matches!(
            parse_begin(&no_device_code),
            Err(QrError::Malformed(message)) if message.contains("device_code")
        ));

        let empty_url = json!({
            "verification_uri_complete": "",
            "device_code": "dev"
        });
        assert!(matches!(
            parse_begin(&empty_url),
            Err(QrError::Malformed(_))
        ));
    }

    #[test]
    fn begin_maps_terminal_errors() {
        let expired = json!({ "error": "expired_token", "error_description": "gone" });
        assert!(matches!(parse_begin(&expired), Err(QrError::Expired)));

        let denied = json!({ "error": "access_denied", "error_description": "nope" });
        match parse_begin(&denied) {
            Err(QrError::Api { code, msg }) => {
                assert_eq!(code, "access_denied");
                assert_eq!(msg, "nope");
            }
            other => panic!("unexpected response: {other:?}"),
        }
    }

    #[test]
    fn qr_url_appends_protocol_params_and_preserves_existing_ones() {
        let url = build_qr_url(
            "https://accounts.feishu.cn/page?code=ABC%2F123&state=a+b#done",
            "larkline",
            Some("cli_123"),
            true,
        )
        .unwrap();

        assert_eq!(query_value(&url, "code").as_deref(), Some("ABC/123"));
        assert_eq!(query_value(&url, "state").as_deref(), Some("a b"));
        assert_eq!(query_value(&url, "from").as_deref(), Some("sdk"));
        assert_eq!(
            query_value(&url, "source").as_deref(),
            Some("node-sdk/larkline")
        );
        assert_eq!(query_value(&url, "tp").as_deref(), Some("sdk"));
        assert_eq!(query_value(&url, "clientID").as_deref(), Some("cli_123"));
        assert_eq!(query_value(&url, "createOnly").as_deref(), Some("true"));
        assert_eq!(Url::parse(&url).unwrap().fragment(), Some("done"));
    }

    #[test]
    fn qr_url_replaces_protocol_params_without_duplicates() {
        let url = build_qr_url(
            "https://accounts.feishu.cn/page?from=old&foo=1&from=dup&source=old&tp=old",
            "",
            None,
            false,
        )
        .unwrap();

        assert_eq!(query_values(&url, "from"), vec!["sdk".to_string()]);
        assert_eq!(query_values(&url, "source"), vec!["node-sdk".to_string()]);
        assert_eq!(query_values(&url, "tp"), vec!["sdk".to_string()]);
        assert_eq!(query_value(&url, "foo").as_deref(), Some("1"));
        assert!(query_value(&url, "createOnly").is_none());
    }

    #[test]
    fn qr_url_omits_optional_params() {
        let url = build_qr_url("https://accounts.feishu.cn/page", "larkline", None, false).unwrap();
        assert!(query_value(&url, "clientID").is_none());
        assert!(query_value(&url, "createOnly").is_none());
        assert_eq!(
            query_value(&url, "source").as_deref(),
            Some("node-sdk/larkline")
        );
    }

    #[test]
    fn qr_url_sets_both_params_when_both_are_present() {
        let url = build_qr_url(
            "https://accounts.feishu.cn/page",
            "larkline",
            Some("cli_123"),
            true,
        )
        .unwrap();
        assert_eq!(query_value(&url, "clientID").as_deref(), Some("cli_123"));
        assert_eq!(query_value(&url, "createOnly").as_deref(), Some("true"));
    }

    #[test]
    fn qr_url_encodes_query_values() {
        let url = build_qr_url(
            "https://accounts.feishu.cn/page",
            "larkline/rust sdk",
            Some("cli_a/b"),
            false,
        )
        .unwrap();

        assert!(!url.contains(' '));
        assert_eq!(
            query_value(&url, "source").as_deref(),
            Some("node-sdk/larkline/rust sdk")
        );
        assert_eq!(query_value(&url, "clientID").as_deref(), Some("cli_a/b"));
    }

    #[test]
    fn qr_url_rejects_relative_or_invalid_urls() {
        assert!(matches!(
            build_qr_url("/relative", "larkline", None, false),
            Err(QrError::Malformed(_))
        ));
        assert!(matches!(
            build_qr_url("not a url", "larkline", None, false),
            Err(QrError::Malformed(_))
        ));
    }

    #[test]
    fn source_tags_follow_the_sdk_convention() {
        assert_eq!(source_tag("larkline"), "node-sdk/larkline");
        assert_eq!(source_tag(""), "node-sdk");
    }

    #[test]
    fn expiry_math_reports_remaining_and_expired() {
        let now = Instant::now();
        let deadline = deadline_from(now, 600);
        assert_eq!(deadline, now + Duration::from_secs(600));
        assert_eq!(remaining_until(deadline, now), Duration::from_secs(600));
        assert_eq!(remaining_until(deadline, deadline), Duration::ZERO);
        assert!(!is_expired_at(deadline, now));
        assert!(is_expired_at(deadline, deadline));

        let session = test_session(60);
        assert!(!session.is_expired());
        assert!(session.remaining() > Duration::ZERO);
        assert!(session.remaining() <= Duration::from_secs(60));
    }

    #[test]
    fn poll_classifies_success() {
        let value = json!({
            "client_id": "cli_1",
            "client_secret": "secret",
            "user_info": { "open_id": "ou_1", "tenant_brand": "feishu" }
        });
        assert_eq!(
            classify_poll(&value, TenantBrand::Feishu).unwrap(),
            PollOutcome::Success {
                app_id: "cli_1".to_string(),
                app_secret: "secret".to_string(),
                tenant: TenantBrand::Feishu,
                open_id: Some("ou_1".to_string()),
            }
        );
    }

    #[test]
    fn poll_switches_to_lark_before_resolving_credentials() {
        let value = json!({
            "client_id": "cli_1",
            "client_secret": "secret",
            "user_info": { "open_id": "ou_1", "tenant_brand": "lark" }
        });
        assert_eq!(
            classify_poll(&value, TenantBrand::Feishu).unwrap(),
            PollOutcome::DomainSwitched
        );
        assert_eq!(
            classify_poll(&value, TenantBrand::Lark).unwrap(),
            PollOutcome::Success {
                app_id: "cli_1".to_string(),
                app_secret: "secret".to_string(),
                tenant: TenantBrand::Lark,
                open_id: Some("ou_1".to_string()),
            }
        );
    }

    #[test]
    fn poll_classifies_pending_and_slow_down() {
        assert_eq!(
            classify_poll(
                &json!({ "error": "authorization_pending" }),
                TenantBrand::Feishu
            )
            .unwrap(),
            PollOutcome::Pending
        );
        assert_eq!(
            classify_poll(&json!({ "error": "slow_down" }), TenantBrand::Feishu).unwrap(),
            PollOutcome::SlowDown
        );
    }

    #[test]
    fn poll_maps_terminal_errors() {
        assert!(matches!(
            classify_poll(&json!({ "error": "expired_token" }), TenantBrand::Feishu),
            Err(QrError::Expired)
        ));

        match classify_poll(
            &json!({ "error": "access_denied", "error_description": "denied" }),
            TenantBrand::Feishu,
        ) {
            Err(QrError::Api { code, msg }) => {
                assert_eq!(code, "access_denied");
                assert_eq!(msg, "denied");
            }
            other => panic!("unexpected response: {other:?}"),
        }

        match classify_poll(&json!({ "error": "invalid_grant" }), TenantBrand::Feishu) {
            Err(QrError::Api { code, msg }) => {
                assert_eq!(code, "invalid_grant");
                assert_eq!(msg, "unknown error");
            }
            other => panic!("unexpected response: {other:?}"),
        }
    }

    #[test]
    fn poll_rejects_malformed_payloads() {
        assert!(matches!(
            classify_poll(&json!({}), TenantBrand::Feishu),
            Err(QrError::Malformed(_))
        ));
        assert!(matches!(
            classify_poll(&json!({ "client_id": "cli_1" }), TenantBrand::Feishu),
            Err(QrError::Malformed(_))
        ));
        assert!(matches!(
            classify_poll(&json!({ "client_secret": "secret" }), TenantBrand::Feishu),
            Err(QrError::Malformed(_))
        ));
        assert!(matches!(
            classify_poll(
                &json!({ "client_id": "", "client_secret": "", "error": "" }),
                TenantBrand::Feishu
            ),
            Err(QrError::Malformed(_))
        ));
    }

    #[test]
    fn registration_result_debug_redacts_the_secret() {
        let result = RegistrationResult {
            app_id: "cli_1".to_string(),
            app_secret: "super-secret".to_string(),
            tenant: TenantBrand::Feishu,
            open_id: None,
        };
        let debug = format!("{result:?}");
        assert!(debug.contains("cli_1"));
        assert!(debug.contains("<redacted>"));
        assert!(!debug.contains("super-secret"));
    }

    #[test]
    fn qr_error_messages_are_actionable() {
        assert_eq!(QrError::Expired.to_string(), "registration QR code expired");
        assert_eq!(QrError::Aborted.to_string(), "registration was aborted");
        assert_eq!(
            QrError::Api {
                code: "access_denied".to_string(),
                msg: "no".to_string(),
            }
            .to_string(),
            "lark registration error access_denied: no"
        );
    }

    #[tokio::test]
    async fn begin_rejects_empty_app_id_before_network() {
        let options = RegistrationOptions {
            app_id: Some(String::new()),
            ..RegistrationOptions::default()
        };
        let error = begin(&options).await.unwrap_err();
        assert!(matches!(
            error,
            QrError::Malformed(message) if message.contains("app_id")
        ));
    }

    #[tokio::test]
    async fn poll_aborts_when_cancel_is_already_true() {
        let session = test_session(600);
        let (_sender, receiver) = watch::channel(true);
        let error = poll(&session, Some(receiver)).await.unwrap_err();
        assert!(matches!(error, QrError::Aborted));
    }

    #[tokio::test]
    async fn poll_aborts_when_cancel_sender_is_dropped() {
        let session = test_session(600);
        let (sender, receiver) = watch::channel(false);
        drop(sender);
        let error = poll(&session, Some(receiver)).await.unwrap_err();
        assert!(matches!(error, QrError::Aborted));
    }

    #[tokio::test]
    async fn poll_expires_without_network_when_deadline_passed() {
        let session = test_session(0);
        assert!(session.is_expired());
        let error = poll(&session, None).await.unwrap_err();
        assert!(matches!(error, QrError::Expired));
    }
}
