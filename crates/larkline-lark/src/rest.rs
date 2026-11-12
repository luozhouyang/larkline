//! Raw OpenAPI access for the endpoints the Channel SDK does not model.
//!
//! File comments, VC bot control, chat metadata, reactions and the QR app
//! registration device flow are plain REST endpoints. The TypeScript bridge
//! reached them through `rawClient` / a generic `request()`; here they go
//! through one small client that owns the tenant token cache.

use std::sync::Arc;
use std::time::{Duration, Instant};

use larkline_core::TenantBrand;
use serde_json::{Value, json};
use tokio::sync::Mutex;

#[derive(Debug, thiserror::Error)]
pub enum RestError {
    #[error("http error: {0}")]
    Http(#[from] reqwest::Error),

    #[error("lark api error {code}: {message}")]
    Api { code: i64, message: String },

    #[error("{0}")]
    Message(String),
}

pub type RestResult<T> = std::result::Result<T, RestError>;

#[derive(Debug)]
struct CachedToken {
    value: String,
    expires_at: Instant,
}

#[derive(Clone)]
pub struct RestClient {
    client: reqwest::Client,
    base: String,
    app_id: String,
    app_secret: String,
    token: Arc<Mutex<Option<CachedToken>>>,
}

impl RestClient {
    pub fn new(
        tenant: TenantBrand,
        app_id: impl Into<String>,
        app_secret: impl Into<String>,
    ) -> Self {
        let base = match tenant {
            TenantBrand::Feishu => "https://open.feishu.cn",
            TenantBrand::Lark => "https://open.larksuite.com",
        };
        Self {
            client: reqwest::Client::builder()
                .timeout(Duration::from_secs(30))
                .build()
                .expect("reqwest client"),
            base: base.to_string(),
            app_id: app_id.into(),
            app_secret: app_secret.into(),
            token: Arc::new(Mutex::new(None)),
        }
    }

    pub fn base_url(&self) -> &str {
        &self.base
    }

    /// Tenant access token, cached until shortly before expiry.
    pub async fn tenant_token(&self) -> RestResult<String> {
        let mut cache = self.token.lock().await;
        if let Some(cached) = cache.as_ref() {
            if cached.expires_at > Instant::now() {
                return Ok(cached.value.clone());
            }
        }

        let response = self
            .client
            .post(format!(
                "{}/open-apis/auth/v3/tenant_access_token/internal",
                self.base
            ))
            .json(&json!({ "app_id": self.app_id, "app_secret": self.app_secret }))
            .send()
            .await?
            .error_for_status()?
            .json::<Value>()
            .await?;

        let token = response
            .get("tenant_access_token")
            .and_then(Value::as_str)
            .ok_or_else(|| {
                RestError::Message(
                    response
                        .get("msg")
                        .and_then(Value::as_str)
                        .unwrap_or("tenant_access_token missing from response")
                        .to_string(),
                )
            })?
            .to_string();
        let ttl = response
            .get("expire")
            .and_then(Value::as_u64)
            .unwrap_or(7200);
        // Refresh a minute early so a long request never races expiry.
        let ttl = ttl.saturating_sub(60).max(30);

        *cache = Some(CachedToken {
            value: token.clone(),
            expires_at: Instant::now() + Duration::from_secs(ttl),
        });
        Ok(token)
    }

    pub async fn get(&self, path: &str, query: &[(&str, &str)]) -> RestResult<Value> {
        self.request(reqwest::Method::GET, path, None, query).await
    }

    pub async fn post(&self, path: &str, body: &Value) -> RestResult<Value> {
        self.request(reqwest::Method::POST, path, Some(body), &[])
            .await
    }

    pub async fn patch(&self, path: &str, body: &Value) -> RestResult<Value> {
        self.request(reqwest::Method::PATCH, path, Some(body), &[])
            .await
    }

    pub async fn request(
        &self,
        method: reqwest::Method,
        path: &str,
        body: Option<&Value>,
        query: &[(&str, &str)],
    ) -> RestResult<Value> {
        let token = self.tenant_token().await?;
        let url = format!("{}{}", self.base, normalize_path(path));
        let mut request = self.client.request(method, url).bearer_auth(token);
        if !query.is_empty() {
            request = request.query(query);
        }
        if let Some(body) = body {
            request = request.json(body);
        }

        let response = request.send().await?;
        let status = response.status();
        let text = response.text().await?;
        if !status.is_success() {
            return Err(RestError::Message(format!(
                "http {status} from {path}: {}",
                truncate(&text, 300)
            )));
        }
        // Some endpoints answer 200 with an empty body; the TypeScript client
        // treats that as `{}` (`if (!text) return {}`) instead of a JSON error.
        if text.trim().is_empty() {
            return Ok(Value::Null);
        }
        let value: Value = serde_json::from_str(&text)
            .map_err(|e| RestError::Message(format!("invalid json from {path}: {e}")))?;
        parse_envelope(value)
    }
}

/// Unwrap Feishu's `{ code, msg, data }` envelope.
fn parse_envelope(value: Value) -> RestResult<Value> {
    let code = value.get("code").and_then(Value::as_i64).unwrap_or(0);
    if code != 0 {
        return Err(RestError::Api {
            code,
            message: value
                .get("msg")
                .and_then(Value::as_str)
                .unwrap_or("unknown lark error")
                .to_string(),
        });
    }
    Ok(match value.get("data") {
        Some(data) => data.clone(),
        None => value,
    })
}

fn normalize_path(path: &str) -> String {
    if path.starts_with('/') {
        path.to_string()
    } else {
        format!("/{path}")
    }
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

    #[test]
    fn unwraps_data_envelope() {
        let value = parse_envelope(json!({ "code": 0, "msg": "ok", "data": { "x": 1 } })).unwrap();
        assert_eq!(value["x"], 1);
    }

    #[test]
    fn surfaces_api_errors() {
        let err = parse_envelope(json!({ "code": 99991663, "msg": "invalid token" })).unwrap_err();
        match err {
            RestError::Api { code, message } => {
                assert_eq!(code, 99991663);
                assert!(message.contains("invalid token"));
            }
            other => panic!("unexpected: {other:?}"),
        }
    }

    #[test]
    fn leaves_non_enveloped_payloads_alone() {
        let value = parse_envelope(json!({ "tenant_access_token": "t" })).unwrap();
        assert_eq!(value["tenant_access_token"], "t");
    }
}
