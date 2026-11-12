//! Runtime app-owner resolution — partial port of `src/policy/owner.ts`.
//!
//! The TypeScript bridge resolves the Feishu app owner once at startup and
//! refreshes it every 30 minutes, then lets `canRunAdminCommand` accept the
//! owner in addition to `access.admins`. The owner is read from
//! `GET /open-apis/application/v6/applications/{app_id}` with
//! `lang=zh_cn&user_id_type=open_id`; the SDK surfaces
//! `data.app.owner.owner_id` as `ownerId`. That is exactly what
//! [`RestOwnerSource`] requests, so no `larkline-lark` helper is needed.
//!
//! # Failure mode (fail closed)
//!
//! Admin commands stay deny-by-default: a missing profile app id, a missing
//! `application-info` scope, an HTTP/API error, or an empty `owner_id` never
//! grants a non-admin access. `access.admins` (and the Rust-only legacy
//! `allowed_users` fallback used while `admins` is empty) is checked first and
//! keeps working without any network call. A previously resolved owner remains
//! usable if a later refresh fails, mirroring the TypeScript controller which
//! keeps `botOwnerId` on a failed refresh; the next attempt is delayed by the
//! refresh interval so a broken API cannot turn every command into a request.
//!
//! # Differences from the TypeScript controller
//!
//! The TypeScript bridge refreshes eagerly on startup and on a 30-minute
//! timer. This bridge resolves lazily on the first admin-gated command and
//! then caches for 30 minutes: deployments that never run `/config set` or
//! `/meeting` pay no API call, and the bridge needs no extra background task
//! (whose lifecycle the CLI would otherwise have to own). The cached/refresh
//! semantics and the 30-minute interval match. Each lookup is bounded at five
//! seconds: the CLI awaits `handle_message`, so a hung owner request must not
//! stall the event loop; the timeout is cached as a failed refresh.

use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use larkline_core::ProfileConfig;
use larkline_lark::RestClient;
use tokio::sync::Mutex;

/// How long a resolved owner (or a failed attempt) is trusted before the next
/// lookup, matching `OWNER_REFRESH_INTERVAL_MS` in `src/policy/owner.ts`.
pub const OWNER_REFRESH_INTERVAL: Duration = Duration::from_secs(30 * 60);

/// Hard ceiling for one owner lookup. The bridge awaits this check inside the
/// command handler (the CLI event loop awaits `handle_message`), so a hung
/// Feishu request must not stall message processing. A timeout is cached like
/// any other failed refresh (fail closed for the next 30 minutes).
const OWNER_FETCH_TIMEOUT: Duration = Duration::from_secs(5);

const OWNER_APP_PATH: &str = "/open-apis/application/v6/applications";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum OwnerRefreshState {
    /// Never attempted (or attempted before any result was recorded).
    #[default]
    Unknown,
    Ok,
    Failed,
}

impl OwnerRefreshState {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Unknown => "unknown",
            Self::Ok => "ok",
            Self::Failed => "failed",
        }
    }
}

/// Point-in-time view of the owner cache (for diagnostics/tests).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OwnerSnapshot {
    /// Last successfully resolved owner, if any. Kept across failed refreshes.
    pub owner_id: Option<String>,
    pub state: OwnerRefreshState,
    /// When the last successful or failed attempt completed.
    pub refreshed_at: Option<Instant>,
    pub error: Option<String>,
}

/// Fetch the app owner for one app id.
///
/// `Ok(None)` means the API answered but carried no usable owner (treated as a
/// failed refresh by [`OwnerGate`]); `Err` carries a diagnostic string.
#[async_trait]
pub trait OwnerSource: Send + Sync {
    async fn fetch_owner(&self, app_id: &str) -> Result<Option<String>, String>;
}

/// Owner lookup over the tenant-token REST client.
#[derive(Clone)]
pub struct RestOwnerSource {
    rest: RestClient,
}

impl RestOwnerSource {
    pub fn new(rest: RestClient) -> Self {
        Self { rest }
    }
}

#[async_trait]
impl OwnerSource for RestOwnerSource {
    async fn fetch_owner(&self, app_id: &str) -> Result<Option<String>, String> {
        let path = format!("{OWNER_APP_PATH}/{app_id}");
        let data: serde_json::Value = self
            .rest
            .get(&path, &[("lang", "zh_cn"), ("user_id_type", "open_id")])
            .await
            .map_err(|error| error.to_string())?;
        Ok(parse_owner_id(&data))
    }
}

/// Extract `app.owner.owner_id` from an application-get response body.
///
/// `RestClient` already unwraps the `{code,msg,data}` envelope, so the value is
/// the `data` object; accepting a top-level `owner_id` as well makes the parser
/// tolerant of a pre-unwrapped shape. `creator_id` is deliberately *not* used:
/// after an ownership transfer it can differ from the current owner.
pub fn parse_owner_id(value: &serde_json::Value) -> Option<String> {
    let app = value.get("app").unwrap_or(value);
    app.pointer("/owner/owner_id")
        .or_else(|| app.get("owner_id"))
        .and_then(serde_json::Value::as_str)
        .map(str::trim)
        .filter(|owner| !owner.is_empty())
        .map(str::to_string)
}

/// Cached owner resolver shared by the `/config set` and `/meeting` gates.
pub struct OwnerGate {
    source: Arc<dyn OwnerSource>,
    refresh_interval: Duration,
    cache: Mutex<OwnerCache>,
}

#[derive(Debug, Default)]
struct OwnerCache {
    owner_id: Option<String>,
    state: OwnerRefreshState,
    refreshed_at: Option<Instant>,
    error: Option<String>,
}

impl OwnerGate {
    pub fn new(source: Arc<dyn OwnerSource>) -> Self {
        Self::with_interval(source, OWNER_REFRESH_INTERVAL)
    }

    pub(crate) fn with_interval(source: Arc<dyn OwnerSource>, refresh_interval: Duration) -> Self {
        Self {
            source,
            refresh_interval,
            cache: Mutex::new(OwnerCache::default()),
        }
    }

    /// Resolve the runtime owner, refreshing once per interval.
    ///
    /// Returns the cached owner if a refresh fails; returns `None` when no
    /// owner has ever been resolved. Callers must treat `None` as "deny the
    /// owner exemption" (fail closed), never as "allow".
    pub async fn owner_id(&self, app_id: &str) -> Option<String> {
        self.resolve_at(app_id, Instant::now()).await.owner_id
    }

    /// Last-known snapshot without triggering a lookup.
    pub async fn snapshot(&self) -> OwnerSnapshot {
        let cache = lock(&self.cache).await;
        snapshot(&cache)
    }

    async fn resolve_at(&self, app_id: &str, now: Instant) -> OwnerSnapshot {
        let mut cache = lock(&self.cache).await;
        if let Some(refreshed_at) = cache.refreshed_at {
            if now.saturating_duration_since(refreshed_at) < self.refresh_interval {
                return snapshot(&cache);
            }
        }

        match tokio::time::timeout(OWNER_FETCH_TIMEOUT, self.source.fetch_owner(app_id)).await {
            Ok(Ok(Some(owner_id))) => {
                tracing::info!(app_id, owner_id, "runtime app owner resolved");
                cache.owner_id = Some(owner_id);
                cache.state = OwnerRefreshState::Ok;
                cache.error = None;
            }
            Ok(Ok(None)) => {
                let message = "application owner missing from API response".to_string();
                tracing::warn!(
                    app_id,
                    error = %message,
                    cached_owner = cache.owner_id.as_deref().unwrap_or("-"),
                    "cannot resolve runtime app owner; admin commands fail closed for non-admins"
                );
                cache.state = OwnerRefreshState::Failed;
                cache.error = Some(message);
            }
            Ok(Err(error)) => {
                tracing::warn!(
                    app_id,
                    error = %error,
                    cached_owner = cache.owner_id.as_deref().unwrap_or("-"),
                    "cannot resolve runtime app owner; admin commands fail closed for non-admins"
                );
                cache.state = OwnerRefreshState::Failed;
                cache.error = Some(error);
            }
            Err(_elapsed) => {
                let error = format!(
                    "application owner lookup timed out after {}s",
                    OWNER_FETCH_TIMEOUT.as_secs()
                );
                tracing::warn!(
                    app_id,
                    error = %error,
                    cached_owner = cache.owner_id.as_deref().unwrap_or("-"),
                    "cannot resolve runtime app owner; admin commands fail closed for non-admins"
                );
                cache.state = OwnerRefreshState::Failed;
                cache.error = Some(error);
            }
        }
        cache.refreshed_at = Some(now);
        snapshot(&cache)
    }
}

/// Shared admin-command gate: `access.admins`, the Rust-only legacy
/// `allowed_users` fallback (only while `admins` is empty), or the runtime
/// owner.
///
/// This is the Rust counterpart of the TypeScript `canRunAdminCommand`
/// (`src/policy/access.ts`) plus the pre-existing meeting/config fallback.
/// Pass `None` for `owner_id` to evaluate only the static lists (no lookup).
pub fn can_run_admin_command(
    profile: &ProfileConfig,
    owner_id: Option<&str>,
    sender_id: &str,
) -> bool {
    let Some(access) = profile.access.as_ref() else {
        return is_runtime_owner(owner_id, sender_id);
    };
    if access.admins.iter().any(|id| id == sender_id) {
        return true;
    }
    // Rust-only compatibility with the previous `can_run_meeting_command`:
    // while no admins are configured, an allowed user may administer. The
    // TypeScript bridge has no such fallback; it stays for configs that have
    // not set `access.admins` yet.
    if access.admins.is_empty() && access.allowed_users.iter().any(|id| id == sender_id) {
        return true;
    }
    is_runtime_owner(owner_id, sender_id)
}

/// Whether `sender_id` is the resolved non-empty runtime owner.
pub fn is_runtime_owner(owner_id: Option<&str>, sender_id: &str) -> bool {
    owner_id.is_some_and(|owner| !owner.is_empty() && owner == sender_id)
}

fn snapshot(cache: &OwnerCache) -> OwnerSnapshot {
    OwnerSnapshot {
        owner_id: cache.owner_id.clone(),
        state: cache.state,
        refreshed_at: cache.refreshed_at,
        error: cache.error.clone(),
    }
}

async fn lock<T>(mutex: &Mutex<T>) -> tokio::sync::MutexGuard<'_, T> {
    mutex.lock().await
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use larkline_core::config::{Access, AgentKind};
    use serde_json::json;

    use async_trait::async_trait;

    use super::*;

    struct FakeSource {
        results: std::sync::Mutex<Vec<Result<Option<String>, String>>>,
        calls: AtomicUsize,
    }

    impl FakeSource {
        fn new(results: Vec<Result<Option<String>, String>>) -> Arc<Self> {
            Arc::new(Self {
                results: std::sync::Mutex::new(results),
                calls: AtomicUsize::new(0),
            })
        }

        fn calls(&self) -> usize {
            self.calls.load(Ordering::SeqCst)
        }
    }

    #[async_trait]
    impl OwnerSource for FakeSource {
        async fn fetch_owner(&self, _app_id: &str) -> Result<Option<String>, String> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            self.results
                .lock()
                .expect("results")
                .pop()
                .unwrap_or(Err("no more fake results".to_string()))
        }
    }

    fn profile() -> ProfileConfig {
        ProfileConfig {
            agent_kind: AgentKind::Claude,
            access: Some(Access::default()),
            ..ProfileConfig::default()
        }
    }

    #[test]
    fn parses_the_application_owner_from_the_api_shape() {
        assert_eq!(
            parse_owner_id(&json!({
                "app": {
                    "app_id": "cli_test",
                    "owner": { "owner_id": "ou_owner", "name": "Owner" }
                }
            }))
            .as_deref(),
            Some("ou_owner")
        );
        // RestClient unwraps `{code,msg,data}`; a bare owner object is accepted.
        assert_eq!(
            parse_owner_id(&json!({ "owner_id": " ou_direct " })).as_deref(),
            Some("ou_direct")
        );
    }

    #[test]
    fn missing_or_blank_owner_is_not_a_lookup_result() {
        assert_eq!(
            parse_owner_id(&json!({ "app": { "app_id": "cli_test" } })),
            None
        );
        assert_eq!(
            parse_owner_id(&json!({ "app": { "owner": { "owner_id": "  " } } })),
            None
        );
        // `creator_id` is not the owner (ownership can be transferred).
        assert_eq!(
            parse_owner_id(&json!({ "app": { "creator_id": "ou_creator" } })),
            None
        );
    }

    #[tokio::test]
    async fn resolves_once_and_then_serves_the_cache() {
        let t0 = Instant::now();
        let source = FakeSource::new(vec![Ok(Some("ou_owner".to_string()))]);
        let gate = OwnerGate::with_interval(source.clone(), Duration::from_secs(60));

        let first = gate.resolve_at("cli_test", t0).await;
        assert_eq!(first.owner_id.as_deref(), Some("ou_owner"));
        assert_eq!(first.state, OwnerRefreshState::Ok);
        assert_eq!(first.refreshed_at, Some(t0));
        assert_eq!(source.calls(), 1);

        let cached = gate
            .resolve_at("cli_test", t0 + Duration::from_secs(30))
            .await;
        assert_eq!(cached.owner_id.as_deref(), Some("ou_owner"));
        assert_eq!(source.calls(), 1);
    }

    #[tokio::test]
    async fn refreshes_after_the_interval() {
        let t0 = Instant::now();
        let source = FakeSource::new(vec![
            Ok(Some("ou_second".to_string())),
            Ok(Some("ou_first".to_string())),
        ]);
        let gate = OwnerGate::with_interval(source.clone(), Duration::from_secs(60));

        assert_eq!(
            gate.resolve_at("cli_test", t0).await.owner_id.as_deref(),
            Some("ou_first")
        );
        let refreshed = gate
            .resolve_at("cli_test", t0 + Duration::from_secs(61))
            .await;
        assert_eq!(refreshed.owner_id.as_deref(), Some("ou_second"));
        assert_eq!(source.calls(), 2);
    }

    #[tokio::test]
    async fn failed_refresh_keeps_the_cached_owner_and_retries_later() {
        let t0 = Instant::now();
        let source = FakeSource::new(vec![
            Ok(Some("ou_new".to_string())),
            Err("permission denied".to_string()),
            Ok(Some("ou_owner".to_string())),
        ]);
        let gate = OwnerGate::with_interval(source.clone(), Duration::from_secs(60));

        let first = gate.resolve_at("cli_test", t0).await;
        assert_eq!(first.owner_id.as_deref(), Some("ou_owner"));

        let failed = gate
            .resolve_at("cli_test", t0 + Duration::from_secs(61))
            .await;
        assert_eq!(failed.owner_id.as_deref(), Some("ou_owner"));
        assert_eq!(failed.state, OwnerRefreshState::Failed);
        assert_eq!(failed.error.as_deref(), Some("permission denied"));
        assert_eq!(source.calls(), 2);

        // The failure is cached too; no retry storm inside the interval.
        let cached = gate
            .resolve_at("cli_test", t0 + Duration::from_secs(90))
            .await;
        assert_eq!(cached.owner_id.as_deref(), Some("ou_owner"));
        assert_eq!(source.calls(), 2);

        let recovered = gate
            .resolve_at("cli_test", t0 + Duration::from_secs(122))
            .await;
        assert_eq!(recovered.owner_id.as_deref(), Some("ou_new"));
        assert_eq!(recovered.state, OwnerRefreshState::Ok);
        assert_eq!(source.calls(), 3);
    }

    #[tokio::test]
    async fn failed_refresh_without_a_cache_resolves_to_none() {
        let t0 = Instant::now();
        let source = FakeSource::new(vec![Ok(None)]);
        let gate = OwnerGate::with_interval(source, Duration::from_secs(60));

        let result = gate.resolve_at("cli_test", t0).await;
        assert_eq!(result.owner_id, None);
        assert_eq!(result.state, OwnerRefreshState::Failed);
        assert_eq!(
            result.error.as_deref(),
            Some("application owner missing from API response")
        );
    }

    #[test]
    fn admin_gate_accepts_admins_legacy_allowed_users_and_the_owner() {
        let mut profile = profile();
        profile.access = Some(Access {
            admins: vec!["ou_admin".into()],
            allowed_users: vec!["ou_user".into()],
            ..Access::default()
        });
        assert!(can_run_admin_command(&profile, None, "ou_admin"));
        // With admins configured, allowedUsers is not an admin list (same as
        // the TypeScript gate) and the owner still wins.
        assert!(!can_run_admin_command(&profile, None, "ou_user"));
        assert!(can_run_admin_command(
            &profile,
            Some("ou_owner"),
            "ou_owner"
        ));

        profile.access = Some(Access {
            allowed_users: vec!["ou_user".into()],
            ..Access::default()
        });
        assert!(can_run_admin_command(&profile, None, "ou_user"));
        assert!(!can_run_admin_command(
            &profile,
            Some("ou_owner"),
            "ou_other"
        ));
        assert!(!can_run_admin_command(&profile, Some(""), ""));
    }
}
