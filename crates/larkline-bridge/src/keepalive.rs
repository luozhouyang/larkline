//! App-level keepalive — port of `src/bot/keepalive.ts`.
//!
//! The TypeScript loop is a defense-in-depth watchdog for the long connection:
//! every 15s it reads the SDK's connection status, and — only when the network
//! can reach Feishu/Lark but the WebSocket is still not connected for three
//! consecutive ticks — forces a reconnect (`controls.restart()`). It also
//! detects machine sleep, guards against a timer storm on wake-up, and
//! rate-limits "network unreachable" logs.
//!
//! ## Typed gap
//!
//! `lark-channel` 0.6 (the SDK behind `larkline-lark`) exposes neither hook:
//!
//! 1. **No connection status.** The TS loop calls
//!    `channel.getConnectionStatus()` and reads `{ state, reconnectAttempts }`.
//!    Here the event loop is owned and driven by `larkline-cli`; its
//!    `EventLoop::run` owns the connection and has no status accessor.
//! 2. **No force-reconnect handle.** The TS loop calls `controls.restart()`.
//!    `larkline-lark` has no in-process supervisor to bounce the socket, and
//!    the CLI only restarts at the OS-service level (`larkline restart`).
//!
//! Because every *decision* the TS loop makes depends on connection status,
//! the Bridge does **not** start this timer yet: it would only burn a timer.
//! Those two hooks are recorded in [`KeepaliveGap`]; implement
//! [`KeepaliveHost`] and call [`start_keepalive`] once they exist.
//!
//! Everything that does not depend on the missing hooks is ported and tested
//! without network: the 15s cadence, wake-up detection, timer-storm guard, the
//! 5s probe timeout, the network-down log rate-limit, and the consecutive-down
//! counter that gates a force reconnect.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use tokio::sync::watch;
use tokio::task::JoinHandle;
use tokio::time::MissedTickBehavior;

/// Independent probe cadence (TS `KEEPALIVE_INTERVAL_MS`).
pub const KEEPALIVE_INTERVAL: Duration = Duration::from_secs(15);
/// A gap larger than this since the previous tick means the machine slept
/// (TS `SLEEP_DETECT_MS`).
pub const SLEEP_DETECT: Duration = Duration::from_secs(30);
/// Ticks closer together than this are a wake-up timer storm
/// (TS `TIMER_STORM_GUARD_MS`).
pub const TIMER_STORM_GUARD: Duration = Duration::from_secs(5);
/// Reachability probe timeout (TS `HTTP_PROBE_TIMEOUT_MS`).
pub const HTTP_PROBE_TIMEOUT: Duration = Duration::from_secs(5);
/// Consecutive stuck ticks before a force reconnect (TS `DEAD_THRESHOLD`).
pub const DEAD_THRESHOLD: u32 = 3;
/// Log "network unreachable" at most every this many ticks while it is down
/// (TS `NETWORK_DOWN_LOG_EVERY`, roughly every 5 minutes).
pub const NETWORK_DOWN_LOG_EVERY: u32 = 20;

/// SDK facilities the TypeScript keepalive relies on and `larkline-lark` lacks.
///
/// This is the typed TODO for wiring the watchdog: until at least the status
/// hook exists, the timer cannot make a decision and is not started by the
/// Bridge.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum KeepaliveGap {
    /// TS `channel.getConnectionStatus()` — no equivalent in `lark-channel`
    /// 0.6; the connection lives inside `EventLoop::run`.
    #[error(
        "lark-channel exposes no WebSocket connection status, so the keepalive cannot detect a stuck socket"
    )]
    NoConnectionStatus,
    /// TS `controls.restart()` — `larkline-lark` has no in-process reconnect
    /// handle (only the OS-service `larkline restart`).
    #[error(
        "larkline-lark has no in-process force-reconnect handle, so the keepalive cannot bounce a stuck socket"
    )]
    NoForceReconnect,
}

/// All missing hooks, for callers that want to report the full gap.
pub const KEEPALIVE_GAPS: [KeepaliveGap; 2] = [
    KeepaliveGap::NoConnectionStatus,
    KeepaliveGap::NoForceReconnect,
];

/// A snapshot of the connection, mirroring the TS `getConnectionStatus()`
/// result closely enough for the watchdog.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConnectionStatus {
    /// SDK state string; only `"connected"` is special-cased, as in the TS.
    pub state: String,
    pub reconnect_attempts: u32,
}

impl ConnectionStatus {
    pub fn new(state: impl Into<String>, reconnect_attempts: u32) -> Self {
        Self {
            state: state.into(),
            reconnect_attempts,
        }
    }

    pub fn is_connected(&self) -> bool {
        self.state == "connected"
    }
}

/// Timing/filter phase of one timer tick.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TickGate {
    /// Time to read the connection status.
    Proceed,
    /// Another timer fired less than [`TIMER_STORM_GUARD`] after the last one;
    /// ignore it (and do not move the last-tick timestamp, as in the TS).
    Storm,
    /// The machine likely slept; counters were reset and this tick is skipped.
    WakeUp { slept: Duration },
}

/// Result of reading the connection status for a tick.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StatusDecision {
    /// `getConnectionStatus()` returned null: channel not initialized yet.
    NotInitialized,
    Connected {
        /// Ticks the socket had been counted as down before recovering.
        recovered_after: u32,
    },
    /// Not connected: probe whether the network can reach the API host.
    Probe,
}

/// Result of finishing a tick with (or without) the reachability probe.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct ProbeDecision {
    /// Present when reachability returned after an outage; the caller logs
    /// "reachable again" with this many down ticks.
    pub network_recovered_after: Option<u32>,
    /// Present when the probe failed (the WS-side counter was reset).
    pub network_down_ticks: Option<u32>,
    /// True on the first failure and every [`NETWORK_DOWN_LOG_EVERY`]th.
    pub log_network_down: bool,
    /// Present when the network is reachable but the socket is still down;
    /// value is the consecutive-down count *before* any threshold reset.
    pub consecutive_down: Option<u32>,
    /// True when [`DEAD_THRESHOLD`] was reached and the caller must force a
    /// reconnect. The TS resets the counter at this point, and so does this.
    pub force_reconnect: bool,
}

/// Pure state behind the keepalive loop.
///
/// Call [`Self::begin_tick`] first, then [`Self::on_status`]; if that returns
/// [`StatusDecision::Probe`], probe and feed the result to [`Self::on_probe`].
/// Keeping the clock and the two async hooks outside makes the logic testable
/// without a runtime.
#[derive(Debug, Default)]
pub struct KeepaliveState {
    last_tick: Option<SystemTime>,
    consecutive_down: u32,
    network_down_ticks: u32,
}

impl KeepaliveState {
    pub fn new() -> Self {
        Self::default()
    }

    /// Storm guard and wake-up detection for one timer tick.
    pub fn begin_tick(&mut self, now: SystemTime) -> TickGate {
        if let Some(last) = self.last_tick {
            let since = now.duration_since(last).unwrap_or_default();
            if !since.is_zero() && since < TIMER_STORM_GUARD {
                return TickGate::Storm;
            }
            if since > SLEEP_DETECT {
                // Don't trust counters captured pre-sleep.
                self.last_tick = Some(now);
                self.consecutive_down = 0;
                self.network_down_ticks = 0;
                return TickGate::WakeUp { slept: since };
            }
        }
        self.last_tick = Some(now);
        TickGate::Proceed
    }

    /// Account for the connection status (TS step after the timing checks).
    pub fn on_status(&mut self, status: Option<&ConnectionStatus>) -> StatusDecision {
        let Some(status) = status else {
            return StatusDecision::NotInitialized;
        };
        if status.is_connected() {
            let recovered_after = self.consecutive_down;
            self.consecutive_down = 0;
            self.network_down_ticks = 0;
            return StatusDecision::Connected { recovered_after };
        }
        StatusDecision::Probe
    }

    /// Account for the reachability probe of an unconnected socket.
    pub fn on_probe(&mut self, reachable: bool) -> ProbeDecision {
        let mut decision = ProbeDecision::default();

        if !reachable {
            // Blocked at the network level, not the WS: force-reconnect would
            // not help, so the WS-side counter is reset.
            self.network_down_ticks = self.network_down_ticks.saturating_add(1);
            self.consecutive_down = 0;
            decision.network_down_ticks = Some(self.network_down_ticks);
            decision.log_network_down = self.network_down_ticks == 1
                || self.network_down_ticks % NETWORK_DOWN_LOG_EVERY == 0;
            return decision;
        }

        if self.network_down_ticks > 0 {
            decision.network_recovered_after = Some(std::mem::take(&mut self.network_down_ticks));
        }

        // Network is fine but the socket is not → the WS is stuck.
        self.consecutive_down = self.consecutive_down.saturating_add(1);
        decision.consecutive_down = Some(self.consecutive_down);
        if self.consecutive_down >= DEAD_THRESHOLD {
            decision.force_reconnect = true;
            self.consecutive_down = 0;
        }
        decision
    }

    /// Consecutive ticks the socket has been counted as down.
    pub fn consecutive_down(&self) -> u32 {
        self.consecutive_down
    }

    /// Consecutive probe failures while the network is unreachable.
    pub fn network_down_ticks(&self) -> u32 {
        self.network_down_ticks
    }
}

/// The two SDK hooks plus the probe the timer needs.
///
/// No production implementation exists yet — see [`KeepaliveGap`] and the
/// module docs. This is the seam to implement when `larkline-lark` grows a
/// connection-status/reconnect API: wire it in `run.rs` with
/// `Channel::rest().base_url()` as the probe target.
pub trait KeepaliveHost: Send + Sync + 'static {
    /// `channel.getConnectionStatus()`; `None` before the channel initializes.
    fn connection_status(&self) -> Option<ConnectionStatus>;

    /// Whether the API host answers at the network level. The TS issues a
    /// `HEAD` request; any answer counts. The timer wraps this in
    /// [`HTTP_PROBE_TIMEOUT`].
    fn probe_reachable(&self) -> Pin<Box<dyn Future<Output = bool> + Send + '_>>;

    /// `controls.restart()` — bounce the long connection.
    fn force_reconnect(&self) -> Pin<Box<dyn Future<Output = anyhow::Result<()>> + Send + '_>>;
}

/// Shutdown handle for the keepalive timer.
pub struct KeepaliveHandle {
    shutdown: watch::Sender<bool>,
    task: JoinHandle<()>,
}

impl KeepaliveHandle {
    /// Stop the timer; safe to call more than once.
    pub fn stop(&self) {
        let _ = self.shutdown.send(true);
        self.task.abort();
    }
}

impl Drop for KeepaliveHandle {
    fn drop(&mut self) {
        self.stop();
    }
}

/// Start the 15s keepalive timer.
///
/// GAP: the Bridge cannot call this yet because nothing implements
/// [`KeepaliveHost`] (see [`KEEPALIVE_GAPS`]). Kept here so the ported state
/// machine has its driver and shutdown path.
pub fn start_keepalive<H: KeepaliveHost>(host: Arc<H>) -> KeepaliveHandle {
    let (shutdown, receiver) = watch::channel(false);
    let task = tokio::spawn(run(host, receiver));
    KeepaliveHandle { shutdown, task }
}

async fn run<H: KeepaliveHost>(host: Arc<H>, mut shutdown: watch::Receiver<bool>) {
    let mut state = KeepaliveState::new();
    // `interval_at` skips tokio's immediate first tick, matching setInterval's
    // first fire after one full interval.
    let mut interval = tokio::time::interval_at(
        tokio::time::Instant::now() + KEEPALIVE_INTERVAL,
        KEEPALIVE_INTERVAL,
    );
    interval.set_missed_tick_behavior(MissedTickBehavior::Delay);

    loop {
        tokio::select! {
            changed = shutdown.changed() => {
                if changed.is_err() || *shutdown.borrow() {
                    return;
                }
            }
            _ = interval.tick() => {}
        }

        match state.begin_tick(SystemTime::now()) {
            TickGate::Storm => continue,
            TickGate::WakeUp { slept } => {
                tracing::info!(slept_ms = slept.as_millis(), "keepalive wake-up");
                continue;
            }
            TickGate::Proceed => {}
        }

        let Some(status) = host.connection_status() else {
            continue;
        };
        match state.on_status(Some(&status)) {
            StatusDecision::NotInitialized => continue,
            StatusDecision::Connected { recovered_after } => {
                if recovered_after > 0 {
                    tracing::info!(after_ticks = recovered_after, "keepalive recovered");
                }
                continue;
            }
            StatusDecision::Probe => {}
        }

        let reachable = tokio::time::timeout(HTTP_PROBE_TIMEOUT, host.probe_reachable())
            .await
            .unwrap_or_default();
        let decision = state.on_probe(reachable);

        if let Some(after_ticks) = decision.network_recovered_after {
            tracing::info!(after_ticks, "network reachable again");
        }
        if let Some(network_down_ticks) = decision.network_down_ticks {
            if decision.log_network_down {
                tracing::warn!(network_down_ticks, "network unreachable");
            }
        }
        if let Some(consecutive_down) = decision.consecutive_down {
            tracing::warn!(
                state = %status.state,
                reconnect_attempts = status.reconnect_attempts,
                consecutive_down,
                "ws stuck"
            );
        }
        if decision.force_reconnect {
            tracing::warn!(state = %status.state, "keepalive force-reconnect");
            if let Err(err) = host.force_reconnect().await {
                tracing::warn!(error = %err, "force reconnect failed");
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn at(base: SystemTime, millis: u64) -> SystemTime {
        base + Duration::from_millis(millis)
    }

    fn connected() -> ConnectionStatus {
        ConnectionStatus::new("connected", 0)
    }

    fn disconnected() -> ConnectionStatus {
        ConnectionStatus::new("reconnecting", 2)
    }

    #[test]
    fn first_tick_always_proceeds() {
        let mut state = KeepaliveState::new();
        assert_eq!(state.begin_tick(SystemTime::now()), TickGate::Proceed);
    }

    #[test]
    fn timer_storm_is_ignored_and_does_not_move_the_last_tick() {
        let t0 = SystemTime::now();
        let mut state = KeepaliveState::new();
        assert_eq!(state.begin_tick(t0), TickGate::Proceed);

        assert_eq!(state.begin_tick(at(t0, 1_000)), TickGate::Storm);
        // The crossing tick is measured from t0, not the storm tick.
        assert_eq!(state.begin_tick(at(t0, 15_000)), TickGate::Proceed);
    }

    #[test]
    fn wake_up_resets_counters_and_skips_the_tick() {
        let t0 = SystemTime::now();
        let mut state = KeepaliveState::new();
        state.begin_tick(t0);
        // Two stuck ticks before the laptop sleeps.
        state.on_status(Some(&disconnected()));
        state.on_probe(true);
        state.on_status(Some(&disconnected()));
        state.on_probe(true);
        assert_eq!(state.consecutive_down(), 2);

        let woke = at(t0, 31_000);
        assert_eq!(
            state.begin_tick(woke),
            TickGate::WakeUp {
                slept: Duration::from_secs(31)
            }
        );
        assert_eq!(state.consecutive_down(), 0);
        assert_eq!(state.network_down_ticks(), 0);
    }

    #[test]
    fn connected_status_clears_counters_and_reports_recovery() {
        let mut state = KeepaliveState::new();
        state.on_status(Some(&disconnected()));
        state.on_probe(true);

        assert_eq!(
            state.on_status(Some(&connected())),
            StatusDecision::Connected { recovered_after: 1 }
        );
        assert_eq!(state.consecutive_down(), 0);
        assert_eq!(state.network_down_ticks(), 0);
    }

    #[test]
    fn missing_status_is_ignored() {
        let mut state = KeepaliveState::new();
        state.on_status(Some(&disconnected()));
        assert_eq!(state.on_status(None), StatusDecision::NotInitialized);
        assert_eq!(state.consecutive_down(), 0);
        assert_eq!(state.network_down_ticks(), 0);
    }

    #[test]
    fn network_down_is_rate_limited_and_resets_the_ws_counter() {
        let mut state = KeepaliveState::new();
        state.on_status(Some(&disconnected()));
        state.on_probe(true); // ws down once
        assert_eq!(state.consecutive_down(), 1);

        let first = state.on_probe(false);
        assert_eq!(first.network_down_ticks, Some(1));
        assert!(first.log_network_down);
        assert_eq!(first.consecutive_down, None);
        assert_eq!(state.consecutive_down(), 0);

        let mut last = ProbeDecision::default();
        for _ in 2..=NETWORK_DOWN_LOG_EVERY {
            last = state.on_probe(false);
        }
        assert_eq!(last.network_down_ticks, Some(NETWORK_DOWN_LOG_EVERY));
        assert!(last.log_network_down);
        assert!(!state.on_probe(false).log_network_down);
    }

    #[test]
    fn reachability_returning_is_reported_once() {
        let mut state = KeepaliveState::new();
        state.on_probe(false);
        state.on_probe(false);

        let decision = state.on_probe(true);
        assert_eq!(decision.network_recovered_after, Some(2));
        assert!(!decision.force_reconnect);
        assert_eq!(state.network_down_ticks(), 0);

        // Already reachable: no repeated "recovered" event.
        let decision = state.on_probe(true);
        assert_eq!(decision.network_recovered_after, None);
    }

    #[test]
    fn three_reachable_but_down_ticks_force_a_reconnect() {
        let mut state = KeepaliveState::new();
        for _ in 0..(DEAD_THRESHOLD - 1) {
            let decision = state.on_probe(true);
            assert!(!decision.force_reconnect);
        }
        let decision = state.on_probe(true);
        assert!(decision.force_reconnect);
        assert_eq!(decision.consecutive_down, Some(DEAD_THRESHOLD));
        // The TS resets the counter right after triggering the reconnect.
        assert_eq!(state.consecutive_down(), 0);
    }

    #[test]
    fn recovery_and_force_reconnect_cannot_happen_on_the_same_tick() {
        // A failed probe resets the WS counter, so a later recover tick can
        // only be the *first* consecutive down tick — never the threshold one.
        let mut state = KeepaliveState::new();
        state.on_probe(false);
        let decision = state.on_probe(true);
        assert_eq!(decision.network_recovered_after, Some(1));
        assert_eq!(decision.consecutive_down, Some(1));
        assert!(!decision.force_reconnect);
    }

    #[test]
    fn ts_constants_are_preserved() {
        assert_eq!(KEEPALIVE_INTERVAL, Duration::from_secs(15));
        assert_eq!(SLEEP_DETECT, Duration::from_secs(30));
        assert_eq!(TIMER_STORM_GUARD, Duration::from_secs(5));
        assert_eq!(HTTP_PROBE_TIMEOUT, Duration::from_secs(5));
        assert_eq!(DEAD_THRESHOLD, 3);
        assert_eq!(NETWORK_DOWN_LOG_EVERY, 20);
        assert_eq!(
            KEEPALIVE_GAPS,
            [
                KeepaliveGap::NoConnectionStatus,
                KeepaliveGap::NoForceReconnect
            ]
        );
    }
}
