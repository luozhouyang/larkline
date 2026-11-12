//! Global cap on concurrent agent runs — port of `src/bot/process-pool.ts`.
//!
//! The TypeScript bridge gates every agent spawn through one `ProcessPool`:
//! `acquire()` succeeds while `active < cap()`, otherwise the caller waits in a
//! FIFO waiter list; `tryAcquire()` is the `nowait` variant and rejects with
//! `pool-full`. The cap is read from the live profile on every call, so a
//! `/config maxConcurrentRuns` change applies to the next run. A slot is
//! released in the executor's `cleanup`, which runs on normal completion,
//! failure and abort (the abort path awaits `run.stop()` first).
//!
//! The Rust bridge starts runs from its per-chat debounce queue instead of
//! awaiting a pool inside `submit`. The limiter therefore mirrors the *slot*
//! bookkeeping, not the waiter list: the queue driver asks for a slot with
//! [`RunLimiter::try_acquire`]; when none is free the due batch stays in the
//! pending queue (blocked only when a run actually starts), and a later
//! release wakes the driver. This is the queued-start equivalent that cannot
//! drop messages, and it keeps per-chat serialization in `PendingQueue`.
//!
//! A plain [`tokio::sync::Semaphore`] cannot be resized downward while permits
//! are checked out, which `/config maxConcurrentRuns` needs. This type tracks
//! the same `active`/`cap` pair the TypeScript pool tracks (a semaphore whose
//! permit count is `cap - active`), so lowering the cap below the number of
//! live runs throttles new starts until enough runs finish — exactly the
//! comment in the TypeScript `release()`.
//!
//! # Differences from the TypeScript pool
//!
//! * **No blocking `acquire()`:** the TypeScript executor awaits the FIFO
//!   waiter list inside `submit`; here a due batch simply stays in the pending
//!   queue, so start order is "earliest flushed batch first" rather than
//!   global arrival order. Per-chat debounce/serialization remains the
//!   authority, and no message is dropped or rejected for being over capacity.
//! * **No `nowait`/`pool-full` path:** the TypeScript executor can reject a
//!   `nowait` run (`RunRejected('pool-full')`, used by `/doctor`). The Rust
//!   bridge has no such caller, so every queued batch waits instead.
//! * **One pool per bridge/profile:** same as one `ProcessPool` per
//!   `startChannel` in TypeScript.
//! * **Slot release:** TypeScript releases in the executor's `cleanup`, after
//!   `activeRuns.unregister` (and after `run.stop()` on the abort path). Rust
//!   releases in [`RunGuard`](crate::run)'s `Drop`, which runs for normal
//!   completion, failure and task cancellation (`/stop`).
//! * **Coverage gap:** this pool only covers runs that start through
//!   `Bridge::start_due_batch` (IM message batches and forwarded card clicks).
//!   Rust comment runs (`comments.rs`) and in-meeting runs (`meeting.rs`) call
//!   `AgentProcess::spawn` directly and bypass the cap; the TypeScript bridge
//!   routes both through `RunExecutor` and its pool. They have no pending
//!   queue to wait in, so wiring them needs an async wait (and a decision
//!   about blocking the comment/meeting task).

use std::sync::{Arc, Mutex as StdMutex};

use tokio::sync::Notify;

/// Hard upper bound, matching `ProfileConfig::max_concurrent_runs` and
/// `getMaxConcurrentRuns` in the TypeScript config schema.
pub const MAX_CONCURRENT_RUNS: u32 = 50;

#[derive(Debug, Default)]
struct State {
    cap: usize,
    active: usize,
}

/// Slot pool shared by every run started through the bridge's pending queue.
#[derive(Debug)]
pub struct RunLimiter {
    state: StdMutex<State>,
    /// Wakes the queue driver whenever a slot is released, including a run
    /// future dropped before it could build its `RunGuard` (e.g. `/stop`
    /// races the first poll). The driver also listens on the bridge's pending
    /// wakeup, but this signal makes slot release self-contained.
    released: Notify,
}

impl RunLimiter {
    /// New limiter for a profile cap (clamped to `1..=50`).
    pub fn new(cap: u32) -> Self {
        Self {
            state: StdMutex::new(State {
                cap: clamp_cap(cap),
                active: 0,
            }),
            released: Notify::new(),
        }
    }

    /// Current cap after clamping; `/config` changes replace it.
    pub fn cap(&self) -> usize {
        lock(&self.state).cap
    }

    /// Runs currently holding a slot.
    pub fn active(&self) -> usize {
        lock(&self.state).active
    }

    /// Slots available right now (`0` when the cap is exhausted or lowered
    /// below the number of live runs).
    pub fn available(&self) -> usize {
        let state = lock(&self.state);
        state.cap.saturating_sub(state.active)
    }

    /// Take a slot without waiting, like `ProcessPool.tryAcquire()`.
    ///
    /// The returned guard releases the slot on drop. `None` means the driver
    /// must leave its batch in the pending queue.
    pub fn try_acquire(self: &Arc<Self>) -> Option<RunPermit> {
        let mut state = lock(&self.state);
        if state.active >= state.cap {
            return None;
        }
        state.active += 1;
        Some(RunPermit {
            limiter: Arc::clone(self),
        })
    }

    /// Wait for the next slot release. Combined with the bridge's pending
    /// wakeup in the queue driver's capacity wait.
    pub async fn slot_released(&self) {
        self.released.notified().await;
    }

    /// Install the live profile's cap (clamped to `1..=50`).
    ///
    /// Returns `true` when the effective cap changed. Lowering below `active`
    /// is allowed: no new run starts until `active < cap` again, which is the
    /// TypeScript `release()` behaviour ("if cap was just lowered via /config,
    /// this naturally throttles by not waking").
    pub fn set_cap(&self, cap: u32) -> bool {
        let cap = clamp_cap(cap);
        let mut state = lock(&self.state);
        if state.cap == cap {
            return false;
        }
        state.cap = cap;
        true
    }
}

/// One checked-out run slot. Dropping it releases the slot; the bridge holds
/// it inside [`RunGuard`](crate::run) so `/stop` releases capacity too.
#[derive(Debug)]
pub struct RunPermit {
    limiter: Arc<RunLimiter>,
}

impl Drop for RunPermit {
    fn drop(&mut self) {
        let mut state = lock(&self.limiter.state);
        state.active = state.active.saturating_sub(1);
        drop(state);
        // Notify after the count is visible: a woken driver must see the
        // released capacity even if this permit never reached a RunGuard.
        self.limiter.released.notify_one();
    }
}

fn clamp_cap(cap: u32) -> usize {
    cap.clamp(1, MAX_CONCURRENT_RUNS) as usize
}

fn lock<T>(mutex: &StdMutex<T>) -> std::sync::MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cap_is_enforced_and_drop_releases_the_slot() {
        let limiter = Arc::new(RunLimiter::new(2));
        let first = limiter.try_acquire().expect("first slot");
        let second = limiter.try_acquire().expect("second slot");
        assert!(limiter.try_acquire().is_none());
        assert_eq!(limiter.active(), 2);
        assert_eq!(limiter.available(), 0);

        drop(first);
        assert_eq!(limiter.active(), 1);
        assert_eq!(limiter.available(), 1);
        let third = limiter.try_acquire().expect("released slot");
        assert!(limiter.try_acquire().is_none());
        drop(third);
        drop(second);
        assert_eq!(limiter.active(), 0);
        assert_eq!(limiter.available(), 2);
    }

    #[test]
    fn cap_is_clamped_to_one_through_fifty() {
        let limiter = Arc::new(RunLimiter::new(0));
        assert_eq!(limiter.cap(), 1);
        assert!(limiter.try_acquire().is_some());

        let limiter = Arc::new(RunLimiter::new(999));
        assert_eq!(limiter.cap(), MAX_CONCURRENT_RUNS as usize);
    }

    #[test]
    fn lowering_the_cap_below_active_throttles_until_runs_finish() {
        let limiter = Arc::new(RunLimiter::new(3));
        let first = limiter.try_acquire().expect("first slot");
        let second = limiter.try_acquire().expect("second slot");

        assert!(limiter.set_cap(1));
        assert_eq!(limiter.cap(), 1);
        // Two runs are still live, so the limiter is over-committed rather
        // than killing a run; no third run may start.
        assert!(limiter.try_acquire().is_none());
        assert_eq!(limiter.available(), 0);

        drop(first);
        // One live run remains, equal to the new cap: still no start.
        assert_eq!(limiter.active(), 1);
        assert!(limiter.try_acquire().is_none());

        drop(second);
        // Active dropped below the cap, so the next run may start.
        let replacement = limiter.try_acquire().expect("slot under new cap");
        assert!(limiter.try_acquire().is_none());
        drop(replacement);
        assert_eq!(limiter.active(), 0);
    }

    #[test]
    fn raising_the_cap_admits_queued_runs_immediately() {
        let limiter = Arc::new(RunLimiter::new(1));
        let first = limiter.try_acquire().expect("only slot");
        assert!(limiter.try_acquire().is_none());

        assert!(limiter.set_cap(2));
        let second = limiter.try_acquire().expect("new slot");
        assert_eq!(limiter.active(), 2);
        assert_eq!(limiter.available(), 0);

        drop(first);
        drop(second);
        assert_eq!(limiter.available(), 2);
    }

    #[tokio::test]
    async fn releasing_a_permit_wakes_a_capacity_waiter() {
        let limiter = Arc::new(RunLimiter::new(1));
        let permit = limiter.try_acquire().expect("only slot");
        let waiter = Arc::clone(&limiter);
        let wait = tokio::spawn(async move { waiter.slot_released().await });

        drop(permit);
        tokio::time::timeout(std::time::Duration::from_secs(1), wait)
            .await
            .expect("release wakes the waiter")
            .expect("waiter task");
    }

    #[test]
    fn setting_the_same_cap_is_a_noop() {
        let limiter = RunLimiter::new(5);
        assert!(!limiter.set_cap(5));
        assert!(limiter.set_cap(6));
        assert_eq!(limiter.cap(), 6);
    }
}
