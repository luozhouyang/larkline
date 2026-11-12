//! Per-scope debounce queue — port of `src/bot/pending-queue.ts`.
//!
//! Semantics copied from the TypeScript:
//!
//! - One entry per `scope` (the session scope string: `chatId` in the TS
//!   bridge for p2p/regular groups, `chatId:threadId` for topic groups).
//!   Messages pushed inside the quiet window are merged into one ordered batch.
//! - Every push re-arms the 600ms quiet-window timer (`DEBOUNCE_MS` from
//!   `src/bot/channel.ts`); the batch flushes only after that much silence.
//! - [`PendingQueue::block`] pauses the timer while an agent run is live on the
//!   scope. Pushed messages still accumulate; no flush fires until
//!   [`PendingQueue::unblock`], which arms a *fresh* quiet window.
//! - Commands bypass the queue; the bridge calls [`PendingQueue::cancel`] for
//!   the scope after a command so its queued messages are dropped. A blocked
//!   scope stays blocked after `cancel` — only the owning run ends the block.
//!
//! The TypeScript queue has **no size limit and no overflow behaviour**: the
//! map holds a plain `NormalizedMessage[]` and `push` only reports its length.
//! This port is therefore unbounded by default. [`QueueLimits`] can add a
//! Rust-only safety cap; when it trips the *oldest* message is dropped so the
//! newest user intent survives (and [`PushOutcome::dropped`] reports it).
//!
//! The queue owns no timers: callers pass an explicit `now` and ask for
//! [`PendingQueue::next_deadline`]. That keeps the structure pure and testable
//! without a runtime or network; `run.rs` drives it from a tokio task.

use std::collections::{HashMap, HashSet, VecDeque};
use std::time::{Duration, Instant};

/// Result of one [`PendingQueue::push`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PushOutcome {
    /// Messages waiting for this scope after the push (the size of the next
    /// batch, not counting the flush in progress).
    pub queued: usize,
    /// Messages discarded to make room. Always `0` for an unbounded queue.
    pub dropped: usize,
}

/// Optional bounds for a [`PendingQueue`].
///
/// The TypeScript queue is unbounded; [`QueueLimits::unbounded`] is the
/// faithful default. `max_per_scope` exists as a memory guard for the Rust
/// bridge, which may hold messages for up to 30 minutes while a run is live.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct QueueLimits {
    /// Maximum number of messages kept per scope. `None` means unbounded.
    pub max_per_scope: Option<usize>,
}

impl QueueLimits {
    /// No cap, matching `src/bot/pending-queue.ts`.
    pub const fn unbounded() -> Self {
        Self {
            max_per_scope: None,
        }
    }

    /// Keep at most `max` messages per scope, dropping the oldest on overflow.
    pub const fn per_scope(max: usize) -> Self {
        Self {
            max_per_scope: Some(max),
        }
    }
}

#[derive(Debug)]
struct Entry<T> {
    messages: VecDeque<T>,
    /// When this batch should flush; `None` while the scope is blocked.
    deadline: Option<Instant>,
}

/// Debounced, per-scope message queue.
#[derive(Debug)]
pub struct PendingQueue<T> {
    delay: Duration,
    limits: QueueLimits,
    entries: HashMap<String, Entry<T>>,
    blocked: HashSet<String>,
}

impl<T> PendingQueue<T> {
    /// An unbounded queue with the given quiet window.
    pub fn new(delay: Duration) -> Self {
        Self::with_limits(delay, QueueLimits::unbounded())
    }

    /// A queue with explicit bounds; see [`QueueLimits`].
    pub fn with_limits(delay: Duration, limits: QueueLimits) -> Self {
        Self {
            delay,
            limits,
            entries: HashMap::new(),
            blocked: HashSet::new(),
        }
    }

    /// Quiet window applied after the last push / unblock.
    pub fn delay(&self) -> Duration {
        self.delay
    }

    /// Configured bounds.
    pub fn limits(&self) -> QueueLimits {
        self.limits
    }

    /// Append a message to the scope's batch and (re)arm its quiet window.
    ///
    /// While the scope is blocked the message is still appended but no
    /// deadline is armed; [`Self::unblock`] starts a fresh window.
    pub fn push(&mut self, scope: &str, message: T, now: Instant) -> PushOutcome {
        if self.limits.max_per_scope == Some(0) {
            return PushOutcome {
                queued: 0,
                dropped: 1,
            };
        }

        let blocked = self.blocked.contains(scope);
        let entry = self
            .entries
            .entry(scope.to_string())
            .or_insert_with(|| Entry {
                messages: VecDeque::new(),
                deadline: None,
            });

        let mut dropped = 0;
        if let Some(max) = self.limits.max_per_scope {
            // Drop the oldest messages, keeping the newest intent. The TS
            // queue has no equivalent; this only runs when a cap was set.
            while entry.messages.len() >= max {
                entry.messages.pop_front();
                dropped += 1;
            }
        }
        entry.messages.push_back(message);
        entry.deadline = if blocked {
            None
        } else {
            Some(now + self.delay)
        };

        PushOutcome {
            queued: entry.messages.len(),
            dropped,
        }
    }

    /// Drop and return the scope's pending batch (a handled command does this).
    ///
    /// The scope's blocked flag is left alone: if a run owns the scope it must
    /// finish and call [`Self::unblock`] itself.
    pub fn cancel(&mut self, scope: &str) -> Vec<T> {
        match self.entries.remove(scope) {
            Some(entry) => entry.messages.into_iter().collect(),
            None => Vec::new(),
        }
    }

    /// Drop every batch and every block (bridge shutdown).
    pub fn cancel_all(&mut self) {
        self.entries.clear();
        self.blocked.clear();
    }

    /// Pause the quiet window for a scope; queued messages keep accumulating.
    pub fn block(&mut self, scope: &str) {
        self.blocked.insert(scope.to_string());
        if let Some(entry) = self.entries.get_mut(scope) {
            entry.deadline = None;
        }
    }

    /// Clear the scope's block and arm a fresh quiet window if work is queued.
    ///
    /// Mirrors the TypeScript: a scope that was not blocked is left untouched
    /// (so this cannot accidentally extend a pending window).
    pub fn unblock(&mut self, scope: &str, now: Instant) {
        if !self.blocked.remove(scope) {
            return;
        }
        if let Some(entry) = self.entries.get_mut(scope) {
            if !entry.messages.is_empty() {
                entry.deadline = Some(now + self.delay);
            }
        }
    }

    /// Whether a run currently owns the scope.
    pub fn is_blocked(&self, scope: &str) -> bool {
        self.blocked.contains(scope)
    }

    /// Messages queued for one scope.
    pub fn queued(&self, scope: &str) -> usize {
        self.entries
            .get(scope)
            .map_or(0, |entry| entry.messages.len())
    }

    /// Every scope with queued messages, sorted, with its batch size.
    ///
    /// Includes scopes whose quiet window is armed but whose run has not
    /// started yet, so `/ps` can show queued work that is not in the active
    /// run map. Empty scopes are omitted.
    pub fn queued_scopes(&self) -> Vec<(String, usize)> {
        let mut scopes: Vec<(String, usize)> = self
            .entries
            .iter()
            .filter(|(_, entry)| !entry.messages.is_empty())
            .map(|(scope, entry)| (scope.clone(), entry.messages.len()))
            .collect();
        scopes.sort_by(|a, b| a.0.cmp(&b.0));
        scopes
    }

    /// Total messages queued across all scopes.
    pub fn len(&self) -> usize {
        self.entries
            .values()
            .map(|entry| entry.messages.len())
            .sum()
    }

    /// Whether no scope has a pending batch.
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Earliest armed deadline, for the driver's `sleep_until`.
    pub fn next_deadline(&self) -> Option<Instant> {
        self.entries
            .values()
            .filter_map(|entry| entry.deadline)
            .min()
    }

    /// Remove and return the earliest batch whose quiet window has elapsed.
    ///
    /// Removing a batch means a run is starting, so the scope is marked
    /// blocked in the same step. The TypeScript flush callback does the same
    /// (`pending.block(scope)`) before its first `await`, so later messages
    /// cannot re-arm the timer for this scope.
    pub fn take_due(&mut self, now: Instant) -> Option<(String, Vec<T>)> {
        let scope = self
            .entries
            .iter()
            .filter(|(_, entry)| entry.deadline.is_some_and(|deadline| deadline <= now))
            .min_by_key(|(_, entry)| entry.deadline)
            .map(|(scope, _)| scope.clone())?;

        let entry = self.entries.remove(&scope)?;
        self.blocked.insert(scope.clone());
        Some((scope, entry.messages.into_iter().collect()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn at(base: Instant, millis: u64) -> Instant {
        base + Duration::from_millis(millis)
    }

    fn queue() -> PendingQueue<&'static str> {
        PendingQueue::new(Duration::from_millis(600))
    }

    #[test]
    fn messages_merge_into_one_batch_after_the_quiet_window() {
        let t0 = Instant::now();
        let mut queue = queue();

        assert_eq!(
            queue.push("oc_1", "a", t0),
            PushOutcome {
                queued: 1,
                dropped: 0
            }
        );
        assert_eq!(
            queue.push("oc_1", "b", at(t0, 300)),
            PushOutcome {
                queued: 2,
                dropped: 0
            }
        );

        // Every push re-arms the 600ms window: the batch is not due at t0+899.
        assert!(queue.take_due(at(t0, 899)).is_none());
        let (scope, batch) = queue.take_due(at(t0, 900)).expect("batch is due");
        assert_eq!(scope, "oc_1");
        assert_eq!(batch, vec!["a", "b"]);
        assert!(queue.is_empty());
        // Dequeuing means a run owns the scope until `unblock`.
        assert!(queue.is_blocked("oc_1"));
    }

    #[test]
    fn scopes_debounce_independently() {
        let t0 = Instant::now();
        let mut queue = queue();
        queue.push("oc_a", "a", t0);
        queue.push("oc_b", "b", at(t0, 100));

        let (scope, batch) = queue.take_due(at(t0, 600)).expect("first scope is due");
        assert_eq!(scope, "oc_a");
        assert_eq!(batch, vec!["a"]);
        // `oc_b` has its own window starting at t0+100.
        assert!(queue.take_due(at(t0, 699)).is_none());
        assert_eq!(
            queue.take_due(at(t0, 700)).expect("second scope is due").1,
            vec!["b"]
        );
    }

    #[test]
    fn queued_scopes_lists_every_non_empty_batch_in_order() {
        let t0 = Instant::now();
        let mut queue = queue();
        queue.push("oc_b", "b1", t0);
        queue.push("oc_a", "a1", t0);
        queue.push("oc_a", "a2", t0);
        queue.push("oc_c", "c1", t0);
        // A scope with an armed quiet window is still reported by `/ps`.
        assert_eq!(
            queue.queued_scopes(),
            vec![
                ("oc_a".to_string(), 2),
                ("oc_b".to_string(), 1),
                ("oc_c".to_string(), 1),
            ]
        );

        // Cancelling a batch drops it from the list.
        queue.cancel("oc_b");
        assert_eq!(
            queue.queued_scopes(),
            vec![("oc_a".to_string(), 2), ("oc_c".to_string(), 1)]
        );
        queue.cancel("oc_a");
        assert_eq!(queue.queued_scopes(), vec![("oc_c".to_string(), 1)]);
    }

    #[test]
    fn take_due_returns_the_oldest_batch_first() {
        let t0 = Instant::now();
        let mut queue = queue();
        queue.push("later", "b", at(t0, 100));
        queue.push("earlier", "a", t0);

        assert_eq!(
            queue.take_due(at(t0, 700)).expect("due").0,
            "earlier".to_string()
        );
        assert_eq!(
            queue.take_due(at(t0, 700)).expect("due").0,
            "later".to_string()
        );
    }

    #[test]
    fn block_pauses_the_timer_and_unblock_arms_a_fresh_window() {
        let t0 = Instant::now();
        let mut queue = queue();
        queue.push("oc_1", "a", t0);
        queue.block("oc_1");

        assert!(queue.next_deadline().is_none());
        queue.push("oc_1", "b", at(t0, 1_000));
        assert!(queue.next_deadline().is_none());
        assert!(queue.take_due(at(t0, 5_000)).is_none());

        // The run ended; a *fresh* 600ms window starts now, not at the pushes.
        queue.unblock("oc_1", at(t0, 2_000));
        assert_eq!(queue.next_deadline(), Some(at(t0, 2_600)));
        assert!(queue.take_due(at(t0, 2_599)).is_none());
        assert_eq!(
            queue.take_due(at(t0, 2_600)).expect("due").1,
            vec!["a", "b"]
        );
    }

    #[test]
    fn unblock_does_not_extend_a_window_that_is_already_armed() {
        let t0 = Instant::now();
        let mut queue = queue();
        queue.push("oc_1", "a", t0);

        queue.unblock("oc_1", at(t0, 100));

        assert_eq!(queue.next_deadline(), Some(at(t0, 600)));
    }

    #[test]
    fn a_dequeued_run_blocks_messages_until_it_ends() {
        let t0 = Instant::now();
        let mut queue = queue();
        queue.push("oc_1", "first", t0);
        assert_eq!(queue.take_due(at(t0, 600)).expect("due").1, vec!["first"]);

        // Message arrives while the run is live: it waits for the next round.
        assert_eq!(
            queue.push("oc_1", "next", at(t0, 700)),
            PushOutcome {
                queued: 1,
                dropped: 0
            }
        );
        assert!(queue.next_deadline().is_none());
        assert!(queue.take_due(at(t0, 10_000)).is_none());

        queue.unblock("oc_1", at(t0, 1_000));
        assert_eq!(queue.next_deadline(), Some(at(t0, 1_600)));
        assert_eq!(queue.take_due(at(t0, 1_600)).expect("due").1, vec!["next"]);
    }

    #[test]
    fn cancel_drops_the_queued_batch_for_a_command() {
        let t0 = Instant::now();
        let mut queue = queue();
        queue.push("oc_1", "a", t0);
        queue.push("oc_1", "b", at(t0, 100));

        assert_eq!(queue.cancel("oc_1"), vec!["a", "b"]);
        assert_eq!(queue.cancel("oc_1"), Vec::<&str>::new());
        assert!(queue.is_empty());
        assert!(queue.next_deadline().is_none());
    }

    /// `/stop` aborts the live run, drops everything that accumulated during
    /// it, and clears the block so the chat is usable again.
    #[test]
    fn stop_drops_queued_messages_and_clears_the_block() {
        let t0 = Instant::now();
        let mut queue = queue();
        queue.push("oc_1", "before", t0);
        queue.block("oc_1");
        queue.push("oc_1", "during-run", at(t0, 100));

        assert_eq!(queue.cancel("oc_1"), vec!["before", "during-run"]);
        queue.unblock("oc_1", at(t0, 200));

        assert!(!queue.is_blocked("oc_1"));
        assert!(queue.is_empty());
        assert!(queue.take_due(at(t0, 10_000)).is_none());
    }

    #[test]
    fn cancel_keeps_a_live_run_blocked() {
        let t0 = Instant::now();
        let mut queue = queue();
        queue.block("oc_1");
        queue.push("oc_1", "queued", t0);

        assert_eq!(queue.cancel("oc_1"), vec!["queued"]);
        // A command such as /help does not end the run.
        assert!(queue.is_blocked("oc_1"));
    }

    #[test]
    fn cancel_all_clears_messages_and_blocks() {
        let t0 = Instant::now();
        let mut queue = queue();
        queue.push("oc_a", "1", t0);
        queue.push("oc_b", "2", t0);
        queue.block("oc_b");

        queue.cancel_all();

        assert!(queue.is_empty());
        assert!(queue.next_deadline().is_none());
        assert!(!queue.is_blocked("oc_b"));
        assert_eq!(queue.cancel("oc_a"), Vec::<&str>::new());
    }

    #[test]
    fn bounded_queue_drops_the_oldest_messages_and_preserves_order() {
        let t0 = Instant::now();
        let mut queue =
            PendingQueue::with_limits(Duration::from_millis(600), QueueLimits::per_scope(3));

        let mut last = PushOutcome {
            queued: 0,
            dropped: 0,
        };
        for (index, message) in ["a", "b", "c", "d", "e"].into_iter().enumerate() {
            last = queue.push("oc_1", message, at(t0, index as u64 * 10));
        }

        assert_eq!(
            last,
            PushOutcome {
                queued: 3,
                dropped: 1
            }
        );
        assert_eq!(queue.queued("oc_1"), 3);
        assert_eq!(
            queue.take_due(at(t0, 640)).expect("due").1,
            vec!["c", "d", "e"]
        );
    }

    #[test]
    fn zero_capacity_drops_every_message() {
        let mut queue =
            PendingQueue::with_limits(Duration::from_millis(600), QueueLimits::per_scope(0));
        assert_eq!(
            queue.push("oc_1", "a", Instant::now()),
            PushOutcome {
                queued: 0,
                dropped: 1
            }
        );
        assert!(queue.is_empty());
    }

    #[test]
    fn default_is_unbounded_like_the_typescript_queue() {
        let now = Instant::now();
        let mut queue = queue();
        for _ in 0..1_000 {
            let outcome = queue.push("oc_1", "x", now);
            assert_eq!(outcome.dropped, 0);
        }
        assert_eq!(queue.len(), 1_000);
        assert_eq!(queue.limits(), QueueLimits::unbounded());
        assert!(queue.take_due(now + Duration::from_millis(600)).is_some());
    }
}
