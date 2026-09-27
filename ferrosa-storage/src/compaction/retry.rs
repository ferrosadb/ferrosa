//! Module: Track bounded per-table eligibility after digest verification failures.
//! Correctness: Retry deadlines grow exponentially to a configured cap and successful work clears the streak.
//! Last revised: 2026-09-27
//! Last changed: Add per-table retry state for compaction output verification failures.

use std::collections::HashMap;
use std::time::{Duration, Instant};

use crate::TableId;

#[derive(Default)]
pub(crate) struct CompactionRetryPolicy {
    tables: HashMap<TableId, RetryState>,
}

#[derive(Clone, Copy, Debug)]
struct RetryState {
    attempts: u32,
    digest_failures: u32,
    next_eligible: Instant,
}

impl CompactionRetryPolicy {
    pub(crate) fn eligible(&self, table_id: &TableId, now: Instant) -> bool {
        self.tables
            .get(table_id)
            .map_or(true, |state| now >= state.next_eligible)
    }

    pub(crate) fn next_deadline(&self, now: Instant) -> Option<Instant> {
        self.tables
            .values()
            .map(|state| state.next_eligible)
            .filter(|deadline| *deadline > now)
            .min()
    }

    /// Record an output digest/verification failure and return its new streak.
    pub(crate) fn record_failure(
        &mut self,
        table_id: TableId,
        now: Instant,
        initial: Duration,
        maximum: Duration,
    ) -> (u32, Duration) {
        let state = self.tables.entry(table_id).or_insert(RetryState {
            attempts: 0,
            digest_failures: 0,
            next_eligible: now,
        });
        state.attempts = state.attempts.saturating_add(1);
        state.digest_failures = state.digest_failures.saturating_add(1);
        let delay = backoff_delay(initial, maximum, state.attempts);
        state.next_eligible = now.checked_add(delay).unwrap_or(now);
        (state.digest_failures, delay)
    }

    /// A successful compaction clears retry history unless the table is paused.
    pub(crate) fn succeeded(&mut self, table_id: &TableId, paused: bool) -> bool {
        if !paused {
            self.tables.remove(table_id).is_some()
        } else {
            false
        }
    }

    pub(crate) fn remove(&mut self, table_id: &TableId) -> bool {
        self.tables.remove(table_id).is_some()
    }
}

fn backoff_delay(initial: Duration, maximum: Duration, attempt: u32) -> Duration {
    let exponent = attempt.saturating_sub(1).min(63);
    let multiplier = 1_u128 << exponent;
    let millis = initial
        .as_millis()
        .saturating_mul(multiplier)
        .min(maximum.as_millis());
    Duration::from_millis(millis.min(u64::MAX as u128) as u64)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn table_id() -> TableId {
        TableId {
            keyspace: "ks".into(),
            table: "tbl".into(),
        }
    }

    #[test]
    fn compaction_backoff_grows_to_the_configured_cap() {
        let now = Instant::now();
        let initial = Duration::from_millis(250);
        let maximum = Duration::from_secs(2);
        let mut policy = CompactionRetryPolicy::default();

        assert_eq!(
            policy.record_failure(table_id(), now, initial, maximum),
            (1, Duration::from_millis(250))
        );
        assert!(!policy.eligible(&table_id(), now));
        assert!(policy.eligible(&table_id(), now + Duration::from_millis(250)));

        assert_eq!(
            policy.record_failure(table_id(), now, initial, maximum),
            (2, Duration::from_millis(500))
        );
        assert_eq!(
            policy.record_failure(table_id(), now, initial, maximum),
            (3, Duration::from_secs(1))
        );
        assert_eq!(
            policy.record_failure(table_id(), now, initial, maximum),
            (4, Duration::from_secs(2))
        );
        assert_eq!(
            policy.record_failure(table_id(), now, initial, maximum),
            (5, Duration::from_secs(2))
        );
    }

    #[test]
    fn compaction_backoff_success_and_table_removal_clear_state() {
        let now = Instant::now();
        let mut policy = CompactionRetryPolicy::default();
        policy.record_failure(
            table_id(),
            now,
            Duration::from_secs(1),
            Duration::from_secs(8),
        );
        assert!(policy.succeeded(&table_id(), false));
        assert!(policy.eligible(&table_id(), now));

        policy.record_failure(
            table_id(),
            now,
            Duration::from_secs(1),
            Duration::from_secs(8),
        );
        assert!(policy.remove(&table_id()));
        assert!(policy.eligible(&table_id(), now));
    }

    #[test]
    fn compaction_backoff_exposes_only_future_deadlines() {
        let now = Instant::now();
        let mut policy = CompactionRetryPolicy::default();
        assert_eq!(policy.next_deadline(now), None);

        let delay = Duration::from_millis(50);
        policy.record_failure(table_id(), now, delay, Duration::from_secs(1));
        let deadline = now + delay;
        assert_eq!(policy.next_deadline(now), Some(deadline));
        assert_eq!(policy.next_deadline(deadline), None);
    }

    #[test]
    fn compaction_backoff_success_keeps_a_paused_table_state() {
        let now = Instant::now();
        let mut policy = CompactionRetryPolicy::default();
        policy.record_failure(
            table_id(),
            now,
            Duration::from_secs(1),
            Duration::from_secs(8),
        );
        policy.succeeded(&table_id(), true);
        assert!(!policy.eligible(&table_id(), now));
    }
}
