use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};

static LOCAL_WRITE_ATTEMPTS: AtomicU64 = AtomicU64::new(0);
static REMOTE_WRITE_ATTEMPTS: AtomicU64 = AtomicU64::new(0);
static LOCAL_WRITE_ACKS: AtomicU64 = AtomicU64::new(0);
static REMOTE_WRITE_ACKS: AtomicU64 = AtomicU64::new(0);
static LOCAL_WRITE_FAILURES: AtomicU64 = AtomicU64::new(0);
static REMOTE_WRITE_FAILURES: AtomicU64 = AtomicU64::new(0);
static POST_QUORUM_REMOTE_ACKS: AtomicU64 = AtomicU64::new(0);
static POST_QUORUM_REMOTE_FAILURES: AtomicU64 = AtomicU64::new(0);
static HINTS_STORED: AtomicU64 = AtomicU64::new(0);
static HINTS_REJECTED: AtomicU64 = AtomicU64::new(0);
static INBOUND_MUTATION_FORWARDS: AtomicU64 = AtomicU64::new(0);
static INBOUND_MUTATION_ROWS: AtomicU64 = AtomicU64::new(0);
static INBOUND_MUTATION_FAILURES: AtomicU64 = AtomicU64::new(0);
static WRITE_ADMISSION_IN_FLIGHT: AtomicI64 = AtomicI64::new(0);
static WRITE_ADMISSION_IN_FLIGHT_MAX: AtomicI64 = AtomicI64::new(0);
static ANTI_ENTROPY_REPAIRS_REQUESTED: AtomicU64 = AtomicU64::new(0);

fn update_max_i64(target: &AtomicI64, value: i64) {
    let mut current = target.load(Ordering::Relaxed);
    while value > current {
        match target.compare_exchange_weak(current, value, Ordering::Relaxed, Ordering::Relaxed) {
            Ok(_) => break,
            Err(next) => current = next,
        }
    }
}

pub fn inc_replica_write_attempt(local: bool) {
    if local {
        LOCAL_WRITE_ATTEMPTS.fetch_add(1, Ordering::Relaxed);
    } else {
        REMOTE_WRITE_ATTEMPTS.fetch_add(1, Ordering::Relaxed);
    }
}

pub fn inc_replica_write_ack(local: bool) {
    if local {
        LOCAL_WRITE_ACKS.fetch_add(1, Ordering::Relaxed);
    } else {
        REMOTE_WRITE_ACKS.fetch_add(1, Ordering::Relaxed);
    }
}

pub fn inc_replica_write_failure(local: bool) {
    if local {
        LOCAL_WRITE_FAILURES.fetch_add(1, Ordering::Relaxed);
    } else {
        REMOTE_WRITE_FAILURES.fetch_add(1, Ordering::Relaxed);
    }
}

pub fn inc_post_quorum_remote_ack() {
    POST_QUORUM_REMOTE_ACKS.fetch_add(1, Ordering::Relaxed);
}

pub fn inc_post_quorum_remote_failure() {
    POST_QUORUM_REMOTE_FAILURES.fetch_add(1, Ordering::Relaxed);
}

pub fn inc_hint_stored(count: usize) {
    HINTS_STORED.fetch_add(count as u64, Ordering::Relaxed);
}

pub fn inc_hint_rejected() {
    HINTS_REJECTED.fetch_add(1, Ordering::Relaxed);
}

pub fn inc_inbound_mutation_forward(rows: usize) {
    INBOUND_MUTATION_FORWARDS.fetch_add(1, Ordering::Relaxed);
    INBOUND_MUTATION_ROWS.fetch_add(rows as u64, Ordering::Relaxed);
}

pub fn inc_inbound_mutation_failure() {
    INBOUND_MUTATION_FAILURES.fetch_add(1, Ordering::Relaxed);
}

pub fn inc_write_admission_in_flight() {
    let current = WRITE_ADMISSION_IN_FLIGHT.fetch_add(1, Ordering::Relaxed) + 1;
    update_max_i64(&WRITE_ADMISSION_IN_FLIGHT_MAX, current);
}

pub fn dec_write_admission_in_flight() {
    WRITE_ADMISSION_IN_FLIGHT.fetch_sub(1, Ordering::Relaxed);
}

/// Record that the read coordinator requested one async anti-entropy repair
/// after serving a read from a healthy replica because a local SSTable was
/// corrupt. Returns the new total (process-wide). Non-zero in CI steady state
/// should alert: it means real SSTable corruption was observed and self-healed.
pub fn inc_anti_entropy_repair_requested() -> u64 {
    ANTI_ENTROPY_REPAIRS_REQUESTED.fetch_add(1, Ordering::Relaxed) + 1
}

/// Process-wide count of async anti-entropy repairs the read coordinator has
/// requested after serving around a corrupt local SSTable.
pub fn anti_entropy_repairs_requested_total() -> u64 {
    ANTI_ENTROPY_REPAIRS_REQUESTED.load(Ordering::Relaxed)
}

pub fn render_prometheus() -> String {
    format!(
        "# HELP ferrosa_coordinator_replica_write_attempts_total Replica write attempts by this coordinator.\n\
         # TYPE ferrosa_coordinator_replica_write_attempts_total counter\n\
         ferrosa_coordinator_replica_write_attempts_total{{target=\"local\"}} {}\n\
         ferrosa_coordinator_replica_write_attempts_total{{target=\"remote\"}} {}\n\
         # HELP ferrosa_coordinator_replica_write_acks_total Successful replica write acknowledgements observed by this coordinator.\n\
         # TYPE ferrosa_coordinator_replica_write_acks_total counter\n\
         ferrosa_coordinator_replica_write_acks_total{{target=\"local\"}} {}\n\
         ferrosa_coordinator_replica_write_acks_total{{target=\"remote\"}} {}\n\
         # HELP ferrosa_coordinator_replica_write_failures_total Failed replica writes observed by this coordinator.\n\
         # TYPE ferrosa_coordinator_replica_write_failures_total counter\n\
         ferrosa_coordinator_replica_write_failures_total{{target=\"local\"}} {}\n\
         ferrosa_coordinator_replica_write_failures_total{{target=\"remote\"}} {}\n\
         # HELP ferrosa_coordinator_post_quorum_remote_acks_total Remote write ACKs observed after client-visible quorum was satisfied.\n\
         # TYPE ferrosa_coordinator_post_quorum_remote_acks_total counter\n\
         ferrosa_coordinator_post_quorum_remote_acks_total {}\n\
         # HELP ferrosa_coordinator_post_quorum_remote_failures_total Remote write failures observed after client-visible quorum was satisfied.\n\
         # TYPE ferrosa_coordinator_post_quorum_remote_failures_total counter\n\
         ferrosa_coordinator_post_quorum_remote_failures_total {}\n\
         # HELP ferrosa_coordinator_hints_stored_total Hints successfully written by coordinators.\n\
         # TYPE ferrosa_coordinator_hints_stored_total counter\n\
         ferrosa_coordinator_hints_stored_total {}\n\
         # HELP ferrosa_coordinator_hints_rejected_total Hint writes rejected by bounded hint backpressure.\n\
         # TYPE ferrosa_coordinator_hints_rejected_total counter\n\
         ferrosa_coordinator_hints_rejected_total {}\n\
         # HELP ferrosa_coordinator_inbound_mutation_forwards_total MutationForward RPCs applied on this node.\n\
         # TYPE ferrosa_coordinator_inbound_mutation_forwards_total counter\n\
         ferrosa_coordinator_inbound_mutation_forwards_total {}\n\
         # HELP ferrosa_coordinator_inbound_mutation_rows_total MutationForward rows applied on this node.\n\
         # TYPE ferrosa_coordinator_inbound_mutation_rows_total counter\n\
         ferrosa_coordinator_inbound_mutation_rows_total {}\n\
         # HELP ferrosa_coordinator_inbound_mutation_failures_total MutationForward writes that failed on this node.\n\
         # TYPE ferrosa_coordinator_inbound_mutation_failures_total counter\n\
         ferrosa_coordinator_inbound_mutation_failures_total {}\n\
         # HELP ferrosa_coordinator_write_admission_in_flight Client-visible writes holding coordinator admission permits, including post-quorum replica drain.\n\
         # TYPE ferrosa_coordinator_write_admission_in_flight gauge\n\
         ferrosa_coordinator_write_admission_in_flight {}\n\
         # HELP ferrosa_coordinator_write_admission_in_flight_max Maximum writes holding coordinator admission permits since process start.\n\
         # TYPE ferrosa_coordinator_write_admission_in_flight_max gauge\n\
         ferrosa_coordinator_write_admission_in_flight_max {}\n\
         # HELP ferrosa_coordinator_anti_entropy_repairs_requested_total Async anti-entropy repairs requested after serving a read around a corrupt local SSTable.\n\
         # TYPE ferrosa_coordinator_anti_entropy_repairs_requested_total counter\n\
         ferrosa_coordinator_anti_entropy_repairs_requested_total {}\n",
        LOCAL_WRITE_ATTEMPTS.load(Ordering::Relaxed),
        REMOTE_WRITE_ATTEMPTS.load(Ordering::Relaxed),
        LOCAL_WRITE_ACKS.load(Ordering::Relaxed),
        REMOTE_WRITE_ACKS.load(Ordering::Relaxed),
        LOCAL_WRITE_FAILURES.load(Ordering::Relaxed),
        REMOTE_WRITE_FAILURES.load(Ordering::Relaxed),
        POST_QUORUM_REMOTE_ACKS.load(Ordering::Relaxed),
        POST_QUORUM_REMOTE_FAILURES.load(Ordering::Relaxed),
        HINTS_STORED.load(Ordering::Relaxed),
        HINTS_REJECTED.load(Ordering::Relaxed),
        INBOUND_MUTATION_FORWARDS.load(Ordering::Relaxed),
        INBOUND_MUTATION_ROWS.load(Ordering::Relaxed),
        INBOUND_MUTATION_FAILURES.load(Ordering::Relaxed),
        WRITE_ADMISSION_IN_FLIGHT.load(Ordering::Relaxed),
        WRITE_ADMISSION_IN_FLIGHT_MAX.load(Ordering::Relaxed),
        ANTI_ENTROPY_REPAIRS_REQUESTED.load(Ordering::Relaxed),
    )
}

/// Prometheus-compatible counters for inline read repair.
pub struct ReadRepairMetrics {
    pub read_repairs_attempted: AtomicI64,
    pub read_repairs_succeeded: AtomicI64,
    pub read_repairs_failed: AtomicI64,
}

impl ReadRepairMetrics {
    pub fn new() -> Self {
        Self {
            read_repairs_attempted: AtomicI64::new(0),
            read_repairs_succeeded: AtomicI64::new(0),
            read_repairs_failed: AtomicI64::new(0),
        }
    }

    pub fn inc_attempted(&self) {
        self.read_repairs_attempted.fetch_add(1, Ordering::Relaxed);
    }

    pub fn inc_succeeded(&self) {
        self.read_repairs_succeeded.fetch_add(1, Ordering::Relaxed);
    }

    pub fn inc_failed(&self) {
        self.read_repairs_failed.fetch_add(1, Ordering::Relaxed);
    }

    /// Renders metrics in Prometheus exposition text format.
    pub fn to_prometheus_text(&self) -> String {
        format!(
            "# HELP ferrosa_read_repairs_attempted_total Read repair attempts\n\
             # TYPE ferrosa_read_repairs_attempted_total counter\n\
             ferrosa_read_repairs_attempted_total {}\n\
             # HELP ferrosa_read_repairs_succeeded_total Successful read repairs\n\
             # TYPE ferrosa_read_repairs_succeeded_total counter\n\
             ferrosa_read_repairs_succeeded_total {}\n\
             # HELP ferrosa_read_repairs_failed_total Failed read repairs\n\
             # TYPE ferrosa_read_repairs_failed_total counter\n\
             ferrosa_read_repairs_failed_total {}\n",
            self.read_repairs_attempted.load(Ordering::Relaxed),
            self.read_repairs_succeeded.load(Ordering::Relaxed),
            self.read_repairs_failed.load(Ordering::Relaxed),
        )
    }
}

impl Default for ReadRepairMetrics {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn metrics_increment_and_render() {
        let m = ReadRepairMetrics::new();
        m.inc_attempted();
        m.inc_attempted();
        m.inc_succeeded();
        m.inc_failed();

        let text = m.to_prometheus_text();
        assert!(text.contains("ferrosa_read_repairs_attempted_total 2"));
        assert!(text.contains("ferrosa_read_repairs_succeeded_total 1"));
        assert!(text.contains("ferrosa_read_repairs_failed_total 1"));
    }

    #[test]
    fn metrics_default_zero() {
        let m = ReadRepairMetrics::new();
        let text = m.to_prometheus_text();
        assert!(text.contains("ferrosa_read_repairs_attempted_total 0"));
        assert!(text.contains("ferrosa_read_repairs_succeeded_total 0"));
        assert!(text.contains("ferrosa_read_repairs_failed_total 0"));
    }

    #[test]
    fn metrics_thread_safe() {
        use std::sync::Arc;
        let m = Arc::new(ReadRepairMetrics::new());
        let handles: Vec<_> = (0..10)
            .map(|_| {
                let m = Arc::clone(&m);
                std::thread::spawn(move || {
                    for _ in 0..100 {
                        m.inc_attempted();
                    }
                })
            })
            .collect();
        for h in handles {
            h.join().unwrap();
        }
        assert_eq!(m.read_repairs_attempted.load(Ordering::Relaxed), 1000);
    }
}
