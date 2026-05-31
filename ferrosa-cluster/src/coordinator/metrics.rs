use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};

static LOCAL_WRITE_ATTEMPTS: AtomicU64 = AtomicU64::new(0);
static REMOTE_WRITE_ATTEMPTS: AtomicU64 = AtomicU64::new(0);
static LOCAL_WRITE_ACKS: AtomicU64 = AtomicU64::new(0);
static REMOTE_WRITE_ACKS: AtomicU64 = AtomicU64::new(0);
static LOCAL_WRITE_FAILURES: AtomicU64 = AtomicU64::new(0);
static REMOTE_WRITE_FAILURES: AtomicU64 = AtomicU64::new(0);
static INBOUND_MUTATION_FORWARDS: AtomicU64 = AtomicU64::new(0);
static INBOUND_MUTATION_ROWS: AtomicU64 = AtomicU64::new(0);
static INBOUND_MUTATION_FAILURES: AtomicU64 = AtomicU64::new(0);

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

pub fn inc_inbound_mutation_forward(rows: usize) {
    INBOUND_MUTATION_FORWARDS.fetch_add(1, Ordering::Relaxed);
    INBOUND_MUTATION_ROWS.fetch_add(rows as u64, Ordering::Relaxed);
}

pub fn inc_inbound_mutation_failure() {
    INBOUND_MUTATION_FAILURES.fetch_add(1, Ordering::Relaxed);
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
         # HELP ferrosa_coordinator_inbound_mutation_forwards_total MutationForward RPCs applied on this node.\n\
         # TYPE ferrosa_coordinator_inbound_mutation_forwards_total counter\n\
         ferrosa_coordinator_inbound_mutation_forwards_total {}\n\
         # HELP ferrosa_coordinator_inbound_mutation_rows_total MutationForward rows applied on this node.\n\
         # TYPE ferrosa_coordinator_inbound_mutation_rows_total counter\n\
         ferrosa_coordinator_inbound_mutation_rows_total {}\n\
         # HELP ferrosa_coordinator_inbound_mutation_failures_total MutationForward writes that failed on this node.\n\
         # TYPE ferrosa_coordinator_inbound_mutation_failures_total counter\n\
         ferrosa_coordinator_inbound_mutation_failures_total {}\n",
        LOCAL_WRITE_ATTEMPTS.load(Ordering::Relaxed),
        REMOTE_WRITE_ATTEMPTS.load(Ordering::Relaxed),
        LOCAL_WRITE_ACKS.load(Ordering::Relaxed),
        REMOTE_WRITE_ACKS.load(Ordering::Relaxed),
        LOCAL_WRITE_FAILURES.load(Ordering::Relaxed),
        REMOTE_WRITE_FAILURES.load(Ordering::Relaxed),
        INBOUND_MUTATION_FORWARDS.load(Ordering::Relaxed),
        INBOUND_MUTATION_ROWS.load(Ordering::Relaxed),
        INBOUND_MUTATION_FAILURES.load(Ordering::Relaxed),
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
