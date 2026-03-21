use std::sync::atomic::{AtomicI64, Ordering};

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
