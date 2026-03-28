use std::sync::atomic::{AtomicI64, Ordering};

/// Prometheus-compatible metrics for compaction S3 operations.
pub struct CompactionMetrics {
    /// Number of compacted SSTables successfully uploaded to S3.
    pub s3_uploads_total: AtomicI64,
    /// Number of input SSTables deleted from S3 after compaction.
    pub s3_deletes_total: AtomicI64,
    /// Total input bytes freed by completed compactions (gauge).
    pub input_bytes_reclaimed: AtomicI64,
}

impl CompactionMetrics {
    pub fn new() -> Self {
        Self {
            s3_uploads_total: AtomicI64::new(0),
            s3_deletes_total: AtomicI64::new(0),
            input_bytes_reclaimed: AtomicI64::new(0),
        }
    }

    /// Increments the S3 upload counter by 1.
    pub fn inc_s3_uploads(&self) {
        self.s3_uploads_total.fetch_add(1, Ordering::Relaxed);
    }

    /// Increments the S3 delete counter by 1.
    pub fn inc_s3_deletes(&self) {
        self.s3_deletes_total.fetch_add(1, Ordering::Relaxed);
    }

    /// Adds `bytes` to the input bytes reclaimed gauge.
    pub fn add_bytes_reclaimed(&self, bytes: i64) {
        self.input_bytes_reclaimed.fetch_add(bytes, Ordering::Relaxed);
    }

    /// Renders metrics in Prometheus exposition text format.
    pub fn to_prometheus_text(&self) -> String {
        format!(
            "# HELP ferrosa_compaction_s3_uploads_total Compacted SSTables uploaded to S3\n\
             # TYPE ferrosa_compaction_s3_uploads_total counter\n\
             ferrosa_compaction_s3_uploads_total {}\n\
             # HELP ferrosa_compaction_s3_deletes_total Input SSTables deleted from S3 after compaction\n\
             # TYPE ferrosa_compaction_s3_deletes_total counter\n\
             ferrosa_compaction_s3_deletes_total {}\n\
             # HELP ferrosa_compaction_input_bytes_reclaimed Total bytes freed by completed compactions\n\
             # TYPE ferrosa_compaction_input_bytes_reclaimed gauge\n\
             ferrosa_compaction_input_bytes_reclaimed {}\n",
            self.s3_uploads_total.load(Ordering::Relaxed),
            self.s3_deletes_total.load(Ordering::Relaxed),
            self.input_bytes_reclaimed.load(Ordering::Relaxed),
        )
    }
}

impl Default for CompactionMetrics {
    fn default() -> Self {
        Self::new()
    }
}

/// Prometheus-compatible metrics for PITR archiving and snapshots.
pub struct PitrMetrics {
    pub archive_segments_uploaded: AtomicI64,
    pub archive_upload_errors: AtomicI64,
    pub archive_lag_segments: AtomicI64,
    pub snapshots_total: AtomicI64,
}

impl PitrMetrics {
    pub fn new() -> Self {
        Self {
            archive_segments_uploaded: AtomicI64::new(0),
            archive_upload_errors: AtomicI64::new(0),
            archive_lag_segments: AtomicI64::new(0),
            snapshots_total: AtomicI64::new(0),
        }
    }

    pub fn inc_segments_uploaded(&self) {
        self.archive_segments_uploaded
            .fetch_add(1, Ordering::Relaxed);
    }

    pub fn inc_upload_errors(&self) {
        self.archive_upload_errors.fetch_add(1, Ordering::Relaxed);
    }

    pub fn set_archive_lag(&self, lag: i64) {
        self.archive_lag_segments.store(lag, Ordering::Relaxed);
    }

    pub fn set_snapshots_total(&self, count: i64) {
        self.snapshots_total.store(count, Ordering::Relaxed);
    }

    /// Renders metrics in Prometheus exposition text format.
    pub fn to_prometheus_text(&self) -> String {
        format!(
            "# HELP ferrosa_archive_segments_uploaded_total Total archived segments\n\
             # TYPE ferrosa_archive_segments_uploaded_total counter\n\
             ferrosa_archive_segments_uploaded_total {}\n\
             # HELP ferrosa_archive_upload_errors_total Total upload errors\n\
             # TYPE ferrosa_archive_upload_errors_total counter\n\
             ferrosa_archive_upload_errors_total {}\n\
             # HELP ferrosa_archive_lag_segments Current archive lag\n\
             # TYPE ferrosa_archive_lag_segments gauge\n\
             ferrosa_archive_lag_segments {}\n\
             # HELP ferrosa_snapshots_total Current snapshot count\n\
             # TYPE ferrosa_snapshots_total gauge\n\
             ferrosa_snapshots_total {}\n",
            self.archive_segments_uploaded.load(Ordering::Relaxed),
            self.archive_upload_errors.load(Ordering::Relaxed),
            self.archive_lag_segments.load(Ordering::Relaxed),
            self.snapshots_total.load(Ordering::Relaxed),
        )
    }
}

impl Default for PitrMetrics {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn metrics_increment_and_render() {
        let m = PitrMetrics::new();
        m.inc_segments_uploaded();
        m.inc_segments_uploaded();
        m.inc_upload_errors();
        m.set_archive_lag(3);
        m.set_snapshots_total(5);
        let text = m.to_prometheus_text();
        assert!(text.contains("ferrosa_archive_segments_uploaded_total 2"));
        assert!(text.contains("ferrosa_archive_upload_errors_total 1"));
        assert!(text.contains("ferrosa_archive_lag_segments 3"));
        assert!(text.contains("ferrosa_snapshots_total 5"));
    }

    #[test]
    fn metrics_default_zero() {
        let m = PitrMetrics::new();
        let text = m.to_prometheus_text();
        assert!(text.contains("ferrosa_archive_segments_uploaded_total 0"));
    }

    #[test]
    fn metrics_thread_safe() {
        use std::sync::Arc;
        let m = Arc::new(PitrMetrics::new());
        let handles: Vec<_> = (0..10)
            .map(|_| {
                let m = Arc::clone(&m);
                std::thread::spawn(move || {
                    for _ in 0..100 {
                        m.inc_segments_uploaded();
                    }
                })
            })
            .collect();
        for h in handles {
            h.join().unwrap();
        }
        assert_eq!(m.archive_segments_uploaded.load(Ordering::Relaxed), 1000);
    }
}
