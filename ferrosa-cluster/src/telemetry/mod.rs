//! Ferrosa telemetry layer for the `tracing` subscriber stack.
//!
//! [`FerrosaTelemetryLayer`] is a lightweight [`tracing_subscriber::Layer`]
//! that samples span events and records them for observability dashboards.
//! It is conditionally added to the subscriber when `FERROSA_TELEMETRY_ENABLED=true`.
//!
//! # Configuration
//!
//! | Env var | Default | Description |
//! |---------|---------|-------------|
//! | `FERROSA_TELEMETRY_SAMPLE_RATE` | `0.01` | Fraction of spans to sample (0.0..=1.0) |

use std::sync::atomic::{AtomicU64, Ordering};

use tracing::Subscriber;

/// A lightweight telemetry layer that samples span close events.
///
/// Records sampled span names and durations into atomic counters
/// for downstream consumption by the observability console and
/// Prometheus exporter.
pub struct FerrosaTelemetryLayer {
    /// Sample rate as a fraction (0.0..=1.0). A value of 0.01 means
    /// roughly 1 in 100 spans are recorded.
    sample_rate: f64,

    /// Monotonically increasing counter used for deterministic sampling.
    /// Every span increments this; if `counter % (1/sample_rate)` == 0
    /// the span is sampled.
    counter: AtomicU64,

    /// Total number of sampled spans since startup.
    pub sampled_count: AtomicU64,
}

impl FerrosaTelemetryLayer {
    /// Create a new telemetry layer with the given sample rate.
    ///
    /// # Panics
    ///
    /// Panics if `sample_rate` is negative.
    pub fn new(sample_rate: f64) -> Self {
        assert!(
            sample_rate >= 0.0,
            "sample_rate must be non-negative, got {sample_rate}"
        );
        Self {
            sample_rate,
            counter: AtomicU64::new(0),
            sampled_count: AtomicU64::new(0),
        }
    }

    /// Read the configured sample rate from the environment.
    ///
    /// Returns the value of `FERROSA_TELEMETRY_SAMPLE_RATE` parsed as `f64`,
    /// or `0.01` if the variable is absent or unparseable.
    pub fn sample_rate_from_env() -> f64 {
        std::env::var("FERROSA_TELEMETRY_SAMPLE_RATE")
            .ok()
            .and_then(|s| s.parse::<f64>().ok())
            .unwrap_or(0.01)
    }

    /// Returns true if this span should be sampled based on the counter.
    fn should_sample(&self) -> bool {
        if self.sample_rate <= 0.0 {
            return false;
        }
        if self.sample_rate >= 1.0 {
            return true;
        }
        let n = self.counter.fetch_add(1, Ordering::Relaxed);
        let interval = (1.0 / self.sample_rate) as u64;
        if interval == 0 {
            return true;
        }
        n % interval == 0
    }

    /// Returns the number of sampled spans.
    pub fn sampled(&self) -> u64 {
        self.sampled_count.load(Ordering::Relaxed)
    }
}

impl<S: Subscriber> tracing_subscriber::Layer<S> for FerrosaTelemetryLayer {
    fn on_close(&self, _id: tracing::span::Id, _ctx: tracing_subscriber::layer::Context<'_, S>) {
        if self.should_sample() {
            self.sampled_count.fetch_add(1, Ordering::Relaxed);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sample_rate_zero_never_samples() {
        let layer = FerrosaTelemetryLayer::new(0.0);
        for _ in 0..1000 {
            assert!(!layer.should_sample());
        }
    }

    #[test]
    fn sample_rate_one_always_samples() {
        let layer = FerrosaTelemetryLayer::new(1.0);
        for _ in 0..100 {
            assert!(layer.should_sample());
        }
    }

    #[test]
    fn sample_rate_half_samples_roughly_half() {
        let layer = FerrosaTelemetryLayer::new(0.5);
        let mut sampled = 0u64;
        let total = 1000u64;
        for _ in 0..total {
            if layer.should_sample() {
                sampled += 1;
            }
        }
        // With rate 0.5, interval=2, so exactly 500 should be sampled.
        assert_eq!(sampled, 500);
    }

    #[test]
    #[should_panic(expected = "sample_rate must be non-negative")]
    fn negative_sample_rate_panics() {
        FerrosaTelemetryLayer::new(-0.1);
    }
}
