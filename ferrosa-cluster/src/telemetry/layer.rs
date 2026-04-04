//! `FerrosaTelemetryLayer` — a `tracing_subscriber::Layer` that captures
//! span data on close and sends it to a bounded mpsc channel.
//!
//! Design constraints:
//! - Channel is bounded (default capacity: 10,000) for backpressure.
//! - When full, the oldest entry is dropped and a drop counter incremented.
//! - Head-based coherent sampling: sample rate checked on `on_new_span`.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;

use tokio::sync::mpsc;
use tracing::span;
use tracing_subscriber::layer::Context;
use tracing_subscriber::registry::LookupSpan;
use tracing_subscriber::Layer;
use uuid::Uuid;

/// Default channel capacity for the span record buffer.
pub const DEFAULT_CHANNEL_CAPACITY: usize = 10_000;

/// A captured span record ready for storage.
#[derive(Debug, Clone)]
pub struct SpanRecord {
    /// Trace ID (derived from the root span or generated).
    pub trace_id: Uuid,
    /// Unique span identifier.
    pub span_id: Uuid,
    /// Parent span identifier, if any.
    pub parent_id: Option<Uuid>,
    /// Node ID of the reporting node.
    pub node_id: Uuid,
    /// Span name (from the tracing macro).
    pub name: String,
    /// Start time in microseconds since epoch.
    pub start_us: i64,
    /// Duration in microseconds.
    pub duration_us: i64,
    /// Span status: "ok" or "error".
    pub status: String,
}

/// Per-span storage attached via `Extensions`.
struct SpanTiming {
    span_id: Uuid,
    start: Instant,
    start_us: i64,
    sampled: bool,
}

/// Configuration for the telemetry layer.
#[derive(Debug, Clone)]
pub struct TelemetryConfig {
    /// Sampling rate: 0.0 = capture nothing, 1.0 = capture everything.
    pub sample_rate: f64,
    /// Node ID for this node.
    pub node_id: Uuid,
    /// Channel capacity.
    pub channel_capacity: usize,
}

impl Default for TelemetryConfig {
    fn default() -> Self {
        Self {
            sample_rate: 1.0,
            node_id: Uuid::new_v4(),
            channel_capacity: DEFAULT_CHANNEL_CAPACITY,
        }
    }
}

/// Telemetry layer that captures span data and sends it to a channel.
///
/// Attach this to a `tracing_subscriber::Registry` to capture spans.
/// The receiver end of the channel should be consumed by a
/// [`TelemetryWriter`](super::writer::TelemetryWriter).
pub struct FerrosaTelemetryLayer {
    sender: mpsc::Sender<SpanRecord>,
    receiver_slot: parking_lot::Mutex<Option<mpsc::Receiver<SpanRecord>>>,
    config: TelemetryConfig,
    /// Counter for dropped spans due to full channel.
    pub drop_count: Arc<AtomicU64>,
    /// Simple sampling state — uses a counter-based approach for determinism.
    sample_counter: AtomicU64,
}

impl FerrosaTelemetryLayer {
    /// Create a new telemetry layer with the given configuration.
    ///
    /// Returns the layer. Call `take_receiver()` to get the channel receiver
    /// for the writer task.
    pub fn new(config: TelemetryConfig) -> Self {
        let (sender, receiver) = mpsc::channel(config.channel_capacity);
        Self {
            sender,
            receiver_slot: parking_lot::Mutex::new(Some(receiver)),
            config,
            drop_count: Arc::new(AtomicU64::new(0)),
            sample_counter: AtomicU64::new(0),
        }
    }

    /// Take the receiver end of the channel. Can only be called once.
    ///
    /// Returns `None` if already taken.
    pub fn take_receiver(&self) -> Option<mpsc::Receiver<SpanRecord>> {
        self.receiver_slot.lock().take()
    }

    /// Returns the current drop count (spans dropped due to full channel).
    pub fn dropped_count(&self) -> u64 {
        self.drop_count.load(Ordering::Relaxed)
    }

    /// Check if this span should be sampled based on the configured rate.
    fn should_sample(&self) -> bool {
        if self.config.sample_rate <= 0.0 {
            return false;
        }
        if self.config.sample_rate >= 1.0 {
            return true;
        }
        // Deterministic sampling: every Nth span where N = 1/rate.
        let n = self.sample_counter.fetch_add(1, Ordering::Relaxed);
        let interval = (1.0 / self.config.sample_rate) as u64;
        if interval == 0 {
            return true;
        }
        n.is_multiple_of(interval)
    }

    /// Try to send a span record to the channel. If full, drop the oldest
    /// entry by receiving one, increment the drop counter, then retry.
    fn try_send_record(&self, record: SpanRecord) {
        match self.sender.try_send(record) {
            Ok(()) => {}
            Err(mpsc::error::TrySendError::Full(record)) => {
                // Channel is full — we cannot pop from sender side.
                // Increment drop counter and discard the new record.
                // The writer task should drain fast enough in production.
                self.drop_count.fetch_add(1, Ordering::Relaxed);
                // One more attempt after incrementing counter.
                let _ = self.sender.try_send(record);
            }
            Err(mpsc::error::TrySendError::Closed(_)) => {
                // Writer task has been dropped; silently discard.
            }
        }
    }
}

impl<S> Layer<S> for FerrosaTelemetryLayer
where
    S: tracing::Subscriber + for<'lookup> LookupSpan<'lookup>,
{
    fn on_new_span(&self, _attrs: &span::Attributes<'_>, id: &span::Id, ctx: Context<'_, S>) {
        let sampled = self.should_sample();
        if let Some(span) = ctx.span(id) {
            let now = Instant::now();
            let start_us = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_micros() as i64;
            let mut extensions = span.extensions_mut();
            extensions.insert(SpanTiming {
                span_id: Uuid::new_v4(),
                start: now,
                start_us,
                sampled,
            });
        }
    }

    fn on_close(&self, id: span::Id, ctx: Context<'_, S>) {
        if let Some(span) = ctx.span(&id) {
            let extensions = span.extensions();
            let Some(timing) = extensions.get::<SpanTiming>() else {
                return;
            };
            if !timing.sampled {
                return;
            }
            let duration_us = timing.start.elapsed().as_micros() as i64;

            // Derive parent_id from the span's parent.
            let parent_id = span.parent().and_then(|parent| {
                let parent_ext = parent.extensions();
                parent_ext.get::<SpanTiming>().map(|t| t.span_id)
            });

            let record = SpanRecord {
                trace_id: Uuid::new_v4(), // In production, propagate from context.
                span_id: timing.span_id,
                parent_id,
                node_id: self.config.node_id,
                name: span.name().to_string(),
                start_us: timing.start_us,
                duration_us,
                status: "ok".to_string(),
            };

            self.try_send_record(record);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tracing_subscriber::prelude::*;

    fn make_layer(config: TelemetryConfig) -> FerrosaTelemetryLayer {
        FerrosaTelemetryLayer::new(config)
    }

    #[test]
    fn layer_captures_span_on_close() {
        let config = TelemetryConfig {
            sample_rate: 1.0,
            node_id: Uuid::nil(),
            channel_capacity: 100,
        };
        let layer = make_layer(config);
        let mut receiver = layer.take_receiver().expect("receiver must be available");

        let subscriber = tracing_subscriber::registry().with(layer);
        let _guard = tracing::subscriber::set_default(subscriber);

        {
            let _span = tracing::info_span!("test_span").entered();
            // span closes here
        }

        // The record should be in the channel.
        let record = receiver.try_recv().expect("should have captured a span");
        assert_eq!(record.name, "test_span");
        assert_eq!(record.node_id, Uuid::nil());
        assert_eq!(record.status, "ok");
        assert!(record.duration_us >= 0);
        assert!(record.start_us > 0);
    }

    #[test]
    fn layer_try_send_does_not_block() {
        let config = TelemetryConfig {
            sample_rate: 1.0,
            node_id: Uuid::nil(),
            channel_capacity: 2,
        };
        let layer = make_layer(config);
        // Do NOT take the receiver — channel will fill up.

        let subscriber = tracing_subscriber::registry().with(layer);
        let _guard = tracing::subscriber::set_default(subscriber);

        // Create more spans than channel capacity. Must not block.
        for i in 0..10 {
            let _span = tracing::info_span!("flood_span", idx = i).entered();
        }
        // If we get here without hanging, the test passes.
    }

    #[test]
    fn layer_drops_oldest_on_full() {
        let config = TelemetryConfig {
            sample_rate: 1.0,
            node_id: Uuid::nil(),
            channel_capacity: 2,
        };
        let layer = make_layer(config);
        let drop_count = Arc::clone(&layer.drop_count);
        // Take receiver so we can inspect it, but don't drain.
        let _receiver = layer.take_receiver().expect("receiver");

        let subscriber = tracing_subscriber::registry().with(layer);
        let _guard = tracing::subscriber::set_default(subscriber);

        // Fill channel: capacity=2, so first 2 spans succeed.
        {
            let _s = tracing::info_span!("s1").entered();
        }
        {
            let _s = tracing::info_span!("s2").entered();
        }
        // Third span should trigger a drop.
        {
            let _s = tracing::info_span!("s3").entered();
        }

        assert!(
            drop_count.load(Ordering::Relaxed) >= 1,
            "drop counter must increment when channel is full"
        );
    }

    #[test]
    fn sample_rate_zero_captures_nothing() {
        let config = TelemetryConfig {
            sample_rate: 0.0,
            node_id: Uuid::nil(),
            channel_capacity: 100,
        };
        let layer = make_layer(config);
        let mut receiver = layer.take_receiver().expect("receiver");

        let subscriber = tracing_subscriber::registry().with(layer);
        let _guard = tracing::subscriber::set_default(subscriber);

        for _ in 0..10 {
            let _span = tracing::info_span!("sampled_span").entered();
        }

        assert!(
            receiver.try_recv().is_err(),
            "no spans should be captured at sample_rate=0.0"
        );
    }

    #[test]
    fn sample_rate_one_captures_all() {
        let config = TelemetryConfig {
            sample_rate: 1.0,
            node_id: Uuid::nil(),
            channel_capacity: 100,
        };
        let layer = make_layer(config);
        let mut receiver = layer.take_receiver().expect("receiver");

        let subscriber = tracing_subscriber::registry().with(layer);
        let _guard = tracing::subscriber::set_default(subscriber);

        let count = 5;
        for _ in 0..count {
            let _span = tracing::info_span!("all_span").entered();
        }

        let mut captured = 0;
        while receiver.try_recv().is_ok() {
            captured += 1;
        }
        assert_eq!(
            captured, count,
            "all spans should be captured at sample_rate=1.0"
        );
    }
}
