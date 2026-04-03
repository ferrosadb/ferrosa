//! Built-in telemetry pipeline: captures tracing spans and writes them
//! to `system_observability.spans` via the storage engine.
//!
//! The pipeline has two parts:
//! - [`FerrosaTelemetryLayer`]: a `tracing_subscriber::Layer` that captures
//!   span data on close and sends it to a bounded mpsc channel.
//! - [`TelemetryWriter`]: a background task that drains the channel in batches
//!   and writes to `StorageEngine::write_observability()`.

pub mod layer;
pub mod writer;

pub use layer::{FerrosaTelemetryLayer, SpanRecord};
pub use writer::TelemetryWriter;
