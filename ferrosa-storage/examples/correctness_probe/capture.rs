//! Fixed-capacity numeric tracing subscriber; export happens outside hot windows.
//! Correctness: malformed events or overflow invalidate the entire capture.
//! Last revised: 2026-10-29
//! Last changed: Capture write, sync, acknowledgment, restart and recovery events.

use std::fmt;
use std::io::Write;
use std::sync::{Arc, Mutex};
use tracing::field::{Field, Visit};
use tracing::span::{Attributes, Id, Record};
use tracing::{Event, Metadata, Subscriber};

pub const TARGET: &str = "ferrosa::correctness";

#[derive(Default)]
struct RecordVisitor {
    values: [u64; 5],
    seen: u8,
    invalid: bool,
}

impl Visit for RecordVisitor {
    fn record_u64(&mut self, field: &Field, value: u64) {
        let index = match field.name() {
            "kind" => 0,
            "segment" => 1,
            "offset" => 2,
            "end" => 3,
            "checksum" => 4,
            _ => {
                self.invalid = true;
                return;
            }
        };
        if self.seen & (1 << index) != 0 {
            self.invalid = true;
        }
        self.seen |= 1 << index;
        self.values[index] = value;
    }
    fn record_debug(&mut self, _: &Field, _: &dyn fmt::Debug) {
        self.invalid = true;
    }
}

struct Buffer {
    events: Vec<[u64; 5]>,
    limit: usize,
    dropped: usize,
}

#[derive(Clone)]
pub struct Capture(Arc<Mutex<Buffer>>);

impl Capture {
    pub fn new(limit: usize) -> Self {
        Self(Arc::new(Mutex::new(Buffer {
            events: Vec::with_capacity(limit),
            limit,
            dropped: 0,
        })))
    }

    pub fn events(&self) -> Result<Vec<[u64; 5]>, Box<dyn std::error::Error>> {
        let buffer = self.0.lock().map_err(|_| "capture lock poisoned")?;
        if buffer.dropped != 0 {
            return Err("incomplete or malformed capture".into());
        }
        Ok(buffer.events.clone())
    }

    pub fn export(&self, path: &str) -> Result<(), Box<dyn std::error::Error>> {
        let events = self.events()?;
        let mut output = std::io::BufWriter::new(std::fs::File::create(path)?);
        writeln!(output, "FERROSA_WAL_TRACE_V1")?;
        for [kind, segment, offset, end, checksum] in &events {
            writeln!(output, "{kind} {segment} {offset} {end} {checksum}")?;
        }
        // The checker requires an explicit end record and exact event count.
        writeln!(output, "6 {} 0 0 0", events.len())?;
        output.flush()?;
        Ok(())
    }
}

impl Subscriber for Capture {
    fn enabled(&self, metadata: &Metadata<'_>) -> bool {
        metadata.is_event() && metadata.target() == TARGET
    }
    fn max_level_hint(&self) -> Option<tracing::level_filters::LevelFilter> {
        Some(tracing::level_filters::LevelFilter::TRACE)
    }
    fn event(&self, event: &Event<'_>) {
        let mut visitor = RecordVisitor::default();
        event.record(&mut visitor);
        let mut buffer = self.0.lock().expect("capture lock poisoned");
        if visitor.invalid || visitor.seen != 31 || buffer.events.len() == buffer.limit {
            buffer.dropped += 1;
        } else {
            buffer.events.push(visitor.values);
        }
    }
    fn new_span(&self, _: &Attributes<'_>) -> Id {
        Id::from_u64(1)
    }
    fn record(&self, _: &Id, _: &Record<'_>) {}
    fn record_follows_from(&self, _: &Id, _: &Id) {}
    fn enter(&self, _: &Id) {}
    fn exit(&self, _: &Id) {}
}

pub fn boundary(kind: u64, segment: u64, offset: u64) {
    tracing::event!(target: "ferrosa::correctness", tracing::Level::TRACE,
        kind, segment, offset, end = 0_u64, checksum = 0_u64);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn overflow_and_bad_fields_fail_closed() {
        let capture = Capture::new(1);
        tracing::subscriber::with_default(capture.clone(), || {
            boundary(4, 0, 0);
            boundary(4, 0, 0);
        });
        assert!(capture.events().is_err());
        let capture = Capture::new(8);
        tracing::subscriber::with_default(capture.clone(), || {
            tracing::event!(target: "ferrosa::correctness", tracing::Level::TRACE, kind = 1_u64);
        });
        assert!(capture.events().is_err());
    }
}
