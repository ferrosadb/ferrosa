//! Change-data-capture (CDC) event types and bus for Ferrosa SUBSCRIBE.
//!
//! SUBSCRIBE was scoped to expose two optional, event-driven change streams,
//! not a polled SELECT snapshot (see
//! `specs/proposed/arrow-flight-endpoint/subscribe-cdc-architecture.md`):
//!
//! - [`CdcStream::WrittenOnNode`] — every mutation durably written to this
//!   node's commit log, ordered by the mutation timestamp.
//! - [`CdcStream::CommittedToCluster`] — mutations the cluster has agreed/acked
//!   (Accord commit, or a regular-CL quorum ack on the coordinator).
//!
//! This crate is a foundation-layer crate: it depends only on `ferrosa-common`
//! and `ferrosa-sstable` so that producers (`ferrosa-storage`, `ferrosa-cluster`)
//! and consumers (`ferrosa-cql`, `ferrosa-flight`) can all depend on it without
//! introducing a dependency cycle. In particular [`CdcEvent`] reuses
//! [`ferrosa_sstable::types::Row`] and never embeds `ferrosa_storage::Mutation`.
//!
//! The bus is bounded per subscriber. A subscriber that falls behind receives an
//! explicit [`CdcRecvError::Lagged`] gap signal carrying the number of dropped
//! events — it is never silently skipped (FMEA F16/F18). A slow subscriber can
//! therefore never block a producer (the write path).

use std::sync::Arc;

use ferrosa_common::accord::Timestamp as AccordTimestamp;
use ferrosa_common::DecoratedKey;
use ferrosa_sstable::types::Row;
use tokio::sync::broadcast;

/// Which change stream an event belongs to. A subscriber selects one stream;
/// to follow both, open two subscriptions.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum CdcStream {
    /// Local durable writes (commit-log append), ordered by mutation timestamp.
    WrittenOnNode,
    /// Cluster-agreed writes (Accord commit / regular-CL quorum ack).
    CommittedToCluster,
}

/// A single change event delivered on a CDC stream.
///
/// Carries the same payload a commit-log `Mutation` does, but expressed only in
/// foundation-layer types so this crate stays below `ferrosa-storage` in the
/// dependency graph. `mutation_id` is the dedup key for at-least-once delivery.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CdcEvent {
    /// Stream this event belongs to.
    pub stream: CdcStream,
    /// Target keyspace.
    pub keyspace: String,
    /// Target table.
    pub table: String,
    /// Decorated partition key.
    pub key: DecoratedKey,
    /// Mutated rows (reused verbatim from the commit-log mutation).
    pub rows: Vec<Row>,
    /// Mutation/write timestamp in microseconds. Ordering key for
    /// `WrittenOnNode` and for regular-CL `CommittedToCluster` events.
    pub timestamp: i64,
    /// Agreed Accord timestamp, present only for Accord-committed transactions
    /// on the `CommittedToCluster` stream. `None` for regular-CL and local writes.
    pub accord_ts: Option<AccordTimestamp>,
    /// Unique mutation id (commit-log `mutation_id`); the dedup key.
    pub mutation_id: [u8; 16],
}

/// Error returned by [`CdcSubscription::recv`] / [`CdcSubscription::try_recv`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CdcRecvError {
    /// The subscriber fell behind and `skipped` events were dropped from its
    /// bounded queue. This is the **gap signal**: the consumer must resync from
    /// a checkpoint rather than assume continuity. Never silent.
    Lagged { skipped: u64 },
    /// No event is currently available (non-blocking `try_recv` only).
    Empty,
    /// The bus was dropped; the stream is closed and will yield no more events.
    Closed,
}

/// A bounded, multi-subscriber CDC event bus with one channel per stream.
///
/// Producers call [`CdcBus::publish`]; consumers call [`CdcBus::subscribe`].
/// The per-stream capacity bounds memory; overflow surfaces as
/// [`CdcRecvError::Lagged`] on the lagging subscriber only.
#[derive(Debug)]
pub struct CdcBus {
    written_on_node: broadcast::Sender<CdcEvent>,
    committed: broadcast::Sender<CdcEvent>,
}

impl CdcBus {
    /// Create a bus whose per-stream subscriber queue holds at most `capacity`
    /// events before a slow subscriber starts receiving gap signals.
    ///
    /// # Panics
    /// Panics if `capacity == 0` (a zero-capacity stream could never deliver).
    pub fn new(capacity: usize) -> Arc<Self> {
        assert!(capacity > 0, "CDC bus capacity must be non-zero");
        let (written_on_node, _) = broadcast::channel(capacity);
        let (committed, _) = broadcast::channel(capacity);
        Arc::new(Self {
            written_on_node,
            committed,
        })
    }

    fn sender(&self, stream: CdcStream) -> &broadcast::Sender<CdcEvent> {
        match stream {
            CdcStream::WrittenOnNode => &self.written_on_node,
            CdcStream::CommittedToCluster => &self.committed,
        }
    }

    /// Publish an event to its stream. Returns the number of live subscribers
    /// the event was delivered to (`0` when nobody is subscribed — a normal,
    /// non-error case: a producer never blocks or fails on absent consumers).
    pub fn publish(&self, event: CdcEvent) -> usize {
        let sender = self.sender(event.stream);
        // `send` errors only when there are zero receivers; that is expected
        // when no consumer is attached, so we treat it as "delivered to none".
        sender.send(event).unwrap_or(0)
    }

    /// Whether `stream` currently has at least one live subscriber.
    ///
    /// Producers on a hot path (e.g. the commit-log append) use this to avoid
    /// building/cloning a [`CdcEvent`] when nobody is listening.
    pub fn has_subscribers(&self, stream: CdcStream) -> bool {
        self.sender(stream).receiver_count() > 0
    }

    /// Subscribe to a single stream. Each subscription has its own bounded
    /// queue; a slow one only affects itself.
    pub fn subscribe(&self, stream: CdcStream) -> CdcSubscription {
        CdcSubscription {
            stream,
            rx: self.sender(stream).subscribe(),
        }
    }
}

/// A consumer's handle to one CDC stream.
#[derive(Debug)]
pub struct CdcSubscription {
    stream: CdcStream,
    rx: broadcast::Receiver<CdcEvent>,
}

impl CdcSubscription {
    /// The stream this subscription follows.
    pub fn stream(&self) -> CdcStream {
        self.stream
    }

    /// Await the next event. Returns [`CdcRecvError::Lagged`] (gap signal) if the
    /// queue overflowed, or [`CdcRecvError::Closed`] once the bus is gone.
    pub async fn recv(&mut self) -> Result<CdcEvent, CdcRecvError> {
        match self.rx.recv().await {
            Ok(event) => Ok(event),
            Err(broadcast::error::RecvError::Lagged(skipped)) => {
                Err(CdcRecvError::Lagged { skipped })
            }
            Err(broadcast::error::RecvError::Closed) => Err(CdcRecvError::Closed),
        }
    }

    /// Non-blocking variant of [`recv`](Self::recv).
    pub fn try_recv(&mut self) -> Result<CdcEvent, CdcRecvError> {
        match self.rx.try_recv() {
            Ok(event) => Ok(event),
            Err(broadcast::error::TryRecvError::Lagged(skipped)) => {
                Err(CdcRecvError::Lagged { skipped })
            }
            Err(broadcast::error::TryRecvError::Empty) => Err(CdcRecvError::Empty),
            Err(broadcast::error::TryRecvError::Closed) => Err(CdcRecvError::Closed),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ferrosa_common::PartitionKey;
    use ferrosa_sstable::types::{DeletionTime, LivenessInfo, Row};

    fn event(stream: CdcStream, id: u8) -> CdcEvent {
        CdcEvent {
            stream,
            keyspace: "ks".to_string(),
            table: "t".to_string(),
            key: DecoratedKey::new(PartitionKey::from(b"pk".as_slice())),
            rows: vec![Row {
                clustering: vec![],
                cells: vec![],
                deletion: DeletionTime::LIVE,
                primary_key_liveness: LivenessInfo::default(),
            }],
            timestamp: id as i64,
            accord_ts: None,
            mutation_id: [id; 16],
        }
    }

    #[test]
    fn streams_are_isolated() {
        let bus = CdcBus::new(8);
        let mut local = bus.subscribe(CdcStream::WrittenOnNode);
        let mut committed = bus.subscribe(CdcStream::CommittedToCluster);

        assert_eq!(bus.publish(event(CdcStream::WrittenOnNode, 1)), 1);

        let got = local.try_recv().expect("local subscriber sees the event");
        assert_eq!(got.mutation_id, [1; 16]);
        // The committed subscriber must not see a written-on-node event.
        assert_eq!(committed.try_recv(), Err(CdcRecvError::Empty));
    }

    #[test]
    fn fans_out_to_all_subscribers() {
        let bus = CdcBus::new(8);
        let mut a = bus.subscribe(CdcStream::CommittedToCluster);
        let mut b = bus.subscribe(CdcStream::CommittedToCluster);

        assert_eq!(bus.publish(event(CdcStream::CommittedToCluster, 7)), 2);

        assert_eq!(a.try_recv().unwrap().mutation_id, [7; 16]);
        assert_eq!(b.try_recv().unwrap().mutation_id, [7; 16]);
    }

    #[test]
    fn overflow_emits_gap_signal_not_silent_drop() {
        // Capacity 4, publish 6 without draining: the 2 oldest are dropped and
        // the subscriber is told via Lagged — never silently skipped (F16/F18).
        let bus = CdcBus::new(4);
        let mut sub = bus.subscribe(CdcStream::WrittenOnNode);
        for i in 0..6u8 {
            bus.publish(event(CdcStream::WrittenOnNode, i));
        }
        assert_eq!(sub.try_recv(), Err(CdcRecvError::Lagged { skipped: 2 }));
        // After observing the gap, delivery resumes from the oldest retained event.
        assert_eq!(sub.try_recv().unwrap().mutation_id, [2; 16]);
    }

    #[test]
    fn publish_without_subscribers_is_not_an_error() {
        let bus = CdcBus::new(4);
        assert_eq!(bus.publish(event(CdcStream::WrittenOnNode, 9)), 0);
    }

    #[test]
    fn has_subscribers_tracks_live_subscriptions_per_stream() {
        let bus = CdcBus::new(4);
        assert!(!bus.has_subscribers(CdcStream::WrittenOnNode));
        let sub = bus.subscribe(CdcStream::WrittenOnNode);
        assert!(bus.has_subscribers(CdcStream::WrittenOnNode));
        // Other stream is unaffected.
        assert!(!bus.has_subscribers(CdcStream::CommittedToCluster));
        drop(sub);
        assert!(!bus.has_subscribers(CdcStream::WrittenOnNode));
    }

    #[tokio::test]
    async fn async_recv_delivers_event() {
        let bus = CdcBus::new(4);
        let mut sub = bus.subscribe(CdcStream::CommittedToCluster);
        bus.publish(event(CdcStream::CommittedToCluster, 3));
        let got = sub.recv().await.expect("event delivered");
        assert_eq!(got.stream, CdcStream::CommittedToCluster);
        assert_eq!(got.mutation_id, [3; 16]);
    }

    #[test]
    fn closed_bus_reports_closed() {
        let bus = CdcBus::new(4);
        let mut sub = bus.subscribe(CdcStream::WrittenOnNode);
        drop(bus);
        assert_eq!(sub.try_recv(), Err(CdcRecvError::Closed));
    }

    #[test]
    #[should_panic(expected = "capacity must be non-zero")]
    fn zero_capacity_panics() {
        let _ = CdcBus::new(0);
    }
}
