//! Background hint delivery task.
//!
//! When a peer recovers after a period of unavailability, [`HintDeliveryTask::run`]
//! drains that peer's hint files and replays each mutation as a
//! [`Message::MutationForward`] on the [`Lane::Data`] channel.
//!
//! # Protocol
//!
//! Hints were stored by the coordinator as the full encoded mutation bytes
//! (the same payload sent over the wire by `coordinate_write`), so they can be
//! forwarded directly without any re-encoding.
//!
//! # Failure handling
//!
//! If a send fails the task returns immediately, leaving the remaining hints
//! on disk.  The caller (typically [`ModeController::on_peer_recovered`]) is
//! responsible for re-spawning the task on the next recovery event.
//!
//! # Cleanup
//!
//! After all hints have been successfully replayed `cleanup()` is called to
//! remove the peer's hint directory.

use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use uuid::Uuid;

use ferrosa_net::codec::Lane;
use ferrosa_net::message::Message;
use ferrosa_net::peer::PeerManager;

use super::{HintConfig, HintStore};

// ---------------------------------------------------------------------------
// HintDeliveryTask
// ---------------------------------------------------------------------------

/// Stateless namespace for the hint delivery background loop.
pub struct HintDeliveryTask;

impl HintDeliveryTask {
    /// Replay all stored hints for `peer_id` and clean up on success.
    ///
    /// Drains the hint store in FIFO order, sending each hint as a
    /// `MutationForward` message.  On any send failure the loop returns
    /// immediately without calling `cleanup()` so hints remain on disk for
    /// the next attempt.
    ///
    /// This function is `async` but not long-running by itself; it exits as
    /// soon as all hints have been delivered (or on the first failure).
    /// Callers should spawn it with `tokio::spawn`.
    pub async fn run(
        peer_id: Uuid,
        hint_store: Arc<HintStore>,
        peer_manager: Arc<PeerManager>,
        config: &HintConfig,
    ) {
        let drain = match hint_store.drain(peer_id) {
            Ok(d) => d,
            Err(e) => {
                tracing::error!(%peer_id, "hint delivery: failed to open drain: {e}");
                return;
            }
        };

        // Collect all hints upfront so we don't hold a lock during async sends.
        // For very large hint stores this could be memory-intensive; a streaming
        // approach is left for a future optimisation (iterator-based chunked send).
        let mut iter = drain.peekable();
        let mut delivered = 0usize;

        loop {
            // Collect one batch.
            let mut batch = Vec::with_capacity(config.delivery_batch_size);
            for hint in iter.by_ref().take(config.delivery_batch_size) {
                batch.push(hint);
            }

            if batch.is_empty() {
                break;
            }

            // Send each hint in the batch.
            for hint in &batch {
                // The `row` field stores the full encoded mutation bytes written
                // by `coordinate_write` via `encode_mutation`.  Send them
                // verbatim as a `MutationForward` — the receiver already has a
                // `MutationForwardHandler` that decodes and applies the mutation.
                let body = Bytes::from(hint.row.clone());
                match peer_manager
                    .send(peer_id, Message::MutationForward(body), Lane::Data)
                    .await
                {
                    Ok(Message::MutationAck(_)) => {
                        delivered += 1;
                    }
                    Ok(other) => {
                        tracing::warn!(
                            %peer_id,
                            delivered,
                            "hint delivery: unexpected response {:?}, stopping",
                            other.msg_type()
                        );
                        return;
                    }
                    Err(e) => {
                        tracing::warn!(
                            %peer_id,
                            delivered,
                            "hint delivery: send failed: {e}, stopping"
                        );
                        return;
                    }
                }
            }

            // Pause between batches to avoid overwhelming the recovering peer.
            tokio::time::sleep(Duration::from_millis(config.delivery_interval_ms)).await;
        }

        tracing::info!(%peer_id, delivered, "hint delivery: all hints replayed");
        hint_store.cleanup(peer_id);
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::hints::{HintConfig, HintStore};
    use tempfile::TempDir;

    fn make_store(dir: &TempDir) -> Arc<HintStore> {
        let config = HintConfig {
            dir: dir.path().to_path_buf(),
            delivery_batch_size: 16,
            delivery_interval_ms: 0, // no sleep in tests
            ..HintConfig::default()
        };
        Arc::new(HintStore::new(config).unwrap())
    }

    fn store_hints(store: &HintStore, peer: Uuid, count: usize) {
        // Use a short, fixed-size row so the test is fast.
        let row = b"encoded_mutation_bytes".to_vec();
        for i in 0..count {
            store
                .store(
                    peer,
                    "ks",
                    "tbl",
                    format!("key_{i}").into_bytes(),
                    row.clone(),
                    i as i64,
                )
                .unwrap();
        }
    }

    // -----------------------------------------------------------------------
    // drain-and-cleanup smoke test (no real PeerManager)
    // -----------------------------------------------------------------------

    /// Verifies that the drain + cleanup path works correctly at the
    /// HintStore level — the foundation that HintDeliveryTask relies on.
    ///
    /// Full end-to-end delivery (with a real or mock PeerManager completing
    /// sends) is validated in docker smoke tests where two ferrosa nodes are
    /// running, because `PeerManager` is a concrete type without a trait
    /// abstraction for send injection.
    #[test]
    fn drain_yields_all_stored_hints() {
        let dir = TempDir::new().unwrap();
        let peer = Uuid::new_v4();
        let store = make_store(&dir);

        const N: usize = 100;
        store_hints(&store, peer, N);

        assert_eq!(store.pending_count(peer), N);

        let drain = store.drain(peer).unwrap();
        let records: Vec<_> = drain.collect();
        assert_eq!(records.len(), N, "drain must yield all {N} hints");

        // Timestamps should be in FIFO order.
        for (i, rec) in records.iter().enumerate() {
            assert_eq!(rec.timestamp, i as i64, "hint {i} out of order");
        }
    }

    #[test]
    fn cleanup_resets_pending_count() {
        let dir = TempDir::new().unwrap();
        let peer = Uuid::new_v4();
        let store = make_store(&dir);

        store_hints(&store, peer, 10);
        assert_eq!(store.pending_count(peer), 10);

        // Drain (simulates delivery consuming the hints).
        let drain = store.drain(peer).unwrap();
        let _consumed: Vec<_> = drain.collect();

        store.cleanup(peer);

        // After cleanup the in-memory record count is reset.
        assert_eq!(store.pending_count(peer), 0);
    }

    /// Verifies partial-drain semantics: if we only consume part of the
    /// stream and then return (simulating a peer failure mid-delivery),
    /// the remaining hints survive on disk.
    ///
    /// This mirrors the `delivery_pauses_on_peer_failure` contract: the
    /// task returns without calling `cleanup()`, leaving unconsumed hints.
    #[test]
    fn partial_drain_leaves_hints_on_disk() {
        let dir = TempDir::new().unwrap();
        let peer = Uuid::new_v4();
        let store = make_store(&dir);

        const N: usize = 50;
        store_hints(&store, peer, N);

        // Consume only half.
        {
            let drain = store.drain(peer).unwrap();
            let _half: Vec<_> = drain.take(N / 2).collect();
            // drain is dropped here; no cleanup called.
        }

        // A second drain picks up only the remaining hints that were not
        // consumed by the first partial drain.
        // Note: because segment files are not deleted mid-drain, the second
        // drain re-reads from the beginning of each segment.  This is
        // intentional — it guarantees at-least-once delivery semantics.
        let drain2 = store.drain(peer).unwrap();
        let remaining: Vec<_> = drain2.collect();
        assert!(
            !remaining.is_empty(),
            "hints must survive a partial drain (at-least-once delivery)"
        );
    }
}
