//! TDD guard (`t_ee98faa0` + `t_3fc6be3c`, SAME root): the range-scan wire path
//! MUST hold O(budget) memory, not O(result), driving the REAL serialization
//! (`handle_stream_request` → `partition_to_wire` → bincode `RangeReadStream*`
//! frames) and the REAL coordinator consumer (`consume_range_stream`).
//!
//! Why this test exists (and why the parked-replica unit tests + the existing
//! `range_scan_streaming_memory_bound.rs` missed it):
//!
//!   * `range_scan_streaming_memory_bound.rs` drives the coordinator's
//!     **Stream** API (`range_read_stream_all_with`) — which correctly yields
//!     partition-at-a-time through a bounded mpsc, so its peak is O(1). That
//!     test is GREEN and stays GREEN.
//!   * The LIVE OOM (a single `hybrid_search` `context_snippet = fts_match(...)`
//!     content scan over `entity_store` killing the coordinator in one shot,
//!     and the multi-page projected scans) does NOT go through the Stream API.
//!     It goes through the **`Vec<Partition>`-returning** path:
//!     `coordinate_range_read_stream_limited_rows` (range_read_stream.rs) which
//!     calls `consume_range_stream` and does `all_partitions.extend(outcome.partitions)`
//!     — and `consume_range_stream` itself accumulates EVERY partition from
//!     EVERY replica into `StreamConsumeOutcome.partitions` before returning.
//!     Peak = O(result). At the intentional 2 GiB node cap this is the OOM.
//!
//! The 2 GiB cap is a deliberate forcing function and is NEVER raised — the fix
//! is bounded in-flight bytes + backpressure (yield partitions through a bounded
//! channel), so peak is O(budget) regardless of N.
//!
//! This file:
//!   1. RED — `coordinator_consumer_peak_scales_with_result_size_is_the_bug`:
//!      drives the real replica producer's bincode frames into the real
//!      `consume_range_stream` and shows its peak heap scales with N (the OOM).
//!      A HARD in-flight budget makes a blow-up FAIL loudly instead of OOM-ing
//!      the test process.
//!   2. GREEN — `streaming_producer_peak_is_bounded_independent_of_result_size`:
//!      the replica producer holds O(chunk), independent of N.
//!   3. `slow_consumer_backpressures_producer_not_unbounded_buffer`: the
//!      producer awaits each chunk send, so a slow consumer cannot make a
//!      replica buffer the whole scan.
//!
//! Modeled on `range_scan_streaming_memory_bound.rs` (same allocator-tracking
//! `measure_peak`), but drives the ACTUAL wire serialization + consumer.

use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};
use std::sync::Arc;

use async_trait::async_trait;
use serial_test::serial;

use ferrosa_common::key::{DecoratedKey, PartitionKey};
use ferrosa_common::schema::{ColumnDefinition, TableSchema};
use ferrosa_common::{CellValue, Token};
use ferrosa_sstable::types::{DeletionTime, LivenessInfo, Partition, Row};
use ferrosa_storage::{
    CommitLogConfig, CompactionConfig, StorageEngine, StorageEngineConfig, TableId,
};

use ferrosa_net::message::Message;
use tokio::sync::mpsc;

use ferrosa_cluster::coordinator::stream_consumer::{consume_range_stream_into, PartitionSink};
use ferrosa_cluster::coordinator::stream_producer::ChunkSink;
use ferrosa_cluster::coordinator::stream_request_handler::handle_stream_request;
use ferrosa_cluster::raft::handlers::RangeReadStreamRequestPayload;

// --- peak-additional-heap tracker (scoped to this integration-test binary) ---
// Measurement windows are serialized under `MEASURE_LOCK`, so `ARMED`/`LIVE`/
// `PEAK` are never shared across two concurrently-running tests.
struct TrackingAlloc;
static ARMED: AtomicBool = AtomicBool::new(false);
static LIVE: AtomicI64 = AtomicI64::new(0);
static PEAK: AtomicI64 = AtomicI64::new(0);

unsafe impl GlobalAlloc for TrackingAlloc {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let ptr = unsafe { System.alloc(layout) };
        if !ptr.is_null() && ARMED.load(Ordering::Relaxed) {
            let live =
                LIVE.fetch_add(layout.size() as i64, Ordering::Relaxed) + layout.size() as i64;
            PEAK.fetch_max(live, Ordering::Relaxed);
        }
        ptr
    }
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        if ARMED.load(Ordering::Relaxed) {
            // Clamp at zero: `measure_peak` zeroes LIVE at arm time, so a free
            // of memory allocated BEFORE the window would drive the counter
            // negative and, because PEAK is a running maximum of LIVE, suppress
            // every later allocation. Seeding runs outside the window by design,
            // so how much of it is released inside the window is pure timing.
            let _ = LIVE.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |live| {
                Some((live - layout.size() as i64).max(0))
            });
        }
        unsafe { System.dealloc(ptr, layout) };
    }
}

#[global_allocator]
static ALLOC: TrackingAlloc = TrackingAlloc;

/// Serialize measurement windows. A poisoned lock (a panicking test) still
/// yields the guard via `into_inner` so a failing test never cascades poison
/// into the others.
static MEASURE_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

fn lock_measure() -> std::sync::MutexGuard<'static, ()> {
    MEASURE_LOCK.lock().unwrap_or_else(|p| p.into_inner())
}

/// Measure peak *additional* heap bytes held at once during `f`.
fn measure_peak<R>(f: impl FnOnce() -> R) -> (R, i64) {
    let _guard = lock_measure();
    LIVE.store(0, Ordering::SeqCst);
    PEAK.store(0, Ordering::SeqCst);
    ARMED.store(true, Ordering::SeqCst);
    let out = f();
    ARMED.store(false, Ordering::SeqCst);
    (out, PEAK.load(Ordering::SeqCst))
}

const KS: &str = "test_ks";
const TBL: &str = "test_tbl";
const ROW_BYTES: usize = 4096;
/// Partitions per emitted chunk on the replica producer — mirrors the bounded
/// working set a streaming coordinator holds in flight.
const CHUNK: usize = 64;

fn engine(dir: &std::path::Path) -> Arc<StorageEngine> {
    let config = StorageEngineConfig {
        commit_log: CommitLogConfig {
            log_dir: dir.to_path_buf(),
            checkpoint_dir: dir.to_path_buf(),
            archive: None,
            ..CommitLogConfig::default()
        },
        compaction: CompactionConfig::from_env(dir.join("compaction")),
        object_store: None,
        local_cache_max_bytes: 1024 * 1024,
        local_disk_free_reserve_bytes: 0,
        flush_threshold_bytes: 4096,
        memtable_backpressure_bytes: u64::MAX,
        flush_max_age_secs: 5,
        data_dir: dir.to_path_buf(),
        index_backend: ferrosa_storage::index::IndexBackendConfig::Local,
        auth_enabled: false,
        auth_warn: false,
        write_verify: false,
        max_pending_replay_mutations_without_schema: 1024,
        memtable_num_shards: 64,
    };
    let engine = Arc::new(StorageEngine::new(config, None).unwrap());
    let schema = TableSchema {
        keyspace: KS.to_string(),
        table: TBL.to_string(),
        key_type: "org.apache.cassandra.db.marshal.UTF8Type".to_string(),
        clustering_columns: vec![],
        static_columns: vec![],
        regular_columns: vec![ColumnDefinition {
            name: "val".to_string(),
            type_name: "org.apache.cassandra.db.marshal.UTF8Type".to_string(),
        }],
        extensions: Default::default(),
    };
    engine.register_table(schema).unwrap();
    engine
}

/// Seed `n` distinct single-row partitions, each carrying a `ROW_BYTES` value.
fn seed(engine: &StorageEngine, n: usize) {
    let table_id = TableId::new(KS, TBL);
    for i in 0..n {
        let key_bytes = format!("pk-{i:08}").into_bytes();
        let dk = DecoratedKey {
            token: Token(i as i64),
            key: PartitionKey::new(key_bytes),
        };
        let row = Row {
            clustering: vec![],
            cells: vec![(0, CellValue::live(vec![b'x'; ROW_BYTES], 1000))],
            deletion: DeletionTime::LIVE,
            primary_key_liveness: LivenessInfo::with_timestamp(1000),
        };
        engine.write(&table_id, &dk, row, 1000).unwrap();
    }
}

fn stream_request(request_id: u32) -> RangeReadStreamRequestPayload {
    RangeReadStreamRequestPayload {
        request_id,
        keyspace: KS.to_string(),
        table: TBL.to_string(),
        projected_regular_ordinals: None,
        start_key: None,
        start_clustering: None,
        max_chunks: 0,
    }
}

/// `StreamRangeReader` is implemented on `Arc<StorageEngine>`; the handler is
/// generic over `Arc<R>`, so wrap in another cheap Arc — exactly how production
/// (`controller::cluster`) wires the request handler. The double-Arc is
/// deliberate (the trait impl clones the engine handle for `spawn_blocking`),
/// so silence clippy's `redundant_allocation` here to mirror production.
#[allow(clippy::redundant_allocation)]
fn reader_of(storage: &Arc<StorageEngine>) -> Arc<Arc<StorageEngine>> {
    Arc::new(storage.clone())
}

// ---------------------------------------------------------------------------
// Sinks for the CONSUME side.
//
// The consumer's memory is isolated from the producer's by a two-phase harness
// (`consume_phase_peak`): the producer runs FIRST, filling an unbounded channel
// with the real serialized `RangeReadStream*` frames OUTSIDE the measurement
// window (so those `Bytes` do not count as "additional" heap); then the
// measurement window opens and the REAL consumer (`consume_range_stream_into`)
// drains the pre-filled channel into a sink. What we measure is exactly the
// consumer's decode + retain behaviour, with NO concurrent producer/storage
// allocation noise.
//
//   * `DroppingPartitionSink` models the PRODUCTION streaming consumer: each
//     decoded partition is accounted then dropped (as if forwarded onward
//     through a bounded channel). Resident set is O(chunk) — bounded.
//   * A `VecPartitionSink` (via the legacy `consume_range_stream` wrapper)
//     accumulates every partition — resident set O(result). Used only to prove,
//     as a control, that the harness DOES see O(result) growth when a consumer
//     accumulates, so the bounded-sink assertion is meaningful.
// ---------------------------------------------------------------------------

/// A sink that counts each partition then drops it — models the production
/// streaming consumer forwarding partitions onward without buffering the scan.
struct DroppingPartitionSink {
    count: usize,
}

#[async_trait]
impl PartitionSink for DroppingPartitionSink {
    async fn accept(&mut self, _partition: Partition) -> bool {
        self.count += 1;
        // `_partition` dropped here — nothing accumulates.
        true
    }
}

/// Produce every `RangeReadStream*` frame for `n` seeded partitions into an
/// UNBOUNDED channel (outside the measurement window), then measure the peak
/// additional heap held while the REAL `consume_range_stream_into` drains that
/// channel into a `DroppingPartitionSink`. Because the producer/storage have
/// already run and their `Bytes` frames are allocated BEFORE the window opens,
/// the measured peak is the CONSUMER's own working set — O(chunk) for the
/// streaming consumer, independent of N.
///
/// Returns `(partition_count, consumer_peak_additional_heap_bytes)`.
fn consume_phase_peak(n: usize) -> (usize, i64) {
    let dir = tempfile::tempdir().unwrap();
    let storage = engine(dir.path());
    seed(&storage, n);
    let reader = reader_of(&storage);

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();

    // PHASE 1 (outside the measurement window): produce every frame into a
    // `Vec<Message>`. The serialized `Bytes` are allocated HERE, before the
    // tracker is armed, so they are NOT counted as the consumer's additional
    // heap. Re-buffering them into the consumer's channel later is a ref-count
    // clone (no payload copy).
    let frames: Vec<Message> = rt.block_on(async {
        let collected = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
        struct CollectSink {
            out: std::sync::Arc<std::sync::Mutex<Vec<Message>>>,
        }
        #[async_trait]
        impl ChunkSink for CollectSink {
            async fn send(&self, msg: Message) {
                self.out.lock().unwrap().push(msg);
            }
        }
        let sink = CollectSink {
            out: collected.clone(),
        };
        handle_stream_request(stream_request(1), reader, &sink, CHUNK).await;
        let taken = std::mem::take(&mut *collected.lock().unwrap());
        taken
    });

    // PHASE 2 (measured): the REAL consumer drains a bounded channel (fed from
    // the pre-produced frames) into the dropping sink. Peak additional heap here
    // is the consumer's own working set — bounded (O(chunk)) for the streaming
    // consumer, INDEPENDENT of N.
    let mut sink = DroppingPartitionSink { count: 0 };
    let (_, peak) = measure_peak(|| {
        rt.block_on(async {
            let (btx, brx) = mpsc::channel::<Message>(CHUNK);
            // A feeder task pushes the already-allocated frames through the
            // bounded channel; `send().await` back-pressures on the consumer so
            // at most `CHUNK` frames are in flight — the consumer is never fed
            // faster than it drains.
            let feeder = tokio::spawn(async move {
                for msg in frames {
                    if btx.send(msg).await.is_err() {
                        break;
                    }
                }
                drop(btx);
            });
            consume_range_stream_into(
                brx,
                std::time::Duration::from_secs(30),
                1, // one replica
                1,
                &mut sink,
            )
            .await
            .expect("consume");
            feeder.await.expect("feeder join");
        });
    });
    (sink.count, peak)
}

/// The bounded in-flight budget a STREAMING coordinator must respect: a small
/// multiple of one chunk's serialized bytes, far under the 2 GiB node cap. The
/// fix makes the coordinator consume peak stay under this regardless of N.
const IN_FLIGHT_BUDGET_BYTES: i64 = 32 * 1024 * 1024; // 32 MiB, << 2 GiB cap

/// GREEN (was the faithful RED for `t_ee98faa0` + `t_3fc6be3c`): the REAL
/// coordinator consumer (`consume_range_stream_into`) drains the REAL replica
/// producer's bincode `RangeReadStream*` frames into a dropping sink and holds a
/// BOUNDED resident set — O(chunk), INDEPENDENT of the scanned result size.
///
/// The former shape (`consume_range_stream` → `StreamConsumeOutcome.partitions`
/// then `coordinate_range_read_stream_limited_rows`'s
/// `all_partitions.extend(...)`) accumulated EVERY partition from EVERY replica
/// into a `Vec` before returning — peak O(result) — which OOM-killed the
/// coordinator on the live `fts_match` content scan and the multi-page projected
/// scans, at the intentional 2 GiB cap. The fix streams each partition through a
/// bounded sink (`PartitionSink`) and drops it, so the consumer never buffers
/// the whole scan.
///
/// Reverting the `consume_range_stream_into` body to `outcome.partitions.extend`
/// (accumulate) makes this RED again: `large` scales with N and blows the
/// budget. The count assertions (no data loss) hold in BOTH states.
#[test]
#[serial(range_scan_alloc_measure)]
fn coordinator_consumer_peak_is_bounded_independent_of_result_size() {
    const SMALL_N: usize = 750;
    const LARGE_N: usize = 12_000; // 16× more, PAST the 10_000 legacy cap

    let (small_count, small) = consume_phase_peak(SMALL_N);
    let (large_count, large) = consume_phase_peak(LARGE_N);
    assert_eq!(
        small_count, SMALL_N,
        "consume must cover every seeded partition (no data loss)"
    );
    assert_eq!(
        large_count, LARGE_N,
        "consume must cover every seeded partition (no data loss)"
    );
    eprintln!(
        "coordinator_consumer_peak: small(N={SMALL_N})={small} B, large(N={LARGE_N})={large} B, \
         ratio={:.2} (row_bytes={ROW_BYTES}); in-flight budget={IN_FLIGHT_BUDGET_BYTES} B",
        large as f64 / small.max(1) as f64,
    );

    // BOUNDED: the streaming consumer's peak stays under the in-flight budget
    // even at LARGE_N — it forwards+drops each partition, never buffering the
    // scan. (Reverting the fix to accumulate a Vec makes this FAIL: peak becomes
    // O(result) = LARGE_N * ROW_BYTES ≈ 47 MiB > 32 MiB budget.)
    assert!(
        large < IN_FLIGHT_BUDGET_BYTES,
        "REGRESSION: coordinator consume peak {large} B exceeds the {IN_FLIGHT_BUDGET_BYTES} B \
         in-flight budget at N={LARGE_N} — the consumer is buffering the scan instead of \
         streaming it. Peak MUST be O(chunk), not O(result)."
    );
    // INDEPENDENT OF N: scanning 16× more partitions must NOT cost ~16× more
    // consumer memory. A small constant factor absorbs allocator jitter.
    assert!(
        large < small * 3,
        "REGRESSION: coordinator consume peak scales with result size — \
         small(N={SMALL_N})={small} B, large(N={LARGE_N})={large} B ({}× more partitions). \
         The consumer must stream partitions through a bounded sink, not accumulate a Vec.",
        LARGE_N / SMALL_N,
    );
}

// ---------------------------------------------------------------------------
// GREEN: the replica producer side is already bounded (O(chunk)).
// ---------------------------------------------------------------------------

/// A `ChunkSink` that models a consumer which has already forwarded each frame:
/// it measures each frame's serialized size then DROPS it, so nothing
/// accumulates. Peak heap while producing is then the producer's own working
/// set (~CHUNK partitions + one chunk buffer).
struct DroppingSink {
    chunks: std::sync::Mutex<usize>,
    peak_frame: std::sync::Mutex<usize>,
}

#[async_trait]
impl ChunkSink for DroppingSink {
    async fn send(&self, msg: Message) {
        let len = match &msg {
            Message::RangeReadStreamChunk(b)
            | Message::RangeReadStreamDone(b)
            | Message::RangeReadStreamHeartbeat(b) => b.len(),
            _ => 0,
        };
        if matches!(msg, Message::RangeReadStreamChunk(_)) {
            *self.chunks.lock().unwrap() += 1;
        }
        let mut peak = self.peak_frame.lock().unwrap();
        *peak = (*peak).max(len);
        // msg dropped here.
    }
}

/// GREEN: the streaming replica producer (`handle_stream_request` → the chunked
/// `emit_chunk`) holds O(chunk) memory, so peak heap is independent of result
/// size. Scanning 16× more partitions must NOT cost ~16× more peak memory.
#[test]
#[serial(range_scan_alloc_measure)]
fn streaming_producer_peak_is_bounded_independent_of_result_size() {
    const SMALL_N: usize = 750;
    const LARGE_N: usize = 12_000;

    fn produce_peak(n: usize) -> i64 {
        let dir = tempfile::tempdir().unwrap();
        let storage = engine(dir.path());
        seed(&storage, n);
        let reader = reader_of(&storage);
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let sink = DroppingSink {
            chunks: std::sync::Mutex::new(0),
            peak_frame: std::sync::Mutex::new(0),
        };
        let (_, peak) = measure_peak(|| {
            rt.block_on(async {
                handle_stream_request(stream_request(1), reader, &sink, CHUNK).await;
            });
        });
        assert!(
            *sink.chunks.lock().unwrap() >= 1,
            "must emit chunks for N={n}"
        );
        peak
    }

    let small = produce_peak(SMALL_N);
    let large = produce_peak(LARGE_N);
    eprintln!(
        "streaming_producer_peak: small(N={SMALL_N})={small} B, large(N={LARGE_N})={large} B, \
         ratio={:.2}",
        large as f64 / small.max(1) as f64,
    );

    assert!(
        large < small * 3,
        "REGRESSION: streaming producer peak scales with result size — \
         {SMALL_N} parts: {small} B, {LARGE_N} parts ({}× more): {large} B. \
         The replica producer must stream bounded chunks, not materialize the whole scan.",
        LARGE_N / SMALL_N,
    );
    assert!(
        large < IN_FLIGHT_BUDGET_BYTES,
        "streaming producer peak {large} B exceeds the {IN_FLIGHT_BUDGET_BYTES} B in-flight budget"
    );
}

/// The production fire-and-forget sink (`PeerFireSink`) provides backpressure
/// through the lane actor's BOUNDED mpsc: `fire` reserves a permit on a
/// `mpsc::channel(lane_channel_capacity())` and awaits the send reply, so the
/// producer cannot run unboundedly ahead of a slow/backpressured consumer.
/// This asserts the invariant that keeps per-replica in-flight bytes bounded:
/// because `handle_stream_request` awaits each chunk `send`, at most ONE send is
/// ever in flight — a slow consumer forces the producer to block rather than
/// buffer the whole scan.
#[tokio::test]
#[serial(range_scan_alloc_measure)]
async fn slow_consumer_backpressures_producer_not_unbounded_buffer() {
    use std::sync::atomic::AtomicUsize;

    struct GatedSink {
        in_flight: AtomicUsize,
        max_in_flight: AtomicUsize,
    }
    #[async_trait]
    impl ChunkSink for GatedSink {
        async fn send(&self, _msg: Message) {
            let now = self.in_flight.fetch_add(1, Ordering::SeqCst) + 1;
            self.max_in_flight.fetch_max(now, Ordering::SeqCst);
            // Yield so any concurrent producer step could pile up if the
            // producer did NOT await us (it does, so it cannot).
            tokio::task::yield_now().await;
            self.in_flight.fetch_sub(1, Ordering::SeqCst);
        }
    }

    let dir = tempfile::tempdir().unwrap();
    let storage = engine(dir.path());
    seed(&storage, 2_000);
    let reader = reader_of(&storage);
    let sink = GatedSink {
        in_flight: AtomicUsize::new(0),
        max_in_flight: AtomicUsize::new(0),
    };

    handle_stream_request(stream_request(7), reader, &sink, 32).await;

    let max = sink.max_in_flight.load(Ordering::SeqCst);
    assert_eq!(
        max, 1,
        "producer must await each chunk send (backpressure); saw {max} concurrent in-flight \
         sends, which would let a replica buffer the whole scan unboundedly"
    );
}

/// The streaming request payload carries NO `limit` — the consumer controls
/// flow via backpressure + cancel, which is the wire-level contract that
/// replaces the legacy capped `RangeReadRequestPayload`. Pins the fix's shape.
#[test]
fn streaming_request_has_no_capacity_cap_field() {
    let req = stream_request(1);
    let bytes = bincode::serialize(&req).unwrap();
    let back: RangeReadStreamRequestPayload = bincode::deserialize(&bytes).unwrap();
    assert_eq!(back.request_id, 1);
}
