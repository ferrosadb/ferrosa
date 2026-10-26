//! Single-row wire encoding on the coordinator fan-out path.
//!
//! The coordinator encodes the row it is about to write locally; it must not
//! clone that row into a throwaway `Mutation` to do so. These tests pin the
//! three things that makes safe: the bytes are identical to the `Mutation`
//! encoder's, the row survives the round trip, and the encode allocates the
//! wire buffer and nothing else.
//!
//! Allocation windows measure only the calling thread: the encoder is
//! synchronous, and test-runner/background threads are outside its budget.

use ferrosa_cluster::pair::coordinator::{
    decode_mutation, encode_mutation, encode_single_row_mutation,
};
use ferrosa_common::{CellValue, DecoratedKey, PartitionKey};
use ferrosa_sstable::types::{DeletionTime, LivenessInfo, Row};
use ferrosa_storage::Mutation;
use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;

struct Counter;
thread_local! {
    // Const initialization never allocates, including from the allocator itself.
    static COUNTS: Cell<(usize, usize)> = const { Cell::new((0, 0)) };
}

fn record_allocation(size: usize) {
    // Allocations during TLS teardown are outside any measurement window.
    let _ = COUNTS.try_with(|counts| {
        let (events, bytes) = counts.get();
        counts.set((events + 1, bytes + size));
    });
}

unsafe impl GlobalAlloc for Counter {
    unsafe fn alloc(&self, l: Layout) -> *mut u8 {
        record_allocation(l.size());
        unsafe { System.alloc(l) }
    }
    unsafe fn alloc_zeroed(&self, l: Layout) -> *mut u8 {
        record_allocation(l.size());
        unsafe { System.alloc_zeroed(l) }
    }
    unsafe fn realloc(&self, p: *mut u8, l: Layout, n: usize) -> *mut u8 {
        record_allocation(n);
        unsafe { System.realloc(p, l, n) }
    }
    unsafe fn dealloc(&self, p: *mut u8, l: Layout) {
        unsafe { System.dealloc(p, l) }
    }
}

#[global_allocator]
static ALLOCATOR: Counter = Counter;

fn events() -> usize {
    COUNTS.with(|counts| counts.get().0)
}

fn bytes() -> usize {
    COUNTS.with(|counts| counts.get().1)
}

fn row() -> Row {
    Row {
        clustering: 7i32.to_be_bytes().to_vec(),
        cells: vec![
            (
                0,
                CellValue::live(b"a-value-long-enough-to-need-its-own-buffer".to_vec(), 1000),
            ),
            (1, CellValue::live(b"second-cell".to_vec(), 1000)),
            (2, CellValue::live(b"third-cell".to_vec(), 1000)),
        ],
        deletion: DeletionTime::LIVE,
        primary_key_liveness: LivenessInfo::with_timestamp(1000),
    }
}

fn key() -> DecoratedKey {
    DecoratedKey::new(PartitionKey::new(b"partition-key".to_vec()))
}

#[test]
fn allocation_counter_excludes_other_threads() {
    use std::hint::black_box;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    // The handshake allocates nothing on this thread inside the window. Keep
    // the worker alive until after the snapshot to exclude its teardown.
    let phase = Arc::new(AtomicUsize::new(0));
    let worker_phase = Arc::clone(&phase);
    let worker = std::thread::spawn(move || {
        while worker_phase.load(Ordering::Acquire) == 0 {
            std::hint::spin_loop();
        }
        let buffer = black_box(vec![0u8; 4096]);
        worker_phase.store(2, Ordering::Release);
        while worker_phase.load(Ordering::Acquire) == 2 {
            std::hint::spin_loop();
        }
        black_box(buffer);
    });

    let before = (events(), bytes());
    phase.store(1, Ordering::Release);
    while phase.load(Ordering::Acquire) != 2 {
        std::hint::spin_loop();
    }
    let delta = (events() - before.0, bytes() - before.1);
    phase.store(3, Ordering::Release);
    worker.join().unwrap();
    assert_eq!(
        delta,
        (0, 0),
        "another thread contaminated the encoder budget"
    );
}

#[test]
fn single_row_encode_matches_the_mutation_encoder_and_copies_no_row() {
    let keyspace = "ks_encode";
    let table = "tbl_encode";
    let timestamp = 1_700_000_000_000_000i64;
    let key = key();
    let row = row();

    // I1: the borrowed encoder writes the same bytes as the owned-Mutation
    // encoder for the same fields. Only the 16-byte mutation id may differ.
    let owned = Mutation::new(
        keyspace.to_string(),
        table.to_string(),
        key.clone(),
        vec![row.clone()],
        timestamp,
    );
    let from_owned = encode_mutation(&owned);
    let borrowed = encode_single_row_mutation(keyspace, table, &key, &row, timestamp);
    assert_eq!(
        from_owned.len(),
        borrowed.len(),
        "borrowed encode changed the wire length"
    );
    assert_eq!(
        &from_owned[16..],
        &borrowed[16..],
        "borrowed encode diverges from the Mutation encoder outside the mutation id"
    );

    // I3: the row survives the wire exactly.
    let decoded = decode_mutation(&borrowed).expect("decode the encoded single-row mutation");
    assert_eq!(
        decoded.rows,
        vec![row.clone()],
        "row bytes changed on the wire"
    );
    assert_eq!(decoded.key, key, "partition key changed on the wire");
    assert_eq!(decoded.keyspace, keyspace);
    assert_eq!(decoded.table, table);
    assert_eq!(decoded.timestamp, timestamp);

    // The encode allocates the wire buffer and nothing else: no `Vec<Row>` for a
    // one-element batch, no copy of the row's clustering key or cells.
    let (events_before, bytes_before) = (events(), bytes());
    let encoded = encode_single_row_mutation(keyspace, table, &key, &row, timestamp);
    assert_eq!(
        events() - events_before,
        1,
        "single-row encode allocated {} buffers; the row is being copied again",
        events() - events_before
    );
    assert_eq!(
        bytes() - bytes_before,
        encoded.len(),
        "single-row encode allocated more than the wire buffer itself"
    );

    // I5: allocations are flat in row count — one buffer per row, and the same
    // per-row cost whether the write carries 1k rows or 10k.
    let mut per_row = Vec::new();
    for count in [1_000usize, 10_000] {
        let (events_before, bytes_before) = (events(), bytes());
        let mut encoded_len = 0usize;
        for _ in 0..count {
            encoded_len += encode_single_row_mutation(keyspace, table, &key, &row, timestamp).len();
        }
        assert_eq!(
            events() - events_before,
            count,
            "one allocation per row expected at {count} rows"
        );
        assert_eq!(
            bytes() - bytes_before,
            encoded_len,
            "allocation bytes differ from the wire bytes at {count} rows"
        );
        per_row.push(encoded_len / count);
    }
    assert_eq!(
        per_row[0], per_row[1],
        "per-row wire cost changed with batch size: {per_row:?}"
    );
}
