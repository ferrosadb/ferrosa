//! Single-row wire encoding on the coordinator fan-out path.
//!
//! The coordinator encodes the row it is about to write locally; it must not
//! clone that row into a throwaway `Mutation` to do so. These tests pin the
//! three things that makes safe: the bytes are identical to the `Mutation`
//! encoder's, the row survives the round trip, and the encode allocates the
//! wire buffer and nothing else.
//!
//! One test owns the process allocator window, so this file holds exactly one.

use ferrosa_cluster::pair::coordinator::{
    decode_mutation, encode_mutation, encode_single_row_mutation,
};
use ferrosa_common::{CellValue, DecoratedKey, PartitionKey};
use ferrosa_sstable::types::{DeletionTime, LivenessInfo, Row};
use ferrosa_storage::Mutation;
use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicUsize, Ordering};

struct Counter;
static EVENTS: AtomicUsize = AtomicUsize::new(0);
static BYTES: AtomicUsize = AtomicUsize::new(0);

unsafe impl GlobalAlloc for Counter {
    unsafe fn alloc(&self, l: Layout) -> *mut u8 {
        EVENTS.fetch_add(1, Ordering::Relaxed);
        BYTES.fetch_add(l.size(), Ordering::Relaxed);
        unsafe { System.alloc(l) }
    }
    unsafe fn alloc_zeroed(&self, l: Layout) -> *mut u8 {
        EVENTS.fetch_add(1, Ordering::Relaxed);
        BYTES.fetch_add(l.size(), Ordering::Relaxed);
        unsafe { System.alloc_zeroed(l) }
    }
    unsafe fn realloc(&self, p: *mut u8, l: Layout, n: usize) -> *mut u8 {
        EVENTS.fetch_add(1, Ordering::Relaxed);
        BYTES.fetch_add(n, Ordering::Relaxed);
        unsafe { System.realloc(p, l, n) }
    }
    unsafe fn dealloc(&self, p: *mut u8, l: Layout) {
        unsafe { System.dealloc(p, l) }
    }
}

#[global_allocator]
static ALLOCATOR: Counter = Counter;

fn events() -> usize {
    EVENTS.load(Ordering::Relaxed)
}

fn bytes() -> usize {
    BYTES.load(Ordering::Relaxed)
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
