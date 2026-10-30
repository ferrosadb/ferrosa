//! A fragment must not clone a whole immutable memtable partition upfront.

#[path = "support/thread_alloc.rs"]
mod thread_alloc;

use std::sync::Arc;

use ferrosa_common::{CellValue, DecoratedKey, PartitionKey};
use ferrosa_sstable::types::{DeletionTime, LivenessInfo, Partition, Row};
use ferrosa_storage::range_merger::{MergeSource, RangeMerger};

fn source_partition(count: u32) -> Arc<Partition> {
    Arc::new(Partition {
        key: DecoratedKey::new(PartitionKey::from(&b"shared"[..])),
        deletion: DeletionTime::LIVE,
        static_row: None,
        rows: (0u32..count)
            .map(|key| Row {
                clustering: key.to_be_bytes().to_vec(),
                cells: vec![(0, CellValue::live(vec![b'x'; 32], 1_000_000))],
                deletion: DeletionTime::LIVE,
                primary_key_liveness: LivenessInfo::with_timestamp(1_000_000),
            })
            .collect(),
    })
}

#[test]
fn first_fragment_retains_shared_source_without_cloning_the_body() {
    let source = source_partition(20_000);
    let mut merger = RangeMerger::<Vec<u8>>::new(
        vec![MergeSource::Memtable {
            iter: Box::new(std::iter::once(Arc::clone(&source))),
            peeked: None,
        }],
        None,
        None,
    )
    .unwrap();
    let (first, bytes) =
        thread_alloc::allocated_bytes(|| merger.next_fragment(1).unwrap().unwrap());
    assert_eq!(first.rows.len(), 1);
    assert_eq!(first.rows[0].clustering, 0u32.to_be_bytes());
    assert!(first.first && !first.last);
    assert!(
        bytes < 64 * 1024,
        "one-row pull allocated {bytes} bytes from a shared 20,000-row source"
    );
    assert!(
        Arc::strong_count(&source) >= 2,
        "cursor must retain immutable source"
    );
    drop(merger);
    assert_eq!(Arc::strong_count(&source), 1);
    assert_eq!(source.rows.len(), 20_000);
    assert_eq!(
        source.rows.last().unwrap().clustering,
        19_999u32.to_be_bytes()
    );
}

#[test]
fn first_fragment_moves_unique_sorted_rows_without_sort_scratch() {
    let source = source_partition(20_000);
    let mut merger = RangeMerger::<Vec<u8>>::new(
        vec![MergeSource::Memtable {
            iter: Box::new(std::iter::once(source)),
            peeked: None,
        }],
        None,
        None,
    )
    .unwrap();
    let (first, bytes) =
        thread_alloc::allocated_bytes(|| merger.next_fragment(1).unwrap().unwrap());
    assert_eq!(first.rows.len(), 1);
    assert_eq!(first.rows[0].clustering, 0u32.to_be_bytes());
    assert!(
        bytes < 64 * 1024,
        "unique sorted source allocated {bytes} bytes"
    );
}

#[test]
fn shared_unsorted_rows_preserve_ordering_without_mutating_the_source() {
    let mut source = source_partition(4);
    Arc::get_mut(&mut source).unwrap().rows.reverse();
    let mut merger = RangeMerger::<Vec<u8>>::new(
        vec![MergeSource::Memtable {
            iter: Box::new(std::iter::once(Arc::clone(&source))),
            peeked: None,
        }],
        None,
        None,
    )
    .unwrap();
    let mut seen = Vec::new();
    while let Some(fragment) = merger.next_fragment(1).unwrap() {
        seen.extend(fragment.rows.into_iter().map(|row| row.clustering));
    }
    assert_eq!(
        seen,
        (0u32..4)
            .map(|key| key.to_be_bytes().to_vec())
            .collect::<Vec<_>>()
    );
    assert_eq!(source.rows[0].clustering, 3u32.to_be_bytes());
    assert_eq!(Arc::strong_count(&source), 1);
}
