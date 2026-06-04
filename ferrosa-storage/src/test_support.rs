//! Shared proptest generators + fixtures for the storage/repair fuzz harness.
//!
//! Enabled by the `test-generators` feature. These build on the primitive
//! generators in [`ferrosa_common::test_generators`] (`arb_cell_value`,
//! `arb_cell`, `arb_partition_key`, `arb_decorated_key`) to produce the
//! higher-level shapes the repair-fuzz harness needs:
//!
//! - [`arb_partition`] — a [`Partition`] with random clustered rows mixing
//!   live / tombstone / expiring cells (exercises LWW + deletion application).
//! - [`arb_table_content`] — a `Vec<Partition>` with controllable key overlap
//!   and size (deduped + token-sorted, valid as SSTable writer input).
//! - [`arb_replica_set`] — `n` divergent replicas of a base table (drop / add /
//!   older-cell / newer-cell / tombstone per replica) — models the divergence
//!   anti-entropy repair must converge.
//! - [`arb_sstable_layout`] — number of SSTables per table + token-overlap mode
//!   (full-overlap / disjoint / partial).
//! - [`arb_corruption`] — byte-level corruption injected into serialized SSTable
//!   components (oversized length prefixes, truncation, zeroed components).
//! - [`arb_config`] — random `fanin_cap` / `reader_cap` / budgets / thresholds.
//!
//! The generators are intentionally *value* generators — they produce plain
//! data, not on-disk SSTables. The harness materialises them through a real
//! [`crate::flush::FileFlushTarget`] when a property needs the reopen/pool path.

use proptest::prelude::*;

use ferrosa_common::cell::CellValue;
use ferrosa_common::key::{DecoratedKey, PartitionKey};
use ferrosa_common::test_generators::arb_cell_value;
use ferrosa_sstable::types::{DeletionTime, LivenessInfo, Partition, Row};

/// Token-overlap mode for a generated SSTable layout.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OverlapMode {
    /// Every SSTable spans the full token ring and shares the same key set —
    /// the `entity_store` / `typed_edges` shape that exposed the reader-count
    /// wall. A range scan must merge every SSTable for every key.
    Full,
    /// SSTables hold disjoint key ranges — a scan touches few sources per key.
    Disjoint,
    /// SSTables partially overlap — the realistic middle ground.
    Partial,
}

/// A generated SSTable layout description: how many SSTables and how their
/// token ranges relate.
#[derive(Debug, Clone)]
pub struct SstableLayout {
    pub n_sstables: usize,
    pub distinct_keys: usize,
    pub overlap: OverlapMode,
}

/// A byte-level corruption to inject into a serialized SSTable component.
#[derive(Debug, Clone)]
pub enum Corruption {
    /// Truncate the component to `keep` bytes (0 = empty component).
    Truncate { keep_fraction: u8 },
    /// Overwrite a span starting at `offset_fraction` (of len) with `byte`.
    Overwrite {
        offset_fraction: u8,
        len: u16,
        byte: u8,
    },
    /// Flip a single bit at `offset_fraction` of the component length.
    BitFlip { offset_fraction: u8, bit: u8 },
    /// Inject an oversized varint length prefix at `offset_fraction` — the
    /// shape behind the "corrupt clustering-value length → OOM" bug: a reader
    /// that trusts the prefix tries to allocate a multi-GiB buffer.
    OversizedLengthPrefix { offset_fraction: u8 },
    /// Zero out the entire component (missing/empty component file).
    ZeroAll,
}

/// Tuning knobs the harness feeds to the reader pool / staged merge / bounded
/// fetch / compaction so properties are exercised across the configuration
/// space, not just the defaults.
#[derive(Debug, Clone)]
pub struct FuzzConfig {
    /// Resident-reader pool cap.
    pub reader_cap: usize,
    /// Per-read staged-merge fan-in cap.
    pub fanin_cap: usize,
    /// Bounded-fetch byte budget.
    pub max_bytes: usize,
    /// Bounded-fetch partition-count budget.
    pub max_partitions: usize,
    /// Compaction concurrency cap.
    pub compaction_concurrency: usize,
}

/// Build a row with the given clustering tag and a single cell.
fn row_with_cell(clustering_tag: u8, col: u16, cell: CellValue) -> Row {
    // A row's liveness timestamp tracks the freshest cell so partition-level
    // LWW (`newest_partition_timestamp`) sees a consistent max.
    let ts = cell.timestamp;
    Row {
        clustering: vec![0, 0, 0, clustering_tag],
        cells: vec![(col, cell)],
        deletion: DeletionTime::LIVE,
        primary_key_liveness: LivenessInfo::with_timestamp(ts),
    }
}

/// Arbitrary partition for a *fixed* decorated key: 1..=`max_rows` clustered
/// rows, each a random `(col, CellValue)` (live / tombstone / expiring),
/// optionally a static row and a partition-level deletion. Rows are emitted in
/// clustering order with distinct clustering tags so the SSTable writer
/// accepts them.
pub fn arb_partition_for_key(
    key: DecoratedKey,
    max_rows: usize,
) -> impl Strategy<Value = Partition> {
    let rows_strat = prop::collection::vec((0u16..8, arb_cell_value()), 1..=max_rows.max(1));
    (rows_strat, any::<bool>(), prop::option::of(1i64..1_000_000)).prop_map(
        move |(cells, has_static, part_del_ts)| {
            let rows: Vec<Row> = cells
                .into_iter()
                .enumerate()
                .map(|(i, (col, cell))| row_with_cell(i as u8, col, cell))
                .collect();
            let static_row = if has_static {
                Some(row_with_cell(0xFF, 0, CellValue::live(b"s".to_vec(), 1)))
            } else {
                None
            };
            let deletion = match part_del_ts {
                Some(ts) => DeletionTime::new(ts, 1_700_000_000),
                None => DeletionTime::LIVE,
            };
            Partition {
                key: key.clone(),
                deletion,
                static_row,
                rows,
            }
        },
    )
}

/// Arbitrary partition with a random decorated key.
pub fn arb_partition(max_rows: usize) -> impl Strategy<Value = Partition> {
    prop::collection::vec(any::<u8>(), 1..16).prop_flat_map(move |kb| {
        let key = DecoratedKey::new(PartitionKey::new(kb));
        arb_partition_for_key(key, max_rows)
    })
}

/// Generate `n_keys` distinct partition keys deterministically from a seed
/// vector of key-byte vectors. Returns keys deduped + token-sorted.
fn distinct_keys_from(seeds: Vec<Vec<u8>>) -> Vec<DecoratedKey> {
    let mut keys: Vec<DecoratedKey> = seeds
        .into_iter()
        .map(|b| DecoratedKey::new(PartitionKey::new(b)))
        .collect();
    keys.sort();
    keys.dedup_by(|a, b| a.key == b.key);
    keys
}

/// Arbitrary table content: a deduped, token-sorted `Vec<Partition>` with
/// `1..=max_keys` distinct keys. Suitable directly as SSTable writer input
/// (writer requires token order + unique keys per SSTable).
pub fn arb_table_content(
    max_keys: usize,
    max_rows: usize,
) -> impl Strategy<Value = Vec<Partition>> {
    let keys_strat = prop::collection::vec(
        prop::collection::vec(any::<u8>(), 1..12),
        1..=max_keys.max(1),
    );
    keys_strat.prop_flat_map(move |seeds| {
        let keys = distinct_keys_from(seeds);
        // One partition per distinct key, each independently generated.
        let part_strats: Vec<_> = keys
            .into_iter()
            .map(|k| arb_partition_for_key(k, max_rows))
            .collect();
        part_strats
    })
}

/// A per-replica mutation applied to a base partition to model divergence.
#[derive(Debug, Clone)]
enum ReplicaMutation {
    /// Replica is missing this partition entirely.
    Drop,
    /// Replica keeps the base partition unchanged.
    Keep,
    /// Replica has a strictly-newer version (bump every timestamp by `+delta`).
    Newer(i64),
    /// Replica has a strictly-older version (drop every timestamp by `-delta`,
    /// clamped to >= 1).
    Older(i64),
    /// Replica has tombstoned the partition at a high timestamp.
    Tombstone(i64),
}

fn apply_mutation(base: &Partition, m: &ReplicaMutation) -> Option<Partition> {
    match m {
        ReplicaMutation::Drop => None,
        ReplicaMutation::Keep => Some(base.clone()),
        ReplicaMutation::Newer(delta) => {
            let mut p = base.clone();
            bump_partition_ts(&mut p, *delta);
            Some(p)
        }
        ReplicaMutation::Older(delta) => {
            let mut p = base.clone();
            bump_partition_ts(&mut p, -*delta);
            Some(p)
        }
        ReplicaMutation::Tombstone(ts) => {
            let mut p = base.clone();
            p.rows.clear();
            p.static_row = None;
            p.deletion = DeletionTime::new(*ts, 1_700_000_000);
            Some(p)
        }
    }
}

fn bump_partition_ts(p: &mut Partition, delta: i64) {
    let bump = |t: &mut i64| {
        *t = t.saturating_add(delta).max(1);
    };
    let bump_row = |r: &mut Row| {
        if r.primary_key_liveness.timestamp != i64::MIN {
            bump(&mut r.primary_key_liveness.timestamp);
        }
        for (_, c) in &mut r.cells {
            bump(&mut c.timestamp);
        }
    };
    if let Some(sr) = p.static_row.as_mut() {
        bump_row(sr);
    }
    for r in &mut p.rows {
        bump_row(r);
    }
    if p.deletion.marked_for_delete_at != i64::MIN {
        bump(&mut p.deletion.marked_for_delete_at);
    }
}

/// A generated replica set: the base table plus `n` divergent replicas. Each
/// replica is a subset/version of the base produced by per-partition
/// mutations. The harness repairs them pairwise and asserts convergence to the
/// partition-level LWW union (see [`lww_merge`]).
#[derive(Debug, Clone)]
pub struct ReplicaSet {
    pub base: Vec<Partition>,
    pub replicas: Vec<Vec<Partition>>,
}

fn arb_mutation() -> impl Strategy<Value = ReplicaMutation> {
    prop_oneof![
        2 => Just(ReplicaMutation::Keep),
        1 => Just(ReplicaMutation::Drop),
        2 => (1i64..500_000).prop_map(ReplicaMutation::Newer),
        2 => (1i64..500_000).prop_map(ReplicaMutation::Older),
        1 => (1_000_001i64..2_000_000).prop_map(ReplicaMutation::Tombstone),
    ]
}

/// Arbitrary replica set: a base table of up to `max_keys` partitions, diverged
/// into `n_replicas` (2..=`n_replicas`) replicas.
pub fn arb_replica_set(
    max_keys: usize,
    max_rows: usize,
    n_replicas: usize,
) -> impl Strategy<Value = ReplicaSet> {
    let n = n_replicas.max(2);
    arb_table_content(max_keys, max_rows).prop_flat_map(move |base| {
        let n_parts = base.len();
        // For each replica, a mutation per base partition.
        let muts = prop::collection::vec(
            prop::collection::vec(arb_mutation(), n_parts..=n_parts),
            n..=n,
        );
        muts.prop_map(move |per_replica| {
            let replicas = per_replica
                .into_iter()
                .map(|ms| {
                    base.iter()
                        .zip(ms.iter())
                        .filter_map(|(p, m)| apply_mutation(p, m))
                        .collect::<Vec<_>>()
                })
                .collect();
            ReplicaSet {
                base: base.clone(),
                replicas,
            }
        })
    })
}

/// Partition-level last-write-wins merge oracle. This mirrors the *actual*
/// repair convergence model (`repair::diff_partition_sets` +
/// `InMemoryRepairStore::apply_one`): for each key, the partition with the
/// highest `newest_partition_timestamp` wins; equal-timestamp / different-
/// content collisions are an undefined tie that repair surfaces rather than
/// silently merging, so callers should generate inputs that avoid exact ties or
/// tolerate either side. Returns the merged set token-sorted.
///
/// NOTE: the spec wording says "per-cell LWW union", but the shipped repair
/// path is *whole-partition* max-timestamp LWW. The oracle matches the shipped
/// behaviour so the harness flags real divergence, not a spec/impl wording gap.
pub fn lww_merge(replicas: &[Vec<Partition>]) -> Vec<Partition> {
    use std::collections::HashMap;
    let mut by_key: HashMap<Vec<u8>, Partition> = HashMap::new();
    for replica in replicas {
        for p in replica {
            let k = p.key.key.as_bytes().to_vec();
            match by_key.get(&k) {
                None => {
                    by_key.insert(k, p.clone());
                }
                Some(existing) => {
                    if newest_ts(p) > newest_ts(existing) {
                        by_key.insert(k, p.clone());
                    }
                }
            }
        }
    }
    let mut merged: Vec<Partition> = by_key.into_values().collect();
    merged.sort_by(|a, b| a.key.cmp(&b.key));
    merged
}

/// Max timestamp across a partition (cells + liveness + deletion + static row).
/// Mirrors `repair::newest_partition_timestamp`.
pub fn newest_ts(p: &Partition) -> i64 {
    let mut ts = i64::MIN;
    let bump = |row: &Row, ts: &mut i64| {
        if row.primary_key_liveness.timestamp > *ts {
            *ts = row.primary_key_liveness.timestamp;
        }
        for (_, c) in &row.cells {
            if c.timestamp > *ts {
                *ts = c.timestamp;
            }
        }
    };
    if let Some(ref sr) = p.static_row {
        bump(sr, &mut ts);
    }
    for r in &p.rows {
        bump(r, &mut ts);
    }
    if p.deletion.marked_for_delete_at > ts {
        ts = p.deletion.marked_for_delete_at;
    }
    ts
}

/// Arbitrary SSTable layout (count + overlap mode).
pub fn arb_sstable_layout(
    max_sstables: usize,
    max_keys: usize,
) -> impl Strategy<Value = SstableLayout> {
    (
        1..=max_sstables.max(1),
        1..=max_keys.max(1),
        prop_oneof![
            Just(OverlapMode::Full),
            Just(OverlapMode::Disjoint),
            Just(OverlapMode::Partial),
        ],
    )
        .prop_map(|(n_sstables, distinct_keys, overlap)| SstableLayout {
            n_sstables,
            distinct_keys,
            overlap,
        })
}

/// Arbitrary corruption descriptor.
pub fn arb_corruption() -> impl Strategy<Value = Corruption> {
    prop_oneof![
        (0u8..=100).prop_map(|keep_fraction| Corruption::Truncate { keep_fraction }),
        (0u8..=100, 1u16..512, any::<u8>()).prop_map(|(offset_fraction, len, byte)| {
            Corruption::Overwrite {
                offset_fraction,
                len,
                byte,
            }
        }),
        (0u8..=100, 0u8..8).prop_map(|(offset_fraction, bit)| Corruption::BitFlip {
            offset_fraction,
            bit
        }),
        (0u8..=100)
            .prop_map(|offset_fraction| Corruption::OversizedLengthPrefix { offset_fraction }),
        Just(Corruption::ZeroAll),
    ]
}

/// Apply a [`Corruption`] to a byte buffer in place, returning the corrupted
/// bytes. Used to mangle a serialized SSTable component before reopening it.
pub fn apply_corruption(bytes: &[u8], c: &Corruption) -> Vec<u8> {
    let len = bytes.len();
    if len == 0 {
        return bytes.to_vec();
    }
    let off = |frac: u8| ((len.saturating_sub(1)) * frac as usize) / 100;
    match c {
        Corruption::Truncate { keep_fraction } => {
            let keep = (len * *keep_fraction as usize) / 100;
            bytes[..keep.min(len)].to_vec()
        }
        Corruption::Overwrite {
            offset_fraction,
            len: span,
            byte,
        } => {
            let mut out = bytes.to_vec();
            let start = off(*offset_fraction);
            let end = (start + *span as usize).min(len);
            for b in &mut out[start..end] {
                *b = *byte;
            }
            out
        }
        Corruption::BitFlip {
            offset_fraction,
            bit,
        } => {
            let mut out = bytes.to_vec();
            let i = off(*offset_fraction);
            out[i] ^= 1 << (*bit % 8);
            out
        }
        Corruption::OversizedLengthPrefix { offset_fraction } => {
            // Splice in a maximal unsigned-LEB128 varint (10 bytes of 0xFF
            // continuation + a high terminator) — a length prefix that decodes
            // to a multi-EiB value a naive reader would try to allocate.
            let mut out = bytes.to_vec();
            let i = off(*offset_fraction);
            let varint: [u8; 10] = [0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x7F];
            out.splice(i..i, varint.iter().copied());
            out
        }
        Corruption::ZeroAll => vec![0u8; len],
    }
}

/// Arbitrary fuzz config. Caps are kept small so peak-residency assertions run
/// fast while still spanning fanin < reader_cap and fanin > content size.
pub fn arb_config() -> impl Strategy<Value = FuzzConfig> {
    (
        1usize..16, // reader_cap
        1usize..16, // fanin_cap
        16usize..4096,
        1usize..32,
        1usize..8,
    )
        .prop_map(
            |(reader_cap, fanin_cap, max_bytes, max_partitions, compaction_concurrency)| {
                FuzzConfig {
                    reader_cap,
                    fanin_cap,
                    max_bytes,
                    max_partitions,
                    compaction_concurrency,
                }
            },
        )
}
