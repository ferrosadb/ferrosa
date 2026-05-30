//! Deterministic corpus generator for compaction soak testing.
//!
//! From a seed, produces a schema plus several input "SSTable" partition groups
//! that overlap on keys and columns and mix live cells, expiring (TTL) cells,
//! and tombstones across wide, sparse rows — shapes the loadgen generator does
//! not otherwise cover. Generation is reproducible: the same seed yields the
//! same corpus, so a soak failure can be replayed by pinning its seed.

use ferrosa_common::key::{DecoratedKey, PartitionKey};
use ferrosa_common::schema::{ColumnDefinition, TableSchema};
use ferrosa_common::CellValue;
use ferrosa_sstable::types::{DeletionTime, LivenessInfo, Partition, Row};

const REGULAR_COLUMNS: u16 = 4;
const KEYS: u64 = 6;
const CLUSTERINGS: u8 = 3;
const GROUPS: usize = 4;

/// A generated corpus: the schema and one partition group per input SSTable.
pub struct Corpus {
    pub schema: TableSchema,
    pub groups: Vec<Vec<Partition>>,
}

/// Reproducible splitmix64 PRNG. Inlined so the soak adds no dependency and the
/// corpus is byte-identical across machines.
struct Rng(u64);

impl Rng {
    fn new(seed: u64) -> Self {
        Rng(seed)
    }

    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }

    fn chance(&mut self, numerator: u64, denominator: u64) -> bool {
        self.below(denominator) < numerator
    }
}

/// The schema every generated corpus uses: one int clustering column and a
/// handful of regular text columns (for wide rows).
pub fn schema() -> TableSchema {
    let regular_columns = (0..REGULAR_COLUMNS)
        .map(|i| ColumnDefinition {
            name: format!("val{i}"),
            type_name: "org.apache.cassandra.db.marshal.UTF8Type".to_string(),
        })
        .collect();
    TableSchema {
        keyspace: "soak".to_string(),
        table: "compaction".to_string(),
        key_type: "org.apache.cassandra.db.marshal.UTF8Type".to_string(),
        clustering_columns: vec![ColumnDefinition {
            name: "ck".to_string(),
            type_name: "org.apache.cassandra.db.marshal.Int32Type".to_string(),
        }],
        static_columns: vec![],
        regular_columns,
        extensions: Default::default(),
    }
}

fn make_cell(rng: &mut Rng, ts: i64) -> CellValue {
    match rng.below(10) {
        0..=1 => CellValue::tombstone(ts, 100 + ts as i32),
        2..=3 => CellValue::expiring(
            vec![b'e', rng.below(8) as u8],
            ts,
            1000,
            1_000_000 + ts as i32,
        ),
        _ => CellValue::live(vec![b'v', rng.below(8) as u8], ts),
    }
}

fn make_row(rng: &mut Rng, clustering: u8) -> Option<Row> {
    let mut cells: Vec<(u16, CellValue)> = Vec::new();
    for col in 0..REGULAR_COLUMNS {
        if rng.chance(2, 3) {
            // Small timestamp range forces conflicts across overlapping SSTables.
            let ts = 1 + rng.below(5) as i64;
            cells.push((col, make_cell(rng, ts)));
        }
    }
    if cells.is_empty() {
        return None;
    }
    cells.sort_by_key(|(col, _)| *col);
    Some(Row {
        clustering: vec![0, 0, 0, clustering],
        cells,
        deletion: DeletionTime::LIVE,
        primary_key_liveness: LivenessInfo::with_timestamp(1 + rng.below(5) as i64),
    })
}

/// Generate a reproducible corpus for `seed`.
pub fn generate(seed: u64) -> Corpus {
    let mut rng = Rng::new(seed);
    let mut groups = Vec::with_capacity(GROUPS);

    for _ in 0..GROUPS {
        let mut partitions: Vec<Partition> = Vec::new();
        for k in 0..KEYS {
            if !rng.chance(2, 3) {
                continue; // each group covers a random subset of keys
            }
            let mut rows: Vec<Row> = (0..CLUSTERINGS)
                .filter_map(|c| make_row(&mut rng, c))
                .collect();
            if rows.is_empty() {
                continue;
            }
            rows.sort_by(|a, b| a.clustering.cmp(&b.clustering));
            partitions.push(Partition {
                key: DecoratedKey::new(PartitionKey::new(format!("k{k:02}").into_bytes())),
                deletion: DeletionTime::LIVE,
                static_row: None,
                rows,
            });
        }
        partitions.sort_by(|a, b| a.key.cmp(&b.key));
        groups.push(partitions);
    }

    Corpus {
        schema: schema(),
        groups,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn generation_is_reproducible_and_nonempty() {
        let a = generate(42);
        let b = generate(42);
        assert_eq!(a.groups, b.groups, "same seed must yield the same corpus");
        let total: usize = a.groups.iter().map(|g| g.len()).sum();
        assert!(total > 0, "corpus must contain partitions");
        assert_ne!(generate(1).groups, generate(2).groups, "seeds must differ");
    }
}
