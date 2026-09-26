//! Acknowledged-write oracle + cancel-safety invariant checker (T-020,
//! `test-specification.md` L10, `compaction-cancel-safety.md` I1-I6).
//!
//! [`WriteOracle`] is a plain model of every acknowledged write and delete a
//! test made, kept independently of the engine. [`assert_cancel_invariants`]
//! compares that model against a (re)opened engine and inspects the table
//! directory on disk, so a cancel-harness or crash-sweep test can assert the
//! same four invariants (I1-I4) regardless of which `CancelPoint` it hit.
//!
//! Scope for this packet: the model tracks one clustering column and one
//! cell per row (exactly what the crash-sweep scenarios write) and I1's
//! comparison is by point read over every key the model knows about, not an
//! independent full-table scan — `ferrosa-storage` has no public full-scan
//! API yet. A point-read comparison still catches every failure mode a
//! cancel-safety bug produces here: a write failing to read back, or a
//! duplicate/rolled-back generation resurrecting a stale value. It would
//! *not* catch a wholly unexpected extra key appearing under a name the test
//! never wrote, which only a genuine range scan proves absent.
//!
//! I2 ("exactly one of {inputs, output}") reports whether the input
//! generations, the output generation, or a mixture of both are discoverable
//! after a (re)open. T-022 (generation reservation, forge t_cb6fa288) and
//! T-023 (C3 startup reconciliation) together close every window this
//! crate's crash-sweep exercises, so no `cancel_crash_sweep_*` case is gated
//! behind a "known open window" feature any more. The remaining T-024 scope
//! (per-component retirement atomicity within one generation) needs a finer
//! hook than the crash-sweep's per-generation `CancelPoint` provides -- see
//! `ferrosa-storage/specs/roadmap.md`.

use std::collections::{BTreeMap, HashSet};
use std::path::{Path, PathBuf};

use ferrosa_common::key::{DecoratedKey, PartitionKey};
use ferrosa_sstable::io::FileReadAt;
use ferrosa_sstable::reader::{SSTableComponents, SSTableReader};

use crate::engine::StorageEngine;
use crate::TableId;

/// One acknowledged write or delete: `(partition key bytes, clustering
/// bytes) -> latest (timestamp, value-or-tombstone)`.
#[derive(Debug, Default, Clone)]
pub struct WriteOracle {
    rows: BTreeMap<(Vec<u8>, Vec<u8>), OracleCell>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct OracleCell {
    timestamp: i64,
    value: Option<Vec<u8>>,
}

impl WriteOracle {
    pub fn new() -> Self {
        Self::default()
    }

    /// Records an acknowledged write, keyed by decorated key + clustering.
    /// Last-write-wins by timestamp, matching engine semantics; a
    /// timestamp equal to or newer than what is recorded replaces it (the
    /// scenarios that build this model issue writes with strictly
    /// increasing timestamps, so ties never arise in practice).
    pub fn record_write(&mut self, key: &DecoratedKey, clustering: &[u8], value: &[u8], ts: i64) {
        self.upsert(key, clustering, ts, Some(value.to_vec()));
    }

    /// Records an acknowledged row delete (tombstone) at `ts`.
    pub fn record_delete(&mut self, key: &DecoratedKey, clustering: &[u8], ts: i64) {
        self.upsert(key, clustering, ts, None);
    }

    fn upsert(&mut self, key: &DecoratedKey, clustering: &[u8], ts: i64, value: Option<Vec<u8>>) {
        let map_key = (key.key.as_bytes().to_vec(), clustering.to_vec());
        let entry = self.rows.entry(map_key).or_insert(OracleCell {
            timestamp: i64::MIN,
            value: None,
        });
        if ts >= entry.timestamp {
            entry.timestamp = ts;
            entry.value = value;
        }
    }

    pub fn len(&self) -> usize {
        self.rows.len()
    }

    pub fn is_empty(&self) -> bool {
        self.rows.is_empty()
    }

    /// I1: every acknowledged write/delete reads back exactly as recorded.
    /// Returns the first mismatch found, if any (message includes the key,
    /// expected and actual state).
    pub fn diff_against_engine(&self, engine: &StorageEngine, tid: &TableId) -> Vec<String> {
        let mut mismatches = Vec::new();
        for ((pk, ck), expected) in &self.rows {
            let key = DecoratedKey::new(PartitionKey::new(pk.clone()));
            let partition = match engine.read(tid, &key) {
                Ok(p) => p,
                Err(e) => {
                    mismatches.push(format!("read error for key {pk:?} clustering {ck:?}: {e}"));
                    continue;
                }
            };
            let actual: Option<Vec<u8>> = partition.as_ref().and_then(|partition| {
                let row = partition.rows.iter().find(|r| &r.clustering == ck)?;
                if !row.deletion.is_live() {
                    return None;
                }
                row.cells
                    .iter()
                    .find(|(idx, _)| *idx == 0)
                    .and_then(|(_, c)| c.value.clone())
            });
            if actual != expected.value {
                mismatches.push(format!(
                    "key {pk:?} clustering {ck:?}: expected {:?} (ts {}), got {:?}",
                    expected.value, expected.timestamp, actual
                ));
            }
        }
        mismatches
    }
}

/// The result of checking I1-I4 for one compaction scenario. Each field is a
/// pass/fail plus enough detail to explain a failure; callers decide whether
/// a given failure is expected (a documented, still-open window, feature
/// gated if one exists) or a real regression.
#[derive(Debug, Default)]
pub struct CancelInvariantReport {
    /// I1: mismatches against the oracle (empty = content intact).
    pub content_mismatches: Vec<String>,
    /// I2: `None` when exactly one of {inputs, output} is discoverable.
    /// `Some(detail)` when today's startup left a mixture.
    pub duplicate_or_missing_generations: Option<String>,
    /// I3: every discoverable `*-Data.db` opened and walked; errors here
    /// name the generation and the failure.
    pub corrupt_generations: Vec<String>,
    /// I4: leaked paths (compaction staging, `.promote-*`, stale `*.tmp`,
    /// orphan sidecars) still present after the (re)open.
    pub leaks: Vec<PathBuf>,
}

impl CancelInvariantReport {
    pub fn assert_i1_content_matches_oracle(&self) {
        assert!(
            self.content_mismatches.is_empty(),
            "I1 violated (content != oracle): {:#?}",
            self.content_mismatches
        );
    }

    pub fn assert_i2_exactly_one(&self) {
        assert!(
            self.duplicate_or_missing_generations.is_none(),
            "I2 violated: {}",
            self.duplicate_or_missing_generations
                .as_deref()
                .unwrap_or("")
        );
    }

    pub fn assert_i3_no_corrupt_generation(&self) {
        assert!(
            self.corrupt_generations.is_empty(),
            "I3 violated (unreadable/corrupt generation): {:#?}",
            self.corrupt_generations
        );
    }

    pub fn assert_i4_no_leaks(&self) {
        assert!(
            self.leaks.is_empty(),
            "I4 violated, leaked paths: {:#?}",
            self.leaks
        );
    }

    /// Panics on the first invariant this report failed. Prefer the
    /// per-invariant asserters when a scenario is a documented, still-open
    /// window that is expected to fail exactly one of I1-I4.
    pub fn assert_all(&self) {
        self.assert_i1_content_matches_oracle();
        self.assert_i2_exactly_one();
        self.assert_i3_no_corrupt_generation();
        self.assert_i4_no_leaks();
    }
}

/// Checks I1-I4 for `tid` against `oracle`, given the generation ids that
/// were this compaction's `inputs`. Assumes the table directory holds
/// exactly those input generations plus, possibly, one compaction output
/// generation and nothing else — true for every scenario this harness
/// builds (a fresh temp data dir per case).
pub fn assert_cancel_invariants(
    engine: &StorageEngine,
    tid: &TableId,
    oracle: &WriteOracle,
    input_gens: &[u64],
) -> CancelInvariantReport {
    let table_dir = engine.table_sstable_dir(tid);
    let data_dir = engine.data_dir();

    let content_mismatches = oracle.diff_against_engine(engine, tid);

    let observed: HashSet<u64> = StorageEngine::list_generations_in_dir(&table_dir)
        .into_iter()
        .collect();
    let inputs: HashSet<u64> = input_gens.iter().copied().collect();
    // Exactly one of {inputs, output} discoverable (I2): either every input
    // is still live and nothing else is (rolled back), or the inputs are
    // entirely gone and a non-empty, disjoint set remains (rolled forward
    // to the output). Anything else is a mixture.
    let rolled_back = observed == inputs;
    let rolled_forward = observed.is_disjoint(&inputs) && !observed.is_empty();
    let duplicate_or_missing_generations = if rolled_back || rolled_forward {
        None
    } else {
        Some(format!(
            "expected exactly the inputs {inputs:?} (rollback) or a disjoint \
             non-empty output set (roll-forward); observed {observed:?}"
        ))
    };

    let mut corrupt_generations = Vec::new();
    for gen in &observed {
        if let Err(e) = open_and_walk_generation(&table_dir, *gen) {
            corrupt_generations.push(format!("generation {gen}: {e}"));
        }
    }

    let mut leaks = Vec::new();
    collect_leaks(&table_dir, &mut leaks);
    // `compaction/<table>` staging: today's startup
    // (`cleanup_stale_compaction_staging`) already wipes the whole
    // `compaction/` tree on every open, so this is normally empty by the
    // time a test calls this function (it runs after reopening the
    // engine). Checked anyway so a future change to that startup sweep
    // cannot silently regress I4 without this test noticing.
    let staging_dir = data_dir.join("compaction").join(tid.to_string());
    if staging_dir.exists() {
        leaks.push(staging_dir);
    }

    CancelInvariantReport {
        content_mismatches,
        duplicate_or_missing_generations,
        corrupt_generations,
        leaks,
    }
}

/// Opens generation `gen` under `table_dir` (flat `<gen>-Data.db` or nested
/// `<gen>/<gen>-Data.db`, mirroring engine discovery) and walks every
/// partition, returning `Err` on any open or read failure (I3).
fn open_and_walk_generation(table_dir: &Path, gen: u64) -> Result<(), String> {
    let gen_str = gen.to_string();
    let nested = table_dir.join(&gen_str);
    let dir = if nested.join(format!("{gen_str}-Data.db")).exists() {
        nested
    } else {
        table_dir.to_path_buf()
    };

    let component = |suffix: &str| dir.join(format!("{gen_str}-{suffix}"));

    let data = FileReadAt::open(component("Data.db")).map_err(|e| format!("Data.db: {e}"))?;
    let partitions =
        FileReadAt::open(component("Partitions.db")).map_err(|e| format!("Partitions.db: {e}"))?;
    let rows = FileReadAt::open(component("Rows.db")).map_err(|e| format!("Rows.db: {e}"))?;
    let filter = std::fs::read(component("Filter.db")).unwrap_or_default();
    let statistics = std::fs::read(component("Statistics.db")).unwrap_or_default();
    let compression_info = std::fs::read(component("CompressionInfo.db")).ok();

    let reader = SSTableReader::open(SSTableComponents {
        data,
        partitions,
        rows,
        filter,
        compression_info,
        statistics,
    })
    .map_err(|e| format!("open: {e}"))?;

    let mut iter = reader
        .partitions_iter()
        .map_err(|e| format!("partitions_iter: {e}"))?;
    while iter
        .next_partition()
        .map_err(|e| format!("walk: {e}"))?
        .is_some()
    {}
    Ok(())
}

/// Lists paths under `table_dir` matching known leak shapes: `.promote-*`
/// staging debris (window B), `.retired-*` (C4's future atomic-retirement
/// marker, T-024), and stale `*.tmp` files.
fn collect_leaks(table_dir: &Path, out: &mut Vec<PathBuf>) {
    let Ok(entries) = std::fs::read_dir(table_dir) else {
        return;
    };
    for entry in entries.flatten() {
        let name = entry.file_name();
        let name = name.to_string_lossy();
        if name.starts_with(".promote-") || name.starts_with(".retired-") || name.ends_with(".tmp")
        {
            out.push(entry.path());
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ferrosa_common::cell::CellValue;
    use ferrosa_sstable::types::{DeletionTime, LivenessInfo, Row};

    use crate::engine::StorageEngineConfig;

    fn key(s: &str) -> DecoratedKey {
        DecoratedKey::new(PartitionKey::new(s.as_bytes().to_vec()))
    }

    fn row(ck: i32, value: &[u8], ts: i64) -> Row {
        Row {
            clustering: ck.to_be_bytes().to_vec(),
            cells: vec![(0, CellValue::live(value.to_vec(), ts))],
            deletion: DeletionTime::LIVE,
            primary_key_liveness: LivenessInfo::with_timestamp(ts),
        }
    }

    fn test_schema() -> ferrosa_common::schema::TableSchema {
        ferrosa_common::schema::TableSchema {
            keyspace: "ks".to_string(),
            table: "t".to_string(),
            key_type: "org.apache.cassandra.db.marshal.UTF8Type".to_string(),
            clustering_columns: vec![ferrosa_common::schema::ColumnDefinition {
                name: "ck".to_string(),
                type_name: "org.apache.cassandra.db.marshal.Int32Type".to_string(),
            }],
            static_columns: vec![],
            regular_columns: vec![ferrosa_common::schema::ColumnDefinition {
                name: "val".to_string(),
                type_name: "org.apache.cassandra.db.marshal.UTF8Type".to_string(),
            }],
            extensions: Default::default(),
        }
    }

    #[test]
    fn oracle_matches_a_freshly_written_and_flushed_table() {
        let dir = tempfile::tempdir().unwrap();
        let engine =
            StorageEngine::new(StorageEngineConfig::test_config(dir.path()), None).unwrap();
        let tid = TableId::new("ks", "t");
        engine.register_table(test_schema()).unwrap();

        let mut oracle = WriteOracle::new();
        let k = key("p1");
        engine.write(&tid, &k, row(1, b"v1", 1000), 1000).unwrap();
        oracle.record_write(&k, &1i32.to_be_bytes(), b"v1", 1000);
        engine.flush(&tid).unwrap();

        let mismatches = oracle.diff_against_engine(&engine, &tid);
        assert!(mismatches.is_empty(), "{mismatches:?}");

        let input_gens = StorageEngine::list_generations_in_dir(&engine.table_sstable_dir(&tid));
        let report = assert_cancel_invariants(&engine, &tid, &oracle, &input_gens);
        report.assert_all();
    }

    #[test]
    fn oracle_detects_a_lost_write() {
        let dir = tempfile::tempdir().unwrap();
        let engine =
            StorageEngine::new(StorageEngineConfig::test_config(dir.path()), None).unwrap();
        let tid = TableId::new("ks", "t");
        engine.register_table(test_schema()).unwrap();

        let mut oracle = WriteOracle::new();
        // Record a write in the oracle that was never actually applied —
        // simulates a lost-write bug so the checker's own logic is proven
        // to catch it.
        oracle.record_write(&key("ghost"), &1i32.to_be_bytes(), b"missing", 500);

        let mismatches = oracle.diff_against_engine(&engine, &tid);
        assert_eq!(mismatches.len(), 1, "{mismatches:?}");
    }

    #[test]
    fn invariant_report_flags_a_promote_staging_leak() {
        let dir = tempfile::tempdir().unwrap();
        let engine =
            StorageEngine::new(StorageEngineConfig::test_config(dir.path()), None).unwrap();
        let tid = TableId::new("ks", "t");
        engine.register_table(test_schema()).unwrap();
        engine
            .write(&tid, &key("p1"), row(1, b"v1", 1000), 1000)
            .unwrap();
        engine.flush(&tid).unwrap();

        let table_dir = engine.table_sstable_dir(&tid);
        std::fs::create_dir_all(table_dir.join(".promote-99-123")).unwrap();

        let oracle = WriteOracle::new();
        let report = assert_cancel_invariants(&engine, &tid, &oracle, &[]);
        assert_eq!(report.leaks.len(), 1, "{:?}", report.leaks);
    }
}
