//! Quarantine path for malformed rows discovered during flush or commit
//! log replay.
//!
//! When a row's encoded cells fail per-cell length validation against the
//! declared column types, the row cannot be serialised to an SSTable
//! (the writer rejects it) and cannot be safely materialised into the
//! memtable on replay (it will wedge the next flush). Rather than
//! crashing the flush task or the entire node, the row is written to a
//! sibling `quarantine/<keyspace>.<table>.<timestamp>.jsonl` file and the
//! flush continues with the rest of the rows.
//!
//! The quarantine file is the durable record of every byte the client
//! wrote that could not be persisted to the SSTable. An operator can
//! later inspect the JSONL, reconstruct the intended values, and replay
//! the salvaged rows once the upstream bug is fixed.
//!
//! See specs/in-process/bug-memtable-flush-wedge-truncated-timeuuid-from-
//! now-function.md for the bug class this protects against.

use std::fs::{File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;
use std::time::{SystemTime, UNIX_EPOCH};

use ferrosa_common::schema::{validate_cell_bytes, validate_clustering_shape, TableSchema};
use ferrosa_common::{Error, Result};
use ferrosa_sstable::types::{DeletionTime, Partition, Row};
use serde::Serialize;

/// Process-wide counter of rows written to the quarantine path. Incremented
/// every time `QuarantineWriter::write_row` succeeds. Read via
/// [`flush_quarantined_rows_total`] for metrics export and tests.
///
/// Alert if non-zero in CI steady state — a non-zero value means a row
/// that was accepted at write time was malformed at flush/replay time,
/// which violates the fail-loud guard in `Memtable::put` and indicates
/// either a write path that bypassed the guard or a stale row from
/// before the guard landed.
pub static FLUSH_QUARANTINED_ROWS_TOTAL: AtomicU64 = AtomicU64::new(0);

/// Read the process-wide quarantined-row counter.
pub fn flush_quarantined_rows_total() -> u64 {
    FLUSH_QUARANTINED_ROWS_TOTAL.load(Ordering::Relaxed)
}

/// Reset the counter to zero. **Tests only.** Production code reads the
/// counter for metric export but never resets it.
#[doc(hidden)]
pub fn _reset_flush_quarantined_rows_total_for_tests() {
    FLUSH_QUARANTINED_ROWS_TOTAL.store(0, Ordering::Relaxed);
}

/// One JSON-serialisable record per quarantined row. The record carries
/// every byte needed to reconstruct the row offline plus the schema
/// metadata that explains what shape was expected.
#[derive(Debug, Serialize)]
pub struct QuarantineRecord {
    /// Unix-milliseconds when the row was quarantined.
    pub ts_ms: i64,
    pub keyspace: String,
    pub table: String,
    /// Hex-encoded partition key bytes.
    pub partition_key_hex: String,
    /// Hex-encoded clustering bytes (composite, as stored in the row).
    pub clustering_hex: String,
    /// Per-cell entries: `(column_index, column_name, type_name, value_hex)`.
    pub cells: Vec<QuarantineCell>,
    /// Human-readable reason the row was rejected.
    pub error: String,
}

#[derive(Debug, Serialize)]
pub struct QuarantineCell {
    pub col_idx: u16,
    pub col_name: String,
    pub type_name: String,
    pub value_hex: Option<String>,
}

/// Append-only writer for one (keyspace, table) pair. Each instance owns
/// a single JSONL file under `<base>/quarantine/<ks>.<table>.<ts>.jsonl`.
///
/// Writers are cheap to construct and intentionally per-flush — sharing a
/// writer across flushes would re-open the file on every call and risk
/// interleaving writes from concurrent flushes.
pub struct QuarantineWriter {
    file: Mutex<File>,
    path: PathBuf,
    keyspace: String,
    table: String,
}

impl QuarantineWriter {
    /// Create a quarantine writer rooted at `base_dir`. The actual file
    /// is `<base_dir>/quarantine/<keyspace>.<table>.<unix_ms>.jsonl`. The
    /// `quarantine/` subdirectory is created if absent.
    pub fn new(base_dir: &Path, keyspace: &str, table: &str) -> Result<Self> {
        let dir = base_dir.join("quarantine");
        std::fs::create_dir_all(&dir).map_err(|e| {
            Error::InvalidData(format!(
                "quarantine: failed to create {}: {e}",
                dir.display()
            ))
        })?;
        let ts_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_millis() as i64)
            .unwrap_or(0);
        let path = dir.join(format!("{keyspace}.{table}.{ts_ms}.jsonl"));
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)
            .map_err(|e| {
                Error::InvalidData(format!(
                    "quarantine: failed to open {}: {e}",
                    path.display()
                ))
            })?;
        Ok(Self {
            file: Mutex::new(file),
            path,
            keyspace: keyspace.to_string(),
            table: table.to_string(),
        })
    }

    /// Path of the JSONL file this writer appends to.
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Append a single row record. Increments
    /// `FLUSH_QUARANTINED_ROWS_TOTAL` on success.
    pub fn write_row(
        &self,
        partition_key: &[u8],
        row: &Row,
        schema: &TableSchema,
        error: &str,
    ) -> Result<()> {
        let ts_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_millis() as i64)
            .unwrap_or(0);

        let static_count = schema.static_columns.len();
        let cells: Vec<QuarantineCell> = row
            .cells
            .iter()
            .map(|(col_idx, cell)| {
                let idx = *col_idx as usize;
                let (name, type_name) = if idx < static_count {
                    let c = &schema.static_columns[idx];
                    (c.name.clone(), c.type_name.clone())
                } else if idx - static_count < schema.regular_columns.len() {
                    let c = &schema.regular_columns[idx - static_count];
                    (c.name.clone(), c.type_name.clone())
                } else {
                    (String::new(), String::new())
                };
                QuarantineCell {
                    col_idx: *col_idx,
                    col_name: name,
                    type_name,
                    value_hex: cell.value.as_ref().map(hex::encode),
                }
            })
            .collect();

        let record = QuarantineRecord {
            ts_ms,
            keyspace: self.keyspace.clone(),
            table: self.table.clone(),
            partition_key_hex: hex::encode(partition_key),
            clustering_hex: hex::encode(&row.clustering),
            cells,
            error: error.to_string(),
        };

        let line = serde_json::to_string(&record)
            .map_err(|e| Error::InvalidData(format!("quarantine: serialize: {e}")))?;

        let mut file = self
            .file
            .lock()
            .map_err(|e| Error::InvalidData(format!("quarantine: lock poisoned: {e}")))?;
        file.write_all(line.as_bytes())
            .map_err(|e| Error::InvalidData(format!("quarantine: write: {e}")))?;
        file.write_all(b"\n")
            .map_err(|e| Error::InvalidData(format!("quarantine: write: {e}")))?;
        file.sync_data()
            .map_err(|e| Error::InvalidData(format!("quarantine: sync: {e}")))?;

        FLUSH_QUARANTINED_ROWS_TOTAL.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }
}

/// Filter a partition's rows: keep only those that pass per-cell length
/// validation against the schema; route the rest to a quarantine file.
///
/// `writer` is `&mut Option<QuarantineWriter>` and the writer is
/// constructed *lazily* via `make_writer` on the first bad row only.
/// Partitions with no malformed rows therefore never touch disk in the
/// quarantine path — important because the engine restart-scan also
/// uses `<table_dir>/quarantine/` for SSTable corruption forensics, and
/// an unconditional empty JSONL would pollute that dir.
///
/// Returns the number of rows that were quarantined.
pub(crate) fn filter_partition_rows<F>(
    partition: &mut Partition,
    schema: &TableSchema,
    writer: &mut Option<QuarantineWriter>,
    make_writer: F,
) -> Result<usize>
where
    F: FnOnce() -> Result<QuarantineWriter>,
{
    let static_count = schema.static_columns.len();
    let mut quarantined = 0usize;
    let mut surviving: Vec<Row> = Vec::with_capacity(partition.rows.len());
    let mut writer_factory = Some(make_writer);

    for row in std::mem::take(&mut partition.rows) {
        let mut bad: Option<String> = None;

        // Clustering check first — production wedge is in clustering
        // bytes, not cells. A pure tombstone Row (empty clustering, no
        // cells, non-LIVE deletion) is the in-memory marker for a
        // partition-level DELETE; let it through.
        let is_partition_tombstone =
            row.clustering.is_empty() && row.cells.is_empty() && row.deletion != DeletionTime::LIVE;
        if !is_partition_tombstone {
            if let Err(reason) =
                validate_clustering_shape(&schema.clustering_columns, &row.clustering)
            {
                bad = Some(format!(
                    "{}.{} (clustering): {}",
                    schema.keyspace, schema.table, reason
                ));
            }
        }

        if bad.is_none() {
            for (col_idx, cell) in &row.cells {
                let bytes = match &cell.value {
                    Some(v) => v,
                    None => continue,
                };
                let idx = *col_idx as usize;
                let column = if idx < static_count {
                    &schema.static_columns[idx]
                } else if idx - static_count < schema.regular_columns.len() {
                    &schema.regular_columns[idx - static_count]
                } else {
                    continue;
                };
                if let Err(reason) = validate_cell_bytes(&column.type_name, bytes) {
                    bad = Some(format!(
                        "{}.{} (column \"{}\", index {}): {}",
                        schema.keyspace, schema.table, column.name, col_idx, reason
                    ));
                    break;
                }
            }
        }
        match bad {
            Some(reason) => {
                if writer.is_none() {
                    let factory = writer_factory
                        .take()
                        .expect("writer_factory consumed only after writer is set");
                    *writer = Some(factory()?);
                }
                let qw = writer.as_ref().expect("just constructed");
                qw.write_row(partition.key.key.as_bytes(), &row, schema, &reason)?;
                quarantined += 1;
            }
            None => surviving.push(row),
        }
    }

    partition.rows = surviving;
    Ok(quarantined)
}

#[cfg(test)]
mod tests {
    use super::*;
    use ferrosa_common::cell::CellValue;
    use ferrosa_common::key::{DecoratedKey, PartitionKey};
    use ferrosa_common::schema::ColumnDefinition;
    use ferrosa_sstable::types::{DeletionTime, LivenessInfo, Partition};
    use serial_test::serial;
    use tempfile::TempDir;

    fn timeuuid_schema() -> TableSchema {
        TableSchema {
            keyspace: "ks".to_string(),
            table: "t".to_string(),
            key_type: "org.apache.cassandra.db.marshal.UTF8Type".to_string(),
            clustering_columns: vec![],
            static_columns: vec![],
            regular_columns: vec![ColumnDefinition {
                name: "call_id".to_string(),
                type_name: "org.apache.cassandra.db.marshal.TimeUUIDType".to_string(),
            }],
            extensions: Default::default(),
        }
    }

    fn row_with_cell(col_idx: u16, value: &[u8]) -> Row {
        Row {
            clustering: vec![],
            cells: vec![(col_idx, CellValue::live(value.to_vec(), 1000))],
            deletion: DeletionTime::LIVE,
            primary_key_liveness: LivenessInfo::with_timestamp(1000),
        }
    }

    #[test]
    #[serial]
    fn write_row_creates_jsonl_and_increments_counter() {
        let before = flush_quarantined_rows_total();
        let dir = TempDir::new().unwrap();
        let writer = QuarantineWriter::new(dir.path(), "ks", "t").unwrap();
        let row = row_with_cell(0, &[0u8; 8]); // bad: 8 bytes for TimeUUID
        let schema = timeuuid_schema();

        writer
            .write_row(
                b"pk1",
                &row,
                &schema,
                "TimeUUIDType expects 16 bytes, got 8",
            )
            .unwrap();

        assert_eq!(flush_quarantined_rows_total() - before, 1);
        let contents = std::fs::read_to_string(writer.path()).unwrap();
        assert_eq!(contents.lines().count(), 1);
        let parsed: serde_json::Value = serde_json::from_str(contents.trim()).unwrap();
        assert_eq!(parsed["keyspace"], "ks");
        assert_eq!(parsed["table"], "t");
        assert_eq!(parsed["partition_key_hex"], hex::encode(b"pk1"));
        assert_eq!(parsed["cells"][0]["col_name"], "call_id");
        assert_eq!(
            parsed["cells"][0]["type_name"],
            "org.apache.cassandra.db.marshal.TimeUUIDType"
        );
        assert_eq!(parsed["cells"][0]["value_hex"], hex::encode([0u8; 8]));
        assert!(parsed["error"]
            .as_str()
            .unwrap()
            .contains("expects 16 bytes"));
    }

    #[test]
    #[serial]
    fn filter_partition_rows_separates_good_and_bad() {
        let before = flush_quarantined_rows_total();
        let dir = TempDir::new().unwrap();
        let base = dir.path().to_path_buf();
        let schema = timeuuid_schema();

        let key = DecoratedKey::new(PartitionKey::new(b"pk1".to_vec()));
        let mut partition = Partition {
            key: key.clone(),
            deletion: DeletionTime::LIVE,
            static_row: None,
            rows: vec![
                row_with_cell(0, &[0u8; 16]), // good
                row_with_cell(0, &[0u8; 8]),  // bad
                row_with_cell(0, &[0u8; 16]), // good
            ],
        };

        let mut writer: Option<QuarantineWriter> = None;
        let quarantined = filter_partition_rows(&mut partition, &schema, &mut writer, || {
            QuarantineWriter::new(&base, "ks", "t")
        })
        .unwrap();
        assert_eq!(quarantined, 1);
        assert_eq!(partition.rows.len(), 2, "good rows must survive");
        assert_eq!(flush_quarantined_rows_total() - before, 1);

        let writer = writer.expect("bad row triggered lazy writer construction");
        let contents = std::fs::read_to_string(writer.path()).unwrap();
        assert_eq!(contents.lines().count(), 1, "one bad row → one JSONL line");
    }

    /// All-good partition: lazy writer must NOT be constructed and the
    /// quarantine directory must NOT exist on disk. The engine startup
    /// scan reuses `<table_dir>/quarantine/` for corrupt-SSTable
    /// forensics, so an unconditional empty file would corrupt that
    /// counter (see engine::startup_does_not_quarantine_zero_byte_rows_db).
    #[test]
    #[serial]
    fn filter_partition_rows_all_good_writes_nothing() {
        let before = flush_quarantined_rows_total();
        let dir = TempDir::new().unwrap();
        let base = dir.path().to_path_buf();
        let schema = timeuuid_schema();

        let key = DecoratedKey::new(PartitionKey::new(b"pk1".to_vec()));
        let mut partition = Partition {
            key,
            deletion: DeletionTime::LIVE,
            static_row: None,
            rows: vec![row_with_cell(0, &[0u8; 16]), row_with_cell(0, &[0u8; 16])],
        };

        let mut writer: Option<QuarantineWriter> = None;
        let quarantined = filter_partition_rows(&mut partition, &schema, &mut writer, || {
            QuarantineWriter::new(&base, "ks", "t")
        })
        .unwrap();
        assert_eq!(quarantined, 0);
        assert_eq!(partition.rows.len(), 2);
        assert_eq!(flush_quarantined_rows_total() - before, 0);
        assert!(writer.is_none(), "lazy writer must not be constructed");
        assert!(
            !dir.path().join("quarantine").exists(),
            "quarantine dir must not exist when no rows were bad"
        );
    }
}
