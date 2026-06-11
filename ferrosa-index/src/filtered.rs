//! Filtered index wrapper.
//!
//! [`FilteredIndexFactory`] wraps any [`IndexFactory`] and applies a
//! [`FilterPredicate`] during index build. Only rows whose filter column
//! value satisfies the predicate are passed to the inner index builder.
//! At read time, the wrapper delegates directly to the inner reader.

use ferrosa_common::CellValue;

use crate::{
    FilterOp, FilterPredicate, IndexBuilder, IndexCapabilities, IndexConfig, IndexFactory,
    IndexFiles, IndexReader, IndexResult, IndexType,
};

// ── Predicate evaluation ─────────────────────────────────────────────────────

/// Evaluate a filter predicate against a cell value (raw bytes).
///
/// This is the single source of truth for partial-index predicate evaluation.
/// The storage build path (`LocalBackend::build`) and the memtable write path
/// both call this so a sidecar and its memtable companion agree on exactly
/// which rows belong in a filtered index.
pub fn evaluate_predicate(predicate: &FilterPredicate, cell_value: &[u8]) -> bool {
    match predicate.op {
        FilterOp::Eq => cell_value == predicate.value.as_slice(),
        FilterOp::NotEq => cell_value != predicate.value.as_slice(),
        FilterOp::Lt => cell_value < predicate.value.as_slice(),
        FilterOp::Gt => cell_value > predicate.value.as_slice(),
        FilterOp::LtEq => cell_value <= predicate.value.as_slice(),
        FilterOp::GtEq => cell_value >= predicate.value.as_slice(),
    }
}

// ── FilteredIndexFactory ─────────────────────────────────────────────────────

/// An index factory that wraps another factory and filters rows during build.
///
/// Only rows matching the configured predicate are passed to the inner
/// index builder. The reader delegates to the inner reader without any
/// additional filtering.
pub struct FilteredIndexFactory {
    inner: Box<dyn IndexFactory>,
    predicate: FilterPredicate,
}

impl FilteredIndexFactory {
    /// Create a new filtered index factory.
    ///
    /// - `inner`: the underlying index factory (e.g., B-tree, hash, or phonetic)
    /// - `predicate`: the filter condition applied during build
    pub fn new(inner: Box<dyn IndexFactory>, predicate: FilterPredicate) -> Self {
        Self { inner, predicate }
    }
}

impl IndexFactory for FilteredIndexFactory {
    fn create_builder(&self, config: &IndexConfig) -> IndexResult<Box<dyn IndexBuilder>> {
        let inner_builder = self.inner.create_builder(config)?;
        Ok(Box::new(FilteredBuilder {
            inner: inner_builder,
            predicate: self.predicate.clone(),
        }))
    }

    fn open_reader(&self, files: &IndexFiles) -> IndexResult<Box<dyn IndexReader>> {
        self.inner.open_reader(files)
    }

    fn index_type(&self) -> IndexType {
        IndexType::Filtered
    }

    fn capabilities(&self) -> IndexCapabilities {
        self.inner.capabilities()
    }
}

// ── Builder ──────────────────────────────────────────────────────────────────

struct FilteredBuilder {
    inner: Box<dyn IndexBuilder>,
    predicate: FilterPredicate,
}

impl IndexBuilder for FilteredBuilder {
    fn add_row(
        &mut self,
        partition_key: &[u8],
        clustering_key: &[u8],
        cells: &[CellValue],
        column_positions: &[usize],
    ) -> IndexResult<()> {
        let filter_pos = self.predicate.column_position;
        if filter_pos >= cells.len() {
            // Filter column not present -> skip this row
            return Ok(());
        }

        let cell = &cells[filter_pos];
        match &cell.value {
            Some(value) => {
                if evaluate_predicate(&self.predicate, value) {
                    self.inner
                        .add_row(partition_key, clustering_key, cells, column_positions)
                } else {
                    Ok(())
                }
            }
            None => {
                // Tombstone -> does not match
                Ok(())
            }
        }
    }

    fn finish(self: Box<Self>) -> IndexResult<IndexFiles> {
        self.inner.finish()
    }
}

// ── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::phonetic::{PhoneticAlgorithm, PhoneticIndexFactory};
    use crate::IndexKey;
    use ferrosa_common::CellValue;

    #[test]
    fn filtered_index_only_includes_matching_rows() {
        let dir = tempfile::tempdir().unwrap();

        // Phonetic index on column 0 (name), filter on column 1 (status)
        let inner = Box::new(PhoneticIndexFactory::new(PhoneticAlgorithm::Soundex));

        let predicate = FilterPredicate {
            column_position: 1,
            op: FilterOp::Eq,
            value: b"active".to_vec(),
        };

        let factory = FilteredIndexFactory::new(inner, predicate);
        let config = IndexConfig {
            index_type: IndexType::Filtered,
            column_positions: vec![0],
            output_dir: dir.path().to_path_buf(),
            name: "test_filtered".to_string(),
        };

        let mut builder = factory.create_builder(&config).unwrap();

        // Row 1: name="Smith", status="active" -> should be indexed
        builder
            .add_row(
                b"pk1",
                b"",
                &[
                    CellValue::live(b"Smith".to_vec(), 1),
                    CellValue::live(b"active".to_vec(), 1),
                ],
                &[0],
            )
            .unwrap();

        // Row 2: name="Jones", status="inactive" -> should NOT be indexed
        builder
            .add_row(
                b"pk2",
                b"",
                &[
                    CellValue::live(b"Jones".to_vec(), 2),
                    CellValue::live(b"inactive".to_vec(), 2),
                ],
                &[0],
            )
            .unwrap();

        // Row 3: name="Smythe", status="active" -> should be indexed
        builder
            .add_row(
                b"pk3",
                b"",
                &[
                    CellValue::live(b"Smythe".to_vec(), 3),
                    CellValue::live(b"active".to_vec(), 3),
                ],
                &[0],
            )
            .unwrap();

        let files = builder.finish().unwrap();
        let reader = factory.open_reader(&files).unwrap();

        // Lookup "Smith" phonetically -> should find only the active rows
        let mut results = reader.lookup(&IndexKey(b"Smith".to_vec())).unwrap();
        results.sort_by(|a, b| a.partition_key.cmp(&b.partition_key));
        assert_eq!(results.len(), 2);
        assert_eq!(results[0].partition_key, b"pk1");
        assert_eq!(results[1].partition_key, b"pk3");

        // Jones was inactive, phonetic lookup for "Jones" should return nothing
        let results = reader.lookup(&IndexKey(b"Jones".to_vec())).unwrap();
        assert!(results.is_empty());
    }

    #[test]
    fn filtered_index_not_eq() {
        let dir = tempfile::tempdir().unwrap();

        let inner = Box::new(PhoneticIndexFactory::new(PhoneticAlgorithm::Soundex));

        let predicate = FilterPredicate {
            column_position: 1,
            op: FilterOp::NotEq,
            value: b"deleted".to_vec(),
        };

        let factory = FilteredIndexFactory::new(inner, predicate);
        let config = IndexConfig {
            index_type: IndexType::Filtered,
            column_positions: vec![0],
            output_dir: dir.path().to_path_buf(),
            name: "test_not_eq".to_string(),
        };

        let mut builder = factory.create_builder(&config).unwrap();

        builder
            .add_row(
                b"pk1",
                b"",
                &[
                    CellValue::live(b"Smith".to_vec(), 1),
                    CellValue::live(b"active".to_vec(), 1),
                ],
                &[0],
            )
            .unwrap();

        builder
            .add_row(
                b"pk2",
                b"",
                &[
                    CellValue::live(b"Jones".to_vec(), 2),
                    CellValue::live(b"deleted".to_vec(), 2),
                ],
                &[0],
            )
            .unwrap();

        let files = builder.finish().unwrap();
        let reader = factory.open_reader(&files).unwrap();

        let results = reader.lookup(&IndexKey(b"Smith".to_vec())).unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].partition_key, b"pk1");

        let results = reader.lookup(&IndexKey(b"Jones".to_vec())).unwrap();
        assert!(results.is_empty());
    }

    #[test]
    fn filtered_index_comparison_ops() {
        let pred_lt = FilterPredicate {
            column_position: 0,
            op: FilterOp::Lt,
            value: b"M".to_vec(),
        };
        assert!(evaluate_predicate(&pred_lt, b"A"));
        assert!(!evaluate_predicate(&pred_lt, b"M"));
        assert!(!evaluate_predicate(&pred_lt, b"Z"));

        let pred_gt = FilterPredicate {
            column_position: 0,
            op: FilterOp::Gt,
            value: b"M".to_vec(),
        };
        assert!(!evaluate_predicate(&pred_gt, b"A"));
        assert!(!evaluate_predicate(&pred_gt, b"M"));
        assert!(evaluate_predicate(&pred_gt, b"Z"));

        let pred_lteq = FilterPredicate {
            column_position: 0,
            op: FilterOp::LtEq,
            value: b"M".to_vec(),
        };
        assert!(evaluate_predicate(&pred_lteq, b"A"));
        assert!(evaluate_predicate(&pred_lteq, b"M"));
        assert!(!evaluate_predicate(&pred_lteq, b"Z"));

        let pred_gteq = FilterPredicate {
            column_position: 0,
            op: FilterOp::GtEq,
            value: b"M".to_vec(),
        };
        assert!(!evaluate_predicate(&pred_gteq, b"A"));
        assert!(evaluate_predicate(&pred_gteq, b"M"));
        assert!(evaluate_predicate(&pred_gteq, b"Z"));
    }

    #[test]
    fn filtered_index_tombstone_skipped() {
        let dir = tempfile::tempdir().unwrap();

        let inner = Box::new(PhoneticIndexFactory::new(PhoneticAlgorithm::Soundex));
        let predicate = FilterPredicate {
            column_position: 1,
            op: FilterOp::Eq,
            value: b"active".to_vec(),
        };

        let factory = FilteredIndexFactory::new(inner, predicate);
        let config = IndexConfig {
            index_type: IndexType::Filtered,
            column_positions: vec![0],
            output_dir: dir.path().to_path_buf(),
            name: "test_tombstone".to_string(),
        };

        let mut builder = factory.create_builder(&config).unwrap();

        // Row with a tombstone in the filter column -> should be skipped
        builder
            .add_row(
                b"pk1",
                b"",
                &[
                    CellValue::live(b"Smith".to_vec(), 1),
                    CellValue::tombstone(1, 1700000000),
                ],
                &[0],
            )
            .unwrap();

        let files = builder.finish().unwrap();
        let reader = factory.open_reader(&files).unwrap();

        let results = reader.lookup(&IndexKey(b"Smith".to_vec())).unwrap();
        assert!(results.is_empty());
    }

    #[test]
    fn filtered_capabilities_match_inner() {
        let dir = tempfile::tempdir().unwrap();

        let inner = Box::new(PhoneticIndexFactory::new(PhoneticAlgorithm::Soundex));
        let predicate = FilterPredicate {
            column_position: 1,
            op: FilterOp::Eq,
            value: b"x".to_vec(),
        };

        let factory = FilteredIndexFactory::new(inner, predicate);
        let config = IndexConfig {
            index_type: IndexType::Filtered,
            column_positions: vec![0],
            output_dir: dir.path().to_path_buf(),
            name: "test_caps".to_string(),
        };

        // Build an empty index so we can open the reader
        let builder = factory.create_builder(&config).unwrap();
        let files = builder.finish().unwrap();

        let reader = factory.open_reader(&files).unwrap();
        let caps = reader.capabilities();
        assert!(caps.contains(IndexCapabilities::POINT_LOOKUP));
        assert!(caps.contains(IndexCapabilities::PHONETIC));
    }

    #[test]
    fn filtered_with_btree_inner() {
        use crate::btree::BTreeIndexFactory;

        let dir = tempfile::tempdir().unwrap();

        let inner = Box::new(BTreeIndexFactory);
        let predicate = FilterPredicate {
            column_position: 1,
            op: FilterOp::Eq,
            value: b"active".to_vec(),
        };

        let factory = FilteredIndexFactory::new(inner, predicate);
        let config = IndexConfig {
            index_type: IndexType::Filtered,
            column_positions: vec![0],
            output_dir: dir.path().to_path_buf(),
            name: "test_filtered_btree".to_string(),
        };

        let mut builder = factory.create_builder(&config).unwrap();

        builder
            .add_row(
                b"pk1",
                b"ck1",
                &[
                    CellValue::live(b"alpha".to_vec(), 1),
                    CellValue::live(b"active".to_vec(), 1),
                ],
                &[0],
            )
            .unwrap();

        builder
            .add_row(
                b"pk2",
                b"ck2",
                &[
                    CellValue::live(b"alpha".to_vec(), 2),
                    CellValue::live(b"inactive".to_vec(), 2),
                ],
                &[0],
            )
            .unwrap();

        builder
            .add_row(
                b"pk3",
                b"ck3",
                &[
                    CellValue::live(b"alpha".to_vec(), 3),
                    CellValue::live(b"active".to_vec(), 3),
                ],
                &[0],
            )
            .unwrap();

        let files = builder.finish().unwrap();
        let reader = factory.open_reader(&files).unwrap();

        // B-tree capabilities
        let caps = reader.capabilities();
        assert!(caps.contains(IndexCapabilities::POINT_LOOKUP));
        assert!(caps.contains(IndexCapabilities::RANGE_SCAN));

        // Only active rows should be indexed
        let results = reader.lookup(&IndexKey(b"alpha".to_vec())).unwrap();
        assert_eq!(results.len(), 2);

        let pks: Vec<&[u8]> = results.iter().map(|r| r.partition_key.as_slice()).collect();
        assert!(pks.contains(&b"pk1".as_slice()));
        assert!(pks.contains(&b"pk3".as_slice()));
        assert!(!pks.contains(&b"pk2".as_slice()));
    }
}
