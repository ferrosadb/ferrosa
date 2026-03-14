//! Filtered index wrapper: evaluates a [`FilterPredicate`] against a filter
//! column and only passes matching rows to an inner index.
//!
//! The reader delegates entirely to the inner index reader. Capabilities are
//! inherited from the inner index.
//!
//! This enables partial indexes that only cover a subset of rows, reducing
//! index size and build time for selective queries (e.g., only index active
//! users, only index orders with status = "pending").

use crate::{
    FilterOp, FilterPredicate, IndexBuilder, IndexConfig, IndexFactory, IndexFiles, IndexReader,
    IndexResult,
};
use ferrosa_common::CellValue;

/// Factory for creating filtered index builders and readers.
///
/// Wraps an inner [`IndexFactory`] and applies a [`FilterPredicate`] during
/// build time. Rows that do not match the predicate are skipped.
pub struct FilteredIndexFactory {
    inner: Box<dyn IndexFactory>,
    predicate: FilterPredicate,
    /// Column position for the filter predicate (separate from the indexed column).
    filter_column_position: usize,
}

impl FilteredIndexFactory {
    /// Create a new filtered index factory.
    ///
    /// # Arguments
    /// * `inner` - The inner index factory that handles actual indexing.
    /// * `predicate` - The filter predicate to evaluate against rows.
    /// * `filter_column_position` - The column position index for the predicate column
    ///   within the cells array. This is distinct from the indexed column(s) specified
    ///   in [`IndexConfig::column_positions`].
    pub fn new(
        inner: Box<dyn IndexFactory>,
        predicate: FilterPredicate,
        filter_column_position: usize,
    ) -> Self {
        Self {
            inner,
            predicate,
            filter_column_position,
        }
    }
}

impl IndexFactory for FilteredIndexFactory {
    fn create_builder(&self, config: &IndexConfig) -> IndexResult<Box<dyn IndexBuilder>> {
        let inner_builder = self.inner.create_builder(config)?;
        Ok(Box::new(FilteredBuilder {
            inner: inner_builder,
            predicate: self.predicate.clone(),
            filter_column_position: self.filter_column_position,
        }))
    }

    fn open_reader(&self, files: &IndexFiles) -> IndexResult<Box<dyn IndexReader>> {
        // The reader delegates entirely to the inner reader: once the index is
        // built, the filtering is already applied and the data file only
        // contains matching rows.
        self.inner.open_reader(files)
    }

    fn merge(
        &self,
        readers: Vec<Box<dyn IndexReader>>,
        builder: Box<dyn IndexBuilder>,
    ) -> IndexResult<IndexFiles> {
        self.inner.merge(readers, builder)
    }
}

/// Builder that evaluates the filter predicate before delegating to the inner builder.
struct FilteredBuilder {
    inner: Box<dyn IndexBuilder>,
    predicate: FilterPredicate,
    filter_column_position: usize,
}

impl FilteredBuilder {
    /// Evaluate the filter predicate against the filter column in the given cells.
    fn matches_predicate(&self, cells: &[(u16, CellValue)]) -> bool {
        let cell = cells
            .iter()
            .find(|(pos, _)| *pos as usize == self.filter_column_position)
            .map(|(_, cv)| cv);

        let cell = match cell {
            Some(c) => c,
            None => return false, // Column not present -> does not match.
        };

        if cell.is_tombstone() {
            return false;
        }

        let value = match &cell.value {
            Some(v) => v.as_slice(),
            None => return false,
        };

        let predicate_value = self.predicate.value.as_slice();

        match self.predicate.op {
            FilterOp::Eq => value == predicate_value,
            FilterOp::NotEq => value != predicate_value,
            FilterOp::Lt => value < predicate_value,
            FilterOp::Gt => value > predicate_value,
            FilterOp::LtEq => value <= predicate_value,
            FilterOp::GtEq => value >= predicate_value,
        }
    }
}

impl IndexBuilder for FilteredBuilder {
    fn add_row(
        &mut self,
        partition_key: &[u8],
        clustering_key: &[u8],
        cells: &[(u16, CellValue)],
    ) -> IndexResult<()> {
        if self.matches_predicate(cells) {
            self.inner.add_row(partition_key, clustering_key, cells)
        } else {
            Ok(())
        }
    }

    fn finish(self: Box<Self>) -> IndexResult<IndexFiles> {
        self.inner.finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::btree::BTreeIndexFactory;
    use crate::{IndexCapabilities, IndexKey, IndexType};
    use std::ops::Bound;

    fn make_config(dir: &std::path::Path) -> IndexConfig {
        IndexConfig {
            index_type: IndexType::Filtered {
                predicate: FilterPredicate {
                    column: "status".into(),
                    op: FilterOp::Eq,
                    value: b"active".to_vec(),
                },
                inner: Box::new(IndexType::BTree),
            },
            column_positions: vec![0], // Index column 0 (name)
            output_dir: dir.to_path_buf(),
            sstable_prefix: "sstable-001".into(),
            index_name: "idx_name_filtered".into(),
        }
    }

    fn cell(value: &[u8]) -> CellValue {
        CellValue::live(value.to_vec(), 1000)
    }

    fn tombstone_cell() -> CellValue {
        CellValue::tombstone(1000, 1_700_000_000)
    }

    #[test]
    fn filtered_only_indexes_matching_rows() {
        let dir = tempfile::tempdir().unwrap();
        let config = make_config(dir.path());
        let factory = FilteredIndexFactory::new(
            Box::new(BTreeIndexFactory),
            FilterPredicate {
                column: "status".into(),
                op: FilterOp::Eq,
                value: b"active".to_vec(),
            },
            1, // filter column position (column 1 = status)
        );

        let mut builder = factory.create_builder(&config).unwrap();

        // Row with status=active -> should be indexed
        builder
            .add_row(b"pk1", b"ck1", &[(0, cell(b"Alice")), (1, cell(b"active"))])
            .unwrap();

        // Row with status=inactive -> should be skipped
        builder
            .add_row(b"pk2", b"ck2", &[(0, cell(b"Bob")), (1, cell(b"inactive"))])
            .unwrap();

        // Row with status=active -> should be indexed
        builder
            .add_row(
                b"pk3",
                b"ck3",
                &[(0, cell(b"Charlie")), (1, cell(b"active"))],
            )
            .unwrap();

        let files = builder.finish().unwrap();
        assert_eq!(files.meta.row_count, 2);

        let reader = factory.open_reader(&files).unwrap();

        // Alice should be found
        let results = reader.lookup(&IndexKey::Bytes(b"Alice".to_vec())).unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].partition_key, b"pk1");

        // Bob should NOT be found (filtered out)
        let results = reader.lookup(&IndexKey::Bytes(b"Bob".to_vec())).unwrap();
        assert!(results.is_empty());

        // Charlie should be found
        let results = reader
            .lookup(&IndexKey::Bytes(b"Charlie".to_vec()))
            .unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].partition_key, b"pk3");
    }

    #[test]
    fn filtered_not_eq() {
        let dir = tempfile::tempdir().unwrap();
        let config = make_config(dir.path());
        let factory = FilteredIndexFactory::new(
            Box::new(BTreeIndexFactory),
            FilterPredicate {
                column: "status".into(),
                op: FilterOp::NotEq,
                value: b"deleted".to_vec(),
            },
            1,
        );

        let mut builder = factory.create_builder(&config).unwrap();

        builder
            .add_row(b"pk1", b"ck1", &[(0, cell(b"Alice")), (1, cell(b"active"))])
            .unwrap();
        builder
            .add_row(b"pk2", b"ck2", &[(0, cell(b"Bob")), (1, cell(b"deleted"))])
            .unwrap();

        let files = builder.finish().unwrap();
        assert_eq!(files.meta.row_count, 1);

        let reader = factory.open_reader(&files).unwrap();
        let results = reader.lookup(&IndexKey::Bytes(b"Alice".to_vec())).unwrap();
        assert_eq!(results.len(), 1);
    }

    #[test]
    fn filtered_comparison_ops() {
        let dir = tempfile::tempdir().unwrap();

        // Test Lt: age < 30 (comparing bytes lexicographically)
        let config = make_config(dir.path());
        let factory = FilteredIndexFactory::new(
            Box::new(BTreeIndexFactory),
            FilterPredicate {
                column: "score".into(),
                op: FilterOp::Lt,
                value: b"50".to_vec(),
            },
            1,
        );

        let mut builder = factory.create_builder(&config).unwrap();

        builder
            .add_row(b"pk1", b"ck1", &[(0, cell(b"Alice")), (1, cell(b"30"))])
            .unwrap();
        builder
            .add_row(b"pk2", b"ck2", &[(0, cell(b"Bob")), (1, cell(b"60"))])
            .unwrap();
        builder
            .add_row(b"pk3", b"ck3", &[(0, cell(b"Charlie")), (1, cell(b"50"))])
            .unwrap();

        let files = builder.finish().unwrap();
        // "30" < "50" (yes), "60" < "50" (no), "50" < "50" (no)
        assert_eq!(files.meta.row_count, 1);
    }

    #[test]
    fn filtered_gt_op() {
        let dir = tempfile::tempdir().unwrap();
        let config = make_config(dir.path());
        let factory = FilteredIndexFactory::new(
            Box::new(BTreeIndexFactory),
            FilterPredicate {
                column: "score".into(),
                op: FilterOp::Gt,
                value: b"50".to_vec(),
            },
            1,
        );

        let mut builder = factory.create_builder(&config).unwrap();

        builder
            .add_row(b"pk1", b"ck1", &[(0, cell(b"Alice")), (1, cell(b"30"))])
            .unwrap();
        builder
            .add_row(b"pk2", b"ck2", &[(0, cell(b"Bob")), (1, cell(b"60"))])
            .unwrap();

        let files = builder.finish().unwrap();
        // "30" > "50" (no), "60" > "50" (yes)
        assert_eq!(files.meta.row_count, 1);

        let reader = factory.open_reader(&files).unwrap();
        let results = reader.lookup(&IndexKey::Bytes(b"Bob".to_vec())).unwrap();
        assert_eq!(results.len(), 1);
    }

    #[test]
    fn filtered_lteq_gteq_ops() {
        let dir = tempfile::tempdir().unwrap();

        // LtEq
        let config = make_config(dir.path());
        let factory = FilteredIndexFactory::new(
            Box::new(BTreeIndexFactory),
            FilterPredicate {
                column: "score".into(),
                op: FilterOp::LtEq,
                value: b"50".to_vec(),
            },
            1,
        );

        let mut builder = factory.create_builder(&config).unwrap();
        builder
            .add_row(b"pk1", b"ck1", &[(0, cell(b"Alice")), (1, cell(b"50"))])
            .unwrap();
        builder
            .add_row(b"pk2", b"ck2", &[(0, cell(b"Bob")), (1, cell(b"60"))])
            .unwrap();

        let files = builder.finish().unwrap();
        // "50" <= "50" (yes), "60" <= "50" (no)
        assert_eq!(files.meta.row_count, 1);

        // GtEq
        let dir2 = tempfile::tempdir().unwrap();
        let config2 = IndexConfig {
            output_dir: dir2.path().to_path_buf(),
            ..config
        };
        let factory2 = FilteredIndexFactory::new(
            Box::new(BTreeIndexFactory),
            FilterPredicate {
                column: "score".into(),
                op: FilterOp::GtEq,
                value: b"50".to_vec(),
            },
            1,
        );

        let mut builder2 = factory2.create_builder(&config2).unwrap();
        builder2
            .add_row(b"pk1", b"ck1", &[(0, cell(b"Alice")), (1, cell(b"50"))])
            .unwrap();
        builder2
            .add_row(b"pk2", b"ck2", &[(0, cell(b"Bob")), (1, cell(b"40"))])
            .unwrap();

        let files2 = builder2.finish().unwrap();
        // "50" >= "50" (yes), "40" >= "50" (no)
        assert_eq!(files2.meta.row_count, 1);
    }

    #[test]
    fn filtered_missing_filter_column_skips_row() {
        let dir = tempfile::tempdir().unwrap();
        let config = make_config(dir.path());
        let factory = FilteredIndexFactory::new(
            Box::new(BTreeIndexFactory),
            FilterPredicate {
                column: "status".into(),
                op: FilterOp::Eq,
                value: b"active".to_vec(),
            },
            1,
        );

        let mut builder = factory.create_builder(&config).unwrap();

        // Row missing the filter column (column 1)
        builder
            .add_row(b"pk1", b"ck1", &[(0, cell(b"Alice"))])
            .unwrap();

        let files = builder.finish().unwrap();
        assert_eq!(files.meta.row_count, 0);
    }

    #[test]
    fn filtered_tombstone_filter_column_skips_row() {
        let dir = tempfile::tempdir().unwrap();
        let config = make_config(dir.path());
        let factory = FilteredIndexFactory::new(
            Box::new(BTreeIndexFactory),
            FilterPredicate {
                column: "status".into(),
                op: FilterOp::Eq,
                value: b"active".to_vec(),
            },
            1,
        );

        let mut builder = factory.create_builder(&config).unwrap();

        // Filter column is a tombstone
        builder
            .add_row(
                b"pk1",
                b"ck1",
                &[(0, cell(b"Alice")), (1, tombstone_cell())],
            )
            .unwrap();

        let files = builder.finish().unwrap();
        assert_eq!(files.meta.row_count, 0);
    }

    #[test]
    fn filtered_empty_index() {
        let dir = tempfile::tempdir().unwrap();
        let config = make_config(dir.path());
        let factory = FilteredIndexFactory::new(
            Box::new(BTreeIndexFactory),
            FilterPredicate {
                column: "status".into(),
                op: FilterOp::Eq,
                value: b"active".to_vec(),
            },
            1,
        );

        let builder = factory.create_builder(&config).unwrap();
        let files = builder.finish().unwrap();
        assert_eq!(files.meta.row_count, 0);

        let reader = factory.open_reader(&files).unwrap();
        let results = reader
            .lookup(&IndexKey::Bytes(b"anything".to_vec()))
            .unwrap();
        assert!(results.is_empty());
    }

    #[test]
    fn filtered_preserves_inner_capabilities() {
        let dir = tempfile::tempdir().unwrap();
        let config = make_config(dir.path());
        let factory = FilteredIndexFactory::new(
            Box::new(BTreeIndexFactory),
            FilterPredicate {
                column: "status".into(),
                op: FilterOp::Eq,
                value: b"active".to_vec(),
            },
            1,
        );

        let builder = factory.create_builder(&config).unwrap();
        let files = builder.finish().unwrap();
        let reader = factory.open_reader(&files).unwrap();

        let caps = reader.capabilities();
        // Should have same capabilities as BTreeIndex
        assert!(caps.contains(IndexCapabilities::POINT_LOOKUP));
        assert!(caps.contains(IndexCapabilities::RANGE_SCAN));
        assert!(!caps.contains(IndexCapabilities::NEAREST));
        assert!(!caps.contains(IndexCapabilities::PHONETIC));
    }

    #[test]
    fn filtered_range_scan_works() {
        let dir = tempfile::tempdir().unwrap();
        let config = make_config(dir.path());
        let factory = FilteredIndexFactory::new(
            Box::new(BTreeIndexFactory),
            FilterPredicate {
                column: "status".into(),
                op: FilterOp::Eq,
                value: b"active".to_vec(),
            },
            1,
        );

        let mut builder = factory.create_builder(&config).unwrap();

        builder
            .add_row(b"pk1", b"ck1", &[(0, cell(b"aaa")), (1, cell(b"active"))])
            .unwrap();
        builder
            .add_row(b"pk2", b"ck2", &[(0, cell(b"bbb")), (1, cell(b"inactive"))])
            .unwrap();
        builder
            .add_row(b"pk3", b"ck3", &[(0, cell(b"ccc")), (1, cell(b"active"))])
            .unwrap();

        let files = builder.finish().unwrap();
        let reader = factory.open_reader(&files).unwrap();

        // Range scan should only return active rows
        let results = reader.range(Bound::Unbounded, Bound::Unbounded).unwrap();
        assert_eq!(results.len(), 2);
        assert_eq!(results[0].partition_key, b"pk1");
        assert_eq!(results[1].partition_key, b"pk3");
    }
}
