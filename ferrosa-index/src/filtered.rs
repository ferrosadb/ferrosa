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

// ── Predicate implication (planner soundness) ────────────────────────────────

/// Does a query constraint `(query_op, query_value)` on a column GUARANTEE that
/// every row it selects is also retained by the partial index `predicate`?
///
/// A filtered index physically holds exactly the rows for which
/// [`evaluate_predicate`] returns `true`. The planner may serve a query from
/// that index — treating the filter column as already enforced — ONLY when the
/// query's value-set on the filter column is a guaranteed **subset** of the
/// index's retained set. Otherwise the index is missing rows the query needs
/// and the result would be silently incomplete.
///
/// This function returns `true` only when subset containment is *provable* from
/// the byte ordering, and withholds (`false`) in every ambiguous case — it is
/// SOUND, never optimistic. All comparisons use the same byte ordering as
/// [`evaluate_predicate`], so the implication is reasoned in exactly the space
/// the index was built in (the query literal and the predicate value both come
/// from the same storage encoding).
///
/// Examples (with value-order encodings such as ascending integers):
/// - query `age = 30` implies predicate `age > 21` (the single value 30 is `> 21`)
/// - query `age > 25` implies predicate `age > 21` (`{x>25} ⊆ {x>21}` since `25 >= 21`)
/// - query `age >= 21` implies predicate `age > 20` (`{x>=21} ⊆ {x>20}` since `21 > 20`)
/// - query `age = 18` does NOT imply `age > 21` (18 is not `> 21`) → withheld
/// - query `age > 10` does NOT imply `age > 21` (selects values below 21) → withheld
pub fn query_constraint_implies_predicate(
    query_op: FilterOp,
    query_value: &[u8],
    predicate: &FilterPredicate,
) -> bool {
    let qv = query_value;
    let pv = predicate.value.as_slice();
    match predicate.op {
        // Predicate retains a single value: only an identical Eq query is a subset.
        FilterOp::Eq => query_op == FilterOp::Eq && qv == pv,
        // Predicate retains everything except `pv`: the query's set must exclude `pv`.
        FilterOp::NotEq => query_excludes_value(query_op, qv, pv),
        // Predicate retains `{x < pv}`.
        FilterOp::Lt => match query_op {
            FilterOp::Eq => qv < pv,
            FilterOp::Lt => qv <= pv,
            FilterOp::LtEq => qv < pv,
            _ => false,
        },
        // Predicate retains `{x <= pv}`.
        FilterOp::LtEq => match query_op {
            FilterOp::Eq | FilterOp::Lt | FilterOp::LtEq => qv <= pv,
            _ => false,
        },
        // Predicate retains `{x > pv}`.
        FilterOp::Gt => match query_op {
            FilterOp::Eq => qv > pv,
            FilterOp::Gt => qv >= pv,
            FilterOp::GtEq => qv > pv,
            _ => false,
        },
        // Predicate retains `{x >= pv}`.
        FilterOp::GtEq => match query_op {
            FilterOp::Eq | FilterOp::Gt | FilterOp::GtEq => qv >= pv,
            _ => false,
        },
    }
}

/// Does the query constraint's value-set provably EXCLUDE the single value
/// `excluded`? Used to prove `Q ⊆ {x != excluded}`. Sound: only `true` when
/// containment is provable from the byte ordering.
fn query_excludes_value(query_op: FilterOp, query_value: &[u8], excluded: &[u8]) -> bool {
    match query_op {
        FilterOp::Eq => query_value != excluded,
        // `{x != qv}` excludes `excluded` only when `qv == excluded` (then the
        // two sets are identical); otherwise `excluded` is selected by the query.
        FilterOp::NotEq => query_value == excluded,
        // `{x < qv}` excludes `excluded` iff `excluded >= qv`.
        FilterOp::Lt => excluded >= query_value,
        // `{x <= qv}` excludes `excluded` iff `excluded > qv`.
        FilterOp::LtEq => excluded > query_value,
        // `{x > qv}` excludes `excluded` iff `excluded <= qv`.
        FilterOp::Gt => excluded <= query_value,
        // `{x >= qv}` excludes `excluded` iff `excluded < qv`.
        FilterOp::GtEq => excluded < query_value,
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

    /// Helper: build a predicate over column 0 with a single-byte value so the
    /// byte ordering matches the intended numeric ordering (`b"a" < b"b"`).
    fn pred(op: FilterOp, value: &[u8]) -> FilterPredicate {
        FilterPredicate {
            column_position: 0,
            op,
            value: value.to_vec(),
        }
    }

    /// Soundness + completeness enumeration for the planner implication helper.
    ///
    /// Each case asserts BOTH directions: implications that MUST hold (true
    /// positives — the query's value-set is a provable subset of the index's
    /// retained set) and implications that MUST be withheld (the query could
    /// select a row outside the index, so using it would be incomplete).
    ///
    /// To make the byte ordering coincide with the intended value ordering we
    /// use single ASCII bytes where `b"1" < b"2" < ... < b"9"`, standing in for
    /// the `age` examples in the feature spec.
    #[test]
    fn query_constraint_implies_predicate_enumeration() {
        // For every byte-comparison case below, also cross-check against
        // `evaluate_predicate`: whenever the helper says the query implies the
        // predicate, EVERY value the query could select must in fact satisfy the
        // predicate (no false positive). We probe a small value alphabet.
        let alphabet: &[&[u8]] = &[b"0", b"2", b"4", b"6", b"8", b"9"];

        // (query_op, query_value, predicate, expected)
        let cases: &[(FilterOp, &[u8], FilterPredicate, bool)] = &[
            // ── Eq predicate (single retained value) ──────────────────────────
            (FilterOp::Eq, b"4", pred(FilterOp::Eq, b"4"), true),
            (FilterOp::Eq, b"5", pred(FilterOp::Eq, b"4"), false),
            (FilterOp::Gt, b"4", pred(FilterOp::Eq, b"4"), false),
            // ── Gt predicate (spec: WHERE age = 30 implies age > 21) ──────────
            (FilterOp::Eq, b"6", pred(FilterOp::Gt, b"2"), true), // age=6 implies age>2
            (FilterOp::Eq, b"2", pred(FilterOp::Gt, b"2"), false), // boundary: 2 is not > 2
            (FilterOp::Eq, b"1", pred(FilterOp::Gt, b"2"), false),
            (FilterOp::Gt, b"4", pred(FilterOp::Gt, b"2"), true), // age>4 implies age>2 (4>=2)
            (FilterOp::Gt, b"2", pred(FilterOp::Gt, b"2"), true), // same set
            (FilterOp::Gt, b"1", pred(FilterOp::Gt, b"2"), false), // selects 2-and-below region
            (FilterOp::GtEq, b"3", pred(FilterOp::Gt, b"2"), true), // age>=3 implies age>2 (3>2)
            (FilterOp::GtEq, b"2", pred(FilterOp::Gt, b"2"), false), // includes 2, not > 2
            (FilterOp::Lt, b"9", pred(FilterOp::Gt, b"2"), false), // unbounded below
            // ── GtEq predicate (spec: WHERE age >= 21 implies age > 20) ───────
            (FilterOp::GtEq, b"4", pred(FilterOp::GtEq, b"2"), true),
            (FilterOp::GtEq, b"2", pred(FilterOp::GtEq, b"2"), true),
            (FilterOp::GtEq, b"1", pred(FilterOp::GtEq, b"2"), false),
            (FilterOp::Gt, b"2", pred(FilterOp::GtEq, b"2"), true), // {x>2} ⊆ {x>=2}
            (FilterOp::Eq, b"2", pred(FilterOp::GtEq, b"2"), true),
            (FilterOp::Eq, b"1", pred(FilterOp::GtEq, b"2"), false),
            // ── Lt predicate (mirror of Gt) ──────────────────────────────────
            (FilterOp::Eq, b"2", pred(FilterOp::Lt, b"6"), true),
            (FilterOp::Eq, b"6", pred(FilterOp::Lt, b"6"), false),
            (FilterOp::Lt, b"4", pred(FilterOp::Lt, b"6"), true), // 4<=6
            (FilterOp::Lt, b"6", pred(FilterOp::Lt, b"6"), true), // same set
            (FilterOp::Lt, b"8", pred(FilterOp::Lt, b"6"), false),
            (FilterOp::LtEq, b"4", pred(FilterOp::Lt, b"6"), true), // 4<6
            (FilterOp::LtEq, b"6", pred(FilterOp::Lt, b"6"), false), // includes 6
            (FilterOp::Gt, b"0", pred(FilterOp::Lt, b"6"), false),  // unbounded above
            // ── LtEq predicate (mirror of GtEq) ──────────────────────────────
            (FilterOp::LtEq, b"4", pred(FilterOp::LtEq, b"6"), true),
            (FilterOp::LtEq, b"6", pred(FilterOp::LtEq, b"6"), true),
            (FilterOp::LtEq, b"8", pred(FilterOp::LtEq, b"6"), false),
            (FilterOp::Lt, b"6", pred(FilterOp::LtEq, b"6"), true), // {x<6} ⊆ {x<=6}
            (FilterOp::Eq, b"6", pred(FilterOp::LtEq, b"6"), true),
            // ── NotEq predicate (retains all but one value) ──────────────────
            (FilterOp::Eq, b"4", pred(FilterOp::NotEq, b"6"), true), // 4 != 6
            (FilterOp::Eq, b"6", pred(FilterOp::NotEq, b"6"), false), // selects the excluded value
            (FilterOp::Lt, b"6", pred(FilterOp::NotEq, b"6"), true), // {x<6} excludes 6
            (FilterOp::Lt, b"8", pred(FilterOp::NotEq, b"6"), false), // {x<8} includes 6
            (FilterOp::Gt, b"6", pred(FilterOp::NotEq, b"6"), true), // {x>6} excludes 6
            (FilterOp::GtEq, b"6", pred(FilterOp::NotEq, b"6"), false), // includes 6
            (FilterOp::NotEq, b"6", pred(FilterOp::NotEq, b"6"), true), // identical set
            (FilterOp::NotEq, b"4", pred(FilterOp::NotEq, b"6"), false), // {x!=4} includes 6
        ];

        for (q_op, q_val, predicate, expected) in cases {
            let got = query_constraint_implies_predicate(*q_op, q_val, predicate);
            assert_eq!(
                got, *expected,
                "implication {q_op:?} {q_val:?} => predicate {:?} {:?}: expected {expected}, got {got}",
                predicate.op, predicate.value
            );

            // Cross-check: a claimed implication must never admit a query value
            // that fails the predicate (no silent incompleteness). We verify
            // every value in our alphabet that the QUERY would select also
            // satisfies the index PREDICATE.
            if got {
                for v in alphabet {
                    let q = pred(*q_op, q_val);
                    if evaluate_predicate(&q, v) {
                        assert!(
                            evaluate_predicate(predicate, v),
                            "UNSOUND: query {q_op:?} {q_val:?} selects {v:?} which is NOT retained by predicate {:?} {:?}",
                            predicate.op, predicate.value
                        );
                    }
                }
            }
        }
    }

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
