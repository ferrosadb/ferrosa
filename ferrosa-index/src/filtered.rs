//! Filtered index wrapper.
//!
//! [`FilteredIndexFactory`] wraps any [`IndexFactory`] and applies a
//! [`FilterPredicate`] during index build. Only rows whose filter column
//! value satisfies the predicate are passed to the inner index builder.
//! At read time, the wrapper delegates directly to the inner reader.

use ferrosa_common::CellValue;

use crate::{
    FilterClause, FilterOp, FilterPredicate, IndexBuilder, IndexCapabilities, IndexConfig,
    IndexFactory, IndexFiles, IndexReader, IndexResult, IndexType,
};

// ── Predicate evaluation ─────────────────────────────────────────────────────

/// Evaluate one filter clause's operator against a single cell value (raw
/// bytes). All comparisons use byte ordering — the same ordering the index was
/// built in (the clause `value` and the cell both come from one storage
/// encoding).
pub fn evaluate_clause(clause: &FilterClause, cell_value: &[u8]) -> bool {
    match clause.op {
        FilterOp::Eq => cell_value == clause.value.as_slice(),
        FilterOp::NotEq => cell_value != clause.value.as_slice(),
        FilterOp::Lt => cell_value < clause.value.as_slice(),
        FilterOp::Gt => cell_value > clause.value.as_slice(),
        FilterOp::LtEq => cell_value <= clause.value.as_slice(),
        FilterOp::GtEq => cell_value >= clause.value.as_slice(),
    }
}

/// Evaluate a filter predicate against a SINGLE cell value, treating every
/// clause as applying to that one value (the single-column case). A conjunction
/// over distinct columns must instead use [`evaluate_predicate_row`].
///
/// An empty conjunction retains nothing (`false`): a partial index with no
/// clause would otherwise silently index every row.
///
/// This is the byte-level evaluator the planner soundness cross-check reasons
/// against. The build/memtable gating uses [`evaluate_predicate_row`], which
/// resolves each clause's own column.
pub fn evaluate_predicate(predicate: &FilterPredicate, cell_value: &[u8]) -> bool {
    !predicate.clauses.is_empty()
        && predicate
            .clauses
            .iter()
            .all(|clause| evaluate_clause(clause, cell_value))
}

/// Evaluate a (possibly multi-column) conjunction predicate against a row,
/// resolving EACH clause's own column via `lookup`. A row is retained only when
/// every clause's column is present (live cell) AND its value satisfies the
/// clause. A missing or tombstoned clause column fails the whole conjunction
/// (the row does not belong in the partial index).
///
/// This is the single source of truth for partial-index gating: the storage
/// build path (`LocalBackend::build`) and the memtable write path both call it,
/// so a sidecar and its memtable companion agree on exactly which rows belong.
///
/// An empty conjunction retains nothing (`false`).
pub fn evaluate_predicate_row<'a, F>(predicate: &FilterPredicate, mut lookup: F) -> bool
where
    F: FnMut(usize) -> Option<&'a [u8]>,
{
    !predicate.clauses.is_empty()
        && predicate.clauses.iter().all(|clause| {
            lookup(clause.column_position).is_some_and(|v| evaluate_clause(clause, v))
        })
}

// ── Predicate implication (planner soundness) ────────────────────────────────

/// Does a query constraint `(query_op, query_value)` GUARANTEE that every value
/// it selects also satisfies a single `clause`? Subset containment in the byte
/// ordering: `{x : query} ⊆ {x : clause}`.
///
/// Returns `true` only when containment is *provable* from the byte ordering and
/// withholds (`false`) in every ambiguous case — SOUND, never optimistic. All
/// comparisons use the same byte ordering as [`evaluate_clause`].
///
/// Examples (with value-order encodings such as ascending integers):
/// - query `age = 30` implies clause `age > 21` (the single value 30 is `> 21`)
/// - query `age > 25` implies clause `age > 21` (`{x>25} ⊆ {x>21}` since `25 >= 21`)
/// - query `age >= 21` implies clause `age > 20` (`{x>=21} ⊆ {x>20}` since `21 > 20`)
/// - query `age = 18` does NOT imply `age > 21` → withheld
/// - query `age > 10` does NOT imply `age > 21` → withheld
pub fn query_clause_implies(query_op: FilterOp, query_value: &[u8], clause: &FilterClause) -> bool {
    let qv = query_value;
    let pv = clause.value.as_slice();
    match clause.op {
        // Clause retains a single value: only an identical Eq query is a subset.
        FilterOp::Eq => query_op == FilterOp::Eq && qv == pv,
        // Clause retains everything except `pv`: the query's set must exclude `pv`.
        FilterOp::NotEq => query_excludes_value(query_op, qv, pv),
        // Clause retains `{x < pv}`.
        FilterOp::Lt => match query_op {
            FilterOp::Eq => qv < pv,
            FilterOp::Lt => qv <= pv,
            FilterOp::LtEq => qv < pv,
            _ => false,
        },
        // Clause retains `{x <= pv}`.
        FilterOp::LtEq => match query_op {
            FilterOp::Eq | FilterOp::Lt | FilterOp::LtEq => qv <= pv,
            _ => false,
        },
        // Clause retains `{x > pv}`.
        FilterOp::Gt => match query_op {
            FilterOp::Eq => qv > pv,
            FilterOp::Gt => qv >= pv,
            FilterOp::GtEq => qv > pv,
            _ => false,
        },
        // Clause retains `{x >= pv}`.
        FilterOp::GtEq => match query_op {
            FilterOp::Eq | FilterOp::Gt | FilterOp::GtEq => qv >= pv,
            _ => false,
        },
    }
}

/// Single-column compatibility shim: does the query constraint imply EVERY
/// clause of `predicate`, all evaluated against the same `query_value`? Only
/// meaningful when every clause is on one column. Multi-column conjunctions must
/// use [`query_constraint_implies_predicate_clause`] per clause with the query
/// constraint on that clause's own column.
///
/// A filtered index physically holds exactly the rows for which
/// [`evaluate_predicate`] returns `true`. The planner may serve a query from it
/// ONLY when the query's value-set is a provable subset of the index's retained
/// set; withholds otherwise.
pub fn query_constraint_implies_predicate(
    query_op: FilterOp,
    query_value: &[u8],
    predicate: &FilterPredicate,
) -> bool {
    !predicate.clauses.is_empty()
        && predicate
            .clauses
            .iter()
            .all(|clause| query_clause_implies(query_op, query_value, clause))
}

/// Per-clause implication for the multi-column planner: does the query
/// constraint on this clause's column imply the clause? A thin alias of
/// [`query_clause_implies`] kept as the planner-facing name.
pub fn query_constraint_implies_predicate_clause(
    query_op: FilterOp,
    query_value: &[u8],
    clause: &FilterClause,
) -> bool {
    query_clause_implies(query_op, query_value, clause)
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
        // Conjunction gating: a row belongs in the partial index only when EVERY
        // clause's column is present (live) and satisfies its comparison. A
        // missing/tombstoned clause column fails the whole conjunction. The
        // closure resolves each clause's own column position into `cells`.
        let matches = evaluate_predicate_row(&self.predicate, |pos| {
            cells.get(pos).and_then(|c| c.value.as_deref())
        });
        if matches {
            self.inner
                .add_row(partition_key, clustering_key, cells, column_positions)
        } else {
            Ok(())
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
        FilterPredicate::single(0, op, value.to_vec())
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
                got,
                *expected,
                "implication {q_op:?} {q_val:?} => predicate {:?}: expected {expected}, got {got}",
                predicate.clauses()
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
                            "UNSOUND: query {q_op:?} {q_val:?} selects {v:?} which is NOT retained by predicate {:?}",
                            predicate.clauses()
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

        let predicate = FilterPredicate::single(1, FilterOp::Eq, b"active".to_vec());

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

        let predicate = FilterPredicate::single(1, FilterOp::NotEq, b"deleted".to_vec());

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
        let pred_lt = FilterPredicate::single(0, FilterOp::Lt, b"M".to_vec());
        assert!(evaluate_predicate(&pred_lt, b"A"));
        assert!(!evaluate_predicate(&pred_lt, b"M"));
        assert!(!evaluate_predicate(&pred_lt, b"Z"));

        let pred_gt = FilterPredicate::single(0, FilterOp::Gt, b"M".to_vec());
        assert!(!evaluate_predicate(&pred_gt, b"A"));
        assert!(!evaluate_predicate(&pred_gt, b"M"));
        assert!(evaluate_predicate(&pred_gt, b"Z"));

        let pred_lteq = FilterPredicate::single(0, FilterOp::LtEq, b"M".to_vec());
        assert!(evaluate_predicate(&pred_lteq, b"A"));
        assert!(evaluate_predicate(&pred_lteq, b"M"));
        assert!(!evaluate_predicate(&pred_lteq, b"Z"));

        let pred_gteq = FilterPredicate::single(0, FilterOp::GtEq, b"M".to_vec());
        assert!(!evaluate_predicate(&pred_gteq, b"A"));
        assert!(evaluate_predicate(&pred_gteq, b"M"));
        assert!(evaluate_predicate(&pred_gteq, b"Z"));
    }

    #[test]
    fn filtered_index_tombstone_skipped() {
        let dir = tempfile::tempdir().unwrap();

        let inner = Box::new(PhoneticIndexFactory::new(PhoneticAlgorithm::Soundex));
        let predicate = FilterPredicate::single(1, FilterOp::Eq, b"active".to_vec());

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
        let predicate = FilterPredicate::single(1, FilterOp::Eq, b"x".to_vec());

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
        let predicate = FilterPredicate::single(1, FilterOp::Eq, b"active".to_vec());

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

    // ── Multi-column conjunction predicates ──────────────────────────────────

    /// A 2-clause conjunction (`age > '2' AND dept = 'eng'`, on columns 1 and 2)
    /// retains a row only when BOTH clauses hold. `evaluate_predicate_row`
    /// resolves each clause's own column. A row missing either column fails.
    #[test]
    fn evaluate_predicate_row_conjunction_all_clauses() {
        let predicate = FilterPredicate::conjunction(vec![
            FilterClause::new(1, FilterOp::Gt, b"2".to_vec()),
            FilterClause::new(2, FilterOp::Eq, b"eng".to_vec()),
        ]);

        // Both clauses satisfied -> retained.
        let cells = [(1usize, b"6".as_slice()), (2usize, b"eng".as_slice())];
        let lookup = |pos: usize| cells.iter().find(|(p, _)| *p == pos).map(|(_, v)| *v);
        assert!(evaluate_predicate_row(&predicate, lookup));

        // First clause fails (age not > 2) -> withheld.
        let cells = [(1usize, b"2".as_slice()), (2usize, b"eng".as_slice())];
        let lookup = |pos: usize| cells.iter().find(|(p, _)| *p == pos).map(|(_, v)| *v);
        assert!(!evaluate_predicate_row(&predicate, lookup));

        // Second clause fails (dept != eng) -> withheld.
        let cells = [(1usize, b"6".as_slice()), (2usize, b"sales".as_slice())];
        let lookup = |pos: usize| cells.iter().find(|(p, _)| *p == pos).map(|(_, v)| *v);
        assert!(!evaluate_predicate_row(&predicate, lookup));

        // Second clause column absent -> withheld (conjunction requires every
        // clause column to be present and live).
        let cells = [(1usize, b"6".as_slice())];
        let lookup = |pos: usize| cells.iter().find(|(p, _)| *p == pos).map(|(_, v)| *v);
        assert!(!evaluate_predicate_row(&predicate, lookup));
    }

    /// An empty conjunction retains nothing — a partial index with no clause
    /// would otherwise silently index every row (unsound).
    #[test]
    fn empty_conjunction_retains_nothing() {
        let predicate = FilterPredicate::conjunction(vec![]);
        let lookup = |_pos: usize| Some(b"anything".as_slice());
        assert!(!evaluate_predicate_row(&predicate, lookup));
        assert!(!evaluate_predicate(&predicate, b"anything"));
    }

    /// Planner soundness + completeness for a 2-clause conjunction. The index is
    /// usable ONLY when the query implies EVERY clause; if even one clause is
    /// not provably implied the index must be withheld (serving it would drop
    /// rows). We model the query as a per-column constraint map and require each
    /// clause to be implied by the query constraint on that clause's column.
    #[test]
    fn conjunction_implication_requires_every_clause() {
        // Index predicate: age (col 1) > '2'  AND  dept (col 2) = 'eng'.
        let predicate = FilterPredicate::conjunction(vec![
            FilterClause::new(1, FilterOp::Gt, b"2".to_vec()),
            FilterClause::new(2, FilterOp::Eq, b"eng".to_vec()),
        ]);

        // Helper: prove the conjunction is implied by a set of query constraints
        // (column_position -> (op, value)). Each clause must be implied by the
        // query constraint on its own column (withhold if any column is
        // unconstrained or not provably contained).
        let implies = |constraints: &[(usize, FilterOp, &[u8])]| -> bool {
            predicate.clauses().iter().all(|clause| {
                constraints.iter().any(|(pos, op, val)| {
                    *pos == clause.column_position
                        && query_constraint_implies_predicate_clause(*op, val, clause)
                })
            })
        };

        // Query: age = '6' AND dept = 'eng' -> implies BOTH clauses -> usable.
        assert!(implies(&[
            (1, FilterOp::Eq, b"6"),
            (2, FilterOp::Eq, b"eng"),
        ]));
        // Query: age > '4' AND dept = 'eng' -> {x>4} ⊆ {x>2} and dept implied.
        assert!(implies(&[
            (1, FilterOp::Gt, b"4"),
            (2, FilterOp::Eq, b"eng"),
        ]));

        // Withheld: only the age clause is implied; dept is unconstrained.
        assert!(!implies(&[(1, FilterOp::Eq, b"6")]));
        // Withheld: dept is implied but age clause is NOT (age = '2' is not > 2).
        assert!(!implies(&[
            (1, FilterOp::Eq, b"2"),
            (2, FilterOp::Eq, b"eng"),
        ]));
        // Withheld: dept constrained to a different value (not a subset of 'eng').
        assert!(!implies(&[
            (1, FilterOp::Eq, b"6"),
            (2, FilterOp::Eq, b"sales"),
        ]));
        // Withheld: nothing constrained.
        assert!(!implies(&[]));
    }

    /// The conjunction build path: only rows where EVERY clause passes reach the
    /// inner index. Columns: 0 = name (indexed), 1 = age, 2 = dept; predicate is
    /// `age > '2' AND dept = 'eng'`.
    #[test]
    fn conjunction_build_includes_only_rows_matching_all_clauses() {
        use crate::btree::BTreeIndexFactory;

        let dir = tempfile::tempdir().unwrap();
        let predicate = FilterPredicate::conjunction(vec![
            FilterClause::new(1, FilterOp::Gt, b"2".to_vec()),
            FilterClause::new(2, FilterOp::Eq, b"eng".to_vec()),
        ]);
        let factory = FilteredIndexFactory::new(Box::new(BTreeIndexFactory), predicate);
        let config = IndexConfig {
            index_type: IndexType::Filtered,
            column_positions: vec![0],
            output_dir: dir.path().to_path_buf(),
            name: "test_conjunction".to_string(),
        };
        let mut builder = factory.create_builder(&config).unwrap();

        let row = |age: &[u8], dept: &[u8], v: i64| {
            vec![
                CellValue::live(b"alpha".to_vec(), v),
                CellValue::live(age.to_vec(), v),
                CellValue::live(dept.to_vec(), v),
            ]
        };
        // pk1: age 6, eng -> BOTH pass -> indexed.
        builder
            .add_row(b"pk1", b"", &row(b"6", b"eng", 1), &[0])
            .unwrap();
        // pk2: age 6, sales -> dept fails -> excluded.
        builder
            .add_row(b"pk2", b"", &row(b"6", b"sales", 2), &[0])
            .unwrap();
        // pk3: age 1, eng -> age fails -> excluded.
        builder
            .add_row(b"pk3", b"", &row(b"1", b"eng", 3), &[0])
            .unwrap();
        // pk4: age 9, eng -> BOTH pass -> indexed.
        builder
            .add_row(b"pk4", b"", &row(b"9", b"eng", 4), &[0])
            .unwrap();

        let files = builder.finish().unwrap();
        let reader = factory.open_reader(&files).unwrap();
        let results = reader.lookup(&IndexKey(b"alpha".to_vec())).unwrap();
        let pks: Vec<&[u8]> = results.iter().map(|r| r.partition_key.as_slice()).collect();
        assert_eq!(pks.len(), 2, "only rows passing BOTH clauses are indexed");
        assert!(pks.contains(&b"pk1".as_slice()));
        assert!(pks.contains(&b"pk4".as_slice()));
        assert!(!pks.contains(&b"pk2".as_slice()));
        assert!(!pks.contains(&b"pk3".as_slice()));
    }

    /// Persistence round-trip of the conjunction predicate via the option-string
    /// helpers (the shape persisted under `__filter_predicate`).
    #[test]
    fn conjunction_predicate_option_string_roundtrip() {
        let predicate = FilterPredicate::conjunction(vec![
            FilterClause::new(1, FilterOp::Gt, vec![0, 0, 0, 21]),
            FilterClause::new(2, FilterOp::Eq, b"eng".to_vec()),
        ]);
        let s = predicate.to_option_string().unwrap();
        let back = FilterPredicate::from_option_string(&s).unwrap();
        assert_eq!(back, predicate);
        assert_eq!(back.clauses().len(), 2);
        assert_eq!(back.version, 2);
    }

    /// Backward-compatible decode: the LEGACY single-clause flat JSON (the exact
    /// shape the original `FilterPredicate` serialized) still deserializes into a
    /// one-clause conjunction. This guarantees old `system_schema.indexes` rows
    /// and in-flight build requests survive the upgrade.
    #[test]
    fn legacy_single_clause_json_decodes() {
        let legacy = r#"{"column_position":2,"op":"Eq","value":[97,99,116,105,118,101]}"#;
        let back = FilterPredicate::from_option_string(legacy).unwrap();
        assert_eq!(back.clauses().len(), 1);
        let clause = &back.clauses()[0];
        assert_eq!(clause.column_position, 2);
        assert_eq!(clause.op, FilterOp::Eq);
        assert_eq!(clause.value, b"active");
        // It evaluates exactly like a single-clause predicate.
        assert!(evaluate_clause(clause, b"active"));
        assert!(!evaluate_clause(clause, b"inactive"));
    }

    /// Malformed JSON (neither shape) fails to decode rather than silently
    /// producing an empty/everything-retaining predicate.
    #[test]
    fn malformed_predicate_json_is_rejected() {
        assert!(FilterPredicate::from_option_string(r#"{"foo":1}"#).is_none());
    }
}
