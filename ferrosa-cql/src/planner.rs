//! Query planner for SELECT statements.
//!
//! Chooses the optimal execution plan based on WHERE predicates,
//! partition key columns, and available secondary indexes.

use std::fmt;

use crate::ast::{ComparisonOp, WhereClause};

/// Execution plan for a SELECT query, chosen by the planner.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ScanPlan {
    /// All partition key columns have `=` predicates.
    PartitionKeyLookup,

    /// One WHERE column matches a secondary index with `=` predicate.
    /// All WHERE columns are covered by indexes.
    SingleIndex {
        index_name: String,
        index_column: String,
    },

    /// One WHERE column matches a secondary index, but other WHERE columns
    /// are not indexed. Use read_by_index() + post-filter remaining predicates.
    IndexScanWithFilter {
        index_name: String,
        index_column: String,
        filter_columns: Vec<String>,
    },

    /// Two or more WHERE columns each match a different index with `=`.
    /// Fetch from each index, intersect RowPosition sets, fetch only intersection.
    IndexIntersection {
        indexes: Vec<(String, String)>, // (index_name, index_column) pairs
    },

    /// `ORDER BY col ANN OF [...] LIMIT k` against a registered vector index.
    /// The router consults the vector index for the k nearest rows instead of
    /// full-scanning the table and post-filtering.
    VectorAnn {
        index_name: String,
        index_column: String,
    },

    /// A geospatial query (`GEO_NEAREST OF`, `GEO_WITHIN_RADIUS`,
    /// `GEO_WITHIN_BBOX`) served by a geo cell-id index. The geo predicate is a
    /// function over the indexed column, not a scalar `=`, so it does not flow
    /// through the generic single-/multi-index matching above; the router builds
    /// this variant directly so EXPLAIN reports the geo index rather than a
    /// FullScan. `op` is the geo operation name (`GeoNearest` / `GeoWithinRadius`
    /// / `GeoWithinBbox`).
    GeoIndex {
        index_name: String,
        index_column: String,
        op: String,
    },

    /// A full-text query (`WHERE col = fts_match('...')`) served by a registered
    /// full-text index. The FTS predicate is a function over the indexed column,
    /// not a scalar `=`, so it does not flow through the generic single-/multi-
    /// index matching above; the router builds this variant directly so EXPLAIN
    /// reports the full-text index rather than a FullScan.
    FullTextIndex {
        index_name: String,
        index_column: String,
    },

    /// No indexes match. Requires ALLOW FILTERING.
    FullScan,
}

impl fmt::Display for ScanPlan {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ScanPlan::PartitionKeyLookup => write!(f, "PartitionKeyLookup"),
            ScanPlan::SingleIndex {
                index_name,
                index_column,
            } => write!(f, "SingleIndex({index_name} on {index_column})"),
            ScanPlan::IndexScanWithFilter {
                index_name,
                index_column,
                filter_columns,
            } => {
                let cols = filter_columns.join(", ");
                write!(
                    f,
                    "IndexScanWithFilter({index_name} on {index_column}, filter: [{cols}])"
                )
            }
            ScanPlan::IndexIntersection { indexes } => {
                let pairs: Vec<String> =
                    indexes.iter().map(|(n, c)| format!("{n} on {c}")).collect();
                write!(f, "IndexIntersection({})", pairs.join(", "))
            }
            ScanPlan::VectorAnn {
                index_name,
                index_column,
            } => write!(f, "VectorAnn({index_name} on {index_column})"),
            ScanPlan::GeoIndex {
                index_name,
                index_column,
                op,
            } => write!(f, "GeoIndex({index_name} on {index_column}, {op})"),
            ScanPlan::FullTextIndex {
                index_name,
                index_column,
            } => write!(f, "FullTextIndex({index_name} on {index_column})"),
            ScanPlan::FullScan => write!(f, "FullScan"),
        }
    }
}

/// Choose an execution plan for a SELECT query.
///
/// # Parameters
/// - `where_clauses`: predicates from the WHERE clause
/// - `pk_columns`: ordered list of partition key column names
/// - `indexes`: available secondary indexes as `(index_name, indexed_columns)` pairs
///
/// # Decision order
/// 1. All PK columns have `Eq` predicates → `PartitionKeyLookup`
/// 2. No WHERE clauses → `FullScan`
/// 3. Collect all WHERE columns with `Eq` that match a single-column index:
///    - 0 matches → `FullScan`
///    - 1 match and no unindexed WHERE columns → `SingleIndex`
///    - 1 match with unindexed WHERE columns → `IndexScanWithFilter`
///    - 2+ matches → `IndexIntersection` (all matched indexes)
/// 4. No usable index → `FullScan`
pub fn plan(
    where_clauses: &[WhereClause],
    pk_columns: &[String],
    indexes: &[(String, Vec<String>)],
) -> ScanPlan {
    plan_with_covered(where_clauses, pk_columns, indexes, &[])
}

/// Like [`plan`], but treats `extra_covered` columns as already satisfied by an
/// index even though they are not in `indexes`.
///
/// This is how a partial (Filtered) index makes its *filter* column count as
/// covered: when the index is selected, the predicate on the filter column is
/// enforced by the index itself (the sidecar holds only matching rows), so that
/// WHERE predicate must NOT push the plan to `IndexScanWithFilter` / require
/// ALLOW FILTERING. The router only passes a filter column here when the
/// corresponding filtered index is implied by the query and thus offered in
/// `indexes` (see `filtered_index_is_usable`), so coverage stays sound.
pub fn plan_with_covered(
    where_clauses: &[WhereClause],
    pk_columns: &[String],
    indexes: &[(String, Vec<String>)],
    extra_covered: &[String],
) -> ScanPlan {
    // Exclude token() predicates — they represent token-range scans,
    // not column-level filters.
    let non_token: Vec<&WhereClause> = where_clauses.iter().filter(|wc| !wc.token_fn).collect();

    if pk_columns_satisfied(&non_token, pk_columns) {
        return ScanPlan::PartitionKeyLookup;
    }

    if non_token.is_empty() {
        return ScanPlan::FullScan;
    }

    let is_covered = |col: &str| {
        indexes
            .iter()
            .any(|(_, cols)| cols.len() == 1 && cols[0] == col)
            || extra_covered.iter().any(|c| c == col)
    };

    // Collect all WHERE columns with Eq that have a matching single-column index.
    let matched: Vec<(String, String)> = non_token
        .iter()
        .filter(|wc| wc.op == ComparisonOp::Eq)
        .filter_map(|wc| {
            indexes
                .iter()
                .find(|(_, cols)| cols.len() == 1 && cols[0] == wc.column)
                .map(|(name, _)| (name.clone(), wc.column.clone()))
        })
        .collect();

    match matched.len() {
        0 => ScanPlan::FullScan,
        1 => {
            let (index_name, index_column) = matched.into_iter().next().unwrap();
            // Collect WHERE columns not covered by any index (or extra coverage).
            let filter_columns: Vec<String> = non_token
                .iter()
                .filter(|wc| wc.column != index_column)
                .filter(|wc| !is_covered(&wc.column))
                .map(|wc| wc.column.clone())
                .collect();
            if filter_columns.is_empty() {
                ScanPlan::SingleIndex {
                    index_name,
                    index_column,
                }
            } else {
                ScanPlan::IndexScanWithFilter {
                    index_name,
                    index_column,
                    filter_columns,
                }
            }
        }
        _ => ScanPlan::IndexIntersection { indexes: matched },
    }
}

fn pk_columns_satisfied(where_clauses: &[&WhereClause], pk_columns: &[String]) -> bool {
    pk_columns.iter().all(|pk_col| {
        where_clauses
            .iter()
            .any(|wc| wc.column == *pk_col && wc.op == ComparisonOp::Eq)
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ast::{ComparisonOp, Term, WhereClause};

    fn wc(col: &str, op: ComparisonOp) -> WhereClause {
        WhereClause {
            column: col.to_string(),
            op,
            value: Term::StringLiteral("dummy".to_string()),
            token_fn: false,
        }
    }

    fn pk(names: &[&str]) -> Vec<String> {
        names.iter().map(|s| s.to_string()).collect()
    }

    fn idx(name: &str, columns: &[&str]) -> (String, Vec<String>) {
        (
            name.to_string(),
            columns.iter().map(|s| s.to_string()).collect(),
        )
    }

    #[test]
    fn pk_lookup_single_column() {
        let plan = plan(&[wc("id", ComparisonOp::Eq)], &pk(&["id"]), &[]);
        assert_eq!(plan, ScanPlan::PartitionKeyLookup);
    }

    #[test]
    fn pk_lookup_composite() {
        let plan = plan(
            &[wc("ks", ComparisonOp::Eq), wc("id", ComparisonOp::Eq)],
            &pk(&["ks", "id"]),
            &[],
        );
        assert_eq!(plan, ScanPlan::PartitionKeyLookup);
    }

    #[test]
    fn pk_incomplete_not_pk_lookup() {
        let plan = plan(&[wc("ks", ComparisonOp::Eq)], &pk(&["ks", "id"]), &[]);
        assert_ne!(plan, ScanPlan::PartitionKeyLookup);
    }

    #[test]
    fn pk_column_with_non_eq_op_not_pk_lookup() {
        let plan = plan(&[wc("id", ComparisonOp::Gt)], &pk(&["id"]), &[]);
        assert_ne!(plan, ScanPlan::PartitionKeyLookup);
    }

    #[test]
    fn single_index_exact_match() {
        let plan = plan(
            &[wc("email", ComparisonOp::Eq)],
            &pk(&["id"]),
            &[idx("idx_email", &["email"])],
        );
        assert_eq!(
            plan,
            ScanPlan::SingleIndex {
                index_name: "idx_email".to_string(),
                index_column: "email".to_string(),
            }
        );
    }

    #[test]
    fn single_index_all_where_columns_indexed() {
        // Both email and city are indexed with Eq — planner should use IndexIntersection.
        let plan = plan(
            &[wc("email", ComparisonOp::Eq), wc("city", ComparisonOp::Eq)],
            &pk(&["id"]),
            &[idx("idx_email", &["email"]), idx("idx_city", &["city"])],
        );
        match &plan {
            ScanPlan::IndexIntersection { indexes } => {
                assert_eq!(indexes.len(), 2);
            }
            other => panic!("expected IndexIntersection, got {other:?}"),
        }
    }

    #[test]
    fn index_scan_with_filter() {
        let plan = plan(
            &[wc("email", ComparisonOp::Eq), wc("age", ComparisonOp::Gt)],
            &pk(&["id"]),
            &[idx("idx_email", &["email"])],
        );
        assert_eq!(
            plan,
            ScanPlan::IndexScanWithFilter {
                index_name: "idx_email".to_string(),
                index_column: "email".to_string(),
                filter_columns: vec!["age".to_string()],
            }
        );
    }

    #[test]
    fn index_scan_with_filter_multiple_unindexed() {
        let plan = plan(
            &[
                wc("email", ComparisonOp::Eq),
                wc("age", ComparisonOp::Gt),
                wc("country", ComparisonOp::Eq),
            ],
            &pk(&["id"]),
            &[idx("idx_email", &["email"])],
        );
        match &plan {
            ScanPlan::IndexScanWithFilter {
                index_name,
                index_column,
                filter_columns,
            } => {
                assert_eq!(index_name, "idx_email");
                assert_eq!(index_column, "email");
                assert_eq!(filter_columns.len(), 2);
                assert!(filter_columns.contains(&"age".to_string()));
                assert!(filter_columns.contains(&"country".to_string()));
            }
            other => panic!("expected IndexScanWithFilter, got {other:?}"),
        }
    }

    #[test]
    fn full_scan_no_indexes() {
        let plan = plan(&[wc("email", ComparisonOp::Eq)], &pk(&["id"]), &[]);
        assert_eq!(plan, ScanPlan::FullScan);
    }

    #[test]
    fn vector_ann_plan_displays_index_and_column() {
        let plan = ScanPlan::VectorAnn {
            index_name: "paper_ann".to_string(),
            index_column: "embedding".to_string(),
        };
        assert_eq!(format!("{plan}"), "VectorAnn(paper_ann on embedding)");
        // It must be distinct from a full scan so EXPLAIN does not report
        // FullScan for an index-consulted ANN query.
        assert_ne!(plan, ScanPlan::FullScan);
    }

    #[test]
    fn full_scan_index_exists_but_wrong_column() {
        let plan = plan(
            &[wc("email", ComparisonOp::Eq)],
            &pk(&["id"]),
            &[idx("idx_city", &["city"])],
        );
        assert_eq!(plan, ScanPlan::FullScan);
    }

    #[test]
    fn full_scan_no_where_clauses() {
        let plan = plan(&[], &pk(&["id"]), &[idx("idx_email", &["email"])]);
        assert_eq!(plan, ScanPlan::FullScan);
    }

    #[test]
    fn full_scan_indexed_column_but_non_eq_op() {
        let plan = plan(
            &[wc("email", ComparisonOp::Gt)],
            &pk(&["id"]),
            &[idx("idx_email", &["email"])],
        );
        assert_eq!(plan, ScanPlan::FullScan);
    }

    // Hash indexes support POINT_LOOKUP only. The planner is index-type-agnostic:
    // it matches any registered single-column index for an Eq predicate.
    // These tests confirm hash index columns produce SingleIndex plans and that
    // non-Eq predicates against hash-indexed columns fall through to FullScan.

    #[test]
    fn hash_index_eq_predicate_produces_single_index() {
        // A hash index registered on `user_id` matches an Eq predicate.
        let plan = plan(
            &[wc("user_id", ComparisonOp::Eq)],
            &pk(&["pk"]),
            &[idx("idx_user_id_hash", &["user_id"])],
        );
        assert_eq!(
            plan,
            ScanPlan::SingleIndex {
                index_name: "idx_user_id_hash".to_string(),
                index_column: "user_id".to_string(),
            }
        );
    }

    #[test]
    fn hash_index_range_predicate_falls_to_full_scan() {
        // Hash indexes don't support range scans. The planner rejects non-Eq
        // predicates for all index types, so a Gt on a hash-indexed column
        // must produce FullScan rather than an index plan.
        let plan = plan(
            &[wc("user_id", ComparisonOp::Gt)],
            &pk(&["pk"]),
            &[idx("idx_user_id_hash", &["user_id"])],
        );
        assert_eq!(plan, ScanPlan::FullScan);
    }

    #[test]
    fn display_partition_key_lookup() {
        assert_eq!(
            format!("{}", ScanPlan::PartitionKeyLookup),
            "PartitionKeyLookup"
        );
    }

    #[test]
    fn display_single_index() {
        let plan = ScanPlan::SingleIndex {
            index_name: "idx_email".to_string(),
            index_column: "email".to_string(),
        };
        assert_eq!(format!("{plan}"), "SingleIndex(idx_email on email)");
    }

    #[test]
    fn display_index_scan_with_filter() {
        let plan = ScanPlan::IndexScanWithFilter {
            index_name: "idx_email".to_string(),
            index_column: "email".to_string(),
            filter_columns: vec!["age".to_string(), "country".to_string()],
        };
        assert_eq!(
            format!("{plan}"),
            "IndexScanWithFilter(idx_email on email, filter: [age, country])"
        );
    }

    #[test]
    fn display_full_scan() {
        assert_eq!(format!("{}", ScanPlan::FullScan), "FullScan");
    }

    #[test]
    fn display_geo_index() {
        let plan = ScanPlan::GeoIndex {
            index_name: "places_location_geo".to_string(),
            index_column: "location".to_string(),
            op: "GeoWithinRadius".to_string(),
        };
        assert_eq!(
            format!("{plan}"),
            "GeoIndex(places_location_geo on location, GeoWithinRadius)"
        );
    }

    #[test]
    fn display_full_text_index() {
        let plan = ScanPlan::FullTextIndex {
            index_name: "ftu_body".to_string(),
            index_column: "body".to_string(),
        };
        assert_eq!(format!("{plan}"), "FullTextIndex(ftu_body on body)");
        // A full-text query is index-accelerated; it must never render FullScan.
        assert_ne!(plan, ScanPlan::FullScan);
    }

    #[test]
    fn index_intersection_two_indexes() {
        let plan = plan(
            &[wc("email", ComparisonOp::Eq), wc("city", ComparisonOp::Eq)],
            &pk(&["id"]),
            &[idx("idx_email", &["email"]), idx("idx_city", &["city"])],
        );
        match &plan {
            ScanPlan::IndexIntersection { indexes } => {
                assert_eq!(indexes.len(), 2);
            }
            other => panic!("expected IndexIntersection, got {other:?}"),
        }
    }

    #[test]
    fn index_intersection_three_indexes() {
        let plan = plan(
            &[
                wc("a", ComparisonOp::Eq),
                wc("b", ComparisonOp::Eq),
                wc("c", ComparisonOp::Eq),
            ],
            &pk(&["id"]),
            &[
                idx("idx_a", &["a"]),
                idx("idx_b", &["b"]),
                idx("idx_c", &["c"]),
            ],
        );
        match &plan {
            ScanPlan::IndexIntersection { indexes } => assert_eq!(indexes.len(), 3),
            other => panic!("expected IndexIntersection, got {other:?}"),
        }
    }

    #[test]
    fn extra_covered_filter_column_yields_single_index_not_filtering() {
        // A filtered index on `name` (offered in `indexes`) plus an `extra_covered`
        // filter column `status`: a query `name = v AND status = a` must plan
        // `SingleIndex` (the index enforces the status predicate) rather than
        // `IndexScanWithFilter` (which would require ALLOW FILTERING).
        let plan = plan_with_covered(
            &[wc("name", ComparisonOp::Eq), wc("status", ComparisonOp::Eq)],
            &pk(&["id"]),
            &[idx("name_active_idx", &["name"])],
            &["status".to_string()],
        );
        assert_eq!(
            plan,
            ScanPlan::SingleIndex {
                index_name: "name_active_idx".to_string(),
                index_column: "name".to_string(),
            }
        );
    }

    #[test]
    fn extra_covered_does_not_invent_an_index() {
        // `extra_covered` only suppresses the ALLOW FILTERING requirement for a
        // covered column; it never makes that column itself index-selectable.
        // With no index on `name`, a query on `name` alone is still FullScan.
        let plan = plan_with_covered(
            &[wc("name", ComparisonOp::Eq)],
            &pk(&["id"]),
            &[],
            &["name".to_string()],
        );
        assert_eq!(plan, ScanPlan::FullScan);
    }

    #[test]
    fn display_index_intersection() {
        let plan = ScanPlan::IndexIntersection {
            indexes: vec![("idx_a".into(), "a".into()), ("idx_b".into(), "b".into())],
        };
        assert_eq!(
            format!("{plan}"),
            "IndexIntersection(idx_a on a, idx_b on b)"
        );
    }
}
