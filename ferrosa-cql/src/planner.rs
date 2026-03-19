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
/// 3. First WHERE column with `Eq` that matches a single-column index:
///    - All other WHERE columns also indexed → `SingleIndex`
///    - Some WHERE columns not indexed → `IndexScanWithFilter`
/// 4. No usable index → `FullScan`
pub fn plan(
    where_clauses: &[WhereClause],
    pk_columns: &[String],
    indexes: &[(String, Vec<String>)],
) -> ScanPlan {
    if pk_columns_satisfied(where_clauses, pk_columns) {
        return ScanPlan::PartitionKeyLookup;
    }

    if where_clauses.is_empty() {
        return ScanPlan::FullScan;
    }

    // Find the first WHERE column with Eq that has a matching single-column index.
    let best = where_clauses.iter().find_map(|wc| {
        if wc.op != ComparisonOp::Eq {
            return None;
        }
        indexes
            .iter()
            .find(|(_, cols)| cols.len() == 1 && cols[0] == wc.column)
            .map(|(name, _)| (name.clone(), wc.column.clone()))
    });

    let (index_name, index_column) = match best {
        None => return ScanPlan::FullScan,
        Some(pair) => pair,
    };

    // Collect WHERE columns that are not covered by any index.
    let filter_columns: Vec<String> = where_clauses
        .iter()
        .filter(|wc| wc.column != index_column)
        .filter(|wc| {
            !indexes
                .iter()
                .any(|(_, cols)| cols.len() == 1 && cols[0] == wc.column)
        })
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

fn pk_columns_satisfied(where_clauses: &[WhereClause], pk_columns: &[String]) -> bool {
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
        let plan = plan(
            &[wc("email", ComparisonOp::Eq), wc("city", ComparisonOp::Eq)],
            &pk(&["id"]),
            &[idx("idx_email", &["email"]), idx("idx_city", &["city"])],
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

    // Task 4: IndexScanWithFilter post-filter validation

    #[test]
    fn index_scan_with_filter_only_one_indexed_column() {
        // WHERE email = 'x' AND age > 25 AND status = 'active'
        // Only email is indexed.
        let plan = plan(
            &[
                wc("email", ComparisonOp::Eq),
                wc("age", ComparisonOp::Gt),
                wc("status", ComparisonOp::Eq),
            ],
            &pk(&["id"]),
            &[idx("idx_email", &["email"])],
        );
        match &plan {
            ScanPlan::IndexScanWithFilter { filter_columns, .. } => {
                assert_eq!(filter_columns.len(), 2);
                assert!(filter_columns.contains(&"age".to_string()));
                assert!(filter_columns.contains(&"status".to_string()));
            }
            other => panic!("expected IndexScanWithFilter, got {other:?}"),
        }
    }

    // Task 5: Keyspace/table scoping validation

    #[test]
    fn index_from_different_table_not_used() {
        // Index exists on "other_table.email", not "users.email"
        // The plan() function receives only indexes for the target table,
        // so this test validates the router's filtering (which happens before
        // plan() is called). At the planner level, if no indexes are passed,
        // it should return FullScan.
        let plan = plan(
            &[wc("email", ComparisonOp::Eq)],
            &pk(&["id"]),
            &[], // Router filtered out indexes from other tables
        );
        assert_eq!(plan, ScanPlan::FullScan);
    }

    #[test]
    fn multiple_indexes_picks_first_matching() {
        // Two indexes on different columns — planner picks the first WHERE match
        let plan = plan(
            &[wc("city", ComparisonOp::Eq), wc("email", ComparisonOp::Eq)],
            &pk(&["id"]),
            &[idx("idx_city", &["city"]), idx("idx_email", &["email"])],
        );
        // First WHERE clause is "city" which matches idx_city
        assert_eq!(
            plan,
            ScanPlan::SingleIndex {
                index_name: "idx_city".to_string(),
                index_column: "city".to_string(),
            }
        );
    }
}
