//! Virtual table abstraction for live, code-backed observability data.
//!
//! A [`VirtualTable`] provides a table-like interface backed by live code
//! rather than SSTables. All observability data in Ferrosa is modeled as
//! virtual tables: metrics, active queries, cluster topology, etc.
//!
//! Virtual tables are read-only and do not participate in replication or
//! compaction. They are registered in the `VirtualTableRegistry` (Task 3)
//! and served via the CQL native protocol as regular `SELECT` queries.

use ferrosa_common::{CellValue, DataType};
use std::time::Duration;

/// A virtual table backed by live code instead of SSTables.
///
/// Implementers supply schema metadata (`name`, `keyspace`, `columns`,
/// `primary_key_columns`) and a `read` method that materialises rows on
/// demand. The query layer applies any remaining predicates after the
/// implementation has done its own filtering.
///
/// # Object Safety
///
/// The trait is object-safe: implementations are typically stored as
/// `Arc<dyn VirtualTable>` in the registry.
pub trait VirtualTable: Send + Sync {
    /// The table name (unqualified, lowercase).
    fn name(&self) -> &str;

    /// The keyspace this table belongs to (e.g. `"system_observability"`).
    fn keyspace(&self) -> &str;

    /// Ordered column definitions, matching the layout of each [`VirtualRow`].
    fn columns(&self) -> &[VirtualColumnDef];

    /// Indices into `columns()` that form the primary key (partition +
    /// clustering, in order).
    fn primary_key_columns(&self) -> &[usize];

    /// Materialise rows, optionally filtered by `predicate`.
    ///
    /// Implementations may apply as much or as little of the predicate as
    /// convenient; the query layer will re-apply it for correctness.
    fn read(&self, predicate: Option<&RowPredicate>) -> Vec<VirtualRow>;

    /// How the table should be kept fresh when watched by a subscriber.
    fn subscription_mode(&self) -> SubscriptionMode;
}

/// A single row returned by a virtual table.
#[derive(Debug, Clone)]
pub struct VirtualRow {
    /// Cell values in column order, matching [`VirtualTable::columns`].
    pub cells: Vec<CellValue>,
}

/// Column definition for a virtual table.
#[derive(Debug, Clone)]
pub struct VirtualColumnDef {
    /// Column name (lowercase).
    pub name: String,
    /// Scalar CQL type of this column.
    pub data_type: DataType,
}

/// A conjunction of column filters applied to a virtual table scan.
///
/// All filters must match for a row to be included (AND semantics).
pub struct RowPredicate {
    pub filters: Vec<ColumnFilter>,
}

/// A single column filter within a [`RowPredicate`].
pub struct ColumnFilter {
    /// Name of the column to filter on.
    pub column: String,
    /// Comparison operator.
    pub op: PredicateOp,
    /// Value to compare against.
    pub value: CellValue,
}

/// Comparison operators for column predicates.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PredicateOp {
    Eq,
    Gt,
    Lt,
    Gte,
    Lte,
}

/// How a virtual table should be refreshed when watched by a subscriber.
#[derive(Debug, Clone)]
pub enum SubscriptionMode {
    /// The table can be polled on any schedule; the subscriber drives timing.
    Pollable,
    /// The table prefers a regular poll interval; `default_interval` is a hint.
    DemandDriven { default_interval: Duration },
    /// The table does not support subscriptions (one-shot reads only).
    None,
}

#[cfg(test)]
mod tests {
    use super::*;
    use ferrosa_common::{CellValue, DataType};
    use std::time::Duration;

    struct TestTable;

    impl VirtualTable for TestTable {
        fn name(&self) -> &str {
            "test_table"
        }

        fn keyspace(&self) -> &str {
            "system_observability"
        }

        fn columns(&self) -> &[VirtualColumnDef] {
            &[]
        }

        fn primary_key_columns(&self) -> &[usize] {
            &[0]
        }

        fn read(&self, _predicate: Option<&RowPredicate>) -> Vec<VirtualRow> {
            vec![VirtualRow { cells: vec![] }]
        }

        fn subscription_mode(&self) -> SubscriptionMode {
            SubscriptionMode::Pollable
        }
    }

    #[test]
    fn virtual_table_trait_object_safety() {
        let table: Box<dyn VirtualTable> = Box::new(TestTable);
        assert_eq!(table.name(), "test_table");
        assert_eq!(table.keyspace(), "system_observability");
        assert_eq!(table.read(None).len(), 1);
    }

    #[test]
    fn subscription_mode_variants() {
        assert!(matches!(
            SubscriptionMode::Pollable,
            SubscriptionMode::Pollable
        ));
        let dm = SubscriptionMode::DemandDriven {
            default_interval: Duration::from_secs(5),
        };
        assert!(matches!(dm, SubscriptionMode::DemandDriven { .. }));
        assert!(matches!(SubscriptionMode::None, SubscriptionMode::None));
    }

    #[test]
    fn row_predicate_conjunction() {
        // CellValue::new_for_test does not exist; use CellValue::live(bytes, timestamp=0).
        let pred = RowPredicate {
            filters: vec![
                ColumnFilter {
                    column: "keyspace".into(),
                    op: PredicateOp::Eq,
                    value: CellValue::live(b"system".to_vec(), 0),
                },
                ColumnFilter {
                    column: "size".into(),
                    op: PredicateOp::Gt,
                    value: CellValue::live(100i64.to_be_bytes().to_vec(), 0),
                },
            ],
        };
        assert_eq!(pred.filters.len(), 2);
    }

    #[test]
    fn virtual_column_def_clone() {
        let col = VirtualColumnDef {
            name: "host_id".into(),
            data_type: DataType::Uuid,
        };
        let col2 = col.clone();
        assert_eq!(col.name, col2.name);
    }

    #[test]
    fn predicate_op_equality() {
        assert_eq!(PredicateOp::Eq, PredicateOp::Eq);
        assert_ne!(PredicateOp::Gt, PredicateOp::Lt);
    }
}
