//! Virtual table abstraction for live, code-backed observability data.
//!
//! A [`VirtualTable`] provides a table-like interface backed by live code
//! rather than SSTables. All observability data in Ferrosa is modeled as
//! virtual tables: metrics, active queries, cluster topology, etc.
//!
//! Virtual tables do not participate in replication or compaction. Most are
//! read-only, but selected admin control tables may accept bounded runtime
//! updates through [`VirtualTable::apply_update`].

use ferrosa_common::{CellValue, DataType};
use std::time::Duration;

/// A virtual table backed by live code instead of SSTables.
///
/// Implementers supply schema metadata (`name`, `keyspace`, `columns`,
/// `primary_key_columns`) and [`VirtualTable::visit_rows`]. That visitor is the
/// read path: the query/serving layer consumes rows through it so a table's
/// peak heap does not scale with its result-set size. [`VirtualTable::read`] is
/// a provided convenience for the bounded callers that genuinely need an owned
/// `Vec`; it collects via `visit_rows` and must never be the only implementation
/// of a large or live table.
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

    /// Visit rows, optionally filtered by `predicate`.
    ///
    /// REQUIRED — this is the virtual-table read path. Implementations must emit
    /// one row at a time through `visit` and must not build an intermediate
    /// collection of rows: a table's peak heap has to stay independent of its
    /// result-set size. A table whose result set is genuinely bounded (by the
    /// caller's own row cap, or by a fixed set of system rows) may still satisfy
    /// this by emitting its rows directly.
    ///
    /// Implementations may apply as much or as little of the predicate as
    /// convenient; the query layer will re-apply it for correctness.
    fn visit_rows(&self, predicate: Option<&RowPredicate>, visit: &mut dyn FnMut(VirtualRow));

    /// Materialise rows, optionally filtered by `predicate`.
    ///
    /// PROVIDED. Collects [`VirtualTable::visit_rows`] into an owned `Vec`, in
    /// visit order, so it returns exactly the rows the visitor emits — same
    /// order, same count, no duplicates. Prefer `visit_rows` on any read path
    /// whose result set is not already bounded by the caller; the `Vec` this
    /// returns is O(rows) in heap.
    fn read(&self, predicate: Option<&RowPredicate>) -> Vec<VirtualRow> {
        let mut rows = Vec::new();
        self.visit_rows(predicate, &mut |row| rows.push(row));
        rows
    }

    /// How the table should be kept fresh when watched by a subscriber.
    fn subscription_mode(&self) -> SubscriptionMode;

    /// Apply a bounded runtime update to this virtual table.
    ///
    /// The default implementation keeps virtual tables read-only. Control-plane
    /// tables override this for explicit, admin-only settings updates.
    fn apply_update(&self, _update: &VirtualTableUpdate) -> Result<(), String> {
        Err(format!(
            "virtual table {}.{} is read-only",
            self.keyspace(),
            self.name()
        ))
    }

    /// Optional richer wire-type for the column at `col_idx`. Default
    /// returns `None` — the column's wire type derives from
    /// `columns()[col_idx].data_type`. Tables whose columns map to
    /// CQL collection types (e.g. `system_schema.types.field_names` is
    /// `frozen<list<text>>`) override this to return `Some(WireType)`.
    /// Cell bytes for such columns MUST be already CQL-encoded per the
    /// variant — see `VirtualColumnDef::encode_list_text`.
    fn wire_type_for(&self, _col_idx: usize) -> Option<WireType> {
        None
    }
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

/// Runtime update applied to a virtual table row.
pub struct VirtualTableUpdate {
    pub assignments: Vec<VirtualColumnUpdate>,
    pub predicate: RowPredicate,
}

/// A single virtual-table column assignment.
pub struct VirtualColumnUpdate {
    pub column: String,
    pub value: CellValue,
}

/// Wire-type families needed for virtual columns whose protocol shape is
/// richer than `DataType`. Returned per-column-index by
/// `VirtualTable::wire_type_for`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WireType {
    /// `list<text>` — frame-encoded as: 4-byte BE element count, then
    /// for each element 4-byte BE length + UTF-8 bytes.
    ListText,
    /// `set<text>` — same collection payload encoding as `list<text>`.
    SetText,
}

impl VirtualColumnDef {
    /// Encode a slice of strings as native CQL `list<text>` bytes.
    pub fn encode_list_text(items: &[String]) -> Vec<u8> {
        let mut buf = Vec::with_capacity(4 + items.iter().map(|s| 4 + s.len()).sum::<usize>());
        buf.extend_from_slice(&(items.len() as i32).to_be_bytes());
        for s in items {
            let bytes = s.as_bytes();
            buf.extend_from_slice(&(bytes.len() as i32).to_be_bytes());
            buf.extend_from_slice(bytes);
        }
        buf
    }
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

        fn visit_rows(&self, _predicate: Option<&RowPredicate>, visit: &mut dyn FnMut(VirtualRow)) {
            visit(VirtualRow { cells: vec![] })
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

    /// Invariant: the provided `read` returns exactly the rows `visit_rows`
    /// emits, in visit order — no drop, no duplicate, no reordering. This is
    /// what lets every existing `read` caller keep its result while the read
    /// path itself streams.
    #[test]
    fn provided_read_collects_visit_rows_in_order() {
        struct Ordered(u32);
        impl VirtualTable for Ordered {
            fn name(&self) -> &str {
                "ordered"
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
            fn visit_rows(
                &self,
                _predicate: Option<&RowPredicate>,
                visit: &mut dyn FnMut(VirtualRow),
            ) {
                for i in 0..self.0 {
                    visit(VirtualRow {
                        cells: vec![CellValue::live(i.to_be_bytes().to_vec(), 0)],
                    });
                }
            }
            fn subscription_mode(&self) -> SubscriptionMode {
                SubscriptionMode::Pollable
            }
        }

        let table = Ordered(5);
        let rows = table.read(None);
        assert_eq!(rows.len(), 5, "read must surface every visited row");
        for (i, row) in rows.iter().enumerate() {
            assert_eq!(
                row.cells[0],
                CellValue::live((i as u32).to_be_bytes().to_vec(), 0)
            );
        }
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
