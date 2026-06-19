//! Pure view-delta computation — the incremental maintenance state machine.
//!
//! [`compute_view_delta`] turns a base-row transition (`prior` → `next` at a
//! mutation `timestamp`) into the view mutations required to keep the view
//! consistent with the base, per `specs/materialized-views/architecture.md`
//! §6.3. It is a **pure** function: the caller (the storage observer / the
//! Accord-coordinated commit in the cluster layer) performs the read-before-write
//! that produces `prior`, and translates the returned [`ViewDelta`]s into real
//! storage mutations. Keeping this logic pure is what makes the strict-
//! serializable guarantee (D2) reproducible and unit-testable (gates G4/G6).
//!
//! ## Projection model
//!
//! A base row is *projected* into the view iff every view primary-key column has
//! a non-null value (the Cassandra `IS NOT NULL` baseline). A row entering or
//! leaving that set is a predicate flip. The optional ferrosa-extension predicate
//! ([`ViewPredicate::extra`](crate::metadata::ViewPredicate)) and UDF-computed
//! column projection are evaluated by later cycles that wire in the predicate and
//! UDF evaluators; this module handles base-column projection and the structural
//! state machine.

use std::collections::BTreeMap;

use crate::metadata::{ColumnSource, ViewMetadata};

/// A snapshot of a base row's column values, keyed by base column name.
///
/// Absence of a column means it is null/absent for projection purposes. The
/// caller builds this from a real storage row; this crate stays free of storage
/// row types (a forbidden dependency edge — see `dsm-proposed.md`).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RowSnapshot {
    /// Base column name → non-null value bytes.
    pub columns: BTreeMap<String, Vec<u8>>,
}

impl RowSnapshot {
    /// The value of a column, or `None` if null/absent.
    pub fn get(&self, column: &str) -> Option<&Vec<u8>> {
        self.columns.get(column)
    }
}

/// A mutation to apply to the view to keep it consistent with the base.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ViewDelta {
    /// Insert or update the view row with this primary key.
    Upsert {
        /// View primary-key column values, in view-PK order.
        view_pk: Vec<(String, Vec<u8>)>,
        /// Projected non-key column values.
        columns: Vec<(String, Vec<u8>)>,
        /// Timestamp of the originating base mutation.
        timestamp: i64,
    },
    /// Delete the view row with this primary key.
    Delete {
        /// View primary-key column values, in view-PK order.
        view_pk: Vec<(String, Vec<u8>)>,
        /// Timestamp of the originating base mutation.
        timestamp: i64,
    },
}

/// Compute the view mutations for a base-row transition.
///
/// `prior` is the base row state before the mutation (from read-before-write),
/// `next` is the state after (`None` for a delete / full tombstone). `timestamp`
/// is the originating base mutation timestamp, stamped on every emitted delta.
pub fn compute_view_delta(
    view: &ViewMetadata,
    prior: Option<&RowSnapshot>,
    next: Option<&RowSnapshot>,
    timestamp: i64,
) -> Vec<ViewDelta> {
    // A row participates in the view only while it is projected.
    let was = prior.filter(|r| projects(view, r));
    let now = next.filter(|r| projects(view, r));
    match (was, now) {
        (None, None) => Vec::new(),
        (None, Some(n)) => vec![upsert(view, n, timestamp)],
        (Some(p), None) => vec![delete(view, p, timestamp)],
        (Some(p), Some(n)) => {
            if view_pk_values(view, p) == view_pk_values(view, n) {
                // Same view row, column update.
                vec![upsert(view, n, timestamp)]
            } else {
                // View primary key changed: remove the old row, add the new one.
                vec![delete(view, p, timestamp), upsert(view, n, timestamp)]
            }
        }
    }
}

/// A base row is projected iff every view primary-key column is non-null.
fn projects(view: &ViewMetadata, row: &RowSnapshot) -> bool {
    view.primary_key().all(|c| row.get(c).is_some())
}

/// View primary-key column values, in view-PK order. Callers must ensure the row
/// is projected (every PK column present).
fn view_pk_values(view: &ViewMetadata, row: &RowSnapshot) -> Vec<(String, Vec<u8>)> {
    view.primary_key()
        .map(|c| (c.to_string(), row.get(c).cloned().unwrap_or_default()))
        .collect()
}

/// Projected non-key columns. UDF/aggregate projection is handled by the
/// udf-eval cycle; base-column projection is handled here.
fn projected_columns(view: &ViewMetadata, row: &RowSnapshot) -> Vec<(String, Vec<u8>)> {
    view.selected
        .iter()
        .filter_map(|vc| match &vc.source {
            ColumnSource::Base(base_col) => {
                row.get(base_col).map(|val| (vc.name.clone(), val.clone()))
            }
            ColumnSource::Udf { .. } | ColumnSource::Aggregate { .. } => None,
        })
        .collect()
}

fn upsert(view: &ViewMetadata, row: &RowSnapshot, timestamp: i64) -> ViewDelta {
    ViewDelta::Upsert {
        view_pk: view_pk_values(view, row),
        columns: projected_columns(view, row),
        timestamp,
    }
}

fn delete(view: &ViewMetadata, row: &RowSnapshot, timestamp: i64) -> ViewDelta {
    ViewDelta::Delete {
        view_pk: view_pk_values(view, row),
        timestamp,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metadata::{ViewColumn, ViewKind, ViewMetadata, ViewPredicate};
    use uuid::Uuid;

    fn b(s: &str) -> Vec<u8> {
        s.as_bytes().to_vec()
    }

    /// View `ks.mv` over base `ks.t`: re-partition by `c` then `p`, project `v`.
    fn view() -> ViewMetadata {
        let base = |n: &str| ViewColumn {
            name: n.into(),
            source: ColumnSource::Base(n.into()),
        };
        ViewMetadata {
            keyspace: "ks".into(),
            name: "mv".into(),
            kind: ViewKind::Incremental,
            base_keyspace: "ks".into(),
            base_table: "t".into(),
            base_table_id: Uuid::nil(),
            id: Uuid::nil(),
            selected: vec![base("c"), base("p"), base("v")],
            partition_key: vec!["c".into()],
            clustering_key: vec!["p".into()],
            predicate: ViewPredicate {
                not_null: vec!["c".into(), "p".into()],
                extra: None,
            },
            include_all_columns: false,
        }
    }

    /// Build a row snapshot from (column, value) pairs.
    fn row(pairs: &[(&str, &str)]) -> RowSnapshot {
        RowSnapshot {
            columns: pairs.iter().map(|(k, v)| (k.to_string(), b(v))).collect(),
        }
    }

    fn upsert(pk: &[(&str, &str)], cols: &[(&str, &str)], ts: i64) -> ViewDelta {
        ViewDelta::Upsert {
            view_pk: pk.iter().map(|(k, v)| (k.to_string(), b(v))).collect(),
            columns: cols.iter().map(|(k, v)| (k.to_string(), b(v))).collect(),
            timestamp: ts,
        }
    }

    fn delete(pk: &[(&str, &str)], ts: i64) -> ViewDelta {
        ViewDelta::Delete {
            view_pk: pk.iter().map(|(k, v)| (k.to_string(), b(v))).collect(),
            timestamp: ts,
        }
    }

    #[test]
    fn insert_into_predicate_emits_view_insert() {
        let next = row(&[("c", "A"), ("p", "1"), ("v", "x")]);
        let got = compute_view_delta(&view(), None, Some(&next), 100);
        assert_eq!(
            got,
            vec![upsert(
                &[("c", "A"), ("p", "1")],
                &[("c", "A"), ("p", "1"), ("v", "x")],
                100
            )]
        );
    }

    #[test]
    fn insert_outside_predicate_emits_nothing() {
        // Missing view-PK column `p` → not projected.
        let next = row(&[("c", "A"), ("v", "x")]);
        assert!(compute_view_delta(&view(), None, Some(&next), 100).is_empty());
    }

    #[test]
    fn update_non_pk_col_updates_view_row() {
        let prior = row(&[("c", "A"), ("p", "1"), ("v", "x")]);
        let next = row(&[("c", "A"), ("p", "1"), ("v", "y")]);
        let got = compute_view_delta(&view(), Some(&prior), Some(&next), 200);
        assert_eq!(
            got,
            vec![upsert(
                &[("c", "A"), ("p", "1")],
                &[("c", "A"), ("p", "1"), ("v", "y")],
                200
            )]
        );
    }

    #[test]
    fn update_view_pk_col_emits_delete_old_and_insert_new() {
        // Partition column `c` changes A → B: delete old view row, insert new.
        let prior = row(&[("c", "A"), ("p", "1"), ("v", "x")]);
        let next = row(&[("c", "B"), ("p", "1"), ("v", "x")]);
        let got = compute_view_delta(&view(), Some(&prior), Some(&next), 300);
        assert_eq!(
            got,
            vec![
                delete(&[("c", "A"), ("p", "1")], 300),
                upsert(
                    &[("c", "B"), ("p", "1")],
                    &[("c", "B"), ("p", "1"), ("v", "x")],
                    300
                ),
            ]
        );
    }

    #[test]
    fn predicate_flip_in_emits_insert() {
        let prior = row(&[("c", "A"), ("v", "x")]); // p null → not in view
        let next = row(&[("c", "A"), ("p", "1"), ("v", "x")]); // p set → enters view
        let got = compute_view_delta(&view(), Some(&prior), Some(&next), 400);
        assert_eq!(
            got,
            vec![upsert(
                &[("c", "A"), ("p", "1")],
                &[("c", "A"), ("p", "1"), ("v", "x")],
                400
            )]
        );
    }

    #[test]
    fn predicate_flip_out_emits_delete() {
        let prior = row(&[("c", "A"), ("p", "1"), ("v", "x")]); // in view
        let next = row(&[("c", "A"), ("v", "x")]); // p null → leaves view
        let got = compute_view_delta(&view(), Some(&prior), Some(&next), 500);
        assert_eq!(got, vec![delete(&[("c", "A"), ("p", "1")], 500)]);
    }

    #[test]
    fn delete_emits_view_delete() {
        let prior = row(&[("c", "A"), ("p", "1"), ("v", "x")]);
        let got = compute_view_delta(&view(), Some(&prior), None, 600);
        assert_eq!(got, vec![delete(&[("c", "A"), ("p", "1")], 600)]);
    }

    #[test]
    fn ttl_expiry_emits_timestamped_delete() {
        let prior = row(&[("c", "A"), ("p", "1"), ("v", "x")]);
        let got = compute_view_delta(&view(), Some(&prior), None, 777);
        match got.as_slice() {
            [ViewDelta::Delete { timestamp, .. }] => assert_eq!(*timestamp, 777),
            other => panic!("expected one timestamped delete, got {other:?}"),
        }
    }

    #[test]
    fn no_change_when_row_absent_before_and_after() {
        assert!(compute_view_delta(&view(), None, None, 1).is_empty());
    }

    #[test]
    fn delta_is_deterministic() {
        let prior = row(&[("c", "A"), ("p", "1"), ("v", "x")]);
        let next = row(&[("c", "B"), ("p", "1"), ("v", "x")]);
        let a = compute_view_delta(&view(), Some(&prior), Some(&next), 9);
        let b = compute_view_delta(&view(), Some(&prior), Some(&next), 9);
        assert_eq!(a, b);
    }
}
