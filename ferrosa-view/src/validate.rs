//! DDL validation for materialized-view definitions.
//!
//! `validate_view_def` is the single gatekeeper shared by every frontend. It
//! enforces the Cassandra-baseline rules and the ferrosa-extension rules from
//! `specs/materialized-views/architecture.md` §4. It is a pure function over
//! metadata — no I/O — so both the CQL and Postgres frontends call it before
//! handing a `ViewMetadata` to the schema-replication path.

use std::collections::HashSet;

use ferrosa_schema::{ColumnKind, TableFlag, TableMetadata};

use crate::metadata::{ColumnSource, ViewMetadata};

/// A reason a materialized-view definition is invalid.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ViewDefError {
    /// The view primary key omits a base primary-key column.
    ViewPkMissingBasePkColumn {
        /// The missing base PK column.
        column: String,
    },
    /// The view primary key adds more than one non-base-PK column.
    TooManyExtraPkColumns {
        /// The offending extra columns.
        extra: Vec<String>,
    },
    /// A view primary-key column lacks the required `IS NOT NULL` restriction.
    ViewPkColumnNotNullRequired {
        /// The view PK column missing the restriction.
        column: String,
    },
    /// A selected column is an aggregate (forbidden in a materialized view).
    AggregateNotAllowed {
        /// The aggregate column name.
        column: String,
    },
    /// A selected column is a static base column (forbidden in a materialized view).
    StaticColumnNotAllowed {
        /// The static column name.
        column: String,
    },
    /// The base table contains counter columns (forbidden as a view base).
    CounterNotAllowed,
    /// The base table is itself a view (chained materialized views are forbidden).
    ChainedViewNotAllowed,
    /// A UDF-computed column is not deterministic (required under Accord; gate G2).
    UdfNotDeterministic {
        /// The non-deterministic UDF column.
        column: String,
    },
    /// A UDF-computed column appears in the view primary key (forbidden initially).
    UdfColumnInPrimaryKey {
        /// The computed column placed in the PK.
        column: String,
    },
}

impl std::fmt::Display for ViewDefError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ViewDefError::ViewPkMissingBasePkColumn { column } => {
                write!(
                    f,
                    "view primary key must include base primary-key column `{column}`"
                )
            }
            ViewDefError::TooManyExtraPkColumns { extra } => write!(
                f,
                "view primary key may add at most one non-base-PK column, got {extra:?}"
            ),
            ViewDefError::ViewPkColumnNotNullRequired { column } => {
                write!(f, "view primary-key column `{column}` requires IS NOT NULL")
            }
            ViewDefError::AggregateNotAllowed { column } => {
                write!(
                    f,
                    "aggregate column `{column}` is not allowed in a materialized view"
                )
            }
            ViewDefError::StaticColumnNotAllowed { column } => {
                write!(
                    f,
                    "static column `{column}` is not allowed in a materialized view"
                )
            }
            ViewDefError::CounterNotAllowed => {
                write!(
                    f,
                    "counter tables cannot be the base of a materialized view"
                )
            }
            ViewDefError::ChainedViewNotAllowed => {
                write!(f, "a materialized view cannot be the base of another view")
            }
            ViewDefError::UdfNotDeterministic { column } => write!(
                f,
                "UDF-computed column `{column}` must be deterministic to be used in a view"
            ),
            ViewDefError::UdfColumnInPrimaryKey { column } => {
                write!(
                    f,
                    "computed column `{column}` cannot be part of the view primary key"
                )
            }
        }
    }
}

impl std::error::Error for ViewDefError {}

/// Validate a materialized-view definition against its base table.
///
/// `base_is_view` is whether the named base table is itself a materialized view
/// (the frontend knows this from the schema); chained views are rejected.
///
/// Returns `Ok(())` if the definition satisfies all rules in
/// `architecture.md` §4, or the first violated rule.
pub fn validate_view_def(
    base: &TableMetadata,
    view: &ViewMetadata,
    base_is_view: bool,
) -> Result<(), ViewDefError> {
    if base_is_view {
        return Err(ViewDefError::ChainedViewNotAllowed);
    }
    if base.flags.contains(&TableFlag::Counter) {
        return Err(ViewDefError::CounterNotAllowed);
    }
    check_pk_covers_base(base, view)?;
    check_extra_pk_count(base, view)?;
    check_no_computed_pk(view)?;
    check_pk_not_null(view)?;
    check_selected_columns(base, view)?;
    Ok(())
}

/// Base primary-key column names, partition columns first then clustering.
fn base_pk_names(base: &TableMetadata) -> impl Iterator<Item = &str> {
    base.partition_key
        .iter()
        .map(String::as_str)
        .chain(base.clustering_key.iter().map(|(n, _)| n.as_str()))
}

/// Rule: the view primary key must include every base primary-key column.
fn check_pk_covers_base(base: &TableMetadata, view: &ViewMetadata) -> Result<(), ViewDefError> {
    let view_pk: HashSet<&str> = view.primary_key().collect();
    for col in base_pk_names(base) {
        if !view_pk.contains(col) {
            return Err(ViewDefError::ViewPkMissingBasePkColumn {
                column: col.to_string(),
            });
        }
    }
    Ok(())
}

/// Rule: the view primary key may add at most one non-base-PK column.
fn check_extra_pk_count(base: &TableMetadata, view: &ViewMetadata) -> Result<(), ViewDefError> {
    let base_pk: HashSet<&str> = base_pk_names(base).collect();
    let extra: Vec<String> = view
        .primary_key()
        .filter(|c| !base_pk.contains(c))
        .map(String::from)
        .collect();
    if extra.len() > 1 {
        return Err(ViewDefError::TooManyExtraPkColumns { extra });
    }
    Ok(())
}

/// Rule: a UDF-computed (or aggregate) column may not be part of the view PK.
fn check_no_computed_pk(view: &ViewMetadata) -> Result<(), ViewDefError> {
    for col in view.primary_key() {
        match view.source_of(col) {
            Some(ColumnSource::Udf { .. }) | Some(ColumnSource::Aggregate { .. }) => {
                return Err(ViewDefError::UdfColumnInPrimaryKey {
                    column: col.to_string(),
                });
            }
            _ => {}
        }
    }
    Ok(())
}

/// Rule: every view primary-key column requires an `IS NOT NULL` restriction.
fn check_pk_not_null(view: &ViewMetadata) -> Result<(), ViewDefError> {
    let not_null: HashSet<&str> = view.predicate.not_null.iter().map(String::as_str).collect();
    for col in view.primary_key() {
        if !not_null.contains(col) {
            return Err(ViewDefError::ViewPkColumnNotNullRequired {
                column: col.to_string(),
            });
        }
    }
    Ok(())
}

/// Rule: no aggregates, no static base columns, no non-deterministic UDF columns.
fn check_selected_columns(base: &TableMetadata, view: &ViewMetadata) -> Result<(), ViewDefError> {
    for vcol in &view.selected {
        match &vcol.source {
            ColumnSource::Aggregate { .. } => {
                return Err(ViewDefError::AggregateNotAllowed {
                    column: vcol.name.clone(),
                });
            }
            ColumnSource::Udf { deterministic, .. } if !deterministic => {
                return Err(ViewDefError::UdfNotDeterministic {
                    column: vcol.name.clone(),
                });
            }
            ColumnSource::Base(base_col) => {
                if let Some(cm) = base.columns.get(base_col) {
                    if cm.kind == ColumnKind::Static {
                        return Err(ViewDefError::StaticColumnNotAllowed {
                            column: vcol.name.clone(),
                        });
                    }
                }
            }
            ColumnSource::Udf { .. } => {}
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metadata::{ViewColumn, ViewKind, ViewMetadata, ViewPredicate};
    use ferrosa_schema::{ClusteringOrder, ColumnMetadata, TableParams};
    use indexmap::IndexMap;
    use std::collections::{HashMap, HashSet};
    use uuid::Uuid;

    fn column(name: &str, kind: ColumnKind, ty: &str) -> ColumnMetadata {
        ColumnMetadata {
            name: name.to_string(),
            kind,
            position: 0,
            column_type: ty.to_string(),
            clustering_order: ClusteringOrder::None,
            mask: None,
        }
    }

    /// Base table `ks.t`: PK = (p), clustering = (c), regular `v`.
    fn base_table() -> TableMetadata {
        let mut columns = IndexMap::new();
        columns.insert(
            "p".to_string(),
            column("p", ColumnKind::PartitionKey, "uuid"),
        );
        columns.insert("c".to_string(), column("c", ColumnKind::Clustering, "text"));
        columns.insert("v".to_string(), column("v", ColumnKind::Regular, "text"));
        TableMetadata {
            keyspace: "ks".into(),
            name: "t".into(),
            id: Uuid::nil(),
            columns,
            partition_key: vec!["p".into()],
            clustering_key: vec![("c".into(), ClusteringOrder::Asc)],
            params: TableParams::default(),
            flags: HashSet::new(),
            extensions: HashMap::new(),
            is_system: false,
        }
    }

    fn vc(name: &str, source: ColumnSource) -> ViewColumn {
        ViewColumn {
            name: name.into(),
            source,
        }
    }

    /// Valid view `ks.mv`: re-partition base by `c` then `p`.
    fn valid_view() -> ViewMetadata {
        ViewMetadata {
            keyspace: "ks".into(),
            name: "mv".into(),
            kind: ViewKind::Incremental,
            base_keyspace: "ks".into(),
            base_table: "t".into(),
            base_table_id: Uuid::nil(),
            id: Uuid::nil(),
            selected: vec![
                vc("c", ColumnSource::Base("c".into())),
                vc("p", ColumnSource::Base("p".into())),
                vc("v", ColumnSource::Base("v".into())),
            ],
            partition_key: vec!["c".into()],
            clustering_key: vec!["p".into()],
            predicate: ViewPredicate {
                not_null: vec!["c".into(), "p".into()],
                extra: None,
            },
            include_all_columns: false,
        }
    }

    #[test]
    fn accepts_minimal_valid_view() {
        assert_eq!(
            validate_view_def(&base_table(), &valid_view(), false),
            Ok(())
        );
    }

    #[test]
    fn accepts_predicate_where() {
        let mut view = valid_view();
        view.predicate.extra = Some("v > 0".into());
        assert_eq!(validate_view_def(&base_table(), &view, false), Ok(()));
    }

    #[test]
    fn rejects_view_pk_missing_base_pk_col() {
        let mut view = valid_view();
        // Drop base PK column `p` from the view PK.
        view.clustering_key.clear();
        view.predicate.not_null = vec!["c".into()];
        assert_eq!(
            validate_view_def(&base_table(), &view, false),
            Err(ViewDefError::ViewPkMissingBasePkColumn { column: "p".into() })
        );
    }

    #[test]
    fn rejects_two_extra_non_pk_pk_cols() {
        let mut base = base_table();
        base.columns
            .insert("x".into(), column("x", ColumnKind::Regular, "text"));
        base.columns
            .insert("y".into(), column("y", ColumnKind::Regular, "text"));
        let mut view = valid_view();
        view.selected.push(vc("x", ColumnSource::Base("x".into())));
        view.selected.push(vc("y", ColumnSource::Base("y".into())));
        view.clustering_key = vec!["p".into(), "x".into(), "y".into()];
        view.predicate.not_null = vec!["c".into(), "p".into(), "x".into(), "y".into()];
        match validate_view_def(&base, &view, false) {
            Err(ViewDefError::TooManyExtraPkColumns { extra }) => {
                assert_eq!(extra.len(), 2);
            }
            other => panic!("expected TooManyExtraPkColumns, got {other:?}"),
        }
    }

    #[test]
    fn requires_is_not_null_on_view_pk() {
        let mut view = valid_view();
        view.predicate.not_null = vec!["c".into()]; // missing `p`
        assert_eq!(
            validate_view_def(&base_table(), &view, false),
            Err(ViewDefError::ViewPkColumnNotNullRequired { column: "p".into() })
        );
    }

    #[test]
    fn rejects_aggregate_select() {
        let mut view = valid_view();
        view.selected.push(vc(
            "cnt",
            ColumnSource::Aggregate {
                function: "count".into(),
                arg: "v".into(),
            },
        ));
        assert_eq!(
            validate_view_def(&base_table(), &view, false),
            Err(ViewDefError::AggregateNotAllowed {
                column: "cnt".into()
            })
        );
    }

    #[test]
    fn rejects_static_column() {
        let mut base = base_table();
        base.columns
            .insert("s".into(), column("s", ColumnKind::Static, "text"));
        let mut view = valid_view();
        view.selected.push(vc("s", ColumnSource::Base("s".into())));
        assert_eq!(
            validate_view_def(&base, &view, false),
            Err(ViewDefError::StaticColumnNotAllowed { column: "s".into() })
        );
    }

    #[test]
    fn rejects_counter() {
        let mut base = base_table();
        base.flags.insert(TableFlag::Counter);
        assert_eq!(
            validate_view_def(&base, &valid_view(), false),
            Err(ViewDefError::CounterNotAllowed)
        );
    }

    #[test]
    fn rejects_chained_mv() {
        assert_eq!(
            validate_view_def(&base_table(), &valid_view(), true),
            Err(ViewDefError::ChainedViewNotAllowed)
        );
    }

    #[test]
    fn rejects_nondeterministic_udf_column() {
        let mut view = valid_view();
        view.selected.push(vc(
            "graded",
            ColumnSource::Udf {
                function: "grade".into(),
                args: vec!["v".into()],
                deterministic: false,
            },
        ));
        assert_eq!(
            validate_view_def(&base_table(), &view, false),
            Err(ViewDefError::UdfNotDeterministic {
                column: "graded".into()
            })
        );
    }

    #[test]
    fn accepts_deterministic_udf_column() {
        let mut view = valid_view();
        view.selected.push(vc(
            "graded",
            ColumnSource::Udf {
                function: "grade".into(),
                args: vec!["v".into()],
                deterministic: true,
            },
        ));
        assert_eq!(validate_view_def(&base_table(), &view, false), Ok(()));
    }

    #[test]
    fn rejects_udf_column_in_pk() {
        let mut view = valid_view();
        // A deterministic computed column placed in the view PK is still rejected.
        view.selected.push(vc(
            "bucket",
            ColumnSource::Udf {
                function: "bucket".into(),
                args: vec!["v".into()],
                deterministic: true,
            },
        ));
        view.clustering_key = vec!["p".into(), "bucket".into()];
        view.predicate.not_null = vec!["c".into(), "p".into(), "bucket".into()];
        assert_eq!(
            validate_view_def(&base_table(), &view, false),
            Err(ViewDefError::UdfColumnInPrimaryKey {
                column: "bucket".into()
            })
        );
    }
}
