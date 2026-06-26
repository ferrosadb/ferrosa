//! Materialized-view metadata types — the schema-replicated description of a view.
//!
//! These types are the single source of truth that every frontend translates
//! its `CREATE MATERIALIZED VIEW` DDL into, and that the engine layer reads to
//! build the view's backing table and maintain it.

use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// Discriminates the maintenance model of a materialized view.
///
/// The discriminator is serialized from the very first schema revision so that
/// adding `Snapshot` (Postgres-style `REFRESH MATERIALIZED VIEW`) later is a
/// non-breaking change — old encodings only ever contain [`ViewKind::Incremental`],
/// whose serialized form is name-tagged and therefore stable when new variants
/// are added (acceptance gate G1). See `decisions.md` D1.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ViewKind {
    /// Incrementally-maintained denormalization (Cassandra-style). Implemented now.
    Incremental,
    // Snapshot — reserved for Postgres-style REFRESH views; not yet implemented.
    // Intentionally absent until the snapshot engine lands (board task t_02a5a95c).
}

/// Where a selected view column gets its value.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ColumnSource {
    /// Projects a base-table column of the given name.
    Base(String),
    /// A UDF-computed column (D4 extension). Under Accord strict-serializable
    /// maintenance (D2) a view UDF must be deterministic; a column whose
    /// `deterministic` flag is false is rejected at DDL time (gate G2).
    Udf {
        /// UDF name.
        function: String,
        /// Base-column arguments passed to the UDF.
        args: Vec<String>,
        /// Whether the UDF is proven/declared deterministic.
        deterministic: bool,
    },
    /// An aggregate over base rows. Forbidden in a materialized view (it breaks
    /// incremental maintenance); retained as a variant so validation can reject it.
    Aggregate {
        /// Aggregate function name.
        function: String,
        /// The aggregated base column.
        arg: String,
    },
}

/// One column projected by the view.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ViewColumn {
    /// Column name as it appears in the view.
    pub name: String,
    /// Where the column's value comes from.
    pub source: ColumnSource,
}

/// The view's selection predicate.
///
/// `not_null` carries the Cassandra-baseline `IS NOT NULL` requirement on every
/// view primary-key column. `extra` carries an optional ferrosa-extension
/// predicate (D4) compiled from the view's `WHERE`; its concrete representation
/// is refined when the delta-computation module lands. Its presence must not
/// cause validation to reject the view.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ViewPredicate {
    /// Columns required to be non-null (Cassandra baseline: every view-PK column).
    pub not_null: Vec<String>,
    /// Optional ferrosa-extension predicate beyond `IS NOT NULL` (D4 placeholder).
    pub extra: Option<String>,
}

/// Full description of a materialized view.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ViewMetadata {
    /// Keyspace the view lives in.
    pub keyspace: String,
    /// View name.
    pub name: String,
    /// Maintenance model discriminator (D1).
    pub kind: ViewKind,
    /// Base table keyspace.
    pub base_keyspace: String,
    /// Base table name.
    pub base_table: String,
    /// Base table id.
    pub base_table_id: Uuid,
    /// View id.
    pub id: Uuid,
    /// Selected columns (base projections and/or UDF-computed columns).
    pub selected: Vec<ViewColumn>,
    /// View partition-key column names, in order.
    pub partition_key: Vec<String>,
    /// View clustering-key column names, in order.
    pub clustering_key: Vec<String>,
    /// Selection predicate (IS NOT NULL baseline + optional extension).
    pub predicate: ViewPredicate,
    /// Whether the view projects all base columns.
    pub include_all_columns: bool,
}

impl ViewMetadata {
    /// View primary-key column names, partition columns first then clustering.
    pub fn primary_key(&self) -> impl Iterator<Item = &str> {
        self.partition_key
            .iter()
            .chain(self.clustering_key.iter())
            .map(String::as_str)
    }

    /// The source of a selected column, by name.
    pub fn source_of(&self, name: &str) -> Option<&ColumnSource> {
        self.selected
            .iter()
            .find(|c| c.name == name)
            .map(|c| &c.source)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn view_kind_incremental_roundtrips() {
        let k = ViewKind::Incremental;
        let s = serde_json::to_string(&k).unwrap();
        let back: ViewKind = serde_json::from_str(&s).unwrap();
        assert_eq!(k, back);
    }

    #[test]
    fn serialized_form_reserves_kind_discriminator() {
        // Name-tagged: a future `Snapshot` variant cannot change this encoding,
        // so adding it is a non-breaking schema change (gate G1).
        assert_eq!(
            serde_json::to_string(&ViewKind::Incremental).unwrap(),
            "\"Incremental\""
        );
    }

    #[test]
    fn primary_key_iterates_partition_then_clustering() {
        let view = ViewMetadata {
            keyspace: "ks".into(),
            name: "mv".into(),
            kind: ViewKind::Incremental,
            base_keyspace: "ks".into(),
            base_table: "t".into(),
            base_table_id: Uuid::nil(),
            id: Uuid::nil(),
            selected: vec![],
            partition_key: vec!["c".into()],
            clustering_key: vec!["p".into()],
            predicate: ViewPredicate::default(),
            include_all_columns: false,
        };
        assert_eq!(view.primary_key().collect::<Vec<_>>(), vec!["c", "p"]);
    }
}
