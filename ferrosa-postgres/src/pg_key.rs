//! The PostgreSQL **declared** primary key of a table, which is not the same thing as
//! ferrosa's storage key.
//!
//! For a table that declared `PRIMARY KEY (aid)`, ferrosa stores `aid` as its partition key
//! and the two coincide. They come apart in two cases, and both are why this is recorded
//! rather than derived:
//!
//! - a table created with **no** `PRIMARY KEY` gets a synthetic `_sys_ck_` partition key
//!   (see `ferrosa_common::timeuuid::SYNTHETIC_KEY_COLUMN`), but PostgreSQL would report
//!   **no** primary key for it — the declared key is empty, not `_sys_ck_`;
//! - a later `ALTER TABLE ... ADD PRIMARY KEY (aid)` gives PostgreSQL a key that ferrosa
//!   does not store as its partition key.
//!
//! So the declared key lives in [`TableMetadata::extensions`] under [`PRIMARY_KEY_EXTENSION`],
//! alongside the other namespaced extension keys this workspace already uses
//! (`graph.label`, `graph.source`, …). Clients introspecting the catalog (`psql`'s `\d`,
//! an ORM reading `pg_index`) must see the declared key, never the storage key.
//!
//! [`TableMetadata::extensions`]: ferrosa_schema::TableMetadata::extensions

use ferrosa_common::timeuuid::is_reserved_column_name;
use ferrosa_schema::TableMetadata;

/// Extension key holding the declared primary-key columns, comma-separated in key order.
///
/// Absent means "no primary key was declared" — which is a different statement from "a key
/// whose columns happen to be empty", and the two are never conflated.
pub const PRIMARY_KEY_EXTENSION: &str = "pg.primary_key";

/// The primary key PostgreSQL should report for `meta`: the declared one when it was
/// recorded, otherwise one derived from the storage key.
///
/// The derivation exists for tables ferrosa created outside the Postgres front end (CQL DDL),
/// which have no declared key to record. It must never leak the synthetic key: a reserved
/// `_sys_` column is ferrosa's own and is not a PostgreSQL primary key, so it is filtered out
/// of the derived result. Filtering is right for the derived path only — a *recorded* key is
/// returned as-is, because it is what the user actually declared.
pub fn of(meta: &TableMetadata) -> Vec<String> {
    if let Some(recorded) = recorded(meta) {
        return recorded;
    }
    storage_key_columns(meta)
}

/// The columns ferrosa actually keys rows by: partition then clustering, with ferrosa's own
/// reserved `_sys_` columns filtered out.
///
/// An `ALTER TABLE ... ADD PRIMARY KEY` compares the key it was given against this to decide
/// whether a secondary index would add anything: a key that already *is* the storage key is
/// served by the primary structure, so indexing it again would be pure write overhead.
pub fn storage_key_columns(meta: &TableMetadata) -> Vec<String> {
    meta.partition_key
        .iter()
        .chain(meta.clustering_key.iter().map(|(name, _)| name))
        .filter(|name| !is_reserved_column_name(name))
        .cloned()
        .collect()
}

/// The primary key as declared, or `None` when none was recorded.
///
/// An empty declaration parses back to `None`, not `Some(vec![])`: a client must not be able
/// to tell the difference between "no key declared" and "a key with no columns" because the
/// latter cannot exist.
pub fn recorded(meta: &TableMetadata) -> Option<Vec<String>> {
    let raw = meta.extensions.get(PRIMARY_KEY_EXTENSION)?;
    let columns: Vec<String> = raw
        .split(',')
        .map(str::trim)
        .filter(|c| !c.is_empty())
        .map(str::to_string)
        .collect();
    if columns.is_empty() {
        None
    } else {
        Some(columns)
    }
}

/// Render `columns` for storage in [`PRIMARY_KEY_EXTENSION`].
pub fn encode(columns: &[String]) -> String {
    columns.join(",")
}

#[cfg(test)]
mod tests {
    use super::*;
    use ferrosa_schema::{ClusteringOrder, ColumnKind, ColumnMetadata, TableParams};
    use indexmap::IndexMap;
    use std::collections::{HashMap, HashSet};

    fn column(name: &str, kind: ColumnKind) -> ColumnMetadata {
        ColumnMetadata {
            name: name.to_string(),
            kind,
            position: 0,
            column_type: "text".to_string(),
            clustering_order: ClusteringOrder::None,
            mask: None,
        }
    }

    fn table(partition: &[&str], clustering: &[&str], extension: Option<&str>) -> TableMetadata {
        let mut columns = IndexMap::new();
        for name in partition.iter().chain(clustering) {
            let kind = if partition.contains(name) {
                ColumnKind::PartitionKey
            } else {
                ColumnKind::Clustering
            };
            columns.insert(name.to_string(), column(name, kind));
        }
        let mut extensions = HashMap::new();
        if let Some(value) = extension {
            extensions.insert(PRIMARY_KEY_EXTENSION.to_string(), value.to_string());
        }
        TableMetadata {
            keyspace: "public".to_string(),
            name: "t".to_string(),
            id: uuid::Uuid::nil(),
            columns,
            partition_key: partition.iter().map(|s| s.to_string()).collect(),
            clustering_key: clustering
                .iter()
                .map(|s| (s.to_string(), ClusteringOrder::Asc))
                .collect(),
            params: TableParams::default(),
            flags: HashSet::new(),
            extensions,
            is_system: false,
        }
    }

    /// A recorded key wins, in the order it was declared.
    #[test]
    fn a_recorded_key_is_reported_as_declared() {
        let meta = table(&["aid"], &[], Some("aid,bid"));
        assert_eq!(of(&meta), vec!["aid".to_string(), "bid".to_string()]);
        assert_eq!(recorded(&meta), Some(vec!["aid".into(), "bid".into()]));
    }

    /// With nothing recorded, the storage key is derived — the path for a table created
    /// through CQL, which has no declared Postgres key.
    #[test]
    fn without_a_record_the_storage_key_is_derived() {
        let meta = table(&["k"], &["c"], None);
        assert_eq!(of(&meta), vec!["k".to_string(), "c".to_string()]);
    }

    /// The synthetic key must never be reported as a Postgres primary key. This is the case
    /// the recording exists for: PostgreSQL would say such a table has NO primary key, and it
    /// would be wrong to show it a ferrosa-internal column instead.
    #[test]
    fn the_synthetic_key_is_never_reported_as_a_primary_key() {
        let meta = table(&[ferrosa_common::timeuuid::SYNTHETIC_KEY_COLUMN], &[], None);
        assert!(
            of(&meta).is_empty(),
            "a PK-less table has no Postgres primary key, synthetic or otherwise"
        );
    }

    /// An empty recording reads back as "none", never as a key with no columns.
    #[test]
    fn an_empty_recording_means_none() {
        for raw in ["", "  ", ",", " , "] {
            let meta = table(&["k"], &[], Some(raw));
            assert_eq!(recorded(&meta), None, "{raw:?} must read as no key");
        }
    }

    /// Round-trips, including the whitespace that would otherwise become a column name.
    #[test]
    fn encode_and_recorded_round_trip() {
        let columns = vec!["aid".to_string(), "bid".to_string()];
        let meta = table(&["aid"], &[], Some(&encode(&columns)));
        assert_eq!(recorded(&meta), Some(columns));
    }
}
