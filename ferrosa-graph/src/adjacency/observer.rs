//! AdjacencyIndexObserver — WriteObserver that maintains the adjacency index.
//!
//! Watches all tables with `extensions["graph.type"] == "edge"`. On each mutation,
//! extracts source and target key bytes and generates OUT and IN adjacency entries.

use std::sync::Arc;

use ferrosa_schema::Schema;
use ferrosa_storage::{Mutation, ObserverMode, TableId, WriteObserver};

use crate::adjacency::schema::{adjacency_keyspace_name, DIRECTION_IN, DIRECTION_OUT};

/// Observer that maintains per-keyspace adjacency index entries.
pub struct AdjacencyIndexObserver {
    /// Schema registry for discovering edge tables and reading extensions.
    schema: Arc<Schema>,
    /// The user keyspace this observer manages.
    keyspace: String,
}

impl AdjacencyIndexObserver {
    pub fn new(schema: Arc<Schema>, keyspace: String) -> Self {
        Self { schema, keyspace }
    }

    /// Find all edge tables in this keyspace from the current schema snapshot.
    fn edge_tables(&self) -> Vec<TableId> {
        let snap = self.schema.schema_ref().load();
        snap.tables
            .iter()
            .filter(|((ks, _), meta)| {
                ks == &self.keyspace
                    && meta.extensions.get("graph.type") == Some(&"edge".to_string())
            })
            .map(|((ks, name), _)| TableId::new(ks, name))
            .collect()
    }
}

impl WriteObserver for AdjacencyIndexObserver {
    fn mode(&self) -> ObserverMode {
        ObserverMode::Sync
    }

    fn tables(&self) -> Vec<TableId> {
        self.edge_tables()
    }

    fn on_write(&self, table: &TableId, mutation: &Mutation) -> Vec<Mutation> {
        derive_adjacency_mutations(&self.schema, table, mutation)
    }
}

pub(crate) fn derive_adjacency_mutations(
    schema: &Schema,
    table: &TableId,
    mutation: &Mutation,
) -> Vec<Mutation> {
    let snap = schema.schema_ref().load();
    let key = (table.keyspace.clone(), table.table.clone());
    let meta = match snap.tables.get(&key) {
        Some(m) => m,
        None => return vec![],
    };

    // Verify this is a valid edge table with source and target extensions.
    // Phase 1 uses partition key as source and clustering as target;
    // full column-name mapping will use these values in a later phase.
    if !meta.extensions.contains_key("graph.source")
        || !meta.extensions.contains_key("graph.target")
    {
        return vec![];
    }
    let Some(source_col) = meta.extensions.get("graph.source") else {
        return vec![];
    };
    let Some(target_col) = meta.extensions.get("graph.target") else {
        return vec![];
    };

    let adj_ks = adjacency_keyspace_name(&table.keyspace);
    // Use the graph label from schema extensions (e.g. "KNOWS") so that the
    // adjacency clustering key matches what the hop executor filters by.
    // Fall back to the table name if the extension is missing.
    let edge_label = meta
        .extensions
        .get("graph.label")
        .cloned()
        .unwrap_or_else(|| table.table.clone());
    let edge_table = format!("{}.{}", table.keyspace, table.table);

    let mut derived = Vec::new();
    for row in &mutation.rows {
        let Some(source_id) = extract_graph_column_bytes(meta, mutation, row, source_col) else {
            continue;
        };
        let Some(target_id) = extract_graph_column_bytes(meta, mutation, row, target_col) else {
            continue;
        };

        derived.push(make_adjacency_mutation(
            &adj_ks,
            &source_id,
            DIRECTION_OUT,
            &edge_label,
            &target_id,
            &edge_table,
            mutation.timestamp,
        ));
        derived.push(make_adjacency_mutation(
            &adj_ks,
            &target_id,
            DIRECTION_IN,
            &edge_label,
            &source_id,
            &edge_table,
            mutation.timestamp,
        ));
    }

    derived
}

/// Build an adjacency table mutation.
pub(crate) fn make_adjacency_mutation(
    adj_keyspace: &str,
    vertex_id: &[u8],
    direction: u8,
    edge_label: &str,
    neighbor_id: &[u8],
    edge_table: &str,
    timestamp: i64,
) -> Mutation {
    use ferrosa_common::cell::CellValue;
    use ferrosa_common::key::{DecoratedKey, PartitionKey};
    use ferrosa_sstable::types::{DeletionTime, LivenessInfo, Row};

    let key = DecoratedKey::new(PartitionKey::new(vertex_id.to_vec()));

    // Clustering uses standard composite layout: each component is
    // [u16 BE length][bytes]. Required by the SSTable writer's multi-column
    // composite parser (and Gate A `validate_clustering_shape`). Components:
    //   0: direction (tinyint, 1 byte)
    //   1: edge_label (text)
    //   2: neighbor_id (blob)
    let mut clustering = Vec::new();
    clustering.extend_from_slice(&1u16.to_be_bytes()); // direction is 1 byte
    clustering.push(direction);
    clustering.extend_from_slice(&(edge_label.len() as u16).to_be_bytes());
    clustering.extend_from_slice(edge_label.as_bytes());
    clustering.extend_from_slice(&(neighbor_id.len() as u16).to_be_bytes());
    clustering.extend_from_slice(neighbor_id);

    let row = Row {
        clustering,
        cells: vec![(
            0,
            CellValue::live(edge_table.as_bytes().to_vec(), timestamp),
        )],
        deletion: DeletionTime::LIVE,
        primary_key_liveness: LivenessInfo::with_timestamp(timestamp),
    };

    Mutation::new(
        adj_keyspace.to_string(),
        "adjacency".to_string(),
        key,
        vec![row],
        timestamp,
    )
}

fn extract_graph_column_bytes(
    meta: &ferrosa_schema::metadata::table::TableMetadata,
    mutation: &Mutation,
    row: &ferrosa_sstable::types::Row,
    column_name: &str,
) -> Option<Vec<u8>> {
    let column = meta.columns.get(column_name)?;
    match column.kind {
        ferrosa_schema::metadata::column::ColumnKind::PartitionKey => {
            let idx = meta
                .partition_key
                .iter()
                .position(|name| name == column_name)?;
            let components =
                decode_partition_components(mutation.key.key.as_bytes(), meta.partition_key.len())?;
            components.get(idx).cloned()
        }
        ferrosa_schema::metadata::column::ColumnKind::Clustering => {
            let idx = meta
                .clustering_key
                .iter()
                .position(|(name, _)| name == column_name)?;
            let components =
                decode_clustering_components(&row.clustering, meta.clustering_key.len())?;
            components.get(idx).cloned()
        }
        ferrosa_schema::metadata::column::ColumnKind::Regular
        | ferrosa_schema::metadata::column::ColumnKind::Static => {
            let mut regular_idx = 0u16;
            for meta_col in meta.columns.values() {
                match meta_col.kind {
                    ferrosa_schema::metadata::column::ColumnKind::Regular
                    | ferrosa_schema::metadata::column::ColumnKind::Static => {
                        if meta_col.name == column_name {
                            return row
                                .cells
                                .iter()
                                .find(|(idx, _)| *idx == regular_idx)
                                .and_then(|(_, cell)| cell.value.clone());
                        }
                        regular_idx += 1;
                    }
                    _ => {}
                }
            }
            None
        }
    }
}

fn decode_partition_components(key: &[u8], count: usize) -> Option<Vec<Vec<u8>>> {
    if count == 1 {
        return Some(vec![key.to_vec()]);
    }

    let mut offset = 0usize;
    let mut out = Vec::with_capacity(count);
    for _ in 0..count {
        if offset + 2 > key.len() {
            return None;
        }
        let len = u16::from_be_bytes([key[offset], key[offset + 1]]) as usize;
        offset += 2;
        if offset + len > key.len() {
            return None;
        }
        out.push(key[offset..offset + len].to_vec());
        offset += len;
        if offset >= key.len() {
            return None;
        }
        offset += 1; // composite end-of-component marker
    }
    Some(out)
}

fn decode_clustering_components(clustering: &[u8], count: usize) -> Option<Vec<Vec<u8>>> {
    if count == 1 {
        return Some(vec![clustering.to_vec()]);
    }

    let mut offset = 0usize;
    let mut out = Vec::with_capacity(count);
    for _ in 0..count {
        if offset + 2 > clustering.len() {
            return None;
        }
        let len = u16::from_be_bytes([clustering[offset], clustering[offset + 1]]) as usize;
        offset += 2;
        if offset + len > clustering.len() {
            return None;
        }
        out.push(clustering[offset..offset + len].to_vec());
        offset += len;
    }
    Some(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn make_adjacency_mutation_out() {
        let m = make_adjacency_mutation(
            "system_graph_social",
            b"alice",
            DIRECTION_OUT,
            "knows",
            b"bob",
            "social.knows",
            1000,
        );
        assert_eq!(m.keyspace, "system_graph_social");
        assert_eq!(m.table, "adjacency");
        assert_eq!(m.key.key.as_bytes(), b"alice");
        assert_eq!(m.rows.len(), 1);
        assert_eq!(m.rows[0].clustering[0], DIRECTION_OUT);
    }

    #[test]
    fn make_adjacency_mutation_in() {
        let m = make_adjacency_mutation(
            "system_graph_social",
            b"bob",
            DIRECTION_IN,
            "knows",
            b"alice",
            "social.knows",
            1000,
        );
        // Clustering is now standard composite: [u16 1][1B direction][...].
        // The direction byte sits at offset 2 (after the u16 length prefix).
        assert_eq!(m.rows[0].clustering[2], DIRECTION_IN);
    }

    /// Pin the adjacency clustering wire format. The SSTable writer's
    /// multi-column composite parser (and Gate A in
    /// `ferrosa-sstable/src/writer.rs::validate_clustering_shape`) requires
    /// every component to be `[u16 BE length][bytes]`. Adjacency must follow
    /// that layout or the writer rejects the row at flush time:
    /// "truncated composite clustering — column 0 (ByteType) length prefix
    ///  claims 256 bytes but only 52 remain"
    /// (the old custom `[1B direction][u16][label][u16][nid]` layout caused
    /// the writer to read the direction+label_len_high as a 256-byte prefix.)
    #[test]
    fn adjacency_clustering_uses_standard_composite_layout() {
        let m = make_adjacency_mutation(
            "system_graph_social",
            b"alice",
            DIRECTION_OUT,
            "knows",
            b"bob",
            "social.knows",
            1000,
        );
        let c = &m.rows[0].clustering;
        // Component 0 (direction tinyint): [u16 1][1B]
        assert_eq!(&c[0..2], &[0x00, 0x01], "direction must be u16-prefixed");
        assert_eq!(c[2], DIRECTION_OUT, "direction value at offset 2");
        // Component 1 (edge_label text): [u16 5][b'k'..]
        assert_eq!(&c[3..5], &[0x00, 0x05], "edge_label u16 length prefix");
        assert_eq!(&c[5..10], b"knows", "edge_label bytes");
        // Component 2 (neighbor_id blob): [u16 3][b'b'..]
        assert_eq!(&c[10..12], &[0x00, 0x03], "neighbor_id u16 length prefix");
        assert_eq!(&c[12..15], b"bob", "neighbor_id bytes");
        assert_eq!(c.len(), 15, "exact composite length");
    }
}
