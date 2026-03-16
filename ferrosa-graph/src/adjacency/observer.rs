//! AdjacencyIndexObserver — async WriteObserver that maintains the adjacency index.
//!
//! Watches all tables with `extensions["graph.type"] == "edge"`. On each mutation,
//! extracts source and target key bytes and generates OUT and IN adjacency entries.

use std::sync::Arc;

use ferrosa_schema::Schema;
use ferrosa_storage::{Mutation, ObserverMode, TableId, WriteObserver};

use crate::adjacency::schema::{adjacency_keyspace_name, DIRECTION_IN, DIRECTION_OUT};

/// Async observer that maintains per-keyspace adjacency index entries.
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
        ObserverMode::Async
    }

    fn tables(&self) -> Vec<TableId> {
        self.edge_tables()
    }

    fn on_write(&self, table: &TableId, mutation: &Mutation) -> Vec<Mutation> {
        let snap = self.schema.schema_ref().load();
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

        let adj_ks = adjacency_keyspace_name(&self.keyspace);
        let edge_label = table.table.clone();
        let edge_table = format!("{}.{}", table.keyspace, table.table);

        // For Phase 1: the source vertex ID is the partition key bytes,
        // the target vertex ID is extracted from clustering/cells.
        let source_id = mutation.key.key.as_bytes().to_vec();

        let mut derived = Vec::new();
        for row in &mutation.rows {
            let target_id = row.clustering.clone();

            // OUT entry: (source -> target)
            derived.push(make_adjacency_mutation(
                &adj_ks,
                &source_id,
                DIRECTION_OUT,
                &edge_label,
                &target_id,
                &edge_table,
                mutation.timestamp,
            ));

            // IN entry: (target -> source)
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

    // Clustering: direction (1 byte) + edge_label (len-prefixed) + neighbor_id (len-prefixed)
    let mut clustering = Vec::new();
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

    Mutation {
        keyspace: adj_keyspace.to_string(),
        table: "adjacency".to_string(),
        key,
        rows: vec![row],
        timestamp,
    }
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
        assert_eq!(m.rows[0].clustering[0], DIRECTION_IN);
    }
}
