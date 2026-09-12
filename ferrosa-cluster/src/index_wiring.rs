//! Module: Build a secondary index into the local storage engine when index
//! DDL is applied, wherever that application happens.
//! Correctness: correct when a node that records an index in its schema also
//! has it on its table, choosing the same engine call the executing node
//! chooses for that column's kind, and saying so loudly when it cannot.
//! Last revised: 2026-09-12
//! Last changed: Created, so a replicated CREATE INDEX builds the index on the
//! node receiving it (t_1f2741a0).
//!
//! ## Why this exists
//!
//! Applying a replicated `CREATE INDEX` used to update the schema registry and
//! write the system-table row, and stop. Only the node that EXECUTED the DDL
//! called `engine.add_index`. Every other node ended up with an index its
//! schema listed and its table did not have, and a read there refused it:
//!
//! ```text
//! secondary index 'idx_entity_by_tenant' is not declared on this node's table,
//! so it cannot answer; refusing to report its absence as zero matching rows
//! ```
//!
//! That is what took ferrosa-memory's entity streams down on `main` and on
//! every open PR of that repo (t_12457d3e). It looked intermittent because a
//! restart repairs it: `reload_indexes_from_system_schema` rebuilds indexes
//! from the system table at boot, so the index became real at the next restart
//! rather than at `CREATE INDEX`.
//!
//! The asymmetry was visible in one screen of `ddl_path.rs`: `DropIndex` called
//! `engine.drop_index`, `CreateIndex` did not call `add_index`.
//!
//! ## Choosing the call
//!
//! An index's target column decides which engine call carries it, and getting
//! this wrong is as bad as not wiring it at all — a partition-key column has no
//! storage-cell ordinal, so registering it as a cell index leaves it
//! permanently empty:
//!
//! | target column is | call |
//! |---|---|
//! | a stored cell (regular or static) | `add_index_with_predicate` for a partial index, else `add_index` |
//! | a clustering component | `add_clustering_index` |
//! | a partition-key component | `add_partition_key_index` |
//!
//! Vector and full-text indexes have their own sidecar builders and their own
//! DDL path; they are not wired here, and a caller that sees one is told.

use std::sync::Arc;

use ferrosa_index::IndexType;
use ferrosa_schema::metadata::table::TableMetadata;
use ferrosa_schema::IndexMetadata;
use ferrosa_storage::engine::StorageEngine;
use ferrosa_storage::TableId;

/// Whether an index kind can be carried by a key-component index.
///
/// A clustering or partition-key index decodes its value out of the key bytes,
/// which the scalar kinds understand and the specialised ones do not.
fn is_scalar_kind(index_type: IndexType) -> bool {
    matches!(
        index_type,
        IndexType::BTree | IndexType::Hash | IndexType::Composite | IndexType::Phonetic
    )
}

/// Build `index` into `engine`'s copy of `table`, if this node can.
///
/// Never fails the DDL: an index that the engine cannot wire is still recorded
/// in schema, and the next restart's reload gets another go at it. What it
/// must not do is fail silently — every path that declines to wire says why,
/// naming the index, because the symptom otherwise is a read that scans (or,
/// before the planner learned to withhold it, one that refuses).
pub fn wire_index_into_engine(
    engine: &Arc<StorageEngine>,
    table: &TableMetadata,
    index: &IndexMetadata,
) {
    let table_id = TableId::new(&index.keyspace, &index.table);
    let Some(target) = index.target_columns.first() else {
        tracing::warn!(
            index = %index.name,
            table = %table_id,
            "index NOT wired: it names no target column"
        );
        return;
    };

    if matches!(index.index_type, IndexType::Vector | IndexType::FullText) {
        tracing::debug!(
            index = %index.name,
            table = %table_id,
            kind = ?index.index_type,
            "index not wired here: vector and full-text indexes build their own sidecars"
        );
        return;
    }

    // A stored cell first: this is the ordinary case and the only one with a
    // storage-column ordinal.
    if let Some(position) = table.storage_column_index(target) {
        let position = position as usize;
        let outcome = if index.index_type == IndexType::Filtered {
            engine.add_index_with_predicate(
                &table_id,
                &index.name,
                position,
                index.index_type,
                index.filter_predicate.clone(),
            )
        } else {
            engine.add_index(&table_id, &index.name, position, index.index_type)
        };
        report(outcome, &table_id, index, "cell");
        return;
    }

    if let Some(component) = table
        .clustering_key
        .iter()
        .position(|(name, _)| name == target)
    {
        if !is_scalar_kind(index.index_type) {
            tracing::warn!(
                index = %index.name, table = %table_id, target, kind = ?index.index_type,
                "index NOT wired: a clustering-column index supports scalar kinds only, \
                 so reads on it will scan"
            );
            return;
        }
        let outcome =
            engine.add_clustering_index(&table_id, &index.name, component, index.index_type);
        report(outcome, &table_id, index, "clustering-component");
        return;
    }

    if let Some(component) = table.partition_key.iter().position(|name| name == target) {
        if !is_scalar_kind(index.index_type) {
            tracing::warn!(
                index = %index.name, table = %table_id, target, kind = ?index.index_type,
                "index NOT wired: a partition-key index supports scalar kinds only, \
                 so reads on it will scan"
            );
            return;
        }
        let outcome =
            engine.add_partition_key_index(&table_id, &index.name, component, index.index_type);
        report(outcome, &table_id, index, "partition-key-component");
        return;
    }

    tracing::warn!(
        index = %index.name, table = %table_id, target,
        "index NOT wired: its target column belongs to no index family this table \
         builds — not a stored cell, not a clustering component, not a partition-key \
         component"
    );
}

/// Log the outcome of one wiring attempt. A failure is a WARN and not an error
/// return: the schema keeps the index either way, and the alternative — failing
/// the replicated DDL on one node — leaves the cluster's schema inconsistent.
fn report(
    outcome: ferrosa_common::Result<()>,
    table_id: &TableId,
    index: &IndexMetadata,
    family: &str,
) {
    match outcome {
        Ok(()) => tracing::debug!(
            index = %index.name, table = %table_id, family,
            "index wired into this node's storage engine"
        ),
        Err(error) => tracing::warn!(
            %error, index = %index.name, table = %table_id, family,
            "index NOT wired into this node's storage engine: it is recorded in schema \
             but this node's table does not have it, so reads here will scan until a \
             restart's index reload builds it"
        ),
    }
}
