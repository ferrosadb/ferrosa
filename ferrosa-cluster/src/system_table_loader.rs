//! Loads system table data from the storage engine at cold start.
//!
//! [`SystemTableLoader`] reads `system_schema.*` and `system_auth.*` tables
//! to reconstruct partial schema state. The caller validates this against
//! Raft state (Raft wins on conflict).

use std::sync::Arc;

use ferrosa_storage::engine::StorageEngine;
use ferrosa_storage::TableId;

/// Loads system table data from the storage engine at cold start.
///
/// Reads `system_schema.*` and `system_auth.*` tables to reconstruct
/// a partial schema state. The caller validates this against Raft state.
pub struct SystemTableLoader {
    engine: Arc<StorageEngine>,
}

impl SystemTableLoader {
    /// Create a new loader backed by the given storage engine.
    pub fn new(engine: Arc<StorageEngine>) -> Self {
        Self { engine }
    }

    /// Load keyspace names from `system_schema.keyspaces`.
    ///
    /// Returns the partition key (keyspace name) for each row.
    pub fn load_keyspace_names(&self) -> ferrosa_common::Result<Vec<String>> {
        let tid = TableId::new("system_schema", "keyspaces");
        let partitions = self.engine.read_range(&tid, None, None, 10_000)?;
        let names: Vec<String> = partitions
            .iter()
            .filter_map(|p| String::from_utf8(p.key.key.as_bytes().to_vec()).ok())
            .collect();
        Ok(names)
    }
}

/// Validate keyspace names from SSTables against Raft state.
///
/// Returns (validated_keyspaces, divergence_messages). Raft is authoritative:
/// - Keyspaces in Raft but not SSTables: included (will be re-written).
/// - Keyspaces in SSTables but not Raft: excluded (stale data).
pub fn validate_keyspaces_against_raft(
    sstable_keyspaces: &[String],
    raft_keyspaces: &[String],
) -> (Vec<String>, Vec<String>) {
    let raft_set: std::collections::HashSet<&String> = raft_keyspaces.iter().collect();
    let sstable_set: std::collections::HashSet<&String> = sstable_keyspaces.iter().collect();

    let mut divergences = Vec::new();

    // Keyspaces in SSTable but not in Raft (stale).
    for ks in &sstable_set {
        if !raft_set.contains(*ks) {
            divergences.push(format!(
                "keyspace '{}' found in SSTables but not in Raft state (stale, ignoring)",
                ks
            ));
        }
    }

    // Keyspaces in Raft but not in SSTable (need re-write).
    for ks in &raft_set {
        if !sstable_set.contains(*ks) {
            divergences.push(format!(
                "keyspace '{}' in Raft state but not in SSTables (will re-persist)",
                ks
            ));
        }
    }

    // Raft wins: return Raft keyspaces.
    let validated = raft_keyspaces.to_vec();
    (validated, divergences)
}

/// Report from bootstrapping system tables.
#[derive(Debug)]
pub struct BootstrapReport {
    /// Keyspaces validated against Raft.
    pub validated_keyspaces: Vec<String>,
    /// Divergence messages (logged as warnings).
    pub divergences: Vec<String>,
}

/// Bootstrap system tables: register schemas, load from SSTables, validate
/// against Raft state.
///
/// Returns a report describing what was found and any divergences. The caller
/// should log divergences as warnings and re-persist any keyspaces that exist
/// in Raft but not in SSTables.
pub fn bootstrap_system_tables(
    engine: Arc<StorageEngine>,
    raft_keyspaces: &[String],
) -> ferrosa_common::Result<BootstrapReport> {
    // Step 1: Register system table schemas (idempotent).
    engine.register_system_tables()?;

    // Step 2: Load existing keyspace names from SSTables.
    let loader = SystemTableLoader::new(Arc::clone(&engine));
    let sstable_keyspaces = loader.load_keyspace_names()?;

    // Step 3: Validate against Raft.
    let (validated, divergences) =
        validate_keyspaces_against_raft(&sstable_keyspaces, raft_keyspaces);

    // Step 4: Log divergences.
    for msg in &divergences {
        eprintln!("[system-table-bootstrap] {msg}");
    }

    Ok(BootstrapReport {
        validated_keyspaces: validated,
        divergences,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn validate_raft_wins_on_conflict() {
        let sstable_keyspaces = vec!["ks1".to_string(), "ks2".to_string()];
        let raft_keyspaces = vec!["ks1".to_string(), "ks3".to_string()];

        let (validated, divergences) =
            validate_keyspaces_against_raft(&sstable_keyspaces, &raft_keyspaces);

        assert_eq!(validated.len(), 2);
        assert!(validated.contains(&"ks1".to_string()));
        assert!(validated.contains(&"ks3".to_string()));
        assert!(!validated.contains(&"ks2".to_string()));

        // Should report divergences.
        assert!(!divergences.is_empty());
    }
}
