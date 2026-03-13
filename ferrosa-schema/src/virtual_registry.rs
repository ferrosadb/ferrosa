//! Registry for virtual tables, keyed by `(keyspace, table_name)`.
//!
//! [`VirtualTableRegistry`] stores [`VirtualTable`] implementations as
//! `Arc<dyn VirtualTable>` and uses [`ArcSwap`] for lock-free reads. Writers
//! clone the current map, insert, and swap — readers never block.

use crate::virtual_table::VirtualTable;
use arc_swap::ArcSwap;
use std::collections::HashMap;
use std::sync::Arc;

type TableKey = (String, String);

/// Lock-free registry mapping `(keyspace, table_name)` to virtual table
/// implementations.
///
/// Reads are fully lock-free via [`ArcSwap`]. Writes clone the current
/// snapshot, mutate, and swap — consistent with the pattern used in
/// [`crate::registry::Schema`].
pub struct VirtualTableRegistry {
    tables: ArcSwap<HashMap<TableKey, Arc<dyn VirtualTable>>>,
}

impl VirtualTableRegistry {
    /// Create an empty registry.
    pub fn new() -> Self {
        Self {
            tables: ArcSwap::new(Arc::new(HashMap::new())),
        }
    }

    /// Register a virtual table. If a table with the same `(keyspace, name)`
    /// already exists it is replaced.
    pub fn register(&self, table: Arc<dyn VirtualTable>) {
        let key = (table.keyspace().to_string(), table.name().to_string());
        let mut new_map = (**self.tables.load()).clone();
        new_map.insert(key, table);
        self.tables.store(Arc::new(new_map));
    }

    /// Look up a single table by keyspace and name.
    ///
    /// Returns `None` if no matching table is registered.
    pub fn get(&self, keyspace: &str, table_name: &str) -> Option<Arc<dyn VirtualTable>> {
        let guard = self.tables.load();
        guard
            .get(&(keyspace.to_string(), table_name.to_string()))
            .cloned()
    }

    /// Return all tables registered under `keyspace`.
    pub fn list(&self, keyspace: &str) -> Vec<Arc<dyn VirtualTable>> {
        let guard = self.tables.load();
        guard
            .iter()
            .filter(|((ks, _), _)| ks == keyspace)
            .map(|(_, table)| table.clone())
            .collect()
    }
}

impl Default for VirtualTableRegistry {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::virtual_table::{RowPredicate, SubscriptionMode, VirtualColumnDef, VirtualRow};
    use std::sync::Arc;

    struct StubTable {
        name: &'static str,
    }

    impl VirtualTable for StubTable {
        fn name(&self) -> &str {
            self.name
        }

        fn keyspace(&self) -> &str {
            "system_observability"
        }

        fn columns(&self) -> &[VirtualColumnDef] {
            &[]
        }

        fn primary_key_columns(&self) -> &[usize] {
            &[]
        }

        fn read(&self, _: Option<&RowPredicate>) -> Vec<VirtualRow> {
            vec![]
        }

        fn subscription_mode(&self) -> SubscriptionMode {
            SubscriptionMode::Pollable
        }
    }

    #[test]
    fn register_and_lookup() {
        let registry = VirtualTableRegistry::new();
        registry.register(Arc::new(StubTable {
            name: "connections",
        }));
        let found = registry.get("system_observability", "connections");
        assert!(found.is_some());
        assert_eq!(found.unwrap().name(), "connections");
    }

    #[test]
    fn lookup_missing_returns_none() {
        let registry = VirtualTableRegistry::new();
        assert!(registry
            .get("system_observability", "nonexistent")
            .is_none());
    }

    #[test]
    fn list_tables_in_keyspace() {
        let registry = VirtualTableRegistry::new();
        registry.register(Arc::new(StubTable {
            name: "connections",
        }));
        registry.register(Arc::new(StubTable {
            name: "storage_stats",
        }));
        let tables = registry.list("system_observability");
        assert_eq!(tables.len(), 2);
    }

    #[test]
    fn list_empty_keyspace_returns_empty() {
        let registry = VirtualTableRegistry::new();
        registry.register(Arc::new(StubTable {
            name: "connections",
        }));
        let tables = registry.list("system_auth");
        assert!(tables.is_empty());
    }

    #[test]
    fn register_replaces_existing() {
        let registry = VirtualTableRegistry::new();
        registry.register(Arc::new(StubTable {
            name: "connections",
        }));
        registry.register(Arc::new(StubTable {
            name: "connections",
        }));
        // Should still be exactly one entry after replacement.
        let tables = registry.list("system_observability");
        assert_eq!(tables.len(), 1);
    }

    #[test]
    fn default_produces_empty_registry() {
        let registry = VirtualTableRegistry::default();
        assert!(registry.get("system_observability", "anything").is_none());
        assert!(registry.list("system_observability").is_empty());
    }
}
