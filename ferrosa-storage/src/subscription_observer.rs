//! SubscriptionObserver: dynamic write observer for SUBSCRIBE queries.
//!
//! When a client issues a CQL SUBSCRIBE on a query, the tables that query
//! touches are registered with this observer. The observer's
//! [`watches_table()`](SubscriptionObserver::watches_table) returns `true`
//! only for tables that have at least one active subscription.
//!
//! `on_write()` returns an empty vec — the actual notification plumbing
//! will be wired via tokio channels in T19.

use crate::commitlog::config::TableId;
use crate::commitlog::mutation::Mutation;
use crate::observer::{ObserverMode, WriteObserver};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::RwLock;

/// Identifies a subscription.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct SubscriptionId(u64);

/// Filter describing which tables a subscription watches.
pub struct SubscriptionFilter {
    pub tables: Vec<TableId>,
}

/// Configuration for the subscription observer.
pub struct SubscriptionConfig {
    // Placeholder for future config (e.g., max subscriptions, channel capacity).
}

impl SubscriptionConfig {
    /// Create a config suitable for tests.
    pub fn test_config() -> Self {
        Self {}
    }
}

/// A [`WriteObserver`] that manages dynamic subscriptions.
///
/// Subscriptions are registered and deregistered at runtime. The observer
/// maintains a ref-counted map of watched tables so that `watches_table()`
/// is O(1) and lock-free for the fast path (a single `RwLock` read).
pub struct SubscriptionObserver {
    subscriptions: RwLock<HashMap<SubscriptionId, SubscriptionFilter>>,
    /// Ref-counted table watch set: TableId -> number of subscriptions watching it.
    table_watch_counts: RwLock<HashMap<TableId, usize>>,
    next_id: AtomicU64,
}

impl SubscriptionObserver {
    pub fn new(_config: SubscriptionConfig) -> Self {
        Self {
            subscriptions: RwLock::new(HashMap::new()),
            table_watch_counts: RwLock::new(HashMap::new()),
            next_id: AtomicU64::new(1),
        }
    }

    /// Register a new subscription. Returns a [`SubscriptionId`] that can be
    /// passed to [`deregister()`](Self::deregister) to remove it.
    pub fn register(&self, filter: SubscriptionFilter) -> SubscriptionId {
        let id = SubscriptionId(self.next_id.fetch_add(1, Ordering::Relaxed));

        // Increment ref counts for watched tables.
        {
            let mut counts = self.table_watch_counts.write().unwrap();
            for table in &filter.tables {
                *counts.entry(table.clone()).or_insert(0) += 1;
            }
        }

        self.subscriptions.write().unwrap().insert(id, filter);
        id
    }

    /// Remove a subscription. Decrements ref counts and stops watching tables
    /// that no longer have any subscribers.
    pub fn deregister(&self, id: SubscriptionId) {
        if let Some(filter) = self.subscriptions.write().unwrap().remove(&id) {
            let mut counts = self.table_watch_counts.write().unwrap();
            for table in &filter.tables {
                if let Some(count) = counts.get_mut(table) {
                    *count -= 1;
                    if *count == 0 {
                        counts.remove(table);
                    }
                }
            }
        }
    }

    /// Returns `true` if at least one subscription is watching `table`.
    pub fn watches_table(&self, table: &TableId) -> bool {
        self.table_watch_counts.read().unwrap().contains_key(table)
    }
}

impl WriteObserver for SubscriptionObserver {
    fn mode(&self) -> ObserverMode {
        ObserverMode::Async
    }

    fn tables(&self) -> Vec<TableId> {
        self.table_watch_counts
            .read()
            .unwrap()
            .keys()
            .cloned()
            .collect()
    }

    fn on_write(&self, _table: &TableId, _mutation: &Mutation) -> Vec<Mutation> {
        // Notification happens via async channels (wired in T19).
        // The observer just signals interest via watches_table().
        vec![]
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn register_and_deregister_subscription() {
        let obs = SubscriptionObserver::new(SubscriptionConfig::test_config());
        let sub_id = obs.register(SubscriptionFilter {
            tables: vec![TableId::new("ks", "users")],
        });
        assert!(obs.watches_table(&TableId::new("ks", "users")));
        obs.deregister(sub_id);
        assert!(!obs.watches_table(&TableId::new("ks", "users")));
    }

    #[test]
    fn watches_table_is_dynamic() {
        let obs = SubscriptionObserver::new(SubscriptionConfig::test_config());
        assert!(!obs.watches_table(&TableId::new("ks", "users")));
        let id = obs.register(SubscriptionFilter {
            tables: vec![TableId::new("ks", "users")],
        });
        assert!(obs.watches_table(&TableId::new("ks", "users")));
        assert!(!obs.watches_table(&TableId::new("ks", "orders")));
        obs.deregister(id);
        assert!(!obs.watches_table(&TableId::new("ks", "users")));
    }

    #[test]
    fn on_write_returns_empty() {
        use ferrosa_common::key::{DecoratedKey, PartitionKey};

        let obs = SubscriptionObserver::new(SubscriptionConfig::test_config());
        obs.register(SubscriptionFilter {
            tables: vec![TableId::new("ks", "users")],
        });
        let mutation = Mutation {
            keyspace: "ks".to_string(),
            table: "users".to_string(),
            key: DecoratedKey::new(PartitionKey::new(b"pk".to_vec())),
            rows: vec![],
            timestamp: 1000,
        };
        let derived = obs.on_write(&TableId::new("ks", "users"), &mutation);
        assert!(derived.is_empty());
    }

    #[test]
    fn multiple_subscriptions_same_table() {
        let obs = SubscriptionObserver::new(SubscriptionConfig::test_config());
        let id1 = obs.register(SubscriptionFilter {
            tables: vec![TableId::new("ks", "users")],
        });
        let id2 = obs.register(SubscriptionFilter {
            tables: vec![TableId::new("ks", "users")],
        });
        assert!(obs.watches_table(&TableId::new("ks", "users")));
        obs.deregister(id1);
        // Still watching because id2 is still registered.
        assert!(obs.watches_table(&TableId::new("ks", "users")));
        obs.deregister(id2);
        assert!(!obs.watches_table(&TableId::new("ks", "users")));
    }

    #[test]
    fn observer_mode_is_async() {
        let obs = SubscriptionObserver::new(SubscriptionConfig::test_config());
        assert_eq!(obs.mode(), ObserverMode::Async);
    }

    #[test]
    fn tables_returns_watched_tables() {
        let obs = SubscriptionObserver::new(SubscriptionConfig::test_config());
        assert!(obs.tables().is_empty());

        obs.register(SubscriptionFilter {
            tables: vec![TableId::new("ks", "users"), TableId::new("ks", "orders")],
        });

        let mut watched = obs.tables();
        watched.sort_by(|a, b| a.table.cmp(&b.table));
        assert_eq!(watched.len(), 2);
        assert_eq!(watched[0], TableId::new("ks", "orders"));
        assert_eq!(watched[1], TableId::new("ks", "users"));
    }
}
