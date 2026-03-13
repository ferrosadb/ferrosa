//! Prepared statement cache using moka W-TinyLFU eviction.

use crate::ast::Statement;
use crate::types::CqlType;
use md5::{Digest, Md5};
use moka::sync::Cache;
use std::sync::Arc;

/// A cached prepared statement plan.
#[derive(Debug, Clone)]
pub struct PreparedPlan {
    pub id: [u8; 16],
    pub query: String,
    pub statement: Statement,
    pub keyspace: Option<String>,
    pub result_columns: Vec<(String, CqlType)>,
    pub bound_columns: Vec<(String, CqlType)>,
    pub table_keyspace: String,
    pub table_name: String,
}

/// Thread-safe prepared statement cache.
///
/// Uses moka's W-TinyLFU (Window Tiny Least Frequently Used) eviction policy
/// for lock-free reads on the EXECUTE hot path. Weight-based capacity
/// ensures memory stays bounded regardless of how many statements are cached.
pub struct PreparedCache {
    cache: Cache<[u8; 16], Arc<PreparedPlan>>,
}

impl PreparedCache {
    /// Create a new cache with the given maximum weight in bytes.
    pub fn new(max_weight: u64) -> Self {
        let cache = Cache::builder()
            .weigher(|_key: &[u8; 16], value: &Arc<PreparedPlan>| -> u32 {
                // Approximate size: query string + fixed overhead
                (value.query.len() + std::mem::size_of::<PreparedPlan>() + 128) as u32
            })
            .max_capacity(max_weight)
            .build();
        Self { cache }
    }

    /// Look up a prepared plan by its ID.
    pub fn get(&self, id: &[u8; 16]) -> Option<Arc<PreparedPlan>> {
        self.cache.get(id)
    }

    /// Insert a prepared plan into the cache.
    pub fn insert(&self, plan: PreparedPlan) {
        let id = plan.id;
        self.cache.insert(id, Arc::new(plan));
    }

    /// Invalidate all cached plans (e.g., after schema change).
    pub fn invalidate_all(&self) {
        self.cache.invalidate_all();
    }

    /// Compute the MD5 ID for a query string (CQL protocol convention).
    pub fn compute_id(query: &str) -> [u8; 16] {
        let mut hasher = Md5::new();
        hasher.update(query.as_bytes());
        let result = hasher.finalize();
        let mut id = [0u8; 16];
        id.copy_from_slice(&result);
        id
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ast::*;

    fn test_plan(query: &str) -> PreparedPlan {
        PreparedPlan {
            id: PreparedCache::compute_id(query),
            query: query.to_string(),
            statement: Statement::Select(SelectStatement {
                keyspace: Some("ks".into()),
                table: "t".into(),
                columns: vec![SelectColumn::Star],
                where_clauses: vec![],
                order_by: vec![],
                limit: None,
                allow_filtering: false,
            }),
            keyspace: Some("ks".into()),
            result_columns: vec![("id".into(), CqlType::Int)],
            bound_columns: vec![],
            table_keyspace: "ks".into(),
            table_name: "t".into(),
        }
    }

    #[test]
    fn compute_id_deterministic() {
        let id1 = PreparedCache::compute_id("SELECT * FROM users");
        let id2 = PreparedCache::compute_id("SELECT * FROM users");
        assert_eq!(id1, id2);
    }

    #[test]
    fn compute_id_different_queries() {
        let id1 = PreparedCache::compute_id("SELECT * FROM users");
        let id2 = PreparedCache::compute_id("SELECT * FROM orders");
        assert_ne!(id1, id2);
    }

    #[test]
    fn insert_and_get() {
        let cache = PreparedCache::new(10 * 1024 * 1024);
        let plan = test_plan("SELECT * FROM t");
        let id = plan.id;
        cache.insert(plan);
        assert!(cache.get(&id).is_some());
    }

    #[test]
    fn get_missing_returns_none() {
        let cache = PreparedCache::new(10 * 1024 * 1024);
        assert!(cache.get(&[0u8; 16]).is_none());
    }

    #[test]
    fn invalidate_all_clears() {
        let cache = PreparedCache::new(10 * 1024 * 1024);
        let plan = test_plan("SELECT * FROM t");
        let id = plan.id;
        cache.insert(plan);
        cache.invalidate_all();
        assert!(cache.get(&id).is_none());
    }
}
