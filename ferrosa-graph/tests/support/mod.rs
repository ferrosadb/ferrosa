//! Shared integration-test support.
//!
//! `GraphEngine` exposes no production entry point that drains a result into a
//! `GraphResult`: transports consume `execute_stream_with_params` row by row.
//! Integration tests compare whole results, so this trait adds the buffered
//! form back for tests only, on top of the one streaming pipeline.
#![allow(dead_code)] // each test binary uses a subset

use std::collections::HashMap;

use ferrosa_graph::engine::GraphEngine;
use ferrosa_graph::error::Result;
use ferrosa_graph::executor::GraphResult;
use ferrosa_schema::auth::role::AuthContext;

#[allow(async_fn_in_trait)] // test-only, never used across a Send boundary by trait
pub trait ExecuteCollect {
    /// Execute and drain the result (test-only).
    async fn execute(&self, query: &str, keyspace: &str, auth: &AuthContext)
        -> Result<GraphResult>;

    /// Execute with parameters and drain the result (test-only).
    async fn execute_with_params(
        &self,
        query: &str,
        keyspace: &str,
        auth: &AuthContext,
        params: &HashMap<String, serde_json::Value>,
    ) -> Result<GraphResult>;
}

impl ExecuteCollect for GraphEngine {
    async fn execute(
        &self,
        query: &str,
        keyspace: &str,
        auth: &AuthContext,
    ) -> Result<GraphResult> {
        self.execute_with_params(query, keyspace, auth, &HashMap::new())
            .await
    }

    async fn execute_with_params(
        &self,
        query: &str,
        keyspace: &str,
        auth: &AuthContext,
        params: &HashMap<String, serde_json::Value>,
    ) -> Result<GraphResult> {
        use futures::TryStreamExt as _;
        let (columns, rows, stats) = self
            .execute_stream_with_params(query, keyspace, auth, params)
            .await?;
        let rows = rows.try_collect().await?;
        Ok(GraphResult {
            columns,
            rows,
            stats,
        })
    }
}
