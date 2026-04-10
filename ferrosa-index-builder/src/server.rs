//! HTTP server for push-mode index building.
//!
//! Exposes two endpoints:
//! - `POST /internal/index/build` — accept a build request
//! - `GET /health` — health check with worker stats

use std::sync::Arc;

use axum::extract::State;
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::routing::{get, post};
use axum::{Json, Router};

use crate::worker::{BuildRequest, WorkerPool};

/// Build the axum router with shared worker pool state.
pub fn router(pool: Arc<WorkerPool>) -> Router {
    Router::new()
        .route("/internal/index/build", post(handle_build))
        .route("/health", get(handle_health))
        .with_state(pool)
}

async fn handle_build(
    State(pool): State<Arc<WorkerPool>>,
    Json(req): Json<BuildRequest>,
) -> impl IntoResponse {
    tracing::info!(
        sstable_id = %req.sstable_id,
        index_name = %req.index_name,
        index_type = %req.index_type,
        "received build request"
    );
    let response = pool.execute(req).await;
    // Application-level errors (status: "failed") still return HTTP 200.
    // The engine distinguishes transport errors (HTTP 5xx) from app errors.
    (StatusCode::OK, Json(response))
}

#[derive(serde::Serialize)]
struct HealthResponse {
    status: String,
    workers_active: usize,
    jobs_completed: usize,
    jobs_failed: usize,
}

async fn handle_health(State(pool): State<Arc<WorkerPool>>) -> impl IntoResponse {
    Json(HealthResponse {
        status: "ok".into(),
        workers_active: pool.active_workers(),
        jobs_completed: pool.jobs_completed(),
        jobs_failed: pool.jobs_failed(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use axum::http::Request;
    use tower::ServiceExt;

    fn test_pool() -> Arc<WorkerPool> {
        let store = Arc::new(object_store::memory::InMemory::new());
        Arc::new(WorkerPool::new(2, store, 1024 * 1024))
    }

    #[tokio::test]
    async fn health_endpoint() {
        let app = router(test_pool());
        let req = Request::builder()
            .uri("/health")
            .body(Body::empty())
            .unwrap();
        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
    }
}
