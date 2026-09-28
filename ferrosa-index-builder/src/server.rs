//! HTTP server for push-mode index building.
//!
//! Exposes two endpoints:
//! - `POST /internal/index/build` — accept a build request
//! - `GET /health` — health check with worker stats

use std::sync::Arc;

use axum::extract::{Request, State};
use axum::http::{header, StatusCode};
use axum::middleware::{self, Next};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};

use crate::worker::{BuildRequest, WorkerPool};

/// Shared secret that authenticates the engine on `POST /internal/index/build`.
///
/// The engine presents it as `Authorization: Bearer <token>`. Construction
/// refuses a short or empty token so an unconfigured deployment cannot run open.
#[derive(Clone)]
pub struct AuthToken(Arc<str>);

impl AuthToken {
    /// Minimum accepted token length in bytes.
    pub const MIN_LEN: usize = 16;

    pub fn new(token: impl AsRef<str>) -> Result<Self, String> {
        let token = token.as_ref().trim();
        if token.len() < Self::MIN_LEN {
            return Err(format!(
                "index builder auth token must be at least {} bytes",
                Self::MIN_LEN
            ));
        }
        Ok(Self(Arc::from(token)))
    }

    fn matches(&self, presented: &str) -> bool {
        constant_time_eq(self.0.as_bytes(), presented.as_bytes())
    }
}

/// Compare without an early exit on the first differing byte.
fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    let mut diff = a.len() ^ b.len();
    for i in 0..a.len().max(b.len()) {
        let x = a.get(i).copied().unwrap_or(0);
        let y = b.get(i).copied().unwrap_or(0);
        diff |= usize::from(x ^ y);
    }
    diff == 0
}

/// Reject any request without a valid engine bearer token, before the handler
/// (and therefore any S3 or filesystem access) runs.
async fn require_engine_auth(State(token): State<AuthToken>, req: Request, next: Next) -> Response {
    let presented = req
        .headers()
        .get(header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "));
    match presented {
        Some(p) if token.matches(p) => next.run(req).await,
        _ => {
            tracing::warn!("rejected unauthenticated index build request");
            StatusCode::UNAUTHORIZED.into_response()
        }
    }
}

/// Build the axum router with shared worker pool state. The build endpoint
/// requires `token`; `/health` stays open (counters only).
pub fn router(pool: Arc<WorkerPool>, token: AuthToken) -> Router {
    let build = Router::new()
        .route("/internal/index/build", post(handle_build))
        .route_layer(middleware::from_fn_with_state(token, require_engine_auth))
        .with_state(Arc::clone(&pool));
    Router::new()
        .route("/health", get(handle_health))
        .with_state(pool)
        .merge(build)
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

    const TOKEN: &str = "engine-shared-secret-0123456789";

    fn test_pool() -> Arc<WorkerPool> {
        let store = Arc::new(object_store::memory::InMemory::new());
        Arc::new(WorkerPool::new(2, store, 1024 * 1024))
    }

    fn app(pool: Arc<WorkerPool>) -> Router {
        router(pool, AuthToken::new(TOKEN).unwrap())
    }

    fn build_body() -> Body {
        Body::from(
            serde_json::json!({
                "job_id": "job-1", "sstable_id": "gen-1", "index_name": "i",
                "index_type": "btree", "table": ["ks", "tbl"],
                "column_position": 0, "priority": "normal",
            })
            .to_string(),
        )
    }

    fn build_req(auth: Option<&str>) -> Request<Body> {
        let mut b = Request::builder()
            .method("POST")
            .uri("/internal/index/build")
            .header("content-type", "application/json");
        if let Some(a) = auth {
            b = b.header("authorization", a);
        }
        b.body(build_body()).unwrap()
    }

    #[tokio::test]
    async fn health_endpoint() {
        let resp = app(test_pool())
            .oneshot(
                Request::builder()
                    .uri("/health")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
    }

    /// JB-T3: no valid engine credential means refusal before any work runs.
    #[tokio::test]
    async fn index_builder_rejects_unauthenticated_request() {
        let pool = test_pool();
        for auth in [
            None,
            Some("Bearer wrong-secret-0123456789abcd"),
            Some("Bearer "),
            Some("Basic engine-shared-secret-0123456789"),
            Some(TOKEN),
        ] {
            let resp = app(Arc::clone(&pool))
                .oneshot(build_req(auth))
                .await
                .unwrap();
            assert_eq!(resp.status(), StatusCode::UNAUTHORIZED, "auth {auth:?}");
        }
        // The handler never ran: no job was counted, completed or failed.
        assert_eq!(pool.jobs_completed() + pool.jobs_failed(), 0);

        // Positive control: the right token reaches the handler (which then
        // reports an application-level failure for the missing SSTable).
        let ok = format!("Bearer {TOKEN}");
        let resp = app(Arc::clone(&pool))
            .oneshot(build_req(Some(&ok)))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        assert_eq!(pool.jobs_failed(), 1);
    }

    #[test]
    fn auth_token_rejects_short_or_empty() {
        assert!(AuthToken::new("").is_err());
        assert!(AuthToken::new("short").is_err());
        assert!(AuthToken::new("engine-shared-secret-0123456789").is_ok());
    }
}
