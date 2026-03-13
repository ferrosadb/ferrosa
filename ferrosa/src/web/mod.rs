pub mod api;
pub mod static_files;

use std::sync::Arc;

use axum::Router;
use ferrosa_schema::VirtualTableRegistry;

/// Build the web interface router.
pub fn build_router(registry: Arc<VirtualTableRegistry>) -> Router {
    Router::new()
        .nest("/api", api::routes(registry))
        .fallback(static_files::fallback_handler)
}
