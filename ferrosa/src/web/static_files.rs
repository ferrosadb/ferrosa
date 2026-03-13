use axum::http::StatusCode;
use axum::response::Html;

/// Fallback handler — serves a simple placeholder page.
/// In production, this will serve rust-embed static files.
pub async fn fallback_handler() -> (StatusCode, Html<String>) {
    (
        StatusCode::OK,
        Html("<html><body><h1>Ferrosa Dashboard</h1><p>Coming soon.</p></body></html>".to_string()),
    )
}
