//! Static file serving — embeds `ferrosa/web/` assets into the binary via `rust-embed`.
//!
//! The `folder` path in `#[folder = "web/"]` is relative to the crate root
//! (the directory containing `ferrosa/Cargo.toml`).

use axum::{
    body::Body,
    http::{header, HeaderValue, StatusCode, Uri},
    response::Response,
};
use rust_embed::RustEmbed;

/// All files under `ferrosa/web/` are embedded at compile time.
#[derive(RustEmbed)]
#[folder = "web/"]
struct WebAssets;

/// Handler that serves embedded static files.
///
/// `GET /`           → `index.html`
/// `GET /*path`      → file at that path within the embedded assets
/// Anything missing → 404
pub async fn static_handler(uri: Uri) -> Response<Body> {
    let path = uri.path().trim_start_matches('/');
    let path = if path.is_empty() { "index.html" } else { path };

    match WebAssets::get(path) {
        Some(content) => {
            let mime = mime_for(path);
            Response::builder()
                .status(StatusCode::OK)
                .header(header::CONTENT_TYPE, HeaderValue::from_static(mime))
                .body(Body::from(content.data.into_owned()))
                .unwrap_or_else(|_| status_only(StatusCode::INTERNAL_SERVER_ERROR))
        }
        None => status_only(StatusCode::NOT_FOUND),
    }
}

fn status_only(status: StatusCode) -> Response<Body> {
    Response::builder()
        .status(status)
        .body(Body::from(status.to_string()))
        .expect("infallible status response")
}

fn mime_for(path: &str) -> &'static str {
    if path.ends_with(".html") {
        "text/html; charset=utf-8"
    } else if path.ends_with(".css") {
        "text/css; charset=utf-8"
    } else if path.ends_with(".js") {
        "application/javascript; charset=utf-8"
    } else if path.ends_with(".json") {
        "application/json"
    } else if path.ends_with(".svg") {
        "image/svg+xml"
    } else if path.ends_with(".png") {
        "image/png"
    } else if path.ends_with(".ico") {
        "image/x-icon"
    } else {
        "application/octet-stream"
    }
}
