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

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::StatusCode;

    // -----------------------------------------------------------------------
    // mime_for — content type detection
    // -----------------------------------------------------------------------

    #[test]
    fn mime_for_html() {
        assert_eq!(mime_for("index.html"), "text/html; charset=utf-8");
    }

    #[test]
    fn mime_for_css() {
        assert_eq!(mime_for("styles.css"), "text/css; charset=utf-8");
    }

    #[test]
    fn mime_for_js() {
        assert_eq!(mime_for("app.js"), "application/javascript; charset=utf-8");
    }

    #[test]
    fn mime_for_json() {
        assert_eq!(mime_for("data.json"), "application/json");
    }

    #[test]
    fn mime_for_svg() {
        assert_eq!(mime_for("logo.svg"), "image/svg+xml");
    }

    #[test]
    fn mime_for_png() {
        assert_eq!(mime_for("image.png"), "image/png");
    }

    #[test]
    fn mime_for_ico() {
        assert_eq!(mime_for("favicon.ico"), "image/x-icon");
    }

    #[test]
    fn mime_for_unknown_extension() {
        assert_eq!(mime_for("file.xyz"), "application/octet-stream");
    }

    #[test]
    fn mime_for_no_extension() {
        assert_eq!(mime_for("README"), "application/octet-stream");
    }

    #[test]
    fn mime_for_nested_path_html() {
        assert_eq!(mime_for("pages/about.html"), "text/html; charset=utf-8");
    }

    #[test]
    fn mime_for_double_extension() {
        // "file.min.js" ends with ".js", should match JS.
        assert_eq!(
            mime_for("file.min.js"),
            "application/javascript; charset=utf-8"
        );
    }

    // -----------------------------------------------------------------------
    // status_only — HTTP status response construction
    // -----------------------------------------------------------------------

    #[test]
    fn status_only_not_found() {
        let resp = status_only(StatusCode::NOT_FOUND);
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    }

    #[test]
    fn status_only_internal_server_error() {
        let resp = status_only(StatusCode::INTERNAL_SERVER_ERROR);
        assert_eq!(resp.status(), StatusCode::INTERNAL_SERVER_ERROR);
    }

    #[test]
    fn status_only_ok() {
        let resp = status_only(StatusCode::OK);
        assert_eq!(resp.status(), StatusCode::OK);
    }

    // -----------------------------------------------------------------------
    // static_handler — end-to-end asset serving
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn static_handler_serves_index_html_for_root() {
        let uri: Uri = "/".parse().expect("valid URI");
        let resp = static_handler(uri).await;
        // index.html is embedded via rust-embed from ferrosa/web/index.html.
        assert_eq!(resp.status(), StatusCode::OK);
        let content_type = resp
            .headers()
            .get(header::CONTENT_TYPE)
            .and_then(|v| v.to_str().ok())
            .unwrap_or("");
        assert_eq!(content_type, "text/html; charset=utf-8");
    }

    #[tokio::test]
    async fn static_handler_returns_404_for_missing_file() {
        let uri: Uri = "/no-such-file-xyz.txt".parse().expect("valid URI");
        let resp = static_handler(uri).await;
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn static_handler_serves_index_html_for_empty_path() {
        let uri: Uri = "/".parse().expect("valid URI");
        let resp = static_handler(uri).await;
        assert_eq!(resp.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn static_handler_returns_404_for_deeply_nested_missing() {
        let uri: Uri = "/a/b/c/d/e.html".parse().expect("valid URI");
        let resp = static_handler(uri).await;
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    }
}
