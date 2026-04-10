//! CLI entry point for the standalone index builder.

use clap::Parser;
use std::net::SocketAddr;
use std::time::Duration;

/// Standalone index builder for Ferrosa.
///
/// Offloads secondary index construction from the main engine.
/// Reads SSTables from S3, builds sidecar index files, writes them back.
#[derive(Parser, Debug)]
#[command(name = "ferrosa-index-builder", version)]
struct Cli {
    /// Operation mode: "push" (HTTP server) or "pull" (manifest watcher).
    #[arg(long, default_value = "push")]
    mode: String,

    /// Listen address for HTTP server (push mode).
    #[arg(long, default_value = "0.0.0.0:8090")]
    listen: SocketAddr,

    /// Number of worker threads for index building.
    #[arg(long, default_value = "4")]
    workers: usize,

    /// S3-compatible endpoint URL.
    #[arg(long, env = "FERROSA_S3_ENDPOINT")]
    s3_endpoint: String,

    /// S3 bucket name.
    #[arg(long, env = "FERROSA_S3_BUCKET")]
    s3_bucket: String,

    /// S3 key prefix for multi-tenant separation.
    #[arg(long, env = "FERROSA_S3_PREFIX", default_value = "")]
    s3_prefix: String,

    /// S3 region.
    #[arg(long, env = "FERROSA_S3_REGION", default_value = "us-east-1")]
    s3_region: String,

    /// Allow non-TLS S3 connections (for MinIO local dev).
    #[arg(long, env = "FERROSA_S3_ALLOW_HTTP")]
    s3_allow_http: bool,

    /// S3 access key ID (optional — falls back to instance profile).
    #[arg(long, env = "FERROSA_S3_ACCESS_KEY_ID")]
    s3_access_key_id: Option<String>,

    /// S3 secret access key.
    #[arg(long, env = "FERROSA_S3_SECRET_ACCESS_KEY")]
    s3_secret_access_key: Option<String>,

    /// Maximum bytes of temporary files on local disk.
    #[arg(long, default_value = "10737418240")]
    max_temp_bytes: u64,

    // ── Pull mode options ───────────────────────────────────────────────
    /// Engine manifest endpoint URL (pull mode).
    #[arg(long)]
    manifest_endpoint: Option<String>,

    /// How often to poll the manifest (pull mode).
    #[arg(long, default_value = "10")]
    poll_interval_secs: u64,
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let cli = Cli::parse();

    let object_store = build_object_store(&cli);

    let worker_pool = ferrosa_index_builder::worker::WorkerPool::new(
        cli.workers,
        std::sync::Arc::clone(&object_store),
        cli.max_temp_bytes,
    );

    let worker_pool = std::sync::Arc::new(worker_pool);

    match cli.mode.as_str() {
        "push" => {
            tracing::info!(
                listen = %cli.listen,
                workers = cli.workers,
                mode = "push",
                "starting ferrosa-index-builder"
            );
            let app = ferrosa_index_builder::server::router(std::sync::Arc::clone(&worker_pool));
            let listener = tokio::net::TcpListener::bind(cli.listen)
                .await
                .expect("failed to bind listener");
            axum::serve(listener, app).await.expect("server error");
        }
        "pull" => {
            let manifest_endpoint = cli.manifest_endpoint.unwrap_or_else(|| {
                eprintln!("--manifest-endpoint is required in pull mode");
                std::process::exit(1);
            });
            tracing::info!(
                manifest_endpoint = %manifest_endpoint,
                poll_interval = ?Duration::from_secs(cli.poll_interval_secs),
                workers = cli.workers,
                mode = "pull",
                "starting ferrosa-index-builder"
            );
            ferrosa_index_builder::pull::run(
                worker_pool,
                manifest_endpoint,
                Duration::from_secs(cli.poll_interval_secs),
                std::sync::Arc::clone(&object_store),
                cli.s3_prefix.clone(),
            )
            .await;
        }
        other => {
            eprintln!("unknown mode: {other} (expected 'push' or 'pull')");
            std::process::exit(1);
        }
    }
}

fn build_object_store(cli: &Cli) -> std::sync::Arc<dyn object_store::ObjectStore> {
    use object_store::aws::{AmazonS3Builder, S3ConditionalPut};

    let mut builder = AmazonS3Builder::new()
        .with_endpoint(&cli.s3_endpoint)
        .with_bucket_name(&cli.s3_bucket)
        .with_region(&cli.s3_region)
        .with_allow_http(cli.s3_allow_http)
        .with_conditional_put(S3ConditionalPut::ETagMatch);

    if let Some(ref key_id) = cli.s3_access_key_id {
        builder = builder.with_access_key_id(key_id);
    }
    if let Some(ref secret) = cli.s3_secret_access_key {
        builder = builder.with_secret_access_key(secret);
    }

    let store = builder.build().expect("failed to build S3 client");
    std::sync::Arc::new(store)
}
