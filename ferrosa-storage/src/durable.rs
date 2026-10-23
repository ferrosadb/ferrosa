//! Async adapter for `ferrosa_common::durable_sync`.
//!
//! `tokio::fs::File::sync_all` / `sync_data` call the std methods, which are
//! `F_FULLFSYNC` on Apple targets whatever `[storage] fsync_mode` says, so they
//! are refused by `clippy.toml`. This runs the shared helper on a duplicate of
//! the file's descriptor, on the blocking pool, as tokio itself does.

use ferrosa_common::durable_sync::{process_mode, sync_with, Scope};

/// Makes `file` durable in the process-wide mode. In-flight writes are
/// completed first (`try_clone` waits for them), exactly as tokio's own
/// `sync_all` does before it syncs.
pub(crate) async fn sync_tokio_file(file: &tokio::fs::File, scope: Scope) -> std::io::Result<()> {
    let std_file = file.try_clone().await?.into_std().await;
    let mode = process_mode();
    tokio::task::spawn_blocking(move || sync_with(&std_file, mode, scope))
        .await
        .map_err(|e| std::io::Error::other(format!("durable sync task failed: {e}")))?
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::AsyncWriteExt;

    #[tokio::test]
    async fn a_tokio_file_is_synced_through_the_shared_helper() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("f");
        let mut file = tokio::fs::File::create(&path).await.unwrap();
        file.write_all(b"durable").await.unwrap();
        let call = process_mode().call(Scope::Data);
        let before = ferrosa_common::durable_sync::syncs_issued(call);

        sync_tokio_file(&file, Scope::Data).await.unwrap();

        assert!(ferrosa_common::durable_sync::syncs_issued(call) > before);
        assert_eq!(std::fs::read(&path).unwrap(), b"durable");
    }
}
