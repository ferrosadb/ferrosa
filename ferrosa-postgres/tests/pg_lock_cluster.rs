//! `LOCK TABLE` on a cluster node (a node with an Accord committer): the locked table
//! enters the transaction's read set, so the commit carries a table-level conflict key and
//! a concurrent writer of the table makes this transaction fail with 40001 — Ecto's
//! migration lock as an optimistic lock on Accord (D11). Writer ownership is node-local
//! and is not taken there.

#[path = "common/pg_server.rs"]
mod pg_server;

use std::sync::{Arc, Mutex};

use ferrosa_common::accord::Timestamp;
use ferrosa_postgres::AccordAccess;
use ferrosa_storage::accord::{
    CommitError, CommitOutcome, PgReadSet, StagedAccordWriteSet, TransactionCommitter,
    TransactionWrite,
};

/// Records the scan tables of every read set a COMMIT hands on.
#[derive(Default)]
struct ScanTableProbe {
    commits: Mutex<Vec<Vec<String>>>,
}

#[async_trait::async_trait]
impl TransactionCommitter for ScanTableProbe {
    async fn commit(&self, _writes: Vec<TransactionWrite>) -> Result<CommitOutcome, CommitError> {
        Ok(CommitOutcome::Committed)
    }
    async fn begin_postgres_snapshot(&self, _keyspace: &str) -> Result<Timestamp, CommitError> {
        Ok(Timestamp {
            epoch: 0,
            time: 1,
            seq: 0,
            node: 0,
        })
    }
    async fn validate_postgres_snapshot(
        &self,
        _keyspace: &str,
        _snapshot: Timestamp,
    ) -> Result<bool, CommitError> {
        Ok(true)
    }
    async fn commit_postgres(
        &self,
        _keyspace: &str,
        _writes: StagedAccordWriteSet,
        reads: PgReadSet,
        _written_tables: Vec<String>,
        _snapshot: Timestamp,
    ) -> Result<CommitOutcome, CommitError> {
        self.commits
            .lock()
            .expect("probe")
            .push(reads.scan_tables().to_vec());
        Ok(CommitOutcome::Committed)
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn lock_registers_the_table_in_the_cluster_read_set() {
    let probe = Arc::new(ScanTableProbe::default());
    let committer: Arc<dyn TransactionCommitter> = probe.clone();
    let server = pg_server::start_server_with_accord(
        0,
        ferrosa_postgres::PortalLimits::default(),
        AccordAccess::fixed(committer),
    )
    .await;
    let client = pg_server::connect(server.port).await;
    let insert = "INSERT INTO ks.t (id, ck, name, score) VALUES ('w', 1, 'x', 0)";

    // Control: a write alone does not put the table in the read set.
    client.batch_execute("BEGIN").await.unwrap();
    client.batch_execute(insert).await.unwrap();
    client.batch_execute("COMMIT").await.unwrap();
    // With the lock, it does.
    client.batch_execute("BEGIN").await.unwrap();
    client
        .batch_execute("LOCK TABLE ks.t IN SHARE UPDATE EXCLUSIVE MODE")
        .await
        .expect("LOCK on a cluster node");
    client.batch_execute(insert).await.unwrap();
    client.batch_execute("COMMIT").await.unwrap();

    let commits = probe.commits.lock().expect("probe").clone();
    assert_eq!(commits.len(), 2, "both transactions reach the committer");
    assert!(!commits[0].contains(&"ks.t".to_string()), "{commits:?}");
    assert!(commits[1].contains(&"ks.t".to_string()), "{commits:?}");
}
