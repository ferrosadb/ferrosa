//! A stalled consumer of a secondary-index stream must not wedge the node.
//!
//! `StorageEngine::read_by_index_stream_after` walks the index on a blocking
//! worker that holds `self.tables.read()` for the WHOLE walk and hands
//! partitions through a 4-slot channel with `blocking_send`. A consumer that
//! stops pulling (a merge parked on another source, a sink blocked on a full
//! internode socket, a slow client) parks the walker INSIDE the read guard.
//!
//! `tables` is a `parking_lot::RwLock`, which is task-fair: once a writer is
//! queued, new readers block behind it. So one DDL (`register_table` takes
//! `tables.write()`) waits for the parked walker, and every later
//! `tables.read()` -- every read and write on the node -- waits for the DDL.
//! Nothing in that chain has a deadline, and if the stalled consumer itself
//! needs any storage read before it pulls again, it is a permanent deadlock.
//!
//! Suspected mechanism behind node2's wedge (2026-10-03): lane deadlines bound
//! request/response RPCs, but not the index walk's guard, nor `fire()` on the
//! replica's stream sink, nor the merge's await on a source.

use std::sync::mpsc;
use std::sync::Arc;
use std::time::Duration;

use ferrosa_common::cell::CellValue;
use ferrosa_common::key::{DecoratedKey, PartitionKey};
use ferrosa_common::schema::{ColumnDefinition, TableSchema};
use ferrosa_sstable::types::{DeletionTime, LivenessInfo, Row};
use ferrosa_storage::{StorageEngine, StorageEngineConfig, TableId};
use futures::StreamExt;

const INDEX_NAME: &str = "stall_val_idx";
const UTF8: &str = "org.apache.cassandra.db.marshal.UTF8Type";
const INT32: &str = "org.apache.cassandra.db.marshal.Int32Type";

fn schema(table: &str) -> TableSchema {
    TableSchema {
        keyspace: "stall_ks".to_string(),
        table: table.to_string(),
        key_type: UTF8.to_string(),
        clustering_columns: vec![ColumnDefinition {
            name: "ck".to_string(),
            type_name: INT32.to_string(),
        }],
        static_columns: vec![],
        regular_columns: vec![ColumnDefinition {
            name: "val".to_string(),
            type_name: UTF8.to_string(),
        }],
        extensions: Default::default(),
    }
}

/// Run `f` on its own thread and report whether it finished within `limit`.
fn finishes_within(limit: Duration, f: impl FnOnce() + Send + 'static) -> bool {
    let (done_tx, done_rx) = mpsc::channel();
    std::thread::spawn(move || {
        f();
        let _ = done_tx.send(());
    });
    done_rx.recv_timeout(limit).is_ok()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_stalled_index_stream_consumer_does_not_block_ddl_or_reads() {
    let dir = tempfile::tempdir().unwrap();
    let engine =
        Arc::new(StorageEngine::new(StorageEngineConfig::test_config(dir.path()), None).unwrap());
    engine
        .register_table_with_indexes(schema("hits"), vec![(INDEX_NAME.to_string(), 0_usize)])
        .unwrap();
    let tid = TableId::new("stall_ks", "hits");

    // More hits than the walker's 4-slot channel, so a consumer that stops
    // after one item leaves the walker parked in `blocking_send`.
    for n in 0..32_i32 {
        let key = DecoratedKey::new(PartitionKey::new(format!("pk{n}").into_bytes()));
        let row = Row {
            clustering: n.to_be_bytes().to_vec(),
            cells: vec![(0, CellValue::live(b"hot".to_vec(), 1))],
            deletion: DeletionTime::LIVE,
            primary_key_liveness: LivenessInfo::with_timestamp(1),
        };
        engine.write(&tid, &key, row, 1).unwrap();
    }

    let mut stalled =
        engine.read_by_index_stream(&tid, INDEX_NAME, &ferrosa_index::IndexKey(b"hot".to_vec()));
    let first = stalled.next().await.expect("one hit").expect("hit decodes");
    assert!(!first.rows.is_empty());
    // The consumer now stalls: it holds the stream and pulls nothing more.
    // Give the walker time to fill the channel and park.
    tokio::time::sleep(Duration::from_millis(200)).await;

    let ddl_engine = Arc::clone(&engine);
    let ddl_done = finishes_within(Duration::from_secs(5), move || {
        ddl_engine
            .register_table(schema("created_during_stall"))
            .unwrap();
    });

    // While that DDL is queued, an ordinary point read on the same node.
    let read_engine = Arc::clone(&engine);
    let read_tid = tid.clone();
    let read_done = finishes_within(Duration::from_secs(5), move || {
        let key = DecoratedKey::new(PartitionKey::new(b"pk0".to_vec()));
        read_engine.read(&read_tid, &key).unwrap();
    });

    // Release the walker so the threads above can finish and the test exits.
    drop(stalled);

    assert!(
        ddl_done,
        "DDL (tables.write) blocked for 5 s behind an index walk whose consumer stalled: the \
         walker holds tables.read() across blocking_send"
    );
    assert!(
        read_done,
        "an ordinary read blocked for 5 s behind the queued DDL: one stalled index-stream \
         consumer wedges every reader on the node"
    );
}
