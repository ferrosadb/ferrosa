//! End-to-end test of the ADR-020 streaming range-read lane.
//!
//! Wires the five pieces under test (idle watchdog, wire variants,
//! StreamRouter, payload structs, consumer/producer, frame router,
//! request handler) through an in-memory "fake wire" so the test
//! exercises the full request → chunks → done → assembled result
//! flow without touching `PeerManager`, sockets, or TLS.
//!
//! Architecture under test (single replica, simplest end-to-end):
//!
//! ```text
//!  test driver
//!     │
//!     │  build StaticReader with N partitions
//!     │  build sink = FakeWireSink { router: Arc<StreamRouter> }
//!     │  router.register(REQ_ID) → mpsc::Receiver
//!     │
//!     │  spawn: handle_stream_request(req, &reader, &sink, chunk_size)
//!     │       └── stream_range_response emits frames into sink
//!     │            └── sink calls frame_router.handle(peer, frame)
//!     │                 └── frame_router routes through StreamRouter
//!     │                      └── chunks land on the registered Receiver
//!     │
//!     │  consume_range_stream(rx, IDLE, expected_done=1, REQ_ID)
//!     │       └── assembles StreamConsumeOutcome { partitions, ... }
//!     │
//!     └── assert partitions == N (same as input)
//! ```
//!
//! This test is the contract that future Phase 2 work (lazy storage
//! iterator, bulk-lane multi-message integration into the lane
//! actor) must preserve.

#![cfg(test)]

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use uuid::Uuid;

use ferrosa_common::key::DecoratedKey;
use ferrosa_common::PartitionKey;
use ferrosa_net::message::Message;
use ferrosa_net::rpc::handler::{PeerId, RpcHandler};
use ferrosa_net::stream_router::StreamRouter;
use ferrosa_sstable::types::{DeletionTime, Partition};
use ferrosa_storage::TableId;

use crate::raft::handlers::RangeReadStreamRequestPayload;

use super::stream_consumer::{consume_range_stream, StreamConsumeError};
use super::stream_frame_router::StreamFrameRouter;
use super::stream_producer::ChunkSink;
use super::stream_request_handler::{handle_stream_request, PartitionStream, StreamRangeReader};

const IDLE: Duration = Duration::from_secs(2);

fn make_partition(tag: u8) -> Partition {
    let key = DecoratedKey::new(PartitionKey::new(vec![tag]));
    Partition {
        key,
        deletion: DeletionTime::LIVE,
        static_row: None,
        rows: vec![],
    }
}

fn peer() -> PeerId {
    (Uuid::nil(), "127.0.0.1:7000".parse().unwrap())
}

/// In-memory sink that simulates the on-wire path: every emitted
/// frame is fed to the coordinator's `StreamFrameRouter`. In
/// production the path is `producer → PeerManager::fire →
/// network → lane inbound dispatch → frame_router`; here we
/// short-circuit through a shared `Arc<StreamRouter>`.
struct FakeWireSink {
    frame_router: StreamFrameRouter,
    from: PeerId,
}

#[async_trait]
impl ChunkSink for FakeWireSink {
    async fn send(&self, msg: Message) {
        // Match the production RpcHandler dispatch shape.
        let _ = self.frame_router.handle(self.from, msg).await;
    }
}

struct StaticReader {
    partitions: Vec<Partition>,
}
impl StreamRangeReader for StaticReader {
    fn range_iter<'a>(
        &'a self,
        _table_id: &TableId,
        _projected_regular_ordinals: Option<&'a [u16]>,
        _start: Option<&'a ferrosa_common::key::DecoratedKey>,
    ) -> ferrosa_common::Result<PartitionStream<'a>> {
        let items: Vec<ferrosa_common::Result<Partition>> =
            self.partitions.iter().cloned().map(Ok).collect();
        Ok(Box::pin(futures::stream::iter(items)))
    }

    /// This replica's slice of an indexed read. A real node consults its own
    /// local index; a static one hands back everything it holds, which is what
    /// makes it usable for asserting the fan-out rather than the index.
    fn index_iter<'a>(
        &'a self,
        _table_id: &TableId,
        _index_name: &str,
        _index_key: &[u8],
        _after: Option<&ferrosa_index::RowPosition>,
    ) -> ferrosa_common::Result<PartitionStream<'a>> {
        let items: Vec<ferrosa_common::Result<Partition>> =
            self.partitions.iter().cloned().map(Ok).collect();
        Ok(Box::pin(futures::stream::iter(items)))
    }
}

/// Single replica, 10 partitions, chunk_size=3 → 4 chunks (3+3+3+1)
/// + 1 Done. Consumer reassembles all 10 partitions in arrival
/// order.
#[tokio::test]
async fn end_to_end_single_replica_streams_all_partitions() {
    let router = Arc::new(StreamRouter::new());
    const REQ_ID: u32 = 0xCAFE_F00D;
    let rx = router.register(REQ_ID, 8);

    let sink = FakeWireSink {
        frame_router: StreamFrameRouter::new(router.clone()),
        from: peer(),
    };
    let reader = StaticReader {
        partitions: (1u8..=10).map(make_partition).collect(),
    };
    let req = RangeReadStreamRequestPayload {
        request_id: REQ_ID,
        keyspace: "ks".into(),
        table: "tbl".into(),
        index_name: None,
        index_key: None,
        projected_regular_ordinals: None,
        start_key: None,
        start_clustering: None,
        max_chunks: 0,
    };

    // Producer runs concurrently with the consumer. In production
    // the producer runs on the handler node and the consumer on the
    // coordinator; here both are tasks on the same runtime sharing
    // the in-memory router.
    let producer = tokio::spawn(async move {
        handle_stream_request(req, Arc::new(reader), &sink, 3).await;
    });

    let outcome = consume_range_stream(rx, IDLE, 1, REQ_ID).await.unwrap();
    producer.await.unwrap();

    assert_eq!(outcome.total_chunks, 4, "10 / 3 = 4 chunks (3+3+3+1)");
    assert_eq!(outcome.partitions.len(), 10);
    assert!(!outcome.any_truncated);

    // Cleanup
    router.unregister(REQ_ID);
    assert!(router.is_empty());
}

/// Two replicas, each streaming 5 partitions: consumer waits for
/// both Done frames before resolving and returns combined
/// partitions.
#[tokio::test]
async fn end_to_end_two_replicas_aggregates_both_streams() {
    let router = Arc::new(StreamRouter::new());
    const REQ_ID: u32 = 0x2222_2222;
    let rx = router.register(REQ_ID, 16);

    let frame_router = Arc::new(StreamFrameRouter::new(router.clone()));

    let req = RangeReadStreamRequestPayload {
        request_id: REQ_ID,
        keyspace: "ks".into(),
        table: "tbl".into(),
        index_name: None,
        index_key: None,
        projected_regular_ordinals: None,
        start_key: None,
        start_clustering: None,
        max_chunks: 0,
    };

    let from_a: PeerId = (Uuid::from_u128(1), "127.0.0.1:7001".parse().unwrap());
    let from_b: PeerId = (Uuid::from_u128(2), "127.0.0.1:7002".parse().unwrap());

    let sink_a = FakeWireSinkShared {
        frame_router: frame_router.clone(),
        from: from_a,
    };
    let sink_b = FakeWireSinkShared {
        frame_router: frame_router.clone(),
        from: from_b,
    };

    let reader_a = StaticReader {
        partitions: (1u8..=5).map(make_partition).collect(),
    };
    let reader_b = StaticReader {
        partitions: (6u8..=10).map(make_partition).collect(),
    };

    let req_a = req.clone();
    let req_b = req.clone();
    let p_a = tokio::spawn(async move {
        handle_stream_request(req_a, Arc::new(reader_a), &sink_a, 2).await;
    });
    let p_b = tokio::spawn(async move {
        handle_stream_request(req_b, Arc::new(reader_b), &sink_b, 2).await;
    });

    let outcome = consume_range_stream(rx, IDLE, 2, REQ_ID).await.unwrap();
    p_a.await.unwrap();
    p_b.await.unwrap();

    assert_eq!(outcome.partitions.len(), 10);
    // 5/2 = 3 chunks (2+2+1) per replica → 6 chunks total
    assert_eq!(outcome.total_chunks, 6);
}

/// Variant of FakeWireSink that holds the frame_router by Arc so it
/// can be shared between multiple producer tasks (replicas).
struct FakeWireSinkShared {
    frame_router: Arc<StreamFrameRouter>,
    from: PeerId,
}
#[async_trait]
impl ChunkSink for FakeWireSinkShared {
    async fn send(&self, msg: Message) {
        let _ = self.frame_router.handle(self.from, msg).await;
    }
}

/// Producer never runs → no frames hit the router → consumer's
/// IdleTimeoutWatchdog fires within the deadline.
#[tokio::test(start_paused = true)]
async fn end_to_end_no_producer_trips_idle_watchdog() {
    let router = Arc::new(StreamRouter::new());
    const REQ_ID: u32 = 1;
    let rx = router.register(REQ_ID, 4);

    let err = consume_range_stream(rx, IDLE, 1, REQ_ID).await.unwrap_err();
    assert!(matches!(err, StreamConsumeError::IdleTimeout { .. }));
}

/// Scatter-gather is the invariant for a cluster-wide indexed read: a
/// secondary index is LOCAL to each node, so the coordinator must ask EVERY
/// node and union what comes back. A read that consults one node returns a
/// subset and reports it as the whole answer, which is indistinguishable from
/// the rows not existing.
///
/// This had a test while the read was `coordinate_index_read`. That function
/// was deleted when the read became streaming, and its tests went with it —
/// including the ones for a missing peer pool and a slow replica — leaving the
/// replacement with no multi-node coverage at all. This is the invariant put
/// back.
#[tokio::test]
async fn an_indexed_read_unions_every_replicas_rows() {
    let router = Arc::new(StreamRouter::new());
    const REQ_ID: u32 = 0x3333_3333;
    let rx = router.register(REQ_ID, 16);
    let frame_router = Arc::new(StreamFrameRouter::new(router.clone()));

    // The shape the coordinator sends for an indexed read: same framing as a
    // range read, with the index named. Both fields must be set; one without
    // the other is refused by the handler.
    let req = RangeReadStreamRequestPayload {
        request_id: REQ_ID,
        keyspace: "ks".into(),
        table: "tbl".into(),
        index_name: Some("tenant_idx".into()),
        index_key: Some(b"tenant-a".to_vec()),
        projected_regular_ordinals: None,
        start_key: None,
        start_clustering: None,
        max_chunks: 0,
    };

    let from_a: PeerId = (Uuid::from_u128(1), "127.0.0.1:7001".parse().unwrap());
    let from_b: PeerId = (Uuid::from_u128(2), "127.0.0.1:7002".parse().unwrap());
    let sink_a = FakeWireSinkShared {
        frame_router: frame_router.clone(),
        from: from_a,
    };
    let sink_b = FakeWireSinkShared {
        frame_router: frame_router.clone(),
        from: from_b,
    };

    // Disjoint rows: every partition exists on exactly one replica, so a
    // union that drops a replica is a short count rather than a duplicate.
    let reader_a = StaticReader {
        partitions: (1u8..=5).map(make_partition).collect(),
    };
    let reader_b = StaticReader {
        partitions: (6u8..=10).map(make_partition).collect(),
    };

    let req_a = req.clone();
    let req_b = req.clone();
    let p_a = tokio::spawn(async move {
        handle_stream_request(req_a, Arc::new(reader_a), &sink_a, 2).await;
    });
    let p_b = tokio::spawn(async move {
        handle_stream_request(req_b, Arc::new(reader_b), &sink_b, 2).await;
    });

    let outcome = consume_range_stream(rx, IDLE, 2, REQ_ID).await.unwrap();
    p_a.await.unwrap();
    p_b.await.unwrap();

    assert_eq!(
        outcome.partitions.len(),
        10,
        "an indexed read must union every replica's rows; a subset reported as \
         the whole answer looks exactly like the rows not existing"
    );
}

// ── Tenant-wide reads through a partition-key index (t_50c8bc7d) ─────────────
//
// `agent_memory.entity_store` is keyed `((tenant_id, session_id), entity_id)`
// and carries `idx_entity_by_tenant ON entity_store (tenant_id)`. A tenant's
// sessions hash all over the ring, so at RF=1 on three nodes each node holds a
// disjoint slice of the tenant, indexed only by its own local index. A
// tenant-wide read is correct only if every node answers from its index and
// the coordinator unions the answers.

const TENANT_A: u8 = 0xA;
const TENANT_B: u8 = 0xB;
const TENANT_INDEX: &str = "idx_by_tenant";

fn tenant_table_id() -> TableId {
    TableId::new("agent_memory", "entity_store")
}

fn tenant_table_schema() -> ferrosa_common::TableSchema {
    ferrosa_common::TableSchema {
        keyspace: "agent_memory".into(),
        table: "entity_store".into(),
        key_type: "org.apache.cassandra.db.marshal.CompositeType(\
                   org.apache.cassandra.db.marshal.UUIDType,\
                   org.apache.cassandra.db.marshal.UUIDType)"
            .into(),
        clustering_columns: vec![],
        static_columns: vec![],
        regular_columns: vec![ferrosa_common::ColumnDefinition {
            name: "body".into(),
            type_name: "org.apache.cassandra.db.marshal.UTF8Type".into(),
        }],
        extensions: Default::default(),
    }
}

/// The `(tenant_id, session_id)` composite key, CQL composite encoding.
fn tenant_session_key(tenant: u8, session: u8) -> DecoratedKey {
    let mut key = Vec::with_capacity(38);
    for component in [tenant, session] {
        let mut uuid = [0u8; 16];
        uuid[0] = component;
        key.extend_from_slice(&16u16.to_be_bytes());
        key.extend_from_slice(&uuid);
        key.push(0x00);
    }
    DecoratedKey::new(PartitionKey::new(key))
}

fn tenant_index_key(tenant: u8) -> Vec<u8> {
    let mut uuid = [0u8; 16];
    uuid[0] = tenant;
    uuid.to_vec()
}

/// Writes one row per `(tenant, session)` and flushes, so every answer comes
/// from an SSTable sidecar rather than the memtable.
fn write_sessions(engine: &ferrosa_storage::StorageEngine, sessions: &[(u8, u8)]) {
    use ferrosa_common::CellValue;
    use ferrosa_sstable::types::{LivenessInfo, Row};
    let table_id = tenant_table_id();
    for &(tenant, session) in sessions {
        let row = Row {
            clustering: vec![],
            cells: vec![(0, CellValue::live(b"entity".to_vec(), 1000))],
            deletion: DeletionTime::LIVE,
            primary_key_liveness: LivenessInfo::with_timestamp(1000),
        };
        engine
            .write(&table_id, &tenant_session_key(tenant, session), row, 1000)
            .unwrap();
    }
    engine.flush(&table_id).unwrap();
}

/// One node of an RF=1 cluster holding `sessions`, with or without the
/// tenant index declared.
fn tenant_node(
    dir: &std::path::Path,
    sessions: &[(u8, u8)],
    declare_index: bool,
) -> Arc<ferrosa_storage::StorageEngine> {
    let config = ferrosa_storage::StorageEngineConfig::test_config(dir);
    let engine = ferrosa_storage::StorageEngine::new(config, None).unwrap();
    engine.register_table(tenant_table_schema()).unwrap();
    if declare_index {
        engine
            .add_partition_key_index(
                &tenant_table_id(),
                TENANT_INDEX,
                0,
                ferrosa_index::IndexType::BTree,
            )
            .unwrap();
    }
    write_sessions(&engine, sessions);
    Arc::new(engine)
}

/// A node that declared the tenant index, took writes, and was RESTARTED —
/// the live cluster's state. The index comes back only through the reload.
fn restarted_tenant_node(
    dir: &std::path::Path,
    sessions: &[(u8, u8)],
) -> Arc<ferrosa_storage::StorageEngine> {
    use ferrosa_schema::system::persistence;
    let indexes_tid = TableId::new("system_schema", "indexes");
    {
        let config = ferrosa_storage::StorageEngineConfig::test_config(dir);
        let engine = ferrosa_storage::StorageEngine::new(config, None).unwrap();
        engine.register_table(tenant_table_schema()).unwrap();
        engine.register_system_tables().unwrap();
        engine
            .add_partition_key_index(
                &tenant_table_id(),
                TENANT_INDEX,
                0,
                ferrosa_index::IndexType::BTree,
            )
            .unwrap();
        let row = persistence::index_to_rows(&ferrosa_schema::metadata::index::IndexMetadata {
            keyspace: "agent_memory".into(),
            table: "entity_store".into(),
            name: TENANT_INDEX.into(),
            index_type: ferrosa_index::IndexType::BTree,
            target_columns: vec!["tenant_id".into()],
            filter_predicate: None,
            options: std::collections::HashMap::new(),
        });
        engine
            .write(&indexes_tid, &row.key, row.row, 1_000_000)
            .unwrap();
        write_sessions(&engine, sessions);
        engine.flush(&indexes_tid).unwrap();
    }
    let config = ferrosa_storage::StorageEngineConfig::test_config(dir);
    let (engine, _pending) = ferrosa_storage::StorageEngine::open(config, None).unwrap();
    engine.register_system_tables().unwrap();
    let partition_keys = ferrosa_storage::engine::PartitionKeyColumns::from([(
        tenant_table_id(),
        vec!["tenant_id".to_string(), "session_id".to_string()],
    )]);
    let outcome = engine
        .reload_indexes_from_system_schema(&partition_keys)
        .unwrap();
    assert_eq!(
        outcome.restored, 1,
        "the tenant index must be restored on restart"
    );
    Arc::new(engine)
}

/// Drives one tenant-index read against every node through the real request
/// handler and the real multi-replica consumer.
async fn scatter_gather_tenant(
    nodes: Vec<Arc<ferrosa_storage::StorageEngine>>,
    tenant: u8,
    after: Option<(u8, u8)>,
) -> Result<super::stream_consumer::StreamConsumeOutcome, StreamConsumeError> {
    const REQ_ID: u32 = 0x7E_7A_17;
    let router = Arc::new(StreamRouter::new());
    let rx = router.register(REQ_ID, 64);
    let frame_router = Arc::new(StreamFrameRouter::new(router.clone()));
    let req = RangeReadStreamRequestPayload {
        request_id: REQ_ID,
        keyspace: "agent_memory".into(),
        table: "entity_store".into(),
        index_name: Some(TENANT_INDEX.into()),
        index_key: Some(tenant_index_key(tenant)),
        projected_regular_ordinals: None,
        // An index read resumes strictly AFTER the last row delivered: its
        // partition key and clustering (empty here: no clustering columns).
        start_key: after.map(|(t, s)| tenant_session_key(t, s).key.as_bytes().to_vec()),
        start_clustering: after.map(|_| Vec::new()),
        max_chunks: 0,
    };
    let expected_done = nodes.len();
    let producers: Vec<_> = nodes
        .into_iter()
        .enumerate()
        .map(|(i, engine)| {
            let sink = FakeWireSinkShared {
                frame_router: frame_router.clone(),
                from: (
                    Uuid::from_u128(i as u128 + 1),
                    format!("127.0.0.1:{}", 7101 + i).parse().unwrap(),
                ),
            };
            let req = req.clone();
            tokio::spawn(async move {
                handle_stream_request(req, Arc::new(engine), &sink, 2).await;
            })
        })
        .collect();
    let outcome = consume_range_stream(rx, IDLE, expected_done, REQ_ID).await;
    futures::future::join_all(producers)
        .await
        .into_iter()
        .for_each(|joined| joined.expect("producer task must not panic"));
    outcome
}

/// The tenant's rows live on three nodes, one of them restarted. A
/// tenant-wide read must return every one of them and nothing of another
/// tenant.
#[tokio::test(flavor = "multi_thread")]
async fn a_tenant_read_scatter_gathers_every_nodes_tenant_index() {
    let dirs: Vec<_> = (0..3).map(|_| tempfile::tempdir().unwrap()).collect();
    let nodes = vec![
        tenant_node(
            dirs[0].path(),
            &[(TENANT_A, 1), (TENANT_A, 2), (TENANT_B, 3)],
            true,
        ),
        tenant_node(
            dirs[1].path(),
            &[(TENANT_A, 4), (TENANT_B, 5), (TENANT_B, 6)],
            true,
        ),
        restarted_tenant_node(
            dirs[2].path(),
            &[(TENANT_A, 7), (TENANT_A, 8), (TENANT_A, 9)],
        ),
    ];

    let outcome = scatter_gather_tenant(nodes, TENANT_A, None)
        .await
        .expect("every node declares the tenant index, so the read must succeed");

    // Byte 2 of the composite key is the first byte of the tenant uuid.
    let mut tenants: Vec<u8> = outcome
        .partitions
        .iter()
        .map(|p| p.key.key.as_bytes()[2])
        .collect();
    tenants.sort_unstable();
    assert_eq!(
        tenants,
        vec![TENANT_A; 6],
        "a tenant-wide read must union all six of the tenant's sessions across \
         the three nodes — including the restarted one — and none of another \
         tenant's"
    );
}

/// A node that does not have the index cannot answer for its slice of the
/// tenant. Its silence must fail the read: an empty contribution unions into
/// a short answer that looks exactly like the rows not existing — the live
/// failure, where 101,848 entities read as an empty database.
#[tokio::test(flavor = "multi_thread")]
async fn a_node_without_the_tenant_index_fails_the_read_rather_than_shortening_it() {
    let dirs: Vec<_> = (0..3).map(|_| tempfile::tempdir().unwrap()).collect();
    let nodes = vec![
        tenant_node(dirs[0].path(), &[(TENANT_A, 1), (TENANT_A, 2)], true),
        tenant_node(dirs[1].path(), &[(TENANT_A, 3), (TENANT_A, 4)], false),
        tenant_node(dirs[2].path(), &[(TENANT_A, 5), (TENANT_A, 6)], true),
    ];

    match scatter_gather_tenant(nodes, TENANT_A, None).await {
        Err(StreamConsumeError::TruncatedReplica { .. }) => {}
        Err(other) => panic!("expected the missing index to truncate a replica, got {other:?}"),
        Ok(outcome) => panic!(
            "a node without the tenant index answered with an empty slice and the \
             read returned {} of 6 rows as if complete",
            outcome.partitions.len()
        ),
    }
}

/// The next page of a tenant-wide read: every node resumes its own index walk
/// strictly after the cursor the coordinator hands it — the last row the
/// previous page delivered — so no node repeats a row or skips one.
#[tokio::test(flavor = "multi_thread")]
async fn a_resumed_tenant_read_returns_only_rows_after_the_cursor_from_every_node() {
    let dirs: Vec<_> = (0..3).map(|_| tempfile::tempdir().unwrap()).collect();
    let nodes = vec![
        tenant_node(
            dirs[0].path(),
            &[(TENANT_A, 1), (TENANT_A, 5), (TENANT_B, 3)],
            true,
        ),
        tenant_node(dirs[1].path(), &[(TENANT_A, 4), (TENANT_A, 8)], true),
        restarted_tenant_node(dirs[2].path(), &[(TENANT_A, 2), (TENANT_A, 9)]),
    ];

    let outcome = scatter_gather_tenant(nodes, TENANT_A, Some((TENANT_A, 4)))
        .await
        .expect("every node declares the tenant index");

    // Byte 21 of the composite key is the first byte of the session uuid.
    let mut sessions: Vec<u8> = outcome
        .partitions
        .iter()
        .map(|p| p.key.key.as_bytes()[21])
        .collect();
    sessions.sort_unstable();
    assert_eq!(
        sessions,
        vec![5, 8, 9],
        "a page resumed after session 4 must hold exactly the tenant's later \
         sessions, from whichever node holds them"
    );
}

// ---------------------------------------------------------------------------
// A posting list bigger than the coordinator's route buffer.
//
// t_bf9b4adf. On the live 3-node cluster, `SELECT COUNT(*) FROM entity_store
// WHERE tenant_id = <9a5f8fbf…>` — ~103,000 of the table's 103,664 rows on one
// index key — failed in 0.34 SECONDS with
//
//   read timeout: CL=ONE, received=0, required=1, data_present=false
//
// A read that "times out" in a third of a second did not time out. node1's log
// at that instant:
//
//   stream consumer buffer full; closing route so consumer fails instead of
//   returning partial data   msg_type=RangeReadStreamChunk
//   streaming range read: remote stream closed before Done — returning
//   retryable ReadTimeout   delivered_done=0 expected_done=1
//
// The producer had no flow-control window, so it fired the whole posting list
// at a bounded route. The tests above never caught it because they register a
// 64-slot route for a handful of rows; production registers
// STREAM_RECEIVER_BUFFER and the tenant's posting list is ~1,600 chunks.
// ---------------------------------------------------------------------------

use super::range_read_stream::{STREAM_RECEIVER_BUFFER, STREAM_WINDOW_CHUNKS};

/// Sessions on the crowded tenant. At `CHUNK_PARTITIONS` per chunk this is
/// comfortably more chunks than the route can hold, which is the whole point:
/// the tenant that broke production owns almost every row in the table.
const CROWDED_SESSIONS: u8 = 80;
const CHUNK_PARTITIONS: usize = 2;

/// Read one tenant's index with the producer running AHEAD of the consumer.
///
/// The producer is driven to completion before the consumer drains a single
/// frame. That is not an artificial ordering — it is the loopback cluster,
/// where the wire outruns a consumer doing a k-way merge and a count fold. It
/// just makes the race deterministic.
async fn index_read_with_producer_ahead_of_consumer(
    node: Arc<ferrosa_storage::StorageEngine>,
    max_chunks: u32,
) -> Result<super::stream_consumer::StreamConsumeOutcome, StreamConsumeError> {
    const REQ_ID: u32 = 0x0F_10_0D;
    let router = Arc::new(StreamRouter::new());
    // The PRODUCTION route size. Registering anything larger tests a cluster
    // that does not exist.
    let rx = router.register(REQ_ID, STREAM_RECEIVER_BUFFER);
    let frame_router = Arc::new(StreamFrameRouter::new(router.clone()));
    let req = RangeReadStreamRequestPayload {
        request_id: REQ_ID,
        keyspace: "agent_memory".into(),
        table: "entity_store".into(),
        index_name: Some(TENANT_INDEX.into()),
        index_key: Some(tenant_index_key(TENANT_A)),
        projected_regular_ordinals: None,
        start_key: None,
        start_clustering: None,
        max_chunks,
    };
    let sink = FakeWireSinkShared {
        frame_router: frame_router.clone(),
        from: (Uuid::from_u128(1), "127.0.0.1:7101".parse().unwrap()),
    };

    // Producer first, to completion. Only then does the consumer look.
    handle_stream_request(req, Arc::new(node), &sink, CHUNK_PARTITIONS).await;
    consume_range_stream(rx, IDLE, 1, REQ_ID).await
}

fn crowded_tenant_node(dir: &std::path::Path) -> Arc<ferrosa_storage::StorageEngine> {
    let sessions: Vec<(u8, u8)> = (1..=CROWDED_SESSIONS).map(|s| (TENANT_A, s)).collect();
    tenant_node(dir, &sessions, true)
}

/// RED for t_bf9b4adf: an unwindowed producer overflows the route and the read
/// dies, even though every row was present and readable.
///
/// This is the failure the live cluster shows. It is kept as a test rather than
/// deleted with the fix: `max_chunks: 0` is still a legal wire value (legacy
/// peers), so the day someone reintroduces it on a scan path, this says what
/// happens.
#[tokio::test(flavor = "multi_thread")]
async fn an_unwindowed_index_producer_overflows_the_route_and_fails_the_read() {
    let dir = tempfile::tempdir().unwrap();
    let node = crowded_tenant_node(dir.path());

    let outcome = index_read_with_producer_ahead_of_consumer(node, 0).await;

    match outcome {
        Err(StreamConsumeError::ChannelClosedBeforeDone {
            delivered_done,
            expected_done,
        }) => {
            assert_eq!((delivered_done, expected_done), (0, 1));
        }
        other => panic!(
            "an unwindowed producer must overflow a {STREAM_RECEIVER_BUFFER}-slot route \
             for a {CROWDED_SESSIONS}-row posting list; got {other:?}"
        ),
    }
}

/// GREEN for t_bf9b4adf: the same posting list, the same route, a window.
///
/// The producer stops at the window and reports where to resume, so the route
/// never overflows and the read stays alive. It does NOT deliver every row in
/// one request — that is the point of a window, and the coordinator's
/// `WindowedReplicaForwarder` fires the continuation once the consumer drains.
#[tokio::test(flavor = "multi_thread")]
async fn a_windowed_index_producer_survives_a_posting_list_larger_than_the_route() {
    let dir = tempfile::tempdir().unwrap();
    let node = crowded_tenant_node(dir.path());

    let outcome = index_read_with_producer_ahead_of_consumer(node, STREAM_WINDOW_CHUNKS)
        .await
        .expect("a windowed producer must not overflow the route");

    assert!(
        outcome.partitions.len() <= STREAM_WINDOW_CHUNKS as usize * CHUNK_PARTITIONS,
        "the producer must stop AT the window, not stream past it: got {} partitions",
        outcome.partitions.len()
    );
    assert!(
        !outcome.partitions.is_empty(),
        "a window that delivers nothing is not back-pressure, it is a stall"
    );
}
