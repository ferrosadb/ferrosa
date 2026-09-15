//! A coordinated `fts_match` must fit inside the Bulk-lane read budget on a
//! replica whose index has a compaction history.
//!
//! Live signature (2026-09-15, a local 3-node loopback cluster): every
//! full-text query failed on BOTH remote replicas at once —
//!
//! ```text
//! coordinate_fulltext_search: internal: fulltext search from node … via …:
//!   net: timeout: Bulk lane timeout
//! coordinate_fulltext_search: replica failure makes the result incomplete
//!   failed_nodes=2 keys_received=0
//! ```
//!
//! — while the cluster was otherwise healthy. The network was not at fault:
//! each replica's response arrived 4–10 s AFTER the coordinator's 3 s
//! `BULK_READ_TIMEOUT` (`orphan RPC response: no pending caller for stream`).
//! The replica's local search was simply that slow, and it was slow because of
//! compaction history, not data size: its table dir held 11 live SSTables but
//! 5,122 FTI sidecars for the queried index, and the live compacted SSTables
//! had no sidecar, so every query re-read every sidecar ever written and
//! re-tokenized every compacted SSTable.
//!
//! Harness: the real path end to end at loopback scale — two `RpcServer`s
//! serving the production `FulltextSearchHandler` over real `StorageEngine`s,
//! a real `PeerManager` connection per replica, and the coordinator's real
//! `coordinate_fulltext_search` fan-out on `Lane::Bulk` with its real 3 s
//! budget. Each replica's table carries the live shape: documents rewritten
//! across many flushed generations, then compacted.

use std::sync::Arc;
use std::time::{Duration, Instant};

use arc_swap::ArcSwap;

use ferrosa_common::key::{DecoratedKey, PartitionKey};
use ferrosa_common::schema::{ColumnDefinition, TableSchema};
use ferrosa_common::CellValue;
use ferrosa_sstable::types::{DeletionTime, LivenessInfo, Row};
use ferrosa_storage::{CommitLogConfig, StorageEngine, StorageEngineConfig, TableId};

use ferrosa_cluster::consistency::ConsistencyLevel;
use ferrosa_cluster::coordinator::ClusterCoordinator;
use ferrosa_cluster::raft::handlers::FulltextSearchHandler;
use ferrosa_cluster::raft::{NodeInfo, NodeState};
use ferrosa_cluster::ring::TokenRing;
use ferrosa_net::codec::MsgType;
use ferrosa_net::config::NetConfig;
use ferrosa_net::peer::{PeerEventListener, PeerManager};
use ferrosa_net::rpc::handler::HandlerRegistry;
use ferrosa_net::rpc::server::RpcServer;

const KS: &str = "fts_ks";
const TBL: &str = "entity_store";
const INDEX: &str = "idx_context_snippet_fts";

/// Documents in a replica's bulk generation. Sized so that tokenizing them
/// on every query — what a replica did for each sidecar-less compacted
/// SSTable — takes longer than the coordinator's 3 s Bulk-lane budget in a
/// debug build, while reading them back from an FTI sidecar does not.
const DOCS: usize = 12_000;
/// Rewrite generations flushed on top of the bulk one before compaction, as
/// the memory server does when it updates an entity's snippet. Each leaves an
/// FTI sidecar behind when compaction removes its data.
const REWRITES: usize = 3;
/// Words per document (~2 KB of text, the size of a memory snippet).
const WORDS_PER_DOC: usize = 300;

struct NoopListener;
impl PeerEventListener for NoopListener {
    fn on_peer_connected(&self, _peer: (uuid::Uuid, std::net::SocketAddr)) {}
    fn on_peer_disconnected(&self, _peer: (uuid::Uuid, std::net::SocketAddr)) {}
    fn on_peer_suspected(&self, _peer: (uuid::Uuid, std::net::SocketAddr)) {}
    fn on_peer_recovered(&self, _peer_id: uuid::Uuid) {}
    fn on_peer_failed(&self, _peer_id: uuid::Uuid) {}
}

fn net_config() -> NetConfig {
    NetConfig {
        bind_addr: "127.0.0.1:0".parse().unwrap(),
        ..NetConfig::default()
    }
}

fn ring_node(host_id: uuid::Uuid, addr: &str) -> NodeInfo {
    NodeInfo {
        host_id,
        addr: addr.to_string(),
        data_center: "dc1".to_string(),
        rack: "rack1".to_string(),
        state: NodeState::Normal,
        cql_broadcast: None,
    }
}

fn engine(dir: &std::path::Path) -> Arc<StorageEngine> {
    let config = StorageEngineConfig {
        // Production segment size and sync strategy: the test config's 4 KB
        // segments and per-write fsync turn seeding into minutes of rotation.
        commit_log: CommitLogConfig {
            log_dir: dir.to_path_buf(),
            checkpoint_dir: dir.to_path_buf(),
            archive: None,
            ..CommitLogConfig::default()
        },
        // Flush only when the test says so: one generation per flush.
        flush_threshold_bytes: u64::MAX,
        flush_max_age_secs: 3600,
        ..StorageEngineConfig::test_config(dir)
    };
    let engine = Arc::new(StorageEngine::new(config, None).unwrap());
    engine
        .register_table(TableSchema {
            keyspace: KS.to_string(),
            table: TBL.to_string(),
            key_type: "org.apache.cassandra.db.marshal.UTF8Type".to_string(),
            clustering_columns: vec![],
            static_columns: vec![],
            regular_columns: vec![ColumnDefinition {
                name: "context_snippet".to_string(),
                type_name: "org.apache.cassandra.db.marshal.UTF8Type".to_string(),
            }],
            extensions: Default::default(),
        })
        .unwrap();
    engine
        .add_fulltext_index(&TableId::new(KS, TBL), INDEX, 0)
        .unwrap();
    engine
}

/// Deterministic ~2 KB snippet: `WORDS_PER_DOC` words from a 4,096-word
/// vocabulary, varied by document and generation.
fn snippet(doc: usize, generation: usize) -> String {
    let mut state = (doc as u64 + 1)
        .wrapping_mul(0x9E37_79B9_7F4A_7C15)
        .wrapping_add(generation as u64);
    let mut text = String::with_capacity(WORDS_PER_DOC * 8);
    for _ in 0..WORDS_PER_DOC {
        state = state
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        text.push_str(&format!("word{} ", (state >> 33) % 4096));
    }
    text
}

fn row(text: &str, ts: i64) -> Row {
    Row {
        clustering: vec![],
        cells: vec![(0, CellValue::live(text.as_bytes().to_vec(), ts))],
        deletion: DeletionTime::LIVE,
        primary_key_liveness: LivenessInfo::with_timestamp(ts),
    }
}

fn doc_key(doc: usize) -> DecoratedKey {
    DecoratedKey::new(PartitionKey::new(format!("entity_{doc:04}").into_bytes()))
}

/// The live shape: a bulk generation of `DOCS` snippets, `REWRITES` flushed
/// generations that rewrite doc 0, then one compaction over all of them. Doc 0
/// carries `stale` only in the bulk generation and `current` only in the last
/// rewrite, so a query for `stale` has exactly one right answer (nothing) and a
/// query for `current` exactly one (doc 0).
async fn seed_compacted_history(engine: &StorageEngine, stale: &str, current: &str) {
    let table = TableId::new(KS, TBL);
    for doc in 0..DOCS {
        let mut text = snippet(doc, 0);
        if doc == 0 {
            text.push_str(stale);
        }
        let ts = (doc + 1) as i64;
        engine
            .write(&table, &doc_key(doc), row(&text, ts), ts)
            .unwrap();
    }
    let written = Instant::now();
    engine.flush(&table).unwrap();
    eprintln!("seed: flushed {DOCS} docs in {:?}", written.elapsed());
    for rewrite in 1..=REWRITES {
        let mut text = snippet(0, rewrite);
        if rewrite == REWRITES {
            text.push_str(current);
        }
        let ts = (DOCS + rewrite) as i64;
        engine
            .write(&table, &doc_key(0), row(&text, ts), ts)
            .unwrap();
        engine.flush(&table).unwrap();
    }

    let before = engine.sstable_count(&table);
    let compact_start = Instant::now();
    engine.force_compact_all();
    for _ in 0..4_800 {
        engine.poll_compactions().await;
        if engine.sstable_count(&table) == 1 {
            eprintln!(
                "seed: compacted {before} SSTables in {:?}",
                compact_start.elapsed()
            );
            return;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    panic!(
        "seed: compaction of {before} SSTables did not swap within 120 s \
         (sstable_count now {})",
        engine.sstable_count(&table)
    );
}

async fn spawn_replica(
    engine: Arc<StorageEngine>,
) -> (Arc<RpcServer>, std::net::SocketAddr, uuid::Uuid) {
    let host_id = uuid::Uuid::new_v4();
    let registry = Arc::new(HandlerRegistry::new());
    registry.register(
        MsgType::FulltextSearchRequest,
        Arc::new(FulltextSearchHandler::new(engine)),
    );
    let server = Arc::new(RpcServer::new(net_config(), host_id, registry));
    let addr = server.start_and_get_addr().await.unwrap();
    (server, addr, host_id)
}

fn partition_keys(hits: &[Vec<u8>]) -> Vec<String> {
    let mut keys: Vec<String> = hits
        .iter()
        .map(|dk| {
            let pk = ferrosa_index::fulltext::keys::doc_key_partition(dk)
                .expect("fulltext search returns row-granular doc keys");
            String::from_utf8_lossy(pk).to_string()
        })
        .collect();
    keys.sort();
    keys.dedup();
    keys
}

#[test]
fn coordinated_fulltext_search_fits_the_bulk_lane_budget_after_compaction() {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .unwrap();

    // Surface the coordinator's own ERROR lines (`failed_nodes=… keys_received=…`)
    // so a failure reads like the live log.
    let _ = tracing_subscriber::fmt()
        .with_max_level(tracing::Level::ERROR)
        .with_test_writer()
        .try_init();

    rt.block_on(async move {
        // Two terms each: the memory server sends multi-word queries, which
        // take the compound (non-streaming) evaluation path on a replica.
        const STALE: &str = " ferrosastalesnippet ferrosastaletopic";
        const CURRENT: &str = " ferrosacurrentsnippet ferrosacurrenttopic";

        let dirs: Vec<tempfile::TempDir> = (0..3).map(|_| tempfile::tempdir().unwrap()).collect();
        let engines: Vec<Arc<StorageEngine>> = dirs.iter().map(|d| engine(d.path())).collect();

        let seed_start = Instant::now();
        // Only the remote replicas carry data: the coordinator's own search is
        // not subject to the Bulk-lane budget, the replicas' is.
        let seeds = engines[1..].iter().map(|e| {
            let e = e.clone();
            tokio::spawn(async move { seed_compacted_history(&e, STALE, CURRENT).await })
        });
        for seed in seeds.collect::<Vec<_>>() {
            seed.await.unwrap();
        }
        let table = TableId::new(KS, TBL);
        eprintln!(
            "seeded 2 replicas in {:?}: {} live SSTable(s) each after compacting {} generations",
            seed_start.elapsed(),
            engines[1].sstable_count(&table),
            REWRITES + 1,
        );

        // The replica-local work one RPC waits on, measured without the lane
        // budget cutting it short.
        let replica = engines[1].clone();
        let local = tokio::task::spawn_blocking(move || {
            let start = Instant::now();
            let keys =
                replica.fulltext_search(&TableId::new(KS, TBL), INDEX, CURRENT.trim(), Some(20));
            (start.elapsed(), keys.map(|k| k.len()))
        })
        .await
        .unwrap();
        eprintln!(
            "replica-local fulltext_search took {:?}: {:?}",
            local.0, local.1
        );

        let (srv2, addr2, r2_id) = spawn_replica(engines[1].clone()).await;
        let (srv3, addr3, r3_id) = spawn_replica(engines[2].clone()).await;

        let coord_id = uuid::Uuid::new_v4();
        let mut ring = TokenRing::new();
        ring.add_node(1, ring_node(coord_id, "127.0.0.1:1"));
        ring.add_node(2, ring_node(r2_id, &addr2.to_string()));
        ring.add_node(3, ring_node(r3_id, &addr3.to_string()));
        ring.assign_tokens(1, &[i64::MIN]);
        ring.assign_tokens(2, &[0]);
        ring.assign_tokens(3, &[i64::MAX / 2]);

        let peers = Arc::new(PeerManager::new(
            Arc::new(net_config()),
            coord_id,
            Arc::new(NoopListener),
        ));
        peers.ensure_peer(r2_id, &addr2.to_string()).await.unwrap();
        peers.ensure_peer(r3_id, &addr3.to_string()).await.unwrap();

        let coordinator = ClusterCoordinator::new(
            Arc::new(ArcSwap::from_pointee(ring)),
            peers,
            1,
            engines[0].clone(),
            3,
            ConsistencyLevel::One,
        );

        // The memory server's shape: a multi-word query with a LIMIT.
        let mut latencies = Vec::new();
        for (query, expected) in [
            (CURRENT.trim(), vec!["entity_0000".to_string()]),
            (STALE.trim(), vec![]),
            (CURRENT.trim(), vec!["entity_0000".to_string()]),
        ] {
            let start = Instant::now();
            let result = coordinator
                .coordinate_fulltext_search(&table, INDEX, query, Some(20))
                .await;
            let elapsed = start.elapsed();
            latencies.push(elapsed);
            eprintln!(
                "fts_match({query:?}) took {elapsed:?}: {:?}",
                result.as_ref().map(|k| k.len())
            );
            let keys = result.unwrap_or_else(|e| {
                panic!(
                    "coordinated fulltext search failed after {elapsed:?} — a replica \
                     did not answer inside the Bulk-lane budget: {e}"
                )
            });
            assert_eq!(
                partition_keys(&keys),
                expected,
                "fts_match({query:?}) must match only current row text"
            );
        }
        eprintln!("coordinated fts_match latencies: {latencies:?}");

        srv2.shutdown(Duration::from_millis(50)).await;
        srv3.shutdown(Duration::from_millis(50)).await;
    });
}

// ── Edge cases: the budget still bites, loudly, when a replica is at fault ──

/// A replica that answers every `FulltextSearchRequest` with `keys` after
/// `delay` — a stand-in for a slow or hung node that exercises the same
/// server, connection and lane as the production handler.
struct ScriptedReplica {
    delay: Duration,
    keys: Vec<Vec<u8>>,
}

#[async_trait::async_trait]
impl ferrosa_net::rpc::handler::RpcHandler for ScriptedReplica {
    async fn handle(
        &self,
        _from: ferrosa_net::rpc::handler::PeerId,
        msg: ferrosa_net::message::Message,
    ) -> Option<ferrosa_net::message::Message> {
        let ferrosa_net::message::Message::FulltextSearchRequest(_) = msg else {
            return None;
        };
        tokio::time::sleep(self.delay).await;
        let payload = ferrosa_cluster::raft::handlers::FulltextSearchResponsePayload {
            matching_keys: self.keys.clone(),
        };
        Some(ferrosa_net::message::Message::FulltextSearchResponse(
            bytes::Bytes::from(bincode::serialize(&payload).unwrap()),
        ))
    }
}

async fn spawn_scripted(
    config: &NetConfig,
    replica: ScriptedReplica,
) -> (Arc<RpcServer>, std::net::SocketAddr, uuid::Uuid) {
    let host_id = uuid::Uuid::new_v4();
    let registry = Arc::new(HandlerRegistry::new());
    registry.register(MsgType::FulltextSearchRequest, Arc::new(replica));
    let server = Arc::new(RpcServer::new(config.clone(), host_id, registry));
    let addr = server.start_and_get_addr().await.unwrap();
    (server, addr, host_id)
}

/// A coordinator (node 1, empty local table) over `replicas`. Each replica is
/// `(host_id, addr, connect)`; `connect: false` leaves it in the ring with no
/// connection opened up front, as a node that went away would be.
async fn coordinator_over(
    config: &NetConfig,
    local: Arc<StorageEngine>,
    replicas: &[(uuid::Uuid, std::net::SocketAddr, bool)],
) -> ClusterCoordinator {
    let coord_id = uuid::Uuid::new_v4();
    let mut ring = TokenRing::new();
    ring.add_node(1, ring_node(coord_id, "127.0.0.1:1"));
    ring.assign_tokens(1, &[i64::MIN]);
    let peers = Arc::new(PeerManager::new(
        Arc::new(config.clone()),
        coord_id,
        Arc::new(NoopListener),
    ));
    for (i, (host_id, addr, connect)) in replicas.iter().enumerate() {
        let node_id = i as u64 + 2;
        ring.add_node(node_id, ring_node(*host_id, &addr.to_string()));
        ring.assign_tokens(node_id, &[i as i64 * 1_000]);
        if *connect {
            peers
                .ensure_peer(*host_id, &addr.to_string())
                .await
                .unwrap();
        }
    }
    ClusterCoordinator::new(
        Arc::new(ArcSwap::from_pointee(ring)),
        peers,
        1,
        local,
        replicas.len() + 1,
        ConsistencyLevel::One,
    )
}

fn key(n: usize) -> Vec<u8> {
    ferrosa_index::fulltext::keys::encode_doc_key(format!("entity_{n:04}").as_bytes(), &[])
}

fn multi_thread_rt() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .unwrap()
}

/// Slow is not down: a replica that takes a second is inside the 3 s budget
/// and its keys are in the union. A replica that never answers fails the query
/// at the budget, naming the node — never a silent partial result.
#[test]
fn slow_replica_inside_the_budget_counts_and_a_hung_one_fails_loudly() {
    multi_thread_rt().block_on(async {
        let config = net_config();
        let dir = tempfile::tempdir().unwrap();
        let local = engine(dir.path());
        let table = TableId::new(KS, TBL);

        let (slow_srv, slow_addr, slow_id) = spawn_scripted(
            &config,
            ScriptedReplica {
                delay: Duration::from_secs(1),
                keys: vec![key(1)],
            },
        )
        .await;
        let (fast_srv, fast_addr, fast_id) = spawn_scripted(
            &config,
            ScriptedReplica {
                delay: Duration::ZERO,
                keys: vec![key(2)],
            },
        )
        .await;
        let coordinator = coordinator_over(
            &config,
            local.clone(),
            &[(slow_id, slow_addr, true), (fast_id, fast_addr, true)],
        )
        .await;
        let start = Instant::now();
        let keys = coordinator
            .coordinate_fulltext_search(&table, INDEX, "anything", Some(10))
            .await
            .expect("a replica answering in 1 s is inside the 3 s budget");
        assert_eq!(
            partition_keys(&keys),
            vec!["entity_0001".to_string(), "entity_0002".to_string()],
            "the slow replica's keys must be in the union"
        );
        assert!(start.elapsed() >= Duration::from_secs(1));

        let (hung_srv, hung_addr, hung_id) = spawn_scripted(
            &config,
            ScriptedReplica {
                delay: Duration::from_secs(3_600),
                keys: vec![key(3)],
            },
        )
        .await;
        let coordinator = coordinator_over(
            &config,
            local,
            &[(fast_id, fast_addr, true), (hung_id, hung_addr, true)],
        )
        .await;
        let start = Instant::now();
        let err = coordinator
            .coordinate_fulltext_search(&table, INDEX, "anything", Some(10))
            .await
            .expect_err("a replica that never answers must fail the query");
        let elapsed = start.elapsed();
        let message = err.to_string();
        assert!(
            message.contains("Bulk lane timeout") && message.contains(&hung_id.to_string()),
            "the failure must be the lane timeout and name the hung node, got: {message}"
        );
        assert!(
            elapsed >= Duration::from_secs(3) && elapsed < Duration::from_secs(10),
            "the hung replica must fail at the 3 s budget, not before or long after: {elapsed:?}"
        );

        for srv in [slow_srv, fast_srv, hung_srv] {
            srv.shutdown(Duration::from_millis(50)).await;
        }
    });
}

/// A replica that is gone — nothing listening at its address — fails the query
/// promptly and names the node, rather than shrinking the result.
#[test]
fn down_replica_fails_the_query_naming_the_node() {
    multi_thread_rt().block_on(async {
        let config = net_config();
        let dir = tempfile::tempdir().unwrap();
        let local = engine(dir.path());

        let (up_srv, up_addr, up_id) = spawn_scripted(
            &config,
            ScriptedReplica {
                delay: Duration::ZERO,
                keys: vec![key(1)],
            },
        )
        .await;
        // An address that was just free: bound, then released.
        let down_addr = std::net::TcpListener::bind("127.0.0.1:0")
            .unwrap()
            .local_addr()
            .unwrap();
        let down_id = uuid::Uuid::new_v4();
        let coordinator = coordinator_over(
            &config,
            local,
            &[(up_id, up_addr, true), (down_id, down_addr, false)],
        )
        .await;

        let start = Instant::now();
        let err = coordinator
            .coordinate_fulltext_search(&TableId::new(KS, TBL), INDEX, "anything", Some(10))
            .await
            .expect_err("a down replica must fail the query, not shrink the result");
        assert!(
            err.to_string().contains(&down_id.to_string()),
            "the failure must name the down node, got: {err}"
        );
        assert!(
            start.elapsed() < Duration::from_secs(10),
            "a down replica must not hang the query: {:?}",
            start.elapsed()
        );
        up_srv.shutdown(Duration::from_millis(50)).await;
    });
}

/// A response near the frame limit is delivered in full. One over it fails the
/// query naming the replica — never a partial union.
///
/// It fails at the Bulk-lane budget, as a `Bulk lane timeout`: the receiving
/// connection rejects the oversized frame and closes, but the request that was
/// waiting on it is only released by its timeout. `ferrosa-net` now logs the
/// rejected frame at ERROR, so the log says why; failing the waiting request
/// the moment its connection dies is tracked as follow-up work.
#[test]
fn replica_response_near_the_frame_limit_is_delivered_and_over_it_fails_the_query() {
    const FRAME_LIMIT: u32 = 64 * 1024;
    multi_thread_rt().block_on(async {
        let config = NetConfig {
            max_frame_body_size: FRAME_LIMIT,
            ..net_config()
        };
        let dir = tempfile::tempdir().unwrap();
        let local = engine(dir.path());
        let table = TableId::new(KS, TBL);

        // Each key encodes to 8 (bincode length) + 4 + 11 = 23 bytes.
        let per_key = bincode::serialize(&vec![key(0)]).unwrap().len() - 8;
        let fits = (FRAME_LIMIT as usize - 64) / per_key;
        let (near_srv, near_addr, near_id) = spawn_scripted(
            &config,
            ScriptedReplica {
                delay: Duration::ZERO,
                keys: (0..fits).map(key).collect(),
            },
        )
        .await;
        let coordinator =
            coordinator_over(&config, local.clone(), &[(near_id, near_addr, true)]).await;
        let keys = coordinator
            .coordinate_fulltext_search(&table, INDEX, "anything", None)
            .await
            .expect("a response under the frame limit must be delivered");
        assert_eq!(keys.len(), fits, "every key in a near-limit response");

        let over = (FRAME_LIMIT as usize * 2) / per_key;
        let (over_srv, over_addr, over_id) = spawn_scripted(
            &config,
            ScriptedReplica {
                delay: Duration::ZERO,
                keys: (0..over).map(key).collect(),
            },
        )
        .await;
        let coordinator = coordinator_over(&config, local, &[(over_id, over_addr, true)]).await;
        let start = Instant::now();
        let err = coordinator
            .coordinate_fulltext_search(&table, INDEX, "anything", None)
            .await
            .expect_err("a response over the frame limit must fail the query");
        eprintln!(
            "over-limit response failed after {:?}: {err}",
            start.elapsed()
        );
        assert!(
            err.to_string().contains(&over_id.to_string()),
            "the failure must name the replica, got: {err}"
        );

        near_srv.shutdown(Duration::from_millis(50)).await;
        over_srv.shutdown(Duration::from_millis(50)).await;
    });
}
