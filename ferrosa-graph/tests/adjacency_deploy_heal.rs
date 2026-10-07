//! Deploying the traversal fix onto data the pre-fix reconcile damaged.
//!
//! Before 330a0c29 the adjacency reconcile assumed every edge table is keyed
//! `(graph.source)` / `(graph.target)`. agent_memory's `typed_edges` is keyed
//! `((tenant_id, session_id), src_id, edge_type, dst_id)`, so the orphan scan
//! never found the edge behind an adjacency entry and TOMBSTONED every one of
//! them, on every pass. Traversals ignored tombstones, so nobody noticed.
//!
//! The fix makes traversals honour tombstones. On a node whose adjacency index
//! the old reconcile already damaged, every typed edge then vanishes from every
//! traversal until something rebuilds the entries. These tests build that
//! state the way a live memory cluster reached it — edges written through the
//! same Cypher ferrosa-memory sends, then the pre-fix reconcile (a faithful
//! copy of main's code, in `old_build`) run over them — and then start a fresh
//! engine on the new code, as the first start after the deploy does.
//!
//! The contract: the first traversals answer from a healed index (every live
//! edge, in the right direction, no deleted edge), or fail retryably. Never a
//! partial answer presented as complete.

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::sync::Arc;

use futures::future::join_all;
use indexmap::IndexMap;
use serde_json::Value;
use tempfile::TempDir;
use uuid::Uuid;

use ferrosa_cluster::write_path::WritePath;
use ferrosa_common::schema::TableSchema;
use ferrosa_common::{CellValue, DecoratedKey, PartitionKey};
use ferrosa_graph::engine::GraphEngine;
use ferrosa_graph::executor::expand::GraphEngineConfig;
use ferrosa_schema::auth::role::AuthContext;
use ferrosa_schema::metadata::column::{ClusteringOrder, ColumnKind, ColumnMetadata};
use ferrosa_schema::metadata::keyspace::{KeyspaceMetadata, ReplicationParams};
use ferrosa_schema::metadata::table::{TableMetadata, TableParams};
use ferrosa_schema::{
    AuthMethod, DeploymentMode, EnvSecretsProvider, PasswordHasher, PasswordPolicy,
    RateLimitConfig, Schema, SchemaConfig, TestAuditSink,
};
use ferrosa_sstable::types::{DeletionTime, LivenessInfo, Row};
use ferrosa_storage::{
    CommitLogConfig, CompactionConfig, StorageEngine, StorageEngineConfig, SyncStrategyConfig,
    TableId,
};

const KEYSPACE: &str = "agent_memory";
const ADJACENCY_KEYSPACE: &str = "system_graph_agent_memory";

// ── Fixture ──────────────────────────────────────────────────────────────

fn storage_config(dir: &TempDir) -> StorageEngineConfig {
    StorageEngineConfig {
        commit_log: CommitLogConfig {
            segment_size: 4 * 1024 * 1024,
            max_segment_age: std::time::Duration::from_secs(60),
            // The production default (periodic fsync), so timings match a
            // node's, not a per-write-fsync configuration's.
            sync_strategy: SyncStrategyConfig::default(),
            batch: Default::default(),
            log_dir: dir.path().join("commitlog"),
            checkpoint_dir: dir.path().join("commitlog"),
            archive: None,
        },
        compaction: CompactionConfig::from_env(dir.path().join("compaction")),
        object_store: None,
        local_cache_max_bytes: 64 * 1024 * 1024,
        local_disk_free_reserve_bytes: 0,
        flush_threshold_bytes: 64 * 1024 * 1024,
        memtable_backpressure_bytes: u64::MAX,
        flush_max_age_secs: 3600,
        data_dir: dir.path().to_path_buf(),
        index_backend: ferrosa_storage::index::IndexBackendConfig::Local,
        write_verify: true,
        auth_enabled: false,
        auth_warn: false,
        max_pending_replay_mutations_without_schema: 1024,
        memtable_num_shards: 64,
        cache_hot_window_secs: 900,
    }
}

fn superuser() -> AuthContext {
    AuthContext {
        role: "ferrosa_admin".to_string(),
        is_superuser: true,
        must_change_password: false,
    }
}

fn column(name: &str, kind: ColumnKind, position: i32, ty: &str) -> ColumnMetadata {
    let clustering_order = if kind == ColumnKind::Clustering {
        ClusteringOrder::Asc
    } else {
        ClusteringOrder::None
    };
    ColumnMetadata {
        name: name.to_string(),
        kind,
        position,
        column_type: ty.to_string(),
        clustering_order,
        mask: None,
    }
}

fn table(
    name: &str,
    columns: &[(&str, ColumnKind, i32, &str)],
    partition_key: &[&str],
    clustering_key: &[&str],
    extensions: &[(&str, &str)],
) -> TableMetadata {
    let mut cols = IndexMap::new();
    for (col, kind, pos, ty) in columns {
        cols.insert(col.to_string(), column(col, *kind, *pos, ty));
    }
    TableMetadata {
        keyspace: KEYSPACE.to_string(),
        name: name.to_string(),
        id: Uuid::new_v4(),
        columns: cols,
        partition_key: partition_key.iter().map(|c| c.to_string()).collect(),
        clustering_key: clustering_key
            .iter()
            .map(|c| (c.to_string(), ClusteringOrder::Asc))
            .collect(),
        params: TableParams::default(),
        flags: HashSet::new(),
        extensions: extensions
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect(),
        is_system: false,
    }
}

/// agent_memory's `entity_store` and `typed_edges`, keyed as
/// ferrosa-memory's `ddl/017_typed_edges.cql` keys them, with the graph
/// extensions its `ALTER TABLE` sets.
fn create_agent_memory_schema(schema: &Schema, replication_factor: usize) {
    use ColumnKind::{Clustering, PartitionKey as Pk, Regular};
    schema
        .create_keyspace_internal(KeyspaceMetadata {
            name: KEYSPACE.to_string(),
            durable_writes: true,
            replication: ReplicationParams {
                strategy: "SimpleStrategy".to_string(),
                options: HashMap::from([(
                    "replication_factor".to_string(),
                    replication_factor.to_string(),
                )]),
            },
        })
        .unwrap();
    schema
        .create_table_internal(table(
            "entity_store",
            &[
                ("tenant_id", Pk, 0, "uuid"),
                ("session_id", Pk, 1, "uuid"),
                ("entity_id", Clustering, 0, "uuid"),
                ("entity_name", Regular, -1, "text"),
            ],
            &["tenant_id", "session_id"],
            &["entity_id"],
            &[("graph.type", "vertex"), ("graph.label", "Entity")],
        ))
        .unwrap();
    schema
        .create_table_internal(table(
            "typed_edges",
            &[
                ("tenant_id", Pk, 0, "uuid"),
                ("session_id", Pk, 1, "uuid"),
                ("src_id", Clustering, 0, "uuid"),
                ("edge_type", Clustering, 1, "text"),
                ("dst_id", Clustering, 2, "uuid"),
                ("weight", Regular, -1, "double"),
                ("metadata", Regular, -1, "text"),
                ("created_at", Regular, -1, "timestamp"),
            ],
            &["tenant_id", "session_id"],
            &["src_id", "edge_type", "dst_id"],
            &[
                ("graph.type", "edge"),
                ("graph.label", "TYPED_EDGE"),
                ("graph.source", "src_id"),
                ("graph.target", "dst_id"),
                ("graph.source_label", "Entity"),
                ("graph.target_label", "Entity"),
            ],
        ))
        .unwrap();
}

fn register_storage_tables(storage: &StorageEngine) {
    for name in ["entity_store", "typed_edges"] {
        storage
            .register_table(TableSchema {
                keyspace: KEYSPACE.to_string(),
                table: name.to_string(),
                key_type: "org.apache.cassandra.db.marshal.BytesType".to_string(),
                clustering_columns: vec![],
                static_columns: vec![],
                regular_columns: vec![],
                extensions: HashMap::new(),
            })
            .unwrap();
    }
}

struct Node {
    schema: Arc<Schema>,
    storage: Arc<StorageEngine>,
    _dir: TempDir,
}

impl Node {
    fn new() -> Self {
        Self::with_replication_factor(1)
    }

    fn with_replication_factor(replication_factor: usize) -> Self {
        let dir = TempDir::new().unwrap();
        let storage = Arc::new(StorageEngine::new(storage_config(&dir), None).unwrap());
        let schema = Arc::new(
            Schema::new(SchemaConfig {
                hasher: PasswordHasher::Bcrypt { cost: 4 },
                password_policy: PasswordPolicy::permissive(),
                auth_method: AuthMethod::Password,
                rate_limit: RateLimitConfig::default(),
                audit_sink: Box::new(TestAuditSink::new()),
                secrets: Box::new(EnvSecretsProvider),
                mode: DeploymentMode::Development,
            })
            .unwrap(),
        );
        create_agent_memory_schema(&schema, replication_factor);
        register_storage_tables(&storage);
        Self {
            schema,
            storage,
            _dir: dir,
        }
    }

    fn write_path(&self) -> WritePath {
        WritePath::direct(Arc::clone(&self.storage))
    }

    /// A graph engine as a freshly started process builds it: nothing
    /// registered, no reconcile run yet.
    fn start_engine(&self) -> GraphEngine {
        GraphEngine::new(
            Arc::clone(&self.schema),
            Arc::clone(&self.storage),
            Arc::new(arc_swap::ArcSwap::from_pointee(self.write_path())),
            GraphEngineConfig::default(),
            std::time::Duration::from_secs(300),
        )
    }
}

// ── The graph, and the reference it is checked against ──────────────────

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Debug)]
struct Edge {
    tenant: Uuid,
    session: Uuid,
    src: Uuid,
    edge_type: &'static str,
    dst: Uuid,
}

fn entity(tenant: usize, session: usize, i: usize) -> Uuid {
    Uuid::from_u128(
        0xE000_0000_0000_0000_0000_0000_0000_0000
            | ((tenant as u128) << 40)
            | ((session as u128) << 20)
            | i as u128,
    )
}

fn tenant_id(t: usize) -> Uuid {
    Uuid::from_u128(0x7000_0000_0000_0000_0000_0000_0000_0000 | t as u128)
}

fn session_id(t: usize, s: usize) -> Uuid {
    Uuid::from_u128(0x5000_0000_0000_0000_0000_0000_0000_0000 | ((t as u128) << 20) | s as u128)
}

/// `tenants` x `sessions` sessions of `per_session` entities. Entity i links to
/// i+1 (`related_to`) and i+3 (`depends_on`); every fourth entity also links to
/// i+1 with `depends_on`, so some pairs carry two edges of different types
/// that share ONE adjacency entry (the entry is keyed by label, not
/// `edge_type`).
fn generate_edges(tenants: usize, sessions: usize, per_session: usize) -> Vec<Edge> {
    let mut edges = Vec::new();
    for t in 0..tenants {
        for s in 0..sessions {
            let (tenant, session) = (tenant_id(t), session_id(t, s));
            for i in 0..per_session {
                let src = entity(t, s, i);
                let mut push = |edge_type, j: usize| {
                    edges.push(Edge {
                        tenant,
                        session,
                        src,
                        edge_type,
                        dst: entity(t, s, j % per_session),
                    })
                };
                push("related_to", i + 1);
                push("depends_on", i + 3);
                if i % 4 == 0 {
                    push("depends_on", i + 1);
                }
            }
        }
    }
    edges
}

fn q(id: Uuid) -> String {
    format!("'{id}'")
}

/// ferrosa-memory's `build_typed_edge_merge_query`.
fn merge_query(e: &Edge) -> String {
    format!(
        "MERGE (a:Entity {{tenant_id: {t}, session_id: {s}, entity_id: {src}}})\
         MERGE (b:Entity {{tenant_id: {t}, session_id: {s}, entity_id: {dst}}})\
         MERGE (a)-[r:TYPED_EDGE {{tenant_id: {t}, session_id: {s}, edge_type: '{ty}'}}]->(b) \
         SET r.weight = 1.0, r.created_at = '2026-10-03T00:00:00Z' RETURN r",
        t = q(e.tenant),
        s = q(e.session),
        src = q(e.src),
        dst = q(e.dst),
        ty = e.edge_type,
    )
}

/// ferrosa-memory's `build_typed_edge_delete_query`.
fn delete_query(e: &Edge) -> String {
    format!(
        "MATCH (a:Entity {{tenant_id: {t}, session_id: {s}, entity_id: {src}}})\
         -[r:TYPED_EDGE {{tenant_id: {t}, session_id: {s}, edge_type: '{ty}'}}]->\
         (b:Entity {{tenant_id: {t}, session_id: {s}, entity_id: {dst}}}) \
         DELETE r",
        t = q(e.tenant),
        s = q(e.session),
        src = q(e.src),
        dst = q(e.dst),
        ty = e.edge_type,
    )
}

/// ferrosa-memory's `find_related_entities`: one hop OUT.
fn related_query(tenant: Uuid, session: Uuid, src: Uuid) -> String {
    format!(
        "MATCH (start:Entity {{tenant_id: {t}, session_id: {s}, entity_id: {src}}})\
         -[r:TYPED_EDGE {{tenant_id: {t}, session_id: {s}}}]->\
         (related:Entity {{tenant_id: {t}, session_id: {s}}}) \
         RETURN DISTINCT related.entity_id AS related_id",
        t = q(tenant),
        s = q(session),
        src = q(src),
    )
}

/// A bare hop: no relationship variable or property, so the answer comes from
/// the adjacency index alone, with no edge-row lookup to filter it.
fn bare_hop_query(tenant: Uuid, session: Uuid, src: Uuid) -> String {
    format!(
        "MATCH (start:Entity {{tenant_id: {t}, session_id: {s}, entity_id: {src}}})\
         -[:TYPED_EDGE]->(related:Entity) \
         RETURN DISTINCT related.entity_id AS related_id",
        t = q(tenant),
        s = q(session),
        src = q(src),
    )
}

/// ferrosa-memory's `list_typed_edges_to`: one hop IN.
fn edges_to_query(tenant: Uuid, session: Uuid, dst: Uuid) -> String {
    format!(
        "MATCH (src:Entity {{tenant_id: {t}, session_id: {s}}})\
         -[r:TYPED_EDGE {{tenant_id: {t}, session_id: {s}}}]->\
         (victim:Entity {{tenant_id: {t}, session_id: {s}, entity_id: {dst}}}) \
         RETURN DISTINCT src.entity_id AS src_id, r.edge_type AS edge_type",
        t = q(tenant),
        s = q(session),
        dst = q(dst),
    )
}

/// ferrosa-memory's `stats` edge count.
fn count_query(tenant: Uuid) -> String {
    format!(
        "MATCH (a:Entity)-[r:TYPED_EDGE {{tenant_id: {}}}]->(b:Entity) RETURN count(r)",
        q(tenant)
    )
}

fn text(v: &Value) -> String {
    match v {
        Value::String(s) => s.to_lowercase(),
        other => panic!("expected a string, got {other}"),
    }
}

async fn run(engine: &GraphEngine, query: &str) -> ferrosa_graph::error::Result<Vec<Vec<Value>>> {
    engine
        .execute(query, KEYSPACE, &superuser())
        .await
        .map(|r| r.rows)
}

async fn run_ok(engine: &GraphEngine, query: &str) -> Vec<Vec<Value>> {
    run(engine, query)
        .await
        .unwrap_or_else(|e| panic!("query failed: {e}\n{query}"))
}

/// What the graph must answer, derived from the live edge set.
struct Reference {
    out: BTreeMap<(Uuid, Uuid, Uuid), BTreeSet<String>>,
    into: BTreeMap<(Uuid, Uuid, Uuid), BTreeSet<(String, String)>>,
    count_by_tenant: BTreeMap<Uuid, usize>,
    vertices: BTreeSet<(Uuid, Uuid, Uuid)>,
}

impl Reference {
    fn new(all: &[Edge], live: &BTreeSet<Edge>) -> Self {
        let mut r = Reference {
            out: BTreeMap::new(),
            into: BTreeMap::new(),
            count_by_tenant: BTreeMap::new(),
            vertices: BTreeSet::new(),
        };
        for e in all {
            r.vertices.insert((e.tenant, e.session, e.src));
            r.vertices.insert((e.tenant, e.session, e.dst));
            r.count_by_tenant.entry(e.tenant).or_default();
        }
        for e in live {
            r.out
                .entry((e.tenant, e.session, e.src))
                .or_default()
                .insert(e.dst.to_string());
            r.into
                .entry((e.tenant, e.session, e.dst))
                .or_default()
                .insert((e.src.to_string(), e.edge_type.to_string()));
            *r.count_by_tenant.entry(e.tenant).or_default() += 1;
        }
        r
    }
}

/// Every traversal ferrosa-memory issues, for every vertex, all issued at once
/// against `engine` — as the first requests after a restart arrive together.
/// Returns the mismatches, so one failure shows the whole damage.
async fn check_all(engine: &GraphEngine, reference: &Reference) -> Vec<String> {
    let queries: Vec<(String, (Uuid, Uuid, Uuid), bool)> = reference
        .vertices
        .iter()
        .flat_map(|&(t, s, v)| {
            [
                (related_query(t, s, v), (t, s, v), true),
                (bare_hop_query(t, s, v), (t, s, v), true),
                (edges_to_query(t, s, v), (t, s, v), false),
            ]
        })
        .collect();
    let answers = join_all(queries.iter().map(|(query, _, _)| run(engine, query))).await;

    let mut mismatches = Vec::new();
    for ((query, key, is_out), answer) in queries.iter().zip(answers) {
        let rows = match answer {
            Ok(rows) => rows,
            Err(e) => {
                mismatches.push(format!("{key:?} failed: {e}\n  {query}"));
                continue;
            }
        };
        if *is_out {
            let got: BTreeSet<String> = rows.iter().map(|r| text(&r[0])).collect();
            let want = reference.out.get(key).cloned().unwrap_or_default();
            if got != want {
                mismatches.push(format!("OUT {key:?}: got {got:?}, want {want:?}"));
            }
        } else {
            // Every source in, and every (source, edge_type) returned is a live
            // edge. Not yet: every edge_type of a two-type pair — a hop binds
            // one edge row per adjacency entry (t_9049eab1, pre-existing on
            // main); tighten this to set equality when that lands.
            let got: BTreeSet<(String, String)> =
                rows.iter().map(|r| (text(&r[0]), text(&r[1]))).collect();
            let want = reference.into.get(key).cloned().unwrap_or_default();
            let sources = |s: &BTreeSet<(String, String)>| -> BTreeSet<String> {
                s.iter().map(|(src, _)| src.clone()).collect()
            };
            if sources(&got) != sources(&want) || !got.is_subset(&want) {
                mismatches.push(format!("IN {key:?}: got {got:?}, want {want:?}"));
            }
        }
    }
    mismatches
}

async fn check_counts(engine: &GraphEngine, reference: &Reference) -> Vec<String> {
    let mut mismatches = Vec::new();
    for (&tenant, &want) in &reference.count_by_tenant {
        let rows = run_ok(engine, &count_query(tenant)).await;
        let got = rows[0][0].as_u64().expect("count(r) is an integer") as usize;
        if got != want {
            mismatches.push(format!("count(r) tenant {tenant}: got {got}, want {want}"));
        }
    }
    mismatches
}

fn assert_clean(what: &str, mismatches: &[String]) {
    assert!(
        mismatches.is_empty(),
        "{what}: {} mismatches, first {}:\n{}",
        mismatches.len(),
        mismatches.len().min(12),
        mismatches[..mismatches.len().min(12)].join("\n")
    );
}

// ── The pre-fix state ────────────────────────────────────────────────────

/// Adjacency clustering, as `make_adjacency_mutation` encodes it.
fn adjacency_clustering(direction: u8, label: &str, neighbor: &[u8]) -> Vec<u8> {
    let mut c = Vec::new();
    c.extend_from_slice(&1u16.to_be_bytes());
    c.push(direction);
    c.extend_from_slice(&(label.len() as u16).to_be_bytes());
    c.extend_from_slice(label.as_bytes());
    c.extend_from_slice(&(neighbor.len() as u16).to_be_bytes());
    c.extend_from_slice(neighbor);
    c
}

fn now_micros() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_micros() as i64
}

fn typed_edge_key(e: &Edge) -> (DecoratedKey, Vec<u8>) {
    let mut pk = Vec::new();
    for c in [e.tenant.as_bytes(), e.session.as_bytes()] {
        pk.extend_from_slice(&(c.len() as u16).to_be_bytes());
        pk.extend_from_slice(c);
        pk.push(0);
    }
    let mut ck = Vec::new();
    for c in [
        &e.src.as_bytes()[..],
        e.edge_type.as_bytes(),
        &e.dst.as_bytes()[..],
    ] {
        ck.extend_from_slice(&(c.len() as u16).to_be_bytes());
        ck.extend_from_slice(c);
    }
    (DecoratedKey::new(PartitionKey::new(pk)), ck)
}

/// A delete as the pre-fix build performed it, after its last reconcile pass:
/// the edge row is tombstoned, and the pre-fix observer derived LIVE OUT/IN
/// entries from that tombstone (and `DELETE r` wrote no adjacency tombstone).
/// The new observer runs on this write and tombstones the entries; the live
/// entries are then written one microsecond later, so the end state is the
/// pre-fix one: a deleted edge whose entries are live.
fn delete_as_prefix_build(node: &Node, e: &Edge) {
    let ts = now_micros();
    let (key, clustering) = typed_edge_key(e);
    node.storage
        .write(
            &TableId::new(KEYSPACE, "typed_edges"),
            &key,
            Row {
                clustering,
                cells: vec![],
                deletion: DeletionTime::new(ts, (ts / 1_000_000) as u32),
                primary_key_liveness: LivenessInfo::NONE,
            },
            ts,
        )
        .unwrap();
    let adj = TableId::new(ADJACENCY_KEYSPACE, "adjacency");
    for (vertex, direction, neighbor) in [(e.src, 0u8, e.dst), (e.dst, 1u8, e.src)] {
        node.storage
            .write(
                &adj,
                &DecoratedKey::new(PartitionKey::new(vertex.as_bytes().to_vec())),
                Row {
                    clustering: adjacency_clustering(direction, "TYPED_EDGE", neighbor.as_bytes()),
                    cells: vec![(
                        0,
                        CellValue::live(b"agent_memory.typed_edges".to_vec(), ts + 1),
                    )],
                    deletion: DeletionTime::LIVE,
                    primary_key_liveness: LivenessInfo::with_timestamp(ts + 1),
                },
                ts + 1,
            )
            .unwrap();
    }
}

/// main's `adjacency::reconcile::reconcile_once` before 330a0c29, copied
/// verbatim but for logging, so the damage it did is reproduced by the code
/// that did it rather than described.
mod old_build {
    use super::*;
    use ferrosa_graph::executor::expand::extract_neighbor_id;
    use futures::StreamExt;

    const DIRECTION_OUT: u8 = 0;
    const DIRECTION_IN: u8 = 1;

    #[derive(Debug, Default)]
    pub struct Metrics {
        pub entries_checked: usize,
        pub entries_repaired: usize,
        pub orphans_removed: usize,
    }

    fn make_adjacency_row(
        direction: u8,
        edge_label: &str,
        neighbor_id: &[u8],
        edge_table: &str,
        timestamp: i64,
    ) -> Row {
        Row {
            clustering: adjacency_clustering(direction, edge_label, neighbor_id),
            cells: vec![(
                0,
                CellValue::live(edge_table.as_bytes().to_vec(), timestamp),
            )],
            deletion: DeletionTime::LIVE,
            primary_key_liveness: LivenessInfo::with_timestamp(timestamp),
        }
    }

    async fn write(wp: &WritePath, tid: &TableId, key: &DecoratedKey, row: Row, ts: i64) -> bool {
        wp.write(
            tid,
            key,
            row,
            ts,
            ferrosa_cluster::consistency::ConsistencyLevel::One,
            &ferrosa_cluster::ring::strategy::ReplicationStrategy::Simple {
                replication_factor: 1,
            },
        )
        .await
        .is_ok()
    }

    async fn adjacency_entry_exists(
        wp: &WritePath,
        adj: &TableId,
        vertex_key: &DecoratedKey,
        direction: u8,
        edge_label: &str,
        neighbor_id: &[u8],
    ) -> bool {
        let partition = match wp.read(adj, vertex_key).await {
            Ok(Some(p)) => p,
            _ => return false,
        };
        let expected = adjacency_clustering(direction, edge_label, neighbor_id);
        partition.rows.iter().any(|row| row.clustering == expected)
    }

    fn extract_edge_label(clustering: &[u8]) -> Option<String> {
        if clustering.len() < 7 {
            return None;
        }
        let dir_len = u16::from_be_bytes([clustering[0], clustering[1]]) as usize;
        let label_len_pos = 2 + dir_len;
        if label_len_pos + 2 > clustering.len() {
            return None;
        }
        let label_len =
            u16::from_be_bytes([clustering[label_len_pos], clustering[label_len_pos + 1]]) as usize;
        let label_start = label_len_pos + 2;
        if label_start + label_len > clustering.len() {
            return None;
        }
        std::str::from_utf8(&clustering[label_start..label_start + label_len])
            .ok()
            .map(|s| s.to_string())
    }

    pub async fn reconcile_once(schema: &Schema, wp: &WritePath, keyspace: &str) -> Metrics {
        let snap = schema.snapshot();
        let mut metrics = Metrics::default();
        let edge_tables: Vec<_> = snap
            .tables
            .iter()
            .filter(|((ks, _), meta)| {
                ks == keyspace && meta.extensions.get("graph.type") == Some(&"edge".to_string())
            })
            .map(|((ks, name), meta)| (TableId::new(ks, name), meta.clone()))
            .collect();
        let adj_ks = format!("system_graph_{keyspace}");
        let adj = TableId::new(&adj_ks, "adjacency");

        // Phase 1: the edge's raw partition key is its source, its raw
        // clustering its target.
        for (edge_tid, edge_meta) in &edge_tables {
            if !edge_meta.extensions.contains_key("graph.source")
                || !edge_meta.extensions.contains_key("graph.target")
            {
                continue;
            }
            let edge_label = edge_meta
                .extensions
                .get("graph.label")
                .cloned()
                .unwrap_or_else(|| edge_tid.table.clone());
            let fqn = format!("{}.{}", edge_tid.keyspace, edge_tid.table);
            let mut partitions = wp.range_read_stream_all(edge_tid, 0).await.unwrap();
            while let Some(partition) = partitions.next().await {
                let partition = partition.unwrap();
                let source_id = partition.key.key.as_bytes().to_vec();
                let source_key = partition.key.clone();
                for row in &partition.rows {
                    let target_id = row.clustering.clone();
                    metrics.entries_checked += 1;
                    if !adjacency_entry_exists(
                        wp,
                        &adj,
                        &source_key,
                        DIRECTION_OUT,
                        &edge_label,
                        &target_id,
                    )
                    .await
                    {
                        let ts = now_micros();
                        let row =
                            make_adjacency_row(DIRECTION_OUT, &edge_label, &target_id, &fqn, ts);
                        if write(wp, &adj, &source_key, row, ts).await {
                            metrics.entries_repaired += 1;
                        }
                    }
                    let target_key = DecoratedKey::new(PartitionKey::new(target_id.clone()));
                    if !adjacency_entry_exists(
                        wp,
                        &adj,
                        &target_key,
                        DIRECTION_IN,
                        &edge_label,
                        &source_id,
                    )
                    .await
                    {
                        let ts = now_micros();
                        let row =
                            make_adjacency_row(DIRECTION_IN, &edge_label, &source_id, &fqn, ts);
                        if write(wp, &adj, &target_key, row, ts).await {
                            metrics.entries_repaired += 1;
                        }
                    }
                }
            }
        }

        // Phase 2: an entry whose (vertex, neighbour) is not an edge row's
        // (partition key, clustering) is an orphan, and is tombstoned.
        let mut adj_partitions = wp.range_read_stream_all(&adj, 0).await.unwrap();
        while let Some(partition) = adj_partitions.next().await {
            let partition = partition.unwrap();
            let vertex_id = partition.key.key.as_bytes().to_vec();
            for row in &partition.rows {
                if row.clustering.len() < 3 {
                    continue;
                }
                let direction = row.clustering[2];
                let Some(neighbor_id) = extract_neighbor_id(&row.clustering, None) else {
                    continue;
                };
                if extract_edge_label(&row.clustering).is_none() {
                    continue;
                }
                let edge_table_fqn = match row.cells.first() {
                    Some((_, cell)) => match &cell.value {
                        Some(bytes) => match std::str::from_utf8(bytes) {
                            Ok(s) => s.to_string(),
                            Err(_) => continue,
                        },
                        None => continue,
                    },
                    None => continue,
                };
                let Some((edge_ks, edge_tbl)) = edge_table_fqn.split_once('.') else {
                    continue;
                };
                let edge_tid = TableId::new(edge_ks, edge_tbl);
                let (source_id, target_id) = if direction == DIRECTION_OUT {
                    (vertex_id.clone(), neighbor_id.clone())
                } else {
                    (neighbor_id.clone(), vertex_id.clone())
                };
                let source_key = DecoratedKey::new(PartitionKey::new(source_id));
                let edge_exists = match wp.read(&edge_tid, &source_key).await {
                    Ok(Some(p)) => p.rows.iter().any(|r| r.clustering == target_id),
                    _ => false,
                };
                if !edge_exists {
                    let ts = now_micros();
                    let tombstone = Row {
                        clustering: row.clustering.clone(),
                        cells: vec![],
                        deletion: DeletionTime::new(ts, (ts / 1_000_000) as u32),
                        primary_key_liveness: LivenessInfo::NONE,
                    };
                    if write(wp, &adj, &partition.key, tombstone, ts).await {
                        metrics.orphans_removed += 1;
                    }
                }
            }
        }
        metrics
    }
}

/// How many OUT/IN adjacency entries of `edges` are live in `node`'s storage.
fn live_entries_of(node: &Node, edges: &BTreeSet<Edge>) -> usize {
    live_entries_in(&node.storage, edges)
}

/// How many OUT/IN adjacency entries of `edges` are live in `storage`.
fn live_entries_in(storage: &StorageEngine, edges: &BTreeSet<Edge>) -> usize {
    let adj = TableId::new(ADJACENCY_KEYSPACE, "adjacency");
    let mut live = 0;
    for e in edges {
        for (vertex, direction, neighbor) in [(e.src, 0u8, e.dst), (e.dst, 1u8, e.src)] {
            let key = DecoratedKey::new(PartitionKey::new(vertex.as_bytes().to_vec()));
            let clustering = adjacency_clustering(direction, "TYPED_EDGE", neighbor.as_bytes());
            let partition = storage.read(&adj, &key).unwrap();
            live += partition.map_or(0, |p| {
                p.rows
                    .iter()
                    .filter(|r| {
                        r.clustering == clustering
                            && !ferrosa_graph::adjacency::schema::row_is_deleted(r)
                    })
                    .count()
            });
        }
    }
    live
}

/// Build the state a memory-cluster node is in when the fixed build first
/// starts on it, and return the live edge set it must answer from.
///
/// 1. Edges written with ferrosa-memory's MERGE; some deleted with its DELETE.
/// 2. The pre-fix reconcile runs: every typed_edges adjacency entry is
///    tombstoned (its orphan check reads the wrong key).
/// 3. More edges written, and some deleted as the pre-fix build deleted them
///    (live entries left behind), after that last pass.
async fn damaged_node(
    tenants: usize,
    sessions: usize,
    per_session: usize,
) -> (Node, Vec<Edge>, BTreeSet<Edge>) {
    damage(Node::new(), tenants, sessions, per_session).await
}

async fn damage(
    node: Node,
    tenants: usize,
    sessions: usize,
    per_session: usize,
) -> (Node, Vec<Edge>, BTreeSet<Edge>) {
    let all = generate_edges(tenants, sessions, per_session);
    let (before, after): (Vec<_>, Vec<_>) = all.iter().enumerate().partition(|(i, _)| i % 10 != 9);
    let mut live: BTreeSet<Edge> = BTreeSet::new();

    let old = node.start_engine();
    for (_, e) in &before {
        run_ok(&old, &merge_query(e)).await;
        live.insert(**e);
    }
    // Deleted before the last pre-fix pass, including the `depends_on` half
    // of some two-type pairs whose `related_to` edge survives.
    for (i, e) in &before {
        if i % 7 == 3 || (e.edge_type == "depends_on" && i % 3 == 0) {
            run_ok(&old, &delete_query(e)).await;
            live.remove(*e);
        }
    }

    let reference = Reference::new(&all, &live);
    assert_clean(
        "counts before the pre-fix reconcile",
        &check_counts(&old, &reference).await,
    );

    let damage = old_build::reconcile_once(&node.schema, &node.write_path(), KEYSPACE).await;
    let untouched = live_entries_of(&node, &live);
    assert_eq!(
        untouched, 0,
        "the pre-fix reconcile must tombstone every live typed edge's OUT and IN entry; \
         {untouched} survived ({damage:?})"
    );

    for (_, e) in &after {
        run_ok(&old, &merge_query(e)).await;
        live.insert(**e);
    }
    for (i, e) in &after {
        if i % 3 == 0 {
            delete_as_prefix_build(&node, e);
            live.remove(*e);
        }
    }
    drop(old);
    (node, all, live)
}

// ── Tests ────────────────────────────────────────────────────────────────

/// The first traversals after the deploy — issued together, as a restarted
/// node receives them — see every live edge in its direction and no deleted
/// edge, and `count(r)` matches the reference.
#[tokio::test]
async fn first_traversals_after_deploy_see_every_live_edge_and_no_deleted_one() {
    // ~500 adjacency entries: past RECONCILE_YIELD_EVERY_CHECKED_ENTRIES
    // (256), so the heal yields mid-pass and the queries issued with it run
    // while it is half done — unless they wait for it.
    let (node, all, live) = damaged_node(2, 1, 40).await;
    let reference = Reference::new(&all, &live);

    let fresh = node.start_engine();
    assert_clean(
        "first traversals after the deploy",
        &check_all(&fresh, &reference).await,
    );
    assert_clean(
        "counts after the deploy",
        &check_counts(&fresh, &reference).await,
    );
    // And they stay right once the heal is done.
    assert_clean(
        "traversals after the heal",
        &check_all(&fresh, &reference).await,
    );
}

/// A heal that cannot read the edge tables does not let the query answer from
/// the damaged index: it fails retryably, and a later query heals once the
/// reads work.
#[tokio::test]
async fn incomplete_heal_fails_retryably_and_the_next_query_heals() {
    let (node, all, live) = damaged_node(1, 1, 12).await;
    let reference = Reference::new(&all, &live);

    let write_path = Arc::new(arc_swap::ArcSwap::from_pointee(WritePath::Unavailable));
    let fresh = GraphEngine::new(
        Arc::clone(&node.schema),
        Arc::clone(&node.storage),
        Arc::clone(&write_path),
        GraphEngineConfig::default(),
        std::time::Duration::from_secs(300),
    );
    let some = *live.iter().next().expect("a live edge");
    let refused = run(&fresh, &related_query(some.tenant, some.session, some.src)).await;
    assert!(
        matches!(
            refused,
            Err(ferrosa_graph::error::GraphError::Unavailable(_))
        ),
        "a heal with failed reads must refuse retryably, got {refused:?}"
    );

    write_path.store(Arc::new(node.write_path()));
    assert_clean(
        "traversals after the retried heal",
        &check_all(&fresh, &reference).await,
    );
}

/// A client that gives up on the first traversal does not cancel the heal.
///
/// Live on 2026-10-06/07 (ferrosa main 220aff3c and c603a488, the memory
/// cluster): the heal ran inside the first query's future. ferrosa-memory's
/// graph client gives up after 10 s, the heal on that cluster takes longer, and
/// dropping the request dropped the heal half done. The next query started it
/// again from the beginning, so it never finished and every edge query timed
/// out — including a MATCH whose anchor does not exist.
#[tokio::test]
async fn a_cancelled_first_traversal_does_not_cancel_the_heal() {
    let (node, all, live) = damaged_node(2, 1, 40).await;
    let reference = Reference::new(&all, &live);
    let fresh = node.start_engine();
    let some = *live.iter().next().expect("a live edge");

    // Poll the first traversal once, then drop it: a client timeout. ~500
    // entries is past the heal's yield interval, so the poll leaves it half
    // done — asserted, or this test would prove nothing.
    let query = related_query(some.tenant, some.session, some.src);
    {
        let first = run(&fresh, &query);
        futures::pin_mut!(first);
        assert!(
            futures::poll!(first.as_mut()).is_pending(),
            "the first traversal must still be healing after one poll"
        );
    }

    // No query drives it now. The heal must finish on its own.
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
    while !fresh.adjacency_heal_completed_for_test(KEYSPACE) {
        assert!(
            std::time::Instant::now() < deadline,
            "the heal was cancelled with the query that started it: \
             30 s later it has not completed"
        );
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
    assert_clean(
        "traversals after a heal whose first query was cancelled",
        &check_all(&fresh, &reference).await,
    );
}

/// The heal is not skipped when the first adjacency query is a FOREACH.
#[tokio::test]
async fn first_query_through_foreach_still_heals_before_traversals() {
    let (node, all, live) = damaged_node(1, 1, 40).await;
    let reference = Reference::new(&all, &live);

    let fresh = node.start_engine();
    // Re-MERGE an edge that is already live: the answer set does not change.
    let some = *live.iter().next().expect("a live edge");
    let foreach = format!(
        "FOREACH (x IN [1] | MERGE (a:Entity {{tenant_id: {t}, session_id: {s}, entity_id: {src}}})\
         -[r:TYPED_EDGE {{tenant_id: {t}, session_id: {s}, edge_type: '{ty}'}}]->\
         (b:Entity {{tenant_id: {t}, session_id: {s}, entity_id: {dst}}}))",
        t = q(some.tenant),
        s = q(some.session),
        src = q(some.src),
        dst = q(some.dst),
        ty = some.edge_type,
    );
    run_ok(&fresh, &foreach).await;
    assert_clean(
        "traversals after a FOREACH first query",
        &check_all(&fresh, &reference).await,
    );
}

/// Deleting one edge of a two-type pair keeps the other traversable: both map
/// to the same adjacency entry.
#[tokio::test]
async fn deleting_one_edge_type_keeps_its_sibling_traversable() {
    let node = Node::new();
    let engine = node.start_engine();
    let (t, s) = (tenant_id(0), session_id(0, 0));
    let (a, b) = (entity(0, 0, 0), entity(0, 0, 1));
    let related = Edge {
        tenant: t,
        session: s,
        src: a,
        edge_type: "related_to",
        dst: b,
    };
    let depends = Edge {
        edge_type: "depends_on",
        ..related
    };
    run_ok(&engine, &merge_query(&related)).await;
    run_ok(&engine, &merge_query(&depends)).await;
    run_ok(&engine, &delete_query(&depends)).await;

    let rows = run_ok(&engine, &related_query(t, s, a)).await;
    let got: BTreeSet<String> = rows.iter().map(|r| text(&r[0])).collect();
    assert_eq!(
        got,
        BTreeSet::from([b.to_string()]),
        "the surviving related_to edge"
    );
    let rows = run_ok(&engine, &edges_to_query(t, s, b)).await;
    let got: BTreeSet<(String, String)> = rows.iter().map(|r| (text(&r[0]), text(&r[1]))).collect();
    assert_eq!(
        got,
        BTreeSet::from([(a.to_string(), "related_to".to_string())])
    );
}

/// Cluster mode, replication factor 2: the heal on one node repairs EVERY
/// replica, not only the token's primary.
///
/// The live memory cluster is RF 3 on 3 nodes: every node is a replica and
/// answers `CL ONE` traversals from its own copy, and each node's pre-fix
/// background reconcile tombstoned its own copy. The remote replica here was
/// damaged later than the local one (its tombstones are newer), so a repair
/// timestamped past only the LOCAL tombstone would lose to it.
mod cluster {
    use super::*;
    use ferrosa_cluster::consistency::ConsistencyLevel;
    use ferrosa_cluster::coordinator::{ClusterCoordinator, MutationForwardHandler};
    use ferrosa_cluster::raft::handlers::ReadRequestHandler;
    use ferrosa_cluster::raft::{NodeInfo, NodeState};
    use ferrosa_cluster::ring::TokenRing;
    use ferrosa_net::codec::MsgType;
    use ferrosa_net::config::NetConfig;
    use ferrosa_net::peer::{PeerEventListener, PeerManager};
    use ferrosa_net::rpc::handler::PeerId;
    use ferrosa_net::rpc::server::RpcServer;
    use ferrosa_net::rpc::HandlerRegistry;

    #[derive(Debug)]
    struct NoopPeers;

    impl PeerEventListener for NoopPeers {
        fn on_peer_connected(&self, _peer: PeerId) {}
        fn on_peer_disconnected(&self, _peer: PeerId) {}
        fn on_peer_suspected(&self, _peer: PeerId) {}
        fn on_peer_recovered(&self, _peer_id: Uuid) {}
        fn on_peer_failed(&self, _peer_id: Uuid) {}
    }

    fn node_info(addr: &str, host_id: Uuid) -> NodeInfo {
        NodeInfo {
            host_id,
            addr: addr.to_string(),
            data_center: "dc1".to_string(),
            rack: "rack1".to_string(),
            state: NodeState::Normal,
            cql_broadcast: None,
        }
    }

    /// A production handler behind a fixed network delay.
    struct Delayed<H> {
        inner: H,
        delay: std::time::Duration,
    }

    #[async_trait::async_trait]
    impl<H: ferrosa_net::rpc::handler::RpcHandler> ferrosa_net::rpc::handler::RpcHandler
        for Delayed<H>
    {
        async fn handle(
            &self,
            from: PeerId,
            msg: ferrosa_net::message::Message,
        ) -> Option<ferrosa_net::message::Message> {
            tokio::time::sleep(self.delay).await;
            self.inner.handle(from, msg).await
        }
    }

    /// A remote replica: its own storage, served by the production write and
    /// read handlers.
    async fn start_replica(storage: Arc<StorageEngine>) -> (Arc<RpcServer>, String, Uuid) {
        start_replica_with_latency(storage, std::time::Duration::ZERO).await
    }

    /// [`start_replica`], with every request answered after `delay`.
    async fn start_replica_with_latency(
        storage: Arc<StorageEngine>,
        delay: std::time::Duration,
    ) -> (Arc<RpcServer>, String, Uuid) {
        let registry = Arc::new(HandlerRegistry::new());
        registry.register(
            MsgType::MutationForward,
            Arc::new(Delayed {
                inner: MutationForwardHandler::new(Arc::clone(&storage)),
                delay,
            }),
        );
        // As `controller::cluster` registers it in production.
        let read_handler = Arc::new(Delayed {
            inner: ReadRequestHandler::new(Arc::clone(&storage)),
            delay,
        });
        registry.register(MsgType::ReadRequest, read_handler.clone());
        registry.register(MsgType::PartitionSuffixReadRequest, read_handler);
        let host_id = Uuid::new_v4();
        let config = NetConfig {
            bind_addr: "127.0.0.1:0".parse().unwrap(),
            ..NetConfig::default()
        };
        let server = Arc::new(RpcServer::new(config, host_id, registry));
        let addr = server.start_and_get_addr().await.unwrap();
        (server, addr.to_string(), host_id)
    }

    /// Tombstone `edges`' OUT and IN entries in `storage`, as a node's own
    /// pre-fix background pass did to its local copy.
    fn tombstone_entries(storage: &StorageEngine, edges: &BTreeSet<Edge>, ts: i64) {
        let adj = TableId::new(ADJACENCY_KEYSPACE, "adjacency");
        for e in edges {
            for (vertex, direction, neighbor) in [(e.src, 0u8, e.dst), (e.dst, 1u8, e.src)] {
                storage
                    .write(
                        &adj,
                        &DecoratedKey::new(PartitionKey::new(vertex.as_bytes().to_vec())),
                        Row {
                            clustering: adjacency_clustering(
                                direction,
                                "TYPED_EDGE",
                                neighbor.as_bytes(),
                            ),
                            cells: vec![],
                            deletion: DeletionTime::new(ts, (ts / 1_000_000) as u32),
                            primary_key_liveness: LivenessInfo::NONE,
                        },
                        ts,
                    )
                    .unwrap();
            }
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn heal_repairs_every_replica_past_its_newest_tombstone() {
        let (node, all, live) = damage(Node::with_replication_factor(2), 1, 1, 24).await;
        let reference = Reference::new(&all, &live);

        let remote_dir = TempDir::new().unwrap();
        let remote = Arc::new(StorageEngine::new(storage_config(&remote_dir), None).unwrap());
        register_storage_tables(&remote);
        remote
            .register_table(
                ferrosa_graph::adjacency::adjacency_table_metadata(KEYSPACE).to_storage_schema(),
            )
            .unwrap();
        tombstone_entries(&remote, &live, now_micros());
        let (server, remote_addr, remote_host) = start_replica(Arc::clone(&remote)).await;

        let local_host = Uuid::new_v4();
        let mut ring = TokenRing::new();
        ring.add_node(1, node_info("127.0.0.1:7000", local_host));
        ring.add_node(2, node_info(&remote_addr, remote_host));
        ring.assign_tokens(1, &[0]);
        ring.assign_tokens(2, &[i64::MIN / 2]);
        let coordinator = Arc::new(ClusterCoordinator::new(
            Arc::new(arc_swap::ArcSwap::from_pointee(ring)),
            Arc::new(PeerManager::new(
                Arc::new(NetConfig::default()),
                local_host,
                Arc::new(NoopPeers),
            )),
            1,
            Arc::clone(&node.storage),
            2,
            ConsistencyLevel::One,
        ));
        let fresh = GraphEngine::new(
            Arc::clone(&node.schema),
            Arc::clone(&node.storage),
            Arc::new(arc_swap::ArcSwap::from_pointee(WritePath::cluster(
                coordinator,
            ))),
            GraphEngineConfig::default(),
            std::time::Duration::from_secs(300),
        );

        // The heal, through the cluster write path. (The rest of a query's
        // pipeline needs more of the RPC surface than this two-node harness
        // serves; the heal is what is under test.)
        assert_eq!(
            live_entries_in(&remote, &live),
            0,
            "the remote replica starts damaged"
        );
        fresh
            .ensure_adjacency_ready_for_test(KEYSPACE)
            .await
            .expect("the heal completes against both replicas");
        // Each node answers CL ONE traversals from its own replica: the local
        // one, read directly.
        assert_clean(
            "traversals answered from the local replica",
            &check_all(&node.start_engine(), &reference).await,
        );
        let want = 2 * live.len();
        assert_eq!(
            live_entries_of(&node, &live),
            want,
            "every live edge's entries are live on the local replica"
        );
        assert_eq!(
            live_entries_in(&remote, &live),
            want,
            "every live edge's entries are live on the remote replica"
        );
        server.shutdown(std::time::Duration::from_millis(50)).await;
    }

    /// The heal does not pay one round trip per entry in sequence.
    ///
    /// Live on 2026-10-07 the memory cluster's heal was still running 30
    /// minutes after node1 started: every adjacency entry is read and then
    /// rewritten at ALL, one entry at a time, so the heal costs two network
    /// round trips per entry and every edge query waits that long. Here each
    /// replica request takes 20 ms.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn the_heal_keeps_entry_repairs_in_flight_together() {
        let delay = std::time::Duration::from_millis(20);
        let (node, all, live) = damage(Node::with_replication_factor(2), 1, 1, 24).await;
        let reference = Reference::new(&all, &live);
        let remote_dir = TempDir::new().unwrap();
        let remote = Arc::new(StorageEngine::new(storage_config(&remote_dir), None).unwrap());
        register_storage_tables(&remote);
        remote
            .register_table(
                ferrosa_graph::adjacency::adjacency_table_metadata(KEYSPACE).to_storage_schema(),
            )
            .unwrap();
        tombstone_entries(&remote, &live, now_micros());
        let (server, remote_addr, remote_host) =
            start_replica_with_latency(Arc::clone(&remote), delay).await;

        let local_host = Uuid::new_v4();
        let mut ring = TokenRing::new();
        ring.add_node(1, node_info("127.0.0.1:7000", local_host));
        ring.add_node(2, node_info(&remote_addr, remote_host));
        ring.assign_tokens(1, &[0]);
        ring.assign_tokens(2, &[i64::MIN / 2]);
        let coordinator = Arc::new(ClusterCoordinator::new(
            Arc::new(arc_swap::ArcSwap::from_pointee(ring)),
            Arc::new(PeerManager::new(
                Arc::new(NetConfig::default()),
                local_host,
                Arc::new(NoopPeers),
            )),
            1,
            Arc::clone(&node.storage),
            2,
            ConsistencyLevel::One,
        ));
        let fresh = GraphEngine::new(
            Arc::clone(&node.schema),
            Arc::clone(&node.storage),
            Arc::new(arc_swap::ArcSwap::from_pointee(WritePath::cluster(
                coordinator,
            ))),
            GraphEngineConfig::default(),
            std::time::Duration::from_secs(300),
        );

        // Every expected entry (two per live edge, plus the deleted edges'
        // entries) is one read and one write to the remote replica.
        let entries = 2 * all.len();
        let sequential = delay * 2 * entries as u32;
        let started = std::time::Instant::now();
        fresh
            .ensure_adjacency_ready_for_test(KEYSPACE)
            .await
            .expect("the heal completes against both replicas");
        let took = started.elapsed();
        assert!(
            took * 4 < sequential,
            "the heal of {entries} entries took {took:?}; one entry at a time costs {sequential:?}"
        );
        // And it is still a complete heal.
        assert_clean(
            "traversals after a concurrent heal",
            &check_all(&node.start_engine(), &reference).await,
        );
        assert_eq!(live_entries_in(&remote, &live), 2 * live.len());
        server.shutdown(std::time::Duration::from_millis(50)).await;
    }
}

/// `count(r)` (ferrosa-memory's `stats`) does not count a deleted edge whose
/// partition still holds live ones: typed_edges keeps a whole session's edges
/// in one partition, so a row tombstone, not a dead partition, is the norm.
#[tokio::test]
async fn edge_count_skips_a_deleted_edge_in_a_live_partition() {
    let node = Node::new();
    let engine = node.start_engine();
    let (t, s) = (tenant_id(0), session_id(0, 0));
    let first = Edge {
        tenant: t,
        session: s,
        src: entity(0, 0, 0),
        edge_type: "related_to",
        dst: entity(0, 0, 1),
    };
    for j in 1..6 {
        run_ok(
            &engine,
            &merge_query(&Edge {
                dst: entity(0, 0, j),
                ..first
            }),
        )
        .await;
    }
    run_ok(&engine, &delete_query(&first)).await;

    let rows = run_ok(&engine, &count_query(t)).await;
    assert_eq!(rows[0][0].as_u64(), Some(4), "five edges, one deleted");
}

/// The deploy heal at the live memory cluster's size. Run it with
/// `cargo test --release -p ferrosa-graph --features slow-tests --test
/// adjacency_deploy_heal -- ::slow:: --nocapture` for timings worth quoting.
#[cfg(feature = "slow-tests")]
mod slow {
    use super::*;

    /// The memory cluster on 2026-09-29: 102,853 entities; typed_edges ~21k.
    const ENTITIES: usize = 102_853;
    const TYPED_EDGES: usize = 21_000;
    const TENANTS: usize = 3;
    const SESSIONS_PER_TENANT: usize = 30;
    const EDGE_TYPES: [&str; 3] = ["related_to", "depends_on", "part_of"];

    fn write_entity(node: &Node, t: usize, s: usize, i: usize) {
        let probe = Edge {
            tenant: tenant_id(t),
            session: session_id(t, s),
            src: entity(t, s, i),
            edge_type: "",
            dst: entity(t, s, i),
        };
        let (key, _) = typed_edge_key(&probe);
        let row = Row {
            clustering: entity(t, s, i).as_bytes().to_vec(),
            cells: vec![(0, CellValue::live(format!("entity-{i}").into_bytes(), 1))],
            deletion: DeletionTime::LIVE,
            primary_key_liveness: LivenessInfo::with_timestamp(1),
        };
        node.storage
            .write(&TableId::new(KEYSPACE, "entity_store"), &key, row, 1)
            .unwrap();
    }

    /// An edge row written as a typed_edges INSERT lands; the adjacency
    /// observer (registered by the engine) derives its entries.
    fn write_edge(node: &Node, e: &Edge, ts: i64) {
        let (key, clustering) = typed_edge_key(e);
        let row = Row {
            clustering,
            cells: vec![],
            deletion: DeletionTime::LIVE,
            primary_key_liveness: LivenessInfo::with_timestamp(ts),
        };
        node.storage
            .write(&TableId::new(KEYSPACE, "typed_edges"), &key, row, ts)
            .unwrap();
    }

    fn production_shaped_edges() -> Vec<Edge> {
        let sessions = TENANTS * SESSIONS_PER_TENANT;
        let per_session = ENTITIES / sessions;
        let mut edges = BTreeSet::new();
        let mut k = 0usize;
        while edges.len() < TYPED_EDGES {
            let session = k % sessions;
            let (t, s) = (session / SESSIONS_PER_TENANT, session % SESSIONS_PER_TENANT);
            let i = (k / sessions) % per_session;
            let j = (i * 7 + 3 + k / (sessions * per_session)) % per_session;
            edges.insert(Edge {
                tenant: tenant_id(t),
                session: session_id(t, s),
                src: entity(t, s, i),
                edge_type: EDGE_TYPES[k % EDGE_TYPES.len()],
                dst: entity(t, s, j),
            });
            k += 1;
        }
        edges.into_iter().collect()
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn deploy_heal_at_memory_cluster_scale() {
        let node = Node::new();
        let sessions = TENANTS * SESSIONS_PER_TENANT;
        let per_session = ENTITIES / sessions;
        let mut entities = 0;
        for session in 0..sessions {
            for i in 0..per_session {
                write_entity(
                    &node,
                    session / SESSIONS_PER_TENANT,
                    session % SESSIONS_PER_TENANT,
                    i,
                );
                entities += 1;
            }
        }

        // Register the observer the way the old build's first query did.
        let old = node.start_engine();
        let all = production_shaped_edges();
        run_ok(
            &old,
            &related_query(all[0].tenant, all[0].session, all[0].src),
        )
        .await;
        let mut live: BTreeSet<Edge> = BTreeSet::new();
        let seed_ts = now_micros();
        for e in &all {
            write_edge(&node, e, seed_ts);
            live.insert(*e);
        }
        for (i, e) in all.iter().enumerate() {
            if i % 40 == 7 {
                run_ok(&old, &delete_query(e)).await;
                live.remove(e);
            }
        }
        eprintln!("SCALE seeded {entities} entities, {} edges", all.len());
        let started = std::time::Instant::now();
        let damage = old_build::reconcile_once(&node.schema, &node.write_path(), KEYSPACE).await;
        let damage_ms = started.elapsed().as_millis();
        assert_eq!(live_entries_of(&node, &live), 0, "{damage:?}");
        for (i, e) in all.iter().enumerate() {
            if i % 50 == 11 && live.contains(e) {
                delete_as_prefix_build(&node, e);
                live.remove(e);
            }
        }
        drop(old);

        // First start of the fixed build: the first query carries the heal.
        let fresh = node.start_engine();
        let some = *live.iter().next().unwrap();
        let started = std::time::Instant::now();
        let first = run_ok(&fresh, &related_query(some.tenant, some.session, some.src)).await;
        let first_query_ms = started.elapsed().as_millis();
        assert!(
            !first.is_empty(),
            "the first query answers from the healed index"
        );

        // Steady state: a second pass finds nothing to do.
        let started = std::time::Instant::now();
        let second = ferrosa_graph::adjacency::reconcile::reconcile_once(
            &node.schema,
            &node.write_path(),
            KEYSPACE,
        )
        .await;
        let second_ms = started.elapsed().as_millis();
        assert!(second.is_complete(), "{second:?}");
        assert_eq!(
            (second.entries_repaired, second.orphans_removed),
            (0, 0),
            "the heal left nothing for the next pass: {second:?}"
        );

        eprintln!(
            "SCALE entities={entities} typed_edges={} live={} \
             prefix_reconcile_ms={damage_ms} prefix_damage={damage:?} \
             first_query_with_heal_ms={first_query_ms} \
             second_pass_ms={second_ms} second_pass={second:?}",
            all.len(),
            live.len(),
        );

        // Answers: the OUT hops ferrosa-memory issues, for the sources of a
        // sample of live edges, against the whole live set.
        let full = Reference::new(&all, &live);
        let sources: BTreeSet<(Uuid, Uuid, Uuid)> = live
            .iter()
            .step_by(live.len() / 60)
            .map(|e| (e.tenant, e.session, e.src))
            .collect();
        let started = std::time::Instant::now();
        let mut mismatches = Vec::new();
        for &(t, s, v) in &sources {
            let want = full.out.get(&(t, s, v)).cloned().unwrap_or_default();
            for query in [related_query(t, s, v), bare_hop_query(t, s, v)] {
                let rows = run_ok(&fresh, &query).await;
                let got: BTreeSet<String> = rows.iter().map(|r| text(&r[0])).collect();
                if got != want {
                    mismatches.push(format!("OUT {v}: got {got:?}, want {want:?}"));
                }
            }
        }
        assert_clean("sampled OUT traversals at scale", &mismatches);
        eprintln!(
            "SCALE checked OUT hops of {} sources in {} ms",
            sources.len(),
            started.elapsed().as_millis()
        );
    }
}
