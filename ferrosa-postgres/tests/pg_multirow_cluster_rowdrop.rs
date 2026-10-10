//! Multi-row INSERT / multi-key Accord commit over a REAL 3-node cluster.
//!
//! # What this file pins now
//!
//! `execute_insert` used to refuse a multi-row INSERT with SQLSTATE `0A000` — a
//! fail-loud holding position standing in for an unimplemented feature, because
//! applying the rows one at a time could leave a partial write behind while the
//! statement still announced a count. The refusal is GONE: `execute_insert` now
//! builds and validates EVERY row of the statement BEFORE it applies any of them and
//! writes them as ONE atomic batch (see `ferrosa-postgres/src/query.rs`,
//! `execute_insert` + `apply_batch_or_buffer`).
//!
//! The first test drives a real 2-row `INSERT` over the real PG wire on a real
//! 3-node cluster and asserts the VALUES of BOTH rows (the first AND the last) on
//! EVERY replica — this is the flip the work item described. It is deliberately NOT
//! a `count` assertion: the live defect reported `INSERT 0 2` while persisting one
//! row, which any count check would have passed.
//!
//! # The multi-key apply path, characterized
//!
//! Two further tests drive the real protocol on a real cluster — 3 independent
//! `AccordStateMachine` nodes, 3 independent `StorageEngine`s, one real PG wire
//! listener per node, and the real `AccordTransactionCommitter`:
//!
//! * RF=3 (every key on every node, the `SimpleStrategy`/RF=3 shape) and
//! * RF=1 one-shard-per-key (the multi-shard shape of a real ring),
//!
//! and in **both** every key of a multi-key commit lands on every replica that owns
//! it. The below-SQL multi-node apply path is therefore not the dropper in this shape.
//!
//! Peer transport is in-process (routing to each node's real `AccordHandler`), so
//! the protocol, state machines, storage engines, PG front-end and committer are
//! all the production ones; only the socket is elided.

use std::collections::{HashMap, HashSet};
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use ferrosa_common::cell::CellValue;
use ferrosa_common::key::{DecoratedKey, PartitionKey};
use ferrosa_postgres::handshake::VerifierStore;
use ferrosa_postgres::scram::ScramVerifier;
use ferrosa_postgres::{server, AccordAccess, QueryContext};
use ferrosa_schema::{
    AuthContext, AuthMethod, ClusteringOrder, ColumnKind, ColumnMetadata, DeploymentMode,
    EnvSecretsProvider, KeyspaceMetadata, PasswordHasher, PasswordPolicy, RateLimitConfig,
    ReplicationParams, Schema, SchemaConfig, TableMetadata, TableParams, TestAuditSink,
};
use ferrosa_sstable::types::{DeletionTime, LivenessInfo, Row};
use ferrosa_storage::{
    CommitLogConfig, CompactionConfig, Mutation, StorageEngine, StorageEngineConfig,
    SyncStrategyConfig, TableId,
};
use indexmap::IndexMap;
use tokio::net::TcpListener;
use tokio_postgres::config::SslMode;
use tokio_postgres::{Config, NoTls};
use uuid::Uuid;

// ── Auth ──────────────────────────────────────────────────────────────────────

struct OneRole {
    user: String,
    verifier: ScramVerifier,
}

impl VerifierStore for OneRole {
    fn verifier(&self, user: &str) -> Option<ScramVerifier> {
        (user == self.user).then(|| self.verifier.clone())
    }
    fn admit(&self, _user: &str) -> Result<(), String> {
        Ok(())
    }
    fn record_failure(&self, _user: &str) {}
    fn record_success(&self, user: &str) -> Result<AuthContext, String> {
        Ok(AuthContext {
            role: user.to_string(),
            is_superuser: true,
            must_change_password: false,
        })
    }
}

fn dev_store() -> Arc<OneRole> {
    let salt = b"ferrosa-dev-salt";
    Arc::new(OneRole {
        user: "ferrosa_user".into(),
        verifier: ScramVerifier::from_password("devpass", salt, 4096),
    })
}

// ── Schema / engine config ────────────────────────────────────────────────────

fn schema_config() -> SchemaConfig {
    SchemaConfig {
        hasher: PasswordHasher::Bcrypt { cost: 4 },
        password_policy: PasswordPolicy::permissive(),
        auth_method: AuthMethod::Password,
        rate_limit: RateLimitConfig::default(),
        audit_sink: Box::new(TestAuditSink::new()),
        secrets: Box::new(EnvSecretsProvider),
        mode: DeploymentMode::Development,
    }
}

fn superuser() -> AuthContext {
    AuthContext {
        role: "cassandra".to_string(),
        is_superuser: true,
        must_change_password: false,
    }
}

fn engine_config(dir: &Path) -> StorageEngineConfig {
    StorageEngineConfig {
        commit_log: CommitLogConfig {
            segment_size: 256 * 1024,
            max_segment_age: Duration::from_secs(60),
            sync_strategy: SyncStrategyConfig::Batch,
            batch: Default::default(),
            log_dir: dir.join("commitlog"),
            checkpoint_dir: dir.join("commitlog"),
            archive: None,
        },
        compaction: CompactionConfig::from_env(dir.join("compaction")),
        object_store: None,
        local_cache_max_bytes: 64 * 1024 * 1024,
        local_disk_free_reserve_bytes: 0,
        flush_threshold_bytes: 4 * 1024 * 1024,
        memtable_backpressure_bytes: u64::MAX,
        flush_max_age_secs: 60,
        data_dir: dir.to_path_buf(),
        index_backend: ferrosa_storage::index::IndexBackendConfig::Local,
        auth_enabled: false,
        auth_warn: false,
        max_pending_replay_mutations_without_schema: 1024,
        memtable_num_shards: 64,
        cache_hot_window_secs: 900,
        write_verify: false,
    }
}

fn column(name: &str, kind: ColumnKind, ty: &str) -> ColumnMetadata {
    ColumnMetadata {
        name: name.to_string(),
        kind,
        position: 0,
        column_type: ty.to_string(),
        clustering_order: ClusteringOrder::None,
        mask: None,
    }
}

/// Keyspace `public` + table `users(id int PK, name text)`, through the public
/// DDL API — the identical fixture the single-node PG tests use.
fn create_schema() -> Schema {
    let schema = Schema::new(schema_config()).expect("schema bootstraps");
    let auth = superuser();

    schema
        .create_keyspace(
            KeyspaceMetadata {
                name: "public".to_string(),
                durable_writes: true,
                replication: ReplicationParams {
                    strategy: "SimpleStrategy".to_string(),
                    options: {
                        let mut o = HashMap::new();
                        o.insert("replication_factor".to_string(), "3".to_string());
                        o
                    },
                },
            },
            &auth,
        )
        .expect("create keyspace public");

    let mut users_cols = IndexMap::new();
    users_cols.insert(
        "id".to_string(),
        column("id", ColumnKind::PartitionKey, "int"),
    );
    users_cols.insert(
        "name".to_string(),
        column("name", ColumnKind::Regular, "text"),
    );
    schema
        .create_table(
            TableMetadata {
                keyspace: "public".to_string(),
                name: "users".to_string(),
                id: Uuid::new_v4(),
                columns: users_cols,
                partition_key: vec!["id".to_string()],
                clustering_key: vec![],
                params: TableParams::default(),
                flags: HashSet::new(),
                extensions: HashMap::new(),
                is_system: false,
            },
            &auth,
        )
        .expect("create table users");

    schema
}

fn users_storage_schema() -> ferrosa_common::schema::TableSchema {
    use ferrosa_common::schema::{ColumnDefinition, TableSchema};
    TableSchema {
        keyspace: "public".to_string(),
        table: "users".to_string(),
        key_type: "org.apache.cassandra.db.marshal.Int32Type".to_string(),
        clustering_columns: vec![],
        static_columns: vec![],
        regular_columns: vec![ColumnDefinition {
            name: "name".to_string(),
            type_name: "org.apache.cassandra.db.marshal.UTF8Type".to_string(),
        }],
        extensions: Default::default(),
    }
}

fn seed_engine(dir: &Path) -> StorageEngine {
    let engine = StorageEngine::new(engine_config(dir), None).unwrap();
    engine.register_table(users_storage_schema()).unwrap();
    engine
}

async fn connect(port: u16) -> tokio_postgres::Client {
    let (client, connection) = Config::new()
        .host("127.0.0.1")
        .port(port)
        .user("ferrosa_user")
        .password("devpass")
        .dbname("ferrosa")
        .ssl_mode(SslMode::Disable)
        .connect(NoTls)
        .await
        .expect("SCRAM handshake should succeed");
    tokio::spawn(async move {
        let _ = connection.await;
    });
    client
}

// ── Real 3-node Accord cluster over an in-process transport ──────────────────

/// In-process network: every message goes through the addressed node's REAL
/// `AccordHandler` and state machine (only the socket is elided).
struct AccordTestTransport {
    handlers: HashMap<Uuid, Arc<ferrosa_cluster::accord::AccordHandler>>,
}

#[async_trait::async_trait]
impl ferrosa_cluster::accord::transport::AccordTransport for AccordTestTransport {
    async fn send(
        &self,
        host: Uuid,
        msg: ferrosa_net::message::Message,
        _lane: ferrosa_net::codec::Lane,
    ) -> ferrosa_net::error::Result<ferrosa_net::message::Message> {
        use ferrosa_net::rpc::handler::{PeerId, RpcHandler};
        let handler = self
            .handlers
            .get(&host)
            .ok_or_else(|| ferrosa_net::error::NetError::Timeout("unknown test peer".into()))?;
        let peer: PeerId = (host, "127.0.0.1:0".parse().unwrap());
        handler
            .handle(peer, msg)
            .await
            .ok_or_else(|| ferrosa_net::error::NetError::Timeout("no test-peer response".into()))
    }
}

struct Node {
    host: Uuid,
    engine: Arc<StorageEngine>,
    handler: Arc<ferrosa_cluster::accord::AccordHandler>,
    applier: Arc<ferrosa_cluster::accord::EngineStorageApplier>,
    local_state: Arc<parking_lot::Mutex<ferrosa_cluster::accord::AccordStateMachine>>,
    _dir: tempfile::TempDir,
}

/// Three real Accord nodes (each: real state machine + real storage engine +
/// real handler). Hosts put the distinguishing value in the FIRST eight bytes,
/// since Accord derives the numeric node id from there.
fn build_nodes() -> Vec<Node> {
    use ferrosa_cluster::accord::{AccordHandler, AccordStateMachine, EngineStorageApplier};
    use ferrosa_storage::accord::sync_writer::MockSyncWriter;

    let hosts = [
        Uuid::from_u128(0xA000_0000_0000_0000_0000_0000_0000_0001),
        Uuid::from_u128(0xB000_0000_0000_0000_0000_0000_0000_0002),
        Uuid::from_u128(0xC000_0000_0000_0000_0000_0000_0000_0003),
    ];
    hosts
        .into_iter()
        .map(|host| {
            let dir = tempfile::tempdir().unwrap();
            let engine = Arc::new(seed_engine(dir.path()));
            let node_id = u64::from_be_bytes(host.as_bytes()[..8].try_into().unwrap());
            let applier = Arc::new(EngineStorageApplier::new(engine.clone()));
            let local_state = Arc::new(parking_lot::Mutex::new(AccordStateMachine::with_applier(
                node_id,
                Arc::new(MockSyncWriter::new()),
                applier.clone(),
            )));
            let handler = Arc::new(AccordHandler::new(local_state.clone(), node_id));
            Node {
                host,
                engine,
                handler,
                applier,
                local_state,
                _dir: dir,
            }
        })
        .collect()
}

/// How the cluster resolves a key's replicas.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Placement {
    /// Every key → all three nodes (RF=3 ⇒ one shard).
    AllNodes,
    /// Each key → exactly ONE node, chosen by its last byte (RF=1 per key ⇒
    /// one shard PER key — the multi-shard topology of a real ring).
    OneNodePerKey,
}

fn resolve_for(placement: Placement, hosts: &[Uuid]) -> ferrosa_cluster::accord::ReplicaResolver {
    let replicas = hosts.to_vec();
    match placement {
        Placement::AllNodes => Arc::new(move |_keyspace: &str, _key: &[u8]| Some(replicas.clone())),
        Placement::OneNodePerKey => Arc::new(move |_keyspace: &str, key: &[u8]| {
            let last = key.last().copied().unwrap_or(0) as usize;
            Some(vec![replicas[last % replicas.len()]])
        }),
    }
}

/// A committer for one node plus the PG listener port it serves.
async fn start_pg_on_node(
    node: &Node,
    hosts: &[Uuid],
    placement: Placement,
    transport: Arc<dyn ferrosa_cluster::accord::transport::AccordTransport>,
) -> (
    u16,
    Arc<ferrosa_cluster::accord::AccordTransactionCommitter>,
) {
    use ferrosa_cluster::accord::AccordTransactionCommitter;
    use ferrosa_common::accord::HybridLogicalClock;
    use ferrosa_storage::accord::TransactionCommitter;

    let node_id = u64::from_be_bytes(node.host.as_bytes()[..8].try_into().unwrap());
    let clock = Arc::new(HybridLogicalClock::new(node_id, 0));
    let resolve = resolve_for(placement, hosts);

    let committer = Arc::new(
        AccordTransactionCommitter::new(node_id, clock, transport, node.applier.clone(), resolve)
            .with_local_accord_state(node.local_state.clone()),
    );
    let query_committer: Arc<dyn TransactionCommitter> = committer.clone();

    let ctx = Arc::new(QueryContext {
        engine: node.engine.clone(),
        schema: Arc::new(create_schema()),
        default_schema: "public".into(),
        mvcc: Arc::new(ferrosa_postgres::MvccManager::default()),
        accord: AccordAccess::fixed(query_committer),
        ddl: None,
        jsonb_limits: ferrosa_postgres::jsonb_wire::test_limits(),
        portals: Default::default(),
    });

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    tokio::spawn(server::serve(
        listener,
        dev_store(),
        ctx,
        server::PgTls::plaintext(),
    ));
    (port, committer)
}

struct Cluster {
    nodes: Vec<Node>,
    ports: Vec<u16>,
    committers: Vec<Arc<ferrosa_cluster::accord::AccordTransactionCommitter>>,
}

/// Bring up the 3-node cluster and one PG listener per node. Node 0 is the
/// coordinator the clients connect to.
async fn cluster(placement: Placement) -> Cluster {
    let nodes = build_nodes();
    let hosts: Vec<Uuid> = nodes.iter().map(|n| n.host).collect();
    let transport: Arc<dyn ferrosa_cluster::accord::transport::AccordTransport> =
        Arc::new(AccordTestTransport {
            handlers: nodes.iter().map(|n| (n.host, n.handler.clone())).collect(),
        });

    let mut ports = Vec::new();
    let mut committers = Vec::new();
    for node in &nodes {
        let (port, committer) = start_pg_on_node(node, &hosts, placement, transport.clone()).await;
        ports.push(port);
        committers.push(committer);
    }
    Cluster {
        nodes,
        ports,
        committers,
    }
}

// ── Raw key mutations (what a PG INSERT buffers, minus the SQL envelope) ─────

fn users_mutation(id: i32, name: &str) -> Mutation {
    let key = DecoratedKey::new(PartitionKey::new(id.to_be_bytes().to_vec()));
    let row = Row {
        clustering: vec![],
        cells: vec![(0u16, CellValue::live(name.as_bytes().to_vec(), 1))],
        deletion: DeletionTime::LIVE,
        primary_key_liveness: LivenessInfo::with_timestamp(1),
    };
    Mutation::new("public".into(), "users".into(), key, vec![row], 1)
}

fn transaction_write(mutation: &Mutation) -> ferrosa_storage::accord::TransactionWrite {
    let mut bytes = vec![0u8; mutation.serialized_size()];
    mutation.serialize_into(&mut bytes);
    ferrosa_storage::accord::TransactionWrite {
        keyspace: "public".to_string(),
        key: mutation.key.key.as_bytes().to_vec(),
        mutation: bytes,
    }
}

/// The `name` cell persisted for `id` in this engine, if any — read straight
/// from storage (independent of the PG MVCC overlay).
fn stored_name(engine: &StorageEngine, id: i32) -> Option<String> {
    let key = DecoratedKey::new(PartitionKey::new(id.to_be_bytes().to_vec()));
    let partition = engine
        .read(&TableId::new("public", "users"), &key)
        .expect("read")?;
    partition.rows.iter().find_map(|row| {
        row.cells
            .iter()
            .find(|(index, _)| *index == 0)
            .and_then(|(_, cell)| cell.value.as_ref())
            .map(|bytes| String::from_utf8_lossy(bytes).into_owned())
    })
}

// ── Tests ─────────────────────────────────────────────────────────────────────

/// The fixed behavior, pinned at the cluster boundary: a multi-row INSERT over the
/// wire is ACCEPTED, reports `INSERT 0 2`, and persists BOTH rows with their OWN
/// values on EVERY replica.
///
/// This test was the fail-loud refusal's negative control. Now it is the positive
/// control for the fix — the flip the work item described. It asserts the VALUES of
/// the FIRST and the LAST row on every node, never the count alone: a write that
/// announced `INSERT 0 2` while persisting only row 1 is exactly the live defect this
/// replaced.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn multi_row_insert_persists_every_row_on_a_real_cluster() {
    let cluster = cluster(Placement::AllNodes).await;
    let client = connect(cluster.ports[0]).await;

    let inserted = client
        .execute(
            "INSERT INTO users (id, name) VALUES (201, 'm-one'), (202, 'm-two')",
            &[],
        )
        .await
        .expect("multi-row INSERT now executes on a real cluster");
    assert_eq!(
        inserted, 2,
        "the reported count must be the count that landed"
    );

    // Accord apply is asynchronous: the coordinator returns at apply QUORUM and the
    // remaining replica(s) apply in the background. Give every replica a bounded
    // window to converge, then assert.
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    loop {
        let all = cluster.nodes.iter().all(|node| {
            stored_name(&node.engine, 201).is_some() && stored_name(&node.engine, 202).is_some()
        });
        if all || std::time::Instant::now() >= deadline {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    let mut report = Vec::new();
    for (i, node) in cluster.nodes.iter().enumerate() {
        report.push(format!(
            "node {i}: 201={:?} 202={:?}",
            stored_name(&node.engine, 201),
            stored_name(&node.engine, 202)
        ));
    }
    for (i, node) in cluster.nodes.iter().enumerate() {
        assert_eq!(
            stored_name(&node.engine, 201).as_deref(),
            Some("m-one"),
            "node {i} is missing or has the wrong value for row id=201; replicas: {report:?}"
        );
        assert_eq!(
            stored_name(&node.engine, 202).as_deref(),
            Some("m-two"),
            "node {i} is missing or has the wrong value for the LAST row id=202; \
             replicas: {report:?}"
        );
    }
}

/// RF=3 (every key on every node): a multi-key Accord commit must land EVERY key
/// on EVERY node. This is the clustered characterization the work item asked for
/// — it drives the real `commit_postgres` with N distinct partition keys, exactly
/// what a multi-row INSERT produces, and reads each key back from each node's
/// storage.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn multi_key_commit_persists_every_key_on_every_node_rf3() {
    use ferrosa_storage::accord::{CommitOutcome, TransactionCommitter};

    let cluster = cluster(Placement::AllNodes).await;
    let committer = cluster.committers[0].clone();

    let m1 = users_mutation(201, "m-one");
    let m2 = users_mutation(202, "m-two");
    let snapshot = committer
        .begin_postgres_snapshot("public")
        .await
        .expect("snapshot barrier must be granted by a live quorum");

    // ONE multi-key commit, two data keys — the shape of a 2-row INSERT.
    let outcome = committer
        .commit_postgres(
            "public",
            vec![transaction_write(&m1), transaction_write(&m2)],
            vec!["public.users".to_string()],
            snapshot,
        )
        .await
        .expect("multi-key commit must reach a decision");
    assert_eq!(outcome, CommitOutcome::Committed);

    // Accord apply is asynchronous: the coordinator returns at apply QUORUM, and
    // the remaining replica(s) apply in the background. Give every replica a
    // bounded window to converge, then assert.
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    loop {
        let all = cluster.nodes.iter().all(|node| {
            stored_name(&node.engine, 201).is_some() && stored_name(&node.engine, 202).is_some()
        });
        if all || std::time::Instant::now() >= deadline {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    let mut report = Vec::new();
    for (i, node) in cluster.nodes.iter().enumerate() {
        report.push(format!(
            "node {i}: 201={:?} 202={:?}",
            stored_name(&node.engine, 201),
            stored_name(&node.engine, 202)
        ));
    }
    for (i, node) in cluster.nodes.iter().enumerate() {
        for (id, want) in [(201, "m-one"), (202, "m-two")] {
            assert_eq!(
                stored_name(&node.engine, id).as_deref(),
                Some(want),
                "node {i} dropped key id={id} of the multi-key commit (RF=3); \
                 replicas: {report:?}"
            );
        }
    }
}

/// RF=1 one shard per key (a real ring): each key is owned by exactly one node,
/// so a multi-key commit spans several shards. Every key must still land — on its
/// owner. A missing key here is the cross-shard apply losing one shard's write
/// while the commit still reports success (the live failure mode).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn multi_key_commit_persists_each_key_on_its_owner_rf1() {
    use ferrosa_storage::accord::{CommitOutcome, TransactionCommitter};

    let cluster = cluster(Placement::OneNodePerKey).await;
    let committer = cluster.committers[0].clone();

    let m1 = users_mutation(201, "m-one");
    let m2 = users_mutation(202, "m-two");
    let snapshot = committer
        .begin_postgres_snapshot("public")
        .await
        .expect("snapshot barrier must be granted by a live quorum");

    let outcome = committer
        .commit_postgres(
            "public",
            vec![transaction_write(&m1), transaction_write(&m2)],
            vec!["public.users".to_string()],
            snapshot,
        )
        .await
        .expect("multi-shard commit must reach a decision");
    assert_eq!(outcome, CommitOutcome::Committed);

    // owner(key) = hosts[last_byte % 3]: id 201 → byte 201 → node 0; id 202 → node
    // 1. Each row must be present on its owner.
    assert_eq!(
        stored_name(&cluster.nodes[0].engine, 201).as_deref(),
        Some("m-one"),
        "shard 0 (id 201) lost its key of the multi-shard commit"
    );
    assert_eq!(
        stored_name(&cluster.nodes[1].engine, 202).as_deref(),
        Some("m-two"),
        "shard 1 (id 202) lost its key of the multi-shard commit"
    );
}
