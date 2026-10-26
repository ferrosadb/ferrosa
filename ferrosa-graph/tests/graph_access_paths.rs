//! Real-storage contracts for graph secondary-index selection and count pushdown.
//! Result equality alone cannot distinguish a keyed read from a table scan;
//! vertices_read additionally pins how many partitions the graph executor opens.
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use ferrosa_cluster::write_path::WritePath;
use ferrosa_common::{CellValue, DecoratedKey, PartitionKey};
use ferrosa_graph::engine::GraphEngine;
use ferrosa_graph::executor::expand::GraphEngineConfig;
use ferrosa_schema::auth::role::AuthContext;
use ferrosa_schema::metadata::index::IndexMetadata;
use ferrosa_schema::metadata::keyspace::{KeyspaceMetadata, ReplicationParams};
use ferrosa_schema::metadata::table::{TableMetadata, TableParams};
use ferrosa_schema::{
    AuthMethod, ClusteringOrder, ColumnKind, ColumnMetadata, DeploymentMode, EnvSecretsProvider,
    PasswordHasher, PasswordPolicy, RateLimitConfig, Schema, SchemaConfig, TestAuditSink,
};
use ferrosa_sstable::types::{DeletionTime, LivenessInfo, Row};
use ferrosa_storage::{StorageEngine, StorageEngineConfig, TableId};
use serde_json::json;
use uuid::Uuid;

fn auth() -> AuthContext {
    AuthContext {
        role: "cassandra".into(),
        is_superuser: true,
        must_change_password: false,
    }
}

struct Fixture {
    engine: GraphEngine,
    storage: Arc<StorageEngine>,
    schema: Arc<Schema>,
    table: TableMetadata,
    _dir: tempfile::TempDir,
}

impl Fixture {
    fn new(indexed: bool) -> Self {
        Self::with_pk_type(indexed, "bigint")
    }

    fn with_pk_type(indexed: bool, pk_type: &str) -> Self {
        let dir = tempfile::tempdir().unwrap();
        let mut config = StorageEngineConfig::test_config(dir.path());
        config.commit_log.segment_size = 1024 * 1024;
        let storage = Arc::new(StorageEngine::new(config, None).unwrap());
        let schema = Arc::new(
            Schema::new(SchemaConfig {
                hasher: PasswordHasher::default(),
                password_policy: PasswordPolicy::permissive(),
                auth_method: AuthMethod::Password,
                rate_limit: RateLimitConfig::default(),
                audit_sink: Box::new(TestAuditSink::new()),
                secrets: Box::new(EnvSecretsProvider),
                mode: DeploymentMode::Development,
            })
            .unwrap(),
        );
        schema
            .create_keyspace(
                KeyspaceMetadata {
                    name: "bench".into(),
                    durable_writes: true,
                    replication: ReplicationParams {
                        strategy: "SimpleStrategy".into(),
                        options: HashMap::from([("replication_factor".into(), "1".into())]),
                    },
                },
                &auth(),
            )
            .unwrap();
        let columns = [
            ("id", ColumnKind::PartitionKey, 0, "uuid"),
            ("pk", ColumnKind::Regular, 0, pk_type),
            ("bucket", ColumnKind::Regular, 1, "bigint"),
            ("payload", ColumnKind::Regular, 2, "text"),
        ]
        .into_iter()
        .map(|(name, kind, position, ty)| {
            (
                name.to_string(),
                ColumnMetadata {
                    name: name.into(),
                    kind,
                    position,
                    column_type: ty.into(),
                    clustering_order: ClusteringOrder::None,
                    mask: None,
                },
            )
        })
        .collect();
        let table = TableMetadata {
            keyspace: "bench".into(),
            name: "benchrow".into(),
            id: Uuid::new_v4(),
            columns,
            partition_key: vec!["id".into()],
            clustering_key: vec![],
            params: TableParams::default(),
            flags: HashSet::new(),
            extensions: HashMap::from([
                ("graph.type".into(), "vertex".into()),
                ("graph.label".into(), "BenchRow".into()),
            ]),
            is_system: false,
        };
        schema.create_table(table.clone(), &auth()).unwrap();
        if indexed {
            let index: IndexMetadata = serde_json::from_value(json!({
                "keyspace":"bench", "table":"benchrow", "name":"bench_pk", "index_type":"BTree",
                "target_columns":["pk"], "filter_predicate":null, "options":{},
            }))
            .unwrap();
            schema.create_index_internal(index).unwrap();
            storage
                .register_table_with_indexes(
                    table.to_storage_schema(),
                    vec![(
                        "bench_pk".into(),
                        table.storage_column_index("pk").unwrap() as usize,
                    )],
                )
                .unwrap();
        } else {
            storage.register_table(table.to_storage_schema()).unwrap();
        }
        let path = Arc::new(arc_swap::ArcSwap::from_pointee(WritePath::direct(
            Arc::clone(&storage),
        )));
        let engine = GraphEngine::new(
            Arc::clone(&schema),
            Arc::clone(&storage),
            path,
            GraphEngineConfig::default(),
            std::time::Duration::ZERO,
        );
        Self {
            engine,
            storage,
            schema,
            table,
            _dir: dir,
        }
    }

    fn write(&self, id: u128, pk: i64, payload: Option<&[u8]>, timestamp: i64) {
        self.write_pk_bytes(id, pk.to_be_bytes().to_vec(), payload, timestamp);
    }

    fn write_pk_bytes(&self, id: u128, pk: Vec<u8>, payload: Option<&[u8]>, timestamp: i64) {
        let key = DecoratedKey::new(PartitionKey::new(Uuid::from_u128(id).as_bytes().to_vec()));
        let mut cells = vec![
            (
                self.table.storage_column_index("pk").unwrap(),
                CellValue::live(pk, timestamp),
            ),
            (
                self.table.storage_column_index("bucket").unwrap(),
                CellValue::live((id as i64).to_be_bytes().to_vec(), timestamp),
            ),
        ];
        if let Some(payload) = payload {
            cells.push((
                self.table.storage_column_index("payload").unwrap(),
                CellValue::live(payload.to_vec(), timestamp),
            ));
        }
        cells.sort_unstable_by_key(|(index, _)| *index);
        self.storage
            .write(
                &TableId::new("bench", "benchrow"),
                &key,
                Row {
                    clustering: vec![],
                    cells,
                    deletion: DeletionTime::LIVE,
                    primary_key_liveness: LivenessInfo::with_timestamp(timestamp),
                },
                timestamp,
            )
            .unwrap();
    }

    async fn query(&self, query: &str) -> ferrosa_graph::executor::result::GraphResult {
        self.engine.execute(query, "bench", &auth()).await.unwrap()
    }
}

#[tokio::test]
async fn secondary_index_reads_only_matching_vertices_for_map_and_where() {
    let f = Fixture::new(true);
    for (id, pk) in [(1, 7), (2, 7), (3, 8), (4, 99)] {
        f.write(id, pk, Some(b"payload"), 1000);
    }
    assert!(f
        .storage
        .index_is_current(&TableId::new("bench", "benchrow"), "bench_pk"));
    for query in [
        "MATCH (n:BenchRow {pk: 7}) RETURN n.pk",
        "MATCH (n:BenchRow) WHERE n.pk = 7 RETURN n.pk",
        "MATCH (n:BenchRow) WHERE 7 = n.pk RETURN n.pk",
    ] {
        let result = f.query(query).await;
        assert_eq!(result.rows, vec![vec![json!(7)], vec![json!(7)]], "{query}");
        assert_eq!(
            result.stats.vertices_read, 2,
            "a maintained 2i must avoid unrelated partitions: {query}"
        );
    }
}

#[tokio::test]
async fn secondary_index_miss_does_not_scan_the_table() {
    let f = Fixture::new(true);
    f.write(1, 7, Some(b"payload"), 1000);
    let result = f.query("MATCH (n:BenchRow {pk: 999}) RETURN n.pk").await;
    assert!(result.rows.is_empty());
    assert_eq!(result.stats.vertices_read, 0);
}

#[tokio::test]
async fn declared_index_metadata_is_required_before_using_storage_postings() {
    let f = Fixture::new(true);
    f.write(1, 7, Some(b"one"), 1000);
    f.write(2, 8, Some(b"two"), 1000);
    f.schema
        .drop_index_internal("bench", "benchrow", "bench_pk")
        .unwrap();
    let result = f.query("MATCH (n:BenchRow {pk: 7}) RETURN n.pk").await;
    assert_eq!(result.rows, vec![vec![json!(7)]]);
    assert_eq!(
        result.stats.vertices_read, 2,
        "without schema declaration the existing scan remains correct"
    );
}

#[tokio::test]
async fn simple_node_counts_do_not_read_vertex_payloads() {
    let f = Fixture::new(false);
    let payload = vec![b'x'; 64 * 1024];
    for id in 1..=4 {
        f.write(id, id as i64, Some(&payload), 1000);
    }
    for query in [
        "MATCH (n:BenchRow) RETURN count(n) AS c",
        "MATCH (n:BenchRow) RETURN count(*) AS c",
    ] {
        let result = f.query(query).await;
        assert_eq!(result.rows, vec![vec![json!(4)]], "{query}");
        assert_eq!(
            result.stats.vertices_read, 0,
            "count-only execution must not materialize vertices: {query}"
        );
    }
}

#[tokio::test]
async fn count_property_nulls_filters_and_distinct_keep_their_semantics() {
    let f = Fixture::new(false);
    f.write(1, 7, Some(b"one"), 1000);
    f.write(2, 7, None, 1000);
    f.write(3, 8, Some(b"three"), 1000);
    for (query, count) in [
        ("MATCH (n:BenchRow) RETURN count(n.payload)", 2),
        ("MATCH (n:BenchRow) RETURN count(DISTINCT n.pk)", 2),
        ("MATCH (n:BenchRow) WHERE n.pk = 7 RETURN count(n)", 2),
    ] {
        assert_eq!(
            f.query(query).await.rows,
            vec![vec![json!(count)]],
            "{query}"
        );
    }
}

#[tokio::test]
async fn unsafe_predicate_shapes_keep_the_complete_scan() {
    let f = Fixture::new(true);
    f.write(1, 7, None, 1000);
    f.write(2, 8, None, 1000);
    for (query, count) in [
        (
            "MATCH (n:BenchRow) WHERE n.pk = 7 OR n.pk = 8 RETURN n.pk",
            2,
        ),
        ("MATCH (n:BenchRow) WHERE n.pk = 7.0 RETURN n.pk", 1),
        ("MATCH (n:BenchRow) WHERE n.pk = null RETURN n.pk", 0),
    ] {
        let result = f.query(query).await;
        assert_eq!(result.rows.len(), count, "{query}");
        assert_eq!(result.stats.vertices_read, 2, "{query}");
    }
}

#[tokio::test]
async fn large_integer_comparisons_are_exact_with_and_without_an_index() {
    let f = Fixture::new(true);
    f.write(1, 9_007_199_254_740_992, None, 1000);
    f.write(2, 9_007_199_254_740_993, None, 1000);
    for query in [
        "MATCH (n:BenchRow {pk: 9007199254740992}) RETURN n.pk",
        "MATCH (n:BenchRow) WHERE n.pk = 9007199254740992 RETURN n.pk",
        "MATCH (n:BenchRow) WHERE 9007199254740992 = n.pk RETURN n.pk",
    ] {
        let result = f.query(query).await;
        assert_eq!(
            result.rows,
            vec![vec![json!(9_007_199_254_740_992i64)]],
            "{query}"
        );
        assert_eq!(result.stats.vertices_read, 1, "{query}");
    }
    for (query, expected) in [
        (
            "MATCH (n:BenchRow) WHERE n.pk > 9007199254740992 RETURN n.pk",
            9_007_199_254_740_993i64,
        ),
        (
            "MATCH (n:BenchRow) WHERE n.pk < 9007199254740993 RETURN n.pk",
            9_007_199_254_740_992i64,
        ),
    ] {
        let result = f.query(query).await;
        assert_eq!(result.rows, vec![vec![json!(expected)]], "{query}");
        assert_eq!(result.stats.vertices_read, 2, "{query}");
    }
    // Mixed floating-point comparison keeps the existing coercion semantics.
    let result = f
        .query("MATCH (n:BenchRow) WHERE n.pk = 9007199254740992.0 RETURN n.pk")
        .await;
    assert_eq!(result.rows.len(), 2);
    assert_eq!(result.stats.vertices_read, 2);
}

#[tokio::test]
async fn metadata_counts_handle_empty_inputs_aliases_and_multiple_counts() {
    let f = Fixture::new(false);
    let query = "MATCH (n:BenchRow) RETURN count(n) AS a, count(*) AS b";
    let result = f.query(query).await;
    assert_eq!(result.rows, vec![vec![json!(0), json!(0)]]);
    assert_eq!(result.columns, vec!["a", "b"]);
    f.write(1, 7, None, 1000);
    let result = f.query(query).await;
    assert_eq!(result.rows, vec![vec![json!(1), json!(1)]]);
    assert_eq!(result.stats.vertices_read, 0);
}

#[tokio::test]
async fn schema_metadata_alone_cannot_authorize_an_index_read() {
    let f = Fixture::new(true);
    f.write(1, 7, None, 1000);
    f.write(2, 8, None, 1000);
    f.storage
        .drop_index(&TableId::new("bench", "benchrow"), "bench_pk")
        .unwrap();
    let result = f.query("MATCH (n:BenchRow {pk: 7}) RETURN n.pk").await;
    assert_eq!(result.rows, vec![vec![json!(7)]]);
    assert_eq!(result.stats.vertices_read, 2);
}

#[tokio::test]
async fn unfinished_backfill_falls_back_to_a_complete_scan() {
    let f = Fixture::new(true);
    f.write(1, 7, None, 1000);
    f.write(2, 8, None, 1000);
    f.storage.index_tracker().mark_pending(
        "bench",
        "benchrow",
        "bench_pk",
        "unfinished-sidecar",
        4096,
    );
    assert!(!f
        .storage
        .index_is_current(&TableId::new("bench", "benchrow"), "bench_pk"));
    let result = f.query("MATCH (n:BenchRow {pk: 7}) RETURN n.pk").await;
    assert_eq!(result.rows, vec![vec![json!(7)]]);
    assert_eq!(result.stats.vertices_read, 2);
}

#[tokio::test]
async fn typed_integer_and_uuid_properties_use_their_storage_encodings() {
    let integer = Fixture::with_pk_type(true, "int");
    integer.write_pk_bytes(1, 7i32.to_be_bytes().to_vec(), None, 1000);
    integer.write_pk_bytes(2, 8i32.to_be_bytes().to_vec(), None, 1000);
    let result = integer
        .query("MATCH (n:BenchRow {pk: 7}) RETURN n.pk")
        .await;
    assert_eq!(result.rows, vec![vec![json!(7)]]);
    assert_eq!(result.stats.vertices_read, 1);
    let miss = integer
        .query("MATCH (n:BenchRow {pk: 2147483648}) RETURN n.pk")
        .await;
    assert!(miss.rows.is_empty());
    assert_eq!(miss.stats.vertices_read, 2);

    let uuid = Fixture::with_pk_type(true, "uuid");
    uuid.write_pk_bytes(1, Uuid::from_u128(7).as_bytes().to_vec(), None, 1000);
    uuid.write_pk_bytes(2, Uuid::from_u128(8).as_bytes().to_vec(), None, 1000);
    let result = uuid
        .query("MATCH (n:BenchRow) WHERE n.pk = '00000000-0000-0000-0000-000000000007' RETURN n.pk")
        .await;
    assert_eq!(
        result.rows,
        vec![vec![json!(Uuid::from_u128(7).to_string())]]
    );
    assert_eq!(result.stats.vertices_read, 1);
}

#[tokio::test]
async fn updates_deletes_and_sstables_keep_index_and_count_answers_complete() {
    let f = Fixture::new(true);
    f.write(1, 7, Some(b"before"), 1000);
    f.write(2, 7, Some(b"other"), 1000);
    f.write(1, 8, Some(b"after"), 2000);
    assert_eq!(
        f.query("MATCH (n:BenchRow {pk: 7}) RETURN n.payload")
            .await
            .rows,
        vec![vec![json!("other")]]
    );
    assert_eq!(
        f.query("MATCH (n:BenchRow) WHERE n.pk = 8 RETURN n.payload")
            .await
            .rows,
        vec![vec![json!("after")]]
    );
    f.query("MATCH (n:BenchRow {pk: 8}) DELETE n").await;
    f.storage.flush_all().unwrap();
    assert!(f
        .query("MATCH (n:BenchRow {pk: 8}) RETURN n.pk")
        .await
        .rows
        .is_empty());
    let count = f.query("MATCH (n:BenchRow) RETURN count(n)").await;
    assert_eq!(count.rows, vec![vec![json!(1)]]);
    assert_eq!(count.stats.vertices_read, 0);
}
