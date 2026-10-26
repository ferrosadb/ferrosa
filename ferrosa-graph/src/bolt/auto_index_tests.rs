//! Exercise the real Bolt RUN/PULL dispatch against storage, without network races.
#[path = "transaction_auto_schema_tests.rs"]
mod transaction_auto_schema_tests;
use super::*;
use crate::engine::{GraphSchemaCoordinator, LocalGraphSchemaCoordinator};
use ferrosa_cluster::write_path::WritePath;
use ferrosa_schema::metadata::keyspace::{KeyspaceMetadata, ReplicationParams};
use ferrosa_schema::{
    AuthMethod, DeploymentMode, EnvSecretsProvider, PasswordHasher, PasswordPolicy,
    RateLimitConfig, SchemaConfig, TestAuditSink,
};
use ferrosa_storage::{StorageEngine, StorageEngineConfig, TableId};
use std::collections::{BTreeSet, HashMap};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

struct ObservedCoordinator {
    local: LocalGraphSchemaCoordinator,
    indexes: AtomicUsize,
    fail_indexes: AtomicBool,
    columns: AtomicUsize,
    fail_columns: AtomicBool,
}

#[async_trait::async_trait]
impl GraphSchemaCoordinator for ObservedCoordinator {
    async fn apply_create_keyspace(&self, keyspace: KeyspaceMetadata) -> crate::error::Result<()> {
        self.local.apply_create_keyspace(keyspace).await
    }
    async fn apply_create_table(
        &self,
        table: ferrosa_schema::metadata::table::TableMetadata,
    ) -> crate::error::Result<()> {
        self.local.apply_create_table(table).await
    }
    async fn apply_add_columns(
        &self,
        keyspace: &str,
        table: &str,
        columns: Vec<ferrosa_schema::metadata::column::ColumnMetadata>,
        storage: &Arc<StorageEngine>,
    ) -> crate::error::Result<()> {
        self.columns.fetch_add(1, Ordering::Relaxed);
        if self.fail_columns.load(Ordering::Relaxed) {
            return Err(GraphError::Validation("injected column DDL failure".into()));
        }
        self.local
            .apply_add_columns(keyspace, table, columns, storage)
            .await
    }
    async fn apply_create_index(
        &self,
        index: ferrosa_schema::metadata::index::IndexMetadata,
    ) -> crate::error::Result<()> {
        self.indexes.fetch_add(1, Ordering::Relaxed);
        if self.fail_indexes.load(Ordering::Relaxed) {
            return Err(GraphError::Validation("injected index DDL failure".into()));
        }
        self.local.apply_create_index(index).await
    }
}

struct Fixture {
    engine: Arc<GraphEngine>,
    schema: Arc<Schema>,
    storage: Arc<StorageEngine>,
    coordinator: Arc<ObservedCoordinator>,
    path: Arc<arc_swap::ArcSwap<WritePath>>,
    _dir: tempfile::TempDir,
}

impl Fixture {
    fn new() -> Self {
        Self::with_config(Default::default())
    }

    fn with_config(config: crate::executor::expand::GraphEngineConfig) -> Self {
        let dir = tempfile::tempdir().unwrap();
        let mut storage_config = StorageEngineConfig::test_config(dir.path());
        storage_config.index_backend = ferrosa_storage::index::IndexBackendConfig::Local;
        let storage = Arc::new(StorageEngine::new(storage_config, None).unwrap());
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
                    name: "bolt_auto".into(),
                    durable_writes: true,
                    replication: ReplicationParams {
                        strategy: "SimpleStrategy".into(),
                        options: HashMap::from([("replication_factor".into(), "1".into())]),
                    },
                },
                &Self::auth(),
            )
            .unwrap();
        let path = Arc::new(arc_swap::ArcSwap::from_pointee(WritePath::direct(
            Arc::clone(&storage),
        )));
        let coordinator = Arc::new(ObservedCoordinator {
            local: LocalGraphSchemaCoordinator::new(Arc::clone(&schema)),
            indexes: AtomicUsize::new(0),
            fail_indexes: AtomicBool::new(false),
            columns: AtomicUsize::new(0),
            fail_columns: AtomicBool::new(false),
        });
        let engine = Arc::new(GraphEngine::new_with_coordinator(
            Arc::clone(&schema),
            Arc::clone(&storage),
            Arc::clone(&path),
            config,
            std::time::Duration::ZERO,
            coordinator.clone(),
        ));
        Self {
            engine,
            schema,
            storage,
            coordinator,
            path,
            _dir: dir,
        }
    }

    fn auth() -> AuthContext {
        AuthContext {
            role: "cassandra".into(),
            is_superuser: true,
            must_change_password: false,
        }
    }

    fn connection(&self) -> ConnectionState {
        let mut state = ConnectionState::new();
        state.keyspace = "bolt_auto".into();
        state.auth_context = Some(Self::auth());
        state
    }

    async fn query(&self, state: &mut ConnectionState, query: &str) -> Vec<Vec<PackValue>> {
        let replies = process_message(
            BoltMessage::Run {
                query: query.into(),
                params: Vec::new(),
                extra: Vec::new(),
            },
            &self.engine,
            &self.schema,
            state,
        )
        .await
        .unwrap();
        assert!(
            matches!(replies.as_slice(), [BoltMessage::Success { .. }]),
            "RUN failed: {replies:?}"
        );
        let replies = process_message(
            BoltMessage::Pull { extra: Vec::new() },
            &self.engine,
            &self.schema,
            state,
        )
        .await
        .unwrap();
        assert!(
            matches!(replies.last(), Some(BoltMessage::Success { .. })),
            "PULL failed: {replies:?}"
        );
        replies
            .into_iter()
            .filter_map(|reply| match reply {
                BoltMessage::Record { values } => Some(values),
                _ => None,
            })
            .collect()
    }

    fn write_raw(&self, id: u128, columns: &[(&str, Vec<u8>)]) {
        use ferrosa_common::{CellValue, DecoratedKey, PartitionKey};
        use ferrosa_sstable::types::{DeletionTime, LivenessInfo, Row};
        let snapshot = self.schema.snapshot();
        let table = &snapshot.tables[&("bolt_auto".into(), "scalar_v".into())];
        let timestamp = 1000;
        let mut cells: Vec<_> = columns
            .iter()
            .map(|(name, bytes)| {
                (
                    table.storage_column_index(name).unwrap(),
                    CellValue::live(bytes.clone(), timestamp),
                )
            })
            .collect();
        cells.sort_unstable_by_key(|(index, _)| *index);
        let key = DecoratedKey::new(PartitionKey::new(
            uuid::Uuid::from_u128(id).as_bytes().to_vec(),
        ));
        self.storage
            .write(
                &TableId::new("bolt_auto", "scalar_v"),
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
}

#[tokio::test]
async fn bolt_first_access_indexes_scalars_and_reuses_them_across_connections() {
    let f = Fixture::new();
    let mut first = f.connection();
    f.query(
        &mut first,
        "CREATE (n:Auto {age: 42, name: 'Alice', active: true}) RETURN 1 AS inserted",
    )
    .await;
    let snapshot = f.schema.snapshot();
    let indexed: BTreeSet<_> = snapshot
        .indexes
        .values()
        .filter(|index| index.keyspace == "bolt_auto" && index.table == "auto_v")
        .flat_map(|index| index.target_columns.iter().cloned())
        .collect();
    assert_eq!(
        indexed,
        BTreeSet::from(["active".into(), "age".into(), "name".into()]),
        "ordinary Bolt CREATE must automatically index scalar properties without CQL DDL"
    );
    let table = TableId::new("bolt_auto", "auto_v");
    for index in snapshot.indexes.values() {
        assert!(f.storage.declares_index(&table, &index.name));
        assert!(f.storage.index_is_current(&table, &index.name));
    }
    let version = snapshot.version;
    assert_eq!(f.engine.bolt_index_initializations_for_test(), 1);
    assert_eq!(f.coordinator.indexes.load(Ordering::Relaxed), 3);
    let _cold_setup_is_blocked = f.engine.hold_bolt_index_setup_for_test().await;
    for _ in 0..4 {
        let mut next = f.connection();
        assert_eq!(
            tokio::time::timeout(
                std::time::Duration::from_secs(1),
                f.query(&mut next, "MATCH (n:Auto {age: 42}) RETURN n.name AS name")
            )
            .await
            .expect("a memoized connection must not acquire the cold setup mutex"),
            vec![vec![PackValue::String("Alice".into())]]
        );
        assert_eq!(
            f.schema.snapshot().version,
            version,
            "a new Bolt connection must reuse schema setup, not repeat DDL"
        );
        assert_eq!(
            f.engine.bolt_index_initializations_for_test(),
            1,
            "cache hits must not enter schema/index validation, even on a new connection"
        );
        assert_eq!(f.coordinator.indexes.load(Ordering::Relaxed), 3);
    }
}

async fn unindexed_scalar_fixture() -> Fixture {
    let f = Fixture::new();
    // Seed through the ordinary graph engine, so first Bolt access must build
    // indexes over existing rows rather than relying on predeclared indexes.
    for age in 0..64 {
        f.engine.execute(&format!(
            "CREATE (n:Scalar {{age: {age}, name: 'name{age}', active: {}, score: {age}.5}}) RETURN 1 AS inserted",
            age == 42), "bolt_auto", &Fixture::auth()).await.unwrap();
    }
    f
}

async fn seeded_scalar_fixture() -> Fixture {
    let f = unindexed_scalar_fixture().await;
    let mut state = f.connection();
    assert_eq!(
        f.query(
            &mut state,
            "MATCH (n:Scalar {age: 42}) RETURN n.name AS name"
        )
        .await,
        vec![vec![PackValue::String("name42".into())]]
    );
    f
}

#[tokio::test]
async fn bolt_first_access_backfills_sstables_and_survives_engine_recreation() {
    let mut f = unindexed_scalar_fixture().await;
    assert!(f.schema.snapshot().indexes.is_empty());
    f.storage.flush_all().unwrap();
    let mut state = f.connection();
    assert_eq!(
        f.query(&mut state, "MATCH (n:Scalar {name: 'name42'}) RETURN n.age")
            .await,
        vec![vec![PackValue::Int(42)]]
    );
    for index in f.schema.snapshot().indexes.values() {
        assert!(f
            .storage
            .index_is_current(&TableId::new("bolt_auto", "scalar_v"), &index.name));
    }
    assert_eq!(f.coordinator.indexes.load(Ordering::Relaxed), 4);
    f.engine = Arc::new(GraphEngine::new_with_coordinator(
        Arc::clone(&f.schema),
        Arc::clone(&f.storage),
        Arc::clone(&f.path),
        Default::default(),
        std::time::Duration::ZERO,
        f.coordinator.clone(),
    ));
    let mut next = f.connection();
    assert_eq!(
        f.query(&mut next, "MATCH (n:Scalar {name: 'name42'}) RETURN n.age")
            .await,
        vec![vec![PackValue::Int(42)]]
    );
    assert_eq!(
        f.coordinator.indexes.load(Ordering::Relaxed),
        4,
        "a fresh engine must reuse existing schema declarations rather than duplicate index DDL"
    );
    f.query(
        &mut next,
        "MATCH (n:Scalar {age: 42}) SET n.name = 'changed' RETURN n.age",
    )
    .await;
    assert!(f
        .query(&mut next, "MATCH (n:Scalar {name: 'name42'}) RETURN n.age")
        .await
        .is_empty());
    assert_eq!(
        f.query(&mut next, "MATCH (n:Scalar {name: 'changed'}) RETURN n.age")
            .await,
        vec![vec![PackValue::Int(42)]]
    );
    f.query(&mut next, "MATCH (n:Scalar {name: 'changed'}) DELETE n")
        .await;
    f.storage.flush_all().unwrap();
    assert!(f
        .query(&mut next, "MATCH (n:Scalar {name: 'changed'}) RETURN n.age")
        .await
        .is_empty());
}

#[tokio::test]
async fn bolt_first_access_after_adding_a_scalar_column_creates_its_index() {
    use ferrosa_schema::metadata::table::TableUpdates;
    let f = Fixture::new();
    let mut state = f.connection();
    f.query(
        &mut state,
        "CREATE (n:Altered {age: 42}) RETURN 1 AS inserted",
    )
    .await;
    f.schema
        .alter_table_internal(
            "bolt_auto",
            "altered_v",
            TableUpdates {
                params: None,
                add_columns: vec![ferrosa_schema::ColumnMetadata {
                    name: "zzrank".into(),
                    kind: ferrosa_schema::ColumnKind::Regular,
                    position: -1,
                    column_type: "bigint".into(),
                    clustering_order: ferrosa_schema::ClusteringOrder::None,
                    mask: None,
                }],
                drop_columns: vec![],
                extensions: None,
            },
        )
        .unwrap();
    let snapshot = f.schema.snapshot();
    let table = &snapshot.tables[&("bolt_auto".into(), "altered_v".into())];
    f.storage
        .update_table_schema(
            &TableId::new("bolt_auto", "altered_v"),
            table.to_storage_schema(),
        )
        .unwrap();
    f.query(
        &mut state,
        "MATCH (n:Altered {age: 42}) SET n.zzrank = 7 RETURN n.age",
    )
    .await;
    assert_eq!(
        f.query(&mut state, "MATCH (n:Altered {zzrank: 7}) RETURN n.age")
            .await,
        vec![vec![PackValue::Int(42)]]
    );
    assert_eq!(f.coordinator.indexes.load(Ordering::Relaxed), 2);
    assert_eq!(f.engine.bolt_index_initializations_for_test(), 2);
}

#[tokio::test]
async fn bolt_first_access_after_write_path_swap_revalidates_without_duplicate_ddl() {
    let f = Fixture::new();
    let mut state = f.connection();
    f.query(&mut state, "CREATE (n:Swap {age: 42}) RETURN 1 AS inserted")
        .await;
    f.path
        .store(Arc::new(WritePath::direct(Arc::clone(&f.storage))));
    assert_eq!(
        f.query(&mut state, "MATCH (n:Swap {age: 42}) RETURN n.age")
            .await,
        vec![vec![PackValue::Int(42)]]
    );
    assert_eq!(f.engine.bolt_index_initializations_for_test(), 2);
    assert_eq!(f.coordinator.indexes.load(Ordering::Relaxed), 1);
}

#[tokio::test]
async fn bolt_default_text_lookup_reads_only_the_matching_vertex() {
    let f = seeded_scalar_fixture().await;
    let result = f
        .engine
        .execute(
            "MATCH (n:Scalar {name: 'name42'}) RETURN n.age AS age",
            "bolt_auto",
            &Fixture::auth(),
        )
        .await
        .unwrap();
    assert_eq!(result.rows, vec![vec![serde_json::json!(42)]]);
    assert_eq!(
        result.stats.vertices_read, 1,
        "text equality must use the automatically created index"
    );
}

#[tokio::test]
async fn bolt_default_boolean_lookup_reads_only_the_matching_vertex() {
    let f = seeded_scalar_fixture().await;
    let result = f
        .engine
        .execute(
            "MATCH (n:Scalar {active: true}) RETURN n.age AS age",
            "bolt_auto",
            &Fixture::auth(),
        )
        .await
        .unwrap();
    assert_eq!(result.rows, vec![vec![serde_json::json!(42)]]);
    assert_eq!(
        result.stats.vertices_read, 1,
        "boolean equality must use the automatically created index"
    );
}

#[tokio::test]
async fn bolt_default_exact_float_lookup_reads_only_the_matching_vertex() {
    let f = seeded_scalar_fixture().await;
    let result = f
        .engine
        .execute(
            "MATCH (n:Scalar {score: 42.5}) RETURN n.age AS age",
            "bolt_auto",
            &Fixture::auth(),
        )
        .await
        .unwrap();
    assert_eq!(result.rows, vec![vec![serde_json::json!(42)]]);
    assert_eq!(
        result.stats.vertices_read, 1,
        "an exact floating-point lookup must use its scalar index"
    );
}

#[tokio::test]
async fn bolt_refreshes_indexes_for_a_new_label_after_the_cache_is_warm() {
    let f = Fixture::new();
    let mut state = f.connection();
    f.query(
        &mut state,
        "CREATE (n:First {age: 42}) RETURN 1 AS inserted",
    )
    .await;
    f.query(
        &mut state,
        "CREATE (n:Second {score: 7}) RETURN 1 AS inserted",
    )
    .await;
    assert!(f.schema.snapshot().indexes.values().any(|index|
        index.table == "second_v" && index.target_columns.as_slice() == ["score"]),
        "a warmed cache must notice a new schema-on-write label, even when its schema UUID is unchanged");
}

#[tokio::test]
async fn bolt_refreshes_a_dropped_index_after_the_cache_is_warm() {
    let f = Fixture::new();
    let mut state = f.connection();
    f.query(
        &mut state,
        "CREATE (n:First {age: 42}) RETURN 1 AS inserted",
    )
    .await;
    let snapshot = f.schema.snapshot();
    let index = snapshot
        .indexes
        .values()
        .find(|index| index.table == "first_v")
        .unwrap();
    f.schema
        .drop_index_internal("bolt_auto", "first_v", &index.name)
        .unwrap();
    f.query(&mut state, "MATCH (n:First {age: 42}) RETURN n.age AS age")
        .await;
    assert!(
        f.schema
            .snapshot()
            .indexes
            .values()
            .any(|index| index.table == "first_v" && index.target_columns.as_slice() == ["age"]),
        "dropping index metadata must invalidate the memoized setup"
    );
}

#[tokio::test]
async fn bolt_concurrent_first_access_initializes_a_label_once() {
    let f = Arc::new(Fixture::new());
    f.engine
        .execute(
            "CREATE (n:Shared {age: 42, name: 'Alice', active: true}) RETURN 1 AS inserted",
            "bolt_auto",
            &Fixture::auth(),
        )
        .await
        .unwrap();
    let mut jobs = tokio::task::JoinSet::new();
    for _ in 0..8 {
        let f = Arc::clone(&f);
        jobs.spawn(async move {
            let mut state = f.connection();
            f.query(
                &mut state,
                "MATCH (n:Shared {age: 42}) RETURN n.name AS name",
            )
            .await
        });
    }
    while let Some(result) = jobs.join_next().await {
        assert_eq!(
            result.unwrap(),
            vec![vec![PackValue::String("Alice".into())]]
        );
    }
    assert_eq!(f.coordinator.indexes.load(Ordering::Relaxed), 3);
    assert_eq!(f.engine.bolt_index_initializations_for_test(), 1);
}

#[tokio::test]
async fn bolt_read_only_first_access_indexes_only_the_authorized_label() {
    use ferrosa_schema::auth::permission::{GrantEntry, Permission, Resource};
    use ferrosa_schema::auth::role::RoleMetadata;
    let f = Fixture::new();
    for label in ["Allowed", "Private"] {
        f.engine
            .execute(
                &format!("CREATE (n:{label} {{age: 42}}) RETURN 1 AS inserted"),
                "bolt_auto",
                &Fixture::auth(),
            )
            .await
            .unwrap();
    }
    f.schema
        .create_role_internal(RoleMetadata {
            name: "reader".into(),
            is_superuser: false,
            can_login: true,
            salted_hash: None,
            member_of: Default::default(),
            scram: None,
        })
        .unwrap();
    f.schema
        .grant_internal(GrantEntry {
            role: "reader".into(),
            resource: Resource::Table("bolt_auto".into(), "allowed_v".into()),
            permissions: std::collections::HashSet::from([Permission::Select]),
        })
        .unwrap();
    let mut state = f.connection();
    state.auth_context = Some(AuthContext {
        role: "reader".into(),
        is_superuser: false,
        must_change_password: false,
    });
    assert_eq!(
        f.query(&mut state, "MATCH (n:Allowed {age: 42}) RETURN n.age")
            .await,
        vec![vec![PackValue::Int(42)]]
    );
    assert_eq!(
        f.coordinator.indexes.load(Ordering::Relaxed),
        1,
        "derived index maintenance must be limited to permission-checked table bindings"
    );
    assert!(f
        .schema
        .snapshot()
        .indexes
        .values()
        .all(|index| index.table == "allowed_v"));
    let calls = f.coordinator.indexes.load(Ordering::Relaxed);
    let replies = process_message(
        BoltMessage::Run {
            query: "MATCH (n:Private) RETURN n.age".into(),
            params: vec![],
            extra: vec![],
        },
        &f.engine,
        &f.schema,
        &mut state,
    )
    .await
    .unwrap();
    assert!(
        matches!(replies.as_slice(), [BoltMessage::Failure { .. }]),
        "{replies:?}"
    );
    assert_eq!(
        f.coordinator.indexes.load(Ordering::Relaxed),
        calls,
        "a denied query must not perform index DDL"
    );
}

#[tokio::test]
async fn bolt_index_setup_failure_happens_before_unwind_writes_and_can_retry() {
    let f = Fixture::new();
    f.coordinator.fail_indexes.store(true, Ordering::Relaxed);
    let mut state = f.connection();
    let query = "UNWIND [1,2,3] AS age CREATE (n:Batch {age: age}) RETURN 1 AS inserted";
    let replies = process_message(
        BoltMessage::Run {
            query: query.into(),
            params: vec![],
            extra: vec![],
        },
        &f.engine,
        &f.schema,
        &mut state,
    )
    .await
    .unwrap();
    assert!(
        matches!(replies.as_slice(), [BoltMessage::Failure { .. }]),
        "{replies:?}"
    );
    assert_eq!(
        f.storage
            .count_range(&TableId::new("bolt_auto", "batch_v"), None, None)
            .unwrap(),
        0,
        "index DDL failure must precede the first data write"
    );
    let reset = process_message(BoltMessage::Reset, &f.engine, &f.schema, &mut state)
        .await
        .unwrap();
    assert!(
        matches!(reset.as_slice(), [BoltMessage::Success { .. }]),
        "RESET after setup failure must allow retry: {reset:?}"
    );
    f.coordinator.fail_indexes.store(false, Ordering::Relaxed);
    f.query(&mut state, query).await;
    assert_eq!(
        f.storage
            .count_range(&TableId::new("bolt_auto", "batch_v"), None, None)
            .unwrap(),
        3,
        "retrying setup must not duplicate earlier writes"
    );
    assert_eq!(
        f.engine.bolt_index_initializations_for_test(),
        2,
        "a failed setup must not be memoized as complete"
    );
    let snapshot = f.schema.snapshot();
    let index = snapshot
        .indexes
        .values()
        .find(|index| index.table == "batch_v")
        .unwrap();
    assert!(f
        .storage
        .index_is_current(&TableId::new("bolt_auto", "batch_v"), &index.name));
}

#[tokio::test]
async fn bolt_waiting_for_first_access_stays_inside_the_query_timeout() {
    let f = Fixture::with_config(crate::executor::expand::GraphEngineConfig {
        query_timeout: std::time::Duration::from_millis(50),
        ..Default::default()
    });
    f.engine
        .execute(
            "CREATE (n:Blocked {age: 42}) RETURN 1 AS inserted",
            "bolt_auto",
            &Fixture::auth(),
        )
        .await
        .unwrap();
    let _blocked = f.engine.hold_bolt_index_setup_for_test().await;
    let result = tokio::time::timeout(
        std::time::Duration::from_secs(1),
        f.engine.execute_bolt_stream_with_params(
            "MATCH (n:Blocked) RETURN n.age",
            "bolt_auto",
            &Fixture::auth(),
            &HashMap::new(),
        ),
    )
    .await
    .expect("the query timeout must include waiting for the setup gate");
    assert!(result.is_err());
}

#[tokio::test]
async fn bolt_scalar_indexes_preserve_noncanonical_byte_and_float_equality() {
    let f = seeded_scalar_fixture().await;
    f.write_raw(
        70,
        &[
            ("age", 70_i64.to_be_bytes().to_vec()),
            ("name", vec![0xff]),
            ("active", vec![2]),
            ("score", 1_f64.next_down().to_be_bytes().to_vec()),
        ],
    );
    f.write_raw(
        71,
        &[
            ("age", 71_i64.to_be_bytes().to_vec()),
            ("name", b"ff".to_vec()),
            ("active", vec![255]),
            ("score", 1_f64.to_be_bytes().to_vec()),
        ],
    );
    for (predicate, mut expected) in [
        ("name: 'ff'", vec![70, 71]),
        ("active: true", vec![42, 70, 71]),
        ("score: 1.0", vec![70, 71]),
    ] {
        let result = f
            .engine
            .execute(
                &format!("MATCH (n:Scalar {{{predicate}}}) RETURN n.age"),
                "bolt_auto",
                &Fixture::auth(),
            )
            .await
            .unwrap();
        let mut ages: Vec<_> = result
            .rows
            .iter()
            .map(|row| row[0].as_i64().unwrap())
            .collect();
        ages.sort_unstable();
        expected.sort_unstable();
        assert_eq!(
            ages, expected,
            "index aliases must cover the decoder's equality for {predicate}"
        );
        assert_eq!(result.stats.vertices_read as usize, expected.len());
    }
    f.write_raw(
        72,
        &[
            ("age", 72_i64.to_be_bytes().to_vec()),
            ("score", 0_f64.to_be_bytes().to_vec()),
        ],
    );
    f.write_raw(
        73,
        &[
            ("age", 73_i64.to_be_bytes().to_vec()),
            ("score", 0_f64.next_up().to_be_bytes().to_vec()),
        ],
    );
    let result = f
        .engine
        .execute(
            "MATCH (n:Scalar {score: 0.0}) RETURN n.age",
            "bolt_auto",
            &Fixture::auth(),
        )
        .await
        .unwrap();
    let mut ages: Vec<_> = result
        .rows
        .iter()
        .map(|row| row[0].as_i64().unwrap())
        .collect();
    ages.sort_unstable();
    assert_eq!(
        ages,
        vec![72, 73],
        "a broad epsilon comparison must keep its complete range fallback"
    );
}

#[tokio::test]
async fn bolt_first_write_adds_a_new_scalar_column_and_index_on_an_existing_label() {
    let f = Fixture::new();
    f.query(&mut f.connection(), "CREATE (n:Evolving {pk: 1})")
        .await;
    f.query(
        &mut f.connection(),
        "CREATE (n:Evolving {pk: 2, score: 42})",
    )
    .await;
    let rows = f
        .query(
            &mut f.connection(),
            "MATCH (n:Evolving) WHERE n.score = 42 RETURN n.pk, n.score",
        )
        .await;
    assert_eq!(
        rows,
        vec![vec![PackValue::Int(2), PackValue::Int(42)]],
        "a new scalar property must be stored on its first write"
    );
    let snapshot = f.schema.snapshot();
    let table = &snapshot.tables[&("bolt_auto".into(), "evolving_v".into())];
    assert!(table.columns.contains_key("score"));
    assert!(snapshot
        .indexes
        .values()
        .any(|index| index.table == "evolving_v" && index.target_columns == ["score"]));
}

#[tokio::test]
async fn bolt_initial_write_infers_negative_scalar_literals() {
    let f = Fixture::new();
    f.query(
        &mut f.connection(),
        "CREATE (n:Negative {pk: -1, score: -1.5})",
    )
    .await;
    assert_eq!(
        f.query(
            &mut f.connection(),
            "MATCH (n:Negative) WHERE n.pk = -1 RETURN n.pk, n.score"
        )
        .await,
        vec![vec![PackValue::Int(-1), PackValue::Float(-1.5)]]
    );
}

#[tokio::test]
async fn bolt_set_and_unwind_add_scalar_columns_once_and_preserve_existing_rows() {
    let f = Fixture::new();
    f.query(
        &mut f.connection(),
        "CREATE (n:Evolving {pk: 1, zulu: 'kept'})",
    )
    .await;
    f.query(
        &mut f.connection(),
        "MATCH (n:Evolving {pk: 1}) SET n.alpha = 42",
    )
    .await;

    assert_eq!(
        f.query(
            &mut f.connection(),
            "MATCH (n:Evolving {alpha: 42}) RETURN n.pk, n.zulu"
        )
        .await,
        vec![vec![PackValue::Int(1), PackValue::String("kept".into())]]
    );
    f.query(&mut f.connection(),
        "UNWIND [{pk: 2, active: true}, {pk: 3, active: false}] AS row CREATE (n:Evolving {pk: row.pk, active: row.active})").await;
    assert_eq!(
        f.query(
            &mut f.connection(),
            "MATCH (n:Evolving {active: true}) RETURN n.pk"
        )
        .await,
        vec![vec![PackValue::Int(2)]]
    );
    assert_eq!(f.coordinator.columns.load(Ordering::Relaxed), 2);
    for pk in 4..8 {
        f.query(
            &mut f.connection(),
            &format!("CREATE (n:Evolving {{pk: {pk}, active: true}})"),
        )
        .await;
    }
    assert_eq!(
        f.coordinator.columns.load(Ordering::Relaxed),
        2,
        "new connections and known properties must not repeat column DDL"
    );
    let hot_guard = f.engine.hold_bolt_schema_setup_for_test().await;
    tokio::time::timeout(
        std::time::Duration::from_secs(2),
        f.query(
            &mut f.connection(),
            "CREATE (n:Evolving {pk: 9, active: true})",
        ),
    )
    .await
    .expect("known columns on a fresh connection must bypass the schema-setup mutex");
    drop(hot_guard);
    let result = f
        .engine
        .execute(
            "MATCH (n:Evolving {alpha: 42}) RETURN n.zulu",
            "bolt_auto",
            &Fixture::auth(),
        )
        .await
        .unwrap();
    assert_eq!(result.stats.vertices_read, 1);
}

#[tokio::test]
async fn bolt_failed_column_setup_writes_nothing_and_is_retryable() {
    let f = Fixture::new();
    f.query(&mut f.connection(), "CREATE (n:Evolving {pk: 1})")
        .await;
    f.coordinator.fail_columns.store(true, Ordering::Relaxed);
    let query = "CREATE (n:Evolving {pk: 2, score: 42})";
    let result = f
        .engine
        .execute_bolt_stream_with_params(query, "bolt_auto", &Fixture::auth(), &HashMap::new())
        .await;
    assert!(result.is_err());
    assert_eq!(
        f.query(&mut f.connection(), "MATCH (n:Evolving) RETURN n.pk")
            .await,
        vec![vec![PackValue::Int(1)]]
    );
    f.coordinator.fail_columns.store(false, Ordering::Relaxed);
    f.query(&mut f.connection(), query).await;
    assert_eq!(
        f.query(
            &mut f.connection(),
            "MATCH (n:Evolving {score: 42}) RETURN n.pk"
        )
        .await,
        vec![vec![PackValue::Int(2)]]
    );
    assert_eq!(f.coordinator.columns.load(Ordering::Relaxed), 2);
}

#[tokio::test]
async fn bolt_schema_evolution_uses_the_production_ddl_coordinator() {
    let mut f = Fixture::new();
    f.storage.register_system_tables().unwrap();
    let ddl = Arc::new(arc_swap::ArcSwap::from_pointee(
        ferrosa_cluster::ddl_path::DdlPath::Direct {
            schema: Arc::clone(&f.schema),
            engine: Arc::clone(&f.storage),
        },
    ));
    f.engine = Arc::new(GraphEngine::new_with_coordinator(
        Arc::clone(&f.schema),
        Arc::clone(&f.storage),
        Arc::clone(&f.path),
        Default::default(),
        std::time::Duration::ZERO,
        Arc::new(crate::engine::ClusterGraphSchemaCoordinator::new(ddl)),
    ));
    f.query(
        &mut f.connection(),
        "CREATE (n:Evolving {pk: -1, zulu: 'before'})",
    )
    .await;
    f.query(
        &mut f.connection(),
        "MATCH (n:Evolving {pk: -1}) SET n.alpha = 42",
    )
    .await;
    assert_eq!(
        f.query(
            &mut f.connection(),
            "MATCH (n:Evolving {alpha: 42}) RETURN n.pk, n.zulu"
        )
        .await,
        vec![vec![PackValue::Int(-1), PackValue::String("before".into())]]
    );
}

#[tokio::test]
async fn bolt_foreach_keeps_values_when_a_later_clause_adds_a_column() {
    let f = Fixture::new();
    f.query(
        &mut f.connection(),
        "CREATE (n:Evolving {pk: 0, zulu: 'seed'})",
    )
    .await;
    f.query(&mut f.connection(),
        "FOREACH (x IN [1] | CREATE (n:Evolving {pk: 1, zulu: 'first'}) CREATE (m:Evolving {pk: 2, alpha: 42}))").await;
    assert_eq!(
        f.query(
            &mut f.connection(),
            "MATCH (n:Evolving {pk: 1}) RETURN n.pk, n.zulu"
        )
        .await,
        vec![vec![PackValue::Int(1), PackValue::String("first".into())]]
    );
    assert_eq!(
        f.query(
            &mut f.connection(),
            "MATCH (n:Evolving {alpha: 42}) RETURN n.pk"
        )
        .await,
        vec![vec![PackValue::Int(2)]]
    );
}

#[tokio::test]
async fn bolt_waiting_for_first_write_column_setup_respects_the_query_timeout() {
    let f = Fixture::with_config(crate::executor::expand::GraphEngineConfig {
        query_timeout: std::time::Duration::from_millis(50),
        ..Default::default()
    });
    f.query(&mut f.connection(), "CREATE (n:Evolving {pk: 1})")
        .await;
    let _blocked = f.engine.hold_bolt_schema_setup_for_test().await;
    let result = tokio::time::timeout(
        std::time::Duration::from_secs(1),
        f.engine.execute_bolt_stream_with_params(
            "CREATE (n:Evolving {pk: 2, score: 42})",
            "bolt_auto",
            &Fixture::auth(),
            &HashMap::new(),
        ),
    )
    .await
    .expect("the setup timeout must include waiting for column DDL");
    assert!(result.is_err());
}

#[tokio::test]
async fn bolt_new_scalar_columns_require_alter_permission() {
    use ferrosa_schema::auth::permission::{GrantEntry, Permission, Resource};
    use ferrosa_schema::auth::role::RoleMetadata;
    let f = Fixture::new();
    f.query(&mut f.connection(), "CREATE (n:Evolving {pk: 1})")
        .await;
    f.schema
        .create_role_internal(RoleMetadata {
            name: "writer".into(),
            is_superuser: false,
            can_login: true,
            salted_hash: None,
            member_of: Default::default(),
            scram: None,
        })
        .unwrap();
    f.schema
        .grant_internal(GrantEntry {
            role: "writer".into(),
            resource: Resource::Table("bolt_auto".into(), "evolving_v".into()),
            permissions: std::collections::HashSet::from([Permission::Modify, Permission::Select]),
        })
        .unwrap();
    let auth = AuthContext {
        role: "writer".into(),
        is_superuser: false,
        must_change_password: false,
    };
    let result = f
        .engine
        .execute_bolt_stream_with_params(
            "CREATE (n:Evolving {pk: 2, score: 42})",
            "bolt_auto",
            &auth,
            &HashMap::new(),
        )
        .await;
    assert!(matches!(result, Err(GraphError::PermissionDenied(_))));
    assert!(
        !f.schema.snapshot().tables[&("bolt_auto".into(), "evolving_v".into())]
            .columns
            .contains_key("score")
    );
    assert_eq!(f.coordinator.columns.load(Ordering::Relaxed), 0);
}

#[tokio::test]
async fn bolt_existing_index_finds_old_rows_after_a_new_column_sorts_before_it() {
    let f = Fixture::new();
    f.query(
        &mut f.connection(),
        "CREATE (n:Evolving {pk: 1, zulu: 'kept'})",
    )
    .await;
    f.query(
        &mut f.connection(),
        "CREATE (n:Evolving {pk: 2, alpha: 42})",
    )
    .await;
    assert_eq!(
        f.query(
            &mut f.connection(),
            "MATCH (n:Evolving {pk: 1}) RETURN n.pk, n.zulu"
        )
        .await,
        vec![vec![PackValue::Int(1), PackValue::String("kept".into())]]
    );
}
