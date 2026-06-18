//! Storage-backed table loading: bridge real ferrosa storage to the bespoke
//! relational engine's row model (`ferrosa-sql`).
//!
//! ## Why this exists (the sync/async impedance mismatch)
//!
//! The relational engine is fully **synchronous**: [`ferrosa_sql::TableProvider::scan`]
//! returns a plain iterator and [`ferrosa_sql::Catalog::resolve`] is a sync call.
//! Real ferrosa storage, by contrast, exposes its range scan as an **async**
//! `Stream<Item = Result<Partition>>` ([`ferrosa_storage::StorageEngine::range_iter`]).
//!
//! We resolve this by **materializing the scan asynchronously up front**: an async
//! loader ([`load_table`]) drains the storage stream, decomposes every partition
//! into rows, and hands back a fully-populated [`ferrosa_sql::InMemoryTable`]. The
//! sync scan/filter/join operators then run over that in-memory snapshot. We do
//! **not** `block_on` inside the sync `scan()` — doing so on the server's async
//! runtime risks deadlock. The query executor is expected to call [`load_table`]
//! (await it) before it runs the sync operators.
//!
//! ## R15 guard: missing table is NOT an empty table
//!
//! `StorageEngine::range_iter` returns an *empty stream* for an unregistered
//! table (`None => stream::empty()`). That is indistinguishable from a registered
//! but empty table at the stream level. To avoid silently scanning "nothing" for
//! a name that does not exist, [`load_table`] checks the **schema metadata** first
//! and fails loud with [`LoadError::NoSuchTable`] when the table is absent — a
//! distinct outcome from `Ok` with zero rows for an existing empty table.
//!
//! ## Column ordering (mirrors the CQL SELECT read path)
//!
//! Partition decomposition is delegated to
//! [`ferrosa_cql::bridge::partition_to_rows_with_storage_mapping`] — the *same*
//! helper the CQL `route_select` path uses (see `ferrosa-cql/src/router.rs`,
//! `decode_agreed_row_to_map` / `route_select_user_table`). Output rows follow the
//! table's **declared column order** (`TableMetadata.columns`, an `IndexMap`):
//! partition-key, clustering-key, and regular/static columns in their DDL order.
//! We do not invent an ordering — reusing the bridge guarantees parity with CQL.
//!
//! ## Lossy [`CqlValue`] -> [`ferrosa_sql::Value`] conversion
//!
//! The first-slice [`ferrosa_sql::Value`] models `Null | Int(i64) | Text | Bool
//! | Float(f64)`. [`cql_to_value`] maps the integral / textual / boolean /
//! floating-point CQL scalars onto it losslessly (f32 widens to f64). Every other
//! CQL type is **known-lossy** and converts to `Value::Null` (with a code comment,
//! never a panic). Widening `Value` to carry these is tracked as follow-up. The
//! lossy types are:
//!
//! - Arbitrary precision: `Decimal`, `Varint`
//! - Temporal: `Timestamp`, `Date`, `Time`, `Duration`
//! - Identifiers / binary: `Uuid`, `Timeuuid`, `Inet`, `Blob`
//! - Collections / composites: `List`, `Set`, `Map`, `Tuple`, `Udt`, `Vector`

use std::fmt;

use ferrosa_common::CqlValue;
use ferrosa_schema::{ColumnKind, Schema, TableMetadata};
use ferrosa_sql::{Column, ColumnType, InMemoryTable, RelSchema, Row, Value};
use ferrosa_storage::{StorageEngine, TableId};
use futures::StreamExt;

/// Convert a single ferrosa [`CqlValue`] to the engine's [`ferrosa_sql::Value`].
///
/// Lossless for the integral / textual / boolean / floating-point scalars; every
/// other variant is a documented lossy gap that maps to [`Value::Null`] (see the
/// module docs for the full list). This is deliberately **not** a panic — widening
/// `Value` to represent these types is follow-up work, and a query over a wider
/// table should still run, treating the as-yet-unmodelled columns as NULL.
pub fn cql_to_value(v: &CqlValue) -> Value {
    match v {
        CqlValue::Null => Value::Null,
        // Integral types widen into i64 losslessly.
        CqlValue::Int(i) => Value::Int(i64::from(*i)),
        CqlValue::Bigint(i) | CqlValue::Counter(i) => Value::Int(*i),
        CqlValue::Smallint(i) => Value::Int(i64::from(*i)),
        CqlValue::Tinyint(i) => Value::Int(i64::from(*i)),
        // Textual types.
        CqlValue::Text(s) | CqlValue::Ascii(s) => Value::Text(s.clone()),
        // Boolean.
        CqlValue::Boolean(b) => Value::Bool(*b),
        // Floating point: `Float`/`Double` carry IEEE-754 bit patterns (so the
        // CQL value type can be Eq/Ord). Reconstruct the float and widen f32→f64.
        CqlValue::Float(bits) => Value::float(f32::from_bits(*bits) as f64),
        CqlValue::Double(bits) => Value::float(f64::from_bits(*bits)),
        // ── Known lossy gaps ──────────────────────────────────────────────
        // The first-slice `ferrosa_sql::Value` cannot represent these yet, so
        // they read as NULL rather than panicking. Widen `Value` (and update
        // this match) when real support lands. See the module-level doc list.
        CqlValue::Decimal { .. }
        | CqlValue::Varint(_)
        | CqlValue::Timestamp(_)
        | CqlValue::Date(_)
        | CqlValue::Time(_)
        | CqlValue::Duration { .. }
        | CqlValue::Uuid(_)
        | CqlValue::Timeuuid(_)
        | CqlValue::Inet(_)
        | CqlValue::Blob(_)
        | CqlValue::List(_)
        | CqlValue::Set(_)
        | CqlValue::Map(_)
        | CqlValue::Tuple(_)
        | CqlValue::Udt(_)
        | CqlValue::Vector(_) => Value::Null,
    }
}

/// Failure modes of [`load_table`].
///
/// [`LoadError::NoSuchTable`] is deliberately distinct from a successful load of
/// an existing-but-empty table (the R15 guard): the former is an error, the
/// latter is `Ok` with zero rows.
#[derive(Debug)]
pub enum LoadError {
    /// The `keyspace.table` is not present in the schema snapshot. The loader
    /// refuses to substitute an empty relation for a missing table.
    NoSuchTable { keyspace: String, table: String },
    /// A storage / decode error surfaced while materializing the scan.
    Storage(String),
}

impl fmt::Display for LoadError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            LoadError::NoSuchTable { keyspace, table } => {
                write!(f, "no such table: {keyspace}.{table}")
            }
            LoadError::Storage(msg) => write!(f, "storage error loading table: {msg}"),
        }
    }
}

impl std::error::Error for LoadError {}

/// Map a CQL column-type string to the engine's [`ColumnType`].
///
/// Only the three first-slice engine types exist (`Int`, `Text`, `Bool`); the CQL
/// integral family collapses to `Int`, the textual family to `Text`. Anything the
/// engine can't yet model (and any unknown type) defaults to `Text` — the most
/// permissive textual representation — consistent with `catalog::type_oid`'s
/// text fallback. The actual *value* for such columns still comes back NULL via
/// [`cql_to_value`]; this only decides the column's declared schema type.
fn engine_column_type(cql_type: &str) -> ColumnType {
    match normalize_type_head(cql_type).as_str() {
        "int" | "bigint" | "counter" | "smallint" | "tinyint" | "varint" => ColumnType::Int,
        "boolean" | "bool" => ColumnType::Bool,
        "text" | "varchar" | "ascii" => ColumnType::Text,
        // Unknown / not-yet-modelled types default to Text (documented fallback).
        _ => ColumnType::Text,
    }
}

/// Lower-case a CQL type name, strip an outer `frozen<...>`, and take the head
/// identifier before any `<` (so `map<text,text>` -> `map`). Mirrors
/// `catalog::normalize_type_name`.
fn normalize_type_head(column_type: &str) -> String {
    let lower = column_type.trim().to_ascii_lowercase();
    let unwrapped = lower
        .strip_prefix("frozen<")
        .and_then(|rest| rest.strip_suffix('>'))
        .unwrap_or(&lower);
    unwrapped
        .split('<')
        .next()
        .unwrap_or(unwrapped)
        .trim()
        .to_string()
}

/// Build the engine [`RelSchema`] from a table's columns in declared order.
fn rel_schema_for(meta: &TableMetadata) -> RelSchema {
    let columns = meta
        .columns
        .values()
        .map(|col| Column::new(col.name.clone(), engine_column_type(&col.column_type)))
        .collect();
    RelSchema::new(columns)
}

/// Column indices (into the declared-order column list) that form the partition
/// key, in partition-key order. Mirrors `router::decode_agreed_row_to_map`.
fn pk_indices(meta: &TableMetadata) -> Vec<usize> {
    meta.partition_key
        .iter()
        .filter_map(|name| meta.columns.get_index_of(name))
        .collect()
}

/// Column indices that form the clustering key, in clustering order. Mirrors
/// `router::decode_agreed_row_to_map`.
fn ck_indices(meta: &TableMetadata) -> Vec<usize> {
    meta.clustering_key
        .iter()
        .filter_map(|(name, _)| meta.columns.get_index_of(name))
        .collect()
}

/// Storage-cell-ordinal -> declared-column-index map for regular/static columns.
///
/// A re-implementation of the (private) `router::storage_to_table_indices`: a
/// storage `Row`'s cells carry a `u16` ordinal in the SSTable's static+regular
/// column space; this turns that ordinal into the column's position in the
/// table's declared order so the bridge can place each cell correctly.
fn storage_to_table_indices(meta: &TableMetadata) -> Vec<usize> {
    let mut pairs: Vec<(u16, usize)> = meta
        .columns
        .iter()
        .filter(|(_, col)| matches!(col.kind, ColumnKind::Regular | ColumnKind::Static))
        .filter_map(|(name, _)| {
            let storage_idx = meta.storage_column_index(name)?;
            let table_idx = meta.columns.get_index_of(name)?;
            Some((storage_idx, table_idx))
        })
        .collect();
    pairs.sort_by_key(|(storage_idx, _)| *storage_idx);
    pairs.into_iter().map(|(_, table_idx)| table_idx).collect()
}

/// Load every row of `keyspace.table` from ferrosa storage into an in-memory,
/// synchronously-scannable [`InMemoryTable`].
///
/// This is the async materialization step described in the module docs: it
/// awaits the full `range_iter` stream up front so the sync engine operators can
/// run over real data without ever blocking the runtime.
///
/// # Errors
///
/// - [`LoadError::NoSuchTable`] if the table is not in the schema snapshot (the
///   R15 guard — never returns an empty relation for a missing table).
/// - [`LoadError::Storage`] if the storage stream yields an error, or a column
///   type string fails to parse.
pub async fn load_table(
    engine: &StorageEngine,
    schema: &Schema,
    keyspace: &str,
    table: &str,
) -> Result<InMemoryTable, LoadError> {
    let snapshot = schema.snapshot();

    // R15 guard: existence is decided by schema metadata, never by an empty
    // stream. A missing table errors; an existing empty table loads zero rows.
    let meta = snapshot
        .tables
        .get(&(keyspace.to_string(), table.to_string()))
        .ok_or_else(|| LoadError::NoSuchTable {
            keyspace: keyspace.to_string(),
            table: table.to_string(),
        })?;

    let rel_schema = rel_schema_for(meta);

    // Column context for the canonical CQL decomposition, in declared order.
    let col_names: Vec<String> = meta.columns.keys().cloned().collect();
    let col_types = meta
        .columns
        .values()
        .map(|c| ferrosa_cql::bridge::parse_cql_type_in_keyspace(&c.column_type, keyspace, schema))
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| LoadError::Storage(format!("failed to resolve column type: {e}")))?;
    let pk_idx = pk_indices(meta);
    let ck_idx = ck_indices(meta);
    let storage_to_table = storage_to_table_indices(meta);

    // Materialize the async scan up front (full-table: no key bounds).
    let table_id = TableId::new(keyspace, table);
    let mut stream = engine.range_iter(&table_id, None, None);

    let mut rows: Vec<Row> = Vec::new();
    while let Some(item) = stream.next().await {
        let partition = item.map_err(|e| LoadError::Storage(e.to_string()))?;
        // Mirror the CQL SELECT path: one engine row per logical CQL row, with
        // values in the table's declared column order.
        for cql_row in ferrosa_cql::bridge::partition_to_rows_with_storage_mapping(
            &partition,
            &col_names,
            &col_types,
            &pk_idx,
            &ck_idx,
            &storage_to_table,
        ) {
            let values = cql_row
                .iter()
                .map(|cell| cell.as_ref().map_or(Value::Null, cql_to_value))
                .collect();
            rows.push(Row::new(values));
        }
    }

    Ok(InMemoryTable::new(rel_schema, rows))
}

#[cfg(test)]
mod tests {
    use super::*;

    use ferrosa_common::cell::CellValue;
    use ferrosa_common::key::{DecoratedKey, PartitionKey};
    use ferrosa_common::CqlValue;
    use ferrosa_schema::{
        AuthContext, AuthMethod, ClusteringOrder, ColumnKind, ColumnMetadata, DeploymentMode,
        EnvSecretsProvider, KeyspaceMetadata, PasswordHasher, PasswordPolicy, RateLimitConfig,
        ReplicationParams, Schema, SchemaConfig, TableMetadata, TableParams, TestAuditSink,
    };
    use ferrosa_sql::TableProvider;
    use ferrosa_sstable::types::{DeletionTime, LivenessInfo, Row as StorageRow};
    use ferrosa_storage::{
        CommitLogConfig, CompactionConfig, StorageEngine, StorageEngineConfig, SyncStrategyConfig,
        TableId,
    };
    use indexmap::IndexMap;
    use std::collections::{HashMap, HashSet};
    use std::path::Path;
    use std::time::Duration;
    use uuid::Uuid;

    // ── Pure conversion tests (no infra) ──────────────────────────────────

    #[test]
    fn cql_to_value_maps_supported_scalars() {
        assert_eq!(cql_to_value(&CqlValue::Null), Value::Null);
        assert_eq!(cql_to_value(&CqlValue::Int(42)), Value::Int(42));
        assert_eq!(
            cql_to_value(&CqlValue::Bigint(9_000_000_000)),
            Value::Int(9_000_000_000)
        );
        assert_eq!(cql_to_value(&CqlValue::Counter(7)), Value::Int(7));
        assert_eq!(cql_to_value(&CqlValue::Smallint(-3)), Value::Int(-3));
        assert_eq!(cql_to_value(&CqlValue::Tinyint(5)), Value::Int(5));
        assert_eq!(
            cql_to_value(&CqlValue::Text("hi".to_string())),
            Value::Text("hi".to_string())
        );
        assert_eq!(
            cql_to_value(&CqlValue::Ascii("a".to_string())),
            Value::Text("a".to_string())
        );
        assert_eq!(cql_to_value(&CqlValue::Boolean(true)), Value::Bool(true));
    }

    #[test]
    fn cql_to_value_maps_lossy_types_to_null() {
        // A representative lossy scalar, temporal, identifier, and collection.
        assert_eq!(cql_to_value(&CqlValue::Timestamp(123)), Value::Null);
        assert_eq!(cql_to_value(&CqlValue::Uuid(Uuid::nil())), Value::Null);
        assert_eq!(cql_to_value(&CqlValue::Blob(vec![1, 2, 3])), Value::Null);
        assert_eq!(
            cql_to_value(&CqlValue::List(vec![CqlValue::Int(1)])),
            Value::Null
        );
    }

    #[test]
    fn cql_to_value_maps_floats() {
        // `Float`/`Double` carry IEEE-754 bit patterns; reconstruct + widen.
        assert_eq!(
            cql_to_value(&CqlValue::Double(1.5f64.to_bits())),
            Value::float(1.5)
        );
        assert_eq!(
            cql_to_value(&CqlValue::Float((-0.5f32).to_bits())),
            Value::float(-0.5)
        );
        // Zero bits decode to 0.0 (not NULL).
        assert_eq!(cql_to_value(&CqlValue::Double(0)), Value::float(0.0));
        assert_eq!(cql_to_value(&CqlValue::Float(0)), Value::float(0.0));
    }

    #[test]
    fn engine_column_type_maps_families() {
        assert_eq!(engine_column_type("int"), ColumnType::Int);
        assert_eq!(engine_column_type("bigint"), ColumnType::Int);
        assert_eq!(engine_column_type("text"), ColumnType::Text);
        assert_eq!(engine_column_type("ASCII"), ColumnType::Text);
        assert_eq!(engine_column_type("boolean"), ColumnType::Bool);
        // Unknown / not-yet-modelled -> Text fallback.
        assert_eq!(engine_column_type("uuid"), ColumnType::Text);
        assert_eq!(engine_column_type("map<text, text>"), ColumnType::Text);
    }

    #[test]
    fn load_error_display_distinguishes_variants() {
        let nst = LoadError::NoSuchTable {
            keyspace: "ks".to_string(),
            table: "t".to_string(),
        };
        assert!(nst.to_string().contains("no such table"));
        assert!(nst.to_string().contains("ks.t"));
        let storage = LoadError::Storage("boom".to_string());
        assert!(storage.to_string().contains("boom"));
    }

    // ── Real-engine integration tests ─────────────────────────────────────
    //
    // These run a real `StorageEngine` against a temp directory with no S3 /
    // Docker / cluster — fully local, no `live-infra-tests` feature, no env
    // vars. The storage `TableSchema` (cell layout) and the `ferrosa_schema`
    // `TableMetadata` (column metadata the loader reads) are built to describe
    // the SAME table so the round trip is consistent.

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

    fn column(name: &str, kind: ColumnKind, ty: &str, position: i32) -> ColumnMetadata {
        ColumnMetadata {
            name: name.to_string(),
            kind,
            position,
            column_type: ty.to_string(),
            clustering_order: ClusteringOrder::None,
            mask: None,
        }
    }

    /// A `ferrosa_schema::Schema` with keyspace `ks` and table
    /// `t(id text PK, ck int CK, name text, score int)` — declared in that
    /// order — created through the public DDL API.
    fn schema_with_table() -> Schema {
        let schema = Schema::new(schema_config()).expect("schema bootstraps");
        let auth = superuser();

        schema
            .create_keyspace(
                KeyspaceMetadata {
                    name: "ks".to_string(),
                    durable_writes: true,
                    replication: ReplicationParams {
                        strategy: "SimpleStrategy".to_string(),
                        options: {
                            let mut o = HashMap::new();
                            o.insert("replication_factor".to_string(), "1".to_string());
                            o
                        },
                    },
                },
                &auth,
            )
            .expect("create keyspace");

        let mut columns = IndexMap::new();
        columns.insert(
            "id".to_string(),
            column("id", ColumnKind::PartitionKey, "text", 0),
        );
        columns.insert(
            "ck".to_string(),
            column("ck", ColumnKind::Clustering, "int", 0),
        );
        columns.insert(
            "name".to_string(),
            column("name", ColumnKind::Regular, "text", 0),
        );
        columns.insert(
            "score".to_string(),
            column("score", ColumnKind::Regular, "int", 0),
        );

        schema
            .create_table(
                TableMetadata {
                    keyspace: "ks".to_string(),
                    name: "t".to_string(),
                    id: Uuid::new_v4(),
                    columns,
                    partition_key: vec!["id".to_string()],
                    clustering_key: vec![("ck".to_string(), ClusteringOrder::Asc)],
                    params: TableParams::default(),
                    flags: HashSet::new(),
                    extensions: HashMap::new(),
                    is_system: false,
                },
                &auth,
            )
            .expect("create table");

        schema
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
            local_cache_max_bytes: 1024 * 1024,
            local_disk_free_reserve_bytes: 0,
            flush_threshold_bytes: 4096,
            memtable_backpressure_bytes: u64::MAX,
            flush_max_age_secs: 5,
            data_dir: dir.to_path_buf(),
            index_backend: ferrosa_storage::index::IndexBackendConfig::Local,
            auth_enabled: false,
            auth_warn: false,
            max_pending_replay_mutations_without_schema: 1024,
            memtable_num_shards: 64,
            write_verify: false,
        }
    }

    /// Storage-layer schema for `ks.t`. The cell layout must match the
    /// `ferrosa_schema` metadata: PK `id` (UTF8), CK `ck` (Int32), regular
    /// `name` (UTF8) + `score` (Int32). Storage orders static+regular cells by
    /// Cassandra's column-name comparator, so `name` < `score` => indices 0, 1.
    fn storage_schema() -> ferrosa_common::schema::TableSchema {
        use ferrosa_common::schema::{ColumnDefinition, TableSchema};
        TableSchema {
            keyspace: "ks".to_string(),
            table: "t".to_string(),
            key_type: "org.apache.cassandra.db.marshal.UTF8Type".to_string(),
            clustering_columns: vec![ColumnDefinition {
                name: "ck".to_string(),
                type_name: "org.apache.cassandra.db.marshal.Int32Type".to_string(),
            }],
            static_columns: vec![],
            regular_columns: vec![
                ColumnDefinition {
                    name: "name".to_string(),
                    type_name: "org.apache.cassandra.db.marshal.UTF8Type".to_string(),
                },
                ColumnDefinition {
                    name: "score".to_string(),
                    type_name: "org.apache.cassandra.db.marshal.Int32Type".to_string(),
                },
            ],
            extensions: Default::default(),
        }
    }

    /// A storage `Row` for clustering value `ck`, regular `name` + `score`.
    /// Cell ordinals follow the column-name comparator (name=0, score=1).
    fn storage_row(ck: i32, name: &str, score: i32, ts: i64) -> StorageRow {
        StorageRow {
            clustering: ck.to_be_bytes().to_vec(),
            cells: vec![
                (0, CellValue::live(name.as_bytes().to_vec(), ts)),
                (1, CellValue::live(score.to_be_bytes().to_vec(), ts)),
            ],
            deletion: DeletionTime::LIVE,
            primary_key_liveness: LivenessInfo::with_timestamp(ts),
        }
    }

    #[tokio::test]
    async fn load_table_materializes_rows_in_declared_order() {
        let dir = tempfile::tempdir().unwrap();
        let engine = StorageEngine::new(engine_config(dir.path()), None).unwrap();
        engine.register_table(storage_schema()).unwrap();

        let schema = schema_with_table();
        let tid = TableId::new("ks", "t");

        // Two partitions, the first with two clustering rows.
        let key_a = DecoratedKey::new(PartitionKey::new(b"alpha".to_vec()));
        let key_b = DecoratedKey::new(PartitionKey::new(b"beta".to_vec()));
        engine
            .write(&tid, &key_a, storage_row(1, "ann", 10, 1000), 1000)
            .unwrap();
        engine
            .write(&tid, &key_a, storage_row(2, "amy", 20, 1001), 1001)
            .unwrap();
        engine
            .write(&tid, &key_b, storage_row(1, "bob", 30, 1002), 1002)
            .unwrap();

        let table = load_table(&engine, &schema, "ks", "t")
            .await
            .expect("load succeeds");

        // Schema is in declared order: id, ck, name, score.
        let cols: Vec<&str> = table
            .schema()
            .columns
            .iter()
            .map(|c| c.name.as_str())
            .collect();
        assert_eq!(cols, vec!["id", "ck", "name", "score"]);
        assert_eq!(table.schema().columns[0].ty, ColumnType::Text); // id text
        assert_eq!(table.schema().columns[1].ty, ColumnType::Int); // ck int
        assert_eq!(table.schema().columns[3].ty, ColumnType::Int); // score int

        let mut rows: Vec<Row> = table.scan().collect();
        assert_eq!(rows.len(), 3, "two partitions, three logical rows");

        // Sort for deterministic assertions: by (id, ck).
        rows.sort_by(|a, b| {
            let (Value::Text(ia), Value::Text(ib)) = (a.get(0), b.get(0)) else {
                panic!("id should be text");
            };
            let (Value::Int(ca), Value::Int(cb)) = (a.get(1), b.get(1)) else {
                panic!("ck should be int");
            };
            ia.cmp(ib).then(ca.cmp(cb))
        });

        // alpha/ck=1 -> ann/10
        assert_eq!(rows[0].0[0], Value::Text("alpha".to_string()));
        assert_eq!(rows[0].0[1], Value::Int(1));
        assert_eq!(rows[0].0[2], Value::Text("ann".to_string()));
        assert_eq!(rows[0].0[3], Value::Int(10));
        // alpha/ck=2 -> amy/20
        assert_eq!(rows[1].0[1], Value::Int(2));
        assert_eq!(rows[1].0[2], Value::Text("amy".to_string()));
        assert_eq!(rows[1].0[3], Value::Int(20));
        // beta/ck=1 -> bob/30
        assert_eq!(rows[2].0[0], Value::Text("beta".to_string()));
        assert_eq!(rows[2].0[2], Value::Text("bob".to_string()));
        assert_eq!(rows[2].0[3], Value::Int(30));

        engine.shutdown().unwrap();
    }

    #[tokio::test]
    async fn load_table_missing_table_is_no_such_table() {
        let dir = tempfile::tempdir().unwrap();
        let engine = StorageEngine::new(engine_config(dir.path()), None).unwrap();
        let schema = schema_with_table();

        // "ghost" is not in the schema snapshot -> NoSuchTable, NOT empty Ok.
        let err = load_table(&engine, &schema, "ks", "ghost")
            .await
            .expect_err("missing table must error");
        match err {
            LoadError::NoSuchTable { keyspace, table } => {
                assert_eq!(keyspace, "ks");
                assert_eq!(table, "ghost");
            }
            other => panic!("expected NoSuchTable, got {other:?}"),
        }

        engine.shutdown().unwrap();
    }

    #[tokio::test]
    async fn load_table_existing_empty_table_is_ok_zero_rows() {
        let dir = tempfile::tempdir().unwrap();
        let engine = StorageEngine::new(engine_config(dir.path()), None).unwrap();
        engine.register_table(storage_schema()).unwrap();
        let schema = schema_with_table();

        // Registered + declared, but no rows written: distinct from NoSuchTable.
        let table = load_table(&engine, &schema, "ks", "t")
            .await
            .expect("existing empty table loads ok");
        assert_eq!(table.scan().count(), 0, "empty table yields zero rows");
        assert_eq!(table.schema().width(), 4, "schema still has all 4 columns");

        engine.shutdown().unwrap();
    }
}
