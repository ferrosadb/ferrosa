//! `pg_catalog` virtual-table projections for the Postgres front-end.
//!
//! Postgres drivers (psql `\d`, JDBC/ODBC, ORMs) introspect the database by
//! querying `pg_catalog.pg_namespace` / `pg_class` / `pg_attribute` / `pg_type`.
//! This module projects those catalog shapes from the *live* `ferrosa-schema`
//! metadata into [`ferrosa_sql::InMemoryTable`]s, so the bespoke relational
//! engine's scan operators (decision D3) can read them with no special-casing.
//!
//! Why here and not in `ferrosa-sql`: the catalog *shapes* (column names,
//! relkind codes) and Postgres **type OIDs** are Postgres-specific. Keeping
//! them in `ferrosa-postgres` leaves `ferrosa-sql` a pure relational engine.
//!
//! Namespace model (D5/D8): a ferrosa keyspace is a Postgres schema; tables
//! live in keyspaces; columns have types. We additionally expose the two
//! reserved Postgres schemas `pg_catalog` and `information_schema` so drivers
//! that resolve them by name do not fault.
//!
//! ## OID scheme
//!
//! Real Postgres uses an `oid` (unsigned 32-bit) type. The first-slice
//! [`ferrosa_sql::Value`] has no unsigned variant, so OIDs are represented as
//! `Value::Int(i64)` here — every OID we mint fits in `u32`, so the widening to
//! `i64` is lossless. OIDs are assigned **deterministically** by a stable
//! FNV-1a hash of a kind-prefixed natural key, folded into the user-OID range
//! `[16384, u32::MAX]` (Postgres reserves `< 16384` for built-ins, so type
//! OIDs like 23/25 never collide with a synthetic namespace/relation OID). The
//! scheme is pure (no counters, no insertion-order dependence), so the same
//! schema always projects the same OIDs.

use ferrosa_schema::{ColumnMetadata, Schema, SchemaSnapshot};
use ferrosa_sql::{Column, ColumnType, InMemoryTable, RelSchema, Row, Value};

/// First OID Postgres hands out to user objects. Everything below is reserved
/// for built-in catalog entries (the fixed type OIDs in [`type_oid`] live here).
const FIRST_USER_OID: u32 = 16_384;

/// Map a ferrosa/CQL column type name to its Postgres type OID.
///
/// The input is the CQL type string as stored in `ColumnMetadata::column_type`
/// (e.g. `"text"`, `"int"`, `"frozen<map<text, text>>"`). Matching is
/// case-insensitive and strips a leading `frozen<...>` wrapper so collection /
/// frozen types resolve to their inner head type. Unknown types fall back to
/// `25` (text) — the most permissive textual representation — which is the
/// documented default rather than a silent panic.
pub fn type_oid(column_type: &str) -> u32 {
    let normalized = normalize_type_name(column_type);
    match normalized.as_str() {
        "text" | "varchar" | "ascii" => 25,             // text
        "int" | "int32" | "smallint" | "tinyint" => 23, // int4
        "bigint" | "counter" | "long" => 20,            // int8
        "boolean" | "bool" => 16,                       // bool
        "uuid" | "timeuuid" => 2950,                    // uuid
        "float" => 700,                                 // float4
        "double" => 701,                                // float8
        "blob" | "bytes" => 17,                         // bytea
        "timestamp" | "datetime" => 1114,               // timestamp (without tz)
        "date" => 1082,                                 // date
        "time" => 1083,                                 // time (without tz)
        "inet" => 869,                                  // inet
        "decimal" | "varint" => 1700,                   // numeric
        // Unknown / unsupported CQL types (collections, UDTs, …) are surfaced to
        // drivers as `text` rather than dropped. Widen this map as the engine
        // grows real support for the underlying types.
        _ => 25,
    }
}

/// Lower-case a CQL type name and strip an outer `frozen<...>` wrapper, taking
/// the head identifier before any `<` (so `map<text,text>` -> `map`).
fn normalize_type_name(column_type: &str) -> String {
    let trimmed = column_type.trim();
    let lower = trimmed.to_ascii_lowercase();
    let unwrapped = lower
        .strip_prefix("frozen<")
        .and_then(|rest| rest.strip_suffix('>'))
        .unwrap_or(&lower);
    let head = unwrapped.split('<').next().unwrap_or(unwrapped);
    head.trim().to_string()
}

/// Deterministic synthetic OID for a named object of a given kind.
///
/// FNV-1a over `"{kind}:{name}"`, folded into `[FIRST_USER_OID, u32::MAX]`. The
/// `kind` prefix keeps a namespace and a same-named relation from colliding.
fn synthetic_oid(kind: &str, name: &str) -> u32 {
    const FNV_OFFSET: u64 = 0xcbf2_9ce4_8422_2325;
    const FNV_PRIME: u64 = 0x0000_0100_0000_01b3;
    let mut hash = FNV_OFFSET;
    for byte in kind
        .bytes()
        .chain(std::iter::once(b':'))
        .chain(name.bytes())
    {
        hash ^= u64::from(byte);
        hash = hash.wrapping_mul(FNV_PRIME);
    }
    let span = u64::from(u32::MAX - FIRST_USER_OID);
    FIRST_USER_OID + (hash % span) as u32
}

/// OID of the namespace (Postgres schema) for a keyspace name.
fn namespace_oid(keyspace: &str) -> u32 {
    synthetic_oid("namespace", keyspace)
}

/// OID of the relation (table) `keyspace.table`.
fn relation_oid(keyspace: &str, table: &str) -> u32 {
    synthetic_oid("relation", &format!("{keyspace}.{table}"))
}

/// OID column value — see the module-level OID-scheme note on the `Int` choice.
fn oid_val(oid: u32) -> Value {
    Value::Int(i64::from(oid))
}

/// Keyspace names from the snapshot, sorted for deterministic row order.
fn sorted_keyspaces(snapshot: &SchemaSnapshot) -> Vec<String> {
    let mut names: Vec<String> = snapshot.keyspaces.keys().cloned().collect();
    names.sort();
    names
}

/// `(keyspace, table)` pairs from the snapshot, sorted for deterministic order.
fn sorted_tables(snapshot: &SchemaSnapshot) -> Vec<(String, String)> {
    let mut keys: Vec<(String, String)> = snapshot.tables.keys().cloned().collect();
    keys.sort();
    keys
}

/// Reserved Postgres schemas always present alongside the user keyspaces.
const RESERVED_NAMESPACES: [&str; 2] = ["pg_catalog", "information_schema"];

/// `pg_catalog.pg_namespace` — one row per keyspace plus the reserved schemas.
///
/// Columns: `oid` (synthetic namespace OID, as `Int`), `nspname` (schema name).
pub fn pg_namespace(schema: &Schema) -> InMemoryTable {
    let snapshot = schema.snapshot();
    let rel_schema = RelSchema::new(vec![
        Column::new("oid", ColumnType::Int),
        Column::new("nspname", ColumnType::Text),
    ]);

    let mut rows: Vec<Row> = RESERVED_NAMESPACES
        .iter()
        .map(|ns| {
            Row::new(vec![
                oid_val(namespace_oid(ns)),
                Value::Text((*ns).to_string()),
            ])
        })
        .collect();

    for ks in sorted_keyspaces(&snapshot) {
        rows.push(Row::new(vec![oid_val(namespace_oid(&ks)), Value::Text(ks)]));
    }

    InMemoryTable::new(rel_schema, rows)
}

/// `pg_catalog.pg_class` — one row per table.
///
/// Columns: `oid` (relation OID), `relname` (table name), `relnamespace`
/// (owning keyspace's namespace OID), `relkind` (`'r'` ordinary table).
pub fn pg_class(schema: &Schema) -> InMemoryTable {
    let snapshot = schema.snapshot();
    let rel_schema = RelSchema::new(vec![
        Column::new("oid", ColumnType::Int),
        Column::new("relname", ColumnType::Text),
        Column::new("relnamespace", ColumnType::Int),
        Column::new("relkind", ColumnType::Text),
    ]);

    let rows: Vec<Row> = sorted_tables(&snapshot)
        .into_iter()
        .map(|(ks, table)| {
            Row::new(vec![
                oid_val(relation_oid(&ks, &table)),
                Value::Text(table),
                oid_val(namespace_oid(&ks)),
                Value::Text("r".to_string()),
            ])
        })
        .collect();

    InMemoryTable::new(rel_schema, rows)
}

/// `pg_catalog.pg_attribute` — one row per column per table.
///
/// Columns: `attrelid` (owning relation OID), `attname` (column name),
/// `atttypid` (Postgres type OID via [`type_oid`]), `attnum` (1-based ordinal
/// in the table's column order).
pub fn pg_attribute(schema: &Schema) -> InMemoryTable {
    let snapshot = schema.snapshot();
    let rel_schema = RelSchema::new(vec![
        Column::new("attrelid", ColumnType::Int),
        Column::new("attname", ColumnType::Text),
        Column::new("atttypid", ColumnType::Int),
        Column::new("attnum", ColumnType::Int),
    ]);

    let mut rows: Vec<Row> = Vec::new();
    for (ks, table) in sorted_tables(&snapshot) {
        let Some(meta) = snapshot.tables.get(&(ks.clone(), table.clone())) else {
            continue;
        };
        let relid = relation_oid(&ks, &table);
        // IndexMap preserves the table's declared column order; attnum is the
        // 1-based position in that order, matching Postgres semantics.
        for (idx, col) in meta.columns.values().enumerate() {
            rows.push(attribute_row(relid, col, idx));
        }
    }

    InMemoryTable::new(rel_schema, rows)
}

/// Build a single `pg_attribute` row for `col` at 0-based `idx`.
fn attribute_row(relid: u32, col: &ColumnMetadata, idx: usize) -> Row {
    let attnum = i64::try_from(idx + 1).unwrap_or(i64::MAX);
    Row::new(vec![
        oid_val(relid),
        Value::Text(col.name.clone()),
        oid_val(type_oid(&col.column_type)),
        Value::Int(attnum),
    ])
}

/// `pg_catalog.pg_type` — one row per distinct Postgres type actually used by
/// the projected columns.
///
/// Columns: `oid` (Postgres type OID), `typname` (canonical type name).
pub fn pg_type(schema: &Schema) -> InMemoryTable {
    let snapshot = schema.snapshot();
    let rel_schema = RelSchema::new(vec![
        Column::new("oid", ColumnType::Int),
        Column::new("typname", ColumnType::Text),
    ]);

    // Collect the distinct OIDs actually referenced by columns, sorted for a
    // deterministic projection.
    let mut oids: Vec<u32> = snapshot
        .tables
        .values()
        .flat_map(|t| t.columns.values())
        .map(|c| type_oid(&c.column_type))
        .collect();
    oids.sort_unstable();
    oids.dedup();

    let rows: Vec<Row> = oids
        .into_iter()
        .map(|oid| Row::new(vec![oid_val(oid), Value::Text(type_name(oid).to_string())]))
        .collect();

    InMemoryTable::new(rel_schema, rows)
}

/// Canonical Postgres `typname` for an OID we mint (inverse of [`type_oid`]).
fn type_name(oid: u32) -> &'static str {
    match oid {
        25 => "text",
        23 => "int4",
        20 => "int8",
        16 => "bool",
        2950 => "uuid",
        700 => "float4",
        701 => "float8",
        17 => "bytea",
        1114 => "timestamp",
        1082 => "date",
        1083 => "time",
        869 => "inet",
        1700 => "numeric",
        _ => "text",
    }
}

/// All catalog tables, keyed by their `pg_catalog` relation name, so a future
/// query path can resolve `pg_catalog.<name>` to a [`ferrosa_sql::TableProvider`].
pub fn catalog_tables(schema: &Schema) -> Vec<(String, InMemoryTable)> {
    vec![
        ("pg_namespace".to_string(), pg_namespace(schema)),
        ("pg_class".to_string(), pg_class(schema)),
        ("pg_attribute".to_string(), pg_attribute(schema)),
        ("pg_type".to_string(), pg_type(schema)),
    ]
}

#[cfg(test)]
mod tests {
    use super::*;
    use ferrosa_schema::{
        AuthContext, AuthMethod, ClusteringOrder, ColumnKind, ColumnMetadata, DeploymentMode,
        EnvSecretsProvider, KeyspaceMetadata, PasswordHasher, PasswordPolicy, RateLimitConfig,
        ReplicationParams, Schema, SchemaConfig, TableMetadata, TableParams, TestAuditSink,
    };
    use ferrosa_sql::{TableProvider, Value};
    use indexmap::IndexMap;
    use std::collections::{HashMap, HashSet};
    use uuid::Uuid;

    fn test_config() -> SchemaConfig {
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

    /// Build a schema with keyspace `ks` and table `tbl(id int PK, name text)`
    /// through the public DDL API (so the projection reads the same metadata a
    /// real CREATE TABLE would produce).
    fn schema_with_ks_tbl() -> Schema {
        let schema = Schema::new(test_config()).expect("schema bootstraps");
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
            .expect("create keyspace ks");

        let mut columns = IndexMap::new();
        columns.insert(
            "id".to_string(),
            column("id", ColumnKind::PartitionKey, "int"),
        );
        columns.insert(
            "name".to_string(),
            column("name", ColumnKind::Regular, "text"),
        );

        schema
            .create_table(
                TableMetadata {
                    keyspace: "ks".to_string(),
                    name: "tbl".to_string(),
                    id: Uuid::new_v4(),
                    columns,
                    partition_key: vec!["id".to_string()],
                    clustering_key: vec![],
                    params: TableParams::default(),
                    flags: HashSet::new(),
                    extensions: HashMap::new(),
                    is_system: false,
                },
                &auth,
            )
            .expect("create table tbl");

        schema
    }

    /// Collect a provider's rows into a Vec for assertions.
    fn rows_of(table: &InMemoryTable) -> Vec<Row> {
        table.scan().collect()
    }

    #[test]
    fn type_oid_maps_core_types() {
        assert_eq!(type_oid("text"), 25);
        assert_eq!(type_oid("varchar"), 25);
        assert_eq!(type_oid("ascii"), 25);
        assert_eq!(type_oid("int"), 23);
        assert_eq!(type_oid("int32"), 23);
        assert_eq!(type_oid("bigint"), 20);
        assert_eq!(type_oid("counter"), 20);
        assert_eq!(type_oid("boolean"), 16);
        assert_eq!(type_oid("uuid"), 2950);
        assert_eq!(type_oid("timeuuid"), 2950);
        assert_eq!(type_oid("float"), 700);
        assert_eq!(type_oid("double"), 701);
        assert_eq!(type_oid("blob"), 17);
        assert_eq!(type_oid("bytes"), 17);
        assert_eq!(type_oid("timestamp"), 1114);
        assert_eq!(type_oid("date"), 1082);
        assert_eq!(type_oid("time"), 1083);
        assert_eq!(type_oid("inet"), 869);
        assert_eq!(type_oid("decimal"), 1700);
        assert_eq!(type_oid("varint"), 1700);
    }

    #[test]
    fn type_oid_is_case_insensitive_and_unwraps_frozen() {
        assert_eq!(type_oid("TEXT"), 25);
        assert_eq!(type_oid("Int"), 23);
        assert_eq!(type_oid("frozen<int>"), 23);
        // Collections / UDTs fall back to text (documented default).
        assert_eq!(type_oid("map<text, text>"), 25);
        assert_eq!(type_oid("some_udt"), 25);
    }

    #[test]
    fn pg_namespace_contains_keyspace_and_reserved_schemas() {
        let schema = schema_with_ks_tbl();
        let table = pg_namespace(&schema);
        let rows = rows_of(&table);

        let names: Vec<&str> = rows
            .iter()
            .filter_map(|r| match r.get(1) {
                Value::Text(s) => Some(s.as_str()),
                _ => None,
            })
            .collect();

        assert!(names.contains(&"ks"), "namespaces: {names:?}");
        assert!(names.contains(&"pg_catalog"));
        assert!(names.contains(&"information_schema"));

        // The ks row carries its synthetic namespace OID.
        let ks_oid = rows
            .iter()
            .find(|r| matches!(r.get(1), Value::Text(s) if s == "ks"))
            .map(|r| r.get(0).clone())
            .expect("ks namespace row present");
        assert_eq!(ks_oid, oid_val(namespace_oid("ks")));
    }

    #[test]
    fn pg_class_lists_table_with_namespace_oid_and_relkind() {
        let schema = schema_with_ks_tbl();
        let table = pg_class(&schema);
        let rows = rows_of(&table);

        let tbl_row = rows
            .iter()
            .find(|r| matches!(r.get(1), Value::Text(s) if s == "tbl"))
            .expect("tbl row present in pg_class");

        // relnamespace == ks's namespace oid
        assert_eq!(tbl_row.get(2).clone(), oid_val(namespace_oid("ks")));
        // relkind == 'r'
        assert_eq!(tbl_row.get(3).clone(), Value::Text("r".to_string()));
        // oid == relation oid
        assert_eq!(tbl_row.get(0).clone(), oid_val(relation_oid("ks", "tbl")));
    }

    #[test]
    fn pg_attribute_lists_columns_with_ordinals_and_type_oids() {
        let schema = schema_with_ks_tbl();
        let table = pg_attribute(&schema);
        let rows = rows_of(&table);

        let relid = relation_oid("ks", "tbl");

        let id_row = rows
            .iter()
            .find(|r| matches!(r.get(1), Value::Text(s) if s == "id"))
            .expect("id attribute present");
        assert_eq!(id_row.get(0).clone(), oid_val(relid)); // attrelid
        assert_eq!(id_row.get(2).clone(), Value::Int(23)); // atttypid int4
        assert_eq!(id_row.get(3).clone(), Value::Int(1)); // attnum 1-based

        let name_row = rows
            .iter()
            .find(|r| matches!(r.get(1), Value::Text(s) if s == "name"))
            .expect("name attribute present");
        assert_eq!(name_row.get(0).clone(), oid_val(relid));
        assert_eq!(name_row.get(2).clone(), Value::Int(25)); // atttypid text
        assert_eq!(name_row.get(3).clone(), Value::Int(2)); // attnum 2
    }

    #[test]
    fn pg_type_maps_used_types() {
        let schema = schema_with_ks_tbl();
        let table = pg_type(&schema);
        let rows = rows_of(&table);

        let pairs: Vec<(i64, &str)> = rows
            .iter()
            .filter_map(|r| match (r.get(0), r.get(1)) {
                (Value::Int(oid), Value::Text(name)) => Some((*oid, name.as_str())),
                _ => None,
            })
            .collect();

        assert!(pairs.contains(&(23, "int4")), "pg_type rows: {pairs:?}");
        assert!(pairs.contains(&(25, "text")), "pg_type rows: {pairs:?}");
    }

    #[test]
    fn catalog_tables_exposes_all_four_relations() {
        let schema = schema_with_ks_tbl();
        let tables = catalog_tables(&schema);
        let names: Vec<&str> = tables.iter().map(|(n, _)| n.as_str()).collect();
        assert_eq!(
            names,
            vec!["pg_namespace", "pg_class", "pg_attribute", "pg_type"]
        );
    }

    #[test]
    fn projections_are_deterministic() {
        // Same schema content projects identical OIDs on every build (pure hash,
        // no counters / insertion-order dependence).
        let a = pg_class(&schema_with_ks_tbl());
        let b = pg_class(&schema_with_ks_tbl());
        assert_eq!(rows_of(&a), rows_of(&b));
    }
}
