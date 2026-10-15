//! jsonb is a FIRST-CLASS data type (T-300 gate REMOVED).
//!
//! The engine already replicates graph materialization over Raft, so a column
//! type is not special, and a gate that can REFUSE a data type is itself the
//! defect. The former capability gate is therefore GONE, not disabled: jsonb DDL
//! is permitted in every `DeploymentMode` with no capability input, and a
//! snapshot carrying jsonb applies anywhere. The surviving constraint is the
//! placement rule (T-154a, D3, D21): jsonb is still refused in partition and
//! clustering key columns, as a set element, as a map key and as a vector
//! element — that rule is about key-byte ordering, not membership.

use std::collections::{HashMap, HashSet};

use ferrosa_common::deployment_mode::DeploymentMode;
use ferrosa_common::CqlType;
use ferrosa_schema::*;
use indexmap::IndexMap;

/// Every variant of `DeploymentMode` the engine can be in.
const ALL_MODES: [DeploymentMode; 6] = [
    DeploymentMode::Standalone,
    DeploymentMode::Pair,
    DeploymentMode::Forming,
    DeploymentMode::Cluster,
    DeploymentMode::DegradedPair,
    DeploymentMode::DegradedCluster,
];

fn schema() -> Schema {
    Schema::new(SchemaConfig {
        hasher: PasswordHasher::Bcrypt { cost: 4 },
        password_policy: PasswordPolicy::permissive(),
        auth_method: AuthMethod::Password,
        rate_limit: RateLimitConfig::default(),
        audit_sink: Box::new(TestAuditSink::new()),
        secrets: Box::new(EnvSecretsProvider),
        mode: ferrosa_schema::DeploymentMode::Development,
    })
    .expect("schema")
}

fn su() -> AuthContext {
    AuthContext {
        role: "cassandra".to_string(),
        is_superuser: true,
        must_change_password: false,
    }
}

fn ks(name: &str) -> KeyspaceMetadata {
    KeyspaceMetadata {
        name: name.to_string(),
        durable_writes: true,
        replication: ReplicationParams {
            strategy: "SimpleStrategy".to_string(),
            options: HashMap::from([("replication_factor".into(), "1".into())]),
        },
    }
}

fn col(name: &str, kind: ColumnKind, ty: &str) -> ColumnMetadata {
    ColumnMetadata {
        name: name.to_string(),
        kind,
        position: 0,
        column_type: ty.to_string(),
        clustering_order: ClusteringOrder::None,
        mask: None,
    }
}

/// A table `t_ks.<name>` with an `int` key `pk` and a regular column `v` of `v_type`.
fn table(name: &str, v_type: &str) -> TableMetadata {
    let mut columns = IndexMap::new();
    columns.insert("pk".to_string(), col("pk", ColumnKind::PartitionKey, "int"));
    columns.insert("v".to_string(), col("v", ColumnKind::Regular, v_type));
    TableMetadata {
        keyspace: "t_ks".to_string(),
        name: name.to_string(),
        id: uuid::Uuid::new_v4(),
        columns,
        partition_key: vec!["pk".to_string()],
        clustering_key: vec![],
        params: TableParams::default(),
        flags: HashSet::new(),
        extensions: HashMap::new(),
        is_system: false,
    }
}

fn udt(name: &str, fields: Vec<(&str, CqlType)>) -> UserTypeMetadata {
    UserTypeMetadata {
        keyspace: "t_ks".to_string(),
        name: name.to_string(),
        fields: fields
            .into_iter()
            .map(|(n, t)| (n.to_string(), t))
            .collect(),
    }
}

fn schema_in(mode: DeploymentMode) -> Schema {
    let s = schema();
    s.create_keyspace(ks("t_ks"), &su()).expect("keyspace");
    s.set_deployment_mode(mode);
    s
}

fn add_column(ty: &str) -> TableUpdates {
    TableUpdates {
        params: None,
        add_columns: vec![col("added", ColumnKind::Regular, ty)],
        drop_columns: vec![],
        extensions: None,
    }
}

/// A jsonb table is first class in EVERY deployment mode, through every DDL
/// entry point, with no capability input.
///
/// Before the gate was removed this failed in all five non-standalone modes: the
/// DDL gate refused jsonb outside standalone because it read an unpublished
/// (default-unknown) member capability view. The positive rule replaces the old
/// exhaustive gate table.
#[test]
fn jsonb_ddl_is_permitted_in_every_deployment_mode() {
    for mode in ALL_MODES {
        let s = schema_in(mode);
        // CREATE TABLE — entry check and apply, both the authed and the
        // replicated (internal) forms.
        s.check_create_table_jsonb(&table("a", "jsonb"))
            .unwrap_or_else(|e| panic!("{mode}: CREATE TABLE entry must be permitted: {e}"));
        s.create_table(table("a", "jsonb"), &su())
            .unwrap_or_else(|e| panic!("{mode}: CREATE TABLE apply must be permitted: {e}"));
        s.create_table_internal(table("a2", "list<jsonb>"))
            .unwrap_or_else(|e| panic!("{mode}: replicated CREATE TABLE must be permitted: {e}"));

        // ALTER TABLE ADD — entry check and apply, both forms.
        s.check_alter_table_jsonb("t_ks", "a", &add_column("jsonb"))
            .unwrap_or_else(|e| panic!("{mode}: ALTER TABLE entry must be permitted: {e}"));
        s.alter_table("t_ks", "a", add_column("jsonb"), &su())
            .unwrap_or_else(|e| panic!("{mode}: ALTER TABLE apply must be permitted: {e}"));
        s.alter_table_internal("t_ks", "a2", add_column("map<text, jsonb>"))
            .unwrap_or_else(|e| panic!("{mode}: replicated ALTER TABLE must be permitted: {e}"));

        // CREATE TYPE / ALTER TYPE ADD.
        s.create_type_internal(&udt("u", vec![("j", CqlType::Jsonb)]))
            .unwrap_or_else(|e| panic!("{mode}: CREATE TYPE must be permitted: {e}"));
        s.alter_type_add_field("t_ks", "u", "j2", CqlType::Jsonb)
            .unwrap_or_else(|e| panic!("{mode}: ALTER TYPE ADD must be permitted: {e}"));

        // A table typed by a UDT that nests jsonb is a jsonb table, permitted too.
        s.create_table_internal(table("over_udt", "frozen<u>"))
            .unwrap_or_else(|e| panic!("{mode}: a jsonb-UDT column must be permitted: {e}"));

        // The column really is there, with its type, rather than the statements
        // merely having returned Ok.
        let snap = s.snapshot();
        let t = &snap.tables[&("t_ks".to_string(), "a".to_string())];
        assert_eq!(
            t.columns["v"].column_type, "jsonb",
            "{mode}: applied column"
        );
    }
}

/// A snapshot carrying jsonb applies in every deployment mode, installs the
/// table, and adopts its version.
#[test]
fn a_snapshot_carrying_jsonb_applies_in_every_deployment_mode() {
    for mode in ALL_MODES {
        let s = schema();
        s.set_deployment_mode(mode);
        let mut snapshot = SchemaSnapshot {
            version: uuid::Uuid::new_v4(),
            ..SchemaSnapshot::default()
        };
        snapshot.keyspaces.insert("t_ks".into(), ks("t_ks"));
        snapshot
            .tables
            .insert(("t_ks".into(), "ok".into()), table("ok", "int"));
        snapshot
            .tables
            .insert(("t_ks".into(), "j".into()), table("j", "jsonb"));
        s.apply_snapshot(snapshot.clone())
            .unwrap_or_else(|e| panic!("{mode}: a jsonb snapshot must apply: {e}"));
        let snap = s.snapshot();
        assert!(
            snap.tables.contains_key(&("t_ks".into(), "j".into())),
            "{mode}: the jsonb table must be installed"
        );
        assert_eq!(snap.version, snapshot.version, "{mode}: version adopted");
    }
}

/// CONTROL — the surviving placement rule is unchanged: jsonb in a partition
/// key column is still refused, in every mode.
#[test]
fn jsonb_in_a_partition_key_is_still_refused_in_every_mode() {
    for mode in ALL_MODES {
        let s = schema_in(mode);
        let mut columns = IndexMap::new();
        columns.insert(
            "pk".to_string(),
            col("pk", ColumnKind::PartitionKey, "jsonb"),
        );
        columns.insert("v".to_string(), col("v", ColumnKind::Regular, "int"));
        let bad = TableMetadata {
            keyspace: "t_ks".to_string(),
            name: "bad".to_string(),
            id: uuid::Uuid::new_v4(),
            columns,
            partition_key: vec!["pk".to_string()],
            clustering_key: vec![],
            params: TableParams::default(),
            flags: HashSet::new(),
            extensions: HashMap::new(),
            is_system: false,
        };
        let e = s
            .create_table_internal(bad)
            .expect_err("jsonb in a partition key is refused in every mode");
        assert!(
            matches!(e, SchemaError::JsonbInKey { .. }),
            "{mode}: expected JsonbInKey, got {e:?}"
        );
    }
}

/// CONTROL — the D21 nesting rule is unchanged: jsonb as a set element, a map
/// key and a vector element is still refused, in every mode.
#[test]
fn jsonb_in_forbidden_nestings_is_still_refused_in_every_mode() {
    for mode in ALL_MODES {
        let s = schema_in(mode);
        for (ty, name) in [
            ("set<jsonb>", "set_elem"),
            ("map<jsonb, text>", "map_key"),
            ("vector<jsonb, 3>", "vector_elem"),
            ("frozen<list<set<jsonb>>>", "deep_set_elem"),
        ] {
            let e = s
                .create_table_internal(table(name, ty))
                .expect_err("a forbidden nesting is refused");
            assert!(
                matches!(e, SchemaError::JsonbNesting { .. }),
                "{mode}: expected JsonbNesting for {ty}, got {e:?}"
            );
        }
    }
}
