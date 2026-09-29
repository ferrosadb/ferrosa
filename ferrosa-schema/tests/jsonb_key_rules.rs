//! T-154a: jsonb is refused in key positions and as set element, map key and
//! vector element, on every schema entry point (D3, D21; FMEA SCH-T154a-*).

use std::collections::{HashMap, HashSet};

use ferrosa_common::CqlType;
use ferrosa_schema::*;
use indexmap::IndexMap;

fn schema() -> Schema {
    Schema::new(SchemaConfig {
        hasher: PasswordHasher::Bcrypt { cost: 4 },
        password_policy: PasswordPolicy::permissive(),
        auth_method: AuthMethod::Password,
        rate_limit: RateLimitConfig::default(),
        audit_sink: Box::new(TestAuditSink::new()),
        secrets: Box::new(EnvSecretsProvider),
        mode: DeploymentMode::Development,
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
        clustering_order: if kind == ColumnKind::Clustering {
            ClusteringOrder::Asc
        } else {
            ClusteringOrder::None
        },
        mask: None,
    }
}

/// A table `t_ks.name` with partition key `pk: pk_type`, an optional
/// clustering key `ck: ck_type` and a regular column `v: v_type`.
fn table(name: &str, pk_type: &str, ck_type: Option<&str>, v_type: &str) -> TableMetadata {
    let mut columns = IndexMap::new();
    columns.insert(
        "pk".to_string(),
        col("pk", ColumnKind::PartitionKey, pk_type),
    );
    let mut clustering_key = vec![];
    if let Some(t) = ck_type {
        columns.insert("ck".to_string(), col("ck", ColumnKind::Clustering, t));
        clustering_key.push(("ck".to_string(), ClusteringOrder::Asc));
    }
    columns.insert("v".to_string(), col("v", ColumnKind::Regular, v_type));
    TableMetadata {
        keyspace: "t_ks".to_string(),
        name: name.to_string(),
        id: uuid::Uuid::new_v4(),
        columns,
        partition_key: vec!["pk".to_string()],
        clustering_key,
        params: TableParams::default(),
        flags: HashSet::new(),
        extensions: HashMap::new(),
        is_system: false,
    }
}

fn schema_with_ks() -> Schema {
    let s = schema();
    s.create_keyspace(ks("t_ks"), &su()).expect("keyspace");
    s
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

fn assert_in_key(err: SchemaError, column: &str, position: &str) {
    match err {
        SchemaError::JsonbInKey {
            column: c,
            position: p,
            ..
        } => {
            assert_eq!(c, column);
            assert_eq!(p, position);
        }
        other => panic!("expected JsonbInKey, got {other:?}"),
    }
}

#[test]
fn create_table_refuses_jsonb_partition_key() {
    let s = schema_with_ks();
    let err = s
        .create_table(table("a", "jsonb", None, "int"), &su())
        .expect_err("refused");
    assert!(err.to_string().contains("'pk'"), "names the column: {err}");
    assert_in_key(err, "pk", "partition key");
}

#[test]
fn create_table_internal_refuses_jsonb_clustering_key() {
    let s = schema_with_ks();
    let err = s
        .create_table_internal(table("a", "int", Some("jsonb"), "int"))
        .expect_err("refused");
    assert_in_key(err, "ck", "clustering key");
}

#[test]
fn regular_jsonb_column_is_accepted() {
    let s = schema_with_ks();
    s.create_table(table("a", "int", None, "jsonb"), &su())
        .expect("jsonb is fine in a regular column");
    s.create_table_internal(table("b", "int", None, "list<jsonb>"))
        .expect("list<jsonb> is fine in a regular column");
}

#[test]
fn jsonb_nested_in_key_is_refused() {
    let s = schema_with_ks();
    s.create_type_internal(&udt("inner_t", vec![("j", CqlType::Jsonb)]))
        .expect("inner");
    s.create_type_internal(&udt(
        "outer_t",
        vec![(
            "i",
            CqlType::Udt {
                keyspace: "t_ks".into(),
                name: "inner_t".into(),
                fields: vec![("j".into(), CqlType::Jsonb)],
            },
        )],
    ))
    .expect("outer");
    for (i, ty) in [
        "frozen<list<jsonb>>",
        "frozen<tuple<int, jsonb>>",
        "frozen<inner_t>",
        "frozen<outer_t>",
        "frozen<map<text, jsonb>>",
    ]
    .iter()
    .enumerate()
    {
        let pk = s.create_table_internal(table(&format!("p{i}"), ty, None, "int"));
        assert_in_key(pk.expect_err(ty), "pk", "partition key");
        let ck = s.create_table_internal(table(&format!("c{i}"), "int", Some(ty), "int"));
        assert_in_key(ck.expect_err(ty), "ck", "clustering key");
    }
}

#[test]
fn set_map_key_and_vector_of_jsonb_rejected_on_every_path() {
    let s = schema_with_ks();
    for (ty, rule) in [
        ("set<jsonb>", "set<jsonb>"),
        ("map<jsonb, int>", "map<jsonb"),
        ("vector<jsonb, 3>", "vector<jsonb>"),
        ("list<frozen<set<jsonb>>>", "set<jsonb>"),
    ] {
        let create = s
            .create_table(table("n", "int", None, ty), &su())
            .expect_err(ty);
        assert!(
            matches!(create, SchemaError::JsonbNesting { .. }),
            "{create:?}"
        );
        assert!(create.to_string().contains("'v'"));
        assert!(create.to_string().contains(rule), "{create}");
        let internal = s
            .create_table_internal(table("n", "int", None, ty))
            .expect_err(ty);
        assert!(matches!(internal, SchemaError::JsonbNesting { .. }));
    }
}

fn add(ty: &str, kind: ColumnKind) -> TableUpdates {
    TableUpdates {
        params: None,
        add_columns: vec![col("added", kind, ty)],
        drop_columns: vec![],
        extensions: None,
    }
}

#[test]
fn alter_table_add_refuses_forbidden_jsonb_and_leaves_table_unchanged() {
    let s = schema_with_ks();
    s.create_table(table("a", "int", None, "int"), &su())
        .expect("create");
    let before = s.snapshot().tables.len();
    for ty in ["set<jsonb>", "map<jsonb, text>", "vector<jsonb, 2>"] {
        let e = s
            .alter_table("t_ks", "a", add(ty, ColumnKind::Regular), &su())
            .expect_err(ty);
        assert!(matches!(e, SchemaError::JsonbNesting { .. }), "{e:?}");
        let e = s
            .alter_table_internal("t_ks", "a", add(ty, ColumnKind::Regular))
            .expect_err(ty);
        assert!(matches!(e, SchemaError::JsonbNesting { .. }), "{e:?}");
    }
    // A key-kind column carrying jsonb cannot be added either.
    let e = s
        .alter_table_internal("t_ks", "a", add("jsonb", ColumnKind::Clustering))
        .expect_err("key jsonb");
    assert_in_key(e, "added", "clustering key");
    let snap = s.snapshot();
    let t = &snap.tables[&("t_ks".to_string(), "a".to_string())];
    assert!(
        !t.columns.contains_key("added"),
        "refused ALTER left no column"
    );
    assert_eq!(snap.tables.len(), before);
    s.alter_table("t_ks", "a", add("jsonb", ColumnKind::Regular), &su())
        .expect("regular jsonb add is fine");
}

#[test]
fn alter_type_add_jsonb_to_key_udt_is_refused() {
    let s = schema_with_ks();
    s.create_type_internal(&udt("k_t", vec![("a", CqlType::Int)]))
        .expect("type");
    s.create_table_internal(table("keyed", "frozen<k_t>", None, "int"))
        .expect("udt key without jsonb");
    let e = s
        .alter_type_add_field("t_ks", "k_t", "j", CqlType::Jsonb)
        .expect_err("refused");
    assert_in_key(e, "pk", "partition key");
    let t = s.get_type("t_ks", "k_t").expect("type still there");
    assert_eq!(t.fields.len(), 1, "refused ALTER TYPE changed nothing");
    // A UDT no key uses may gain a jsonb field.
    s.create_type_internal(&udt("free_t", vec![("a", CqlType::Int)]))
        .expect("type");
    s.alter_type_add_field("t_ks", "free_t", "j", CqlType::Jsonb)
        .expect("unkeyed udt may hold jsonb");
}

#[test]
fn snapshot_with_jsonb_key_is_refused_on_load_not_skipped() {
    let s = schema();
    let mut snapshot = SchemaSnapshot::default();
    snapshot.keyspaces.insert("t_ks".into(), ks("t_ks"));
    let good = table("good", "int", None, "jsonb");
    let bad = table("bad", "jsonb", None, "int");
    snapshot.tables.insert(("t_ks".into(), "good".into()), good);
    snapshot.tables.insert(("t_ks".into(), "bad".into()), bad);
    let err = s.apply_snapshot(snapshot).expect_err("refused loudly");
    assert_in_key(err, "pk", "partition key");
    let applied = s.snapshot();
    assert!(
        !applied.tables.keys().any(|(k, _)| k == "t_ks"),
        "a refused snapshot applies nothing, not even its valid tables"
    );
}

#[test]
fn snapshot_with_forbidden_nesting_is_refused_on_load() {
    let s = schema();
    let mut snapshot = SchemaSnapshot::default();
    snapshot.tables.insert(
        ("t_ks".into(), "n".into()),
        table("n", "int", None, "set<jsonb>"),
    );
    let err = s.apply_snapshot(snapshot).expect_err("refused");
    assert!(matches!(err, SchemaError::JsonbNesting { .. }), "{err:?}");
}

#[test]
fn unparseable_column_type_is_an_error_not_a_skip() {
    let s = schema_with_ks();
    let err = s
        .create_table_internal(table("u", "int", None, "list<"))
        .expect_err("refused");
    assert!(matches!(err, SchemaError::InvalidSchema(_)), "{err:?}");
}
