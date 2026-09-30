//! T-300: jsonb DDL is allowed on a standalone node only, until the D15a
//! capability ledger lands (D24; FMEA SCH-T300-*). The gate is checked at DDL
//! entry and again at apply, including snapshot apply.

use std::collections::{HashMap, HashSet};

use ferrosa_common::deployment_mode::DeploymentMode;
use ferrosa_common::CqlType;
use ferrosa_schema::jsonb_rules::{check_jsonb_ddl_allowed, jsonb_ddl_refused_total, TypeMap};
use ferrosa_schema::*;
use indexmap::IndexMap;

const NON_STANDALONE: [DeploymentMode; 5] = [
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

fn assert_refused(err: SchemaError, mode: DeploymentMode) {
    match err {
        SchemaError::JsonbDdlRefused { mode: m, .. } => assert_eq!(m, mode),
        other => panic!("expected JsonbDdlRefused in {mode}, got {other:?}"),
    }
}

#[test]
fn standalone_allows_jsonb_create_alter_and_type() {
    let s = schema_in(DeploymentMode::Standalone);
    s.check_create_table_jsonb(&table("a", "jsonb"))
        .expect("entry");
    s.create_table(table("a", "jsonb"), &su()).expect("create");
    s.alter_table("t_ks", "a", add_column("jsonb"), &su())
        .expect("alter add");
    s.create_type_internal(&udt("u", vec![("j", CqlType::Jsonb)]))
        .expect("udt with jsonb field");
    s.alter_type_add_field("t_ks", "u", "j2", CqlType::Jsonb)
        .expect("alter type add");
    s.create_table_internal(table("b", "frozen<u>"))
        .expect("table over a jsonb udt");
}

#[test]
fn jsonb_schema_change_refused_at_apply_outside_standalone() {
    for mode in NON_STANDALONE {
        let s = schema_in(mode);
        s.create_table_internal(table("plain", "int"))
            .expect("non-jsonb DDL is unaffected");
        let before = jsonb_ddl_refused_total(mode);

        // Entry.
        let e = s
            .check_create_table_jsonb(&table("a", "jsonb"))
            .expect_err("entry refused");
        assert_refused(e, mode);
        // Apply, both the authed and the replicated (internal) forms.
        let e = s
            .create_table(table("a", "jsonb"), &su())
            .expect_err("apply");
        assert_refused(e, mode);
        let e = s
            .create_table_internal(table("a", "list<jsonb>"))
            .expect_err("replicated apply");
        assert_refused(e, mode);
        // ALTER TABLE ADD at entry and both apply forms.
        let e = s
            .check_alter_table_jsonb("t_ks", "plain", &add_column("jsonb"))
            .expect_err("alter entry");
        assert_refused(e, mode);
        let e = s
            .alter_table("t_ks", "plain", add_column("jsonb"), &su())
            .expect_err("alter apply");
        assert_refused(e, mode);
        let e = s
            .alter_table_internal("t_ks", "plain", add_column("map<text, jsonb>"))
            .expect_err("alter replicated apply");
        assert_refused(e, mode);

        let snap = s.snapshot();
        assert!(!snap.tables.contains_key(&("t_ks".into(), "a".into())));
        let plain = &snap.tables[&("t_ks".to_string(), "plain".to_string())];
        assert!(!plain.columns.contains_key("added"), "no column added");
        assert!(
            jsonb_ddl_refused_total(mode) >= before + 6,
            "counter increments per refusal in {mode}"
        );
    }
}

#[test]
fn nested_jsonb_in_a_udt_is_refused_outside_standalone() {
    for mode in NON_STANDALONE {
        let s = schema_in(DeploymentMode::Standalone);
        s.create_type_internal(&udt("has_j", vec![("j", CqlType::Jsonb)]))
            .expect("created while standalone");
        s.create_type_internal(&udt("clean", vec![("a", CqlType::Int)]))
            .expect("clean type");
        s.set_deployment_mode(mode);

        let fields = vec![("j".to_string(), CqlType::Jsonb)];
        let e = s
            .check_create_type_jsonb("t_ks", "new_t", &fields)
            .expect_err("entry");
        assert_refused(e, mode);
        let e = s
            .create_type_internal(&udt(
                "new_t",
                vec![("l", CqlType::List(Box::new(CqlType::Jsonb)))],
            ))
            .expect_err("create type apply");
        assert_refused(e, mode);
        let e = s
            .alter_type_add_field("t_ks", "clean", "j", CqlType::Jsonb)
            .expect_err("alter type apply");
        assert_refused(e, mode);
        assert_eq!(s.get_type("t_ks", "clean").expect("type").fields.len(), 1);
        // A column typed by a UDT that nests jsonb is a jsonb column.
        let e = s
            .create_table_internal(table("over_udt", "frozen<has_j>"))
            .expect_err("udt-nested jsonb column");
        assert_refused(e, mode);
        s.create_table_internal(table("over_clean", "frozen<clean>"))
            .expect("clean udt column is fine");
    }
}

#[test]
fn snapshot_carrying_jsonb_is_refused_on_a_cluster_node() {
    for mode in NON_STANDALONE {
        let s = schema();
        s.set_deployment_mode(mode);
        let mut snapshot = SchemaSnapshot::default();
        snapshot.keyspaces.insert("t_ks".into(), ks("t_ks"));
        snapshot
            .tables
            .insert(("t_ks".into(), "ok".into()), table("ok", "int"));
        snapshot
            .tables
            .insert(("t_ks".into(), "j".into()), table("j", "jsonb"));
        let e = s.apply_snapshot(snapshot.clone()).expect_err("refused");
        assert_refused(e, mode);
        assert!(
            !s.snapshot().tables.keys().any(|(k, _)| k == "t_ks"),
            "a refused snapshot applies nothing"
        );
        // The same snapshot on a standalone node applies.
        let s = schema();
        s.apply_snapshot(snapshot).expect("standalone applies");
    }
}

#[test]
fn shared_gate_function_is_exposed_for_other_wires() {
    let types = TypeMap::new();
    check_jsonb_ddl_allowed(DeploymentMode::Standalone, "ks", &["jsonb"], &types).expect("ok");
    check_jsonb_ddl_allowed(DeploymentMode::Cluster, "ks", &["int", "text"], &types)
        .expect("no jsonb, no refusal");
    let e = check_jsonb_ddl_allowed(
        DeploymentMode::Cluster,
        "ks",
        &["frozen<list<jsonb>>"],
        &types,
    )
    .expect_err("refused");
    let text = e.to_string();
    assert!(text.contains("D15a") && text.contains("cluster"), "{text}");
    assert!(!text.to_lowercase().contains("disable"), "{text}");
    let bad = check_jsonb_ddl_allowed(DeploymentMode::Cluster, "ks", &["list<"], &types)
        .expect_err("an unparseable type is an error, not a skip");
    assert!(matches!(bad, SchemaError::InvalidSchema(_)), "{bad:?}");
}

#[test]
fn tables_holding_jsonb_are_named_for_the_transition_check() {
    let s = schema_in(DeploymentMode::Standalone);
    s.create_table_internal(table("plain", "int")).expect("t");
    assert!(s.tables_with_jsonb().expect("scan").is_empty());
    s.create_table_internal(table("docs", "jsonb")).expect("t");
    s.create_type_internal(&udt("u", vec![("j", CqlType::Jsonb)]))
        .expect("t");
    s.create_table_internal(table("via_udt", "frozen<u>"))
        .expect("t");
    let mut names = s.tables_with_jsonb().expect("scan");
    names.sort();
    assert_eq!(names, vec!["t_ks.docs", "t_ks.via_udt"]);
}

#[test]
fn deployment_mode_handle_is_shared_live() {
    let s = schema();
    let handle = s.deployment_mode_handle();
    assert_eq!(**handle.load(), DeploymentMode::Standalone);
    handle.store(std::sync::Arc::new(DeploymentMode::Cluster));
    assert_eq!(s.deployment_mode(), DeploymentMode::Cluster);
}
