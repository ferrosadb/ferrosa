//! Integration tests for ferrosa-schema: full workflow, permissions, audit.

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};

use ferrosa_schema::*;

/// Mutex to serialize tests that manipulate the FERROSA_SUPERUSER_PASSWORD env var.
static ENV_LOCK: Mutex<()> = Mutex::new(());

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

fn test_schema() -> Schema {
    let _guard = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    // Safety: env var mutation is not thread-safe; protected by ENV_LOCK.
    unsafe {
        std::env::remove_var("FERROSA_SUPERUSER_PASSWORD");
    }
    Schema::new(test_config()).unwrap()
}

fn test_schema_with_sink(sink: Arc<TestAuditSink>) -> Schema {
    let _guard = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    // Safety: env var mutation is not thread-safe; protected by ENV_LOCK.
    unsafe {
        std::env::remove_var("FERROSA_SUPERUSER_PASSWORD");
    }
    Schema::new(SchemaConfig {
        audit_sink: Box::new(sink),
        ..test_config()
    })
    .unwrap()
}

fn superuser_auth() -> AuthContext {
    AuthContext {
        role: "cassandra".to_string(),
        is_superuser: true,
        must_change_password: false,
    }
}

fn test_keyspace(name: &str) -> KeyspaceMetadata {
    KeyspaceMetadata {
        name: name.to_string(),
        durable_writes: true,
        replication: ReplicationParams {
            strategy: "SimpleStrategy".to_string(),
            options: HashMap::from([("replication_factor".into(), "1".into())]),
        },
    }
}

fn test_table(keyspace: &str, name: &str) -> TableMetadata {
    use indexmap::IndexMap;
    let mut columns = IndexMap::new();
    columns.insert(
        "id".to_string(),
        ColumnMetadata {
            name: "id".to_string(),
            kind: ColumnKind::PartitionKey,
            position: 0,
            column_type: "uuid".to_string(),
            clustering_order: ClusteringOrder::None,
            mask: None,
        },
    );
    TableMetadata {
        keyspace: keyspace.to_string(),
        name: name.to_string(),
        id: uuid::Uuid::new_v4(),
        columns,
        partition_key: vec!["id".to_string()],
        clustering_key: vec![],
        params: TableParams::default(),
        flags: HashSet::new(),
        extensions: HashMap::new(),
    }
}

fn test_role(name: &str) -> RoleMetadata {
    RoleMetadata {
        name: name.to_string(),
        is_superuser: false,
        can_login: true,
        salted_hash: None,
        member_of: HashSet::new(),
    }
}

#[test]
fn full_workflow_create_role_auth_keyspace_table_query() {
    let sink = Arc::new(TestAuditSink::new());
    let schema = test_schema_with_sink(sink.clone());
    let auth = schema.authenticate("cassandra", "cassandra").unwrap();
    assert!(auth.is_superuser);

    // Create a keyspace
    schema
        .create_keyspace(
            KeyspaceMetadata {
                name: "production".into(),
                durable_writes: true,
                replication: ReplicationParams {
                    strategy: "SimpleStrategy".into(),
                    options: HashMap::from([("replication_factor".into(), "1".into())]),
                },
            },
            &auth,
        )
        .unwrap();

    // Create a table with columns
    let table = test_table("production", "users");
    schema.create_table(table, &auth).unwrap();

    // Verify system_schema reflects state
    let snap = schema.snapshot();
    let ks_rows = query_keyspaces(&snap);
    assert!(ks_rows.iter().any(|r| r.keyspace_name == "production"));
    let tbl_rows = query_tables(&snap);
    assert!(tbl_rows
        .iter()
        .any(|r| r.keyspace_name == "production" && r.table_name == "users"));
}

#[test]
fn non_superuser_cannot_create_keyspace_without_grant() {
    let schema = test_schema();
    let su = superuser_auth();
    schema
        .create_role(test_role("limited"), Some("P@ssw0rd!!"), &su)
        .unwrap();
    let ctx = schema.authenticate("limited", "P@ssw0rd!!").unwrap();
    let result = schema.create_keyspace(test_keyspace("ks1"), &ctx);
    assert!(matches!(result, Err(SchemaError::PermissionDenied { .. })));
}

#[test]
fn rate_limiting_blocks_after_failures() {
    use std::time::Duration;
    let _guard = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    // Safety: env var mutation is not thread-safe; protected by ENV_LOCK.
    unsafe {
        std::env::remove_var("FERROSA_SUPERUSER_PASSWORD");
    }
    let schema = Schema::new(SchemaConfig {
        rate_limit: RateLimitConfig {
            max_attempts: 3,
            base_backoff: Duration::from_secs(60),
            max_backoff: Duration::from_secs(60),
            lockout_duration: Duration::from_secs(60),
            window: Duration::from_secs(60),
        },
        ..test_config()
    })
    .unwrap();
    for _ in 0..3 {
        let _ = schema.authenticate("cassandra", "wrong");
    }
    let result = schema.authenticate("cassandra", "cassandra");
    assert!(matches!(result, Err(SchemaError::AuthenticationThrottled)));
}

#[test]
fn every_mutation_emits_audit_event() {
    let sink = Arc::new(TestAuditSink::new());
    let schema = test_schema_with_sink(sink.clone());
    let auth = superuser_auth();
    sink.clear();

    schema.create_keyspace(test_keyspace("ks1"), &auth).unwrap();
    schema.create_table(test_table("ks1", "t1"), &auth).unwrap();
    schema.create_role(test_role("r1"), None, &auth).unwrap();
    schema
        .grant(
            "r1",
            &Resource::AllKeyspaces,
            HashSet::from([Permission::Select]),
            &auth,
        )
        .unwrap();
    schema
        .revoke(
            "r1",
            &Resource::AllKeyspaces,
            HashSet::from([Permission::Select]),
            &auth,
        )
        .unwrap();
    schema.drop_role("r1", &auth).unwrap();
    schema.drop_table("ks1", "t1", &auth).unwrap();
    schema.drop_keyspace("ks1", &auth).unwrap();

    let events = sink.events();
    assert_eq!(
        events.len(),
        8,
        "8 operations should emit 8 audit events, got: {}",
        events.len()
    );
}
