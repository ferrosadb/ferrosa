use super::*;

#[derive(Clone, Copy, Debug)]
enum ConditionalChange {
    CreateKeyspace,
    DropKeyspace,
    CreateTable,
    DropTable,
    CreateIndex,
    DropIndex,
    CreateRole,
    DropRole,
    CreateIndexMissingParent,
    CreateIndexMissingColumn,
    DropIndexChangedOwner,
    DropIndexChangedOwnerUnauthorized,
}

impl ConditionalChange {
    fn query(self) -> &'static str {
        match self {
            Self::CreateKeyspace => "CREATE KEYSPACE IF NOT EXISTS conditional_new WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': '2'}",
            Self::DropKeyspace => "DROP KEYSPACE IF EXISTS conditional_drop",
            Self::CreateTable => "CREATE TABLE IF NOT EXISTS conditional_guard.new_table (id int PRIMARY KEY, different text)",
            Self::DropTable => "DROP TABLE IF EXISTS conditional_guard.drop_table",
            Self::CreateIndex => "CREATE INDEX IF NOT EXISTS new_index ON conditional_guard.template (b)",
            Self::DropIndex => "DROP INDEX IF EXISTS conditional_guard.drop_index",
            Self::CreateRole => "CREATE ROLE IF NOT EXISTS conditional_new WITH LOGIN = true",
            Self::DropRole => "DROP ROLE IF EXISTS conditional_drop",
            Self::CreateIndexMissingParent | Self::CreateIndexMissingColumn => "CREATE INDEX IF NOT EXISTS new_index ON conditional_guard.template (b)",
            Self::DropIndexChangedOwner | Self::DropIndexChangedOwnerUnauthorized => "DROP INDEX IF EXISTS conditional_guard.drop_index",
        }
    }

    fn winning_operations(self, state: &SharedState) -> Vec<DdlOperation> {
        let snapshot = state.schema.snapshot();
        let operation = match self {
            Self::CreateKeyspace => {
                let mut metadata = snapshot.keyspaces["conditional_guard"].clone();
                metadata.name = "conditional_new".into();
                DdlOperation::CreateKeyspace(metadata)
            }
            Self::DropKeyspace => DdlOperation::DropKeyspace("conditional_drop".into()),
            Self::CreateTable => {
                let mut metadata =
                    snapshot.tables[&("conditional_guard".into(), "template".into())].clone();
                metadata.name = "new_table".into();
                metadata.id = uuid::Uuid::new_v4();
                DdlOperation::CreateTable(Box::new(metadata))
            }
            Self::DropTable => DdlOperation::DropTable {
                keyspace: "conditional_guard".into(),
                table: "drop_table".into(),
            },
            Self::CreateIndex => {
                let mut metadata = snapshot
                    .indexes
                    .values()
                    .find(|index| index.name == "template_index")
                    .unwrap()
                    .clone();
                metadata.name = "new_index".into();
                DdlOperation::CreateIndex(metadata)
            }
            Self::DropIndex => DdlOperation::DropIndex {
                keyspace: "conditional_guard".into(),
                table: "template".into(),
                index: "drop_index".into(),
            },
            Self::CreateRole => {
                let mut metadata = snapshot.roles["conditional_template"].clone();
                metadata.name = "conditional_new".into();
                DdlOperation::CreateRole(metadata)
            }
            Self::DropRole => DdlOperation::DropRole("conditional_drop".into()),
            Self::CreateIndexMissingParent => DdlOperation::DropTable {
                keyspace: "conditional_guard".into(),
                table: "template".into(),
            },
            Self::CreateIndexMissingColumn => {
                let mut metadata =
                    snapshot.tables[&("conditional_guard".into(), "template".into())].clone();
                metadata.id = uuid::Uuid::new_v4();
                metadata.columns.shift_remove("b");
                return vec![
                    DdlOperation::DropTable {
                        keyspace: "conditional_guard".into(),
                        table: "template".into(),
                    },
                    DdlOperation::CreateTable(Box::new(metadata)),
                ];
            }
            Self::DropIndexChangedOwner | Self::DropIndexChangedOwnerUnauthorized => {
                let mut metadata = snapshot
                    .indexes
                    .values()
                    .find(|index| index.name == "drop_index")
                    .unwrap()
                    .clone();
                metadata.table = "owner_b".into();
                return vec![
                    DdlOperation::DropIndex {
                        keyspace: "conditional_guard".into(),
                        table: "template".into(),
                        index: "drop_index".into(),
                    },
                    DdlOperation::CreateIndex(metadata),
                ];
            }
        };
        vec![operation]
    }
}

fn persisted_indexes(engine: &StorageEngine) -> Vec<ferrosa_storage::engine::PersistedIndexRow> {
    let mut indexes: Vec<_> = engine
        .read_persisted_indexes()
        .unwrap()
        .into_iter()
        .filter(|index| index.keyspace_name == "conditional_guard")
        .collect();
    indexes.sort_by(|a, b| (&a.table_name, &a.index_name).cmp(&(&b.table_name, &b.index_name)));
    indexes
}

async fn check_queued_condition(change: ConditionalChange, guarded: bool) {
    let (state, _dir) = setup();
    let admin = dev_auth();
    let mut ctx = RequestContext {
        auth: &admin,
        current_keyspace: &None,
        consistency: ConsistencyLevel::One,
        serial_consistency: None,
        paging: crate::paging::PagingParams::default(),
        client_address: String::new(),
        protocol_version: 4,
    };
    for cql in [
        "CREATE KEYSPACE conditional_guard WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': '1'}",
        "CREATE KEYSPACE conditional_drop WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': '1'}",
        "CREATE TABLE conditional_guard.template (id int PRIMARY KEY, a text, b text)",
        "CREATE TABLE conditional_guard.drop_table (id int PRIMARY KEY)",
        "CREATE TABLE conditional_guard.owner_b (id int PRIMARY KEY, a text, b text)",
        "CREATE INDEX template_index ON conditional_guard.template (a)",
        "CREATE INDEX drop_index ON conditional_guard.template (b)",
        "CREATE ROLE conditional_template WITH LOGIN = false",
        "CREATE ROLE conditional_drop WITH LOGIN = false",
    ] {
        route(&state, &ctx, crate::parser::parse(cql).unwrap()).await.unwrap();
    }
    let limited = AuthContext {
        role: "conditional_limited".into(),
        is_superuser: false,
        must_change_password: false,
    };
    if matches!(change, ConditionalChange::DropIndexChangedOwnerUnauthorized) {
        let mut role = state.schema.snapshot().roles["conditional_template"].clone();
        role.name = limited.role.clone();
        state.schema.create_role_internal(role).unwrap();
        state
            .schema
            .grant_internal(GrantEntry {
                role: limited.role.clone(),
                resource: Resource::Table("conditional_guard".into(), "template".into()),
                permissions: [Permission::Alter].into_iter().collect(),
            })
            .unwrap();
        state
            .schema
            .check_permission(
                &limited,
                Permission::Alter,
                &Resource::Table("conditional_guard".into(), "template".into()),
            )
            .unwrap();
        ctx.auth = &limited;
    }
    let operations = change.winning_operations(&state);
    let mut pauses = Vec::new();
    for operation in &operations {
        if let DdlOperation::DropTable { keyspace, table } = operation {
            pauses.push(
                state
                    .engine
                    .pause_table_compactions(
                        &TableId::new(keyspace, table),
                        ferrosa_common::CancelReason::TableDropped,
                    )
                    .await
                    .unwrap(),
            );
        }
    }
    let mut events = state.event_sender.subscribe();
    let schema = state.schema.clone();
    let engine = state.engine.clone();
    let (ready, held) = tokio::sync::oneshot::channel();
    let (release, waiting) = std::sync::mpsc::channel();
    let (observed, winner_applied) = tokio::sync::oneshot::channel();
    let writer = std::thread::spawn(move || {
        let _pauses = pauses;
        let _writer = schema.lifecycle_write();
        ready.send(()).unwrap();
        waiting
            .recv_timeout(std::time::Duration::from_secs(5))
            .unwrap();
        for operation in &operations {
            ferrosa_cluster::ddl_path::apply_direct(operation, &schema, &engine).unwrap();
        }
        observed
            .send((schema.snapshot().version, persisted_indexes(&engine)))
            .unwrap();
    });
    held.await.unwrap();
    let query = if guarded {
        change.query().to_string()
    } else {
        change
            .query()
            .replace(" IF NOT EXISTS", "")
            .replace(" IF EXISTS", "")
    };
    let queued = route(&state, &ctx, crate::parser::parse(&query).unwrap());
    tokio::pin!(queued);
    // The initial snapshot condition has run, but this mutation cannot enter
    // the writer until the other operation changes the condition.
    assert!(futures::poll!(&mut queued).is_pending());
    release.send(()).unwrap();
    let (winning_version, winning_indexes) = winner_applied.await.unwrap();
    let result = queued.await;
    writer.join().unwrap();
    if matches!(change, ConditionalChange::DropIndexChangedOwner) {
        assert!(
            result.is_ok(),
            "current authorized index owner must be dropped: {:?}",
            result.as_ref().err()
        );
        assert!(!state
            .schema
            .snapshot()
            .indexes
            .values()
            .any(|index| index.name == "drop_index"));
        assert!(!persisted_indexes(&state.engine)
            .iter()
            .any(|index| index.index_name == "drop_index"));
        let event = events
            .try_recv()
            .expect("successful current-owner drop must publish its actual target");
        assert!(matches!(event, CqlEvent::SchemaChange {
            target: SchemaTarget::Table, keyspace, name: Some(name), ..
        } if keyspace == "conditional_guard" && name == "owner_b"));
        assert!(matches!(
            events.try_recv(),
            Err(tokio::sync::broadcast::error::TryRecvError::Empty)
        ));
        return;
    } else if matches!(change, ConditionalChange::DropIndexChangedOwnerUnauthorized) {
        assert!(
            matches!(result, Err(CqlError::Unauthorized(_))),
            "old-owner grant must not authorize the rebound owner: {:?}",
            result.as_ref().err()
        );
    } else if matches!(
        change,
        ConditionalChange::CreateIndexMissingParent | ConditionalChange::CreateIndexMissingColumn
    ) {
        assert!(
            matches!(result, Err(CqlError::Invalid(_))),
            "missing current parent/column must refuse index creation: {:?}",
            result.as_ref().err()
        );
    } else if guarded {
        assert!(
            result.is_ok(),
            "{query} must be an unchanged success: {:?}",
            result.as_ref().err()
        );
    } else {
        assert!(
            result.is_err(),
            "{query} must reject a duplicate or missing index"
        );
    }
    assert_eq!(
        state.schema.snapshot().version,
        winning_version,
        "{query} changed the winning schema instead of retaining it"
    );
    assert_eq!(
        persisted_indexes(&state.engine),
        winning_indexes,
        "{query} overwrote the winning durable index definition"
    );
    assert!(
        matches!(
            events.try_recv(),
            Err(tokio::sync::broadcast::error::TryRecvError::Empty)
        ),
        "{query} published a schema-change event without a change"
    );
}

macro_rules! conditional_case {
    ($name:ident, $case:ident) => {
        #[tokio::test(flavor = "current_thread")]
        async fn $name() {
            check_queued_condition(ConditionalChange::$case, true).await;
        }
    };
}

conditional_case!(
    conditional_create_keyspace_rechecks_under_writer,
    CreateKeyspace
);
conditional_case!(
    conditional_drop_keyspace_rechecks_under_writer,
    DropKeyspace
);
conditional_case!(conditional_create_table_rechecks_under_writer, CreateTable);
conditional_case!(conditional_drop_table_rechecks_under_writer, DropTable);
conditional_case!(
    conditional_create_index_preserves_winner_and_disk,
    CreateIndex
);
conditional_case!(conditional_drop_index_has_no_duplicate_effects, DropIndex);
conditional_case!(conditional_create_role_preserves_winner, CreateRole);
conditional_case!(conditional_drop_role_has_no_duplicate_effects, DropRole);
conditional_case!(
    conditional_index_missing_parent_is_refused,
    CreateIndexMissingParent
);
conditional_case!(
    conditional_index_current_column_is_validated,
    CreateIndexMissingColumn
);
conditional_case!(
    conditional_index_drop_uses_current_owner_and_event,
    DropIndexChangedOwner
);
conditional_case!(
    conditional_index_drop_reauthorizes_current_owner,
    DropIndexChangedOwnerUnauthorized
);

#[tokio::test(flavor = "current_thread")]
async fn unconditional_duplicate_index_is_an_error_without_side_effects() {
    check_queued_condition(ConditionalChange::CreateIndex, false).await;
}

#[tokio::test(flavor = "current_thread")]
async fn unconditional_missing_index_is_an_error_without_side_effects() {
    check_queued_condition(ConditionalChange::DropIndex, false).await;
}

#[tokio::test(flavor = "current_thread")]
async fn conditional_existing_aggregate_ignores_unused_dependencies() {
    let (state, _dir) = setup();
    let admin = dev_auth();
    let ctx = RequestContext {
        auth: &admin,
        current_keyspace: &None,
        consistency: ConsistencyLevel::One,
        serial_consistency: None,
        paging: crate::paging::PagingParams::default(),
        client_address: String::new(),
        protocol_version: 4,
    };
    let function = format!(
        "CREATE FUNCTION conditional_guard.state_fn(acc int, val int) CALLED ON NULL INPUT RETURNS int LANGUAGE wasm AS '{}'",
        hex_encode(&minimal_wasm_component())
    );
    for query in [
        "CREATE KEYSPACE conditional_guard WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': '1'}",
        function.as_str(),
        "CREATE AGGREGATE conditional_guard.existing_agg(int) SFUNC state_fn STYPE int INITCOND 0",
    ] {
        route(&state, &ctx, crate::parser::parse(query).unwrap()).await.unwrap();
    }
    let version = state.schema.snapshot().version;
    let mut events = state.event_sender.subscribe();
    let query = "CREATE AGGREGATE IF NOT EXISTS conditional_guard.existing_agg(int) SFUNC nonexistent_state STYPE int FINALFUNC nonexistent_final INITCOND 'not-an-int'";
    let result = route(&state, &ctx, crate::parser::parse(query).unwrap()).await;
    assert!(
        result.is_ok(),
        "existing signature must ignore unused replacement dependencies: {:?}",
        result.as_ref().err()
    );
    assert_eq!(state.schema.snapshot().version, version);
    assert!(matches!(
        events.try_recv(),
        Err(tokio::sync::broadcast::error::TryRecvError::Empty)
    ));
}

#[tokio::test(flavor = "current_thread")]
async fn conditional_drop_index_refuses_ambiguous_existing_namespace() {
    let (state, _dir) = setup();
    let admin = dev_auth();
    let ctx = RequestContext {
        auth: &admin,
        current_keyspace: &None,
        consistency: ConsistencyLevel::One,
        serial_consistency: None,
        paging: crate::paging::PagingParams::default(),
        client_address: String::new(),
        protocol_version: 4,
    };
    for query in [
        "CREATE KEYSPACE conditional_guard WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': '1'}",
        "CREATE TABLE conditional_guard.owner_a (id int PRIMARY KEY, a text)",
        "CREATE TABLE conditional_guard.owner_b (id int PRIMARY KEY, a text)",
        "CREATE INDEX duplicate_name ON conditional_guard.owner_a (a)",
    ] {
        route(&state, &ctx, crate::parser::parse(query).unwrap()).await.unwrap();
    }
    // Older registry/pair writers allowed table-scoped duplicate names. A
    // keyspace-scoped DROP must refuse this existing state, never choose one.
    let mut second = state
        .schema
        .snapshot()
        .indexes
        .values()
        .find(|index| index.name == "duplicate_name")
        .unwrap()
        .clone();
    second.table = "owner_b".into();
    ferrosa_cluster::ddl_path::apply_direct(
        &DdlOperation::CreateIndex(second),
        &state.schema,
        &state.engine,
    )
    .unwrap();
    assert_eq!(
        state
            .schema
            .snapshot()
            .indexes
            .values()
            .filter(|index| index.name == "duplicate_name")
            .count(),
        2
    );
    let indexes = persisted_indexes(&state.engine);
    assert_eq!(
        indexes
            .iter()
            .filter(|index| index.index_name == "duplicate_name")
            .count(),
        2
    );
    let version = state.schema.snapshot().version;
    let mut events = state.event_sender.subscribe();
    let result = route(
        &state,
        &ctx,
        crate::parser::parse("DROP INDEX IF EXISTS conditional_guard.duplicate_name").unwrap(),
    )
    .await;
    assert!(
        matches!(&result, Err(CqlError::Invalid(message)) if message.contains("ambiguous")),
        "ambiguous namespace must refuse without choosing a table: {:?}",
        result.as_ref().err()
    );
    assert_eq!(state.schema.snapshot().version, version);
    assert_eq!(persisted_indexes(&state.engine), indexes);
    assert!(matches!(
        events.try_recv(),
        Err(tokio::sync::broadcast::error::TryRecvError::Empty)
    ));
}
