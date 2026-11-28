//! A DDL that arrives while the node is forming its Raft group (forge
//! t_0cfbb511): it waits for the formation task to publish the replicated path
//! and runs there, instead of being refused with a false "peer lost".

use super::*;

fn ctx(auth: &AuthContext) -> RequestContext<'_> {
    RequestContext {
        auth,
        current_keyspace: &None,
        consistency: ConsistencyLevel::One,
        serial_consistency: None,
        paging: crate::paging::PagingParams::default(),
        client_address: String::new(),
        protocol_version: 4,
    }
}

/// CREATE KEYSPACE sent during formation succeeds once formation publishes the
/// path, and the keyspace exists afterwards. Before the fix the router answered
/// it at once with "DDL unavailable: peer lost" and applied nothing.
#[tokio::test]
async fn create_keyspace_during_formation_waits_for_the_published_path() {
    let (state, _dir) = setup();
    let admin = dev_auth();
    let published = state.ddl_path.load_full();
    let (queue, mut queued) = tokio::sync::mpsc::channel(1);
    state.ddl_path.store(Arc::new(DdlPath::Forming { queue }));

    let swap = Arc::clone(&state.ddl_path);
    let publish_after = std::time::Duration::from_millis(150);
    let publisher = tokio::spawn(async move {
        tokio::time::sleep(publish_after).await;
        // Stand-in for the formation task publishing its outcome.
        swap.store(published);
    });

    let started = std::time::Instant::now();
    let cql = "CREATE KEYSPACE formed_ks WITH REPLICATION = \
               {'class': 'SimpleStrategy', 'replication_factor': '1'}";
    route(&state, &ctx(&admin), crate::parser::parse(cql).unwrap())
        .await
        .expect("a DDL sent during formation must run once the path is published");
    assert!(
        started.elapsed() >= publish_after,
        "the DDL must wait for publication, returned after {:?}",
        started.elapsed()
    );
    assert!(
        state.schema.snapshot().keyspaces.contains_key("formed_ks"),
        "the keyspace must exist after the DDL reported success"
    );
    assert!(
        queued.try_recv().is_err(),
        "the DDL must not also be queued on the forming path for a later replay"
    );
    publisher.await.expect("publisher task");
}

/// Formation that does not finish in time is reported as formation, with the
/// DDL unapplied and retryable, never as a lost peer.
#[test]
fn a_formation_timeout_reaches_the_client_as_formation_not_peer_lost() {
    let err = CqlError::from(ferrosa_cluster::ClusterError::DdlFormationInProgress {
        waited_ms: 10_000,
        bound_ms: 10_000,
    });
    let msg = err.to_string();
    assert!(msg.contains("formation in progress"), "got: {msg}");
    assert!(msg.contains("not applied"), "got: {msg}");
    assert!(!msg.contains("peer lost"), "got: {msg}");
    assert!(!ddl_path_still_forming().to_string().contains("peer lost"));
}
