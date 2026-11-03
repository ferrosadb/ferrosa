use super::*;

fn info(id: u128, state: NodeState) -> NodeInfo {
    NodeInfo {
        host_id: Uuid::from_u128(id),
        addr: "127.0.0.1:9000".into(),
        data_center: "dc1".into(),
        rack: "rack1".into(),
        state,
        cql_broadcast: None,
    }
}
fn owners(states: &[NodeState]) -> TokenRing {
    let mut ring = TokenRing::new();
    for (i, state) in states.iter().enumerate() {
        let node = info(i as u128 + 1, *state);
        let id = uuid_to_node_id(node.host_id);
        ring.add_node(id, node);
        ring.assign_tokens(id, &[(i as i64 + 1) * 100]);
    }
    ring
}
fn schema(rf: usize) -> ferrosa_schema::SchemaSnapshot {
    use ferrosa_schema::metadata::{
        keyspace::{KeyspaceMetadata, ReplicationParams},
        table::TableParams,
    };
    let mut schema = ferrosa_schema::SchemaSnapshot::default();
    schema.keyspaces.insert(
        "ks".into(),
        KeyspaceMetadata {
            name: "ks".into(),
            durable_writes: true,
            replication: ReplicationParams {
                strategy: "SimpleStrategy".into(),
                options: HashMap::from([("replication_factor".into(), rf.to_string())]),
            },
        },
    );
    schema.tables.insert(
        ("ks".into(), "tbl".into()),
        ferrosa_schema::TableMetadata {
            keyspace: "ks".into(),
            name: "tbl".into(),
            id: Uuid::new_v4(),
            columns: Default::default(),
            partition_key: vec![],
            clustering_key: vec![],
            params: TableParams::default(),
            flags: Default::default(),
            extensions: Default::default(),
            is_system: false,
        },
    );
    schema
}

#[test]
fn verification_admission_counts_owning_learners_and_refuses_eviction() {
    let ring = owners(&[
        NodeState::Normal,
        NodeState::Normal,
        NodeState::Learner { owns_tokens: true },
    ]);
    assert!(
        monotonic(&ring, &info(4, NodeState::Joining), &schema(3)).is_err(),
        "owning learner is a prior data owner"
    );
    assert!(monotonic(&ring, &info(4, NodeState::Joining), &schema(4)).is_ok());
    let ring = owners(&[
        NodeState::Normal,
        NodeState::Normal,
        NodeState::Learner { owns_tokens: false },
    ]);
    assert!(
        monotonic(&ring, &info(4, NodeState::Joining), &schema(3)).is_ok(),
        "non-owning analytics learner does not change placement"
    );
}

#[tokio::test(start_paused = true)]
async fn verification_admission_serializes_guard_verification_and_promotion() {
    let gate = Arc::new(tokio::sync::Mutex::new(()));
    let ring = Arc::new(parking_lot::Mutex::new(owners(&[NodeState::Normal; 3])));
    let schema = Arc::new(schema(4));
    let (entered_tx, entered_rx) = tokio::sync::oneshot::channel();
    let (release_tx, release_rx) = tokio::sync::oneshot::channel();
    let first = {
        let gate = gate.clone();
        let ring = ring.clone();
        let schema = schema.clone();
        tokio::spawn(async move {
            serialized(&gate, async {
                monotonic(&ring.lock(), &info(4, NodeState::Joining), &schema)?;
                entered_tx.send(()).unwrap();
                release_rx.await.unwrap(); // Represents streamed verification.
                let node = info(4, NodeState::Normal);
                let id = uuid_to_node_id(node.host_id);
                let mut state = ring.lock();
                state.add_node(id, node);
                state.assign_tokens(id, &[400]);
                Ok::<(), ClusterError>(())
            })
            .await
        })
    };
    entered_rx.await.unwrap();
    let (queued_tx, queued_rx) = tokio::sync::oneshot::channel();
    let (second_tx, mut second_rx) = tokio::sync::oneshot::channel();
    let second = {
        let gate = gate.clone();
        let ring = ring.clone();
        let schema = schema.clone();
        tokio::spawn(async move {
            queued_tx.send(()).unwrap();
            serialized(&gate, async {
                second_tx.send(()).unwrap();
                monotonic(&ring.lock(), &info(5, NodeState::Joining), &schema)
            })
            .await
        })
    };
    queued_rx.await.unwrap();
    assert!(
        tokio::time::timeout(Duration::from_millis(1), &mut second_rx)
            .await
            .is_err(),
        "distinct host cannot evaluate its guard during another host's verification"
    );
    release_tx.send(()).unwrap();
    assert!(first.await.unwrap().is_ok());
    second_rx.await.unwrap();
    assert!(
        second.await.unwrap().is_err(),
        "second guard must see the first promotion and refuse RF4 eviction"
    );
}
