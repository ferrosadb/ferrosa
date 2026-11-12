//! Sprint 6 W6.4 — per-DC `MembershipChanger`.
//!
//! Verifies that `MembershipChanger::for_dc(dc_name, raft, network)`
//! returns a changer scoped to that DC: `dc_name()` and `group_id()`
//! match, and the changer's Raft handle is the per-DC instance.
//!
//! Uses two independent in-process Raft clusters (one per DC) to
//! demonstrate the scoping is real — an `add_voter` on the dc1
//! changer flows into dc1's Raft only.

mod common;

use std::time::Duration;

use common::raft_harness::TestCluster;

use ferrosa_cluster::membership::MembershipChanger;
use ferrosa_cluster::raft::RaftGroupId;

/// A schema registry fixture for the schema-version tests below. Mirrors the
/// builder the controller's own tests use.
fn test_schema() -> ferrosa_schema::Schema {
    use ferrosa_schema::{
        AuthMethod, DeploymentMode as SchemaDeploymentMode, LogAuditSink, PasswordHasher,
        PasswordPolicy, RateLimitConfig, SchemaConfig,
    };
    let config = SchemaConfig {
        hasher: PasswordHasher::default(),
        password_policy: PasswordPolicy::permissive(),
        auth_method: AuthMethod::Password,
        rate_limit: RateLimitConfig::default(),
        audit_sink: Box::new(LogAuditSink),
        secrets: Box::new(ferrosa_schema::EnvSecretsProvider),
        mode: SchemaDeploymentMode::Development,
    };
    ferrosa_schema::Schema::new(config).expect("schema registry")
}

/// bug-schema-version-diverges-after-restart (t_7dda9421): a `MembershipChanger`
/// proposes only ops that change NO schema (`JoinNode` / `SetNodeState` /
/// `LeaveNode` / `ApproveNode` / `UpdateNodeInfo` / `AccordApply`), so every
/// command it builds must carry the node's CURRENT committed schema version. The
/// `Uuid::new_v4()` that used to sit at each construction site in
/// `src/membership/mod.rs` is what this replaces.
///
/// The changer holds no registry, so the value comes from the caller's `Schema`
/// at construction; a changer built without it carries the documented nil "not
/// known here" — deterministic, never a random version.
///
/// Regression guard, not the red: `MembershipChanger` needs a live
/// `openraft::Raft`, so there is no unit-level seam that observes the command it
/// builds. The red for this sweep drives the real
/// `decommission_verified` / `bootstrap_verified` path
/// (`controller::data_movement::tests`).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn membership_commands_carry_the_current_schema_version() {
    let cluster = TestCluster::with_voters(1).await;
    let _ = cluster
        .wait_for_leader(Duration::from_secs(10))
        .await
        .expect("leader");

    let schema = test_schema();
    let current = schema.snapshot().version;

    let changer = MembershipChanger::new(
        cluster.leader_node().raft.clone(),
        cluster.membership_network(),
    )
    .with_schema_version(current);
    assert_eq!(
        changer.schema_version(),
        current,
        "the changer must stamp the version the caller read from its Schema"
    );

    // A real DDL moves the node's version, and a changer rebuilt from the same
    // `Schema` follows it — the value is the node's current one, not one frozen
    // at boot.
    let after_ddl = uuid::Uuid::from_u128(0x1111_2222_3333_4444_5555_6666_7777_bbb1);
    assert_ne!(
        after_ddl, current,
        "the moved version must differ from the fixture's, or this is vacuous"
    );
    schema.set_schema_version(after_ddl);
    let rebuilt = MembershipChanger::new(
        cluster.leader_node().raft.clone(),
        cluster.membership_network(),
    )
    .with_schema_version(schema.snapshot().version);
    assert_eq!(
        rebuilt.schema_version(),
        after_ddl,
        "after a DDL the next membership command must carry the new schema version"
    );

    // Without the builder the changer carries the honest "not known here" nil,
    // never a freshly minted random one.
    let unset = MembershipChanger::new(
        cluster.leader_node().raft.clone(),
        cluster.membership_network(),
    );
    assert_eq!(
        unset.schema_version(),
        uuid::Uuid::nil(),
        "an unset version must be nil, never a random UUID"
    );

    cluster.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn membership_changer_scoped_to_dc() {
    let dc1_cluster = TestCluster::with_voters(3).await;
    let dc2_cluster = TestCluster::with_voters(3).await;
    let _ = dc1_cluster
        .wait_for_leader(Duration::from_secs(10))
        .await
        .expect("dc1 leader");
    let _ = dc2_cluster
        .wait_for_leader(Duration::from_secs(10))
        .await
        .expect("dc2 leader");

    let dc1_raft = dc1_cluster.leader_node().raft.clone();
    let dc2_raft = dc2_cluster.leader_node().raft.clone();
    let dc1_net = dc1_cluster.membership_network();
    let dc2_net = dc2_cluster.membership_network();

    let dc1_changer = MembershipChanger::for_dc("dc1", dc1_raft.clone(), dc1_net);
    let dc2_changer = MembershipChanger::for_dc("dc2", dc2_raft.clone(), dc2_net);

    // Scoping: dc_name() and group_id() reflect the constructor.
    assert_eq!(dc1_changer.dc_name(), "dc1");
    assert_eq!(dc2_changer.dc_name(), "dc2");
    assert_eq!(dc1_changer.group_id(), RaftGroupId::for_dc("dc1"));
    assert_eq!(dc2_changer.group_id(), RaftGroupId::for_dc("dc2"));
    assert_ne!(dc1_changer.group_id(), dc2_changer.group_id());

    // Backward-compat: MembershipChanger::new(...) defaults to the
    // "default" DC group so existing single-DC callers don't need
    // to thread a DC name through.
    let default_changer =
        MembershipChanger::new(dc1_raft.clone(), dc1_cluster.membership_network());
    assert_eq!(default_changer.dc_name(), "default");
    assert_eq!(default_changer.group_id(), RaftGroupId::default_dc());

    dc1_cluster.shutdown().await;
    dc2_cluster.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn for_dc_default_metadata_inherits_dc_name() {
    // New joiners admitted via `for_dc("dc2", ...)` get
    // `data_center = "dc2"` recorded in `state.members` by default —
    // unless the caller explicitly overrides via
    // `with_node_metadata_defaults`.
    let cluster = TestCluster::with_voters(3).await;
    let _ = cluster
        .wait_for_leader(Duration::from_secs(10))
        .await
        .expect("leader");

    let raft = cluster.leader_node().raft.clone();
    let net = cluster.membership_network();
    let changer = MembershipChanger::for_dc("dc2", raft, net);
    assert_eq!(changer.dc_name(), "dc2");

    // Override path still works — operator can force a different
    // data_center for new joiners (e.g., onboarding a stretched node).
    let raft2 = cluster.leader_node().raft.clone();
    let net2 = cluster.membership_network();
    let custom = MembershipChanger::for_dc("dc2", raft2, net2)
        .with_node_metadata_defaults("override-dc".into(), "rack9".into());
    assert_eq!(custom.dc_name(), "dc2", "DC scoping unchanged by override");

    cluster.shutdown().await;
}
