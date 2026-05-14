use capnp::{message, serialize};
use ferrosa_net::protocol::envelope_capnp::{cluster_control, envelope, MessageFamily};

#[test]
fn generated_envelope_exposes_stable_common_fields() {
    let mut message = message::Builder::new_default();
    {
        let mut envelope = message.init_root::<envelope::Builder>();
        envelope.set_magic(0x4645_5231);
        envelope.set_transport_version(1);
        envelope.set_min_supported_transport_version(1);
        envelope.set_schema_version(1);
        envelope.set_message_family(MessageFamily::Lifecycle);
        envelope.set_message_kind(0);
        envelope.set_required_features(1);
        envelope.set_optional_features(0);
        envelope.set_stream_id(42);
    }

    let words = serialize::write_message_to_words(&message);
    let reader = serialize::read_message_from_flat_slice(
        &mut words.as_slice(),
        message::ReaderOptions::new(),
    )
    .expect("generated envelope should decode from capnp words");
    let envelope = reader
        .get_root::<envelope::Reader>()
        .expect("generated envelope root should be readable");

    assert_eq!(envelope.get_magic(), 0x4645_5231);
    assert_eq!(envelope.get_transport_version(), 1);
    assert_eq!(envelope.get_min_supported_transport_version(), 1);
    assert_eq!(envelope.get_schema_version(), 1);
    assert_eq!(envelope.get_message_family(), Ok(MessageFamily::Lifecycle));
    assert_eq!(envelope.get_message_kind(), 0);
    assert_eq!(envelope.get_required_features(), 1);
    assert_eq!(envelope.get_stream_id(), 42);
}

#[test]
fn generated_cluster_invite_family_round_trips_without_legacy_message_migration() {
    let invite_id = [0x11_u8; 16];
    let mut message = message::Builder::new_default();
    {
        let mut envelope = message.init_root::<envelope::Builder>();
        envelope.set_magic(0x4645_5231);
        envelope.set_message_family(MessageFamily::ClusterControl);
        envelope.set_message_kind(0);
        let cluster = envelope.init_payload().init_cluster();
        let mut invite = cluster.init_op().init_invite();
        invite.set_formation_epoch(7);
        invite.set_expires_at_unix_nanos(1_234_567);
        invite
            .reborrow()
            .init_invite_id(invite_id.len() as u32)
            .copy_from_slice(&invite_id);
        invite.init_peers(0);
    }

    let words = serialize::write_message_to_words(&message);
    let reader = serialize::read_message_from_flat_slice(
        &mut words.as_slice(),
        message::ReaderOptions::new(),
    )
    .expect("generated cluster invite should decode from capnp words");
    let envelope = reader
        .get_root::<envelope::Reader>()
        .expect("generated envelope root should be readable");

    assert_eq!(
        envelope.get_message_family(),
        Ok(MessageFamily::ClusterControl)
    );
    let cluster = match envelope
        .get_payload()
        .which()
        .expect("payload union tag is known")
    {
        envelope::payload::Cluster(cluster) => cluster.expect("cluster payload is present"),
        _ => panic!("expected cluster-control payload"),
    };
    let invite = match cluster
        .get_op()
        .which()
        .expect("cluster op union tag is known")
    {
        cluster_control::op::Invite(invite) => invite.expect("invite payload is present"),
        _ => panic!("expected cluster invite op"),
    };

    assert_eq!(invite.get_formation_epoch(), 7);
    assert_eq!(invite.get_expires_at_unix_nanos(), 1_234_567);
    assert_eq!(
        invite.get_invite_id().expect("invite id is set"),
        &invite_id
    );
    assert_eq!(invite.get_peers().expect("peers list is set").len(), 0);
}
