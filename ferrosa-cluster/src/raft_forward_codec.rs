//! Cap'n Proto codec for the forwarded Raft schema/membership command.
//!
//! Module: encode/decode the `RaftCommand` a non-leader forwards to the Raft
//! leader inside `Message::ClusterMembershipForward` (`Lane::Data`).
//! Correctness: a framed command round-trips every value of the schema/membership
//! ops it covers, a bincode body still decodes (mixed-version dual-read), and an
//! unreadable or unknown frame is refused by name — never silently half-parsed.
//! Last revised: 2026-10-14
//! Last changed: Added the Cap'n Proto frame beside the shipped bincode encoding.
//!
//! # Why a frame at all
//!
//! The forwarded op is structured *schema metadata* (`TableMetadata`,
//! `KeyspaceMetadata`, the node-membership records). The project rule is that
//! serialization on such a path is Cap'n Proto: bincode/JSON there is a bug. The
//! schema lives beside the Accord family in
//! `ferrosa-net/schema/ferrosa/internode/envelope.capnp`
//! (`ForwardedRaftCommand` and its `Fwd*` children); the encode/decode lives
//! here because only this crate knows `ferrosa-schema`'s metadata types and
//! `ferrosa-cluster`'s `RaftOp`.
//!
//! # Bincode is a shipped contract — hence dual-read
//!
//! `ferrosa-cluster/src/raft/mod.rs`'s `raft_op_variant_tag_stability` pins the
//! bincode variant tags of [`RaftOp`], and a peer running the *previous* build
//! sends bincode only. So the capnp frame is **added alongside** the bincode
//! body and [`decode_forwarded_command`] accepts both: it peeks the wire for the
//! [`FORWARD_RAFT_FRAME_MAGIC`] prefix and, absent it, decodes bincode. The
//! sender only chooses capnp for a peer that advertised
//! [`ferrosa_net::handshake::CAP_RAFT_SCHEMA_CAPNP`] **and** for an op the frame
//! covers; everything else keeps the bincode body, and the magic is what says so.
//!
//! # Fail loud
//!
//! A body that carries the magic but does not parse is refused with a named
//! [`ForwardFrameError`]. There is no `unwrap_or_default`, no "try bincode after
//! capnp failed", and no partial command: a malformed frame aborts the forward
//! and the caller logs the named error instead of applying a half-decoded op.

use std::collections::{HashMap, HashSet};
use std::io::Cursor;

use bytes::Bytes;
use capnp::message::{Builder, ReaderOptions};
use capnp::serialize;
use indexmap::IndexMap;

use ferrosa_net::protocol::envelope_capnp::forwarded_raft_command::op as fwd_op;
use ferrosa_net::protocol::envelope_capnp::{
    forwarded_raft_command, fwd_caching_params, fwd_clustering_key, fwd_column_mask,
    fwd_column_metadata, fwd_keyspace_metadata, fwd_kv_pair, fwd_node_info, fwd_node_state,
    fwd_optional_text, fwd_replication_params, fwd_string_map, fwd_table_metadata,
    fwd_table_params, fwd_table_updates, FwdClusteringOrder, FwdColumnKind, FwdTableFlag,
};
use ferrosa_schema::metadata::{
    CachingParams, ClusteringOrder, ColumnKind, ColumnMask, ColumnMetadata, KeyspaceMetadata,
    KeyspaceUpdates, ReplicationParams, TableFlag, TableMetadata, TableParams, TableUpdates,
};
use uuid::Uuid;

use crate::raft::{DataMovementEvidence, NodeInfo, NodeState, RaftCommand, RaftOp};

/// Wire magic marking a Cap'n Proto `ForwardedRaftCommand` body: the ASCII
/// bytes `FRWF` (forwarded raft wire frame), big-endian on the wire.
///
/// Distinct from the Accord envelope's `Envelope.magic` (`0x4645_5231`, `FER1`)
/// and from every plausible bincode prefix: a bincode `RaftCommand` begins with
/// the little-endian `u32` variant tag of [`RaftOp`] (0x0000_0000..=0x0000_001a),
/// far below this value, so the two encodings are unambiguous.
pub const FORWARD_RAFT_FRAME_MAGIC: u32 = 0x4652_5746;

/// Version of the `[magic][version]` wire prefix and of the `ForwardedRaftCommand`
/// schema this build emits. Bumped only for an incompatible frame change.
pub const FORWARD_RAFT_FRAME_VERSION: u8 = 1;

/// Length of the `[magic:u32 BE][version:u8]` prefix that precedes the Cap'n
/// Proto message in a framed body.
pub const FORWARD_RAFT_FRAME_PREFIX_LEN: usize = 5;

/// Traversal limit for a forwarded-command frame, in words (8 bytes each).
/// Mirrors `ferrosa_net::protocol::decode_envelope`'s bound. A schema/metadata
/// command is small; the limit is the fail-loud backstop against a hostile frame.
const FRAME_TRAVERSAL_LIMIT_WORDS: usize = 8 * 1024 * 1024;

/// A refusal to encode or decode a forwarded-command body.
///
/// Every arm names what was refused. There is deliberately no arm that means
/// "unreadable, carrying on anyway": a caller that cannot decode a frame must
/// surface this error rather than apply a half-parsed command.
#[derive(Debug, PartialEq, Eq)]
pub enum ForwardFrameError {
    /// The body carried the frame magic at the wire prefix, but the Cap'n Proto
    /// root's own `magic` field did not match — a torn or foreign frame.
    BadMagic { magic: u32 },
    /// The wire prefix, or the Cap'n Proto root's `version`, is not one this
    /// build understands.
    UnsupportedVersion { version: u16 },
    /// The body is shorter than the wire prefix.
    Truncated,
    /// The Cap'n Proto frame did not parse, or a text/data field was malformed.
    Malformed(String),
    /// The op is not one the capnp frame covers; it must travel as bincode.
    NotFramed { op: &'static str },
    /// The body was not a capnp frame and did not decode as bincode either.
    Bincode(String),
}

impl std::fmt::Display for ForwardFrameError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            // Named `decode:` so the existing ack contract (an ack `Err` whose
            // text begins `decode: `) is preserved for the bincode path; the
            // capnp arms name the frame explicitly.
            Self::Bincode(e) => write!(f, "decode: {e}"),
            Self::BadMagic { magic } => write!(
                f,
                "decode: forwarded raft frame: bad magic 0x{magic:08x} (want 0x{FORWARD_RAFT_FRAME_MAGIC:08x})"
            ),
            Self::UnsupportedVersion { version } => write!(
                f,
                "decode: forwarded raft frame: unsupported version {version} (want {FORWARD_RAFT_FRAME_VERSION})"
            ),
            Self::Truncated => write!(
                f,
                "decode: forwarded raft frame: truncated (shorter than the {FORWARD_RAFT_FRAME_PREFIX_LEN}-byte prefix)"
            ),
            Self::Malformed(m) => write!(f, "decode: forwarded raft frame: malformed: {m}"),
            Self::NotFramed { op } => write!(
                f,
                "decode: forwarded raft frame: op {op} is not framed; it travels as bincode"
            ),
        }
    }
}

impl std::error::Error for ForwardFrameError {}

impl From<capnp::Error> for ForwardFrameError {
    fn from(value: capnp::Error) -> Self {
        Self::Malformed(value.to_string())
    }
}

impl From<std::str::Utf8Error> for ForwardFrameError {
    fn from(value: std::str::Utf8Error) -> Self {
        Self::Malformed(format!("invalid UTF-8 text field: {value}"))
    }
}

impl From<capnp::NotInSchema> for ForwardFrameError {
    fn from(value: capnp::NotInSchema) -> Self {
        Self::Malformed(format!("value not in schema: {value}"))
    }
}

/// Whether `op` is one of the schema/membership operations the Cap'n Proto
/// frame carries. Any other variant keeps the bincode body (`is_framed_op`
/// false ⇒ [`encode_forwarded_command`] emits bincode), and the wire magic is
/// what tells the receiver which encoding to read.
pub fn is_framed_op(op: &RaftOp) -> bool {
    matches!(
        op,
        RaftOp::CreateKeyspace(_)
            | RaftOp::DropKeyspace(_)
            | RaftOp::CreateTable(_)
            | RaftOp::DropTable { .. }
            | RaftOp::AlterKeyspace { .. }
            | RaftOp::AlterTable { .. }
            | RaftOp::JoinNode(_)
            | RaftOp::UpdateNodeInfo(_)
            | RaftOp::ApproveNode { .. }
            | RaftOp::SetNodeState { .. }
            | RaftOp::RecordBootstrapComplete { .. }
    )
}

/// The name of `op` for a [`ForwardFrameError::NotFramed`] refusal.
fn op_name(op: &RaftOp) -> &'static str {
    match op {
        RaftOp::CreateKeyspace(_) => "CreateKeyspace",
        RaftOp::DropKeyspace(_) => "DropKeyspace",
        RaftOp::CreateTable(_) => "CreateTable",
        RaftOp::DropTable { .. } => "DropTable",
        RaftOp::AlterKeyspace { .. } => "AlterKeyspace",
        RaftOp::AlterTable { .. } => "AlterTable",
        RaftOp::CreateRole(_) => "CreateRole",
        RaftOp::AlterRole { .. } => "AlterRole",
        RaftOp::DropRole(_) => "DropRole",
        RaftOp::Grant(_) => "Grant",
        RaftOp::Revoke { .. } => "Revoke",
        RaftOp::GrantRole { .. } => "GrantRole",
        RaftOp::RevokeRole { .. } => "RevokeRole",
        RaftOp::CreateIndex(_) => "CreateIndex",
        RaftOp::DropIndex { .. } => "DropIndex",
        RaftOp::IndexStatus { .. } => "IndexStatus",
        RaftOp::CreateType(_) => "CreateType",
        RaftOp::DropType { .. } => "DropType",
        RaftOp::CreateFunction(_) => "CreateFunction",
        RaftOp::DropFunction { .. } => "DropFunction",
        RaftOp::CreateAggregate(_) => "CreateAggregate",
        RaftOp::DropAggregate { .. } => "DropAggregate",
        RaftOp::JoinNode(_) => "JoinNode",
        RaftOp::UpdateNodeInfo(_) => "UpdateNodeInfo",
        RaftOp::LeaveNode { .. } => "LeaveNode",
        RaftOp::AssignTokens { .. } => "AssignTokens",
        RaftOp::UpdateConfig(_) => "UpdateConfig",
        RaftOp::ApproveNode { .. } => "ApproveNode",
        RaftOp::SetNodeState { .. } => "SetNodeState",
        RaftOp::AccordApply { .. } => "AccordApply",
        RaftOp::RecordBootstrapComplete { .. } => "RecordBootstrapComplete",
    }
}

/// Encode `cmd` for a `Message::ClusterMembershipForward` body.
///
/// Chooses the Cap'n Proto frame when `capnp_allowed` (the peer advertised
/// [`ferrosa_net::handshake::CAP_RAFT_SCHEMA_CAPNP`]) **and** `cmd.op` is a framed
/// op; otherwise emits the shipped bincode body. The receiver tells the two apart
/// by the wire magic, so the same body shape works whether or not every member has
/// upgraded.
pub fn encode_forwarded_command(
    cmd: &RaftCommand,
    capnp_allowed: bool,
) -> Result<Bytes, ForwardFrameError> {
    if capnp_allowed && is_framed_op(&cmd.op) {
        Ok(Bytes::from(encode_frame(cmd)?))
    } else {
        bincode::serialize(cmd)
            .map(Bytes::from)
            .map_err(|e| ForwardFrameError::Bincode(e.to_string()))
    }
}

/// Encode `cmd` as a `[magic][version]`-prefixed Cap'n Proto `ForwardedRaftCommand`.
///
/// Refuses ([`ForwardFrameError::NotFramed`]) an op the frame does not cover rather
/// than emitting a frame with an unset union.
pub fn encode_frame(cmd: &RaftCommand) -> Result<Vec<u8>, ForwardFrameError> {
    if !is_framed_op(&cmd.op) {
        return Err(ForwardFrameError::NotFramed {
            op: op_name(&cmd.op),
        });
    }

    let mut message = Builder::new_default();
    {
        let mut root = message.init_root::<forwarded_raft_command::Builder>();
        root.set_magic(FORWARD_RAFT_FRAME_MAGIC);
        root.set_version(u16::from(FORWARD_RAFT_FRAME_VERSION));
        root.set_schema_version(cmd.schema_version.as_bytes());
        write_op(&cmd.op, root.init_op())?;
    }

    let words = serialize::write_message_to_words(&message);
    let mut body = Vec::with_capacity(FORWARD_RAFT_FRAME_PREFIX_LEN + words.len());
    body.extend_from_slice(&FORWARD_RAFT_FRAME_MAGIC.to_be_bytes());
    body.push(FORWARD_RAFT_FRAME_VERSION);
    body.extend_from_slice(&words);
    Ok(body)
}

/// Decode a forwarded-command body: a magic-prefixed Cap'n Proto frame, else
/// bincode (a peer on the previous build sends bincode only).
///
/// A body that carries the magic but does not parse is refused with a named
/// error; it is never retried as bincode.
pub fn decode_forwarded_command(body: &[u8]) -> Result<RaftCommand, ForwardFrameError> {
    if body.len() >= 4
        && u32::from_be_bytes([body[0], body[1], body[2], body[3]]) == FORWARD_RAFT_FRAME_MAGIC
    {
        decode_frame(body)
    } else {
        bincode::deserialize(body).map_err(|e| ForwardFrameError::Bincode(e.to_string()))
    }
}

/// Decode a `[magic][version]`-prefixed Cap'n Proto `ForwardedRaftCommand`.
fn decode_frame(body: &[u8]) -> Result<RaftCommand, ForwardFrameError> {
    if body.len() < FORWARD_RAFT_FRAME_PREFIX_LEN {
        return Err(ForwardFrameError::Truncated);
    }
    // body[0..4] == FORWARD_RAFT_FRAME_MAGIC was checked by the caller.
    let wire_version = body[4];
    if wire_version != FORWARD_RAFT_FRAME_VERSION {
        return Err(ForwardFrameError::UnsupportedVersion {
            version: u16::from(wire_version),
        });
    }

    let reader = serialize::read_message(
        &mut Cursor::new(&body[FORWARD_RAFT_FRAME_PREFIX_LEN..]),
        ReaderOptions {
            traversal_limit_in_words: Some(FRAME_TRAVERSAL_LIMIT_WORDS),
            nesting_limit: 64,
        },
    )?;
    let root = reader.get_root::<forwarded_raft_command::Reader>()?;

    let magic = root.get_magic();
    if magic != FORWARD_RAFT_FRAME_MAGIC {
        return Err(ForwardFrameError::BadMagic { magic });
    }
    let inner_version = root.get_version();
    if inner_version != u16::from(FORWARD_RAFT_FRAME_VERSION) {
        return Err(ForwardFrameError::UnsupportedVersion {
            version: inner_version,
        });
    }

    let schema_version = read_uuid("schemaVersion", root.get_schema_version()?)?;
    let op = read_op(root.get_op())?;
    Ok(RaftCommand { op, schema_version })
}

// ---------------------------------------------------------------------------
// Write path
// ---------------------------------------------------------------------------

fn write_op(op: &RaftOp, mut builder: fwd_op::Builder<'_>) -> Result<(), ForwardFrameError> {
    match op {
        RaftOp::CreateKeyspace(ks) => write_keyspace_metadata(ks, builder.init_create_keyspace()),
        RaftOp::DropKeyspace(name) => builder.set_drop_keyspace(name.as_str()),
        RaftOp::CreateTable(table) => write_table_metadata(table, builder.init_create_table()),
        RaftOp::DropTable { keyspace, table } => {
            let mut b = builder.init_drop_table();
            b.set_keyspace(keyspace.as_str());
            b.set_table(table.as_str());
        }
        RaftOp::AlterKeyspace { name, updates } => {
            let mut b = builder.init_alter_keyspace();
            b.set_name(name.as_str());
            if let Some(replication) = &updates.replication {
                write_replication_params(replication, b.reborrow().init_replication());
            }
            if let Some(durable_writes) = updates.durable_writes {
                let mut d = b.init_durable_writes();
                d.set_present(true);
                d.set_value(durable_writes);
            }
        }
        RaftOp::AlterTable {
            keyspace,
            table,
            updates,
        } => {
            let mut b = builder.init_alter_table();
            b.set_keyspace(keyspace.as_str());
            b.set_table(table.as_str());
            write_table_updates(updates, b.init_updates());
        }
        RaftOp::JoinNode(info) => write_node_info(info, builder.init_join_node()),
        RaftOp::UpdateNodeInfo(info) => write_node_info(info, builder.init_update_node_info()),
        RaftOp::ApproveNode { host_id } => builder.set_approve_node(host_id.as_bytes()),
        RaftOp::SetNodeState { node_id, state } => {
            let mut b = builder.init_set_node_state();
            b.set_node_id(*node_id);
            write_node_state(*state, b.init_state());
        }
        RaftOp::RecordBootstrapComplete { node_id, evidence } => {
            let mut b = builder.init_record_bootstrap_complete();
            b.set_node_id(*node_id);
            let mut e = b.init_evidence();
            e.set_sessions(evidence.sessions);
            e.set_ranges(evidence.ranges);
            e.set_partitions(evidence.partitions);
        }
        other => return Err(ForwardFrameError::NotFramed { op: op_name(other) }),
    }
    Ok(())
}

fn write_kv_pairs(
    map: &HashMap<String, String>,
    builder: capnp::struct_list::Builder<'_, fwd_kv_pair::Owned>,
) {
    let mut builder = builder;
    for (idx, (key, value)) in map.iter().enumerate() {
        let mut pair = builder.reborrow().get(idx as u32);
        pair.set_key(key.as_str());
        pair.set_value(value.as_str());
    }
}

fn write_text_list(items: &[String], builder: capnp::text_list::Builder<'_>) {
    let mut builder = builder;
    for (idx, item) in items.iter().enumerate() {
        builder.set(idx as u32, item.as_str());
    }
}

fn write_flag_list(
    flags: &HashSet<TableFlag>,
    builder: capnp::enum_list::Builder<'_, FwdTableFlag>,
) {
    let mut builder = builder;
    for (idx, flag) in flags.iter().enumerate() {
        builder.set(idx as u32, table_flag_tag(*flag));
    }
}

fn write_clustering_key_list(
    entries: &[(String, ClusteringOrder)],
    builder: capnp::struct_list::Builder<'_, fwd_clustering_key::Owned>,
) {
    let mut builder = builder;
    for (idx, (name, order)) in entries.iter().enumerate() {
        let mut entry = builder.reborrow().get(idx as u32);
        entry.set_name(name.as_str());
        entry.set_order(clustering_order_tag(*order));
    }
}

fn write_replication_params(
    replication: &ReplicationParams,
    mut builder: fwd_replication_params::Builder<'_>,
) {
    builder.set_strategy(replication.strategy.as_str());
    write_kv_pairs(
        &replication.options,
        builder.init_options(replication.options.len() as u32),
    );
}

fn write_keyspace_metadata(ks: &KeyspaceMetadata, mut builder: fwd_keyspace_metadata::Builder<'_>) {
    builder.set_name(ks.name.as_str());
    builder.set_durable_writes(ks.durable_writes);
    write_replication_params(&ks.replication, builder.init_replication());
}

fn write_column_mask(mask: &ColumnMask, mut builder: fwd_column_mask::Builder<'_>) {
    builder.set_function_name(mask.function_name.as_str());
    write_text_list(
        &mask.arguments,
        builder.init_arguments(mask.arguments.len() as u32),
    );
}

fn write_column_metadata(column: &ColumnMetadata, mut builder: fwd_column_metadata::Builder<'_>) {
    builder.set_name(column.name.as_str());
    builder.set_kind(column_kind_tag(column.kind));
    builder.set_position(column.position);
    builder.set_column_type(column.column_type.as_str());
    builder.set_clustering_order(clustering_order_tag(column.clustering_order));
    if let Some(mask) = &column.mask {
        write_column_mask(mask, builder.init_mask());
    }
}

fn write_table_params(params: &TableParams, mut builder: fwd_table_params::Builder<'_>) {
    builder.set_bloom_filter_fp_chance(params.bloom_filter_fp_chance);
    {
        let mut caching = builder.reborrow().init_caching();
        caching.set_keys(params.caching.keys.as_str());
        caching.set_rows_per_partition(params.caching.rows_per_partition.as_str());
    }
    builder.set_comment(params.comment.as_str());
    write_kv_pairs(
        &params.compaction,
        builder
            .reborrow()
            .init_compaction(params.compaction.len() as u32),
    );
    write_kv_pairs(
        &params.compression,
        builder
            .reborrow()
            .init_compression(params.compression.len() as u32),
    );
    builder.set_crc_check_chance(params.crc_check_chance);
    builder.set_default_time_to_live(params.default_time_to_live);
    builder.set_gc_grace_seconds(params.gc_grace_seconds);
    builder.set_max_index_interval(params.max_index_interval);
    builder.set_min_index_interval(params.min_index_interval);
    builder.set_memtable_flush_period_in_ms(params.memtable_flush_period_in_ms);
    builder.set_speculative_retry(params.speculative_retry.as_str());
    builder.set_additional_write_policy(params.additional_write_policy.as_str());
    builder.set_cdc(params.cdc);
    builder.set_read_repair(params.read_repair.as_str());
    builder.set_allow_auto_snapshot(params.allow_auto_snapshot);
    builder.set_incremental_backups(params.incremental_backups);
}

fn write_string_map(map: &HashMap<String, String>, builder: fwd_string_map::Builder<'_>) {
    write_kv_pairs(map, builder.init_entries(map.len() as u32));
}

fn write_table_updates(updates: &TableUpdates, mut builder: fwd_table_updates::Builder<'_>) {
    if let Some(params) = &updates.params {
        write_table_params(params, builder.reborrow().init_params());
    }
    {
        let mut list = builder
            .reborrow()
            .init_add_columns(updates.add_columns.len() as u32);
        for (idx, column) in updates.add_columns.iter().enumerate() {
            write_column_metadata(column, list.reborrow().get(idx as u32));
        }
    }
    write_text_list(
        &updates.drop_columns,
        builder
            .reborrow()
            .init_drop_columns(updates.drop_columns.len() as u32),
    );
    if let Some(extensions) = &updates.extensions {
        write_string_map(extensions, builder.init_extensions());
    }
}

fn write_table_metadata(table: &TableMetadata, mut builder: fwd_table_metadata::Builder<'_>) {
    builder.set_keyspace(table.keyspace.as_str());
    builder.set_name(table.name.as_str());
    builder.set_id(table.id.as_bytes());
    {
        let mut list = builder.reborrow().init_columns(table.columns.len() as u32);
        for (idx, column) in table.columns.values().enumerate() {
            write_column_metadata(column, list.reborrow().get(idx as u32));
        }
    }
    write_text_list(
        &table.partition_key,
        builder
            .reborrow()
            .init_partition_key(table.partition_key.len() as u32),
    );
    write_clustering_key_list(
        &table.clustering_key,
        builder
            .reborrow()
            .init_clustering_key(table.clustering_key.len() as u32),
    );
    write_table_params(&table.params, builder.reborrow().init_params());
    write_flag_list(
        &table.flags,
        builder.reborrow().init_flags(table.flags.len() as u32),
    );
    write_kv_pairs(
        &table.extensions,
        builder
            .reborrow()
            .init_extensions(table.extensions.len() as u32),
    );
    builder.set_is_system(table.is_system);
}

fn write_node_state(state: NodeState, builder: fwd_node_state::Builder<'_>) {
    let mut union = builder.init_state();
    match state {
        NodeState::Joining => union.set_joining(()),
        NodeState::Normal => union.set_normal(()),
        NodeState::Leaving => union.set_leaving(()),
        NodeState::Decommissioned => union.set_decommissioned(()),
        NodeState::Learner { owns_tokens } => union.set_learner(owns_tokens),
    }
}

fn write_node_info(info: &NodeInfo, mut builder: fwd_node_info::Builder<'_>) {
    builder.set_host_id(info.host_id.as_bytes());
    builder.set_addr(info.addr.as_str());
    builder.set_data_center(info.data_center.as_str());
    builder.set_rack(info.rack.as_str());
    write_node_state(info.state, builder.reborrow().init_state());
    let mut cql_broadcast = builder.init_cql_broadcast();
    match &info.cql_broadcast {
        Some(value) => {
            cql_broadcast.set_present(true);
            cql_broadcast.set_value(value.as_str());
        }
        None => cql_broadcast.set_present(false),
    }
}

fn column_kind_tag(kind: ColumnKind) -> FwdColumnKind {
    match kind {
        ColumnKind::PartitionKey => FwdColumnKind::PartitionKey,
        ColumnKind::Clustering => FwdColumnKind::Clustering,
        ColumnKind::Regular => FwdColumnKind::Regular,
        ColumnKind::Static => FwdColumnKind::Static,
    }
}

fn clustering_order_tag(order: ClusteringOrder) -> FwdClusteringOrder {
    match order {
        ClusteringOrder::Asc => FwdClusteringOrder::Asc,
        ClusteringOrder::Desc => FwdClusteringOrder::Desc,
        ClusteringOrder::None => FwdClusteringOrder::None,
    }
}

fn table_flag_tag(flag: TableFlag) -> FwdTableFlag {
    match flag {
        TableFlag::Compound => FwdTableFlag::Compound,
        TableFlag::Counter => FwdTableFlag::Counter,
        TableFlag::Dense => FwdTableFlag::Dense,
        TableFlag::Super => FwdTableFlag::Super,
    }
}

// ---------------------------------------------------------------------------
// Read path
// ---------------------------------------------------------------------------

fn read_op(reader: fwd_op::Reader<'_>) -> Result<RaftOp, ForwardFrameError> {
    Ok(match reader.which()? {
        fwd_op::CreateKeyspace(ks) => RaftOp::CreateKeyspace(read_keyspace_metadata(ks?)?),
        fwd_op::DropKeyspace(name) => RaftOp::DropKeyspace(name?.to_string()?),
        fwd_op::CreateTable(table) => RaftOp::CreateTable(Box::new(read_table_metadata(table?)?)),
        fwd_op::DropTable(entry) => {
            let entry = entry?;
            RaftOp::DropTable {
                keyspace: entry.get_keyspace()?.to_string()?,
                table: entry.get_table()?.to_string()?,
            }
        }
        fwd_op::AlterKeyspace(entry) => {
            let entry = entry?;
            let replication = if entry.has_replication() {
                Some(read_replication_params(entry.get_replication()?)?)
            } else {
                None
            };
            let durable_writes = {
                let optional = entry.get_durable_writes()?;
                if optional.get_present() {
                    Some(optional.get_value())
                } else {
                    None
                }
            };
            RaftOp::AlterKeyspace {
                name: entry.get_name()?.to_string()?,
                updates: KeyspaceUpdates {
                    replication,
                    durable_writes,
                },
            }
        }
        fwd_op::AlterTable(entry) => {
            let entry = entry?;
            RaftOp::AlterTable {
                keyspace: entry.get_keyspace()?.to_string()?,
                table: entry.get_table()?.to_string()?,
                updates: Box::new(read_table_updates(entry.get_updates()?)?),
            }
        }
        fwd_op::JoinNode(info) => RaftOp::JoinNode(read_node_info(info?)?),
        fwd_op::UpdateNodeInfo(info) => RaftOp::UpdateNodeInfo(read_node_info(info?)?),
        fwd_op::ApproveNode(host_id) => RaftOp::ApproveNode {
            host_id: read_uuid("approveNode", host_id?)?,
        },
        fwd_op::SetNodeState(entry) => {
            let entry = entry?;
            RaftOp::SetNodeState {
                node_id: entry.get_node_id(),
                state: read_node_state(entry.get_state()?)?,
            }
        }
        fwd_op::RecordBootstrapComplete(entry) => {
            let entry = entry?;
            let evidence = entry.get_evidence()?;
            RaftOp::RecordBootstrapComplete {
                node_id: entry.get_node_id(),
                evidence: DataMovementEvidence {
                    sessions: evidence.get_sessions(),
                    ranges: evidence.get_ranges(),
                    partitions: evidence.get_partitions(),
                },
            }
        }
    })
}

fn read_kv_pairs(
    reader: capnp::struct_list::Reader<'_, fwd_kv_pair::Owned>,
) -> Result<HashMap<String, String>, ForwardFrameError> {
    let mut map = HashMap::with_capacity(reader.len() as usize);
    for idx in 0..reader.len() {
        let pair = reader.get(idx);
        map.insert(pair.get_key()?.to_string()?, pair.get_value()?.to_string()?);
    }
    Ok(map)
}

fn read_replication_params(
    reader: fwd_replication_params::Reader<'_>,
) -> Result<ReplicationParams, ForwardFrameError> {
    Ok(ReplicationParams {
        strategy: reader.get_strategy()?.to_string()?,
        options: read_kv_pairs(reader.get_options()?)?,
    })
}

fn read_keyspace_metadata(
    reader: fwd_keyspace_metadata::Reader<'_>,
) -> Result<KeyspaceMetadata, ForwardFrameError> {
    Ok(KeyspaceMetadata {
        name: reader.get_name()?.to_string()?,
        durable_writes: reader.get_durable_writes(),
        replication: read_replication_params(reader.get_replication()?)?,
    })
}

fn read_text_list(reader: capnp::text_list::Reader<'_>) -> Result<Vec<String>, ForwardFrameError> {
    let mut items = Vec::with_capacity(reader.len() as usize);
    for idx in 0..reader.len() {
        items.push(reader.get(idx)?.to_string()?);
    }
    Ok(items)
}

fn read_column_metadata(
    reader: fwd_column_metadata::Reader<'_>,
) -> Result<ColumnMetadata, ForwardFrameError> {
    Ok(ColumnMetadata {
        name: reader.get_name()?.to_string()?,
        kind: column_kind_from_tag(reader.get_kind()?),
        position: reader.get_position(),
        column_type: reader.get_column_type()?.to_string()?,
        clustering_order: clustering_order_from_tag(reader.get_clustering_order()?),
        mask: if reader.has_mask() {
            let mask = reader.get_mask()?;
            Some(ColumnMask {
                function_name: mask.get_function_name()?.to_string()?,
                arguments: read_text_list(mask.get_arguments()?)?,
            })
        } else {
            None
        },
    })
}

fn read_table_params(
    reader: fwd_table_params::Reader<'_>,
) -> Result<TableParams, ForwardFrameError> {
    let caching: fwd_caching_params::Reader<'_> = reader.get_caching()?;
    Ok(TableParams {
        bloom_filter_fp_chance: reader.get_bloom_filter_fp_chance(),
        caching: CachingParams {
            keys: caching.get_keys()?.to_string()?,
            rows_per_partition: caching.get_rows_per_partition()?.to_string()?,
        },
        comment: reader.get_comment()?.to_string()?,
        compaction: read_kv_pairs(reader.get_compaction()?)?,
        compression: read_kv_pairs(reader.get_compression()?)?,
        crc_check_chance: reader.get_crc_check_chance(),
        default_time_to_live: reader.get_default_time_to_live(),
        gc_grace_seconds: reader.get_gc_grace_seconds(),
        max_index_interval: reader.get_max_index_interval(),
        min_index_interval: reader.get_min_index_interval(),
        memtable_flush_period_in_ms: reader.get_memtable_flush_period_in_ms(),
        speculative_retry: reader.get_speculative_retry()?.to_string()?,
        additional_write_policy: reader.get_additional_write_policy()?.to_string()?,
        cdc: reader.get_cdc(),
        read_repair: reader.get_read_repair()?.to_string()?,
        allow_auto_snapshot: reader.get_allow_auto_snapshot(),
        incremental_backups: reader.get_incremental_backups(),
    })
}

fn read_table_updates(
    reader: fwd_table_updates::Reader<'_>,
) -> Result<TableUpdates, ForwardFrameError> {
    let params = if reader.has_params() {
        Some(read_table_params(reader.get_params()?)?)
    } else {
        None
    };

    let columns_reader = reader.get_add_columns()?;
    let mut add_columns = Vec::with_capacity(columns_reader.len() as usize);
    for idx in 0..columns_reader.len() {
        add_columns.push(read_column_metadata(columns_reader.get(idx))?);
    }

    let extensions = if reader.has_extensions() {
        Some(read_kv_pairs(reader.get_extensions()?.get_entries()?)?)
    } else {
        None
    };

    Ok(TableUpdates {
        params,
        add_columns,
        drop_columns: read_text_list(reader.get_drop_columns()?)?,
        extensions,
    })
}

fn read_table_metadata(
    reader: fwd_table_metadata::Reader<'_>,
) -> Result<TableMetadata, ForwardFrameError> {
    let columns_reader = reader.get_columns()?;
    let mut columns = IndexMap::with_capacity(columns_reader.len() as usize);
    for idx in 0..columns_reader.len() {
        let column = read_column_metadata(columns_reader.get(idx))?;
        columns.insert(column.name.clone(), column);
    }

    let clustering_reader = reader.get_clustering_key()?;
    let mut clustering_key = Vec::with_capacity(clustering_reader.len() as usize);
    for idx in 0..clustering_reader.len() {
        let entry: fwd_clustering_key::Reader<'_> = clustering_reader.get(idx);
        clustering_key.push((
            entry.get_name()?.to_string()?,
            clustering_order_from_tag(entry.get_order()?),
        ));
    }

    let flags_reader = reader.get_flags()?;
    let mut flags = HashSet::with_capacity(flags_reader.len() as usize);
    for idx in 0..flags_reader.len() {
        flags.insert(table_flag_from_tag(flags_reader.get(idx)?));
    }

    Ok(TableMetadata {
        keyspace: reader.get_keyspace()?.to_string()?,
        name: reader.get_name()?.to_string()?,
        id: read_uuid("table.id", reader.get_id()?)?,
        columns,
        partition_key: read_text_list(reader.get_partition_key()?)?,
        clustering_key,
        params: read_table_params(reader.get_params()?)?,
        flags,
        extensions: read_kv_pairs(reader.get_extensions()?)?,
        is_system: reader.get_is_system(),
    })
}

fn read_node_state(reader: fwd_node_state::Reader<'_>) -> Result<NodeState, ForwardFrameError> {
    use ferrosa_net::protocol::envelope_capnp::fwd_node_state::state;
    Ok(match reader.get_state().which()? {
        state::Joining(()) => NodeState::Joining,
        state::Normal(()) => NodeState::Normal,
        state::Leaving(()) => NodeState::Leaving,
        state::Decommissioned(()) => NodeState::Decommissioned,
        state::Learner(owns_tokens) => NodeState::Learner { owns_tokens },
    })
}

fn read_node_info(reader: fwd_node_info::Reader<'_>) -> Result<NodeInfo, ForwardFrameError> {
    let cql_broadcast_reader: fwd_optional_text::Reader<'_> = reader.get_cql_broadcast()?;
    let cql_broadcast = if cql_broadcast_reader.get_present() {
        Some(cql_broadcast_reader.get_value()?.to_string()?)
    } else {
        None
    };
    Ok(NodeInfo {
        host_id: read_uuid("nodeInfo.hostId", reader.get_host_id()?)?,
        addr: reader.get_addr()?.to_string()?,
        data_center: reader.get_data_center()?.to_string()?,
        rack: reader.get_rack()?.to_string()?,
        state: read_node_state(reader.get_state()?)?,
        cql_broadcast,
    })
}

fn read_uuid(field: &str, bytes: &[u8]) -> Result<Uuid, ForwardFrameError> {
    Uuid::from_slice(bytes).map_err(|e| ForwardFrameError::Malformed(format!("{field}: {e}")))
}

fn column_kind_from_tag(tag: FwdColumnKind) -> ColumnKind {
    match tag {
        FwdColumnKind::PartitionKey => ColumnKind::PartitionKey,
        FwdColumnKind::Clustering => ColumnKind::Clustering,
        FwdColumnKind::Regular => ColumnKind::Regular,
        FwdColumnKind::Static => ColumnKind::Static,
    }
}

fn clustering_order_from_tag(tag: FwdClusteringOrder) -> ClusteringOrder {
    match tag {
        FwdClusteringOrder::Asc => ClusteringOrder::Asc,
        FwdClusteringOrder::Desc => ClusteringOrder::Desc,
        FwdClusteringOrder::None => ClusteringOrder::None,
    }
}

fn table_flag_from_tag(tag: FwdTableFlag) -> TableFlag {
    match tag {
        FwdTableFlag::Compound => TableFlag::Compound,
        FwdTableFlag::Counter => TableFlag::Counter,
        FwdTableFlag::Dense => TableFlag::Dense,
        FwdTableFlag::Super => TableFlag::Super,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_command(op: RaftOp) -> RaftCommand {
        RaftCommand {
            op,
            schema_version: Uuid::from_u128(0xfeed_face_dead_beef_1234_5678_9abc_def0),
        }
    }

    /// The wire magic and version are a SHIPPED contract: the two constants and
    /// the framed bytes a peer sees must not drift apart. This pins the literal
    /// the fixture in `raft_forward`'s tests also spells out.
    #[test]
    fn frame_magic_and_version_are_the_pinned_wire_constants() {
        assert_eq!(FORWARD_RAFT_FRAME_MAGIC, 0x4652_5746);
        assert_eq!(FORWARD_RAFT_FRAME_VERSION, 1);

        let cmd = sample_command(RaftOp::DropKeyspace("app".to_string()));
        let body = encode_frame(&cmd).expect("framed");
        assert_eq!(&body[..4], &0x4652_5746u32.to_be_bytes());
        assert_eq!(body[4], 1);
    }

    /// The magic is a body-level discriminator, because a Cap'n Proto message's
    /// segment framing starts with a segment-table word (often four zero bytes),
    /// not with the struct's `magic` field.
    #[test]
    fn framed_body_does_not_start_with_the_inner_magic_field() {
        let cmd = sample_command(RaftOp::DropKeyspace("app".to_string()));
        let body = encode_frame(&cmd).expect("framed");
        let words = &body[FORWARD_RAFT_FRAME_PREFIX_LEN..];
        // The first segment-table word's low 32 bits are the segment count - 1.
        assert_eq!(
            u32::from_le_bytes([words[0], words[1], words[2], words[3]]),
            0
        );
    }

    /// A frame that carries the magic but cannot parse is REFUSED as a frame; it
    /// is never retried as bincode (which would silently accept a mangled payload
    /// whenever the bytes happened to be valid bincode).
    #[test]
    fn corrupt_frame_is_refused_as_a_frame_not_retried_as_bincode() {
        let mut body = Vec::new();
        body.extend_from_slice(&FORWARD_RAFT_FRAME_MAGIC.to_be_bytes());
        body.push(FORWARD_RAFT_FRAME_VERSION);
        body.extend_from_slice(b"not a capnp message at all");

        let err = decode_forwarded_command(&body).expect_err("corrupt frame refused");
        assert!(
            matches!(err, ForwardFrameError::Malformed(_)),
            "got {err:?}"
        );
        assert!(err.to_string().contains("forwarded raft frame"));
    }

    /// A body shorter than the prefix that still carries the magic word is refused
    /// as truncated.
    #[test]
    fn magic_only_prefix_is_refused_as_truncated() {
        let body = FORWARD_RAFT_FRAME_MAGIC.to_be_bytes().to_vec();
        let err = decode_forwarded_command(&body).expect_err("truncated frame refused");
        assert_eq!(err, ForwardFrameError::Truncated);
    }

    /// The op coverage predicate is the sender's fallback decision: exactly the
    /// schema/membership ops are framed; every other admin variant keeps bincode.
    #[test]
    fn is_framed_op_covers_only_the_schema_and_membership_ops() {
        assert!(is_framed_op(&RaftOp::DropKeyspace("app".to_string())));
        assert!(is_framed_op(&RaftOp::ApproveNode {
            host_id: Uuid::nil()
        }));
        assert!(!is_framed_op(&RaftOp::DropRole("analyst".to_string())));
        assert!(!is_framed_op(&RaftOp::LeaveNode { node_id: 3 }));
    }
}
