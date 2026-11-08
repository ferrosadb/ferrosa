//! Module: `bigserial` ids without a sequence (decision D15; Lean `Ferrosa.Sql.Serial`).
//!
//! A `bigserial` column gets a server-generated 64-bit id when an INSERT omits it:
//!
//! ```text
//! id = millis << 22 | node << 12 | seq      (41 + 10 + 12 = 63 bits)
//! ```
//!
//! * `millis` — milliseconds since [`ID_EPOCH_MILLIS`] (2025-01-01T00:00:00Z), 41 bits
//!   (until 2094);
//! * `node` — this process's node tag, 10 bits ([`node_tag`]);
//! * `seq` — a counter within one millisecond, 12 bits; when it runs out the generator
//!   borrows the next millisecond instead of waiting (HLC-style), so ids never repeat and
//!   never go backwards on one node even if the wall clock steps back.
//!
//! Properties (proved in the Lean model): the composition is injective
//! (`compose_injective`); ids from distinct node tags never collide
//! (`distinct_nodes_never_collide`); ids are positive and fit a signed `bigint`
//! (`fits_a_positive_bigint`); one node never repeats (`next_strictly_increases`,
//! `same_node_distinct_pairs`). Ids are NOT dense and NOT ordered across nodes. `serial`
//! (int4) cannot hold one (`serial_cannot_hold_an_id`), so `serial`/`smallserial` are refused
//! at DDL time.
//!
//! The node tag. Uniqueness across nodes needs distinct tags, and nothing in the front end
//! assigns small unique node numbers yet. The tag is `FERROSA_PG_SERIAL_NODE_TAG` (0–1023)
//! when set — the operator gives each node a distinct one; otherwise it is drawn at random
//! once per process and a WARN says that cross-node uniqueness is then probabilistic (two
//! nodes collide on the tag with probability 1/1024, and their ids can then collide only if
//! minted in the same millisecond with the same sequence number). Deriving it from cluster
//! membership is the remaining work (oban-compat.md).
//!
//! Last revised: 2026-11-08

use std::sync::{Mutex, OnceLock};

/// The id epoch: 2025-01-01T00:00:00Z, in Unix milliseconds.
// Consumed by INSERT's bigserial default (W2 lane, feat/pg-w2-dml); here only by tests.
#[cfg_attr(not(test), allow(dead_code))]
pub(crate) const ID_EPOCH_MILLIS: u64 = 1_735_689_600_000;
const NODE_BITS: u32 = 10;
const SEQ_BITS: u32 = 12;
const MAX_NODE: u64 = (1 << NODE_BITS) - 1;
const MAX_SEQ: u64 = (1 << SEQ_BITS) - 1;
const MAX_MILLIS: u64 = (1 << 41) - 1;
/// The environment variable naming this node's tag.
// Consumed by INSERT's bigserial default (W2 lane, feat/pg-w2-dml); here only by tests.
#[cfg_attr(not(test), allow(dead_code))]
pub(crate) const NODE_TAG_ENV: &str = "FERROSA_PG_SERIAL_NODE_TAG";

/// `millis << 22 | node << 12 | seq`. The Lean model's `compose`.
pub(crate) fn compose(millis: u64, node: u64, seq: u64) -> i64 {
    assert!(
        millis <= MAX_MILLIS,
        "serial id clock past its 41-bit range"
    );
    assert!(
        node <= MAX_NODE && seq <= MAX_SEQ,
        "serial id field out of range"
    );
    // 41 + 10 + 12 = 63 bits: always a non-negative i64.
    ((millis << (NODE_BITS + SEQ_BITS)) | (node << SEQ_BITS) | seq) as i64
}

/// One node's id generator: the last `(millis, seq)` it issued. The Lean model's `Gen`.
#[derive(Debug)]
pub(crate) struct SerialIdGenerator {
    node: u64,
    last: Mutex<(u64, u64)>,
}

impl SerialIdGenerator {
    pub(crate) fn new(node: u16) -> Self {
        let node = u64::from(node);
        assert!(
            node <= MAX_NODE,
            "serial node tag {node} exceeds {MAX_NODE}"
        );
        Self {
            node,
            last: Mutex::new((0, 0)),
        }
    }

    /// The next id at wall-clock `now_millis` (milliseconds since [`ID_EPOCH_MILLIS`]).
    /// The Lean model's `Gen.next`: the clock if it advanced, else the next sequence
    /// number, else the next millisecond.
    pub(crate) fn next_at(&self, now_millis: u64) -> i64 {
        let mut last = self
            .last
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let (millis, seq) = *last;
        let next = if millis < now_millis {
            (now_millis, 0)
        } else if seq < MAX_SEQ {
            (millis, seq + 1)
        } else {
            (millis + 1, 0)
        };
        debug_assert!(next > *last, "the issued pair must strictly increase");
        *last = next;
        compose(next.0, self.node, next.1)
    }

    /// The next id at the current wall clock.
    #[cfg_attr(not(test), allow(dead_code))] // see `next_id`
    pub(crate) fn next(&self) -> i64 {
        self.next_at(now_millis())
    }
}

// Consumed by INSERT's bigserial default (W2 lane, feat/pg-w2-dml); here only by tests.
#[cfg_attr(not(test), allow(dead_code))]
fn now_millis() -> u64 {
    let unix = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis())
        .unwrap_or(0);
    u64::try_from(unix)
        .unwrap_or(u64::MAX)
        .saturating_sub(ID_EPOCH_MILLIS)
        .max(1)
}

/// This process's node tag: `FERROSA_PG_SERIAL_NODE_TAG`, else a random tag (logged).
// Consumed by INSERT's bigserial default (W2 lane, feat/pg-w2-dml); here only by tests.
#[cfg_attr(not(test), allow(dead_code))]
pub(crate) fn node_tag() -> u16 {
    static TAG: OnceLock<u16> = OnceLock::new();
    *TAG.get_or_init(|| match std::env::var(NODE_TAG_ENV) {
        Ok(text) => match text.trim().parse::<u16>() {
            Ok(tag) if u64::from(tag) <= MAX_NODE => tag,
            _ => panic!("{NODE_TAG_ENV}={text:?} is not a node tag in 0..={MAX_NODE}"),
        },
        Err(_) => {
            let tag = (uuid::Uuid::new_v4().as_u128() as u16) & (MAX_NODE as u16);
            tracing::warn!(
                tag,
                "{NODE_TAG_ENV} is not set: bigserial ids use a random node tag, so their \
                 uniqueness across nodes is probabilistic; give each node a distinct tag"
            );
            tag
        }
    })
}

/// The next `bigserial` id from this process's generator.
// Consumed by INSERT's bigserial default (W2 lane, feat/pg-w2-dml); here only by tests.
#[cfg_attr(not(test), allow(dead_code))]
pub(crate) fn next_id() -> i64 {
    static GENERATOR: OnceLock<SerialIdGenerator> = OnceLock::new();
    GENERATOR
        .get_or_init(|| SerialIdGenerator::new(node_tag()))
        .next()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;

    /// `compose_injective`: the fields read back from the id.
    #[test]
    fn fields_read_back() {
        let id = compose(123_456, 789, 4_000) as u64;
        assert_eq!(id >> 22, 123_456);
        assert_eq!((id >> 12) & MAX_NODE, 789);
        assert_eq!(id & MAX_SEQ, 4_000);
    }

    /// `fits_a_positive_bigint` and `serial_cannot_hold_an_id`.
    #[test]
    fn ids_are_positive_bigints_too_wide_for_int4() {
        let max = compose(MAX_MILLIS, MAX_NODE, MAX_SEQ);
        assert!(max > 0, "the largest id is still positive");
        assert!(compose(512, 0, 0) > i64::from(i32::MAX));
    }

    /// `next_strictly_increases`: within one millisecond, across the sequence limit, and
    /// with the clock stepping backwards.
    #[test]
    fn one_node_never_repeats_or_goes_back() {
        let generator = SerialIdGenerator::new(7);
        let mut seen = HashSet::new();
        let mut previous = 0;
        for i in 0..10_000u64 {
            let now = if i < 9_000 { 1_000 } else { 500 }; // frozen, then stepped back
            let id = generator.next_at(now);
            assert!(id > previous, "ids strictly increase");
            assert!(seen.insert(id), "no id repeats");
            previous = id;
        }
    }

    /// The process-wide generator hands out increasing positive ids.
    #[test]
    fn next_id_is_positive_and_increasing() {
        let a = next_id();
        let b = next_id();
        assert!(a > 0 && b > a);
        assert!(u64::from(node_tag()) <= MAX_NODE);
    }

    /// `distinct_nodes_never_collide`: the same clock and sequence on two tags.
    #[test]
    fn distinct_node_tags_never_collide() {
        let a = SerialIdGenerator::new(1);
        let b = SerialIdGenerator::new(2);
        let ids_a: HashSet<i64> = (0..5_000).map(|_| a.next_at(42)).collect();
        assert!((0..5_000).all(|_| !ids_a.contains(&b.next_at(42))));
    }
}
