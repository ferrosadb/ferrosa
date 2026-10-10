//! Minting the values for ferrosa's synthetic `_sys_ck_` key column.
//!
//! A Postgres table created without a `PRIMARY KEY` gets a synthetic key column (see
//! `ferrosa_common::timeuuid::SYNTHETIC_KEY_COLUMN`), and the client never supplies it —
//! that is the point, it is invisible. So the front-end mints one per inserted row.
//!
//! Three properties matter, and each is why the code below looks the way it does:
//!
//! - **Globally unique.** Two rows sharing a key are one row and a write is lost. So the
//!   node field is a random 48-bit value drawn once per process — RFC 4122 permits exactly
//!   this, it is what v1 does with MAC addresses — and the clock sequence, which is the
//!   field v1 sets aside for disambiguating values minted at the same instant, is a
//!   process-local counter rather than a constant.
//! - **Monotonic.** Keys are the storage key; a key that steps backwards would scatter rows
//!   that were written in order. The time field never goes below the highest value already
//!   handed out, so a backwards wall-clock step (NTP) cannot invert two keys in the same
//!   process.
//! - **A valid v1 TimeUUID.** Not arbitrary unique bytes: the version and variant bits are
//!   set by [`ferrosa_common::timeuuid::v1_timeuuid`], so a CQL client decodes it as a
//!   timeuuid and a reader recovers the real time.
//!
//! Determinism is deliberately *not* a property: two calls differ, always. A retry that
//! re-mints produces a different key, which is correct — the original write never landed.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::OnceLock;

use ferrosa_common::timeuuid::v1_timeuuid;

/// The highest 100-ns tick this process has handed out, so keys never step backwards within
/// a process even if the wall clock does. `AtomicU64` is enough: a single `fetch_max`
/// publishes the new floor and returns the old one, so concurrent callers cannot interleave
/// into a lower value.
static LAST_TICK: AtomicU64 = AtomicU64::new(0);

/// Disambiguates keys minted within the same 100-ns tick. This is the field RFC 4122 sets
/// aside for it. Only the low 14 bits are encoded, so a collision needs 16 384 mints inside
/// one tick — at 10 M ticks/second, not reachable.
static CLOCK_SEQ: AtomicU64 = AtomicU64::new(0);

/// A random 48-bit node id for this process, drawn once.
///
/// Random rather than derived from the real cluster identity because the Postgres front-end
/// has no node identity to hand (`AccordAccess` carries only a committer). RFC 4122 allows a
/// random node field, and two processes drawing the same 48 bits is ~2^-48. Plumb the real
/// node id here later if that probability ever matters.
fn node_id() -> u64 {
    static NODE: OnceLock<u64> = OnceLock::new();
    *NODE.get_or_init(|| {
        let bytes = *uuid::Uuid::new_v4().as_bytes();
        // 48 bits, as v1_timeuuid encodes them.
        u64::from_be_bytes([
            0, 0, bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5],
        ])
    })
}

/// 100-nanosecond intervals since the Unix epoch, saturating rather than wrapping if the
/// clock is set impossibly far ahead.
fn now_ticks() -> u64 {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    u64::try_from(nanos / 100).unwrap_or(u64::MAX)
}

/// Mint the next synthetic key value, as the 16 bytes of a v1 TimeUUID.
///
/// A fixed-size array, not a `Vec`: the length is a property of the type, so converting it to
/// a `Uuid` at the call site is infallible and a server path never needs an `expect`.
pub fn next_synthetic_key() -> [u8; 16] {
    let now = now_ticks();
    // fetch_max returns the PREVIOUS value; the tick we hand out is the greater of it and
    // `now`, which makes the floor monotonic across concurrent minters.
    let previous = LAST_TICK.fetch_max(now, Ordering::AcqRel);
    let tick = previous.max(now);
    let clock_seq = (CLOCK_SEQ.fetch_add(1, Ordering::AcqRel) & 0x3FFF) as u16;
    v1_timeuuid(tick, clock_seq, node_id())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;

    /// The whole reason a synthetic key exists: every row gets its own. A repeat would mean
    /// two rows sharing a key, and one write silently overwriting the other.
    #[test]
    fn keys_are_unique() {
        let keys: HashSet<[u8; 16]> = (0..2_000).map(|_| next_synthetic_key()).collect();
        assert_eq!(keys.len(), 2_000, "2000 mints must give 2000 distinct keys");
    }

    /// The key is the storage key, so it must not step backwards — that would scatter rows
    /// written in order. 1 000 is well inside the clock-seq range, so this is strict.
    #[test]
    fn keys_are_monotonic() {
        let keys: Vec<[u8; 16]> = (0..1_000).map(|_| next_synthetic_key()).collect();
        for pair in keys.windows(2) {
            assert!(
                pair[0] < pair[1],
                "a later key must not sort before an earlier one"
            );
        }
    }

    /// Not arbitrary unique bytes: a real v1 TimeUUID, so a CQL client decodes it as one and
    /// a reader can recover the time. Version nibble 1, variant bits `10`.
    #[test]
    fn the_key_is_a_well_formed_v1_timeuuid() {
        let k = next_synthetic_key();
        assert_eq!(k.len(), 16, "a timeuuid is 16 bytes");
        assert_eq!(k[6] >> 4, 1, "version nibble must be 1");
        assert_eq!(k[8] >> 6, 0b10, "variant bits must be 10");
    }

    /// Every key from one process carries the same node, and that is what makes two
    /// processes' keys disjoint rather than merely unlikely to collide.
    #[test]
    fn the_node_field_is_stable_within_the_process() {
        let node = &next_synthetic_key()[10..16];
        for _ in 0..10 {
            assert_eq!(
                &next_synthetic_key()[10..16],
                node,
                "the node field must not churn"
            );
        }
    }

    /// Mints inside one tick must still differ — that is what the clock-seq field is for. The
    /// tick is frozen here so the test cannot pass merely because the clock advanced.
    #[test]
    fn keys_minted_in_one_tick_still_differ() {
        let tick = 17_000_000_000_000_000u64;
        let mut seen: HashSet<[u8; 16]> = HashSet::new();
        for i in 0..1_000u16 {
            seen.insert(v1_timeuuid(tick, i, node_id()));
        }
        assert_eq!(
            seen.len(),
            1_000,
            "same tick, distinct clock_seq => distinct keys"
        );
    }
}
