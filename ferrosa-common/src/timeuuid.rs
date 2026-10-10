//! Minting a v1 TimeUUID for the synthetic partition key of a Postgres table that
//! declared no `PRIMARY KEY` (the reserved `_sys_ck_` column).
//!
//! A PK-less `CREATE TABLE` has no natural row identity, so ferrosa gives the table a
//! synthetic key column and every row is unique by construction. That key must be:
//!
//! - **globally unique**, or two rows collide on the shared key and one write is lost —
//!   the same defect as keying on a non-unique user column, only better hidden;
//! - **time-ordered**, so rows sort by insertion and the key stays index-friendly.
//!
//! A v1 TimeUUID gives both. Its 60-bit time field orders it; the node field makes it
//! unique across writers. This module is the byte layout only — it deliberately has no
//! clock of its own, so callers pass the time in and the value stays testable and
//! deterministic.
//!
//! Layout (RFC 4122 §4.1.2, bytes big-endian):
//!
//! ```text
//!  0..4   time_low
//!  4..6   time_mid
//!  6..8   time_hi_and_version   (high nibble = version 1)
//!  8..10  clock_seq_hi_and_reserved (high two bits = variant 10)
//! 10..16  node (48 bits)
//! ```

/// 100-nanosecond intervals between the UUID epoch (1582-10-15) and the Unix epoch
/// (1970-01-01). A v1 TimeUUID carries time on the UUID epoch, so Unix times are
/// offset by this before being encoded.
pub use crate::complex_cell::UUID_EPOCH_OFFSET;

/// Mint the 16 bytes of a v1 TimeUUID.
///
/// `time_100ns_since_unix` is the number of 100-nanosecond intervals since the Unix
/// epoch. `clock_seq` breaks ties between values minted at the same instant (it is
/// truncated to 14 bits); `node` is the writer's 48-bit identity (truncated).
///
/// Deterministic: the same inputs always produce the same bytes. Uniqueness comes from
/// the caller supplying a distinct `(time, clock_seq, node)` per row — this function
/// cannot and does not invent one.
pub fn v1_timeuuid(time_100ns_since_unix: u64, clock_seq: u16, node: u64) -> [u8; 16] {
    // Wrap rather than panic: the time field is 60 bits and a wrapped value is still a
    // valid, ordered-against-its-neighbours TimeUUID. Year 5236 is the caller's problem.
    let uuid_ts = time_100ns_since_unix.wrapping_add(UUID_EPOCH_OFFSET);
    let time_low = (uuid_ts & 0xFFFF_FFFF) as u32;
    let time_mid = ((uuid_ts >> 32) & 0xFFFF) as u16;
    let time_hi_and_version = ((uuid_ts >> 48) & 0x0FFF) as u16 | 0x1000; // version 1
    let clock_seq_and_reserved = (clock_seq & 0x3FFF) | 0x8000; // variant 10

    let mut bytes = [0u8; 16];
    bytes[0..4].copy_from_slice(&time_low.to_be_bytes());
    bytes[4..6].copy_from_slice(&time_mid.to_be_bytes());
    bytes[6..8].copy_from_slice(&time_hi_and_version.to_be_bytes());
    bytes[8..10].copy_from_slice(&clock_seq_and_reserved.to_be_bytes());
    bytes[10..14].copy_from_slice(&((node >> 16) as u32).to_be_bytes());
    bytes[14..16].copy_from_slice(&((node & 0xFFFF) as u16).to_be_bytes());
    bytes
}

/// The version nibble of a v1 TimeUUID's `time_hi_and_version` field.
pub const V1_VERSION: u8 = 1;

/// The reserved column name carrying a synthetic row key on a table whose `CREATE TABLE`
/// declared no `PRIMARY KEY`.
///
/// A user may not name a column this: the Postgres front end filters it out of `SELECT *`,
/// out of `COPY`'s expected column list, and out of `INSERT` arity, so it must mean exactly
/// one thing. See [`is_reserved_column_name`].
pub const SYNTHETIC_KEY_COLUMN: &str = "_sys_ck_";

/// True when `name` is the synthetic key column ferrosa mints for a table whose
/// `CREATE TABLE` declared no `PRIMARY KEY`.
///
/// Case-insensitive, because it decides whether an `INSERT` that did not name the column
/// gets one minted for it: a client writing `_SYS_CK_` must not be treated as having
/// supplied the key, nor as having omitted some ordinary column.
pub fn is_synthetic_key_column(name: &str) -> bool {
    name.eq_ignore_ascii_case(SYNTHETIC_KEY_COLUMN)
}

/// True when `name` is reserved for ferrosa's own use and a user may not declare it.
///
/// Matches the whole `_sys_` prefix rather than the one column we mint today, so a future
/// system column cannot collide with a user's table the day it is added.
pub fn is_reserved_column_name(name: &str) -> bool {
    name.len() > 5 && name.as_bytes()[..5].eq_ignore_ascii_case(b"_sys_")
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The version nibble is 1 and the variant bits are `10` — what makes these bytes a
    /// v1 TimeUUID rather than arbitrary 16 bytes. A drift here would produce a value
    /// that is unique but that no TimeUUID reader (or CQL client) would decode as one.
    #[test]
    fn the_bytes_carry_v1_version_and_rfc4122_variant() {
        let b = v1_timeuuid(17_000_000_000_000_000, 0, 0);
        assert_eq!(b[6] >> 4, V1_VERSION, "version nibble must be 1");
        assert_eq!(b[8] >> 6, 0b10, "variant bits must be 10");
    }

    /// The time field round-trips. Ordering is the whole point of choosing v1 over v4, so
    /// a reader recovering the wrong time would silently misorder every synthetic key.
    #[test]
    fn the_time_field_round_trips_through_the_encoding() {
        for t in [
            0u64,
            1,
            0xFFFF_FFFF,
            0x0123_4567_89AB_CDEF,
            17_000_000_000_000_000,
        ] {
            let b = v1_timeuuid(t, 0, 0);
            let uuid_ts = t.wrapping_add(UUID_EPOCH_OFFSET);
            let time_low = u32::from_be_bytes([b[0], b[1], b[2], b[3]]) as u64;
            let time_mid = u16::from_be_bytes([b[4], b[5]]) as u64;
            let time_hi = (u16::from_be_bytes([b[6], b[7]]) & 0x0FFF) as u64;
            let got = (time_hi << 48) | (time_mid << 32) | time_low;
            assert_eq!(
                got,
                uuid_ts & 0x0FFF_FFFF_FFFF_FFFF,
                "time round-trip for {t}"
            );
        }
        let _ = 0;
    }

    /// Later time must sort later as raw bytes. If this inverts, synthetic keys order
    /// backwards and time-ordered scans degrade to a shuffle.
    #[test]
    fn later_time_sorts_later_as_bytes() {
        let a = v1_timeuuid(1_000_000_000_000_000, 0, 7);
        let b = v1_timeuuid(1_000_000_000_000_001, 0, 7);
        assert!(a < b, "a later timestamp must produce greater bytes");
    }

    /// The three inputs are genuinely independent: changing any one changes the bytes.
    /// This is what lets a writer guarantee uniqueness by varying the clock_seq or node
    /// when two rows land in the same 100-ns tick.
    #[test]
    fn each_input_changes_the_output() {
        let t = 17_000_000_000_000_000;
        let base = v1_timeuuid(t, 0, 0);
        assert_ne!(base, v1_timeuuid(t + 1, 0, 0), "time");
        assert_ne!(base, v1_timeuuid(t, 1, 0), "clock_seq");
        assert_ne!(base, v1_timeuuid(t, 0, 1), "node");
    }

    /// Deterministic, so a retry of the same logical row mints the same key rather than
    /// leaving an orphan behind.
    #[test]
    fn minting_is_deterministic() {
        let t = 17_000_000_000_000_000;
        assert_eq!(
            v1_timeuuid(t, 42, 0xABCD_EF12_3456),
            v1_timeuuid(t, 42, 0xABCD_EF12_3456)
        );
    }

    /// The node is the full 48 bits of bytes 10..16, big-endian — the RFC 4122 layout.
    /// Truncating to fewer bits would shrink the space writers draw distinct ids from and
    /// make cross-node collisions likelier.
    #[test]
    fn the_node_field_uses_all_48_bits() {
        let t = 17_000_000_000_000_000;
        for node in [
            0x0000_0000_0001u64,
            0x0000_0001_0000,
            0x0001_0000_0000,
            0xABCD_EF12_3456,
        ] {
            let b = v1_timeuuid(t, 0, node);
            let encoded = u64::from_be_bytes([0, 0, b[10], b[11], b[12], b[13], b[14], b[15]]);
            assert_eq!(
                encoded, node,
                "node {node:#x} must round-trip through bytes 10..16"
            );
        }
    }

    /// The synthetic key column is recognised however it is spelled, so an INSERT that
    /// names it in the wrong case is neither given a minted key nor refused for a missing one.
    #[test]
    fn the_synthetic_key_column_is_recognised_case_insensitively() {
        assert!(is_synthetic_key_column("_sys_ck_"));
        assert!(is_synthetic_key_column("_SYS_CK_"));
        assert!(!is_synthetic_key_column("_sys_ck"));
        assert!(!is_synthetic_key_column("ck_"));
    }

    /// The reserved prefix is refused however it is typed, and ordinary names are not.
    /// Case- and quote-variants must not slip a colliding column past the filter.
    #[test]
    fn the_reserved_prefix_is_recognised_case_insensitively() {
        for name in ["_sys_ck_", "_SYS_CK_", "_Sys_Ck_", "_sys_whatever"] {
            assert!(is_reserved_column_name(name), "{name} must be reserved");
        }
        for name in [
            "aid", "filler", "sys_ck_", "_sys", "a_sys_b", "SYS_CK", "_sysx",
        ] {
            assert!(
                !is_reserved_column_name(name),
                "{name} must NOT be reserved"
            );
        }
    }
}
