//! Module: Build per-element cells for collection column writes (CRDT-collections).
//! Correctness: Correct when a collection add/remove/put maps to the Cassandra-exact
//! per-element cells — set element → cell path = encoded element (empty value); map
//! entry → path = encoded key, value = encoded value; list append → path = a v1
//! TimeUUID minted from the write timestamp so later appends sort after earlier ones;
//! and a remove is a tombstone at the element's path — all read-free (commutative), so
//! concurrent updates converge (see [`ferrosa_common::complex_cell`]). Read-modify-write
//! shapes (list remove-by-value, prepend, positional index) are rejected loudly here
//! rather than silently mis-encoded. Verified by the unit tests below.
//! Last revised: 2026-07-20
//! Last changed: Relocated here from `ferrosa-cql::collection_cells` so the
//!   primary SELECT read path (`crate::row::decode_output_row`) can assemble
//!   complex columns; `ferrosa-cql` re-exports it (crdt-collections increment 3,
//!   part D-read-2).
//!
//! This turns a `v = v + [..]` / `v = v + {..}` / `v = v - {..}` collection assignment
//! into the per-element cells that flow through the memtable, commit log, and Accord
//! apply (increments 1–2), and assembles them back on read. It replaces the
//! whole-collection read-modify-write, which could not be expressed by the Accord
//! transaction path (the write was silently dropped — see t_83c4f093).

use crate::codec::{decode_value, encode_value};
use ferrosa_common::{CellValue, CqlType, CqlValue, Timestamp, NO_DELETION_TIME};

/// 100-nanosecond intervals between the UUID epoch (1582-10-15) and the Unix epoch
/// (1970-01-01) — the same constant `bridge::eval_now` uses to mint v1 TimeUUIDs.
const UUID_EPOCH_OFFSET: u64 = 0x01B2_1DD2_1381_4000;

/// A collection update could not be expressed as read-free per-element cells.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UnsupportedCollectionOp {
    pub reason: String,
}

impl std::fmt::Display for UnsupportedCollectionOp {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.reason)
    }
}

impl std::error::Error for UnsupportedCollectionOp {}

/// The collection assignment operator: `col = col + rhs` (Add) or `col = col - rhs`
/// (Sub).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CollectionOp {
    Add,
    Sub,
}

/// Mint a v1 TimeUUID cell path for a `list` element written at `write_ts` (micros
/// since the Unix epoch) as the `seq`-th element of this append. The time field
/// carries `write_ts`, so a later append (larger `write_ts`) sorts after an earlier
/// one, and `seq` orders the elements of a single append. `node` is fixed (the path
/// only needs to be unique and time-ordered, not node-attributed).
///
/// The bytes are a valid v1 TimeUUID (Cassandra-exact layout). Because a v1 UUID's
/// raw-byte order is NOT its time order, the read-assembly orders list elements by
/// the extracted time — see [`timeuuid_time`].
pub fn list_cell_path(write_ts: Timestamp, seq: u16) -> Vec<u8> {
    let uuid_ts = (write_ts.max(0) as u64)
        .wrapping_mul(10)
        .wrapping_add(UUID_EPOCH_OFFSET);
    let time_low = (uuid_ts & 0xFFFF_FFFF) as u32;
    let time_mid = ((uuid_ts >> 32) & 0xFFFF) as u16;
    let time_hi = ((uuid_ts >> 48) & 0x0FFF) as u16 | 0x1000; // version 1
    let clock_seq = (seq & 0x3FFF) | 0x8000; // variant 1, seq disambiguates
    let mut b = [0u8; 16];
    b[0..4].copy_from_slice(&time_low.to_be_bytes());
    b[4..6].copy_from_slice(&time_mid.to_be_bytes());
    b[6..8].copy_from_slice(&time_hi.to_be_bytes());
    b[8..10].copy_from_slice(&clock_seq.to_be_bytes());
    // b[10..16] = node, left zero.
    b.to_vec()
}

/// The `(time, seq)` ordering key of a `list` cell path minted by [`list_cell_path`]:
/// reconstruct the v1 TimeUUID time field and the clock-seq. List elements materialize
/// in this order (append order), which raw-byte path order does not give.
pub fn timeuuid_time(path: &[u8]) -> Option<(u64, u16)> {
    if path.len() != 16 {
        return None;
    }
    let time_low = u32::from_be_bytes([path[0], path[1], path[2], path[3]]) as u64;
    let time_mid = u16::from_be_bytes([path[4], path[5]]) as u64;
    let time_hi = (u16::from_be_bytes([path[6], path[7]]) & 0x0FFF) as u64;
    let time = time_low | (time_mid << 32) | (time_hi << 48);
    let clock_seq = u16::from_be_bytes([path[8], path[9]]) & 0x3FFF;
    Some((time, clock_seq))
}

/// Build the per-element cells to write for a collection assignment `col = col {op}
/// rhs` at `write_ts`. Read-free — no current value is read.
///
/// - `rhs = Set{..}`, Add → live cells, path = encoded element, empty value (set add).
/// - `rhs = Set{..}`, Sub → tombstones, path = encoded element (set remove; also map
///   key removal, `map = map - {k}`, since keys arrive as a set).
/// - `rhs = Map{..}`, Add → live cells, path = encoded key, value = encoded value.
/// - `rhs = List[..]`, Add → live cells, path = v1 TimeUUID (append order), value =
///   encoded element.
///
/// Rejected (need a read; kept out of the read-free path): list remove-by-value
/// (`list - [..]`), any Sub on a `Map` rhs, prepend, and positional index ops.
pub fn build_collection_cells(
    op: CollectionOp,
    rhs: &CqlValue,
    write_ts: Timestamp,
) -> Result<Vec<CellValue>, UnsupportedCollectionOp> {
    match (rhs, op) {
        // ---- set add / set remove / map-key remove --------------------------
        (CqlValue::Set(items), CollectionOp::Add) => Ok(items
            .iter()
            .map(|e| CellValue::live(Vec::new(), write_ts).with_path(encode_value(e)))
            .collect()),
        (CqlValue::Set(items), CollectionOp::Sub) => Ok(items
            .iter()
            .map(|e| CellValue::tombstone(write_ts, NO_DELETION_TIME).with_path(encode_value(e)))
            .collect()),

        // ---- map put --------------------------------------------------------
        (CqlValue::Map(pairs), CollectionOp::Add) => Ok(pairs
            .iter()
            .map(|(k, v)| CellValue::live(encode_value(v), write_ts).with_path(encode_value(k)))
            .collect()),
        (CqlValue::Map(_), CollectionOp::Sub) => Err(UnsupportedCollectionOp {
            reason: "map subtraction removes keys given as a set, not a map".into(),
        }),

        // ---- list append ----------------------------------------------------
        (CqlValue::List(items), CollectionOp::Add) => Ok(items
            .iter()
            .enumerate()
            .map(|(i, e)| {
                CellValue::live(encode_value(e), write_ts)
                    .with_path(list_cell_path(write_ts, i as u16))
            })
            .collect()),
        (CqlValue::List(_), CollectionOp::Sub) => Err(UnsupportedCollectionOp {
            // Cassandra removes ALL occurrences of the value — an inherent read.
            reason: "list remove-by-value requires a read-modify-write (not yet supported \
                     in a transaction)"
                .into(),
        }),

        _ => Err(UnsupportedCollectionOp {
            reason: format!(
                "unsupported collection assignment: {op:?} of a {} value",
                match rhs {
                    CqlValue::List(_) => "list",
                    CqlValue::Set(_) => "set",
                    CqlValue::Map(_) => "map",
                    _ => "non-collection",
                }
            ),
        }),
    }
}

/// A complex column's per-element cells could not be assembled into a value.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AssembleError {
    pub reason: String,
}

impl std::fmt::Display for AssembleError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.reason)
    }
}

impl std::error::Error for AssembleError {}

/// Assemble the per-element cells of one complex column back into a [`CqlValue`]
/// collection — the read-side inverse of [`build_collection_cells`]. `cells` are all
/// cells for the column (with paths); tombstones are excluded. `col_type` selects the
/// shape and the element/key/value decoding.
///
/// - `set<T>`  → elements decoded from each live cell's PATH (value is empty), sorted
///   by element.
/// - `map<K,V>`→ entries `(decode PATH as K, decode VALUE as V)`, sorted by key.
/// - `list<T>` → elements = decode each live cell's VALUE, ordered by the path's
///   TimeUUID time (append order) — raw-byte path order would be wrong.
///
/// Cells are assumed already reconciled (one live-or-tombstone cell per path), which
/// is what the memtable/SSTable merge produces.
pub fn assemble_collection(
    col_type: &CqlType,
    cells: &[CellValue],
) -> Result<CqlValue, AssembleError> {
    let live = || cells.iter().filter(|c| c.value.is_some());
    let decode = |ty: &CqlType, bytes: &[u8]| -> Result<CqlValue, AssembleError> {
        decode_value(ty, bytes).map_err(|e| AssembleError {
            reason: format!("decode element: {e}"),
        })
    };
    let path_of = |c: &CellValue| -> Result<Vec<u8>, AssembleError> {
        c.path.clone().ok_or_else(|| AssembleError {
            reason: "complex column cell is missing its path".into(),
        })
    };

    match col_type {
        CqlType::Set(elem_ty) => {
            let mut elems: Vec<CqlValue> = live()
                .map(|c| decode(elem_ty, &path_of(c)?))
                .collect::<Result<_, _>>()?;
            elems.sort();
            elems.dedup();
            Ok(CqlValue::Set(elems))
        }
        CqlType::Map(key_ty, val_ty) => {
            let mut entries: Vec<(CqlValue, CqlValue)> = live()
                .map(|c| {
                    Ok((
                        decode(key_ty, &path_of(c)?)?,
                        decode(val_ty, c.value.as_deref().unwrap_or_default())?,
                    ))
                })
                .collect::<Result<_, AssembleError>>()?;
            entries.sort_by(|a, b| a.0.cmp(&b.0));
            entries.dedup_by(|a, b| a.0 == b.0);
            Ok(CqlValue::Map(entries))
        }
        CqlType::List(elem_ty) => {
            // Order by the path's TimeUUID (time, seq) — append order.
            let mut keyed: Vec<((u64, u16), CqlValue)> = live()
                .map(|c| {
                    let key = timeuuid_time(&path_of(c)?).ok_or_else(|| AssembleError {
                        reason: "list cell path is not a 16-byte TimeUUID".into(),
                    })?;
                    Ok((
                        key,
                        decode(elem_ty, c.value.as_deref().unwrap_or_default())?,
                    ))
                })
                .collect::<Result<_, AssembleError>>()?;
            keyed.sort_by_key(|(k, _)| *k);
            Ok(CqlValue::List(keyed.into_iter().map(|(_, v)| v).collect()))
        }
        other => Err(AssembleError {
            reason: format!("not a collection column type: {other:?}"),
        }),
    }
}

/// Read-path assembly of a single column's cells into its value — the one place
/// both the primary SELECT path (`crate::row::decode_output_row`) and the
/// metadata variant (`ferrosa_cql::bridge`) share, so their collection handling
/// cannot silently diverge.
///
/// - Complex column (any cell carries a `path`): reconcile the per-element cells
///   by path (CRDT LWW — higher timestamp wins, tombstone wins a tie), drop the
///   cells that are no longer live at `now_secs` (tombstoned or TTL-expired),
///   and [`assemble_collection`] the survivors. A genuine assembly failure is
///   returned as `Err` for the caller to log loudly.
/// - Simple column (all `path == None`): newest cell wins (LWW); returns `None`
///   if it is not live, or if its bytes fail to decode (lenient, matching the
///   long-standing scalar read behavior). A legacy whole-value collection — one
///   `path == None` cell holding the entire encoded collection — decodes here as
///   the whole value, preserving backward compatibility (lazy dual-read).
///
/// `None` means the column has no live value (absent, deleted, or expired).
pub fn assemble_column_cells(
    col_type: &CqlType,
    cells: &[&CellValue],
    now_secs: i32,
) -> Result<Option<CqlValue>, AssembleError> {
    if cells.is_empty() {
        return Ok(None);
    }

    if cells.iter().any(|c| c.path.is_some()) {
        let mut by_path: std::collections::BTreeMap<Vec<u8>, CellValue> =
            std::collections::BTreeMap::new();
        for &c in cells {
            let key = c.path.clone().unwrap_or_default();
            by_path
                .entry(key)
                .and_modify(|e| *e = ferrosa_common::reconcile(e, c))
                .or_insert_with(|| c.clone());
        }
        let live: Vec<CellValue> = by_path
            .into_values()
            .filter(|c| crate::row::cell_is_live(c, now_secs))
            .collect();
        return Ok(Some(assemble_collection(col_type, &live)?));
    }

    let newest = cells
        .iter()
        .copied()
        .max_by_key(|c| c.timestamp)
        .expect("cells is non-empty");
    if !crate::row::cell_is_live(newest, now_secs) {
        return Ok(None);
    }
    match &newest.value {
        Some(bytes) => Ok(decode_value(col_type, bytes).ok()),
        None => Ok(None),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn t(v: &str) -> CqlValue {
        CqlValue::Text(v.to_string())
    }

    fn text_list() -> CqlType {
        CqlType::List(Box::new(CqlType::Varchar))
    }
    fn text_set() -> CqlType {
        CqlType::Set(Box::new(CqlType::Varchar))
    }
    fn text_map() -> CqlType {
        CqlType::Map(Box::new(CqlType::Varchar), Box::new(CqlType::Varchar))
    }

    #[test]
    fn set_add_makes_live_element_cells() {
        let rhs = CqlValue::Set(vec![t("a"), t("b")]);
        let cells = build_collection_cells(CollectionOp::Add, &rhs, 100).unwrap();
        assert_eq!(cells.len(), 2);
        for (cell, elem) in cells.iter().zip([t("a"), t("b")]) {
            assert!(
                cell.value.as_deref() == Some(&[][..]),
                "set element value is empty"
            );
            assert!(cell.is_live());
            assert_eq!(cell.path.as_deref(), Some(encode_value(&elem).as_slice()));
            assert_eq!(cell.timestamp, 100);
        }
    }

    #[test]
    fn set_sub_makes_tombstones_at_element_paths() {
        let rhs = CqlValue::Set(vec![t("a")]);
        let cells = build_collection_cells(CollectionOp::Sub, &rhs, 100).unwrap();
        assert_eq!(cells.len(), 1);
        assert!(cells[0].is_tombstone());
        assert_eq!(
            cells[0].path.as_deref(),
            Some(encode_value(&t("a")).as_slice())
        );
    }

    #[test]
    fn map_put_makes_live_cells_keyed_by_key() {
        let rhs = CqlValue::Map(vec![(t("k1"), t("v1")), (t("k2"), t("v2"))]);
        let cells = build_collection_cells(CollectionOp::Add, &rhs, 100).unwrap();
        assert_eq!(cells.len(), 2);
        assert_eq!(
            cells[0].path.as_deref(),
            Some(encode_value(&t("k1")).as_slice())
        );
        assert_eq!(
            cells[0].value.as_deref(),
            Some(encode_value(&t("v1")).as_slice())
        );
    }

    #[test]
    fn list_append_makes_timeuuid_paths_in_append_order() {
        let rhs = CqlValue::List(vec![t("x"), t("y"), t("z")]);
        let cells = build_collection_cells(CollectionOp::Add, &rhs, 100).unwrap();
        assert_eq!(cells.len(), 3);
        // Values are the elements, in order.
        assert_eq!(
            cells[0].value.as_deref(),
            Some(encode_value(&t("x")).as_slice())
        );
        // Paths are distinct 16-byte v1 TimeUUIDs.
        let paths: Vec<_> = cells.iter().map(|c| c.path.clone().unwrap()).collect();
        assert!(paths.iter().all(|p| p.len() == 16));
        assert_ne!(paths[0], paths[1]);
        // Same-append elements order by seq.
        let k: Vec<_> = paths.iter().map(|p| timeuuid_time(p).unwrap()).collect();
        assert!(
            k[0] < k[1] && k[1] < k[2],
            "seq preserves append order: {k:?}"
        );
    }

    #[test]
    fn later_append_sorts_after_earlier_by_time() {
        let early =
            build_collection_cells(CollectionOp::Add, &CqlValue::List(vec![t("x")]), 100).unwrap();
        let late =
            build_collection_cells(CollectionOp::Add, &CqlValue::List(vec![t("y")]), 200).unwrap();
        let ke = timeuuid_time(early[0].path.as_deref().unwrap()).unwrap();
        let kl = timeuuid_time(late[0].path.as_deref().unwrap()).unwrap();
        assert!(ke < kl, "later write_ts sorts after: {ke:?} < {kl:?}");
    }

    #[test]
    fn read_modify_write_shapes_are_rejected_loudly() {
        // list remove-by-value
        assert!(
            build_collection_cells(CollectionOp::Sub, &CqlValue::List(vec![t("a")]), 1).is_err()
        );
        // map subtraction of a map (keys come as a set, not a map)
        assert!(build_collection_cells(
            CollectionOp::Sub,
            &CqlValue::Map(vec![(t("k"), t("v"))]),
            1
        )
        .is_err());
    }

    /// The v1 TimeUUID is valid: version nibble is 1, variant bits are 10xx.
    #[test]
    fn list_path_is_a_valid_v1_timeuuid() {
        let p = list_cell_path(123_456, 7);
        assert_eq!(p.len(), 16);
        assert_eq!(p[6] & 0xF0, 0x10, "version 1");
        assert_eq!(p[8] & 0xC0, 0x80, "variant 1 (10xx)");
    }

    #[test]
    fn timeuuid_time_round_trips_write_ts_and_seq() {
        let p = list_cell_path(999_000, 42);
        let (time, seq) = timeuuid_time(&p).unwrap();
        assert_eq!(time, 999_000u64 * 10 + UUID_EPOCH_OFFSET);
        assert_eq!(seq, 42);
    }

    // ---- read assembly (round-trips build_collection_cells) -----------------

    #[test]
    fn list_append_round_trips_in_order() {
        let cells = build_collection_cells(
            CollectionOp::Add,
            &CqlValue::List(vec![t("x"), t("y")]),
            100,
        )
        .unwrap();
        let back = assemble_collection(&text_list(), &cells).unwrap();
        assert_eq!(back, CqlValue::List(vec![t("x"), t("y")]));
    }

    #[test]
    fn list_two_appends_merge_and_assemble_in_append_order() {
        // Two appends at different timestamps, stored/delivered in REVERSE order:
        // assembly yields append order via the TimeUUID time.
        let mut cells =
            build_collection_cells(CollectionOp::Add, &CqlValue::List(vec![t("later")]), 200)
                .unwrap();
        cells.extend(
            build_collection_cells(CollectionOp::Add, &CqlValue::List(vec![t("earlier")]), 100)
                .unwrap(),
        );
        let back = assemble_collection(&text_list(), &cells).unwrap();
        assert_eq!(back, CqlValue::List(vec![t("earlier"), t("later")]));
    }

    /// The load-bearing ordering guarantee: a v1 TimeUUID's RAW-BYTE order is not its
    /// time order (the low time bits are stored first). This crafts two list cells
    /// whose paths sort OPPOSITELY by raw bytes vs by time, and asserts assembly uses
    /// TIME. If assembly ever fell back to the BTreeMap's raw-byte path order, list
    /// reads would silently reorder across ~430s append boundaries.
    #[test]
    fn list_orders_by_timeuuid_time_not_raw_bytes() {
        let list_cell = |value: &str, path: [u8; 16]| {
            CellValue::live(encode_value(&t(value)), 1).with_path(path.to_vec())
        };
        // A: time_low = all-ones, mid/hi = 0  => time = 0x0000_FFFF_FFFF, bytes start 0xFF.
        let mut a = [0u8; 16];
        a[0..4].copy_from_slice(&0xFFFF_FFFFu32.to_be_bytes());
        a[6] = 0x10; // version 1
        a[8] = 0x80; // variant
                     // B: time_low = 0, time_mid = 1 => time = 0x0001_0000_0000 (> A), bytes start 0x00.
        let mut b = [0u8; 16];
        b[4..6].copy_from_slice(&1u16.to_be_bytes());
        b[6] = 0x10;
        b[8] = 0x80;

        assert!(b < a, "raw-byte order puts B before A");
        assert!(
            timeuuid_time(&a).unwrap() < timeuuid_time(&b).unwrap(),
            "but A's TimeUUID time is earlier than B's"
        );

        let cells = vec![list_cell("A", a), list_cell("B", b)];
        let back = assemble_collection(&text_list(), &cells).unwrap();
        assert_eq!(
            back,
            CqlValue::List(vec![t("A"), t("B")]),
            "assembled in TimeUUID-time order, not raw-byte path order"
        );
    }

    #[test]
    fn set_add_round_trips_sorted() {
        let cells =
            build_collection_cells(CollectionOp::Add, &CqlValue::Set(vec![t("b"), t("a")]), 100)
                .unwrap();
        let back = assemble_collection(&text_set(), &cells).unwrap();
        assert_eq!(back, CqlValue::Set(vec![t("a"), t("b")]));
    }

    #[test]
    fn map_put_round_trips_sorted_by_key() {
        let cells = build_collection_cells(
            CollectionOp::Add,
            &CqlValue::Map(vec![(t("k2"), t("v2")), (t("k1"), t("v1"))]),
            100,
        )
        .unwrap();
        let back = assemble_collection(&text_map(), &cells).unwrap();
        assert_eq!(
            back,
            CqlValue::Map(vec![(t("k1"), t("v1")), (t("k2"), t("v2"))])
        );
    }

    #[test]
    fn tombstones_are_excluded_from_assembly() {
        let mut cells =
            build_collection_cells(CollectionOp::Add, &CqlValue::Set(vec![t("a"), t("b")]), 100)
                .unwrap();
        // Remove "a" at a later timestamp (tombstone at its path).
        cells.extend(
            build_collection_cells(CollectionOp::Sub, &CqlValue::Set(vec![t("a")]), 200).unwrap(),
        );
        // Reconcile by path (higher-ts tombstone wins), mirroring the memtable merge.
        let reconciled = reconcile_by_path(&cells);
        let back = assemble_collection(&text_set(), &reconciled).unwrap();
        assert_eq!(back, CqlValue::Set(vec![t("b")]));
    }

    /// Test helper: reduce cells to one per path via the CRDT reconcile (what the
    /// memtable does), so an assembly test can exercise adds + removes together.
    fn reconcile_by_path(cells: &[CellValue]) -> Vec<CellValue> {
        use std::collections::BTreeMap;
        let mut by_path: BTreeMap<Vec<u8>, CellValue> = BTreeMap::new();
        for c in cells {
            let key = c.path.clone().unwrap_or_default();
            by_path
                .entry(key)
                .and_modify(|existing| *existing = ferrosa_common::reconcile(existing, c))
                .or_insert_with(|| c.clone());
        }
        by_path.into_values().collect()
    }
}
