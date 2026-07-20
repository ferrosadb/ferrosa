//! CQL result set pagination with opaque paging state.
//!
//! Implements the CQL v5 protocol pagination mechanism:
//! - Client sends `page_size` in QUERY/EXECUTE frame flags
//! - Server returns at most `page_size` rows plus a `paging_state` opaque token
//! - Client sends `paging_state` back in next QUERY/EXECUTE to resume
//! - When there are no more pages, `paging_state` is absent
//!
//! The `PagingState` encodes the position to resume from using a simple
//! length-prefixed binary format. This is an opaque token from the client's
//! perspective.

use std::sync::OnceLock;

use hmac::{Hmac, Mac};
use rand::Rng;
use sha2::{Digest, Sha256};

use crate::error::CqlError;

type HmacSha256 = Hmac<Sha256>;

/// Length of the HMAC-SHA256 tag appended to every paging token.
const PAGING_HMAC_LEN: usize = 32;

/// The cluster-wide paging HMAC signing key, set once at node startup by
/// [`init_paging_hmac_key`]. If it is never set (single-node dev, tests, or a
/// bare binary), [`paging_hmac_key`] falls back to the env var or a random
/// per-process key.
static PAGING_KEY: OnceLock<[u8; 32]> = OnceLock::new();

/// `FERROSA_PAGING_HMAC_KEY` (64 hex chars) parsed into a key, if set + valid.
fn env_paging_key() -> Option<[u8; 32]> {
    let hex = std::env::var("FERROSA_PAGING_HMAC_KEY").ok()?;
    match decode_hex_32(hex.trim()) {
        Some(k) => Some(k),
        None => {
            tracing::warn!("FERROSA_PAGING_HMAC_KEY is set but is not 64 hex chars; ignoring it");
            None
        }
    }
}

/// Derive a 32-byte key from a domain-separated seed (`SHA-256(domain || 0 || seed)`).
/// Deterministic, so every coordinator with the same seed computes the same key.
fn derive_paging_key(domain: &[u8], seed: &[u8]) -> [u8; 32] {
    let mut h = Sha256::new();
    h.update(domain);
    h.update([0u8]);
    h.update(seed);
    h.finalize().into()
}

/// Initialize the cluster-wide paging HMAC signing key at node startup — call
/// this BEFORE the CQL server serves any query.
///
/// The paging cursor is HMAC-signed (FMEA CQL-2) so a client cannot forge one to
/// resume at an arbitrary partition (an IDOR-class cross-partition read). Every
/// coordinator in a cluster MUST derive the SAME key, or a cursor issued by one
/// coordinator is rejected by another and the paged read breaks mid-scan.
/// Priority:
///   1. `FERROSA_PAGING_HMAC_KEY` (64 hex) — an explicit shared secret.
///   2. the internode PSK — already a shared secret across the cluster.
///   3. the cluster name — CONSISTENT across coordinators (so paging works), but
///      NOT a secret; set a PSK or the env var for full cross-partition-read
///      protection.
///
/// No-op if `FERROSA_PAGING_HMAC_KEY` is set (the lazy path uses it) or the key
/// is already initialized.
pub fn init_paging_hmac_key(psk: Option<&str>, cluster_name: &str) {
    if PAGING_KEY.get().is_some() || env_paging_key().is_some() {
        return;
    }
    let (key, is_secret) = match psk.map(str::trim).filter(|s| !s.is_empty()) {
        Some(psk) => (
            derive_paging_key(b"ferrosa-paging-psk-v1", psk.as_bytes()),
            true,
        ),
        None => (
            derive_paging_key(b"ferrosa-paging-cluster-v1", cluster_name.as_bytes()),
            false,
        ),
    };
    if PAGING_KEY.set(key).is_ok() {
        if is_secret {
            tracing::info!("paging HMAC key derived from the internode PSK (shared + secret)");
        } else {
            tracing::warn!(
                cluster_name,
                "paging HMAC key derived from the cluster name — consistent across coordinators \
                 (paged reads work) but NOT a secret; set an internode PSK or \
                 FERROSA_PAGING_HMAC_KEY for full cross-partition-read protection"
            );
        }
    }
}

/// Process-wide key used to sign paging tokens. Prefers the cluster-wide key set
/// by [`init_paging_hmac_key`]; otherwise the env var; otherwise a random
/// per-process key (single-node only — multi-node paging would reject cursors).
fn paging_hmac_key() -> &'static [u8; 32] {
    PAGING_KEY.get_or_init(|| {
        if let Some(k) = env_paging_key() {
            return k;
        }
        let mut k = [0u8; 32];
        rand::rng().fill_bytes(&mut k);
        tracing::warn!(
            "paging HMAC key not initialized and FERROSA_PAGING_HMAC_KEY unset — using a random \
             per-process key. Multi-node paging will reject cursors across coordinators; call \
             init_paging_hmac_key() at startup so all nodes agree."
        );
        k
    })
}

/// Parse exactly 64 hex chars into a 32-byte key, or `None` if malformed.
fn decode_hex_32(s: &str) -> Option<[u8; 32]> {
    if s.len() != 64 {
        return None;
    }
    let mut out = [0u8; 32];
    for (i, byte) in out.iter_mut().enumerate() {
        *byte = u8::from_str_radix(&s[i * 2..i * 2 + 2], 16).ok()?;
    }
    Some(out)
}

/// Opaque cursor encoding the position to resume from.
///
/// Contains enough info to find the next row after the last returned row.
/// The encoding is opaque to clients; they just pass it back unchanged.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PagingState {
    /// Serialized partition key of the last returned row.
    pub partition_key: Vec<u8>,
    /// Serialized clustering key of the last returned row.
    pub clustering_key: Vec<u8>,
    /// True if there are more rows remaining in this partition.
    pub remaining_in_partition: bool,
}

impl PagingState {
    /// Serialize to opaque bytes for inclusion in RESULT frames.
    ///
    /// Format: `[u32 pk_len][pk_bytes][u32 ck_len][ck_bytes][u8 remaining_flag]`
    /// followed by an `HMAC-SHA256` tag over that payload, so a forged or
    /// tampered cursor is rejected at [`decode`](Self::decode) (FMEA CQL-2).
    pub fn encode(&self) -> Vec<u8> {
        let payload_len = 4 + self.partition_key.len() + 4 + self.clustering_key.len() + 1;
        let mut buf = Vec::with_capacity(payload_len + PAGING_HMAC_LEN);
        buf.extend_from_slice(&(self.partition_key.len() as u32).to_be_bytes());
        buf.extend_from_slice(&self.partition_key);
        buf.extend_from_slice(&(self.clustering_key.len() as u32).to_be_bytes());
        buf.extend_from_slice(&self.clustering_key);
        buf.push(if self.remaining_in_partition { 1 } else { 0 });

        let mut mac =
            HmacSha256::new_from_slice(paging_hmac_key()).expect("HMAC accepts a 32-byte key");
        mac.update(&buf);
        buf.extend_from_slice(&mac.finalize().into_bytes());
        buf
    }

    /// Deserialize from opaque bytes received in QUERY/EXECUTE frames.
    ///
    /// The HMAC signature is verified (constant-time) BEFORE any contents are
    /// parsed or trusted — a client-forged cursor cannot redirect the read to
    /// another partition/tenant.
    pub fn decode(bytes: &[u8]) -> Result<Self, CqlError> {
        if bytes.len() < PAGING_HMAC_LEN {
            return Err(CqlError::Protocol("paging_state too short".into()));
        }
        let (payload, tag) = bytes.split_at(bytes.len() - PAGING_HMAC_LEN);
        let mut mac =
            HmacSha256::new_from_slice(paging_hmac_key()).expect("HMAC accepts a 32-byte key");
        mac.update(payload);
        mac.verify_slice(tag)
            .map_err(|_| CqlError::Protocol("paging_state: invalid or forged signature".into()))?;

        // Signature verified — the payload is authentic; parse it.
        if payload.len() < 9 {
            return Err(CqlError::Protocol("paging_state too short".into()));
        }

        let mut pos = 0;

        // Partition key
        let pk_len = u32::from_be_bytes(payload[pos..pos + 4].try_into().unwrap()) as usize;
        pos += 4;
        if pos + pk_len > payload.len() {
            return Err(CqlError::Protocol(
                "paging_state: partition key truncated".into(),
            ));
        }
        let partition_key = payload[pos..pos + pk_len].to_vec();
        pos += pk_len;

        // Clustering key
        if pos + 4 > payload.len() {
            return Err(CqlError::Protocol(
                "paging_state: clustering key length truncated".into(),
            ));
        }
        let ck_len = u32::from_be_bytes(payload[pos..pos + 4].try_into().unwrap()) as usize;
        pos += 4;
        if pos + ck_len > payload.len() {
            return Err(CqlError::Protocol(
                "paging_state: clustering key truncated".into(),
            ));
        }
        let clustering_key = payload[pos..pos + ck_len].to_vec();
        pos += ck_len;

        // Remaining flag
        if pos >= payload.len() {
            return Err(CqlError::Protocol(
                "paging_state: missing remaining_in_partition flag".into(),
            ));
        }
        let remaining_in_partition = payload[pos] != 0;

        Ok(Self {
            partition_key,
            clustering_key,
            remaining_in_partition,
        })
    }
}

/// Default page size applied to unbounded range/full scans when the client
/// supplies no `page_size` on the wire.
///
/// Without this, a full-table `SELECT *` with no client page_size accumulates
/// the entire result into the coordinator's `all_rows` buffer, which OOM-kills
/// the coordinator on large tables. Bounding the page guarantees the scan
/// returns at most this many rows per response with a continuation token.
///
/// Tunable via `FERROSA_CQL_DEFAULT_PAGE_SIZE`. A non-positive or unparseable
/// value falls back to [`DEFAULT_SCAN_PAGE_SIZE`].
pub const DEFAULT_SCAN_PAGE_SIZE: usize = 5_000;

/// Resolve the default scan page size from the environment, falling back to
/// [`DEFAULT_SCAN_PAGE_SIZE`]. The python driver's default `fetch_size` is
/// 5000, so an unpaged client query gets the same effective bound.
pub fn default_scan_page_size() -> usize {
    std::env::var("FERROSA_CQL_DEFAULT_PAGE_SIZE")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .filter(|&n| n > 0)
        .unwrap_or(DEFAULT_SCAN_PAGE_SIZE)
}

/// Parameters for pagination extracted from the QUERY/EXECUTE frame.
#[derive(Debug, Clone, Default)]
pub struct PagingParams {
    /// Maximum number of rows to return in this page. `None` means no limit.
    pub page_size: Option<i32>,
    /// Opaque paging state from a previous response. `None` for the first page.
    pub paging_state: Option<Vec<u8>>,
}

/// The result of applying pagination to a row set.
pub struct PaginatedResult {
    /// Row index range to return: `start..end` into the original row slice.
    pub start: usize,
    pub end: usize,
    /// Paging state to include in the response, if there are more pages.
    pub next_paging_state: Option<Vec<u8>>,
}

/// Apply pagination to a row set that has already been filtered, sorted,
/// and had LIMIT applied.
///
/// `rows_len` is the total number of rows available.
/// `page_size` is the maximum rows per page.
/// `paging_state` is the opaque cursor from the previous page (or None).
///
/// The paging state encodes a 0-based row offset for simplicity. This works
/// for in-memory result sets where the full row set is materialized. For
/// truly large datasets, a cursor-based approach would be used instead.
pub fn apply_pagination(
    rows_len: usize,
    page_size: Option<i32>,
    paging_state: Option<&[u8]>,
) -> Result<PaginatedResult, CqlError> {
    let page_size = match page_size {
        Some(ps) if ps > 0 => ps as usize,
        Some(_) | None => {
            // No pagination — return all rows.
            return Ok(PaginatedResult {
                start: 0,
                end: rows_len,
                next_paging_state: None,
            });
        }
    };

    // Determine the start offset from paging_state.
    let start = if let Some(state_bytes) = paging_state {
        let state = PagingState::decode(state_bytes)?;
        // We encode the row offset in the partition_key field (as a u64).
        if state.partition_key.len() == 8 {
            u64::from_be_bytes(state.partition_key.as_slice().try_into().unwrap()) as usize
        } else {
            0
        }
    } else {
        0
    };

    if start >= rows_len {
        return Ok(PaginatedResult {
            start: 0,
            end: 0,
            next_paging_state: None,
        });
    }

    let end = std::cmp::min(start + page_size, rows_len);
    let next_paging_state = if end < rows_len {
        // Encode the next start offset as the paging state.
        let next_state = PagingState {
            partition_key: (end as u64).to_be_bytes().to_vec(),
            clustering_key: Vec::new(),
            remaining_in_partition: false,
        };
        Some(next_state.encode())
    } else {
        None
    };

    Ok(PaginatedResult {
        start,
        end,
        next_paging_state,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn paging_state_roundtrip() {
        let state = PagingState {
            partition_key: vec![1, 2, 3, 4],
            clustering_key: vec![5, 6],
            remaining_in_partition: true,
        };
        let encoded = state.encode();
        let decoded = PagingState::decode(&encoded).unwrap();
        assert_eq!(decoded, state);
    }

    /// The fix's core invariant: every coordinator deriving the paging key from
    /// the same shared seed computes the SAME key, so a cursor issued by one
    /// coordinator verifies on another (paged reads no longer break mid-scan on
    /// a multi-node cluster). Different clusters and different seed sources are
    /// domain-separated.
    #[test]
    fn derive_paging_key_is_deterministic_and_domain_separated() {
        // Two nodes, same cluster name -> identical key.
        let node_a = derive_paging_key(b"ferrosa-paging-cluster-v1", b"ferrosa-memory-dev");
        let node_b = derive_paging_key(b"ferrosa-paging-cluster-v1", b"ferrosa-memory-dev");
        assert_eq!(
            node_a, node_b,
            "same cluster name -> same key (cross-coordinator paging works)"
        );
        // A different cluster gets a different key.
        let other = derive_paging_key(b"ferrosa-paging-cluster-v1", b"some-other-cluster");
        assert_ne!(node_a, other);
        // PSK-derived and cluster-name-derived keys are separated even for the
        // same seed bytes (domain separation).
        let psk_key = derive_paging_key(b"ferrosa-paging-psk-v1", b"ferrosa-memory-dev");
        assert_ne!(node_a, psk_key);
        // The derived key is a valid 32-byte HMAC key that signs + verifies a
        // paging token (round-trips through the HMAC construction).
        let mut mac = HmacSha256::new_from_slice(&node_a).unwrap();
        mac.update(b"payload");
        let tag = mac.finalize().into_bytes();
        let mut v = HmacSha256::new_from_slice(&node_b).unwrap();
        v.update(b"payload");
        assert!(
            v.verify_slice(&tag).is_ok(),
            "token signed under node A's derived key verifies under node B's"
        );
    }

    #[test]
    fn paging_state_roundtrip_empty_keys() {
        let state = PagingState {
            partition_key: Vec::new(),
            clustering_key: Vec::new(),
            remaining_in_partition: false,
        };
        let encoded = state.encode();
        let decoded = PagingState::decode(&encoded).unwrap();
        assert_eq!(decoded, state);
    }

    #[test]
    fn paging_state_decode_too_short() {
        let result = PagingState::decode(&[0, 1, 2]);
        assert!(result.is_err());
    }

    #[test]
    fn tampered_paging_state_is_rejected() {
        // FMEA CQL-2: an attacker edits a signed cursor to resume at a different
        // partition key. The HMAC must reject it (IDOR prevention).
        let state = PagingState {
            partition_key: vec![1, 2, 3, 4],
            clustering_key: vec![5, 6],
            remaining_in_partition: true,
        };
        let mut encoded = state.encode();
        encoded[5] ^= 0xff; // flip a payload byte (forge a different pk)
        assert!(
            PagingState::decode(&encoded).is_err(),
            "a tampered paging cursor must be rejected"
        );
    }

    #[test]
    fn unsigned_forged_paging_state_is_rejected() {
        // A hand-built cursor with a bogus signature — what a client could forge
        // to read an arbitrary partition — must not be accepted.
        let mut buf = Vec::new();
        buf.extend_from_slice(&8u32.to_be_bytes());
        buf.extend_from_slice(&999u64.to_be_bytes()); // forged offset / pk
        buf.extend_from_slice(&0u32.to_be_bytes());
        buf.push(0);
        buf.extend_from_slice(&[0u8; PAGING_HMAC_LEN]); // invalid tag
        assert!(
            PagingState::decode(&buf).is_err(),
            "an unsigned/forged paging cursor must be rejected"
        );
    }

    #[test]
    fn paging_state_decode_truncated_pk() {
        // pk_len=100 but only 3 bytes follow
        let mut buf = Vec::new();
        buf.extend_from_slice(&100u32.to_be_bytes());
        buf.extend_from_slice(&[1, 2, 3]);
        let result = PagingState::decode(&buf);
        assert!(result.is_err());
    }

    #[test]
    fn paging_state_decode_truncated_ck() {
        // Valid pk (0 length), but ck_len=50 with not enough bytes
        let mut buf = Vec::new();
        buf.extend_from_slice(&0u32.to_be_bytes());
        buf.extend_from_slice(&50u32.to_be_bytes());
        buf.push(1); // not enough for 50 bytes
        let result = PagingState::decode(&buf);
        assert!(result.is_err());
    }

    #[test]
    fn pagination_basic() {
        // 10 rows, page_size=3, first page
        let result = apply_pagination(10, Some(3), None).unwrap();
        assert_eq!(result.start, 0);
        assert_eq!(result.end, 3);
        assert!(result.next_paging_state.is_some());
    }

    #[test]
    fn pagination_multi_page() {
        // Walk through all pages of 10 rows with page_size=3
        let total_rows = 10;
        let page_size = 3;
        let mut collected_ranges: Vec<(usize, usize)> = Vec::new();
        let mut paging_state: Option<Vec<u8>> = None;
        let mut pages = 0;

        loop {
            let result =
                apply_pagination(total_rows, Some(page_size), paging_state.as_deref()).unwrap();

            if result.start == result.end {
                break;
            }

            collected_ranges.push((result.start, result.end));
            pages += 1;
            paging_state = result.next_paging_state;

            if paging_state.is_none() {
                break;
            }
        }

        // Should have 4 pages: 3+3+3+1
        assert_eq!(pages, 4);
        assert_eq!(collected_ranges, vec![(0, 3), (3, 6), (6, 9), (9, 10)]);

        // Verify total rows collected
        let total: usize = collected_ranges.iter().map(|(s, e)| e - s).sum();
        assert_eq!(total, 10);
    }

    #[test]
    fn pagination_end() {
        // Last page has < page_size rows and no paging_state
        let result = apply_pagination(5, Some(3), None).unwrap();
        assert_eq!(result.start, 0);
        assert_eq!(result.end, 3);
        assert!(result.next_paging_state.is_some());

        // Second page
        let result2 = apply_pagination(5, Some(3), result.next_paging_state.as_deref()).unwrap();
        assert_eq!(result2.start, 3);
        assert_eq!(result2.end, 5);
        assert!(result2.next_paging_state.is_none()); // No more pages
    }

    #[test]
    fn pagination_no_page_size() {
        // Without page_size, returns all rows
        let result = apply_pagination(10, None, None).unwrap();
        assert_eq!(result.start, 0);
        assert_eq!(result.end, 10);
        assert!(result.next_paging_state.is_none());
    }

    #[test]
    fn pagination_single_row_pages() {
        // page_size=1, table with 5 rows
        let total_rows = 5;
        let page_size = 1;
        let mut paging_state: Option<Vec<u8>> = None;
        let mut pages = 0;

        loop {
            let result =
                apply_pagination(total_rows, Some(page_size), paging_state.as_deref()).unwrap();

            if result.start == result.end {
                break;
            }

            assert_eq!(
                result.end - result.start,
                1,
                "each page should have exactly 1 row"
            );
            pages += 1;
            paging_state = result.next_paging_state;

            if paging_state.is_none() {
                break;
            }
        }

        assert_eq!(
            pages, 5,
            "should have exactly 5 pages for 5 rows with page_size=1"
        );
    }

    #[test]
    fn pagination_page_size_larger_than_result() {
        // page_size=100, table with 3 rows
        let result = apply_pagination(3, Some(100), None).unwrap();
        assert_eq!(result.start, 0);
        assert_eq!(result.end, 3);
        assert!(result.next_paging_state.is_none());
    }

    #[test]
    fn pagination_zero_rows() {
        let result = apply_pagination(0, Some(10), None).unwrap();
        assert_eq!(result.start, 0);
        assert_eq!(result.end, 0);
        assert!(result.next_paging_state.is_none());
    }

    #[test]
    fn pagination_negative_page_size() {
        // Negative page_size treated as no pagination
        let result = apply_pagination(10, Some(-1), None).unwrap();
        assert_eq!(result.start, 0);
        assert_eq!(result.end, 10);
        assert!(result.next_paging_state.is_none());
    }

    #[test]
    fn pagination_stale_paging_state() {
        // Paging state pointing past the end of results
        let state = PagingState {
            partition_key: (100u64).to_be_bytes().to_vec(),
            clustering_key: Vec::new(),
            remaining_in_partition: false,
        };
        let result = apply_pagination(5, Some(3), Some(&state.encode())).unwrap();
        assert_eq!(result.start, 0);
        assert_eq!(result.end, 0);
        assert!(result.next_paging_state.is_none());
    }
}
