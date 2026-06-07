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

use crate::error::CqlError;

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
    pub fn encode(&self) -> Vec<u8> {
        let total_len = 4 + self.partition_key.len() + 4 + self.clustering_key.len() + 1;
        let mut buf = Vec::with_capacity(total_len);
        buf.extend_from_slice(&(self.partition_key.len() as u32).to_be_bytes());
        buf.extend_from_slice(&self.partition_key);
        buf.extend_from_slice(&(self.clustering_key.len() as u32).to_be_bytes());
        buf.extend_from_slice(&self.clustering_key);
        buf.push(if self.remaining_in_partition { 1 } else { 0 });
        buf
    }

    /// Deserialize from opaque bytes received in QUERY/EXECUTE frames.
    pub fn decode(bytes: &[u8]) -> Result<Self, CqlError> {
        if bytes.len() < 9 {
            return Err(CqlError::Protocol("paging_state too short".into()));
        }

        let mut pos = 0;

        // Partition key
        let pk_len = u32::from_be_bytes(bytes[pos..pos + 4].try_into().unwrap()) as usize;
        pos += 4;
        if pos + pk_len > bytes.len() {
            return Err(CqlError::Protocol(
                "paging_state: partition key truncated".into(),
            ));
        }
        let partition_key = bytes[pos..pos + pk_len].to_vec();
        pos += pk_len;

        // Clustering key
        if pos + 4 > bytes.len() {
            return Err(CqlError::Protocol(
                "paging_state: clustering key length truncated".into(),
            ));
        }
        let ck_len = u32::from_be_bytes(bytes[pos..pos + 4].try_into().unwrap()) as usize;
        pos += 4;
        if pos + ck_len > bytes.len() {
            return Err(CqlError::Protocol(
                "paging_state: clustering key truncated".into(),
            ));
        }
        let clustering_key = bytes[pos..pos + ck_len].to_vec();
        pos += ck_len;

        // Remaining flag
        if pos >= bytes.len() {
            return Err(CqlError::Protocol(
                "paging_state: missing remaining_in_partition flag".into(),
            ));
        }
        let remaining_in_partition = bytes[pos] != 0;

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
