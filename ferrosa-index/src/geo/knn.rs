//! Expanding-ring k-nearest-neighbour search over the geo cell index.
//!
//! k-NN has no fixed radius, so we cannot compute a single covering up front.
//! Instead we probe rings of geometrically increasing radius around the query
//! point, fetch candidates inside each ring's covering, and stop once we hold
//! `k` results *and* the next ring cannot contain anything closer than the
//! current k-th nearest. The fetch is supplied as a closure so this module
//! stays storage-free and unit-testable.

use super::cover::{cover_radius, CellRange, DEFAULT_COVER_LEVEL};
use super::refine::haversine_m;

/// A scored candidate during k-NN: an opaque caller-supplied id plus its point.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct GeoCandidate<T> {
    pub id: T,
    pub lat: f64,
    pub lon: f64,
}

/// Starting probe radius in meters for the first expanding ring.
pub const INITIAL_RING_RADIUS_M: f64 = 1_000.0;

/// Multiplier applied to the radius on each unsuccessful expansion.
pub const RING_GROWTH: f64 = 4.0;

/// Hard cap on ring expansions so a sparse dataset cannot loop unboundedly.
/// `1000 * 4^11` already exceeds Earth's circumference.
pub const MAX_RINGS: u32 = 12;

/// Run expanding-ring k-NN around `(qlat, qlon)`.
///
/// `fetch` receives the covering ranges for the current probe radius and must
/// return all candidate points whose cell ids fall inside any range. The same
/// candidate may be returned across rings; results are de-duplicated by the
/// caller's `T` (which must be `Eq`-comparable via the returned vec — we dedup
/// on exact coordinates and id). Returns up to `k` candidates sorted nearest
/// first.
pub fn nearest_k<T, F>(qlat: f64, qlon: f64, k: usize, mut fetch: F) -> Vec<GeoCandidate<T>>
where
    T: Clone + PartialEq,
    F: FnMut(&[CellRange]) -> Vec<GeoCandidate<T>>,
{
    if k == 0 {
        return Vec::new();
    }
    let mut radius = INITIAL_RING_RADIUS_M;
    let mut best: Vec<(f64, GeoCandidate<T>)> = Vec::new();

    for _ring in 0..MAX_RINGS {
        let ranges = cover_radius(qlat, qlon, radius, DEFAULT_COVER_LEVEL);
        let candidates = fetch(&ranges);
        merge_candidates(qlat, qlon, candidates, &mut best);
        best.sort_by(|a, b| a.0.total_cmp(&b.0));

        // Stop when we have k and the kth distance is within the probed radius,
        // so no closer point can hide in a wider ring.
        if best.len() >= k && best[k - 1].0 <= radius {
            break;
        }
        radius *= RING_GROWTH;
    }

    best.into_iter().take(k).map(|(_, c)| c).collect()
}

/// Fold new candidates into the running best list, computing exact distance and
/// dropping duplicates (same id + coordinates already present).
fn merge_candidates<T: PartialEq>(
    qlat: f64,
    qlon: f64,
    candidates: Vec<GeoCandidate<T>>,
    best: &mut Vec<(f64, GeoCandidate<T>)>,
) {
    for c in candidates {
        let dup = best
            .iter()
            .any(|(_, e)| e.id == c.id && e.lat == c.lat && e.lon == c.lon);
        if dup {
            continue;
        }
        let d = haversine_m(qlat, qlon, c.lat, c.lon);
        best.push((d, c));
    }
}

#[cfg(test)]
mod tests {
    use super::super::cover::CellRange;
    use super::super::encode::encode_point;
    use super::*;

    fn in_ranges(ranges: &[CellRange], lat: f64, lon: f64) -> bool {
        let id = encode_point(lat, lon);
        ranges.iter().any(|r| id >= r.start && id <= r.end)
    }

    #[test]
    fn knn_returns_k_nearest_in_order() {
        let points = vec![
            (1u32, 37.7749, -122.4194), // query point itself
            (2, 37.7849, -122.4094),    // ~1.4 km
            (3, 37.8049, -122.3894),    // ~4 km
            (4, 40.7128, -74.0060),     // NYC, far
        ];
        let pts = points.clone();
        let result = nearest_k(37.7749, -122.4194, 2, |ranges| {
            pts.iter()
                .filter(|(_, la, lo)| in_ranges(ranges, *la, *lo))
                .map(|(id, la, lo)| GeoCandidate {
                    id: *id,
                    lat: *la,
                    lon: *lo,
                })
                .collect()
        });
        assert_eq!(result.len(), 2);
        assert_eq!(result[0].id, 1);
        assert_eq!(result[1].id, 2);
    }

    #[test]
    fn knn_k_zero_is_empty() {
        let result: Vec<GeoCandidate<u32>> = nearest_k(0.0, 0.0, 0, |_| {
            vec![GeoCandidate {
                id: 1,
                lat: 0.0,
                lon: 0.0,
            }]
        });
        assert!(result.is_empty());
    }

    #[test]
    fn knn_expands_to_find_sparse_neighbours() {
        // Only one point, ~50 km away — requires several ring expansions.
        let far = (9u32, 38.2, -122.42);
        let result = nearest_k(37.77, -122.42, 1, |ranges| {
            if in_ranges(ranges, far.1, far.2) {
                vec![GeoCandidate {
                    id: far.0,
                    lat: far.1,
                    lon: far.2,
                }]
            } else {
                Vec::new()
            }
        });
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].id, 9);
    }

    #[test]
    fn knn_across_antimeridian() {
        let pts = [(1u32, 0.0, 179.99), (2, 0.0, -179.99), (3, 0.0, 178.0)];
        let result = nearest_k(0.0, 180.0, 2, |ranges| {
            pts.iter()
                .filter(|(_, la, lo)| in_ranges(ranges, *la, *lo))
                .map(|(id, la, lo)| GeoCandidate {
                    id: *id,
                    lat: *la,
                    lon: *lo,
                })
                .collect()
        });
        assert_eq!(result.len(), 2);
        // The two points straddling the dateline are nearest to lon 180.
        let ids: Vec<u32> = result.iter().map(|c| c.id).collect();
        assert!(ids.contains(&1) && ids.contains(&2), "got {ids:?}");
    }

    #[test]
    fn knn_dedups_repeated_candidates() {
        let result = nearest_k(0.0, 0.0, 3, |_| {
            vec![
                GeoCandidate {
                    id: 1u32,
                    lat: 0.01,
                    lon: 0.01,
                },
                GeoCandidate {
                    id: 1,
                    lat: 0.01,
                    lon: 0.01,
                },
            ]
        });
        assert_eq!(result.len(), 1);
    }
}
