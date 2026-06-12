//! Exact `ST_CONTAINS` / `ST_INTERSECTS` predicates between two *stored*
//! geometries, the algorithmic foundation of geospatial slice **P2-c**.
//!
//! The geometries are the marshalled WKB values produced by
//! [`ferrosa_common::Geometry`] (slice P2-b): a `Point` or a single-outer-ring
//! `Polygon`. This module bridges that marshalled value to the pure
//! point-in-polygon / ray-cast machinery in [`super::geometry`] and answers the
//! predicates **exactly** for every combination it supports.
//!
//! ## Scope and loud rejection
//!
//! Polygon-vs-polygon containment/intersection needs ring-edge crossing
//! detection (and, for holes/multi-ring, the richer geometry deferred to P2-d).
//! Rather than return a plausible-but-wrong answer for that case, the helpers
//! return [`PredicateError::UnsupportedPair`] so the caller rejects the query
//! loudly. Antimeridian-crossing polygons never reach here: WKB parsing rejects
//! them before a `Geometry` is ever constructed.

use ferrosa_common::Geometry;

use super::geometry::{point_in_polygon, Polygon};

/// Why an `ST_*` predicate could not be evaluated exactly.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PredicateError {
    /// The two-geometry combination is not yet supported exactly (polygon vs
    /// polygon). Deferred to P2-d; the caller must reject the query rather than
    /// guess. Carries a human-readable description of the offending pair.
    UnsupportedPair(String),
}

impl std::fmt::Display for PredicateError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PredicateError::UnsupportedPair(pair) => {
                write!(
                    f,
                    "ST predicate unsupported for geometry pair ({pair}); \
                     polygon-vs-polygon is deferred to P2-d"
                )
            }
        }
    }
}

impl std::error::Error for PredicateError {}

/// Convert a marshalled polygon `Geometry` into the index's [`Polygon`].
fn to_polygon(ring: &[(f64, f64)]) -> Polygon {
    Polygon::new(ring.to_vec())
}

/// `ST_CONTAINS(a, b)`: does geometry `a` spatially contain geometry `b`?
///
/// Exact for the supported combinations:
/// - **Point contains Point** — iff the two points are equal.
/// - **Polygon contains Point** — iff the point lies inside the polygon
///   (boundary counts as inside, matching [`point_in_polygon`]).
/// - **Point contains Polygon** — never (a point has no area).
///
/// Polygon-vs-polygon returns [`PredicateError::UnsupportedPair`] (P2-d).
pub fn st_contains(a: &Geometry, b: &Geometry) -> Result<bool, PredicateError> {
    match (a, b) {
        (Geometry::Point { lat: la, lon: loa }, Geometry::Point { lat: lb, lon: lob }) => {
            Ok(la == lb && loa == lob)
        }
        (Geometry::Polygon { ring }, Geometry::Point { lat, lon }) => {
            Ok(point_in_polygon(*lat, *lon, &to_polygon(ring)))
        }
        (Geometry::Point { .. }, Geometry::Polygon { .. }) => Ok(false),
        (Geometry::Polygon { .. }, Geometry::Polygon { .. }) => Err(
            PredicateError::UnsupportedPair("polygon, polygon".to_string()),
        ),
    }
}

/// `ST_INTERSECTS(a, b)`: do geometries `a` and `b` share at least one point?
///
/// Symmetric. Exact for the supported combinations:
/// - **Point / Point** — iff equal.
/// - **Point / Polygon** (either order) — iff the point lies inside (or on the
///   boundary of) the polygon.
///
/// Polygon-vs-polygon returns [`PredicateError::UnsupportedPair`] (P2-d).
pub fn st_intersects(a: &Geometry, b: &Geometry) -> Result<bool, PredicateError> {
    match (a, b) {
        (Geometry::Point { lat: la, lon: loa }, Geometry::Point { lat: lb, lon: lob }) => {
            Ok(la == lb && loa == lob)
        }
        (Geometry::Polygon { ring }, Geometry::Point { lat, lon })
        | (Geometry::Point { lat, lon }, Geometry::Polygon { ring }) => {
            Ok(point_in_polygon(*lat, *lon, &to_polygon(ring)))
        }
        (Geometry::Polygon { .. }, Geometry::Polygon { .. }) => Err(
            PredicateError::UnsupportedPair("polygon, polygon".to_string()),
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sf_square() -> Geometry {
        Geometry::polygon(vec![
            (37.70, -122.52),
            (37.83, -122.52),
            (37.83, -122.35),
            (37.70, -122.35),
        ])
    }

    fn ferry_building() -> Geometry {
        Geometry::Point {
            lat: 37.7955,
            lon: -122.3937,
        }
    }

    fn nyc() -> Geometry {
        Geometry::Point {
            lat: 40.7580,
            lon: -73.9855,
        }
    }

    #[test]
    fn polygon_contains_interior_point() {
        assert!(st_contains(&sf_square(), &ferry_building()).unwrap());
    }

    #[test]
    fn polygon_does_not_contain_exterior_point() {
        assert!(!st_contains(&sf_square(), &nyc()).unwrap());
    }

    #[test]
    fn polygon_contains_is_not_symmetric_for_point() {
        // A point never contains a polygon.
        assert!(!st_contains(&ferry_building(), &sf_square()).unwrap());
    }

    #[test]
    fn point_contains_only_equal_point() {
        assert!(st_contains(&ferry_building(), &ferry_building()).unwrap());
        assert!(!st_contains(&ferry_building(), &nyc()).unwrap());
    }

    #[test]
    fn intersects_is_symmetric_for_point_and_polygon() {
        assert!(st_intersects(&sf_square(), &ferry_building()).unwrap());
        assert!(st_intersects(&ferry_building(), &sf_square()).unwrap());
    }

    #[test]
    fn intersects_false_for_exterior_point() {
        assert!(!st_intersects(&sf_square(), &nyc()).unwrap());
        assert!(!st_intersects(&nyc(), &sf_square()).unwrap());
    }

    #[test]
    fn intersects_points_iff_equal() {
        assert!(st_intersects(&ferry_building(), &ferry_building()).unwrap());
        assert!(!st_intersects(&ferry_building(), &nyc()).unwrap());
    }

    #[test]
    fn boundary_point_is_contained() {
        // A point exactly on the polygon's south edge (lat 37.70) is inside.
        let edge = Geometry::Point {
            lat: 37.70,
            lon: -122.45,
        };
        assert!(st_contains(&sf_square(), &edge).unwrap());
        assert!(st_intersects(&sf_square(), &edge).unwrap());
    }

    #[test]
    fn polygon_polygon_contains_is_rejected_loudly() {
        let err = st_contains(&sf_square(), &sf_square()).unwrap_err();
        assert!(matches!(err, PredicateError::UnsupportedPair(_)));
        assert!(err.to_string().contains("polygon"));
    }

    #[test]
    fn polygon_polygon_intersects_is_rejected_loudly() {
        let err = st_intersects(&sf_square(), &sf_square()).unwrap_err();
        assert!(matches!(err, PredicateError::UnsupportedPair(_)));
    }
}
