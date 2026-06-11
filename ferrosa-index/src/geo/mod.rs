//! Geospatial point index (Phase 1): a space-filling-curve encoder over the
//! existing BTree sidecar.
//!
//! A `(lat, lon)` point is encoded to a sortable `u64` **cell id** (see
//! [`encode`]). Nearby points share high-order bits, so a bbox/radius/k-NN
//! query becomes a small set of contiguous cell-id ranges over the sorted
//! index ([`cover`], [`knn`]), refined with exact distance/containment
//! ([`refine`]). This module is pure — it never touches storage; the geo-aware
//! index *builder* derives cell ids and writes them through the BTree sidecar,
//! and the *reader* maps a [`GeoPredicate`] to [`cover::CellRange`]s.

pub mod cover;
pub mod encode;
pub mod knn;
pub mod refine;

pub use cover::{cover_bbox, cover_radius, CellRange, DEFAULT_COVER_LEVEL};
pub use encode::{encode_cell, encode_point, BITS_PER_AXIS, CELL_ID_BITS};
pub use knn::{nearest_k, GeoCandidate};
pub use refine::{haversine_m, within_bbox, within_radius, EARTH_RADIUS_M};

use std::fmt;

/// Coordinate reference system / distance model for a geo index. The analog of
/// `DistanceMetric` for vector indexes.
#[derive(
    Debug, Default, Clone, Copy, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize,
)]
pub enum GeoCrs {
    /// WGS84 on a sphere; distances via haversine, meters. The default.
    #[default]
    Wgs84Spherical,
    /// Planar (flat) approximation; cheap, only valid for small local extents.
    Planar,
}

impl fmt::Display for GeoCrs {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            GeoCrs::Wgs84Spherical => f.write_str("wgs84_spherical"),
            GeoCrs::Planar => f.write_str("planar"),
        }
    }
}

/// A geo query predicate, the input to the read binding. Each variant maps to a
/// set of covering cell-id ranges plus an exact refinement test.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum GeoPredicate {
    /// k nearest neighbours of `(lat, lon)`.
    Nearest { lat: f64, lon: f64, k: usize },
    /// All points within `radius_m` meters of `(lat, lon)`.
    WithinRadius { lat: f64, lon: f64, radius_m: f64 },
    /// All points inside the box with `sw`/`ne` `(lat, lon)` corners.
    WithinBbox { sw: (f64, f64), ne: (f64, f64) },
}

impl GeoPredicate {
    /// Covering cell-id ranges for this predicate at `level`. For
    /// [`GeoPredicate::Nearest`] this returns the *initial* ring only; k-NN
    /// expansion is driven by [`knn::nearest_k`].
    pub fn covering(&self, level: u32) -> Vec<CellRange> {
        match *self {
            GeoPredicate::Nearest { lat, lon, .. } => {
                cover_radius(lat, lon, knn::INITIAL_RING_RADIUS_M, level)
            }
            GeoPredicate::WithinRadius { lat, lon, radius_m } => {
                cover_radius(lat, lon, radius_m, level)
            }
            GeoPredicate::WithinBbox { sw, ne } => cover_bbox(sw, ne, level),
        }
    }

    /// Exact refinement test: true if `(lat, lon)` truly satisfies the
    /// predicate (drops cell-cover false positives). [`GeoPredicate::Nearest`]
    /// has no scalar test — every candidate is kept and ranked — so it returns
    /// `true`.
    pub fn refine(&self, lat: f64, lon: f64) -> bool {
        match *self {
            GeoPredicate::Nearest { .. } => true,
            GeoPredicate::WithinRadius {
                lat: clat,
                lon: clon,
                radius_m,
            } => within_radius(clat, clon, radius_m, lat, lon),
            GeoPredicate::WithinBbox { sw, ne } => within_bbox(sw.0, sw.1, ne.0, ne.1, lat, lon),
        }
    }
}

/// Encode a `(lat, lon)` point to the 8-byte big-endian cell-id key used as the
/// BTree sidecar key. Big-endian keeps cell ids sortable as raw bytes, which is
/// what the BTree index compares.
pub fn cell_id_key(lat: f64, lon: f64) -> [u8; 8] {
    encode_point(lat, lon).to_be_bytes()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn crs_default_is_spherical() {
        assert_eq!(GeoCrs::default(), GeoCrs::Wgs84Spherical);
        assert_eq!(GeoCrs::default().to_string(), "wgs84_spherical");
    }

    #[test]
    fn cell_id_key_is_sortable_big_endian() {
        let a = cell_id_key(0.0, 0.0);
        let b = cell_id_key(0.0, 0.0);
        assert_eq!(a, b);
        // Big-endian bytes compare in the same order as the u64 id.
        let id_a = encode_point(10.0, 20.0);
        let id_b = encode_point(11.0, 20.0);
        let key_a = cell_id_key(10.0, 20.0);
        let key_b = cell_id_key(11.0, 20.0);
        assert_eq!(id_a < id_b, key_a < key_b);
    }

    #[test]
    fn within_radius_predicate_refines() {
        let p = GeoPredicate::WithinRadius {
            lat: 37.77,
            lon: -122.42,
            radius_m: 1500.0,
        };
        assert!(p.refine(37.775, -122.425));
        assert!(!p.refine(40.71, -74.0));
        assert!(!p.covering(DEFAULT_COVER_LEVEL).is_empty());
    }

    #[test]
    fn bbox_predicate_refines() {
        let p = GeoPredicate::WithinBbox {
            sw: (37.70, -122.52),
            ne: (37.83, -122.35),
        };
        assert!(p.refine(37.77, -122.42));
        assert!(!p.refine(39.0, -122.42));
    }

    #[test]
    fn nearest_predicate_keeps_all_candidates() {
        let p = GeoPredicate::Nearest {
            lat: 0.0,
            lon: 0.0,
            k: 5,
        };
        assert!(p.refine(80.0, 170.0));
        assert!(!p.covering(DEFAULT_COVER_LEVEL).is_empty());
    }
}
