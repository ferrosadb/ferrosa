//! Exact geometric refinement: haversine distance and bounding-box containment.
//!
//! Cell-id covering ranges are a coarse over-approximation. After fetching
//! candidate rows from the index, callers must refine with these exact
//! predicates to drop false positives.

/// Mean Earth radius in meters (WGS84 spherical approximation).
pub const EARTH_RADIUS_M: f64 = 6_371_008.8;

/// Great-circle distance between two `(lat, lon)` points in meters, on a
/// spherical Earth (haversine formula). Correct across the antimeridian and at
/// the poles because it operates on absolute angular positions.
pub fn haversine_m(lat1: f64, lon1: f64, lat2: f64, lon2: f64) -> f64 {
    let (p1, p2) = (lat1.to_radians(), lat2.to_radians());
    let dphi = (lat2 - lat1).to_radians();
    let dlambda = (lon2 - lon1).to_radians();
    let a = (dphi / 2.0).sin().powi(2) + p1.cos() * p2.cos() * (dlambda / 2.0).sin().powi(2);
    2.0 * EARTH_RADIUS_M * a.sqrt().clamp(0.0, 1.0).asin()
}

/// True if `(lat, lon)` is within `radius_m` meters of `(clat, clon)`.
pub fn within_radius(clat: f64, clon: f64, radius_m: f64, lat: f64, lon: f64) -> bool {
    haversine_m(clat, clon, lat, lon) <= radius_m
}

/// Normalize longitude into `[-180, 180)` for containment comparisons.
fn norm_lon(lon: f64) -> f64 {
    let mut x = (lon + 180.0) % 360.0;
    if x < 0.0 {
        x += 360.0;
    }
    x - 180.0
}

/// True if `(lat, lon)` falls inside the axis-aligned box defined by its
/// south-west `(sw_lat, sw_lon)` and north-east `(ne_lat, ne_lon)` corners.
///
/// When `sw_lon > ne_lon` the box is treated as **crossing the antimeridian**
/// (e.g. SW lon = 170, NE lon = -170 spans the 20° straddling ±180°), so the
/// longitude test becomes a union of two ranges.
pub fn within_bbox(sw_lat: f64, sw_lon: f64, ne_lat: f64, ne_lon: f64, lat: f64, lon: f64) -> bool {
    if lat < sw_lat || lat > ne_lat {
        return false;
    }
    // A box spanning a full 360° of longitude (e.g. -180..180) covers all
    // meridians regardless of how the corners normalize.
    if (ne_lon - sw_lon).abs() >= 360.0 - f64::EPSILON {
        return true;
    }
    let (sw_lon, ne_lon, lon) = (norm_lon(sw_lon), norm_lon(ne_lon), norm_lon(lon));
    if sw_lon <= ne_lon {
        lon >= sw_lon && lon <= ne_lon
    } else {
        // Crosses the antimeridian: inside if east of SW or west of NE.
        lon >= sw_lon || lon <= ne_lon
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn haversine_zero_for_same_point() {
        assert!(haversine_m(37.77, -122.42, 37.77, -122.42) < 1e-6);
    }

    #[test]
    fn haversine_known_distance_sf_to_nyc() {
        // SF to NYC is ~4130 km; allow a generous tolerance for the sphere.
        let d = haversine_m(37.7749, -122.4194, 40.7128, -74.006);
        assert!((d - 4_129_000.0).abs() < 50_000.0, "got {d}");
    }

    #[test]
    fn haversine_across_antimeridian_is_short() {
        // 0.0002 degrees of longitude near the equator is ~22 m, not ~40000 km.
        let d = haversine_m(0.0, 179.9999, 0.0, -179.9999);
        assert!(d < 100.0, "got {d}");
    }

    #[test]
    fn within_radius_includes_and_excludes() {
        assert!(within_radius(37.77, -122.42, 2000.0, 37.775, -122.425));
        assert!(!within_radius(37.77, -122.42, 100.0, 40.71, -74.0));
    }

    #[test]
    fn bbox_simple_containment() {
        assert!(within_bbox(37.70, -122.52, 37.83, -122.35, 37.77, -122.42));
        assert!(!within_bbox(37.70, -122.52, 37.83, -122.35, 39.0, -122.42));
        assert!(!within_bbox(37.70, -122.52, 37.83, -122.35, 37.77, -120.0));
    }

    #[test]
    fn bbox_crossing_antimeridian() {
        // Box from lon 170 east to lon -170 spans 20 degrees over the dateline.
        assert!(within_bbox(-10.0, 170.0, 10.0, -170.0, 0.0, 179.0));
        assert!(within_bbox(-10.0, 170.0, 10.0, -170.0, 0.0, -179.0));
        assert!(!within_bbox(-10.0, 170.0, 10.0, -170.0, 0.0, 0.0));
    }

    #[test]
    fn bbox_polar_latitude_band() {
        assert!(within_bbox(80.0, -180.0, 90.0, 180.0, 85.0, 100.0));
        assert!(!within_bbox(80.0, -180.0, 90.0, 180.0, 70.0, 100.0));
    }
}
