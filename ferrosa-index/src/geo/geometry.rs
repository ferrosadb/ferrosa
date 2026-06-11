//! Pure polygon geometry: a single-outer-ring `Polygon` over `(lat, lon)`
//! vertices, an exact point-in-polygon test (ray casting), and a bounding box.
//!
//! This module is the exact-refinement counterpart of [`super::refine`] for the
//! `ST_WITHIN(point, polygon)` query: the cell-cover of the polygon's bbox is an
//! over-approximation, so each candidate point fetched from the index is tested
//! here for true containment.
//!
//! ## Antimeridian limitation
//!
//! Ray casting operates on the raw `(lat, lon)` plane. A polygon whose vertices
//! straddle the ±180° meridian (e.g. one vertex at lon 179, the next at lon
//! -179) is **not** handled — the edge between them is treated as wrapping the
//! long way around the globe, which is wrong. We do **not** silently produce a
//! plausible-but-wrong answer: [`Polygon::crosses_antimeridian`] detects this
//! case so callers can reject it loudly. Splitting such a polygon at the
//! meridian is deferred (it needs the same two-span treatment the bbox cover
//! already does). For all polygons that stay within a single ±180° longitude
//! span, the test is exact.

/// A simple polygon defined by a single outer ring of `(lat, lon)` vertices in
/// degrees. The ring is treated as implicitly closed: the last vertex connects
/// back to the first, whether or not the caller repeats it. Holes and multiple
/// rings are not supported (a Phase-2 foundation slice).
#[derive(Debug, Clone, PartialEq)]
pub struct Polygon {
    /// Outer-ring vertices, in `(lat, lon)` order.
    vertices: Vec<(f64, f64)>,
}

impl Polygon {
    /// Build a polygon from an outer ring of `(lat, lon)` vertices. A trailing
    /// vertex equal to the first is dropped (the ring is implicitly closed), so
    /// both open and explicitly-closed rings produce the same polygon.
    ///
    /// A polygon needs at least three distinct vertices to enclose any area; a
    /// ring with fewer is **degenerate** and reports
    /// [`Polygon::is_degenerate`] `== true`. Such a polygon contains no points.
    pub fn new(vertices: Vec<(f64, f64)>) -> Self {
        let mut vertices = vertices;
        // Drop an explicit closing vertex so iteration over edges is uniform.
        if vertices.len() >= 2 && vertices.first() == vertices.last() {
            vertices.pop();
        }
        Polygon { vertices }
    }

    /// The outer-ring vertices (closing vertex already stripped).
    pub fn vertices(&self) -> &[(f64, f64)] {
        &self.vertices
    }

    /// True if the ring cannot enclose any area (fewer than three vertices).
    /// A degenerate polygon contains no points.
    pub fn is_degenerate(&self) -> bool {
        self.vertices.len() < 3
    }

    /// True if any ring edge spans more than 180° of longitude, which happens
    /// when the polygon crosses the ±180° antimeridian. Ray casting is not
    /// correct for such polygons (see the module docs), so callers should
    /// reject them rather than return a wrong result.
    pub fn crosses_antimeridian(&self) -> bool {
        let n = self.vertices.len();
        if n < 2 {
            return false;
        }
        (0..n).any(|i| {
            let (_, lon_a) = self.vertices[i];
            let (_, lon_b) = self.vertices[(i + 1) % n];
            (lon_a - lon_b).abs() > 180.0
        })
    }
}

/// Axis-aligned bounding box of a polygon as `(sw, ne)` corners, each `(lat,
/// lon)`. Returns `None` for a polygon with no vertices.
pub fn polygon_bbox(poly: &Polygon) -> Option<((f64, f64), (f64, f64))> {
    let verts = poly.vertices();
    let (first_lat, first_lon) = *verts.first()?;
    let mut min_lat = first_lat;
    let mut max_lat = first_lat;
    let mut min_lon = first_lon;
    let mut max_lon = first_lon;
    for &(lat, lon) in verts.iter().skip(1) {
        min_lat = min_lat.min(lat);
        max_lat = max_lat.max(lat);
        min_lon = min_lon.min(lon);
        max_lon = max_lon.max(lon);
    }
    Some(((min_lat, min_lon), (max_lat, max_lon)))
}

/// Exact point-in-polygon test for `(lat, lon)` against a single-ring polygon,
/// via ray casting (the even–odd / crossing-number rule).
///
/// A horizontal ray is cast east from the point; the number of ring edges it
/// crosses determines inside (odd) vs outside (even). A point exactly **on** an
/// edge or vertex is treated as inside (`true`) so boundary points are never
/// silently dropped.
///
/// Degenerate polygons (`< 3` vertices) contain no points and return `false`.
/// The ray is cast along longitude at fixed latitude; this is exact for any
/// polygon that does not cross the antimeridian (see module docs).
pub fn point_in_polygon(lat: f64, lon: f64, poly: &Polygon) -> bool {
    let verts = poly.vertices();
    if poly.is_degenerate() {
        return false;
    }
    let n = verts.len();
    let mut inside = false;
    let mut j = n - 1;
    for i in 0..n {
        let (lat_i, lon_i) = verts[i];
        let (lat_j, lon_j) = verts[j];

        // On-boundary check: the point lies on the edge (i, j) → inside.
        if point_on_segment(lat, lon, lat_i, lon_i, lat_j, lon_j) {
            return true;
        }

        // Standard crossing test: does the edge straddle the point's latitude,
        // and is the edge's longitude at that latitude to the east (> lon)?
        let straddles = (lat_i > lat) != (lat_j > lat);
        if straddles {
            // Longitude of the edge at the point's latitude.
            let cross_lon = lon_i + (lat - lat_i) / (lat_j - lat_i) * (lon_j - lon_i);
            if lon < cross_lon {
                inside = !inside;
            }
        }
        j = i;
    }
    inside
}

/// True if `(lat, lon)` lies on the closed segment from `(lat_a, lon_a)` to
/// `(lat_b, lon_b)`, within a small numerical tolerance.
fn point_on_segment(lat: f64, lon: f64, lat_a: f64, lon_a: f64, lat_b: f64, lon_b: f64) -> bool {
    // Collinearity via the cross product of (b - a) and (p - a). Near zero ⇒
    // the three points are collinear.
    let cross = (lon_b - lon_a) * (lat - lat_a) - (lat_b - lat_a) * (lon - lon_a);
    const EPS: f64 = 1e-12;
    if cross.abs() > EPS {
        return false;
    }
    // Collinear: confirm the point is within the segment's bounding box.
    let within_lat = lat >= lat_a.min(lat_b) - EPS && lat <= lat_a.max(lat_b) + EPS;
    let within_lon = lon >= lon_a.min(lon_b) - EPS && lon <= lon_a.max(lon_b) + EPS;
    within_lat && within_lon
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A unit square (lat 0..1, lon 0..1) used by several containment tests.
    fn unit_square() -> Polygon {
        Polygon::new(vec![(0.0, 0.0), (0.0, 1.0), (1.0, 1.0), (1.0, 0.0)])
    }

    #[test]
    fn explicit_closing_vertex_is_stripped() {
        let open = Polygon::new(vec![(0.0, 0.0), (0.0, 1.0), (1.0, 1.0)]);
        let closed = Polygon::new(vec![(0.0, 0.0), (0.0, 1.0), (1.0, 1.0), (0.0, 0.0)]);
        assert_eq!(open.vertices(), closed.vertices());
        assert_eq!(open.vertices().len(), 3);
    }

    #[test]
    fn point_inside_square() {
        let sq = unit_square();
        assert!(point_in_polygon(0.5, 0.5, &sq));
    }

    #[test]
    fn point_outside_square() {
        let sq = unit_square();
        assert!(!point_in_polygon(2.0, 0.5, &sq));
        assert!(!point_in_polygon(0.5, 2.0, &sq));
        assert!(!point_in_polygon(-1.0, -1.0, &sq));
    }

    #[test]
    fn point_on_edge_is_inside() {
        let sq = unit_square();
        // Mid-edge along the bottom (lat=0).
        assert!(point_in_polygon(0.0, 0.5, &sq));
        // Mid-edge along the right side (lon=1).
        assert!(point_in_polygon(0.5, 1.0, &sq));
    }

    #[test]
    fn vertex_is_inside() {
        let sq = unit_square();
        assert!(point_in_polygon(0.0, 0.0, &sq));
        assert!(point_in_polygon(1.0, 1.0, &sq));
    }

    #[test]
    fn concave_polygon_excludes_the_notch() {
        // An arrow / chevron shape with a notch cut into the top edge.
        //   (0,0) -- (0,4) -- (2,4) -- (1,2) -- (4,4) ... actually build a
        // simple "C"-ish concave poly: a square with a triangular bite.
        let poly = Polygon::new(vec![
            (0.0, 0.0),
            (0.0, 4.0),
            (4.0, 4.0),
            (4.0, 0.0),
            (2.0, 0.0),
            (2.0, 3.0), // notch pushes inward
            (1.0, 3.0),
            (1.0, 0.0),
        ]);
        // A point inside the main body.
        assert!(point_in_polygon(0.5, 2.0, &poly));
        // A point inside the notch (between the two inward fingers) is OUTSIDE.
        assert!(!point_in_polygon(1.5, 1.0, &poly));
    }

    #[test]
    fn winding_independent_of_orientation() {
        // Same square wound clockwise instead of counter-clockwise.
        let cw = Polygon::new(vec![(0.0, 0.0), (1.0, 0.0), (1.0, 1.0), (0.0, 1.0)]);
        assert!(point_in_polygon(0.5, 0.5, &cw));
        assert!(!point_in_polygon(5.0, 5.0, &cw));
    }

    #[test]
    fn degenerate_polygon_contains_nothing() {
        let line = Polygon::new(vec![(0.0, 0.0), (0.0, 1.0)]);
        assert!(line.is_degenerate());
        assert!(!point_in_polygon(0.0, 0.5, &line));
        let point = Polygon::new(vec![(0.0, 0.0)]);
        assert!(point.is_degenerate());
        assert!(!point_in_polygon(0.0, 0.0, &point));
        let empty = Polygon::new(vec![]);
        assert!(empty.is_degenerate());
        assert!(!point_in_polygon(0.0, 0.0, &empty));
    }

    #[test]
    fn bbox_of_square() {
        let sq = unit_square();
        let (sw, ne) = polygon_bbox(&sq).unwrap();
        assert_eq!(sw, (0.0, 0.0));
        assert_eq!(ne, (1.0, 1.0));
    }

    #[test]
    fn bbox_of_irregular_polygon() {
        let poly = Polygon::new(vec![(37.70, -122.52), (37.83, -122.35), (37.74, -122.40)]);
        let (sw, ne) = polygon_bbox(&poly).unwrap();
        assert_eq!(sw, (37.70, -122.52));
        assert_eq!(ne, (37.83, -122.35));
    }

    #[test]
    fn bbox_of_empty_is_none() {
        assert!(polygon_bbox(&Polygon::new(vec![])).is_none());
    }

    #[test]
    fn antimeridian_polygon_is_flagged() {
        // Vertices straddling ±180° → an edge spanning > 180° of longitude.
        let poly = Polygon::new(vec![
            (-1.0, 179.0),
            (1.0, 179.0),
            (1.0, -179.0),
            (-1.0, -179.0),
        ]);
        assert!(poly.crosses_antimeridian());
        // A normal SF polygon does not.
        let sf = Polygon::new(vec![(37.70, -122.52), (37.83, -122.52), (37.83, -122.35)]);
        assert!(!sf.crosses_antimeridian());
    }

    #[test]
    fn sf_polygon_contains_sf_excludes_nyc() {
        // A polygon around central SF (matches the example query).
        let poly = Polygon::new(vec![
            (37.70, -122.52),
            (37.83, -122.52),
            (37.83, -122.35),
            (37.70, -122.35),
        ]);
        assert!(point_in_polygon(37.7955, -122.3937, &poly)); // Ferry Building
        assert!(point_in_polygon(37.7880, -122.4074, &poly)); // Union Square
        assert!(point_in_polygon(37.7694, -122.4862, &poly)); // Golden Gate Park
        assert!(!point_in_polygon(40.7580, -73.9855, &poly)); // NYC
    }
}
