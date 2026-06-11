//! Covering-range computation: turn a bbox or radius query into a small set of
//! contiguous `[start, end]` cell-id ranges over the sorted index.
//!
//! The strategy is a grid scan at a chosen `level`: walk every cell of that
//! level that overlaps the query's lat/lon box, encode each to its
//! `[start, end]` id span, then merge adjacent/overlapping spans. The result is
//! an over-approximation that the caller refines with exact distance/bbox.

use super::encode::{encode_cell, shift_for_level, wrap_lon, BITS_PER_AXIS};
use super::refine::EARTH_RADIUS_M;

/// A closed range of cell ids `[start, end]` (both inclusive).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CellRange {
    pub start: u64,
    pub end: u64,
}

/// Default cell level for covering queries. Level 16 keeps 32 interleaved bits
/// (16 per axis), giving ~0.005° cells (~600 m of latitude) — a reasonable
/// selectivity/refine trade-off.
pub const DEFAULT_COVER_LEVEL: u32 = 16;

/// Maximum number of grid cells to enumerate before widening to a coarser
/// level. Bounds the work of [`cover_bbox`] so a huge box cannot blow up.
const MAX_GRID_CELLS: u64 = 4096;

/// Degrees of latitude per cell at a given level.
fn lat_step(level: u32) -> f64 {
    180.0 / (1u64 << level.min(BITS_PER_AXIS)) as f64
}

/// Degrees of longitude per cell at a given level.
fn lon_step(level: u32) -> f64 {
    360.0 / (1u64 << level.min(BITS_PER_AXIS)) as f64
}

/// Compute covering cell-id ranges for an axis-aligned lat/lon box.
///
/// `sw`/`ne` are `(lat, lon)` corners. If `sw_lon > ne_lon` the box is taken to
/// cross the antimeridian and is scanned as two longitude sub-spans. The chosen
/// `level` is automatically coarsened if the grid would exceed `MAX_GRID_CELLS`.
pub fn cover_bbox(sw: (f64, f64), ne: (f64, f64), level: u32) -> Vec<CellRange> {
    let (sw_lat, sw_lon) = sw;
    let (ne_lat, ne_lon) = ne;
    let lat_lo = sw_lat.min(ne_lat).clamp(-90.0, 90.0);
    let lat_hi = sw_lat.max(ne_lat).clamp(-90.0, 90.0);

    let lon_spans = lon_subspans(sw_lon, ne_lon);
    let level = choose_level(lat_lo, lat_hi, &lon_spans, level);

    let mut cells = Vec::new();
    for &(lon_lo, lon_hi) in &lon_spans {
        collect_grid_cells(lat_lo, lat_hi, lon_lo, lon_hi, level, &mut cells);
    }
    merge_ranges(cells, level)
}

/// Compute covering cell-id ranges for a radius query: the bounding box of the
/// circle of `radius_m` meters around `(clat, clon)`, then [`cover_bbox`].
pub fn cover_radius(clat: f64, clon: f64, radius_m: f64, level: u32) -> Vec<CellRange> {
    let radius_m = radius_m.max(0.0);
    let dlat = (radius_m / EARTH_RADIUS_M).to_degrees();
    let coslat = clat.to_radians().cos().abs().max(1e-6);
    let dlon = (radius_m / (EARTH_RADIUS_M * coslat)).to_degrees();

    let lat_lo = (clat - dlat).clamp(-90.0, 90.0);
    let lat_hi = (clat + dlat).clamp(-90.0, 90.0);
    if dlon >= 180.0 {
        // The radius wraps the whole globe in longitude; cover the lat band.
        return cover_bbox((lat_lo, -180.0), (lat_hi, 180.0), level);
    }
    cover_bbox((lat_lo, clon - dlon), (lat_hi, clon + dlon), level)
}

/// Split the requested longitude range into one or two `[lo, hi]` spans in
/// `[-180, 180]`, handling antimeridian crossing.
fn lon_subspans(sw_lon: f64, ne_lon: f64) -> Vec<(f64, f64)> {
    let lo = wrap_lon(sw_lon);
    let mut hi = wrap_lon(ne_lon);
    if (ne_lon - sw_lon).abs() >= 360.0 {
        return vec![(-180.0, 180.0)];
    }
    if hi < lo {
        // Crosses the antimeridian: two spans.
        return vec![(lo, 180.0), (-180.0, hi)];
    }
    if (hi - lo).abs() < f64::EPSILON {
        hi = lo;
    }
    vec![(lo, hi)]
}

/// Coarsen `level` until the grid fits within [`MAX_GRID_CELLS`].
fn choose_level(lat_lo: f64, lat_hi: f64, lon_spans: &[(f64, f64)], level: u32) -> u32 {
    let mut level = level.min(BITS_PER_AXIS);
    loop {
        let nlat = ((lat_hi - lat_lo) / lat_step(level)).ceil() as u64 + 1;
        let nlon: u64 = lon_spans
            .iter()
            .map(|&(lo, hi)| ((hi - lo) / lon_step(level)).ceil() as u64 + 1)
            .sum();
        if level == 0 || nlat.saturating_mul(nlon) <= MAX_GRID_CELLS {
            return level;
        }
        level -= 1;
    }
}

/// Enumerate every level-cell overlapping the box and push its `[start, end]`.
fn collect_grid_cells(
    lat_lo: f64,
    lat_hi: f64,
    lon_lo: f64,
    lon_hi: f64,
    level: u32,
    out: &mut Vec<CellRange>,
) {
    let shift = shift_for_level(level);
    let span: u64 = if shift >= 64 {
        u64::MAX
    } else {
        (1u64 << shift) - 1
    };
    let (dlat, dlon) = (lat_step(level), lon_step(level));

    let mut lat = lat_lo;
    while lat <= lat_hi + dlat * 0.5 {
        let mut lon = lon_lo;
        while lon <= lon_hi + dlon * 0.5 {
            let start = encode_cell(lat, lon.min(180.0 - 1e-9), level);
            out.push(CellRange {
                start,
                end: start.saturating_add(span),
            });
            lon += dlon;
        }
        lat += dlat;
    }
}

/// Sort and merge overlapping/adjacent cell ranges into a minimal set.
fn merge_ranges(mut ranges: Vec<CellRange>, _level: u32) -> Vec<CellRange> {
    if ranges.is_empty() {
        return ranges;
    }
    ranges.sort_by_key(|r| r.start);
    let mut merged: Vec<CellRange> = Vec::with_capacity(ranges.len());
    for r in ranges {
        match merged.last_mut() {
            Some(last) if r.start <= last.end.saturating_add(1) => {
                last.end = last.end.max(r.end);
            }
            _ => merged.push(r),
        }
    }
    merged
}

#[cfg(test)]
mod tests {
    use super::super::encode::encode_point;
    use super::*;

    fn ranges_contain(ranges: &[CellRange], id: u64) -> bool {
        ranges.iter().any(|r| id >= r.start && id <= r.end)
    }

    #[test]
    fn bbox_covers_contained_point() {
        let ranges = cover_bbox((37.70, -122.52), (37.83, -122.35), DEFAULT_COVER_LEVEL);
        assert!(!ranges.is_empty());
        let id = encode_point(37.77, -122.42);
        assert!(ranges_contain(&ranges, id));
    }

    #[test]
    fn bbox_excludes_far_point_mostly() {
        let ranges = cover_bbox((37.70, -122.52), (37.83, -122.35), DEFAULT_COVER_LEVEL);
        let nyc = encode_point(40.71, -74.0);
        assert!(!ranges_contain(&ranges, nyc));
    }

    #[test]
    fn radius_covers_center_and_nearby() {
        let ranges = cover_radius(37.77, -122.42, 1500.0, DEFAULT_COVER_LEVEL);
        assert!(ranges_contain(&ranges, encode_point(37.77, -122.42)));
        assert!(ranges_contain(&ranges, encode_point(37.775, -122.425)));
    }

    #[test]
    fn ranges_are_sorted_and_disjoint() {
        let ranges = cover_radius(0.0, 0.0, 50_000.0, DEFAULT_COVER_LEVEL);
        for w in ranges.windows(2) {
            assert!(w[0].end < w[1].start, "ranges must be disjoint and sorted");
        }
    }

    #[test]
    fn antimeridian_bbox_produces_two_or_more_spans() {
        // SW lon 179, NE lon -179 crosses the dateline.
        let ranges = cover_bbox((-1.0, 179.0), (1.0, -179.0), DEFAULT_COVER_LEVEL);
        assert!(!ranges.is_empty());
        assert!(ranges_contain(&ranges, encode_point(0.0, 179.5)));
        assert!(ranges_contain(&ranges, encode_point(0.0, -179.5)));
    }

    #[test]
    fn polar_radius_does_not_panic() {
        let ranges = cover_radius(89.9, 0.0, 30_000.0, DEFAULT_COVER_LEVEL);
        assert!(!ranges.is_empty());
        assert!(ranges_contain(&ranges, encode_point(89.9, 0.0)));
    }

    #[test]
    fn huge_bbox_coarsens_level_and_stays_bounded() {
        let ranges = cover_bbox((-80.0, -179.0), (80.0, 179.0), DEFAULT_COVER_LEVEL);
        assert!(!ranges.is_empty());
        // Whole-world coverage should collapse to very few ranges.
        assert!(ranges.len() <= 64, "got {} ranges", ranges.len());
    }
}
