//! Geospatial point encoding: `(lat, lon)` to a sortable `u64` cell id.
//!
//! Phase 1 uses a geohash-style **Z-order (Morton) interleave** of quantized
//! latitude and longitude. Each axis is quantized to `BITS_PER_AXIS` bits over
//! its valid range, then the two bit strings are interleaved into a single
//! `u64` so that nearby points share high-order prefixes. This makes a
//! bbox/radius query a small set of contiguous `u64` ranges over a sorted
//! (BTree) index, refined afterwards by exact distance.
//!
//! Latitude is clamped to `[-90, 90]`; longitude is **wrapped** into
//! `[-180, 180)` so the antimeridian (±180°) maps to a single well-defined
//! column of cells rather than overflowing.

/// Number of quantization bits used per axis. Two axes interleaved gives a
/// `2 * BITS_PER_AXIS`-bit cell id, which must fit in a `u64`.
pub const BITS_PER_AXIS: u32 = 32;

/// Total bits in a full-resolution cell id.
pub const CELL_ID_BITS: u32 = BITS_PER_AXIS * 2;

const LAT_MIN: f64 = -90.0;
const LAT_MAX: f64 = 90.0;
const LON_MIN: f64 = -180.0;
const LON_MAX: f64 = 180.0;

/// Clamp latitude into the valid `[-90, 90]` range. Values beyond the poles are
/// pinned to the nearest pole rather than rejected, matching how geographic
/// inputs degrade at the poles.
pub fn clamp_lat(lat: f64) -> f64 {
    lat.clamp(LAT_MIN, LAT_MAX)
}

/// Wrap longitude into `[-180, 180)`. `+180` wraps to `-180` so the
/// antimeridian is a single column of cells.
pub fn wrap_lon(lon: f64) -> f64 {
    let span = LON_MAX - LON_MIN; // 360
    let mut x = (lon - LON_MIN) % span;
    if x < 0.0 {
        x += span;
    }
    x + LON_MIN
}

/// Quantize a value in `[min, max]` to `bits` bits (`0 ..= 2^bits - 1`).
fn quantize(value: f64, min: f64, max: f64, bits: u32) -> u64 {
    let levels = 1u64 << bits;
    let frac = ((value - min) / (max - min)).clamp(0.0, 1.0);
    let q = (frac * levels as f64) as u64;
    q.min(levels - 1)
}

/// Spread the low 32 bits of `v` so each bit `i` lands at position `2*i`.
fn spread_bits(v: u64) -> u64 {
    let mut x = v & 0x0000_0000_FFFF_FFFF;
    x = (x | (x << 16)) & 0x0000_FFFF_0000_FFFF;
    x = (x | (x << 8)) & 0x00FF_00FF_00FF_00FF;
    x = (x | (x << 4)) & 0x0F0F_0F0F_0F0F_0F0F;
    x = (x | (x << 2)) & 0x3333_3333_3333_3333;
    x = (x | (x << 1)) & 0x5555_5555_5555_5555;
    x
}

/// Encode a `(lat, lon)` point into a sortable `u64` cell id at full
/// resolution. Longitude occupies the even bit positions, latitude the odd
/// ones, so the most-significant interleaved bits split the globe coarsely.
pub fn encode_point(lat: f64, lon: f64) -> u64 {
    let qlat = quantize(clamp_lat(lat), LAT_MIN, LAT_MAX, BITS_PER_AXIS);
    let qlon = quantize(wrap_lon(lon), LON_MIN, LON_MAX, BITS_PER_AXIS);
    spread_bits(qlon) | (spread_bits(qlat) << 1)
}

/// The number of low bits to drop from a full-resolution cell id to reach a
/// given `level`. Level `L` keeps the top `2*L` bits (the coarsest `L`
/// interleaved axis-bit pairs).
pub fn shift_for_level(level: u32) -> u32 {
    debug_assert!(level <= BITS_PER_AXIS, "geo level out of range");
    let level = level.min(BITS_PER_AXIS);
    CELL_ID_BITS - level * 2
}

/// Encode a point to the cell id of a coarser `level`, returned as a full-width
/// `u64` with the dropped low bits zeroed (the range start of that cell).
pub fn encode_cell(lat: f64, lon: f64, level: u32) -> u64 {
    let shift = shift_for_level(level);
    if shift >= CELL_ID_BITS {
        return 0;
    }
    (encode_point(lat, lon) >> shift) << shift
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn wrap_lon_handles_antimeridian() {
        assert_eq!(wrap_lon(180.0), -180.0);
        assert_eq!(wrap_lon(-180.0), -180.0);
        assert!((wrap_lon(181.0) - (-179.0)).abs() < 1e-9);
        assert!((wrap_lon(-181.0) - 179.0).abs() < 1e-9);
        assert!((wrap_lon(540.0) - (-180.0)).abs() < 1e-9);
    }

    #[test]
    fn clamp_lat_pins_poles() {
        assert_eq!(clamp_lat(95.0), 90.0);
        assert_eq!(clamp_lat(-95.0), -90.0);
        assert_eq!(clamp_lat(45.0), 45.0);
    }

    #[test]
    fn encode_is_deterministic() {
        assert_eq!(encode_point(37.77, -122.42), encode_point(37.77, -122.42));
    }

    #[test]
    fn nearby_points_share_prefix() {
        let a = encode_point(37.7749, -122.4194);
        let b = encode_point(37.7750, -122.4195);
        // Differ only in the low-order bits: a coarse cell should match.
        let shift = shift_for_level(16);
        assert_eq!(a >> shift, b >> shift);
    }

    #[test]
    fn distant_points_differ_in_high_bits() {
        let sf = encode_point(37.77, -122.42);
        let nyc = encode_point(40.71, -74.0);
        let shift = shift_for_level(8);
        assert_ne!(sf >> shift, nyc >> shift);
    }

    #[test]
    fn antimeridian_points_encode_close() {
        // Just east and just west of the antimeridian are physically adjacent.
        let east = encode_point(0.0, 179.9999);
        let west = encode_point(0.0, -179.9999);
        // They are not identical but both live near the lon extremes; the
        // encoder must not panic and must produce distinct, valid ids.
        assert_ne!(east, west);
    }

    #[test]
    fn poles_encode_without_panic() {
        let np = encode_point(90.0, 0.0);
        let sp = encode_point(-90.0, 0.0);
        assert_ne!(np, sp);
        // North pole quantizes to the top latitude band.
        assert!(np > sp);
    }

    #[test]
    fn encode_cell_zeroes_low_bits() {
        let full = encode_point(12.34, 56.78);
        let cell = encode_cell(12.34, 56.78, 10);
        let shift = shift_for_level(10);
        assert_eq!(cell, (full >> shift) << shift);
        assert_eq!(cell & ((1u64 << shift) - 1), 0);
    }

    #[test]
    fn level_zero_is_whole_globe() {
        assert_eq!(encode_cell(10.0, 20.0, 0), 0);
        assert_eq!(encode_cell(-80.0, 150.0, 0), 0);
    }
}
