//! Stored geometry column type, marshalled as **WKB** (well-known binary).
//!
//! This is the Phase-2 **P2-b** foundation slice of the geospatial index work
//! (`specs/proposed/geospatial-index.md` §6a): a marshalled `GEOMETRY` value
//! that can be parsed from / serialized to the OGC WKB byte format so two
//! *stored* geometries can later be compared by `ST_*` predicates (P2-c).
//!
//! ## Scope (deliberately narrow, per the spec)
//!
//! Two geometry kinds are supported, both over `(lat, lon)` degrees:
//!
//! - **Point** — a single coordinate.
//! - **Polygon** — a *single outer ring* (the same shape the live `ST_WITHIN`
//!   path already uses; see `ferrosa-index::geo::geometry::Polygon`).
//!
//! Everything richer — multi-ring polygons (holes), `MultiPolygon`,
//! `LineString`, `MultiPoint`, Z/M (3D/measured) coordinates — is **rejected
//! loudly** with [`Error::InvalidData`]. We never silently drop a hole or a
//! second ring and return a plausible-but-wrong geometry. Antimeridian-crossing
//! polygons are likewise rejected loudly (see [`Geometry::crosses_antimeridian`]):
//! splitting them is deferred to P2-d.
//!
//! ## WKB byte layout (OGC SFA 1.2.1)
//!
//! ```text
//! byte 0         : byte order   — 0 = big-endian (XDR), 1 = little-endian (NDR)
//! bytes 1..5     : u32 geometry type — 1 = Point, 3 = Polygon
//! Point  payload : f64 X (lon), f64 Y (lat)
//! Polygon payload: u32 numRings, then per ring:
//!                    u32 numPoints, then numPoints × (f64 X, f64 Y)
//! ```
//!
//! Coordinates are stored OGC-style as `(X=lon, Y=lat)`; the public API uses
//! `(lat, lon)` to match the rest of the geo stack, so [`marshal_wkb`] /
//! [`parse_wkb`] transpose at the boundary. Serialization always emits
//! little-endian (NDR); parsing accepts either byte order.

use crate::error::{Error, Result};

/// OGC WKB geometry-type code for a 2D Point.
const WKB_POINT: u32 = 1;
/// OGC WKB geometry-type code for a 2D Polygon.
const WKB_POLYGON: u32 = 3;

/// Byte-order flag: little-endian (NDR). This is what we emit.
const BYTE_ORDER_LE: u8 = 1;
/// Byte-order flag: big-endian (XDR). We parse it but never write it.
const BYTE_ORDER_BE: u8 = 0;

/// A stored geometry value over WGS84 `(lat, lon)` degrees.
///
/// Only the two kinds the Phase-2 foundation needs are modelled. Richer
/// geometry is rejected at parse time rather than represented here, so an
/// in-memory `Geometry` is always one this engine can reason about exactly.
#[derive(Debug, Clone, PartialEq)]
pub enum Geometry {
    /// A single `(lat, lon)` coordinate in degrees.
    Point { lat: f64, lon: f64 },
    /// A single-outer-ring polygon. The ring is held with its closing vertex
    /// stripped (the last vertex implicitly connects to the first), so an open
    /// and an explicitly-closed input ring marshal to the same `Geometry`.
    Polygon { ring: Vec<(f64, f64)> },
}

impl Geometry {
    /// Build a polygon from an outer ring of `(lat, lon)` vertices, dropping a
    /// trailing vertex equal to the first (the ring is implicitly closed).
    pub fn polygon(ring: Vec<(f64, f64)>) -> Self {
        let mut ring = ring;
        if ring.len() >= 2 && ring.first() == ring.last() {
            ring.pop();
        }
        Geometry::Polygon { ring }
    }

    /// True if any polygon ring edge spans more than 180° of longitude, which
    /// indicates the polygon crosses the ±180° antimeridian. WKB for such a
    /// polygon is rejected loudly by [`parse_wkb`] rather than mis-stored.
    /// Points never cross the antimeridian.
    pub fn crosses_antimeridian(&self) -> bool {
        let ring = match self {
            Geometry::Point { .. } => return false,
            Geometry::Polygon { ring } => ring,
        };
        let n = ring.len();
        if n < 2 {
            return false;
        }
        (0..n).any(|i| {
            let (_, lon_a) = ring[i];
            let (_, lon_b) = ring[(i + 1) % n];
            (lon_a - lon_b).abs() > 180.0
        })
    }
}

/// Serialize a [`Geometry`] to little-endian (NDR) WKB bytes.
///
/// Polygon rings are emitted explicitly closed (the first vertex is repeated as
/// the last), per the OGC convention, even though the in-memory ring stores it
/// stripped. A point is 21 bytes; a polygon is `13 + 16·(n+1)` bytes for an
/// `n`-vertex stripped ring.
pub fn marshal_wkb(geom: &Geometry) -> Vec<u8> {
    let mut out = Vec::new();
    out.push(BYTE_ORDER_LE);
    match geom {
        Geometry::Point { lat, lon } => {
            out.extend_from_slice(&WKB_POINT.to_le_bytes());
            write_coord(&mut out, *lat, *lon);
        }
        Geometry::Polygon { ring } => {
            out.extend_from_slice(&WKB_POLYGON.to_le_bytes());
            // One ring.
            out.extend_from_slice(&1u32.to_le_bytes());
            // Emit the ring explicitly closed: n stored vertices + the repeated
            // first vertex. An empty ring stays empty (numPoints = 0).
            let closed_len = if ring.is_empty() { 0 } else { ring.len() + 1 };
            out.extend_from_slice(&(closed_len as u32).to_le_bytes());
            for &(lat, lon) in ring {
                write_coord(&mut out, lat, lon);
            }
            if let Some(&(lat, lon)) = ring.first() {
                write_coord(&mut out, lat, lon);
            }
        }
    }
    out
}

/// Append one `(lat, lon)` coordinate as OGC `(X=lon, Y=lat)` little-endian f64s.
fn write_coord(out: &mut Vec<u8>, lat: f64, lon: f64) {
    out.extend_from_slice(&lon.to_le_bytes());
    out.extend_from_slice(&lat.to_le_bytes());
}

/// A cursor that reads WKB primitives in a fixed byte order, surfacing a
/// truncated buffer as [`Error::InvalidData`] rather than panicking.
struct WkbReader<'a> {
    buf: &'a [u8],
    pos: usize,
    little_endian: bool,
}

impl WkbReader<'_> {
    fn read_u32(&mut self) -> Result<u32> {
        let bytes: [u8; 4] = self
            .buf
            .get(self.pos..self.pos + 4)
            .ok_or_else(|| Error::InvalidData("WKB: truncated u32".to_string()))?
            .try_into()
            .map_err(|_| Error::InvalidData("WKB: truncated u32".to_string()))?;
        self.pos += 4;
        Ok(if self.little_endian {
            u32::from_le_bytes(bytes)
        } else {
            u32::from_be_bytes(bytes)
        })
    }

    fn read_f64(&mut self) -> Result<f64> {
        let bytes: [u8; 8] = self
            .buf
            .get(self.pos..self.pos + 8)
            .ok_or_else(|| Error::InvalidData("WKB: truncated f64".to_string()))?
            .try_into()
            .map_err(|_| Error::InvalidData("WKB: truncated f64".to_string()))?;
        self.pos += 8;
        Ok(if self.little_endian {
            f64::from_le_bytes(bytes)
        } else {
            f64::from_be_bytes(bytes)
        })
    }

    /// Read one OGC `(X=lon, Y=lat)` coordinate and return it as `(lat, lon)`.
    fn read_coord(&mut self) -> Result<(f64, f64)> {
        let lon = self.read_f64()?;
        let lat = self.read_f64()?;
        Ok((lat, lon))
    }
}

/// Parse WKB bytes into a [`Geometry`], accepting either byte order.
///
/// Rejects loudly with [`Error::InvalidData`] (never a silent wrong answer):
/// an unknown byte-order flag, an unsupported geometry type (anything but Point
/// or single-ring Polygon — `MultiPolygon`, `LineString`, Z/M variants, etc.),
/// a polygon with zero or more than one ring (holes), a degenerate ring, an
/// antimeridian-crossing polygon, truncated bytes, or trailing garbage.
pub fn parse_wkb(buf: &[u8]) -> Result<Geometry> {
    let order = *buf
        .first()
        .ok_or_else(|| Error::InvalidData("WKB: empty buffer".to_string()))?;
    let little_endian = match order {
        BYTE_ORDER_LE => true,
        BYTE_ORDER_BE => false,
        other => {
            return Err(Error::InvalidData(format!(
                "WKB: unknown byte-order flag {other:#x} (expected 0 or 1)"
            )))
        }
    };
    let mut r = WkbReader {
        buf,
        pos: 1,
        little_endian,
    };
    let geom_type = r.read_u32()?;
    let geom = match geom_type {
        WKB_POINT => {
            let (lat, lon) = r.read_coord()?;
            Geometry::Point { lat, lon }
        }
        WKB_POLYGON => parse_polygon(&mut r)?,
        other => {
            return Err(Error::InvalidData(format!(
                "WKB: unsupported geometry type {other} \
                 (only Point=1 and single-ring Polygon=3 are supported)"
            )))
        }
    };
    if r.pos != buf.len() {
        return Err(Error::InvalidData(format!(
            "WKB: {} trailing byte(s) after geometry",
            buf.len() - r.pos
        )));
    }
    if geom.crosses_antimeridian() {
        return Err(Error::InvalidData(
            "WKB: antimeridian-crossing polygon is not supported (deferred to P2-d)".to_string(),
        ));
    }
    Ok(geom)
}

/// Parse a Polygon body (`numRings`, then the single ring) from `r`.
fn parse_polygon(r: &mut WkbReader<'_>) -> Result<Geometry> {
    let num_rings = r.read_u32()?;
    if num_rings != 1 {
        return Err(Error::InvalidData(format!(
            "WKB: polygon has {num_rings} ring(s); only a single outer ring is \
             supported (holes/multi-ring deferred to P2-d)"
        )));
    }
    let num_points = r.read_u32()?;
    let mut ring = Vec::with_capacity(num_points as usize);
    for _ in 0..num_points {
        ring.push(r.read_coord()?);
    }
    // Strip an explicit closing vertex (OGC rings are closed) for a uniform
    // in-memory representation.
    if ring.len() >= 2 && ring.first() == ring.last() {
        ring.pop();
    }
    if ring.len() < 3 {
        return Err(Error::InvalidData(format!(
            "WKB: polygon ring has {} distinct vertices; a ring needs at least 3",
            ring.len()
        )));
    }
    Ok(Geometry::Polygon { ring })
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

    #[test]
    fn point_round_trips() {
        let p = Geometry::Point {
            lat: 37.7955,
            lon: -122.3937,
        };
        let bytes = marshal_wkb(&p);
        assert_eq!(parse_wkb(&bytes).unwrap(), p);
    }

    #[test]
    fn point_wkb_is_21_bytes_le() {
        let p = Geometry::Point { lat: 1.0, lon: 2.0 };
        let bytes = marshal_wkb(&p);
        assert_eq!(bytes.len(), 21);
        assert_eq!(bytes[0], BYTE_ORDER_LE);
        assert_eq!(
            u32::from_le_bytes(bytes[1..5].try_into().unwrap()),
            WKB_POINT
        );
        // OGC stores X=lon first, then Y=lat.
        assert_eq!(f64::from_le_bytes(bytes[5..13].try_into().unwrap()), 2.0);
        assert_eq!(f64::from_le_bytes(bytes[13..21].try_into().unwrap()), 1.0);
    }

    #[test]
    fn polygon_round_trips() {
        let poly = sf_square();
        let bytes = marshal_wkb(&poly);
        assert_eq!(parse_wkb(&bytes).unwrap(), poly);
    }

    #[test]
    fn polygon_marshals_explicitly_closed() {
        let poly = sf_square();
        let bytes = marshal_wkb(&poly);
        // header(1) + type(4) + numRings(4) + numPoints(4)
        let num_points = u32::from_le_bytes(bytes[9..13].try_into().unwrap());
        // 4 stored vertices emitted closed → 5 points.
        assert_eq!(num_points, 5);
    }

    #[test]
    fn polygon_open_and_closed_input_are_equal() {
        let open = Geometry::polygon(vec![(0.0, 0.0), (0.0, 1.0), (1.0, 1.0)]);
        let closed = Geometry::polygon(vec![(0.0, 0.0), (0.0, 1.0), (1.0, 1.0), (0.0, 0.0)]);
        assert_eq!(open, closed);
        assert_eq!(parse_wkb(&marshal_wkb(&open)).unwrap(), closed);
    }

    #[test]
    fn parses_big_endian_point() {
        // Hand-built big-endian (XDR) point at (lat=1.0, lon=2.0).
        let mut bytes = vec![BYTE_ORDER_BE];
        bytes.extend_from_slice(&WKB_POINT.to_be_bytes());
        bytes.extend_from_slice(&2.0f64.to_be_bytes()); // X = lon
        bytes.extend_from_slice(&1.0f64.to_be_bytes()); // Y = lat
        let geom = parse_wkb(&bytes).unwrap();
        assert_eq!(geom, Geometry::Point { lat: 1.0, lon: 2.0 });
    }

    #[test]
    fn empty_buffer_is_rejected() {
        let err = parse_wkb(&[]).unwrap_err();
        assert!(matches!(err, Error::InvalidData(_)));
    }

    #[test]
    fn unknown_byte_order_is_rejected() {
        let err = parse_wkb(&[0x42, 0, 0, 0, 0]).unwrap_err();
        assert!(err.to_string().contains("byte-order"));
    }

    #[test]
    fn unsupported_geometry_type_is_rejected_loudly() {
        // Type 6 = MultiPolygon — not supported.
        let mut bytes = vec![BYTE_ORDER_LE];
        bytes.extend_from_slice(&6u32.to_le_bytes());
        let err = parse_wkb(&bytes).unwrap_err();
        assert!(err.to_string().contains("unsupported geometry type 6"));
    }

    #[test]
    fn linestring_type_is_rejected_loudly() {
        // Type 2 = LineString — not supported (deferred to P2-d).
        let mut bytes = vec![BYTE_ORDER_LE];
        bytes.extend_from_slice(&2u32.to_le_bytes());
        let err = parse_wkb(&bytes).unwrap_err();
        assert!(err.to_string().contains("unsupported geometry type 2"));
    }

    #[test]
    fn multi_ring_polygon_is_rejected_loudly() {
        // A polygon WKB claiming 2 rings (a hole) must be rejected, not have its
        // hole silently dropped.
        let mut bytes = vec![BYTE_ORDER_LE];
        bytes.extend_from_slice(&WKB_POLYGON.to_le_bytes());
        bytes.extend_from_slice(&2u32.to_le_bytes()); // numRings = 2
        let err = parse_wkb(&bytes).unwrap_err();
        assert!(err.to_string().contains("2 ring"));
    }

    #[test]
    fn degenerate_ring_is_rejected() {
        // A polygon ring with only two distinct vertices cannot enclose area.
        let mut bytes = vec![BYTE_ORDER_LE];
        bytes.extend_from_slice(&WKB_POLYGON.to_le_bytes());
        bytes.extend_from_slice(&1u32.to_le_bytes()); // numRings
        bytes.extend_from_slice(&2u32.to_le_bytes()); // numPoints
        for (lat, lon) in [(0.0f64, 0.0f64), (1.0, 1.0)] {
            bytes.extend_from_slice(&lon.to_le_bytes());
            bytes.extend_from_slice(&lat.to_le_bytes());
        }
        let err = parse_wkb(&bytes).unwrap_err();
        assert!(err.to_string().contains("at least 3"));
    }

    #[test]
    fn truncated_point_is_rejected() {
        let full = marshal_wkb(&Geometry::Point { lat: 1.0, lon: 2.0 });
        let err = parse_wkb(&full[..full.len() - 3]).unwrap_err();
        assert!(err.to_string().contains("truncated"));
    }

    #[test]
    fn trailing_bytes_are_rejected() {
        let mut bytes = marshal_wkb(&Geometry::Point { lat: 1.0, lon: 2.0 });
        bytes.push(0xFF);
        let err = parse_wkb(&bytes).unwrap_err();
        assert!(err.to_string().contains("trailing"));
    }

    #[test]
    fn antimeridian_polygon_is_rejected_loudly() {
        let poly = Geometry::polygon(vec![
            (-1.0, 179.0),
            (1.0, 179.0),
            (1.0, -179.0),
            (-1.0, -179.0),
        ]);
        assert!(poly.crosses_antimeridian());
        let bytes = marshal_wkb(&poly);
        let err = parse_wkb(&bytes).unwrap_err();
        assert!(err.to_string().contains("antimeridian"));
    }

    #[test]
    fn normal_polygon_does_not_cross_antimeridian() {
        assert!(!sf_square().crosses_antimeridian());
    }

    #[test]
    fn point_never_crosses_antimeridian() {
        assert!(!Geometry::Point {
            lat: 0.0,
            lon: 179.9
        }
        .crosses_antimeridian());
    }
}
