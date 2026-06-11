-- Geospatial point index (Phase 1) — example schema, data, and queries.
--
-- Phase 1 indexes POINT geometry only. A `geo_point` is modelled as a
-- `frozen<tuple<double, double>>` of (latitude, longitude) — no new column
-- type is required. A `USING 'geo'` secondary index encodes each point to a
-- sortable space-filling-curve cell id (a geohash-style Z-order interleave of
-- quantised lat/lon) and stores it in the ordinary BTree sidecar. Nearby
-- points share key prefixes, so bbox / radius / k-NN queries become a small set
-- of contiguous cell-id ranges, refined afterwards with exact haversine
-- distance. The encoder handles the antimeridian (±180° longitude) and the
-- poles correctly.
--
-- Distances are metres on a WGS84 sphere (the default `crs = 'wgs84_spherical'`).

CREATE KEYSPACE IF NOT EXISTS geo
  WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

USE geo;

-- A table of places. `location` is the (lat, lon) point we will index.
CREATE TABLE IF NOT EXISTS places (
    id        int PRIMARY KEY,
    name      text,
    location  frozen<tuple<double, double>>
);

-- The geo index. `level` controls cell granularity (larger = finer cells,
-- more selective ranges, more refine work); the default suits city-scale
-- queries. `crs` selects the distance model.
CREATE INDEX places_location_geo ON places (location)
  USING 'geo'
  WITH OPTIONS = {'level': '16', 'crs': 'wgs84_spherical'};

-- Sample points: a cluster in San Francisco, one in New York, plus two points
-- straddling the antimeridian and one near the North Pole to exercise the
-- edge cases.
INSERT INTO places (id, name, location) VALUES (1, 'SF Ferry Building',   (37.7955, -122.3937));
INSERT INTO places (id, name, location) VALUES (2, 'SF Union Square',     (37.7880, -122.4074));
INSERT INTO places (id, name, location) VALUES (3, 'SF Golden Gate Park', (37.7694, -122.4862));
INSERT INTO places (id, name, location) VALUES (4, 'NYC Times Square',    (40.7580,  -73.9855));
INSERT INTO places (id, name, location) VALUES (5, 'Dateline East',       ( 0.0000,  179.9900));
INSERT INTO places (id, name, location) VALUES (6, 'Dateline West',       ( 0.0000, -179.9900));
INSERT INTO places (id, name, location) VALUES (7, 'Near North Pole',     (89.9000,    0.0000));

-- ── Phase 1 geo query surface ─────────────────────────────────────────────
-- These three predicates mirror the vector `ANN OF` path: the planner detects
-- the geo function, asks the geo index for candidate cell ranges, then refines
-- with exact distance / containment before returning rows. EXPLAIN reports the
-- geo index scan, and the `index_usage` counter increments on a hit.

-- 1) k nearest neighbours of a point (expanding-ring search + refine).
--    From Union Square, the three SF places rank ahead of NYC.
SELECT id, name FROM places
  ORDER BY location GEO_NEAREST OF (37.7880, -122.4074) LIMIT 3;

-- 2) Everything within a radius (metres) of a point.
--    A 3 km radius around the Ferry Building keeps the downtown SF places and
--    drops Golden Gate Park and NYC.
SELECT id, name FROM places
  WHERE GEO_WITHIN_RADIUS(location, (37.7955, -122.3937), 3000);

-- 3) A bounding box (south-west corner, north-east corner).
--    This box covers central San Francisco only.
SELECT id, name FROM places
  WHERE GEO_WITHIN_BBOX(location, (37.70, -122.52), (37.83, -122.35));

-- 4) Antimeridian bbox: SW lon 179, NE lon -179 crosses ±180°, so it must
--    return BOTH dateline points (ids 5 and 6), not zero rows.
SELECT id, name FROM places
  WHERE GEO_WITHIN_BBOX(location, (-1.0, 179.0), (1.0, -179.0));

-- ── Phase 2 (foundation): arbitrary query polygon ─────────────────────────
-- 5) ST_WITHIN finds indexed POINTS inside an arbitrary query POLYGON. The
--    planner covers the polygon's bounding box with cell ranges (same as the
--    bbox path), then refines each candidate with an exact point-in-polygon
--    ray-casting test. This quadrilateral hugs central San Francisco: it
--    contains the three SF points (ids 1, 2, 3) and excludes NYC and both
--    dateline points. The ring is given as ((lat, lon), ...) and is closed
--    implicitly. Polygons crossing the ±180° antimeridian are rejected (planar
--    ray casting is not correct across the dateline).
SELECT id, name FROM places
  WHERE ST_WITHIN(location, ((37.70, -122.52), (37.83, -122.52),
                             (37.83, -122.35), (37.70, -122.35)));
