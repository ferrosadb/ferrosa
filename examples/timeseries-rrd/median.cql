-- RRD Cascading Time-Series Aggregation: Median Example
--
-- Median requires sorting values within a window (O(n log n)).
-- Current live RRD materialization rejects median at DDL time because the
-- worker is single-pass streaming only and does not materialize full windows.
-- Treat this file as the future behavior contract for a bounded ordered/sketch
-- implementation, not as a runnable script for this release.

USE sensors;

CREATE TABLE latency_raw (
    service_id uuid,
    ts timestamp,
    latency_ms double,
    PRIMARY KEY (service_id, ts)
) WITH CLUSTERING ORDER BY (ts DESC)
  AND extensions = {
    'consolidation.interval': '1m',
    'consolidation.functions': 'median,avg,stddev',
    'consolidation.target': 'latency_1m',
    'consolidation.columns': 'latency_ms'
  };

-- Output table latency_1m will have columns:
--   latency_ms_median  double   (50th percentile)
--   latency_ms_avg     double   (arithmetic mean)
--   latency_ms_stddev  double   (population standard deviation)
--
-- Median is useful for latency monitoring because it is robust to
-- outliers (unlike avg), showing the typical user experience.

-- Sample data: API response latencies (mostly fast, one outlier).
INSERT INTO latency_raw (service_id, ts, latency_ms)
  VALUES (880e8400-e29b-41d4-a716-446655440001, '2026-03-20 00:00:01', 12.5);
INSERT INTO latency_raw (service_id, ts, latency_ms)
  VALUES (880e8400-e29b-41d4-a716-446655440001, '2026-03-20 00:00:02', 14.2);
INSERT INTO latency_raw (service_id, ts, latency_ms)
  VALUES (880e8400-e29b-41d4-a716-446655440001, '2026-03-20 00:00:03', 11.8);
INSERT INTO latency_raw (service_id, ts, latency_ms)
  VALUES (880e8400-e29b-41d4-a716-446655440001, '2026-03-20 00:00:04', 13.1);
INSERT INTO latency_raw (service_id, ts, latency_ms)
  VALUES (880e8400-e29b-41d4-a716-446655440001, '2026-03-20 00:00:05', 450.0);

-- Expected output in latency_1m:
--   latency_ms_median = 13.1     (middle value: 11.8, 12.5, *13.1*, 14.2, 450.0)
--   latency_ms_avg    = 100.32   (skewed by the 450ms outlier)
--   latency_ms_stddev = 174.0... (high due to outlier)
--
-- The median (13.1) better represents the typical latency than the
-- avg (100.32), which is inflated by the single slow request.

-- Query the 1-minute aggregates.
SELECT service_id, ts, latency_ms_median, latency_ms_avg, latency_ms_stddev
  FROM latency_1m
  WHERE service_id = 880e8400-e29b-41d4-a716-446655440001
  LIMIT 10;
