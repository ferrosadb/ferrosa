-- RRD Cascading Time-Series Aggregation: Built-in Functions
--
-- Demonstrate each built-in consolidation function individually.

USE sensors;

-- MIN: lowest value in each 5-minute window.
CREATE TABLE demo_min (
    id uuid, ts timestamp, value double,
    PRIMARY KEY (id, ts)
) WITH extensions = {
    'consolidation.interval': '5m',
    'consolidation.functions': 'min',
    'consolidation.target': 'demo_min_5m',
    'consolidation.columns': 'value'
  };

-- MAX: highest value in each 5-minute window.
CREATE TABLE demo_max (
    id uuid, ts timestamp, value double,
    PRIMARY KEY (id, ts)
) WITH extensions = {
    'consolidation.interval': '5m',
    'consolidation.functions': 'max',
    'consolidation.target': 'demo_max_5m',
    'consolidation.columns': 'value'
  };

-- AVG: arithmetic mean of all values in the window.
CREATE TABLE demo_avg (
    id uuid, ts timestamp, value double,
    PRIMARY KEY (id, ts)
) WITH extensions = {
    'consolidation.interval': '5m',
    'consolidation.functions': 'avg',
    'consolidation.target': 'demo_avg_5m',
    'consolidation.columns': 'value'
  };

-- STDDEV: population standard deviation (Welford's online algorithm).
CREATE TABLE demo_stddev (
    id uuid, ts timestamp, value double,
    PRIMARY KEY (id, ts)
) WITH extensions = {
    'consolidation.interval': '5m',
    'consolidation.functions': 'stddev',
    'consolidation.target': 'demo_stddev_5m',
    'consolidation.columns': 'value'
  };

-- COUNT: number of data points in the window.
CREATE TABLE demo_count (
    id uuid, ts timestamp, value double,
    PRIMARY KEY (id, ts)
) WITH extensions = {
    'consolidation.interval': '5m',
    'consolidation.functions': 'count',
    'consolidation.target': 'demo_count_5m',
    'consolidation.columns': 'value'
  };

-- SUM: total of all values in the window.
CREATE TABLE demo_sum (
    id uuid, ts timestamp, value double,
    PRIMARY KEY (id, ts)
) WITH extensions = {
    'consolidation.interval': '5m',
    'consolidation.functions': 'sum',
    'consolidation.target': 'demo_sum_5m',
    'consolidation.columns': 'value'
  };

-- MEDIAN: middle value (requires sort, O(n log n)).
-- Current live RRD materialization rejects median at DDL time because the
-- worker is single-pass streaming only. See median.cql for the future design.
CREATE TABLE demo_median (
    id uuid, ts timestamp, value double,
    PRIMARY KEY (id, ts)
) WITH extensions = {
    'consolidation.interval': '5m',
    'consolidation.functions': 'median',
    'consolidation.target': 'demo_median_5m',
    'consolidation.columns': 'value'
  };

-- All streaming-safe functions combined in a single table. Do not include
-- median here until the bounded ordered/sketch design lands.
-- Produces columns: value_min, value_max, value_avg, value_stddev,
--                   value_count, value_sum.
CREATE TABLE demo_all (
    id uuid, ts timestamp, value double,
    PRIMARY KEY (id, ts)
) WITH extensions = {
    'consolidation.interval': '5m',
    'consolidation.functions': 'min,max,avg,stddev,count,sum',
    'consolidation.target': 'demo_all_5m',
    'consolidation.columns': 'value'
  };

-- Sample data: 15 readings, 1 per second.
INSERT INTO demo_all (id, ts, value)
  VALUES (660e8400-e29b-41d4-a716-446655440001, '2026-03-20 00:00:01', 10.0);
INSERT INTO demo_all (id, ts, value)
  VALUES (660e8400-e29b-41d4-a716-446655440001, '2026-03-20 00:00:02', 20.0);
INSERT INTO demo_all (id, ts, value)
  VALUES (660e8400-e29b-41d4-a716-446655440001, '2026-03-20 00:00:03', 30.0);
INSERT INTO demo_all (id, ts, value)
  VALUES (660e8400-e29b-41d4-a716-446655440001, '2026-03-20 00:00:04', 40.0);
INSERT INTO demo_all (id, ts, value)
  VALUES (660e8400-e29b-41d4-a716-446655440001, '2026-03-20 00:00:05', 50.0);

-- Expected output in demo_all_5m after window consolidation:
--   value_min    = 10.0
--   value_max    = 50.0
--   value_avg    = 30.0
--   value_stddev = 14.142... (population stddev)
--   value_count  = 5.0
--   value_sum    = 150.0
