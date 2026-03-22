-- RRD Cascading Time-Series Aggregation: Schema
--
-- Creates a 4-tier cascade: 1s -> 5m -> 15m -> 1h.
-- Consolidation is driven by data timestamps, not wall clock.

CREATE KEYSPACE IF NOT EXISTS sensors
  WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

USE sensors;

-- Tier 0: Raw 1-second sensor readings.
CREATE TABLE sensor_1s (
    sensor_id uuid,
    ts timestamp,
    value double,
    PRIMARY KEY (sensor_id, ts)
) WITH CLUSTERING ORDER BY (ts DESC)
  AND extensions = {
    'consolidation.interval': '5m',
    'consolidation.functions': 'min,max,avg,stddev',
    'consolidation.target': 'sensor_5m',
    'consolidation.cascade': 'true',
    'consolidation.late_window': '10m',
    'consolidation.columns': 'value'
  };

-- Tiers 1-3 are auto-created by cascade. Shown here for reference:
--
-- sensor_5m:  5-minute aggregates  (consolidation.interval: '15m', target: 'sensor_15m')
-- sensor_15m: 15-minute aggregates (consolidation.interval: '1h', target: 'sensor_1h')
-- sensor_1h:  1-hour aggregates    (terminal, no consolidation config)
