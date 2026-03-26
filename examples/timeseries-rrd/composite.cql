-- RRD Cascading Time-Series Aggregation: Composite Multi-Column
--
-- Composite function: compute multiple aggregates across multiple source
-- columns in a single pass. Produces one output column per
-- (source_column, function) pair.

USE sensors;

CREATE TABLE multi_sensor (
    device_id uuid,
    ts timestamp,
    temperature double,
    humidity double,
    pressure double,
    PRIMARY KEY (device_id, ts)
) WITH CLUSTERING ORDER BY (ts DESC)
  AND extensions = {
    'consolidation.interval': '5m',
    'consolidation.functions': 'min,max,avg',
    'consolidation.target': 'multi_sensor_5m',
    'consolidation.columns': 'temperature,humidity,pressure'
  };

-- Output table multi_sensor_5m will have columns:
--   device_id              uuid           (partition key, copied)
--   ts                     timestamp      (clustering, window start)
--   temperature_min        double
--   temperature_max        double
--   temperature_avg        double
--   humidity_min           double
--   humidity_max           double
--   humidity_avg           double
--   pressure_min           double
--   pressure_max           double
--   pressure_avg           double

-- Sample data: weather station readings.
INSERT INTO multi_sensor (device_id, ts, temperature, humidity, pressure)
  VALUES (770e8400-e29b-41d4-a716-446655440001,
          '2026-03-20 00:00:01', 22.1, 65.0, 1013.2);
INSERT INTO multi_sensor (device_id, ts, temperature, humidity, pressure)
  VALUES (770e8400-e29b-41d4-a716-446655440001,
          '2026-03-20 00:00:30', 22.3, 64.8, 1013.1);
INSERT INTO multi_sensor (device_id, ts, temperature, humidity, pressure)
  VALUES (770e8400-e29b-41d4-a716-446655440001,
          '2026-03-20 00:01:00', 22.0, 65.2, 1013.3);
INSERT INTO multi_sensor (device_id, ts, temperature, humidity, pressure)
  VALUES (770e8400-e29b-41d4-a716-446655440001,
          '2026-03-20 00:01:30', 21.8, 65.5, 1013.0);
INSERT INTO multi_sensor (device_id, ts, temperature, humidity, pressure)
  VALUES (770e8400-e29b-41d4-a716-446655440001,
          '2026-03-20 00:02:00', 22.5, 64.5, 1013.4);

-- Query the 5-minute composite aggregates.
SELECT device_id, ts,
       temperature_min, temperature_max, temperature_avg,
       humidity_min, humidity_max, humidity_avg,
       pressure_min, pressure_max, pressure_avg
  FROM multi_sensor_5m
  WHERE device_id = 770e8400-e29b-41d4-a716-446655440001
  LIMIT 10;
