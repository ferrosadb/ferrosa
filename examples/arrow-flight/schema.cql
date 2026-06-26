-- Arrow Flight example — schema + seed data.
--
-- Run this against a running Ferrosa (CQL on localhost:9042) before exercising
-- the Flight endpoint (gRPC on localhost:8815). The Flight DoGet/DoPut RPCs read
-- and write this same table over Arrow; the CQL here is the setup the
-- "Example CQL Scripts" CI job executes.

CREATE KEYSPACE IF NOT EXISTS flight_demo
  WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

CREATE TABLE IF NOT EXISTS flight_demo.sensor_readings (
    sensor_id  text,
    ts         timestamp,
    temperature double,
    humidity   double,
    PRIMARY KEY (sensor_id, ts)
) WITH CLUSTERING ORDER BY (ts DESC);

INSERT INTO flight_demo.sensor_readings (sensor_id, ts, temperature, humidity)
    VALUES ('sensor-1', '2026-06-19T10:00:00Z', 21.5, 44.0);
INSERT INTO flight_demo.sensor_readings (sensor_id, ts, temperature, humidity)
    VALUES ('sensor-1', '2026-06-19T10:01:00Z', 21.7, 43.8);
INSERT INTO flight_demo.sensor_readings (sensor_id, ts, temperature, humidity)
    VALUES ('sensor-2', '2026-06-19T10:00:00Z', 19.2, 51.3);

-- Read it back over CQL (the same rows DoGet returns as an Arrow stream):
SELECT sensor_id, ts, temperature, humidity FROM flight_demo.sensor_readings;
