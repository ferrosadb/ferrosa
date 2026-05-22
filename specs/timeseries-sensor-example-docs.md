# Example Documentation Blueprint — Time-Series Sensor Rollups

> Last updated: 2026-05-20
> Status: Draft example docs plan

## Purpose

The example should make the value obvious for sensor data:

- raw readings are too granular for most dashboards;
- min/max show operating envelope and excursions;
- avg shows baseline trend;
- standard deviation shows volatility and anomaly risk;
- WASM UDFs let teams add domain-specific math without shipping raw data to an
  external processor.

## Target Example Story

A factory monitors vibration on rotating equipment. Sensors write one row per
second:

```sql
CREATE TABLE plant.sensor_readings_1s (
    sensor_id uuid,
    ts timestamp,
    vibration_mm_s double,
    temperature_c double,
    PRIMARY KEY (sensor_id, ts)
) WITH CLUSTERING ORDER BY (ts DESC)
  AND extensions = {
    'consolidation.interval': '5m',
    'consolidation.functions': 'min,max,avg,stddev',
    'consolidation.target': 'sensor_readings_5m',
    'consolidation.cascade': 'true',
    'consolidation.late_window': '15m',
    'consolidation.columns': 'vibration_mm_s,temperature_c'
  };
```

The rollup table exposes one row per sensor per 5-minute window:

```text
sensor_id
window_start
vibration_mm_s_min
vibration_mm_s_max
vibration_mm_s_avg
vibration_mm_s_stddev
temperature_c_min
temperature_c_max
temperature_c_avg
temperature_c_stddev
```

## WASM Stddev UDF Section

The docs should show all supported loading forms, with the file/URL forms marked
admin-only:

```sql
CREATE OR REPLACE FUNCTION plant.stddev(values list<double>)
CALLED ON NULL INPUT
RETURNS double
LANGUAGE wasm
AS '0x<hex-encoded-component-bytes>';

CREATE OR REPLACE FUNCTION plant.stddev(values list<double>)
CALLED ON NULL INPUT
RETURNS double
LANGUAGE wasm
AS FILE '/secure/udf/stddev.wasm';

CREATE OR REPLACE FUNCTION plant.stddev(values list<double>)
CALLED ON NULL INPUT
RETURNS double
LANGUAGE wasm
AS URL 'https://artifacts.example/ferrosa/stddev.wasm'
WITH SHA256 = '<expected-digest>';
```

## Queries to Demonstrate Power

```sql
-- See volatility and excursions for a sensor over the last hour.
SELECT window_start,
       vibration_mm_s_min,
       vibration_mm_s_max,
       vibration_mm_s_avg,
       vibration_mm_s_stddev
FROM plant.sensor_readings_5m
WHERE sensor_id = 2f4d6a9e-6a9a-4f75-a6c4-cd8d210e7e34
  AND window_start >= '2026-05-20T12:00:00Z';

-- Subscribe to new rollups as each window materializes.
SUBSCRIBE SELECT sensor_id, window_start, vibration_mm_s_avg, vibration_mm_s_stddev
FROM plant.sensor_readings_5m
EVERY 5s DELTA;

-- Subscribe to delayed materialization alerts.
SUBSCRIBE SELECT keyspace_name, table_name, queue_depth, oldest_task_age_ms, alerting
FROM system_observability.materialization_queues
WHERE alerting = true
EVERY 5s DELTA;

-- Inspect runtime RRD ring memory controls.
SELECT setting_name, setting_value, source
FROM system_observability.rrd_runtime_settings;

-- Admin-only runtime tuning; the startup default can be overridden with
-- FERROSA_RRD_RING_MEMORY_BUDGET_BYTES.
UPDATE system_observability.rrd_runtime_settings
SET setting_value = 134217728
WHERE setting_name = 'ring_memory_budget_bytes';
```

## Files to Update

- `examples/timeseries-rrd/schema.cql`
- `examples/timeseries-rrd/data.cql`
- `examples/timeseries-rrd/queries.cql`
- `examples/timeseries-rrd/custom-wasm-udf.cql`
- `examples/timeseries-rrd/timeseries-rrd.adoc`
- `examples/test-examples.sh` or CI example harness if extra files become
  executable examples.

## Acceptance Criteria

- The canonical example scripts create real rollup rows.
- The docs explain why min/max/avg/stddev matter for sensor operations.
- The canonical example scripts use the built-in streaming stddev path.
- WASM stddev loading forms remain documented separately and clearly state that
  live aggregate execution is blocked on the streaming aggregate ABI.
- Queue observability is shown as an operational practice, not an internal
  debugging footnote.
