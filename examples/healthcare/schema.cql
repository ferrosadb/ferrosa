-- Healthcare Records Schema
-- Run: cqlsh -f schema.cql

CREATE KEYSPACE IF NOT EXISTS healthcare
  WITH replication = {
    'class': 'SimpleStrategy',
    'replication_factor': 3
  };

USE healthcare;

CREATE TABLE IF NOT EXISTS patients (
    patient_id      uuid PRIMARY KEY,
    name            text,
    dob             date,
    blood_type      text,
    allergies       set<text>,
    emergency_contact text
);

CREATE TABLE IF NOT EXISTS clinical_events (
    patient_id  uuid,
    event_time  timestamp,
    event_id    uuid,
    event_type  text,
    department  text,
    provider    text,
    notes       text,
    PRIMARY KEY (patient_id, event_time, event_id)
) WITH CLUSTERING ORDER BY (event_time DESC, event_id ASC);

CREATE TABLE IF NOT EXISTS lab_results (
    patient_id      uuid,
    collected_at    timestamp,
    test_name       text,
    result_value    text,
    unit            text,
    reference_range text,
    abnormal        boolean,
    PRIMARY KEY (patient_id, collected_at)
) WITH CLUSTERING ORDER BY (collected_at DESC);

CREATE TABLE IF NOT EXISTS medications (
    patient_id    uuid,
    prescribed_at timestamp,
    medication    text,
    dosage        text,
    frequency     text,
    prescriber    text,
    active        boolean,
    PRIMARY KEY (patient_id, medication)
);
