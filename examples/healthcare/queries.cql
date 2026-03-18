-- Healthcare Records Queries
-- Run: cqlsh -f queries.cql (after schema.cql and data.cql)

USE healthcare;

-- Full clinical timeline for Maria Chen
SELECT event_time, event_type, department, provider, notes
FROM clinical_events
WHERE patient_id = 11111111-1111-1111-1111-111111111111;

-- Last 5 lab results for Robert Okafor
SELECT collected_at, test_name, result_value, unit, reference_range, abnormal
FROM lab_results
WHERE patient_id = 22222222-2222-2222-2222-222222222222
LIMIT 5;

-- Current medications for Maria Chen
SELECT medication, dosage, frequency, prescriber
FROM medications
WHERE patient_id = 11111111-1111-1111-1111-111111111111
  AND active = true
ALLOW FILTERING;

-- All abnormal results for Maria Chen
SELECT collected_at, test_name, result_value, unit, reference_range
FROM lab_results
WHERE patient_id = 11111111-1111-1111-1111-111111111111
  AND abnormal = true
ALLOW FILTERING;

-- All imaging events for Priya Sharma
SELECT event_time, provider, notes
FROM clinical_events
WHERE patient_id = 33333333-3333-3333-3333-333333333333
  AND event_type = 'imaging'
ALLOW FILTERING;

-- Look up Maria Chen's full profile
SELECT name, dob, blood_type, allergies, emergency_contact
FROM patients
WHERE patient_id = 11111111-1111-1111-1111-111111111111;

-- Maria's clinical events from March 10-11 (hospital stay)
SELECT event_time, event_type, department, provider, notes
FROM clinical_events
WHERE patient_id = 11111111-1111-1111-1111-111111111111
  AND event_time >= '2026-03-10 00:00:00'
  AND event_time <= '2026-03-11 23:59:59';

-- Full medication history for Priya Sharma
SELECT medication, dosage, frequency, prescriber, active, prescribed_at
FROM medications
WHERE patient_id = 33333333-3333-3333-3333-333333333333;
