-- Healthcare Records Sample Data
-- Run: cqlsh -f data.cql (after schema.cql)

USE healthcare;

-- ── Patients ──

INSERT INTO patients (patient_id, name, dob, blood_type, allergies, emergency_contact)
VALUES (11111111-1111-1111-1111-111111111111,
        'Maria Chen', '1985-03-14', 'A+',
        {'penicillin', 'sulfa'}, 'James Chen (spouse) 555-0142');

INSERT INTO patients (patient_id, name, dob, blood_type, allergies, emergency_contact)
VALUES (22222222-2222-2222-2222-222222222222,
        'Robert Okafor', '1972-11-28', 'O-',
        {'latex'}, 'Amara Okafor (daughter) 555-0198');

INSERT INTO patients (patient_id, name, dob, blood_type, allergies, emergency_contact)
VALUES (33333333-3333-3333-3333-333333333333,
        'Priya Sharma', '1998-07-02', 'B+',
        {}, 'Dev Sharma (father) 555-0276');

-- ── Clinical Events ──

-- Maria Chen: ER visit and follow-up
INSERT INTO clinical_events (patient_id, event_time, event_id, event_type, department, provider, notes)
VALUES (11111111-1111-1111-1111-111111111111, '2026-03-10 08:15:00', uuid(), 'admission', 'Emergency', 'Dr. Williams', 'Patient presents with acute abdominal pain, onset 6 hours ago');

INSERT INTO clinical_events (patient_id, event_time, event_id, event_type, department, provider, notes)
VALUES (11111111-1111-1111-1111-111111111111, '2026-03-10 09:30:00', uuid(), 'imaging', 'Radiology', 'Dr. Park', 'CT abdomen with contrast ordered, pending penicillin allergy review');

INSERT INTO clinical_events (patient_id, event_time, event_id, event_type, department, provider, notes)
VALUES (11111111-1111-1111-1111-111111111111, '2026-03-10 11:00:00', uuid(), 'procedure', 'Surgery', 'Dr. Reyes', 'Laparoscopic appendectomy performed without complications');

INSERT INTO clinical_events (patient_id, event_time, event_id, event_type, department, provider, notes)
VALUES (11111111-1111-1111-1111-111111111111, '2026-03-11 07:00:00', uuid(), 'discharge', 'Surgery', 'Dr. Reyes', 'Discharged in stable condition, follow-up in 2 weeks');

INSERT INTO clinical_events (patient_id, event_time, event_id, event_type, department, provider, notes)
VALUES (11111111-1111-1111-1111-111111111111, '2026-03-15 14:00:00', uuid(), 'consultation', 'Primary Care', 'Dr. Nguyen', 'Post-op follow-up, incision healing well, no signs of infection');

-- Robert Okafor: cardiology workup
INSERT INTO clinical_events (patient_id, event_time, event_id, event_type, department, provider, notes)
VALUES (22222222-2222-2222-2222-222222222222, '2026-03-08 10:00:00', uuid(), 'consultation', 'Cardiology', 'Dr. Adams', 'Annual cardiac evaluation, patient reports occasional palpitations');

INSERT INTO clinical_events (patient_id, event_time, event_id, event_type, department, provider, notes)
VALUES (22222222-2222-2222-2222-222222222222, '2026-03-08 11:30:00', uuid(), 'diagnostic', 'Cardiology', 'Tech. Rivera', '12-lead ECG performed, sinus rhythm, no acute changes');

INSERT INTO clinical_events (patient_id, event_time, event_id, event_type, department, provider, notes)
VALUES (22222222-2222-2222-2222-222222222222, '2026-03-09 08:00:00', uuid(), 'diagnostic', 'Cardiology', 'Tech. Rivera', '24-hour Holter monitor placed');

INSERT INTO clinical_events (patient_id, event_time, event_id, event_type, department, provider, notes)
VALUES (22222222-2222-2222-2222-222222222222, '2026-03-12 15:00:00', uuid(), 'consultation', 'Cardiology', 'Dr. Adams', 'Holter results normal, palpitations likely benign, follow-up in 6 months');

-- Priya Sharma: sports medicine
INSERT INTO clinical_events (patient_id, event_time, event_id, event_type, department, provider, notes)
VALUES (33333333-3333-3333-3333-333333333333, '2026-03-05 16:45:00', uuid(), 'admission', 'Emergency', 'Dr. Kim', 'Right ankle injury during soccer match, significant swelling');

INSERT INTO clinical_events (patient_id, event_time, event_id, event_type, department, provider, notes)
VALUES (33333333-3333-3333-3333-333333333333, '2026-03-05 17:30:00', uuid(), 'imaging', 'Radiology', 'Dr. Park', 'X-ray right ankle: no fracture, grade 2 lateral sprain');

INSERT INTO clinical_events (patient_id, event_time, event_id, event_type, department, provider, notes)
VALUES (33333333-3333-3333-3333-333333333333, '2026-03-05 18:15:00', uuid(), 'discharge', 'Emergency', 'Dr. Kim', 'RICE protocol, air cast fitted, follow-up with sports medicine');

INSERT INTO clinical_events (patient_id, event_time, event_id, event_type, department, provider, notes)
VALUES (33333333-3333-3333-3333-333333333333, '2026-03-12 09:00:00', uuid(), 'consultation', 'Sports Medicine', 'Dr. Torres', 'Swelling reduced, starting physical therapy, 4-6 week recovery');

INSERT INTO clinical_events (patient_id, event_time, event_id, event_type, department, provider, notes)
VALUES (33333333-3333-3333-3333-333333333333, '2026-03-17 09:00:00', uuid(), 'therapy', 'Physical Therapy', 'PT. Marshall', 'First PT session, range of motion exercises, patient tolerating well');

-- ── Lab Results ──

-- Maria Chen: pre-surgical and post-op labs
INSERT INTO lab_results (patient_id, collected_at, test_name, result_value, unit, reference_range, abnormal)
VALUES (11111111-1111-1111-1111-111111111111, '2026-03-10 08:30:00', 'WBC', '14.2', 'x10^9/L', '4.5-11.0', true);

INSERT INTO lab_results (patient_id, collected_at, test_name, result_value, unit, reference_range, abnormal)
VALUES (11111111-1111-1111-1111-111111111111, '2026-03-10 08:30:01', 'Hemoglobin', '13.8', 'g/dL', '12.0-16.0', false);

INSERT INTO lab_results (patient_id, collected_at, test_name, result_value, unit, reference_range, abnormal)
VALUES (11111111-1111-1111-1111-111111111111, '2026-03-10 08:30:02', 'CRP', '48.5', 'mg/L', '0-10', true);

INSERT INTO lab_results (patient_id, collected_at, test_name, result_value, unit, reference_range, abnormal)
VALUES (11111111-1111-1111-1111-111111111111, '2026-03-11 06:00:00', 'WBC', '9.1', 'x10^9/L', '4.5-11.0', false);

-- Robert Okafor: cardiac panel
INSERT INTO lab_results (patient_id, collected_at, test_name, result_value, unit, reference_range, abnormal)
VALUES (22222222-2222-2222-2222-222222222222, '2026-03-08 10:15:00', 'Total Cholesterol', '242', 'mg/dL', '<200', true);

INSERT INTO lab_results (patient_id, collected_at, test_name, result_value, unit, reference_range, abnormal)
VALUES (22222222-2222-2222-2222-222222222222, '2026-03-08 10:15:01', 'LDL', '158', 'mg/dL', '<100', true);

INSERT INTO lab_results (patient_id, collected_at, test_name, result_value, unit, reference_range, abnormal)
VALUES (22222222-2222-2222-2222-222222222222, '2026-03-08 10:15:02', 'Troponin I', '0.02', 'ng/mL', '<0.04', false);

-- Priya Sharma: routine bloods
INSERT INTO lab_results (patient_id, collected_at, test_name, result_value, unit, reference_range, abnormal)
VALUES (33333333-3333-3333-3333-333333333333, '2026-03-05 17:00:00', 'Hemoglobin', '14.1', 'g/dL', '12.0-16.0', false);

INSERT INTO lab_results (patient_id, collected_at, test_name, result_value, unit, reference_range, abnormal)
VALUES (33333333-3333-3333-3333-333333333333, '2026-03-05 17:00:01', 'Platelets', '285', 'x10^9/L', '150-400', false);

INSERT INTO lab_results (patient_id, collected_at, test_name, result_value, unit, reference_range, abnormal)
VALUES (33333333-3333-3333-3333-333333333333, '2026-03-05 17:00:02', 'D-Dimer', '0.3', 'mg/L', '<0.5', false);

-- ── Medications ──

-- Maria Chen: post-surgical meds
INSERT INTO medications (patient_id, prescribed_at, medication, dosage, frequency, prescriber, active)
VALUES (11111111-1111-1111-1111-111111111111, '2026-03-11 07:00:00', 'Acetaminophen', '500mg', 'every 6 hours', 'Dr. Reyes', true);

INSERT INTO medications (patient_id, prescribed_at, medication, dosage, frequency, prescriber, active)
VALUES (11111111-1111-1111-1111-111111111111, '2026-03-11 07:00:00', 'Cephalexin', '500mg', 'twice daily', 'Dr. Reyes', true);

INSERT INTO medications (patient_id, prescribed_at, medication, dosage, frequency, prescriber, active)
VALUES (11111111-1111-1111-1111-111111111111, '2026-03-10 09:00:00', 'Morphine IV', '4mg', 'as needed', 'Dr. Williams', false);

-- Robert Okafor: cardiac meds
INSERT INTO medications (patient_id, prescribed_at, medication, dosage, frequency, prescriber, active)
VALUES (22222222-2222-2222-2222-222222222222, '2026-03-08 12:00:00', 'Atorvastatin', '20mg', 'once daily', 'Dr. Adams', true);

INSERT INTO medications (patient_id, prescribed_at, medication, dosage, frequency, prescriber, active)
VALUES (22222222-2222-2222-2222-222222222222, '2026-03-08 12:00:00', 'Aspirin', '81mg', 'once daily', 'Dr. Adams', true);

-- Priya Sharma: sports injury meds
INSERT INTO medications (patient_id, prescribed_at, medication, dosage, frequency, prescriber, active)
VALUES (33333333-3333-3333-3333-333333333333, '2026-03-05 18:00:00', 'Ibuprofen', '400mg', 'three times daily', 'Dr. Kim', true);

INSERT INTO medications (patient_id, prescribed_at, medication, dosage, frequency, prescriber, active)
VALUES (33333333-3333-3333-3333-333333333333, '2026-03-05 18:00:00', 'Naproxen', '250mg', 'twice daily', 'Dr. Torres', false);
