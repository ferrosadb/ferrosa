-- Telecommunications Platform Sample Data
-- Run: cqlsh -f data.cql (after schema.cql)

USE telecom;

-- ── Subscribers ──

INSERT INTO subscribers (subscriber_id, name, plan_type, monthly_limit_minutes, monthly_limit_data_gb, balance, activated_at)
VALUES ('SUB-1001', 'Alice Nakamura', 'unlimited', -1, 50, 85.00, '2024-06-15 10:00:00');

INSERT INTO subscribers (subscriber_id, name, plan_type, monthly_limit_minutes, monthly_limit_data_gb, balance, activated_at)
VALUES ('SUB-1002', 'Ben Carlisle', 'prepaid', 500, 10, 22.50, '2025-01-20 14:30:00');

INSERT INTO subscribers (subscriber_id, name, plan_type, monthly_limit_minutes, monthly_limit_data_gb, balance, activated_at)
VALUES ('SUB-1003', 'Carmen Delgado', 'family', 1000, 30, 110.00, '2023-09-01 09:00:00');

-- ── Call Records ──

-- Alice Nakamura: March 17, 2026
INSERT INTO call_records (subscriber_id, call_date, call_time, call_id, call_type, duration_seconds, data_bytes, destination, cell_tower, cost)
VALUES ('SUB-1001', '2026-03-17', '2026-03-17 08:12:00', uuid(), 'voice', 245, 0, '+1-555-0199', 'TWR-NYC-042', 0.00);

INSERT INTO call_records (subscriber_id, call_date, call_time, call_id, call_type, duration_seconds, data_bytes, destination, cell_tower, cost)
VALUES ('SUB-1001', '2026-03-17', '2026-03-17 09:45:00', uuid(), 'data', 0, 52428800, 'streaming', 'TWR-NYC-042', 0.00);

INSERT INTO call_records (subscriber_id, call_date, call_time, call_id, call_type, duration_seconds, data_bytes, destination, cell_tower, cost)
VALUES ('SUB-1001', '2026-03-17', '2026-03-17 12:30:00', uuid(), 'sms', 0, 0, '+1-555-0234', 'TWR-NYC-015', 0.00);

INSERT INTO call_records (subscriber_id, call_date, call_time, call_id, call_type, duration_seconds, data_bytes, destination, cell_tower, cost)
VALUES ('SUB-1001', '2026-03-17', '2026-03-17 14:05:00', uuid(), 'voice', 480, 0, '+1-555-0301', 'TWR-NYC-015', 0.00);

INSERT INTO call_records (subscriber_id, call_date, call_time, call_id, call_type, duration_seconds, data_bytes, destination, cell_tower, cost)
VALUES ('SUB-1001', '2026-03-17', '2026-03-17 18:20:00', uuid(), 'data', 0, 104857600, 'video-call', 'TWR-NYC-042', 0.00);

INSERT INTO call_records (subscriber_id, call_date, call_time, call_id, call_type, duration_seconds, data_bytes, destination, cell_tower, cost)
VALUES ('SUB-1001', '2026-03-17', '2026-03-17 21:10:00', uuid(), 'sms', 0, 0, '+1-555-0199', 'TWR-NYC-042', 0.00);

-- Ben Carlisle: March 17, 2026 (prepaid, costs apply)
INSERT INTO call_records (subscriber_id, call_date, call_time, call_id, call_type, duration_seconds, data_bytes, destination, cell_tower, cost)
VALUES ('SUB-1002', '2026-03-17', '2026-03-17 07:30:00', uuid(), 'voice', 120, 0, '+1-555-0455', 'TWR-CHI-008', 0.24);

INSERT INTO call_records (subscriber_id, call_date, call_time, call_id, call_type, duration_seconds, data_bytes, destination, cell_tower, cost)
VALUES ('SUB-1002', '2026-03-17', '2026-03-17 10:15:00', uuid(), 'sms', 0, 0, '+1-555-0488', 'TWR-CHI-008', 0.10);

INSERT INTO call_records (subscriber_id, call_date, call_time, call_id, call_type, duration_seconds, data_bytes, destination, cell_tower, cost)
VALUES ('SUB-1002', '2026-03-17', '2026-03-17 13:00:00', uuid(), 'data', 0, 20971520, 'browsing', 'TWR-CHI-012', 0.50);

INSERT INTO call_records (subscriber_id, call_date, call_time, call_id, call_type, duration_seconds, data_bytes, destination, cell_tower, cost)
VALUES ('SUB-1002', '2026-03-17', '2026-03-17 16:45:00', uuid(), 'voice', 900, 0, '+44-20-7946-0958', 'TWR-CHI-008', 4.50);

INSERT INTO call_records (subscriber_id, call_date, call_time, call_id, call_type, duration_seconds, data_bytes, destination, cell_tower, cost)
VALUES ('SUB-1002', '2026-03-17', '2026-03-17 19:30:00', uuid(), 'sms', 0, 0, '+1-555-0455', 'TWR-CHI-012', 0.10);

-- Carmen Delgado: March 17, 2026
INSERT INTO call_records (subscriber_id, call_date, call_time, call_id, call_type, duration_seconds, data_bytes, destination, cell_tower, cost)
VALUES ('SUB-1003', '2026-03-17', '2026-03-17 06:50:00', uuid(), 'voice', 360, 0, '+1-555-0612', 'TWR-MIA-003', 0.36);

INSERT INTO call_records (subscriber_id, call_date, call_time, call_id, call_type, duration_seconds, data_bytes, destination, cell_tower, cost)
VALUES ('SUB-1003', '2026-03-17', '2026-03-17 11:20:00', uuid(), 'data', 0, 209715200, 'app-download', 'TWR-MIA-003', 1.00);

INSERT INTO call_records (subscriber_id, call_date, call_time, call_id, call_type, duration_seconds, data_bytes, destination, cell_tower, cost)
VALUES ('SUB-1003', '2026-03-17', '2026-03-17 15:00:00', uuid(), 'voice', 1200, 0, '+52-55-1234-5678', 'TWR-MIA-007', 6.00);

INSERT INTO call_records (subscriber_id, call_date, call_time, call_id, call_type, duration_seconds, data_bytes, destination, cell_tower, cost)
VALUES ('SUB-1003', '2026-03-17', '2026-03-17 17:40:00', uuid(), 'sms', 0, 0, '+1-555-0612', 'TWR-MIA-003', 0.10);

INSERT INTO call_records (subscriber_id, call_date, call_time, call_id, call_type, duration_seconds, data_bytes, destination, cell_tower, cost)
VALUES ('SUB-1003', '2026-03-17', '2026-03-17 20:15:00', uuid(), 'data', 0, 524288000, 'streaming', 'TWR-MIA-003', 2.50);

-- ── Billing Summaries ──

INSERT INTO billing_summary (subscriber_id, billing_month, total_voice_minutes, total_sms, total_data_mb, total_cost)
VALUES ('SUB-1001', '2026-03', 142, 38, 4200, 85.00);

INSERT INTO billing_summary (subscriber_id, billing_month, total_voice_minutes, total_sms, total_data_mb, total_cost)
VALUES ('SUB-1001', '2026-02', 128, 45, 3800, 85.00);

INSERT INTO billing_summary (subscriber_id, billing_month, total_voice_minutes, total_sms, total_data_mb, total_cost)
VALUES ('SUB-1002', '2026-03', 67, 22, 1500, 18.40);

INSERT INTO billing_summary (subscriber_id, billing_month, total_voice_minutes, total_sms, total_data_mb, total_cost)
VALUES ('SUB-1002', '2026-02', 45, 18, 980, 12.75);

INSERT INTO billing_summary (subscriber_id, billing_month, total_voice_minutes, total_sms, total_data_mb, total_cost)
VALUES ('SUB-1003', '2026-03', 210, 55, 8500, 110.00);

INSERT INTO billing_summary (subscriber_id, billing_month, total_voice_minutes, total_sms, total_data_mb, total_cost)
VALUES ('SUB-1003', '2026-02', 195, 62, 7200, 110.00);

-- ── Network Events ──

INSERT INTO network_events (cell_tower, event_date, event_time, event_type, severity, description)
VALUES ('TWR-NYC-042', '2026-03-17', '2026-03-17 03:00:00', 'maintenance', 'info', 'Scheduled firmware update completed successfully');

INSERT INTO network_events (cell_tower, event_date, event_time, event_type, severity, description)
VALUES ('TWR-NYC-042', '2026-03-17', '2026-03-17 14:22:00', 'capacity', 'warning', 'Active connections at 85% capacity, consider load balancing');

INSERT INTO network_events (cell_tower, event_date, event_time, event_type, severity, description)
VALUES ('TWR-CHI-008', '2026-03-17', '2026-03-17 09:15:00', 'outage', 'critical', 'Power failure detected, switching to backup generator');

INSERT INTO network_events (cell_tower, event_date, event_time, event_type, severity, description)
VALUES ('TWR-CHI-008', '2026-03-17', '2026-03-17 09:18:00', 'recovery', 'info', 'Backup generator online, service restored in 3 minutes');

INSERT INTO network_events (cell_tower, event_date, event_time, event_type, severity, description)
VALUES ('TWR-MIA-003', '2026-03-17', '2026-03-17 20:00:00', 'capacity', 'warning', 'Streaming traffic surge, bandwidth utilization at 92%');

INSERT INTO network_events (cell_tower, event_date, event_time, event_type, severity, description)
VALUES ('TWR-MIA-007', '2026-03-17', '2026-03-17 11:00:00', 'maintenance', 'info', 'Antenna alignment check passed, signal strength nominal');
