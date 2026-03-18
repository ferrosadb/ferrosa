-- Final verification: confirm all data across the scaled cluster

USE app;

-- Event counts per tenant
SELECT COUNT(*) FROM events WHERE tenant_id = 'acme' AND event_date = '2026-03-18';
-- Expected: 10
SELECT COUNT(*) FROM events WHERE tenant_id = 'globex' AND event_date = '2026-03-18';
-- Expected: 7
SELECT COUNT(*) FROM events WHERE tenant_id = 'initech' AND event_date = '2026-03-18';
-- Expected: 3

-- Daily summaries
SELECT * FROM daily_summary WHERE tenant_id = 'acme' AND event_date = '2026-03-18';
SELECT * FROM daily_summary WHERE tenant_id = 'globex' AND event_date = '2026-03-18';

-- Tenant registry
SELECT * FROM tenants;

-- Cluster health
SELECT cluster_name, listen_address, data_center, rack FROM system.local;
SELECT peer, data_center FROM system.peers;
