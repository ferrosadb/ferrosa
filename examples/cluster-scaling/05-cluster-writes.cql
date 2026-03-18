-- Act 3: Raft cluster — writes across a 3-node cluster
-- 2 more for acme (total 10), 3 for new tenant initech (total 3)

USE app;

-- acme events (IDs 9-10)
INSERT INTO events (tenant_id, event_date, event_id, event_type, payload)
    VALUES ('acme', '2026-03-18', 9, 'page_view', '{"url": "/billing"}');
INSERT INTO events (tenant_id, event_date, event_id, event_type, payload)
    VALUES ('acme', '2026-03-18', 10, 'click', '{"button": "upgrade_plan"}');

-- initech events (IDs 1-3, new tenant)
INSERT INTO events (tenant_id, event_date, event_id, event_type, payload)
    VALUES ('initech', '2026-03-18', 1, 'page_view', '{"url": "/home"}');
INSERT INTO events (tenant_id, event_date, event_id, event_type, payload)
    VALUES ('initech', '2026-03-18', 2, 'signup', '{"plan": "trial"}');
INSERT INTO events (tenant_id, event_date, event_id, event_type, payload)
    VALUES ('initech', '2026-03-18', 3, 'page_view', '{"url": "/onboarding"}');

-- Verify counts across all tenants
SELECT COUNT(*) FROM events WHERE tenant_id = 'acme' AND event_date = '2026-03-18';
-- Expected: 10
SELECT COUNT(*) FROM events WHERE tenant_id = 'globex' AND event_date = '2026-03-18';
-- Expected: 7
SELECT COUNT(*) FROM events WHERE tenant_id = 'initech' AND event_date = '2026-03-18';
-- Expected: 3
