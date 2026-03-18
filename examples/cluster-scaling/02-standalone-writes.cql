-- Act 1: Standalone — seed initial event data
-- 5 events for acme, 5 for globex (10 total)

USE app;

-- acme events (IDs 1-5)
INSERT INTO events (tenant_id, event_date, event_id, event_type, payload)
    VALUES ('acme', '2026-03-18', 1, 'page_view', '{"url": "/home"}');
INSERT INTO events (tenant_id, event_date, event_id, event_type, payload)
    VALUES ('acme', '2026-03-18', 2, 'click', '{"button": "signup"}');
INSERT INTO events (tenant_id, event_date, event_id, event_type, payload)
    VALUES ('acme', '2026-03-18', 3, 'signup', '{"plan": "free"}');
INSERT INTO events (tenant_id, event_date, event_id, event_type, payload)
    VALUES ('acme', '2026-03-18', 4, 'page_view', '{"url": "/dashboard"}');
INSERT INTO events (tenant_id, event_date, event_id, event_type, payload)
    VALUES ('acme', '2026-03-18', 5, 'purchase', '{"item": "pro_plan", "amount": 29.99}');

-- globex events (IDs 1-5)
INSERT INTO events (tenant_id, event_date, event_id, event_type, payload)
    VALUES ('globex', '2026-03-18', 1, 'page_view', '{"url": "/products"}');
INSERT INTO events (tenant_id, event_date, event_id, event_type, payload)
    VALUES ('globex', '2026-03-18', 2, 'click', '{"button": "add_to_cart"}');
INSERT INTO events (tenant_id, event_date, event_id, event_type, payload)
    VALUES ('globex', '2026-03-18', 3, 'purchase', '{"item": "widget", "amount": 9.99}');
INSERT INTO events (tenant_id, event_date, event_id, event_type, payload)
    VALUES ('globex', '2026-03-18', 4, 'page_view', '{"url": "/checkout"}');
INSERT INTO events (tenant_id, event_date, event_id, event_type, payload)
    VALUES ('globex', '2026-03-18', 5, 'signup', '{"plan": "enterprise"}');

-- Verify counts
SELECT COUNT(*) FROM events WHERE tenant_id = 'acme' AND event_date = '2026-03-18';
-- Expected: 5
SELECT COUNT(*) FROM events WHERE tenant_id = 'globex' AND event_date = '2026-03-18';
-- Expected: 5
