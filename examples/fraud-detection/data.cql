-- Fraud Detection Sample Data
-- Run: cqlsh -f data.cql (after schema.cql)

USE fraud;

-- ── Account Profiles ──

-- Regular consumer, moderate spending
INSERT INTO account_profile
    (account_id, customer_name, home_country, avg_transaction, max_transaction,
     last_transaction_time, flagged, risk_level)
VALUES ('ACCT-1001', 'Sarah Chen', 'US', 85.50, 450.00,
    '2026-03-17 18:30:00+0000', false, 'low');

-- Business traveler, higher spending, multiple countries
INSERT INTO account_profile
    (account_id, customer_name, home_country, avg_transaction, max_transaction,
     last_transaction_time, flagged, risk_level)
VALUES ('ACCT-2002', 'Marcus Johnson', 'US', 220.00, 3500.00,
    '2026-03-17 20:15:00+0000', false, 'medium');

-- New account, limited history
INSERT INTO account_profile
    (account_id, customer_name, home_country, avg_transaction, max_transaction,
     last_transaction_time, flagged, risk_level)
VALUES ('ACCT-3003', 'Priya Patel', 'GB', 45.00, 200.00,
    '2026-03-17 14:00:00+0000', true, 'high');

-- ── Normal Transactions ──

-- Sarah's normal spending
INSERT INTO transactions (account_id, transaction_time, transaction_id, amount, currency, merchant, merchant_category, location, country, status, risk_score)
VALUES ('ACCT-1001', '2026-03-17 08:15:00+0000', uuid(), 4.75, 'USD', 'Blue Bottle Coffee', 'food_beverage', 'San Francisco, CA', 'US', 'approved', 2);

INSERT INTO transactions (account_id, transaction_time, transaction_id, amount, currency, merchant, merchant_category, location, country, status, risk_score)
VALUES ('ACCT-1001', '2026-03-17 12:30:00+0000', uuid(), 32.50, 'USD', 'Whole Foods Market', 'grocery', 'San Francisco, CA', 'US', 'approved', 1);

INSERT INTO transactions (account_id, transaction_time, transaction_id, amount, currency, merchant, merchant_category, location, country, status, risk_score)
VALUES ('ACCT-1001', '2026-03-17 14:00:00+0000', uuid(), 67.99, 'USD', 'Target', 'retail', 'San Francisco, CA', 'US', 'approved', 3);

-- Marcus's business travel
INSERT INTO transactions (account_id, transaction_time, transaction_id, amount, currency, merchant, merchant_category, location, country, status, risk_score)
VALUES ('ACCT-2002', '2026-03-16 09:00:00+0000', uuid(), 189.00, 'USD', 'Delta Airlines', 'travel', 'Atlanta, GA', 'US', 'approved', 5);

INSERT INTO transactions (account_id, transaction_time, transaction_id, amount, currency, merchant, merchant_category, location, country, status, risk_score)
VALUES ('ACCT-2002', '2026-03-16 19:30:00+0000', uuid(), 245.00, 'USD', 'Marriott Downtown', 'lodging', 'Chicago, IL', 'US', 'approved', 4);

INSERT INTO transactions (account_id, transaction_time, transaction_id, amount, currency, merchant, merchant_category, location, country, status, risk_score)
VALUES ('ACCT-2002', '2026-03-17 07:45:00+0000', uuid(), 28.00, 'USD', 'Starbucks', 'food_beverage', 'Chicago, IL', 'US', 'approved', 1);

-- Priya's normal UK spending
INSERT INTO transactions (account_id, transaction_time, transaction_id, amount, currency, merchant, merchant_category, location, country, status, risk_score)
VALUES ('ACCT-3003', '2026-03-16 10:00:00+0000', uuid(), 12.50, 'GBP', 'Tesco Express', 'grocery', 'London', 'GB', 'approved', 1);

INSERT INTO transactions (account_id, transaction_time, transaction_id, amount, currency, merchant, merchant_category, location, country, status, risk_score)
VALUES ('ACCT-3003', '2026-03-16 15:20:00+0000', uuid(), 35.00, 'GBP', 'Boots Pharmacy', 'pharmacy', 'London', 'GB', 'approved', 2);

-- ── Suspicious Transactions ──

-- Sarah: sudden large purchase, way above her average
INSERT INTO transactions (account_id, transaction_time, transaction_id, amount, currency, merchant, merchant_category, location, country, status, risk_score)
VALUES ('ACCT-1001', '2026-03-17 18:30:00+0000', d1a7e100-e39b-11ef-8001-000000000010, 2899.99, 'USD', 'ElectroMart Online', 'electronics', 'San Francisco, CA', 'US', 'pending_review', 85);

-- Sarah: another large purchase 2 minutes later
INSERT INTO transactions (account_id, transaction_time, transaction_id, amount, currency, merchant, merchant_category, location, country, status, risk_score)
VALUES ('ACCT-1001', '2026-03-17 18:32:00+0000', d1a7e100-e39b-11ef-8001-000000000011, 1549.00, 'USD', 'LuxWatch.com', 'luxury', 'San Francisco, CA', 'US', 'declined', 92);

-- Marcus: transaction in a country he has never visited
INSERT INTO transactions (account_id, transaction_time, transaction_id, amount, currency, merchant, merchant_category, location, country, status, risk_score)
VALUES ('ACCT-2002', '2026-03-17 20:15:00+0000', d1a7e100-e39b-11ef-8001-000000000012, 3200.00, 'EUR', 'Premium Electronics GmbH', 'electronics', 'Berlin', 'DE', 'pending_review', 78);

-- Priya: card used in two countries within hours (impossible travel)
INSERT INTO transactions (account_id, transaction_time, transaction_id, amount, currency, merchant, merchant_category, location, country, status, risk_score)
VALUES ('ACCT-3003', '2026-03-17 11:00:00+0000', d1a7e100-e39b-11ef-8001-000000000013, 850.00, 'USD', 'Best Buy', 'electronics', 'New York, NY', 'US', 'declined', 95);

INSERT INTO transactions (account_id, transaction_time, transaction_id, amount, currency, merchant, merchant_category, location, country, status, risk_score)
VALUES ('ACCT-3003', '2026-03-17 12:30:00+0000', d1a7e100-e39b-11ef-8001-000000000014, 420.00, 'USD', 'Amazon.com', 'online_retail', 'New York, NY', 'US', 'declined', 88);

INSERT INTO transactions (account_id, transaction_time, transaction_id, amount, currency, merchant, merchant_category, location, country, status, risk_score)
VALUES ('ACCT-3003', '2026-03-17 14:00:00+0000', d1a7e100-e39b-11ef-8001-000000000015, 199.99, 'GBP', 'John Lewis', 'retail', 'London', 'GB', 'approved', 15);

-- ── Alerts ──

INSERT INTO alerts (alert_date, alert_time, alert_id, account_id, transaction_id, rule_triggered, severity, details, resolved)
VALUES ('2026-03-17', '2026-03-17 18:30:05+0000', uuid(),
    'ACCT-1001', d1a7e100-e39b-11ef-8001-000000000010,
    'amount_exceeds_3x_average', 'high',
    'Transaction of $2899.99 is 33x above account average of $85.50', false);

INSERT INTO alerts (alert_date, alert_time, alert_id, account_id, transaction_id, rule_triggered, severity, details, resolved)
VALUES ('2026-03-17', '2026-03-17 18:32:05+0000', uuid(),
    'ACCT-1001', d1a7e100-e39b-11ef-8001-000000000011,
    'rapid_succession', 'critical',
    'Two high-value transactions within 2 minutes. Combined: $4448.99', false);

INSERT INTO alerts (alert_date, alert_time, alert_id, account_id, transaction_id, rule_triggered, severity, details, resolved)
VALUES ('2026-03-17', '2026-03-17 20:15:05+0000', uuid(),
    'ACCT-2002', d1a7e100-e39b-11ef-8001-000000000012,
    'unusual_country', 'medium',
    'Transaction in DE (Germany), account has no history in this country', false);

INSERT INTO alerts (alert_date, alert_time, alert_id, account_id, transaction_id, rule_triggered, severity, details, resolved)
VALUES ('2026-03-17', '2026-03-17 12:30:05+0000', uuid(),
    'ACCT-3003', d1a7e100-e39b-11ef-8001-000000000013,
    'impossible_travel', 'critical',
    'Card used in US (New York) 1 hour after UK (London) purchase. Travel time: ~8 hours minimum.', false);

INSERT INTO alerts (alert_date, alert_time, alert_id, account_id, transaction_id, rule_triggered, severity, details, resolved)
VALUES ('2026-03-17', '2026-03-17 12:30:10+0000', uuid(),
    'ACCT-3003', d1a7e100-e39b-11ef-8001-000000000014,
    'velocity_check', 'high',
    '3 transactions in 3 hours from a new-geography IP range', true);

-- ── Additional Normal Transactions ──

-- More of Sarah's routine spending
INSERT INTO transactions (account_id, transaction_time, transaction_id, amount, currency, merchant, merchant_category, location, country, status, risk_score)
VALUES ('ACCT-1001', '2026-03-16 08:00:00+0000', uuid(), 5.25, 'USD', 'Blue Bottle Coffee', 'food_beverage', 'San Francisco, CA', 'US', 'approved', 1);

INSERT INTO transactions (account_id, transaction_time, transaction_id, amount, currency, merchant, merchant_category, location, country, status, risk_score)
VALUES ('ACCT-1001', '2026-03-16 12:45:00+0000', uuid(), 18.90, 'USD', 'Chipotle', 'food_beverage', 'San Francisco, CA', 'US', 'approved', 1);

INSERT INTO transactions (account_id, transaction_time, transaction_id, amount, currency, merchant, merchant_category, location, country, status, risk_score)
VALUES ('ACCT-1001', '2026-03-16 17:30:00+0000', uuid(), 42.00, 'USD', 'Shell Gas Station', 'fuel', 'San Francisco, CA', 'US', 'approved', 2);

-- Marcus's typical business expenses
INSERT INTO transactions (account_id, transaction_time, transaction_id, amount, currency, merchant, merchant_category, location, country, status, risk_score)
VALUES ('ACCT-2002', '2026-03-17 12:00:00+0000', uuid(), 78.50, 'USD', 'Ruth Chris Steakhouse', 'restaurant', 'Chicago, IL', 'US', 'approved', 3);

INSERT INTO transactions (account_id, transaction_time, transaction_id, amount, currency, merchant, merchant_category, location, country, status, risk_score)
VALUES ('ACCT-2002', '2026-03-17 16:00:00+0000', uuid(), 35.00, 'USD', 'Uber', 'transportation', 'Chicago, IL', 'US', 'approved', 2);
