-- Phase 3: Raft cluster — DDL through consensus
-- Create a tenants table and register all known tenants

USE app;

CREATE TABLE IF NOT EXISTS tenants (
    tenant_id   text PRIMARY KEY,
    name        text,
    plan        text,
    created_at  timestamp,
    contact     text
);

INSERT INTO tenants (tenant_id, name, plan, created_at, contact)
    VALUES ('acme', 'Acme Corporation', 'pro', '2026-01-15T09:00:00Z', 'admin@acme.example');
INSERT INTO tenants (tenant_id, name, plan, created_at, contact)
    VALUES ('globex', 'Globex Corporation', 'enterprise', '2026-02-01T14:30:00Z', 'ops@globex.example');
INSERT INTO tenants (tenant_id, name, plan, created_at, contact)
    VALUES ('initech', 'Initech', 'trial', '2026-03-18T10:00:00Z', 'peter@initech.example');

-- Verify tenant records
SELECT * FROM tenants;
