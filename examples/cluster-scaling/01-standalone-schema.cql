-- Act 1: Development / Standalone Mode
-- Create the application keyspace and core table.
-- RF=1 is appropriate for single-node development.

CREATE KEYSPACE IF NOT EXISTS app
    WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

CREATE TABLE IF NOT EXISTS app.events (
    tenant_id  text,
    event_date text,
    event_id   int,
    event_type text,
    payload    text,
    PRIMARY KEY ((tenant_id, event_date), event_id)
) WITH CLUSTERING ORDER BY (event_id ASC);
